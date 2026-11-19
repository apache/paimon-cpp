/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/common/reader/concat_batch_reader.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/executor/reader_build_executor.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {

class LifetimeTrackingBatchReader : public BatchReader {
 public:
    LifetimeTrackingBatchReader(std::unique_ptr<BatchReader> reader,
                                const std::shared_ptr<void>& lifetime)
        : reader_(std::move(reader)), lifetime_(lifetime) {}

    Result<ReadBatch> NextBatch() override {
        return reader_->NextBatch();
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        return reader_->NextBatchWithBitmap();
    }

    void Close() override {
        reader_->Close();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

 private:
    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<void> lifetime_;
};

class FixedMetricsBatchReader : public BatchReader {
 public:
    FixedMetricsBatchReader(std::unique_ptr<BatchReader> reader, uint64_t latency,
                            uint64_t io_count)
        : reader_(std::move(reader)), metrics_(std::make_shared<MetricsImpl>()) {
        metrics_->SetCounter("orc.read.inclusive.latency.us", latency);
        metrics_->SetCounter("orc.read.io.count", io_count);
    }

    Result<ReadBatch> NextBatch() override {
        return reader_->NextBatch();
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        return reader_->NextBatchWithBitmap();
    }

    void Close() override {
        reader_->Close();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

 private:
    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<Metrics> metrics_;
};

namespace {
struct ReaderCounts {
    std::atomic<size_t> created = 0;
    std::atomic<size_t> live = 0;
    std::atomic<size_t> peak = 0;
    std::atomic<size_t> closed = 0;
};

class CountedFileBatchReader : public MockFileBatchReader {
 public:
    CountedFileBatchReader(const std::shared_ptr<arrow::Array>& data,
                           const std::shared_ptr<ReaderCounts>& counts, bool empty_bitmap = false)
        : MockFileBatchReader(data, data->type(), 2), counts_(counts), empty_bitmap_(empty_bitmap) {
        EnableRandomizeBatchSize(false);
        counts_->created++;
        const size_t live = ++counts_->live;
        size_t peak = counts_->peak.load();
        while (peak < live && !counts_->peak.compare_exchange_weak(peak, live)) {
        }
    }

    ~CountedFileBatchReader() override {
        counts_->live--;
    }

    void SelectSecondRow() {
        bitmap_ = RoaringBitmap32::From({1});
    }

    void SetCloseCallback(std::function<void()> callback) {
        close_callback_ = std::move(callback);
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch,
                               MockFileBatchReader::NextBatchWithBitmap());
        if (empty_bitmap_) {
            batch.second = RoaringBitmap32();
        }
        return batch;
    }

    void Close() override {
        counts_->closed++;
        if (close_callback_) {
            close_callback_();
        }
    }

 private:
    std::shared_ptr<ReaderCounts> counts_;
    bool empty_bitmap_;
    std::function<void()> close_callback_;
};

std::shared_ptr<arrow::Array> ReaderData(size_t index) {
    return arrow::ipc::internal::json::ArrayFromJSON(
               arrow::struct_({arrow::field("f1", arrow::int32())}),
               fmt::format("[[{}], [{}]]", index * 2, index * 2 + 1))
        .ValueOrDie();
}

/// Logs its Warmup() and NextBatchWithBitmap() calls as "warmup <name>" and "read <name>" into a
/// log the test owns, because ConcatBatchReader destroys a child once it reaches EOF and the
/// child's own state goes with it.
class EventLoggingFileBatchReader : public MockFileBatchReader {
 public:
    EventLoggingFileBatchReader(const std::shared_ptr<arrow::Array>& data, const std::string& name,
                                std::vector<std::string>* events)
        : MockFileBatchReader(data, data->type(), /*read_batch_size=*/1),
          name_(name),
          events_(events) {}

    void Warmup() override {
        events_->push_back("warmup " + name_);
        MockFileBatchReader::Warmup();
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        events_->push_back("read " + name_);
        return MockFileBatchReader::NextBatchWithBitmap();
    }

 private:
    std::string name_;
    std::vector<std::string>* events_;
};

int64_t CountEvent(const std::vector<std::string>& events, const std::string& event) {
    return std::count(events.begin(), events.end(), event);
}

/// Position of the first \p event in \p events, or events.size() when it never happened.
size_t FirstEvent(const std::vector<std::string>& events, const std::string& event) {
    return static_cast<size_t>(std::find(events.begin(), events.end(), event) - events.begin());
}
}  // namespace

class ConcatBatchReaderTest : public ::testing::Test {
    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void CheckResult(const std::vector<std::string>& batches, const std::string& expected) {
        std::vector<std::pair<std::string, std::vector<int32_t>>> batches_with_bitmap;
        for (const auto& batch_str : batches) {
            int32_t row_count = std::count(batch_str.begin(), batch_str.end(), ',');
            if (batch_str != "[]") {
                row_count += 1;
            }
            std::vector<int32_t> bitmap_data;
            for (int32_t i = 0; i < row_count; i++) {
                bitmap_data.push_back(i);
            }
            batches_with_bitmap.emplace_back(batch_str, bitmap_data);
        }
        return CheckResult(batches_with_bitmap, expected);
    }

    void CheckResult(const std::vector<std::pair<std::string, std::vector<int32_t>>>& batches,
                     const std::string& expected) {
        for (const auto& batch_size : {1, 2, 4, 8}) {
            SCOPED_TRACE(batch_size);
            ConcatBatchReader::ReaderFactory factory =
                [batches, batch_size](size_t index) -> Result<std::unique_ptr<BatchReader>> {
                const auto& [batch_str, bitmap_data] = batches[index];
                auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), batch_str)
                              .ValueOrDie();
                std::shared_ptr<arrow::Array> data =
                    arrow::StructArray::Make({f1}, {arrow::field("f1", arrow::int32())})
                        .ValueOrDie();
                return std::make_unique<MockFileBatchReader>(
                    data, data->type(), RoaringBitmap32::From(bitmap_data), batch_size);
            };
            for (bool lazy : {false, true}) {
                SCOPED_TRACE(lazy);
                std::unique_ptr<ConcatBatchReader> concat_reader;
                if (lazy) {
                    ASSERT_OK_AND_ASSIGN(
                        concat_reader,
                        ConcatBatchReader::CreateLazy(
                            batches.size(), factory, ConcatBatchReader::kDefaultPreparationWindow,
                            GetArrowPool(pool_), GetReaderBuildExecutor()));
                } else {
                    std::vector<std::unique_ptr<BatchReader>> readers;
                    for (size_t i = 0; i < batches.size(); i++) {
                        ASSERT_OK_AND_ASSIGN(auto reader, factory(i));
                        readers.push_back(std::move(reader));
                    }
                    concat_reader = std::make_unique<ConcatBatchReader>(std::move(readers),
                                                                        GetArrowPool(pool_));
                }
                ASSERT_OK_AND_ASSIGN(auto result_chunk_array,
                                     ReadResultCollector::CollectResult(std::move(concat_reader)));
                if (expected.empty()) {
                    ASSERT_FALSE(result_chunk_array);
                    continue;
                }
                auto expected_f1 =
                    arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), expected)
                        .ValueOrDie();
                std::shared_ptr<arrow::Array> expected_array =
                    arrow::StructArray::Make({expected_f1}, {arrow::field("f1", arrow::int32())})
                        .ValueOrDie();
                auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
                ASSERT_TRUE(expected_chunk_array->Equals(result_chunk_array))
                    << result_chunk_array->ToString();
            }
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};
TEST_F(ConcatBatchReaderTest, TestSimple) {
    CheckResult({"[10, 11, 12, 13, 14]"}, "[10, 11, 12, 13, 14]");

    CheckResult({"[10, 11, 12, 13, 14]", "[16, 17, 20]", "[24]", "[100]"},
                "[10, 11, 12, 13, 14, 16, 17, 20, 24, 100]");

    CheckResult({"[]", "[10, 11, 12, 13, 14]", "[16, 17, 20]", "[24]", "[100]"},
                "[10, 11, 12, 13, 14, 16, 17, 20, 24, 100]");

    CheckResult({"[10, 11, 12, 13, 14]", "[]", "[16, 17, 20]", "[24]", "[100]"},
                "[10, 11, 12, 13, 14, 16, 17, 20, 24, 100]");

    CheckResult({"[10, 11, 12, 13, 14]", "[16, 17, 20]", "[24]", "[100]", "[]"},
                "[10, 11, 12, 13, 14, 16, 17, 20, 24, 100]");

    // no data in reader
    CheckResult({"[]"}, "");

    // no reader
    CheckResult(std::vector<std::string>{}, "");
}

TEST_F(ConcatBatchReaderTest, TestSimpleWithBitmap) {
    {
        std::vector<std::pair<std::string, std::vector<int32_t>>> src_data = {
            {"[10, 11, 12, 13, 14]", {0, 1, 3, 4}}};
        CheckResult(src_data, "[10, 11, 13, 14]");
    }
    {
        std::vector<std::pair<std::string, std::vector<int32_t>>> src_data = {
            {"[10, 11, 12, 13, 14]", {1, 2, 3}},
            {"[16, 17, 20]", {0, 2}},
            {"[24]", {}},
            {"[100]", {0}}};
        CheckResult(src_data, "[11, 12, 13, 16, 20, 100]");
    }
    {
        std::vector<std::pair<std::string, std::vector<int32_t>>> src_data = {
            {"[]", {}},
            {"[10, 11, 12, 13, 14]", {1, 2, 3}},
            {"[16, 17, 20]", {0, 2}},
            {"[24]", {}},
            {"[100]", {0}}};
        CheckResult(src_data, "[11, 12, 13, 16, 20, 100]");
    }
    {
        std::vector<std::pair<std::string, std::vector<int32_t>>> src_data = {
            {"[10, 11, 12, 13, 14]", {1, 2, 3}},
            {"[]", {}},
            {"[16, 17, 20]", {0, 2}},
            {"[24]", {}},
            {"[100]", {0}}};
        CheckResult(src_data, "[11, 12, 13, 16, 20, 100]");
    }
    {
        std::vector<std::pair<std::string, std::vector<int32_t>>> src_data = {
            {"[10, 11, 12, 13, 14]", {1, 2, 3}},
            {"[16, 17, 20]", {0, 2}},
            {"[24]", {}},
            {"[100]", {0}},
            {"[]", {}},
        };
        CheckResult(src_data, "[11, 12, 13, 16, 20, 100]");
    }
    {
        // no data in reader
        std::vector<std::pair<std::string, std::vector<int32_t>>> src_data = {{"[]", {}},
                                                                              {"[]", {}}};
        CheckResult(src_data, "");
    }
    {
        // no reader
        std::vector<std::pair<std::string, std::vector<int32_t>>> src_data = {};
        CheckResult(src_data, "");
    }
}

TEST_F(ConcatBatchReaderTest, TestReleaseReaderAtEof) {
    auto empty = arrow::ipc::internal::json::ArrayFromJSON(
                     arrow::struct_({arrow::field("f1", arrow::int32())}), "[]")
                     .ValueOrDie();
    auto data = arrow::ipc::internal::json::ArrayFromJSON(
                    arrow::struct_({arrow::field("f1", arrow::int32())}), "[[1]]")
                    .ValueOrDie();
    std::shared_ptr<int32_t> lifetime = std::make_shared<int32_t>(0);
    std::weak_ptr<int32_t> weak_lifetime = lifetime;

    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.push_back(std::make_unique<LifetimeTrackingBatchReader>(
        std::make_unique<MockFileBatchReader>(empty, empty->type(), /*read_batch_size=*/1),
        lifetime));
    readers.push_back(
        std::make_unique<MockFileBatchReader>(data, data->type(), /*read_batch_size=*/1));
    lifetime.reset();

    ConcatBatchReader reader(std::move(readers), GetArrowPool(pool_));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, reader.NextBatchWithBitmap());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    ASSERT_TRUE(weak_lifetime.expired());
    ReaderUtils::ReleaseReadBatch(std::move(batch.first));
}

TEST_F(ConcatBatchReaderTest, TestCollectMetricsAfterReleasingReaders) {
    std::shared_ptr<arrow::DataType> type = arrow::struct_({arrow::field("f1", arrow::int32())});
    std::shared_ptr<arrow::Array> data1 =
        arrow::ipc::internal::json::ArrayFromJSON(type, "[[1]]").ValueOrDie();
    std::shared_ptr<arrow::Array> data2 =
        arrow::ipc::internal::json::ArrayFromJSON(type, "[[2]]").ValueOrDie();

    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.push_back(std::make_unique<FixedMetricsBatchReader>(
        std::make_unique<MockFileBatchReader>(data1, type, /*read_batch_size=*/1),
        /*latency=*/11, /*io_count=*/2));
    readers.push_back(std::make_unique<FixedMetricsBatchReader>(
        std::make_unique<MockFileBatchReader>(data2, type, /*read_batch_size=*/1),
        /*latency=*/17, /*io_count=*/3));
    auto concat_reader =
        std::make_unique<ConcatBatchReader>(std::move(readers), GetArrowPool(pool_));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(concat_reader.get()));
    ASSERT_EQ(result->length(), 2);
    std::shared_ptr<Metrics> metrics = concat_reader->GetReaderMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t latency, metrics->GetCounter("orc.read.inclusive.latency.us"));
    ASSERT_EQ(latency, 28);
    ASSERT_OK_AND_ASSIGN(uint64_t io_count, metrics->GetCounter("orc.read.io.count"));
    ASSERT_EQ(io_count, 5);
}

TEST_F(ConcatBatchReaderTest, TestWarmupLooksOneReaderAhead) {
    const std::vector<std::optional<int32_t>> windows = {std::nullopt, 1, 4, 8};
    for (const auto& window : windows) {
        SCOPED_TRACE(window.value_or(0));
        const bool lookahead = !window || window.value() > 1;
        std::vector<std::string> events;
        std::vector<std::shared_ptr<arrow::Array>> data;
        for (size_t i = 0; i < ConcatBatchReader::kDefaultPreparationWindow + 2; i++) {
            data.push_back(ReaderData(i)->Slice(0, i == 0 ? 2 : 1));
        }
        ConcatBatchReader::ReaderFactory factory =
            [&data, &events](size_t index) -> Result<std::unique_ptr<BatchReader>> {
            return std::make_unique<EventLoggingFileBatchReader>(
                data[index], std::string(1, 'a' + index), &events);
        };
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(1));
        std::unique_ptr<ConcatBatchReader> reader;
        if (window) {
            ASSERT_OK_AND_ASSIGN(reader,
                                 ConcatBatchReader::CreateLazy(data.size(), factory, window.value(),
                                                               GetArrowPool(pool_), executor));
        } else {
            std::vector<std::unique_ptr<BatchReader>> readers;
            for (size_t i = 0; i < data.size(); i++) {
                ASSERT_OK_AND_ASSIGN(auto child, factory(i));
                readers.push_back(std::move(child));
            }
            reader = std::make_unique<ConcatBatchReader>(std::move(readers), GetArrowPool(pool_));
        }
        ASSERT_TRUE(events.empty());

        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, reader->NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        ReaderUtils::ReleaseReadBatch(std::move(batch.first));
        ASSERT_LT(FirstEvent(events, "warmup a"), FirstEvent(events, "read a"));
        if (lookahead) {
            ASSERT_LT(FirstEvent(events, "warmup b"), FirstEvent(events, "read a"));
        } else {
            ASSERT_EQ(CountEvent(events, "warmup b"), 0);
        }
        ASSERT_EQ(CountEvent(events, "warmup c"), 0);

        ASSERT_OK_AND_ASSIGN(batch, reader->NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        ReaderUtils::ReleaseReadBatch(std::move(batch.first));
        ASSERT_EQ(CountEvent(events, "warmup c"), 0);

        ASSERT_OK_AND_ASSIGN(batch, reader->NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        ReaderUtils::ReleaseReadBatch(std::move(batch.first));
        ASSERT_LT(FirstEvent(events, "warmup b"), FirstEvent(events, "read b"));
        if (lookahead) {
            ASSERT_LT(FirstEvent(events, "warmup c"), FirstEvent(events, "read b"));
        } else {
            ASSERT_EQ(CountEvent(events, "warmup c"), 0);
        }
        for (size_t i = 2; i < data.size(); i++) {
            ASSERT_EQ(Via(executor.get(), []() {}).wait_for(std::chrono::seconds(5)),
                      std::future_status::ready);
            ASSERT_OK_AND_ASSIGN(batch, reader->NextBatchWithBitmap());
            ASSERT_FALSE(BatchReader::IsEofBatch(batch));
            ReaderUtils::ReleaseReadBatch(std::move(batch.first));
            ASSERT_LT(FirstEvent(events, "warmup " + std::string(1, 'a' + i)),
                      FirstEvent(events, "read " + std::string(1, 'a' + i)));
            if (i + 1 < data.size()) {
                if (lookahead) {
                    ASSERT_LT(FirstEvent(events, "warmup " + std::string(1, 'a' + i + 1)),
                              FirstEvent(events, "read " + std::string(1, 'a' + i)));
                } else {
                    ASSERT_EQ(CountEvent(events, "warmup " + std::string(1, 'a' + i + 1)), 0);
                }
            }
            if (i + 2 < data.size()) {
                ASSERT_EQ(CountEvent(events, "warmup " + std::string(1, 'a' + i + 2)), 0);
            }
        }
        ASSERT_OK_AND_ASSIGN(batch, reader->NextBatchWithBitmap());
        ASSERT_TRUE(BatchReader::IsEofBatch(batch));
        reader->Close();
    }
}

TEST_F(ConcatBatchReaderTest, TestWarmupSkipsReaderThatIsNotFileBatchReader) {
    std::shared_ptr<arrow::DataType> type = arrow::struct_({arrow::field("f1", arrow::int32())});
    std::vector<std::string> events;
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.push_back(std::make_unique<EventLoggingFileBatchReader>(
        arrow::ipc::internal::json::ArrayFromJSON(type, "[[1], [2]]").ValueOrDie(), "a", &events));
    readers.push_back(std::make_unique<LifetimeTrackingBatchReader>(
        std::make_unique<EventLoggingFileBatchReader>(
            arrow::ipc::internal::json::ArrayFromJSON(type, "[[3]]").ValueOrDie(), "b", &events),
        /*lifetime=*/nullptr));
    readers.push_back(std::make_unique<EventLoggingFileBatchReader>(
        arrow::ipc::internal::json::ArrayFromJSON(type, "[[4]]").ValueOrDie(), "c", &events));
    auto concat_reader =
        std::make_unique<ConcatBatchReader>(std::move(readers), GetArrowPool(pool_));

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch,
                         concat_reader->NextBatchWithBitmap());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    ReaderUtils::ReleaseReadBatch(std::move(batch.first));
    // The lookahead window counts every child, so it does not reach past the skipped one.
    ASSERT_EQ(CountEvent(events, "warmup c"), 0);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(concat_reader.get()));
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(type, "[[2], [3], [4]]").ValueOrDie();
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(result))
        << result->ToString();
    ASSERT_EQ(CountEvent(events, "warmup b"), 0);
    ASSERT_LT(FirstEvent(events, "warmup c"), FirstEvent(events, "read b"));
}

TEST_F(ConcatBatchReaderTest, LazyCreationRejectsInvalidInputsBeforeBuilding) {
    std::atomic<int32_t> calls = 0;
    ConcatBatchReader::ReaderFactory factory =
        [&calls](size_t) -> Result<std::unique_ptr<BatchReader>> {
        ++calls;
        return std::unique_ptr<BatchReader>();
    };
    auto pool = GetArrowPool(pool_);
    auto executor = GetReaderBuildExecutor();
    for (size_t reader_count : {0, 3}) {
        SCOPED_TRACE(reader_count);
        ASSERT_NOK_WITH_MSG(ConcatBatchReader::CreateLazy(reader_count, {}, 1, pool, executor),
                            "requires a factory and a build executor");
        ASSERT_NOK_WITH_MSG(ConcatBatchReader::CreateLazy(reader_count, factory, 1, pool, nullptr),
                            "requires a factory and a build executor");
        for (int32_t window : {0, -1}) {
            SCOPED_TRACE(window);
            ASSERT_NOK_WITH_MSG(
                ConcatBatchReader::CreateLazy(reader_count, factory, window, pool, executor),
                "reader preparation window must be positive");
        }
    }
    ASSERT_EQ(calls.load(), 0);
}

TEST_F(ConcatBatchReaderTest, LazyWindowPreservesOrderBitmapAndMetrics) {
    constexpr size_t kFiles = 31;
    for (uint32_t threads : {1u, 3u}) {
        SCOPED_TRACE(threads);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(threads));
        for (int32_t window : {1, 2, 4, 8, 32}) {
            SCOPED_TRACE(window);
            auto counts = std::make_shared<ReaderCounts>();
            auto lifetime = std::make_shared<int32_t>(0);
            std::weak_ptr<int32_t> weak_lifetime = lifetime;
            std::atomic<size_t> factory_calls = 0;
            ConcatBatchReader::ReaderFactory factory =
                [counts, lifetime,
                 &factory_calls](size_t index) -> Result<std::unique_ptr<BatchReader>> {
                factory_calls++;
                if (index % 5 == 0) {
                    return std::unique_ptr<BatchReader>();
                }
                auto data = ReaderData(index);
                if (index % 5 == 1) {
                    data = data->Slice(0, 0);
                }
                auto reader = std::make_unique<CountedFileBatchReader>(data, counts);
                reader->SelectSecondRow();
                return reader;
            };
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 ConcatBatchReader::CreateLazy(kFiles, std::move(factory), window,
                                                               GetArrowPool(pool_), executor));
            lifetime.reset();
            const size_t initial = std::min<size_t>(window, kFiles);
            ASSERT_EQ(factory_calls.load(), initial);
            ASSERT_LE(counts->created.load(), initial);
            uint64_t expected_rows = 0;
            for (size_t i = 0; i < kFiles; i++) {
                if (i % 5 <= 1) {
                    continue;
                }
                auto expected = ReaderData(i);
                std::shared_ptr<arrow::Array> actual;
                if (i % 2 == 0) {
                    ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatchWithBitmap());
                    ASSERT_OK_AND_ASSIGN(actual,
                                         ReadResultCollector::GetArray(std::move(batch.first)));
                    ASSERT_EQ(batch.second, RoaringBitmap32::From({1}));
                } else {
                    ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
                    ASSERT_OK_AND_ASSIGN(actual, ReadResultCollector::GetArray(std::move(batch)));
                    expected = expected->Slice(1, 1);
                }
                ASSERT_NE(actual, nullptr);
                ASSERT_TRUE(actual->Equals(expected)) << actual->ToString();
                expected_rows += 2;
            }
            ASSERT_OK_AND_ASSIGN(auto eof, reader->NextBatch());
            ASSERT_TRUE(BatchReader::IsEofBatch(eof));
            ASSERT_EQ(factory_calls.load(), kFiles);
            ASSERT_LE(counts->peak.load(), initial);
            ASSERT_EQ(counts->live.load(), 0);
            ASSERT_EQ(counts->closed.load(), counts->created.load());
            ASSERT_TRUE(weak_lifetime.expired());
            ASSERT_OK_AND_ASSIGN(auto rows,
                                 reader->GetReaderMetrics()->GetCounter("mock.number.of.rows"));
            ASSERT_EQ(rows, expected_rows);
            reader->Close();
            ASSERT_OK_AND_ASSIGN(auto closed_rows,
                                 reader->GetReaderMetrics()->GetCounter("mock.number.of.rows"));
            ASSERT_EQ(rows, closed_rows);
            ASSERT_OK_AND_ASSIGN(auto eof_with_bitmap, reader->NextBatchWithBitmap());
            ASSERT_TRUE(BatchReader::IsEofBatch(eof_with_bitmap));
        }
    }
}

TEST_F(ConcatBatchReaderTest, LazyEarlyCloseAndDestructionReleaseCreators) {
    constexpr size_t kFiles = 11;
    for (int32_t window : {1, 2, 4, 8, 32}) {
        SCOPED_TRACE(window);
        for (bool explicit_close : {false, true}) {
            SCOPED_TRACE(explicit_close);
            auto counts = std::make_shared<ReaderCounts>();
            auto lifetime = std::make_shared<int32_t>(0);
            std::weak_ptr<int32_t> weak_lifetime = lifetime;
            ASSERT_OK_AND_ASSIGN(
                auto reader,
                ConcatBatchReader::CreateLazy(
                    kFiles,
                    [counts, lifetime](size_t index) -> Result<std::unique_ptr<BatchReader>> {
                        return std::make_unique<CountedFileBatchReader>(ReaderData(index), counts);
                    },
                    window, GetArrowPool(pool_), GetReaderBuildExecutor()));
            lifetime.reset();
            const size_t initial = std::min<size_t>(window, kFiles);
            ASSERT_EQ(counts->created.load(), initial);
            ASSERT_EQ(counts->live.load(), initial);
            ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatchWithBitmap());
            ASSERT_FALSE(BatchReader::IsEofBatch(batch));
            ASSERT_EQ(counts->created.load(), initial);
            if (explicit_close) {
                reader->Close();
                reader->Close();
                ASSERT_OK_AND_ASSIGN(auto eof, reader->NextBatchWithBitmap());
                ASSERT_TRUE(BatchReader::IsEofBatch(eof));
            } else {
                reader.reset();
            }
            ASSERT_EQ(counts->created.load(), initial);
            ASSERT_LE(counts->peak.load(), initial);
            ASSERT_EQ(counts->live.load(), 0);
            ASSERT_EQ(counts->closed.load(), initial);
            ASSERT_TRUE(weak_lifetime.expired());
            auto array = arrow::ImportArray(batch.first.first.get(), batch.first.second.get());
            ASSERT_TRUE(array.ok()) << array.status().ToString();
            ASSERT_TRUE(array.ValueOrDie()->Equals(ReaderData(0)));
        }
    }
}

TEST_F(ConcatBatchReaderTest, LazyEarlyCloseAndDestructionDrainExceptionalBuilds) {
    for (bool explicit_close : {true, false}) {
        SCOPED_TRACE(explicit_close);
        auto counts = std::make_shared<ReaderCounts>();
        auto lifetime = std::make_shared<int32_t>(0);
        std::weak_ptr<int32_t> weak_lifetime = lifetime;
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(1));
        std::promise<void> release;
        auto gate = release.get_future().share();
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            ConcatBatchReader::CreateLazy(
                100,
                [counts, lifetime, gate](size_t index) -> Result<std::unique_ptr<BatchReader>> {
                    if (index == ConcatBatchReader::kDefaultPreparationWindow) {
                        gate.wait();
                        throw std::bad_alloc();
                    }
                    return std::make_unique<CountedFileBatchReader>(ReaderData(index), counts);
                },
                ConcatBatchReader::kDefaultPreparationWindow, GetArrowPool(pool_), executor));
        ScopeGuard unblock([&release]() { release.set_value(); });
        lifetime.reset();
        // Keep the failed build pending until we stop reading, so cleanup consumes its exception.
        for (size_t i = 0; i < ConcatBatchReader::kDefaultPreparationWindow; i++) {
            ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
            ASSERT_FALSE(BatchReader::IsEofBatch(batch));
            ReaderUtils::ReleaseReadBatch(std::move(batch));
        }
        release.set_value();
        unblock.Release();
        // The single worker completes the failed build and the successful builds after it.
        ASSERT_EQ(Via(executor.get(), []() {}).wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        const size_t created = 2 * ConcatBatchReader::kDefaultPreparationWindow - 2;
        ASSERT_EQ(counts->created.load(), created);
        ASSERT_EQ(counts->live.load(), ConcatBatchReader::kDefaultPreparationWindow - 1);
        ASSERT_FALSE(weak_lifetime.expired());
        if (explicit_close) {
            ASSERT_NO_THROW(reader->Close());
            ASSERT_NO_THROW(reader->Close());
        } else {
            reader.reset();
        }
        ASSERT_EQ(counts->created.load(), created);
        ASSERT_EQ(counts->closed.load(), created);
        ASSERT_EQ(counts->live.load(), 0);
        ASSERT_TRUE(weak_lifetime.expired());
    }
}

TEST_F(ConcatBatchReaderTest, LazyInitialFailureDrainsPartialWindow) {
    auto counts = std::make_shared<ReaderCounts>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(2));
    auto lifetime = std::make_shared<int32_t>(0);
    std::weak_ptr<int32_t> weak_lifetime = lifetime;
    std::promise<void> started;
    auto started_future = started.get_future().share();
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::promise<void> closing;
    auto closing_future = closing.get_future();
    ConcatBatchReader::ReaderFactory factory =
        [counts, lifetime, started_future, gate, &started,
         &closing](size_t index) -> Result<std::unique_ptr<BatchReader>> {
        if (index == 1) {
            if (started_future.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
                return Status::IOError("initial builds did not overlap");
            }
            throw std::runtime_error("initial build failed");
        }
        if (index == 2) {
            started.set_value();
            gate.wait();
        }
        auto child = std::make_unique<CountedFileBatchReader>(ReaderData(index), counts);
        if (index == 0) {
            child->SetCloseCallback([&closing]() { closing.set_value(); });
        }
        return child;
    };
    auto create = std::async(std::launch::async, [&, factory = std::move(factory)]() mutable {
        return ConcatBatchReader::CreateLazy(100, std::move(factory),
                                             ConcatBatchReader::kDefaultPreparationWindow,
                                             GetArrowPool(pool_), executor);
    });
    ScopeGuard unblock([&release]() { release.set_value(); });
    lifetime.reset();
    ASSERT_EQ(closing_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(create.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    EXPECT_FALSE(weak_lifetime.expired());
    release.set_value();
    unblock.Release();
    const auto result = create.get();
    ASSERT_NOK_WITH_MSG(result, "initial build failed");
    ASSERT_TRUE(result.status().IsUnknownError());
    ASSERT_EQ(counts->live.load(), 0);
    ASSERT_EQ(counts->created.load(), counts->closed.load());
    ASSERT_LT(counts->created.load(), ConcatBatchReader::kDefaultPreparationWindow);
    ASSERT_TRUE(weak_lifetime.expired());
}

TEST_F(ConcatBatchReaderTest, LazyDeferredFailuresAreTerminal) {
    enum class Failure { Build, OutOfMemory, Read, Bitmap };
    for (int32_t window : {1, 2, 4, 8}) {
        SCOPED_TRACE(window);
        for (Failure failure :
             {Failure::Build, Failure::OutOfMemory, Failure::Read, Failure::Bitmap}) {
            SCOPED_TRACE(static_cast<int32_t>(failure));
            auto counts = std::make_shared<ReaderCounts>();
            ASSERT_OK_AND_ASSIGN(
                auto reader,
                ConcatBatchReader::CreateLazy(
                    100,
                    [counts, failure,
                     window](size_t index) -> Result<std::unique_ptr<BatchReader>> {
                        const bool fail = index == static_cast<size_t>(window);
                        if (fail && failure == Failure::Build) {
                            return Status::IOError("deferred failure");
                        }
                        if (fail && failure == Failure::OutOfMemory) {
                            throw std::bad_alloc();
                        }
                        auto child = std::make_unique<CountedFileBatchReader>(
                            ReaderData(index), counts, fail && failure == Failure::Bitmap);
                        if (fail && failure == Failure::Read) {
                            child->SetNextBatchStatus(Status::IOError("deferred failure"));
                        }
                        return child;
                    },
                    window, GetArrowPool(pool_), GetReaderBuildExecutor()));
            for (int32_t i = 0; i < window; i++) {
                ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
                ASSERT_FALSE(BatchReader::IsEofBatch(batch));
                ReaderUtils::ReleaseReadBatch(std::move(batch));
            }
            const auto result = reader->NextBatch();
            ASSERT_FALSE(result.ok());
            if (failure == Failure::OutOfMemory) {
                ASSERT_TRUE(result.status().IsOutOfMemory());
            } else {
                const char* message =
                    failure == Failure::Bitmap ? "at least one valid row" : "deferred failure";
                ASSERT_NOK_WITH_MSG(result, message);
            }
            const size_t created = counts->created.load();
            ASSERT_EQ(reader->NextBatchWithBitmap().status(), result.status());
            ASSERT_EQ(reader->NextBatch().status(), result.status());
            reader->Close();
            ASSERT_EQ(counts->created.load(), created);
            ASSERT_EQ(counts->live.load(), 0);
            ASSERT_EQ(counts->closed.load(), created);
            ASSERT_LE(counts->peak.load(), window);
        }
    }
}

TEST_F(ConcatBatchReaderTest, LazyRefillOverlapsReadingAndCloseCancelsQueuedBuilds) {
    for (int32_t window : {2, 4, 8}) {
        SCOPED_TRACE(window);
        for (bool explicit_close : {false, true}) {
            SCOPED_TRACE(explicit_close);
            auto counts = std::make_shared<ReaderCounts>();
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(1));
            std::promise<void> started;
            auto started_future = started.get_future();
            std::promise<void> release;
            auto gate = release.get_future().share();
            std::promise<void> closing;
            auto closing_future = closing.get_future();
            ConcatBatchReader::ReaderFactory factory =
                [counts, gate, window, &started,
                 &closing](size_t index) -> Result<std::unique_ptr<BatchReader>> {
                if (index == static_cast<size_t>(window)) {
                    started.set_value();
                    gate.wait();
                }
                auto child = std::make_unique<CountedFileBatchReader>(ReaderData(index), counts);
                if (index == static_cast<size_t>(window - 1)) {
                    child->SetCloseCallback([&closing]() { closing.set_value(); });
                }
                return child;
            };
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 ConcatBatchReader::CreateLazy(100, std::move(factory), window,
                                                               GetArrowPool(pool_), executor));
            ScopeGuard unblock([&release]() { release.set_value(); });
            auto read = std::async(std::launch::async, [&reader, window]() -> Status {
                for (int32_t i = 0; i < window; i++) {
                    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
                    ReaderUtils::ReleaseReadBatch(std::move(batch));
                }
                return Status::OK();
            });
            EXPECT_EQ(read.wait_for(std::chrono::seconds(5)), std::future_status::ready);
            if (read.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
                release.set_value();
                unblock.Release();
                ASSERT_OK(read.get());
                return;
            }
            ASSERT_OK(read.get());
            ASSERT_EQ(started_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
            auto close = std::async(std::launch::async, [&reader, explicit_close]() {
                if (explicit_close) {
                    reader->Close();
                } else {
                    reader.reset();
                }
            });
            const auto closing_status = closing_future.wait_for(std::chrono::seconds(5));
            const auto close_status = close.wait_for(std::chrono::seconds(0));
            release.set_value();
            unblock.Release();
            close.get();
            ASSERT_EQ(closing_status, std::future_status::ready);
            ASSERT_EQ(close_status, std::future_status::timeout);
            ASSERT_EQ(counts->created.load(), window + 1);
            ASSERT_EQ(counts->closed.load(), counts->created.load());
            ASSERT_EQ(counts->live.load(), 0);
        }
    }
}

}  // namespace paimon::test
