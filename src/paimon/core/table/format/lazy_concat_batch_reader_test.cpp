/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/table/format/lazy_concat_batch_reader.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class OneColumnBatchReader : public BatchReader {
 public:
    OneColumnBatchReader(int32_t value, int32_t batches, bool* closed_flag,
                         bool empty_bitmap = false)
        : value_(value),
          remaining_(batches),
          closed_flag_(closed_flag),
          empty_bitmap_(empty_bitmap),
          metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (remaining_-- <= 0) {
            return BatchReader::MakeEofBatch();
        }
        arrow::Int32Builder builder;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(value_));
        std::shared_ptr<arrow::Array> column;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&column));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> struct_array,
            arrow::StructArray::Make({column}, std::vector<std::string>{"id"}));
        auto c_array = std::make_unique<ArrowArray>();
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportArray(*struct_array, c_array.get(), c_schema.get()));
        metrics_->SetCounter("test.rows", ++rows_read_);
        return std::make_pair(std::move(c_array), std::move(c_schema));
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, BatchReader::NextBatchWithBitmap());
        if (empty_bitmap_ && !BatchReader::IsEofBatch(batch)) {
            batch.second = RoaringBitmap32();
        }
        return batch;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        if (closed_flag_ != nullptr) {
            *closed_flag_ = true;
        }
    }

 private:
    int32_t value_;
    int32_t remaining_;
    bool* closed_flag_;
    bool empty_bitmap_;
    uint64_t rows_read_ = 0;
    std::shared_ptr<Metrics> metrics_;
};

class FailingBatchReader : public BatchReader {
 public:
    explicit FailingBatchReader(Status failure, bool* closed_flag = nullptr)
        : failure_(std::move(failure)),
          closed_flag_(closed_flag),
          metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        return failure_;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        if (closed_flag_) {
            *closed_flag_ = true;
        }
    }

 private:
    Status failure_;
    bool* closed_flag_;
    std::shared_ptr<Metrics> metrics_;
};

class ReadErrorDetail final : public StatusDetail {
 public:
    const char* type_id() const override {
        return "read-error";
    }
    std::string ToString() const override {
        return "test read error";
    }
};

std::vector<LazyConcatBatchReader::Source> SourcesOf(
    std::vector<LazyConcatBatchReader::ReaderFactory>&& factories) {
    std::vector<LazyConcatBatchReader::Source> sources;
    sources.reserve(factories.size());
    for (size_t i = 0; i < factories.size(); i++) {
        sources.push_back({fmt::format("file-{}", i), std::move(factories[i])});
    }
    return sources;
}

}  // namespace

TEST(LazyConcatBatchReaderTest, TestReadersOpenInOrderAndPreserveMetricsAfterClose) {
    int32_t opened = 0;
    bool first_closed = false;
    bool second_closed = false;
    bool first_closed_before_second_opened = false;
    auto first_lifetime = std::make_shared<int32_t>(0);
    auto second_lifetime = std::make_shared<int32_t>(0);
    std::weak_ptr<int32_t> weak_first_lifetime = first_lifetime;
    std::weak_ptr<int32_t> weak_second_lifetime = second_lifetime;
    std::vector<LazyConcatBatchReader::ReaderFactory> factories;
    factories.push_back([&, first_lifetime]() -> Result<std::unique_ptr<BatchReader>> {
        ++opened;
        return std::make_unique<OneColumnBatchReader>(0, 2, &first_closed);
    });
    factories.push_back([&, second_lifetime]() -> Result<std::unique_ptr<BatchReader>> {
        ++opened;
        first_closed_before_second_opened = first_closed;
        return std::make_unique<OneColumnBatchReader>(1, 1, &second_closed);
    });
    LazyConcatBatchReader reader(SourcesOf(std::move(factories)), GetArrowPool(GetDefaultPool()));
    first_lifetime.reset();
    second_lifetime.reset();
    ASSERT_EQ(opened, 0);
    ASSERT_FALSE(weak_first_lifetime.expired());
    ASSERT_FALSE(weak_second_lifetime.expired());

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first, reader.NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(first));
    ASSERT_EQ(opened, 1);
    ASSERT_FALSE(first_closed);
    ASSERT_TRUE(weak_first_lifetime.expired());
    ASSERT_FALSE(weak_second_lifetime.expired());
    ASSERT_OK_AND_ASSIGN(uint64_t rows, reader.GetReaderMetrics()->GetCounter("test.rows"));
    ASSERT_EQ(rows, 1);
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch second, reader.NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(second));
    ReaderUtils::ReleaseReadBatch(std::move(second));
    ASSERT_EQ(opened, 1);
    ASSERT_FALSE(first_closed);
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch third, reader.NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(third));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array,
                         ReadResultCollector::GetArray(std::move(third)));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_NE(struct_array, nullptr);
    auto values = std::dynamic_pointer_cast<arrow::Int32Array>(struct_array->field(0));
    ASSERT_NE(values, nullptr);
    ASSERT_EQ(values->length(), 1);
    ASSERT_EQ(values->Value(0), 1);
    ASSERT_EQ(opened, 2);
    ASSERT_TRUE(first_closed_before_second_opened);
    ASSERT_FALSE(second_closed);
    ASSERT_TRUE(weak_second_lifetime.expired());
    ASSERT_OK_AND_ASSIGN(rows, reader.GetReaderMetrics()->GetCounter("test.rows"));
    ASSERT_EQ(rows, 3);
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap eof, reader.NextBatchWithBitmap());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));
    ASSERT_TRUE(second_closed);
    reader.Close();
    ASSERT_OK_AND_ASSIGN(rows, reader.GetReaderMetrics()->GetCounter("test.rows"));
    ASSERT_EQ(rows, 3);
    ASSERT_EQ(opened, 2);
    ASSERT_OK_AND_ASSIGN(array, ReadResultCollector::GetArray(std::move(first)));
    ASSERT_EQ(array->length(), 1);
}

TEST(LazyConcatBatchReaderTest, TestEarlyClosePreservesCompletedPreparationMetrics) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(1));
    auto arrow_pool = GetArrowPool(GetDefaultPool());
    std::atomic<int32_t> prepared = 0;
    std::promise<void> release;
    auto gate = release.get_future().share();
    ConcatBatchReader::ReaderFactory factory =
        [gate, &prepared](size_t index) -> Result<std::unique_ptr<BatchReader>> {
        if (index == 2) {
            gate.wait();
        }
        auto reader =
            std::make_unique<OneColumnBatchReader>(static_cast<int32_t>(index), 1, nullptr);
        reader->GetReaderMetrics()->SetCounter("test.prepared-readers", 1);
        ++prepared;
        return reader;
    };
    std::vector<LazyConcatBatchReader::ReaderFactory> factories;
    factories.push_back([factory, arrow_pool, executor]() -> Result<std::unique_ptr<BatchReader>> {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ConcatBatchReader> reader,
                               ConcatBatchReader::CreateLazy(3, factory, 2, arrow_pool, executor));
        return std::unique_ptr<BatchReader>(std::move(reader));
    });
    LazyConcatBatchReader reader(SourcesOf(std::move(factories)), arrow_pool);
    ScopeGuard unblock([&release]() { release.set_value(); });
    for (int32_t i = 0; i < 2; ++i) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, reader.NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        ReaderUtils::ReleaseReadBatch(std::move(batch.first));
    }
    release.set_value();
    unblock.Release();
    ASSERT_EQ(Via(executor.get(), []() {}).wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    ASSERT_EQ(prepared.load(), 3);
    reader.Close();
    reader.Close();
    ASSERT_OK_AND_ASSIGN(uint64_t prepared_readers,
                         reader.GetReaderMetrics()->GetCounter("test.prepared-readers"));
    ASSERT_EQ(prepared_readers, 3);
}

TEST(LazyConcatBatchReaderTest, TestClosingDoesNotOpenWhatWasNeverReached) {
    for (bool read_first : {false, true}) {
        SCOPED_TRACE(read_first);
        for (bool explicit_close : {false, true}) {
            SCOPED_TRACE(explicit_close);
            int32_t opened = 0;
            bool first_closed = false;
            auto lifetime = std::make_shared<int32_t>(0);
            std::weak_ptr<int32_t> weak_lifetime = lifetime;
            std::vector<LazyConcatBatchReader::ReaderFactory> factories;
            for (int32_t i = 0; i < 3; i++) {
                factories.push_back([&opened, &first_closed, i,
                                     lifetime]() -> Result<std::unique_ptr<BatchReader>> {
                    opened++;
                    return std::make_unique<OneColumnBatchReader>(i, 1,
                                                                  i == 0 ? &first_closed : nullptr);
                });
            }
            auto reader = std::make_unique<LazyConcatBatchReader>(SourcesOf(std::move(factories)),
                                                                  GetArrowPool(GetDefaultPool()));
            lifetime.reset();
            if (read_first) {
                ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first, reader->NextBatch());
                ASSERT_FALSE(BatchReader::IsEofBatch(first));
                ReaderUtils::ReleaseReadBatch(std::move(first));
            }
            ASSERT_FALSE(weak_lifetime.expired());
            if (explicit_close) {
                reader->Close();
                reader->Close();
                ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, reader->NextBatch());
                ASSERT_TRUE(BatchReader::IsEofBatch(eof));
            } else {
                reader.reset();
            }
            ASSERT_EQ(opened, read_first ? 1 : 0);
            ASSERT_EQ(first_closed, read_first);
            ASSERT_TRUE(weak_lifetime.expired());
        }
    }
}

TEST(LazyConcatBatchReaderTest, TestFailuresNameSourceAndReleaseResources) {
    enum class Failure { Open, Read, Bitmap };
    auto detail = std::make_shared<ReadErrorDetail>();
    for (Failure failure : {Failure::Open, Failure::Read, Failure::Bitmap}) {
        SCOPED_TRACE(static_cast<int32_t>(failure));
        for (int32_t failed_index : {0, 1}) {
            SCOPED_TRACE(failed_index);
            int32_t opened = 0;
            bool closed = false;
            auto lifetime = std::make_shared<int32_t>(0);
            std::weak_ptr<int32_t> weak_lifetime = lifetime;
            const Status expected =
                failure == Failure::Open
                    ? Status::NotImplemented("cannot open the file").WithDetail(detail)
                : failure == Failure::Read
                    ? Status::IOError("cannot read the batch").WithDetail(detail)
                    : Status::Invalid("at least one valid row");
            std::vector<LazyConcatBatchReader::ReaderFactory> factories;
            for (int32_t i = 0; i < 3; ++i) {
                factories.push_back([&, i, lifetime]() -> Result<std::unique_ptr<BatchReader>> {
                    ++opened;
                    if (i == failed_index) {
                        if (failure == Failure::Open) {
                            return expected;
                        }
                        if (failure == Failure::Read) {
                            return std::make_unique<FailingBatchReader>(expected, &closed);
                        }
                    }
                    return std::make_unique<OneColumnBatchReader>(
                        i, 1, i == failed_index ? &closed : nullptr,
                        i == failed_index && failure == Failure::Bitmap);
                });
            }
            LazyConcatBatchReader reader(SourcesOf(std::move(factories)),
                                         GetArrowPool(GetDefaultPool()));
            lifetime.reset();
            for (int32_t i = 0; i < failed_index; ++i) {
                ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader.NextBatch());
                ASSERT_FALSE(BatchReader::IsEofBatch(batch));
                ReaderUtils::ReleaseReadBatch(std::move(batch));
            }
            const auto failed = reader.NextBatch();
            ASSERT_NOK_WITH_MSG(failed, fmt::format("cannot read file-{}", failed_index));
            ASSERT_NOK_WITH_MSG(failed, expected.message());
            ASSERT_EQ(failed.status().code(), expected.code());
            ASSERT_EQ(failed.status().detail(), expected.detail());
            ASSERT_EQ(closed, failure != Failure::Open);
            ASSERT_TRUE(weak_lifetime.expired());
            ASSERT_NOK_WITH_MSG(reader.NextBatchWithBitmap(), failed.status().message());
            reader.Close();
            ASSERT_NOK_WITH_MSG(reader.NextBatch(), failed.status().message());
            ASSERT_EQ(opened, failed_index + 1);
            if (failed_index > 0) {
                ASSERT_OK_AND_ASSIGN(uint64_t rows,
                                     reader.GetReaderMetrics()->GetCounter("test.rows"));
                ASSERT_EQ(rows, failed_index + (failure == Failure::Bitmap ? 1 : 0));
            }
        }
    }
}

TEST(LazyConcatBatchReaderTest, TestEmptyAndSkippedSourcesPreserveSelectionAndReleaseFactories) {
    auto data = arrow::ipc::internal::json::ArrayFromJSON(
                    arrow::struct_({arrow::field("id", arrow::int32())}), "[[7], [8], [9], [10]]")
                    .ValueOrDie();
    for (int32_t source_count : {0, 1, 2, 3}) {
        SCOPED_TRACE(source_count);
        bool empty_closed = false;
        auto lifetime = std::make_shared<int32_t>(0);
        std::weak_ptr<int32_t> weak_lifetime = lifetime;
        std::vector<LazyConcatBatchReader::ReaderFactory> factories;
        for (int32_t i = 0; i < source_count; ++i) {
            factories.push_back(
                [i, data, lifetime, &empty_closed]() -> Result<std::unique_ptr<BatchReader>> {
                    if (i == 0) {
                        return std::unique_ptr<BatchReader>();
                    }
                    if (i == 1) {
                        return std::make_unique<OneColumnBatchReader>(7, 0, &empty_closed);
                    }
                    auto reader = std::make_unique<MockFileBatchReader>(
                        data, data->type(), RoaringBitmap32::From({1, 3}), 2);
                    reader->EnableRandomizeBatchSize(false);
                    return reader;
                });
        }
        LazyConcatBatchReader reader(SourcesOf(std::move(factories)),
                                     GetArrowPool(GetDefaultPool()));
        lifetime.reset();
        if (source_count == 3) {
            ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch,
                                 reader.NextBatchWithBitmap());
            ASSERT_FALSE(BatchReader::IsEofBatch(batch));
            ASSERT_EQ(batch.second, RoaringBitmap32::From({1}));
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> result,
                                 ReadResultCollector::GetArray(std::move(batch.first)));
            ASSERT_TRUE(result->Equals(data->Slice(0, 2)));
            ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch filtered, reader.NextBatch());
            ASSERT_FALSE(BatchReader::IsEofBatch(filtered));
            ASSERT_OK_AND_ASSIGN(result, ReadResultCollector::GetArray(std::move(filtered)));
            ASSERT_TRUE(result->Equals(data->Slice(3, 1)));
        }
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap eof, reader.NextBatchWithBitmap());
        ASSERT_TRUE(BatchReader::IsEofBatch(eof));
        ASSERT_EQ(empty_closed, source_count >= 2);
        ASSERT_TRUE(weak_lifetime.expired());
        ASSERT_OK_AND_ASSIGN(eof, reader.NextBatchWithBitmap());
        ASSERT_TRUE(BatchReader::IsEofBatch(eof));
    }
}

}  // namespace paimon::test
