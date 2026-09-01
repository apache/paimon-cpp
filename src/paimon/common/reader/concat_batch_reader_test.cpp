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
#include <cstdint>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
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
            std::vector<std::unique_ptr<BatchReader>> readers;
            for (const auto& [batch_str, bitmap_data] : batches) {
                auto f1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), batch_str)
                              .ValueOrDie();
                std::shared_ptr<arrow::Array> data =
                    arrow::StructArray::Make({f1}, {arrow::field("f1", arrow::int32())})
                        .ValueOrDie();
                auto reader = std::make_unique<MockFileBatchReader>(
                    data, data->type(), RoaringBitmap32::From(bitmap_data), batch_size);
                readers.push_back(std::move(reader));
            }
            auto concat_reader =
                std::make_unique<ConcatBatchReader>(std::move(readers), GetArrowPool(pool_));
            ASSERT_OK_AND_ASSIGN(auto result_chunk_array,
                                 ReadResultCollector::CollectResult(std::move(concat_reader)));
            if (expected.empty()) {
                ASSERT_FALSE(result_chunk_array);
                return;
            }
            auto expected_f1 =
                arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), expected).ValueOrDie();
            std::shared_ptr<arrow::Array> expected_array =
                arrow::StructArray::Make({expected_f1}, {arrow::field("f1", arrow::int32())})
                    .ValueOrDie();
            auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
            ASSERT_TRUE(expected_chunk_array->Equals(result_chunk_array))
                << result_chunk_array->ToString();
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

}  // namespace paimon::test
