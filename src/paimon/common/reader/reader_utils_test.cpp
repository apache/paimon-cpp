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

#include "paimon/common/reader/reader_utils.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {
namespace {

class ErrorAfterOneBatchReader : public BatchReader {
 public:
    explicit ErrorAfterOneBatchReader(ReadBatch batch) : batch_(std::move(batch)) {}

    Result<ReadBatch> NextBatch() override {
        if (batch_.first) {
            return std::move(batch_);
        }
        return Status::IOError("expected test error");
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return nullptr;
    }

    void Close() override {}

 private:
    ReadBatch batch_;
};

}  // namespace

TEST(ReaderUtilsTest, TestAddAllValidBitmap) {
    auto check_result = [](const std::string& src_str) {
        if (src_str.empty()) {
            auto batch_with_bitmap = ReaderUtils::AddAllValidBitmap(BatchReader::MakeEofBatch());
            ASSERT_TRUE(BatchReader::IsEofBatch(batch_with_bitmap));
            auto& [_, bitmap] = batch_with_bitmap;
            ASSERT_TRUE(bitmap.IsEmpty());
            return;
        }
        auto array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), src_str).ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto batch, ReadResultCollector::GetReadBatch(array));
        auto batch_with_bitmap = ReaderUtils::AddAllValidBitmap(std::move(batch));
        auto& [c_batch, bitmap] = batch_with_bitmap;
        ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::GetArray(std::move(c_batch)));
        ASSERT_EQ(bitmap.Cardinality(), result_array->length());
        ASSERT_TRUE(result_array->Equals(array));
    };

    check_result("[0, 1, 2, 3, 4]");
    check_result("[10, 20, 30]");
    check_result("");
}

TEST(ReaderUtilsTest, TestCollectResultReleasesBufferedBatchesOnError) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1]").ValueOrDie();
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, ReadResultCollector::GetReadBatch(array));

    std::shared_ptr<int32_t> lifetime = std::make_shared<int32_t>(1);
    std::weak_ptr<int32_t> weak_lifetime = lifetime;
    ASSERT_OK(AddArrowArrayLifetime(batch.first.get(), batch.second.get(), lifetime));
    lifetime.reset();

    auto reader = std::make_unique<ErrorAfterOneBatchReader>(std::move(batch));
    ASSERT_NOK_WITH_MSG(ReadResultCollector::CollectResult(std::move(reader)),
                        "expected test error");
    ASSERT_TRUE(weak_lifetime.expired());
}

TEST(ReaderUtilsTest, TestApplyBitmapToReadBatch) {
    auto check_result = [](const std::string& src_str, const std::vector<int32_t>& bitmap_vec,
                           const std::string& target_str, const std::string& erro_msg = "") {
        std::shared_ptr<arrow::MemoryPool> arrow_pool(arrow::default_memory_pool(),
                                                      [](arrow::MemoryPool*) {});
        auto bitmap = RoaringBitmap32::From(bitmap_vec);
        if (src_str.empty()) {
            auto batch_with_bitmap = std::make_pair(BatchReader::MakeEofBatch(), std::move(bitmap));
            ASSERT_OK_AND_ASSIGN(auto result_batch, ReaderUtils::ApplyBitmapToReadBatch(
                                                        std::move(batch_with_bitmap), arrow_pool));
            ASSERT_TRUE(BatchReader::IsEofBatch(result_batch));
            return;
        }
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), src_str).ValueOrDie();
        auto target_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), target_str).ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto src_batch, ReadResultCollector::GetReadBatch(src_array));
        auto batch_with_bitmap = std::make_pair(std::move(src_batch), std::move(bitmap));
        if (!erro_msg.empty()) {
            ASSERT_NOK_WITH_MSG(
                ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool),
                erro_msg);
            return;
        }
        ASSERT_OK_AND_ASSIGN(auto result_batch, ReaderUtils::ApplyBitmapToReadBatch(
                                                    std::move(batch_with_bitmap), arrow_pool));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::GetArray(std::move(result_batch)));
        ASSERT_TRUE(result_array->Equals(target_array));
    };
    check_result("[10, 11, 12, 13, 14]", {1}, "[11]");
    check_result("[10, 11, 12, 13, 14]", {0, 1}, "[10, 11]");
    check_result("[10, 11, 12, 13, 14]", {2, 4}, "[12, 14]");
    check_result("[10, 11, 12, 13, 14]", {2, 3}, "[12, 13]");
    check_result("[10, 11, 12, 13, 14]", {0, 1, 3, 4}, "[10, 11, 13, 14]");
    check_result("[10, 11, 12, 13, 14]", {0, 1, 2, 3, 4}, "[10, 11, 12, 13, 14]");
    // eof batch
    check_result("", {}, "");
    // bitmap is empty, invalid
    check_result("[10, 11, 12, 13, 14]", {}, "[]",
                 "NextBatchWithBitmap should always return the result with at least one valid row "
                 "except eof");
}

}  // namespace paimon::test
