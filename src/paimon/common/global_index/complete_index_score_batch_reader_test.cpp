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

#include "paimon/common/global_index/complete_index_score_batch_reader.h"

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class CompleteIndexScoreBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void TearDown() override {
        pool_.reset();
    }

    std::unique_ptr<BatchReader> PrepareCompleteIndexScoreBatchReader(
        const std::shared_ptr<arrow::Array>& src_array, const RoaringBitmap32& selected_bitmap,
        std::unordered_map<int64_t, float>&& scores, int32_t batch_size) const {
        auto file_batch_reader = std::make_unique<MockFileBatchReader>(src_array, src_array->type(),
                                                                       selected_bitmap, batch_size);
        return std::make_unique<CompleteIndexScoreBatchReader>(
            std::move(file_batch_reader), std::move(scores), /*remove_row_id=*/false,
            GetArrowPool(pool_));
    }

    std::unique_ptr<BatchReader> PrepareCompleteIndexScoreBatchReader(
        const std::shared_ptr<arrow::Array>& src_array, std::unordered_map<int64_t, float>&& scores,
        int32_t batch_size) const {
        auto file_batch_reader =
            std::make_unique<MockFileBatchReader>(src_array, src_array->type(), batch_size);
        return std::make_unique<CompleteIndexScoreBatchReader>(
            std::move(file_batch_reader), std::move(scores), /*remove_row_id=*/false,
            GetArrowPool(pool_));
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(CompleteIndexScoreBatchReaderTest, TestSimple) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_INDEX_SCORE", arrow::float32()),
        arrow::field("_ROW_ID", arrow::int64()),
    };

    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["Alice", 10, null, 0],
        ["Bob", 11, null, 1],
        ["Cathy", 12, null, 2]
    ])")
                         .ValueOrDie();

    std::unordered_map<int64_t, float> scores = {{0, 1.23f}, {1, 2.34f}, {2, 100.10f}};
    auto reader =
        PrepareCompleteIndexScoreBatchReader(src_array, std::move(scores), /*batch_size=*/1);

    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(std::move(reader)));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields), {R"([
        ["Alice", 10, 1.23, 0],
        ["Bob", 11, 2.34, 1],
        ["Cathy", 12, 100.10, 2]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(expected_array->ApproxEquals(*result_array));
}

TEST_F(CompleteIndexScoreBatchReaderTest, TestWithBitmap) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_INDEX_SCORE", arrow::float32()),
        arrow::field("_ROW_ID", arrow::int64()),
    };

    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["Alice", 10, null, 0],
        ["Bob", 11, null, 1],
        ["Cathy", 12, null, 2],
        ["David", 13, null, 4]
    ])")
                         .ValueOrDie();

    std::unordered_map<int64_t, float> scores = {
        {0, 1.23f}, {1, 2.34f}, {2, 100.10f}, {4, -19.12f}};
    auto selected_bitmap = RoaringBitmap32::From({0, 3});
    auto reader = PrepareCompleteIndexScoreBatchReader(src_array, selected_bitmap,
                                                       std::move(scores), /*batch_size=*/2);

    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(std::move(reader)));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields), {R"([
        ["Alice", 10, 1.23, 0],
        ["David", 13, -19.12, 4]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(expected_array->ApproxEquals(*result_array));
}

TEST_F(CompleteIndexScoreBatchReaderTest, TestEmptyScoresRejectReturnedRows) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_INDEX_SCORE", arrow::float32()),
        arrow::field("_ROW_ID", arrow::int64()),
    };

    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["Alice", 10, null, 0],
        ["Bob", 11, null, 1],
        ["Cathy", 12, null, 2]
    ])")
                         .ValueOrDie();

    // Unscored splits bypass this reader. A returned row without a score is invalid here.
    auto reader = PrepareCompleteIndexScoreBatchReader(src_array, /*scores=*/{}, /*batch_size=*/1);

    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "Missing global index score for row id 0");
}

TEST_F(CompleteIndexScoreBatchReaderTest, TestGlobalScoresFollowRowIds) {
    arrow::FieldVector fields = {
        arrow::field("_INDEX_SCORE", arrow::float32()),
        arrow::field("f0", arrow::int32()),
        arrow::field("_ROW_ID", arrow::int64()),
    };
    // Includes compacted-out candidates, a bitmap-filtered row without a score, and
    // deliberately non-monotonic output ids. Scores must depend only on the row id.
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                         arrow::struct_(fields),
                         R"([[null, 12, 102], [null, 99, null], [null, 10, 100], [null, 18, 108]])")
                         .ValueOrDie();
    for (bool remove_row_id : {false, true}) {
        auto expected_fields = fields;
        if (remove_row_id) {
            expected_fields.pop_back();
        }
        std::string expected_json = remove_row_id ? R"([[3, 12], [1, 10], [9, 18]])"
                                                  : R"([[3, 12, 102], [1, 10, 100], [9, 18, 108]])";
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                        arrow::struct_(expected_fields), {expected_json}, &expected_array)
                        .ok());
        for (int32_t batch_size : {1, 3, 10}) {
            SCOPED_TRACE(fmt::format("remove_row_id={}, batch_size={}", remove_row_id, batch_size));
            auto inner = std::make_unique<MockFileBatchReader>(
                src_array, src_array->type(), RoaringBitmap32::From({0, 2, 3}), batch_size);
            inner->EnableRandomizeBatchSize(false);
            auto reader = std::make_unique<CompleteIndexScoreBatchReader>(
                std::move(inner),
                std::unordered_map<int64_t, float>{
                    {100, 1.0f}, {101, 2.0f}, {102, 3.0f}, {108, 9.0f}, {109, 10.0f}},
                remove_row_id, GetArrowPool(pool_));
            ASSERT_OK_AND_ASSIGN(auto result_array,
                                 ReadResultCollector::CollectResult(std::move(reader)));
            ASSERT_TRUE(result_array);
            ASSERT_TRUE(expected_array->Equals(*result_array)) << result_array->ToString();
        }
    }
}

TEST_F(CompleteIndexScoreBatchReaderTest, TestGlobalScoresLeaveUnselectedRowsNull) {
    arrow::FieldVector fields = {
        arrow::field("_INDEX_SCORE", arrow::float32()),
        arrow::field("f0", arrow::int32()),
        arrow::field("_ROW_ID", arrow::int64()),
    };
    auto type = arrow::struct_(fields);
    auto src_array =
        arrow::ipc::internal::json::ArrayFromJSON(type, R"([[null, 99, null], [null, 12, 102]])")
            .ValueOrDie();
    auto selected_bitmap = RoaringBitmap32::From({1});
    for (bool remove_row_id : {false, true}) {
        auto inner = std::make_unique<MockFileBatchReader>(src_array, type, selected_bitmap,
                                                           /*read_batch_size=*/2);
        inner->EnableRandomizeBatchSize(false);
        auto reader = std::make_unique<CompleteIndexScoreBatchReader>(
            std::move(inner), std::unordered_map<int64_t, float>{{102, 3.0f}}, remove_row_id,
            GetArrowPool(pool_));
        ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatchWithBitmap());
        ASSERT_EQ(batch.second, selected_bitmap);
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::GetArray(std::move(batch.first)));
        auto expected_fields = fields;
        if (remove_row_id) {
            expected_fields.pop_back();
        }
        std::string expected_json =
            remove_row_id ? R"([[null, 99], [3, 12]])" : R"([[null, 99, null], [3, 12, 102]])";
        auto expected_array = arrow::ipc::internal::json::ArrayFromJSON(
                                  arrow::struct_(expected_fields), expected_json)
                                  .ValueOrDie();
        ASSERT_TRUE(expected_array->Equals(*result_array)) << result_array->ToString();
    }
}

TEST_F(CompleteIndexScoreBatchReaderTest, TestGlobalScoresRequireReturnedRowId) {
    auto check_error = [&](const arrow::FieldVector& fields, const std::string& json,
                           const std::string& message) {
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), json).ValueOrDie();
        auto inner = std::make_unique<MockFileBatchReader>(src_array, src_array->type(), 1);
        auto reader = std::make_unique<CompleteIndexScoreBatchReader>(
            std::move(inner), std::unordered_map<int64_t, float>{{100, 1.0f}},
            /*remove_row_id=*/false, GetArrowPool(pool_));
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), message);
    };
    auto score = arrow::field("_INDEX_SCORE", arrow::float32());
    auto row_id = arrow::field("_ROW_ID", arrow::int64());
    check_error({score, row_id}, "[[null, 101]]", "Missing global index score for row id 101");
    check_error({row_id}, "[[100]]", "Missing _INDEX_SCORE in CompleteIndexScoreBatchReader");
    check_error({score}, "[[null]]", "requires an int64 _ROW_ID field");
    check_error({score, arrow::field("_ROW_ID", arrow::int32())}, "[[null, 100]]",
                "requires an int64 _ROW_ID field");
}

}  // namespace paimon::test
