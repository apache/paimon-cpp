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

#include "paimon/common/reader/complete_index_score_file_batch_reader.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {

class CompleteIndexScoreFileBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void TearDown() override {
        pool_.reset();
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestFileReaderForwardsOperationsAndResetsScores) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("_INDEX_SCORE", arrow::float32())};
    auto data = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["Alice", null],
        ["Bob", null]
    ])")
                    .ValueOrDie();
    auto inner_reader = std::make_unique<MockFileBatchReader>(data, data->type(), /*batch_size=*/1);
    MockFileBatchReader* inner = inner_reader.get();
    auto reader = std::make_unique<CompleteIndexScoreFileBatchReader>(
        std::move(inner_reader), std::vector<float>{1.25f, 2.5f}, GetArrowPool(GetDefaultPool()));

    ASSERT_OK_AND_ASSIGN(uint64_t row_count, reader->GetNumberOfRows());
    EXPECT_EQ(2, row_count);
    EXPECT_FALSE(reader->SupportPreciseBitmapSelection());
    reader->Warmup();
    EXPECT_EQ(1, inner->GetWarmupCount());

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, reader->NextBatchWithBitmap());
    ASSERT_OK_AND_ASSIGN(uint64_t file_row_id, reader->GetPreviousBatchFileRowId(0));
    EXPECT_EQ(0, file_row_id);
    auto& [first_array_data, first_schema] = batch.first;
    auto first_array = arrow::ImportArray(first_array_data.get(), first_schema.get()).ValueOrDie();
    auto first_struct = std::dynamic_pointer_cast<arrow::StructArray>(first_array);
    ASSERT_TRUE(first_struct);
    auto first_scores =
        std::dynamic_pointer_cast<arrow::FloatArray>(first_struct->GetFieldByName("_INDEX_SCORE"));
    ASSERT_TRUE(first_scores);
    EXPECT_FLOAT_EQ(1.25f, first_scores->Value(0));

    ::ArrowSchema read_schema;
    ASSERT_TRUE(arrow::ExportSchema(*arrow::schema(fields), &read_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&read_schema, /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap restarted, reader->NextBatchWithBitmap());
    auto& [restarted_array_data, restarted_schema] = restarted.first;
    auto restarted_array =
        arrow::ImportArray(restarted_array_data.get(), restarted_schema.get()).ValueOrDie();
    auto restarted_struct = std::dynamic_pointer_cast<arrow::StructArray>(restarted_array);
    ASSERT_TRUE(restarted_struct);
    auto restarted_scores = std::dynamic_pointer_cast<arrow::FloatArray>(
        restarted_struct->GetFieldByName("_INDEX_SCORE"));
    ASSERT_TRUE(restarted_scores);
    EXPECT_FLOAT_EQ(1.25f, restarted_scores->Value(0));
}

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestFileScoresFollowSelectedRows) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32()),
                                 arrow::field("_INDEX_SCORE", arrow::float32())};
    auto data = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                          R"([[10, null], [11, null], [12, null]])")
                    .ValueOrDie();
    auto selected = RoaringBitmap32::From({0, 2});
    auto inner = std::make_unique<MockFileBatchReader>(data, data->type(), selected,
                                                       /*read_batch_size=*/3);
    inner->EnableRandomizeBatchSize(false);
    CompleteIndexScoreFileBatchReader reader(std::move(inner), std::vector<float>{1.25f, 2.5f},
                                             GetArrowPool(pool_));

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, reader.NextBatchWithBitmap());
    EXPECT_EQ(selected, batch.second);
    auto array = arrow::ImportArray(batch.first.first.get(), batch.first.second.get()).ValueOrDie();
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array);
    auto scores =
        std::dynamic_pointer_cast<arrow::FloatArray>(struct_array->GetFieldByName("_INDEX_SCORE"));
    ASSERT_TRUE(scores);
    EXPECT_FLOAT_EQ(1.25f, scores->Value(0));
    EXPECT_TRUE(scores->IsNull(1));
    EXPECT_FLOAT_EQ(2.5f, scores->Value(2));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap eof, reader.NextBatchWithBitmap());
    EXPECT_TRUE(BatchReader::IsEofBatch(eof));
}

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestScoresFollowSparseSelectionsAcrossBatches) {
    auto type = arrow::struct_(
        {arrow::field("f0", arrow::int32()), arrow::field("_INDEX_SCORE", arrow::float32())});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, R"([
        [10, null], [11, null], [12, null],
        [13, null], [14, null], [15, null],
        [16, null], [17, null], [18, null],
        [19, null], [20, null], [21, null]
    ])")
                    .ValueOrDie();
    // The second physical batch has no matches and is skipped by the underlying reader.
    auto selected = RoaringBitmap32::From({0, 2, 8, 9, 10, 11});
    auto inner = std::make_unique<MockFileBatchReader>(data, type, selected,
                                                       /*read_batch_size=*/3);
    inner->EnableRandomizeBatchSize(false);
    CompleteIndexScoreFileBatchReader reader(
        std::move(inner), std::vector<float>{1.25f, 2.5f, 3.75f, 5.0f, 6.25f, 7.5f},
        GetArrowPool(pool_));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(&reader));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(type, {R"([
        [10, 1.25], [12, 2.5], [18, 3.75],
        [19, 5.0], [20, 6.25], [21, 7.5]
    ])"},
                                                                 &expected_array)
                    .ok());
    ASSERT_TRUE(expected_array->Equals(*result_array)) << result_array->ToString();
}

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestSetReadSchemaReordersScoreColumn) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::int32()),
                                 arrow::field("_INDEX_SCORE", arrow::float32())};
    auto type = arrow::struct_(fields);
    auto data =
        arrow::ipc::internal::json::ArrayFromJSON(type, R"([[10, null], [11, null]])").ValueOrDie();
    auto inner = std::make_unique<MockFileBatchReader>(data, type, /*read_batch_size=*/1);
    inner->EnableRandomizeBatchSize(false);
    CompleteIndexScoreFileBatchReader reader(std::move(inner), std::vector<float>{1.25f, 2.5f},
                                             GetArrowPool(pool_));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(&reader));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    type, {R"([[10, 1.25], [11, 2.5]])"}, &expected_array)
                    .ok());
    ASSERT_TRUE(expected_array->Equals(*result_array)) << result_array->ToString();

    arrow::FieldVector reordered_fields = {fields[1], fields[0]};
    ::ArrowSchema read_schema;
    ASSERT_TRUE(arrow::ExportSchema(*arrow::schema(reordered_fields), &read_schema).ok());
    ASSERT_OK(reader.SetReadSchema(&read_schema, /*predicate=*/nullptr,
                                   /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(result_array, ReadResultCollector::CollectResult(&reader));
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(reordered_fields),
                                                                 {R"([[1.25, 10], [2.5, 11]])"},
                                                                 &expected_array)
                    .ok());
    ASSERT_TRUE(expected_array->Equals(*result_array)) << result_array->ToString();
}

}  // namespace paimon::test
