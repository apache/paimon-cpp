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
#include <map>
#include <memory>
#include <string>
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

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestMultipleScoresFollowSelectedRowsAndProjection) {
    arrow::FieldVector fields = {arrow::field("score_z", arrow::float32()),
                                 arrow::field("f0", arrow::int32()),
                                 arrow::field("score_a", arrow::float32())};
    auto type = arrow::struct_(fields);
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, R"([
        [null, 10, null], [null, 11, null], [null, 12, null],
        [null, 13, null], [null, 14, null], [null, 15, null],
        [null, 16, null], [null, 17, null], [null, 18, null],
        [null, 19, null], [null, 20, null], [null, 21, null]
    ])")
                    .ValueOrDie();
    auto selected = RoaringBitmap32::From({0, 2, 8, 9, 10, 11});
    auto inner = std::make_unique<MockFileBatchReader>(data, type, selected,
                                                       /*read_batch_size=*/3);
    inner->EnableRandomizeBatchSize(false);
    std::map<std::string, std::vector<float>> scores = {
        {"score_a", {1.25f, 2.5f, 3.75f, 5.0f, 6.25f, 7.5f}},
        {"score_z", {12.5f, 25.0f, 37.5f, 50.0f, 62.5f, 75.0f}}};
    CompleteIndexScoreFileBatchReader reader(std::move(inner), scores, GetArrowPool(pool_));

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap first, reader.NextBatchWithBitmap());
    ASSERT_EQ(RoaringBitmap32::From({0, 2}), first.second);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> first_array,
                         ReadResultCollector::GetArray(std::move(first.first)));
    auto expected_first = arrow::ipc::internal::json::ArrayFromJSON(
                              type, R"([[12.5, 10, 1.25], [null, 11, null], [25, 12, 2.5]])")
                              .ValueOrDie();
    ASSERT_TRUE(expected_first->Equals(*first_array)) << first_array->ToString();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                         ReadResultCollector::CollectResult(&reader));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    type, {R"([[37.5, 18, 3.75], [50, 19, 5], [62.5, 20, 6.25], [75, 21, 7.5]])"},
                    &expected_array)
                    .ok());
    ASSERT_TRUE(expected_array->Equals(*result_array)) << result_array->ToString();

    arrow::FieldVector reordered_fields = {fields[2], fields[0], fields[1]};
    ::ArrowSchema read_schema;
    ASSERT_TRUE(arrow::ExportSchema(*arrow::schema(reordered_fields), &read_schema).ok());
    ASSERT_OK(reader.SetReadSchema(&read_schema, /*predicate=*/nullptr,
                                   /*selection_bitmap=*/selected));
    ASSERT_OK_AND_ASSIGN(result_array, ReadResultCollector::CollectResult(&reader));
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_(reordered_fields),
                    {R"([[1.25, 12.5, 10], [2.5, 25, 12], [3.75, 37.5, 18],
                         [5, 50, 19], [6.25, 62.5, 20], [7.5, 75, 21]])"},
                    &expected_array)
                    .ok());
    ASSERT_TRUE(expected_array->Equals(*result_array)) << result_array->ToString();
}

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestMultipleScoresWithEmptyResult) {
    auto type = arrow::struct_(
        {arrow::field("score_a", arrow::float32()), arrow::field("score_b", arrow::float32())});
    for (const std::string& json : {"[]", "[[null, null]]"}) {
        auto data = arrow::ipc::internal::json::ArrayFromJSON(type, json).ValueOrDie();
        auto inner = std::make_unique<MockFileBatchReader>(data, type, RoaringBitmap32(),
                                                           /*read_batch_size=*/1);
        CompleteIndexScoreFileBatchReader reader(
            std::move(inner),
            std::map<std::string, std::vector<float>>{{"score_a", {}}, {"score_b", {}}},
            GetArrowPool(pool_));
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap eof, reader.NextBatchWithBitmap());
        ASSERT_TRUE(BatchReader::IsEofBatch(eof));
    }
}

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestRejectsTooFewScoresForEachField) {
    auto type = arrow::struct_(
        {arrow::field("score_a", arrow::float32()), arrow::field("score_b", arrow::float32())});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, "[[null, null], [null, null]]")
                    .ValueOrDie();
    for (const std::string& field_name : {"score_a", "score_b"}) {
        auto inner = std::make_unique<MockFileBatchReader>(data, type, /*read_batch_size=*/1);
        std::map<std::string, std::vector<float>> scores = {{"score_a", {1.0f, 2.0f}},
                                                            {"score_b", {3.0f, 4.0f}}};
        scores[field_name].resize(1);
        CompleteIndexScoreFileBatchReader reader(std::move(inner), scores, GetArrowPool(pool_));
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first, reader.NextBatch());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> first_array,
                             ReadResultCollector::GetArray(std::move(first)));
        ASSERT_TRUE(first_array);
        ASSERT_EQ(1, first_array->length());
        ASSERT_NOK_WITH_MSG(reader.NextBatch(), "Index score count 1 for field '" + field_name +
                                                    "' is smaller than rows read 2");
    }
}

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestRejectsUnusedScoresForEachField) {
    auto type = arrow::struct_(
        {arrow::field("score_a", arrow::float32()), arrow::field("score_b", arrow::float32())});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, "[[null, null]]").ValueOrDie();
    for (const std::string& field_name : {"score_a", "score_b"}) {
        auto inner = std::make_unique<MockFileBatchReader>(data, type, /*read_batch_size=*/1);
        std::map<std::string, std::vector<float>> scores = {{"score_a", {1.0f}},
                                                            {"score_b", {3.0f}}};
        scores[field_name].push_back(2.0f);
        CompleteIndexScoreFileBatchReader reader(std::move(inner), scores, GetArrowPool(pool_));
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first, reader.NextBatch());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> first_array,
                             ReadResultCollector::GetArray(std::move(first)));
        ASSERT_TRUE(first_array);
        ASSERT_EQ(1, first_array->length());
        ASSERT_NOK_WITH_MSG(reader.NextBatch(), "Index score count 2 for field '" + field_name +
                                                    "' does not match rows read 1");
    }
}

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestRejectsEmptyScoresWithNonemptyResult) {
    auto type = arrow::struct_({arrow::field("score", arrow::float32())});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, "[[null]]").ValueOrDie();
    auto inner = std::make_unique<MockFileBatchReader>(data, type, /*read_batch_size=*/1);
    CompleteIndexScoreFileBatchReader reader(
        std::move(inner), std::map<std::string, std::vector<float>>{{"score", {}}},
        GetArrowPool(pool_));
    ASSERT_NOK_WITH_MSG(reader.NextBatch(),
                        "Index score count 0 for field 'score' is smaller than rows read 1");
}

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestRejectsInvalidScoreFields) {
    auto check_error = [&](const arrow::FieldVector& fields, const std::string& json,
                           const std::string& message) {
        auto type = arrow::struct_(fields);
        auto data = arrow::ipc::internal::json::ArrayFromJSON(type, json).ValueOrDie();
        auto inner = std::make_unique<MockFileBatchReader>(data, type, /*read_batch_size=*/1);
        CompleteIndexScoreFileBatchReader reader(
            std::move(inner), std::map<std::string, std::vector<float>>{{"score", {1.25f}}},
            GetArrowPool(pool_));
        ASSERT_NOK_WITH_MSG(reader.NextBatch(), message);
    };
    auto score = arrow::field("score", arrow::float32());
    check_error({arrow::field("f0", arrow::int32())}, "[[10]]",
                "Missing index score field 'score'");
    check_error({score, score}, "[[null, null]]", "Duplicate index score field 'score'");
    check_error({arrow::field("score", arrow::float64())}, "[[null]]",
                "Index score field 'score' must have float32 type");
}

TEST_F(CompleteIndexScoreFileBatchReaderTest, TestLegacyEmptyScoresPassThrough) {
    auto type = arrow::struct_({arrow::field("f0", arrow::int32())});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, "[[10], [11]]").ValueOrDie();
    auto inner = std::make_unique<MockFileBatchReader>(data, type, /*read_batch_size=*/2);
    inner->EnableRandomizeBatchSize(false);
    CompleteIndexScoreFileBatchReader reader(std::move(inner), std::vector<float>{},
                                             GetArrowPool(pool_));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader.NextBatch());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> result,
                         ReadResultCollector::GetArray(std::move(batch)));
    ASSERT_TRUE(data->Equals(*result)) << result->ToString();
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, reader.NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));
}

}  // namespace paimon::test
