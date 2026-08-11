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

#include "paimon/common/reader/late_materialization_batch_reader.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

std::shared_ptr<arrow::StructArray> StructFromJson(const std::shared_ptr<arrow::Schema>& schema,
                                                   const std::string& json) {
    return std::static_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json)
            .ValueOrDie());
}

Result<std::shared_ptr<arrow::ChunkedArray>> Collect(BatchReader* reader) {
    return ReadResultCollector::CollectResult(reader);
}

void AssertZeroOffsets(const ArrowArray* array) {
    ASSERT_NE(nullptr, array);
    ASSERT_EQ(0, array->offset);
    for (int64_t i = 0; i < array->n_children; ++i) {
        AssertZeroOffsets(array->children[i]);
    }
}

}  // namespace

class LateMaterializationBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        read_schema_ =
            arrow::schema({arrow::field("k", arrow::int64()), arrow::field("v1", arrow::utf8()),
                           arrow::field("v2", arrow::boolean())});
        probe_schema_ = arrow::schema({arrow::field("k", arrow::int64())});
        payload_schema_ = arrow::schema(
            {arrow::field("v1", arrow::utf8()), arrow::field("v2", arrow::boolean())});
    }

 protected:
    std::shared_ptr<arrow::Schema> read_schema_;
    std::shared_ptr<arrow::Schema> probe_schema_;
    std::shared_ptr<arrow::Schema> payload_schema_;
};

TEST_F(LateMaterializationBatchReaderTest, TestMergeProbeAndPayloadColumns) {
    std::shared_ptr<arrow::StructArray> probe_data =
        StructFromJson(probe_schema_, R"([[1], [3], [5]])");
    std::shared_ptr<arrow::StructArray> payload_data =
        StructFromJson(payload_schema_, R"([["v1", true], ["v3", false], ["v5", true]])");
    std::unique_ptr<MockFileBatchReader> payload_reader = std::make_unique<MockFileBatchReader>(
        payload_data, arrow::struct_(payload_schema_->fields()), /*read_batch_size=*/2);
    payload_reader->EnableRandomizeBatchSize(false);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<LateMaterializationBatchReader> reader,
                         LateMaterializationBatchReader::Create(
                             read_schema_, probe_schema_, probe_data, payload_schema_,
                             std::move(payload_reader), /*read_batch_size=*/2, GetDefaultPool()));

    std::shared_ptr<arrow::ChunkedArray> expected = std::make_shared<arrow::ChunkedArray>(
        StructFromJson(read_schema_, R"([[1, "v1", true], [3, "v3", false], [5, "v5", true]])"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual, Collect(reader.get()));
    ASSERT_TRUE(actual->Equals(expected));
}

TEST_F(LateMaterializationBatchReaderTest, TestProbeOnlyRead) {
    std::shared_ptr<arrow::StructArray> probe_data =
        StructFromJson(probe_schema_, R"([[1], [3], [5]])");

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<LateMaterializationBatchReader> reader,
                         LateMaterializationBatchReader::Create(
                             probe_schema_, probe_schema_, probe_data, arrow::schema({}),
                             /*payload_reader=*/nullptr, /*read_batch_size=*/2, GetDefaultPool()));

    std::shared_ptr<arrow::ChunkedArray> expected =
        std::make_shared<arrow::ChunkedArray>(probe_data);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual, Collect(reader.get()));
    ASSERT_TRUE(actual->Equals(expected));
}

TEST_F(LateMaterializationBatchReaderTest, TestPayloadBitmapIsApplied) {
    std::shared_ptr<arrow::StructArray> probe_data = StructFromJson(probe_schema_, R"([[1], [5]])");
    std::shared_ptr<arrow::StructArray> payload_data =
        StructFromJson(payload_schema_,
                       R"([["skip", false], ["v1", true], ["skip", false],
                           ["skip", false], ["v5", true]])");
    RoaringBitmap32 bitmap;
    bitmap.Add(1);
    bitmap.Add(4);
    std::unique_ptr<MockFileBatchReader> payload_reader = std::make_unique<MockFileBatchReader>(
        payload_data, arrow::struct_(payload_schema_->fields()), bitmap, /*read_batch_size=*/5);
    payload_reader->EnableRandomizeBatchSize(false);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<LateMaterializationBatchReader> reader,
                         LateMaterializationBatchReader::Create(
                             read_schema_, probe_schema_, probe_data, payload_schema_,
                             std::move(payload_reader), /*read_batch_size=*/2, GetDefaultPool()));

    std::shared_ptr<arrow::ChunkedArray> expected = std::make_shared<arrow::ChunkedArray>(
        StructFromJson(read_schema_, R"([[1, "v1", true], [5, "v5", true]])"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual, Collect(reader.get()));
    ASSERT_TRUE(actual->Equals(expected));
}

TEST_F(LateMaterializationBatchReaderTest, TestImportedPayloadBatchRemainsValid) {
    std::shared_ptr<arrow::StructArray> probe_data = StructFromJson(probe_schema_, R"([[7]])");
    std::shared_ptr<arrow::StructArray> payload_data =
        StructFromJson(payload_schema_, R"([["v7", true]])");
    std::unique_ptr<MockFileBatchReader> payload_reader = std::make_unique<MockFileBatchReader>(
        payload_data, arrow::struct_(payload_schema_->fields()), /*read_batch_size=*/1);
    payload_reader->EnableRandomizeBatchSize(false);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<LateMaterializationBatchReader> reader,
                         LateMaterializationBatchReader::Create(
                             read_schema_, probe_schema_, probe_data, payload_schema_,
                             std::move(payload_reader), /*read_batch_size=*/1, GetDefaultPool()));

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                         reader->NextBatchWithBitmap());
    ASSERT_EQ(1, batch_with_bitmap.second.Cardinality());
    arrow::Result<std::shared_ptr<arrow::Array>> import_result = arrow::ImportArray(
        batch_with_bitmap.first.first.get(), batch_with_bitmap.first.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> array = import_result.ValueOrDie();
    std::shared_ptr<arrow::StructArray> struct_array =
        std::static_pointer_cast<arrow::StructArray>(array);
    ASSERT_EQ(1, struct_array->length());
    ASSERT_EQ(3, struct_array->num_fields());
    ASSERT_EQ(7, std::static_pointer_cast<arrow::Int64Array>(struct_array->field(0))->Value(0));
    ASSERT_EQ("v7",
              std::static_pointer_cast<arrow::StringArray>(struct_array->field(1))->GetString(0));
    ASSERT_TRUE(std::static_pointer_cast<arrow::BooleanArray>(struct_array->field(2))->Value(0));
}

TEST_F(LateMaterializationBatchReaderTest, TestSecondBatchOffsetsAreZero) {
    std::shared_ptr<arrow::StructArray> probe_data =
        StructFromJson(probe_schema_, R"([[1], [3], [5]])");
    std::shared_ptr<arrow::StructArray> payload_data =
        StructFromJson(payload_schema_, R"([["v1", true], ["v3", false], ["v5", true]])");
    std::unique_ptr<MockFileBatchReader> payload_reader = std::make_unique<MockFileBatchReader>(
        payload_data, arrow::struct_(payload_schema_->fields()), /*read_batch_size=*/2);
    payload_reader->EnableRandomizeBatchSize(false);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<LateMaterializationBatchReader> reader,
                         LateMaterializationBatchReader::Create(
                             read_schema_, probe_schema_, probe_data, payload_schema_,
                             std::move(payload_reader), /*read_batch_size=*/2, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap first_batch,
                         reader->NextBatchWithBitmap());
    ReaderUtils::ReleaseReadBatch(std::move(first_batch.first));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap second_batch,
                         reader->NextBatchWithBitmap());
    AssertZeroOffsets(second_batch.first.first.get());
    ReaderUtils::ReleaseReadBatch(std::move(second_batch.first));
}

TEST_F(LateMaterializationBatchReaderTest, TestPayloadLongerThanProbeFails) {
    std::shared_ptr<arrow::StructArray> probe_data = StructFromJson(probe_schema_, R"([[1]])");
    std::shared_ptr<arrow::StructArray> payload_data =
        StructFromJson(payload_schema_, R"([["v1", true], ["v2", false]])");
    std::unique_ptr<MockFileBatchReader> payload_reader = std::make_unique<MockFileBatchReader>(
        payload_data, arrow::struct_(payload_schema_->fields()), /*read_batch_size=*/2);
    payload_reader->EnableRandomizeBatchSize(false);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<LateMaterializationBatchReader> reader,
                         LateMaterializationBatchReader::Create(
                             read_schema_, probe_schema_, probe_data, payload_schema_,
                             std::move(payload_reader), /*read_batch_size=*/2, GetDefaultPool()));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "payload row count exceeds probe row count");
}

TEST_F(LateMaterializationBatchReaderTest, TestPayloadShorterThanProbeFails) {
    std::shared_ptr<arrow::StructArray> probe_data = StructFromJson(probe_schema_, R"([[1], [2]])");
    std::shared_ptr<arrow::StructArray> payload_data =
        StructFromJson(payload_schema_, R"([["v1", true]])");
    std::unique_ptr<MockFileBatchReader> payload_reader = std::make_unique<MockFileBatchReader>(
        payload_data, arrow::struct_(payload_schema_->fields()), /*read_batch_size=*/1);
    payload_reader->EnableRandomizeBatchSize(false);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<LateMaterializationBatchReader> reader,
                         LateMaterializationBatchReader::Create(
                             read_schema_, probe_schema_, probe_data, payload_schema_,
                             std::move(payload_reader), /*read_batch_size=*/2, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
    ReaderUtils::ReleaseReadBatch(std::move(batch));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "payload ended at 1, but probe row count is 2");
}

TEST_F(LateMaterializationBatchReaderTest, TestMissingPayloadReaderFailsAtCreation) {
    std::shared_ptr<arrow::StructArray> probe_data = StructFromJson(probe_schema_, R"([[1]])");

    ASSERT_NOK_WITH_MSG(LateMaterializationBatchReader::Create(
                            read_schema_, probe_schema_, probe_data, payload_schema_,
                            /*payload_reader=*/nullptr, /*read_batch_size=*/1, GetDefaultPool()),
                        "requires a payload reader for payload fields");
}

TEST_F(LateMaterializationBatchReaderTest, TestInvalidArgumentsFailAtCreation) {
    std::shared_ptr<arrow::StructArray> probe_data = StructFromJson(probe_schema_, R"([[1]])");

    ASSERT_NOK_WITH_MSG(LateMaterializationBatchReader::Create(
                            /*read_schema=*/nullptr, probe_schema_, probe_data, arrow::schema({}),
                            /*payload_reader=*/nullptr, /*read_batch_size=*/1, GetDefaultPool()),
                        "requires non-null schemas and data");
    ASSERT_NOK_WITH_MSG(LateMaterializationBatchReader::Create(
                            probe_schema_, probe_schema_, probe_data, arrow::schema({}),
                            /*payload_reader=*/nullptr, /*read_batch_size=*/0, GetDefaultPool()),
                        "read batch size should be positive");
    ASSERT_NOK_WITH_MSG(LateMaterializationBatchReader::Create(
                            read_schema_, probe_schema_, probe_data, arrow::schema({}),
                            /*payload_reader=*/nullptr, /*read_batch_size=*/1, GetDefaultPool()),
                        "field v1 is missing from both probe and payload schemas");
}

}  // namespace paimon::test
