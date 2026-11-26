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

#include "paimon/common/reader/projection_batch_reader.h"

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(ProjectionBatchReaderTest, PreservesSelectionEncodingAndMetadata) {
    auto json = [](const std::shared_ptr<arrow::DataType>& type, const std::string& value) {
        return arrow::ipc::internal::json::ArrayFromJSON(type, value).ValueOrDie();
    };
    auto dictionary_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    auto names =
        arrow::DictionaryArray::FromArrays(dictionary_type, json(arrow::int32(), "[0, 1, 0]"),
                                           json(arrow::utf8(), R"(["Alice", "Bob"])"))
            .ValueOrDie();
    auto metadata = arrow::key_value_metadata({"field-id"}, {"42"});
    auto name_field = arrow::field("name", dictionary_type, true, metadata);
    auto data = arrow::StructArray::Make(
                    {json(arrow::int8(), "[0, 0, 0]"), json(arrow::int32(), "[1, 2, 3]"),
                     json(arrow::int32(), "[10, 20, 30]"), names},
                    {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("id", arrow::int32()),
                     arrow::field("dependency", arrow::int32()), name_field})
                    .ValueOrDie();
    for (bool with_bitmap : {false, true}) {
        auto mock = std::make_unique<MockFileBatchReader>(data, data->type(),
                                                          RoaringBitmap32::From({0, 2}), 3);
        mock->EnableRandomizeBatchSize(false);
        auto reader = std::make_unique<ProjectionBatchReader>(
            std::move(mock), std::vector<std::string>{"name", "id"},
            GetArrowPool(GetDefaultPool()));
        BatchReader::ReadBatch batch;
        if (with_bitmap) {
            ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap selected,
                                 reader->NextBatchWithBitmap());
            ASSERT_EQ(selected.second, RoaringBitmap32::From({0, 2}));
            batch = std::move(selected.first);
        } else {
            ASSERT_OK_AND_ASSIGN(batch, reader->NextBatch());
        }
        auto imported = arrow::ImportArray(batch.first.get(), batch.second.get());
        ASSERT_TRUE(imported.ok()) << imported.status().ToString();
        std::shared_ptr<arrow::Array> result = std::move(imported).ValueOrDie();
        auto rows = checked_pointer_cast<arrow::StructArray>(result);
        ASSERT_TRUE(rows->ValidateFull().ok());
        ASSERT_EQ(rows->length(), with_bitmap ? 3 : 2);
        ASSERT_EQ(rows->struct_type()->num_fields(), 3);
        ASSERT_EQ(rows->struct_type()->field(0)->name(), "_VALUE_KIND");
        ASSERT_TRUE(rows->struct_type()->field(1)->Equals(name_field, true));
        ASSERT_EQ(rows->struct_type()->field(2)->name(), "id");
        ASSERT_TRUE(
            rows->field(2)->Equals(json(arrow::int32(), with_bitmap ? "[1, 2, 3]" : "[1, 3]")));
        reader->Close();
    }
}

TEST(ProjectionBatchReaderTest, RejectsMissingOutputAndPreservesEmptyProjectionRows) {
    auto data = arrow::ipc::internal::json::ArrayFromJSON(
                    arrow::struct_({arrow::field("id", arrow::int32())}), "[[1], [2]]")
                    .ValueOrDie();
    ProjectionBatchReader invalid(std::make_unique<MockFileBatchReader>(data, data->type(), 2),
                                  {"missing"}, GetArrowPool(GetDefaultPool()));
    ASSERT_NOK_WITH_MSG(invalid.NextBatch(), "Output field is missing");
    invalid.Close();
    auto mock = std::make_unique<MockFileBatchReader>(data, data->type(), 2);
    mock->EnableRandomizeBatchSize(false);
    ProjectionBatchReader empty(std::move(mock), {}, GetArrowPool(GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, empty.NextBatch());
    ASSERT_EQ(batch.first->length, 2);
    ASSERT_EQ(batch.first->n_children, 0);
    ReaderUtils::ReleaseReadBatch(std::move(batch));
    ASSERT_OK_AND_ASSIGN(batch, empty.NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
    empty.Close();
}

TEST(ProjectionBatchReaderTest, PreservesParentNullsAndRowCountWithEmptyProjection) {
    auto data = arrow::ipc::internal::json::ArrayFromJSON(
                    arrow::struct_({arrow::field("id", arrow::int32()),
                                    arrow::field("dependency", arrow::int32())}),
                    "[[10, 1], null, [30, 3], [40, 4]]")
                    .ValueOrDie();
    for (bool empty_projection : {false, true}) {
        for (bool with_bitmap : {false, true}) {
            auto source = std::make_unique<MockFileBatchReader>(
                data, data->type(), RoaringBitmap32::From({0, 1, 3}), 4);
            source->EnableRandomizeBatchSize(false);
            std::vector<std::string> output_fields;
            if (!empty_projection) {
                output_fields.push_back("id");
            }
            ProjectionBatchReader reader(std::move(source), std::move(output_fields),
                                         GetArrowPool(GetDefaultPool()));
            BatchReader::ReadBatch batch;
            if (with_bitmap) {
                ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap selected,
                                     reader.NextBatchWithBitmap());
                ASSERT_EQ(selected.second, RoaringBitmap32::From({0, 1, 3}));
                batch = std::move(selected.first);
            } else {
                ASSERT_OK_AND_ASSIGN(batch, reader.NextBatch());
            }
            auto imported = arrow::ImportArray(batch.first.get(), batch.second.get());
            ASSERT_TRUE(imported.ok()) << imported.status().ToString();
            auto rows = checked_pointer_cast<arrow::StructArray>(std::move(imported).ValueOrDie());
            ASSERT_TRUE(rows->ValidateFull().ok());
            ASSERT_EQ(rows->length(), with_bitmap ? 4 : 3);
            ASSERT_EQ(rows->null_count(), 1);
            ASSERT_TRUE(rows->IsNull(1));
            ASSERT_EQ(rows->num_fields(), empty_projection ? 0 : 1);
            if (!empty_projection) {
                auto ids = checked_pointer_cast<arrow::Int32Array>(rows->field(0));
                ASSERT_EQ(ids->Value(0), 10);
                ASSERT_EQ(ids->Value(2), with_bitmap ? 30 : 40);
                ASSERT_EQ(ids->Value(ids->length() - 1), 40);
            }
            reader.Close();
        }
    }
}
}  // namespace paimon::test
