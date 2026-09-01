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

#include "paimon/core/realtime/realtime_store_read_pipeline.h"

#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/data/shredding/map_shared_shredding_schema_utils.h"
#include "paimon/data/variant.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/variant_test_data.h"

namespace paimon::test {
namespace {

std::shared_ptr<arrow::Field> MapReadField(const std::shared_ptr<arrow::DataType>& map_type,
                                           const std::string& selected_keys) {
    return arrow::field(
        "tags", map_type, /*nullable=*/true,
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {selected_keys}));
}

Result<std::shared_ptr<arrow::Field>> MapAccessField(
    const std::shared_ptr<arrow::Field>& map_field, const std::vector<std::string>& selected_keys) {
    auto c_map_field = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportField(*map_field, c_map_field.get()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MapSharedShreddingAccessBuilder> access_builder,
                           MapSharedShreddingAccessBuilder::Create(c_map_field.get()));
    for (const std::string& selected_key : selected_keys) {
        PAIMON_RETURN_NOT_OK(access_builder->AddKey(selected_key));
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ArrowSchema> c_access_field, access_builder->Build());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Field> access_field,
                                      arrow::ImportField(c_access_field.get()));
    return access_field;
}

std::shared_ptr<arrow::StructArray> AddRowKindAndOffset(
    const std::shared_ptr<arrow::StructArray>& data) {
    arrow::Int8Builder row_kind_builder;
    EXPECT_TRUE(row_kind_builder.AppendValues(std::vector<int8_t>(data->length(), 0)).ok());
    std::shared_ptr<arrow::Array> row_kinds;
    EXPECT_TRUE(row_kind_builder.Finish(&row_kinds).ok());
    arrow::Int64Builder offset_builder;
    for (int64_t offset = 0; offset < data->length(); ++offset) {
        EXPECT_TRUE(offset_builder.Append(offset).ok());
    }
    std::shared_ptr<arrow::Array> offsets;
    EXPECT_TRUE(offset_builder.Finish(&offsets).ok());
    arrow::ArrayVector arrays = {row_kinds, offsets};
    arrays.insert(arrays.end(), data->fields().begin(), data->fields().end());
    arrow::FieldVector fields = {
        arrow::field("_VALUE_KIND", arrow::int8()),
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset())};
    fields.insert(fields.end(), data->struct_type()->fields().begin(),
                  data->struct_type()->fields().end());
    auto result = arrow::StructArray::Make(arrays, fields);
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    return result.ValueOrDie();
}

struct ReadResult {
    std::shared_ptr<arrow::StructArray> array;
};

Result<ReadResult> ReadOne(const RealtimeStoreReadPipeline& pipeline,
                           const std::shared_ptr<arrow::StructArray>& source_array) {
    auto source_reader = std::make_unique<MockFileBatchReader>(
        source_array, source_array->type(), static_cast<int32_t>(source_array->length()));
    source_reader->EnableRandomizeBatchSize(false);
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BatchReader> wrapped,
        pipeline.Wrap(std::move(source_reader), OffsetRange(0, source_array->length())));
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           wrapped->NextBatchWithBitmap());
    BatchReader::ReadBatch batch = std::move(batch_with_bitmap.first);
    wrapped->Close();
    wrapped.reset();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                      arrow::ImportArray(batch.first.get(), batch.second.get()));
    return ReadResult{checked_pointer_cast<arrow::StructArray>(imported)};
}

}  // namespace

TEST(RealtimeStoreReadPipelineTest, SelectedMapKeysAsMapAndStruct) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<arrow::DataType> map_type = arrow::map(arrow::utf8(), arrow::int64());
    std::shared_ptr<arrow::Field> value_kind_field =
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind());
    std::shared_ptr<arrow::Field> offset_field =
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset());
    auto id_field = arrow::field("id", arrow::int64());
    auto tags_field = arrow::field("tags", map_type);
    auto write_schema = arrow::schema({offset_field, id_field, tags_field});
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({id_field, tags_field}), R"([
            [1, [["a", 10], ["c", 30]]],
            [2, [["b", 20]]],
            [3, null]
        ])")
            .ValueOrDie();
    auto source = AddRowKindAndOffset(checked_pointer_cast<arrow::StructArray>(data));

    auto selected_map = MapReadField(map_type, "c,a,missing");
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RealtimeStoreReadPipeline> map_pipeline,
        RealtimeStoreReadPipeline::Create(arrow::schema({value_kind_field, id_field, selected_map}),
                                          write_schema, pool));
    ASSERT_TRUE(map_pipeline->StoreReadSchema()->field(2)->type()->Equals(map_type));
    ASSERT_OK_AND_ASSIGN(ReadResult map_result, ReadOne(*map_pipeline, source));
    std::shared_ptr<arrow::Array> expected_map =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()), id_field, selected_map}),
            R"([
            [0, 1, [["c", 30], ["a", 10]]],
            [0, 2, []],
            [0, 3, null]
        ])")
            .ValueOrDie();
    ASSERT_TRUE(map_result.array->Equals(expected_map))
        << "actual: " << map_result.array->ToString();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> selected_struct,
                         MapAccessField(tags_field, {"a", "missing"}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RealtimeStoreReadPipeline> struct_pipeline,
        RealtimeStoreReadPipeline::Create(
            arrow::schema({value_kind_field, id_field, selected_struct}), write_schema, pool));
    ASSERT_EQ(struct_pipeline->StoreReadSchema()->field(2)->type()->id(), arrow::Type::MAP);
    ASSERT_OK_AND_ASSIGN(ReadResult struct_result, ReadOne(*struct_pipeline, source));
    std::shared_ptr<arrow::Array> expected_struct =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()), id_field, selected_struct}),
            R"([
            [0, 1, [10, null]],
            [0, 2, [null, null]],
            [0, 3, null]
        ])")
            .ValueOrDie();
    ASSERT_TRUE(struct_result.array->Equals(expected_struct))
        << "actual: " << struct_result.array->ToString();
}

TEST(RealtimeStoreReadPipelineTest, VariantAccessOnLogicalVariant) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<arrow::Field> value_kind_field =
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind());
    std::shared_ptr<arrow::Field> offset_field =
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset());
    auto id_field = arrow::field("id", arrow::int32());
    auto variant_field = VariantTypeUtils::ToArrowField("v");
    auto write_schema = arrow::schema({offset_field, id_field, variant_field});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> data,
                         VariantTestData::BuildVariantBatch(id_field, variant_field,
                                                            {R"({"a":5,"city":"hangzhou"})"}, pool,
                                                            /*id_offset=*/1));

    VariantAccessBuilder access_builder;
    auto int_target = std::make_unique<ArrowSchema>();
    auto string_target = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportField(*arrow::field("a", arrow::int64()), int_target.get()).ok());
    ASSERT_TRUE(arrow::ExportField(*arrow::field("city", arrow::utf8()), string_target.get()).ok());
    ASSERT_OK(access_builder.AddField(int_target.get(), "$.a", /*fail_on_error=*/false));
    ASSERT_OK(access_builder.AddField(string_target.get(), "$.city", /*fail_on_error=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowSchema> c_access_field, access_builder.Build("v"));
    auto access_field_result = arrow::ImportField(c_access_field.get());
    ASSERT_TRUE(access_field_result.ok()) << access_field_result.status().ToString();
    std::shared_ptr<arrow::Field> access_field = access_field_result.ValueOrDie();
    auto read_schema = arrow::schema({value_kind_field, id_field, access_field});

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RealtimeStoreReadPipeline> pipeline,
                         RealtimeStoreReadPipeline::Create(read_schema, write_schema, pool));
    ASSERT_TRUE(pipeline->StoreReadSchema()->field(2)->type()->Equals(variant_field->type()));
    ASSERT_OK_AND_ASSIGN(ReadResult result, ReadOne(*pipeline, AddRowKindAndOffset(data)));
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()), id_field, access_field}),
            R"([[0, 1, [5, "hangzhou"]]])")
            .ValueOrDie();
    ASSERT_TRUE(result.array->Equals(expected)) << "actual: " << result.array->ToString();
}

}  // namespace paimon::test
