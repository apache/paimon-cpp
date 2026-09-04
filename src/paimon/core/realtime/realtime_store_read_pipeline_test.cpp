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
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/data/shredding/map_shared_shredding_schema_utils.h"
#include "paimon/data/variant.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
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
    std::shared_ptr<arrow::DataType> source_type =
        arrow::struct_({value_kind_field, offset_field, id_field, tags_field});
    std::shared_ptr<arrow::Array> source =
        arrow::ipc::internal::json::ArrayFromJSON(source_type, R"([
            [0, 0, 1, [["a", 10], ["c", 30]]],
            [0, 1, 2, [["b", 20]]],
            [0, 2, 3, null]
        ])")
            .ValueOrDie();

    auto selected_map = MapReadField(map_type, "c,a,missing");
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RealtimeStoreReadPipeline> map_pipeline,
        RealtimeStoreReadPipeline::Create(arrow::schema({value_kind_field, id_field, selected_map}),
                                          write_schema, pool, GetArrowPool(pool)));
    ASSERT_TRUE(map_pipeline->StoreReadSchema()->field(2)->type()->Equals(map_type));
    auto map_source_reader = std::make_unique<MockFileBatchReader>(
        source, source_type, static_cast<int32_t>(source->length()));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<BatchReader> map_reader,
        map_pipeline->Wrap(std::move(map_source_reader), OffsetRange(0, source->length())));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> map_result,
                         ReadResultCollector::CollectResult(std::move(map_reader)));
    std::shared_ptr<arrow::Array> expected_map =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()), id_field, selected_map}),
            R"([
            [0, 1, [["c", 30], ["a", 10]]],
            [0, 2, []],
            [0, 3, null]
        ])")
            .ValueOrDie();
    std::shared_ptr<arrow::ChunkedArray> expected_map_result =
        arrow::ChunkedArray::Make({expected_map}).ValueOrDie();
    ASSERT_TRUE(map_result->Equals(*expected_map_result)) << "actual: " << map_result->ToString();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> selected_struct,
                         MapAccessField(tags_field, {"a", "missing"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RealtimeStoreReadPipeline> struct_pipeline,
                         RealtimeStoreReadPipeline::Create(
                             arrow::schema({value_kind_field, id_field, selected_struct}),
                             write_schema, pool, GetArrowPool(pool)));
    ASSERT_EQ(struct_pipeline->StoreReadSchema()->field(2)->type()->id(), arrow::Type::MAP);
    auto struct_source_reader = std::make_unique<MockFileBatchReader>(
        source, source_type, static_cast<int32_t>(source->length()));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<BatchReader> struct_reader,
        struct_pipeline->Wrap(std::move(struct_source_reader), OffsetRange(0, source->length())));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> struct_result,
                         ReadResultCollector::CollectResult(std::move(struct_reader)));
    std::shared_ptr<arrow::Array> expected_struct =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()), id_field, selected_struct}),
            R"([
            [0, 1, [10, null]],
            [0, 2, [null, null]],
            [0, 3, null]
        ])")
            .ValueOrDie();
    std::shared_ptr<arrow::ChunkedArray> expected_struct_result =
        arrow::ChunkedArray::Make({expected_struct}).ValueOrDie();
    ASSERT_TRUE(struct_result->Equals(*expected_struct_result))
        << "actual: " << struct_result->ToString();
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
    std::shared_ptr<arrow::Array> value_kinds =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int8(), "[0]").ValueOrDie();
    std::shared_ptr<arrow::Array> offsets =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[0]").ValueOrDie();
    std::shared_ptr<arrow::StructArray> source =
        arrow::StructArray::Make({value_kinds, offsets, data->field(0), data->field(1)},
                                 {value_kind_field, offset_field, id_field, variant_field})
            .ValueOrDie();

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

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RealtimeStoreReadPipeline> pipeline,
        RealtimeStoreReadPipeline::Create(read_schema, write_schema, pool, GetArrowPool(pool)));
    ASSERT_TRUE(pipeline->StoreReadSchema()->field(2)->type()->Equals(variant_field->type()));
    auto source_reader = std::make_unique<MockFileBatchReader>(
        source, source->type(), static_cast<int32_t>(source->length()));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<BatchReader> reader,
        pipeline->Wrap(std::move(source_reader), OffsetRange(0, source->length())));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(std::move(reader)));
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()), id_field, access_field}),
            R"([[0, 1, [5, "hangzhou"]]])")
            .ValueOrDie();
    std::shared_ptr<arrow::ChunkedArray> expected_result =
        arrow::ChunkedArray::Make({expected}).ValueOrDie();
    ASSERT_TRUE(result->Equals(*expected_result)) << "actual: " << result->ToString();
}

}  // namespace paimon::test
