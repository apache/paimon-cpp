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

#include "paimon/common/data/shredding/map_shared_shredding_file_reader.h"

#include <optional>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/util/key_value_metadata.h"
#include "fmt/format.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/core/utils/nested_projection_utils.h"

namespace paimon {
namespace {

std::vector<std::pair<std::string, int32_t>> ResolveSelectedKeyIds(
    const MapSharedShreddingFieldMeta& meta, const std::vector<std::string>& selected_keys) {
    std::vector<std::pair<std::string, int32_t>> selected_key_ids;
    selected_key_ids.reserve(selected_keys.size());
    for (const auto& selected_key : selected_keys) {
        auto id_iter = meta.name_to_id.find(selected_key);
        if (id_iter != meta.name_to_id.end()) {
            selected_key_ids.emplace_back(selected_key, id_iter->second);
        }
    }
    return selected_key_ids;
}

void CollectPhysicalColumns(
    const std::shared_ptr<arrow::StructArray>& physical_struct_array,
    std::map<std::string, std::shared_ptr<arrow::Array>>* physical_column_name_to_array,
    std::shared_ptr<arrow::MapArray>* overflow_array) {
    const auto& struct_type = physical_struct_array->struct_type();
    for (int32_t i = 0; i < struct_type->num_fields(); ++i) {
        const auto& sub_field = struct_type->field(i);
        if (sub_field->name() == MapSharedShreddingDefine::kFieldMapping) {
            continue;
        }
        if (sub_field->name() == MapSharedShreddingDefine::kOverflow) {
            *overflow_array = arrow::internal::checked_pointer_cast<arrow::MapArray>(
                physical_struct_array->field(i));
            continue;
        }
        (*physical_column_name_to_array)[sub_field->name()] = physical_struct_array->field(i);
    }
}

class FullMapReadPlan : public MapFieldReadPlan {
 public:
    FullMapReadPlan(const std::shared_ptr<arrow::Field>& logical_field,
                    const std::shared_ptr<arrow::Field>& physical_read_field,
                    std::vector<std::pair<std::string, int32_t>>&& selected_key_ids)
        : MapFieldReadPlan(logical_field, physical_read_field),
          selected_key_ids_(std::move(selected_key_ids)),
          logical_map_type_(
              arrow::internal::checked_pointer_cast<arrow::MapType>(logical_field->type())) {}

    Result<std::shared_ptr<arrow::Array>> Materialize(
        const std::shared_ptr<arrow::Array>& physical_array,
        arrow::MemoryPool* arrow_pool) const override;

 private:
    std::vector<std::pair<std::string, int32_t>> selected_key_ids_;
    std::shared_ptr<arrow::MapType> logical_map_type_;
};

class SharedSelectedKeysReadPlan : public MapFieldReadPlan {
 public:
    struct SelectedKey {
        int32_t field_id = -1;
        std::vector<int32_t> candidate_columns;
        bool may_use_overflow = false;
    };

    SharedSelectedKeysReadPlan(const std::shared_ptr<arrow::Field>& logical_field,
                               const std::shared_ptr<arrow::Field>& physical_read_field,
                               std::vector<SelectedKey>&& selected_keys)
        : MapFieldReadPlan(logical_field, physical_read_field),
          selected_keys_(std::move(selected_keys)) {}

    Result<std::shared_ptr<arrow::Array>> Materialize(
        const std::shared_ptr<arrow::Array>& physical_array,
        arrow::MemoryPool* arrow_pool) const override;

 private:
    std::vector<SelectedKey> selected_keys_;
};

class DefaultSelectedKeysReadPlan : public MapFieldReadPlan {
 public:
    DefaultSelectedKeysReadPlan(const std::shared_ptr<arrow::Field>& logical_field,
                                const std::shared_ptr<arrow::Field>& physical_read_field,
                                const std::vector<std::string>& selected_keys)
        : MapFieldReadPlan(logical_field, physical_read_field), selected_keys_(selected_keys) {}

    Result<std::shared_ptr<arrow::Array>> Materialize(
        const std::shared_ptr<arrow::Array>& physical_array,
        arrow::MemoryPool* arrow_pool) const override;

 private:
    std::vector<std::string> selected_keys_;
};

}  // namespace

Result<std::unique_ptr<MapFieldReadPlan>> MapFieldReadPlanFactory::CreateMapReadPlan(
    const std::shared_ptr<arrow::Field>& logical_map_field,
    const MapSharedShreddingFieldMeta& meta) {
    if (logical_map_field->type()->id() != arrow::Type::MAP) {
        return Status::Invalid(fmt::format("full MAP read plan requires MAP field {}, got {}",
                                           logical_map_field->name(),
                                           logical_map_field->type()->ToString()));
    }
    auto logical_map_type =
        arrow::internal::checked_pointer_cast<arrow::MapType>(logical_map_field->type());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> selected_keys,
                           NestedProjectionUtils::GetMapSelectedKeys(logical_map_field));
    if (selected_keys.empty()) {
        selected_keys.reserve(meta.name_to_id.size());
        for (const auto& [key_name, _] : meta.name_to_id) {
            selected_keys.push_back(key_name);
        }
    }
    std::set<int32_t> selected_physical_column_ids;
    bool include_overflow = false;
    for (const auto& selected_key : selected_keys) {
        auto field_id_iter = meta.name_to_id.find(selected_key);
        if (field_id_iter == meta.name_to_id.end()) {
            continue;
        }
        int32_t field_id = field_id_iter->second;
        include_overflow = include_overflow || meta.overflow_field_set.count(field_id) > 0;
        auto columns_iter = meta.field_to_columns.find(field_id);
        if (columns_iter != meta.field_to_columns.end()) {
            selected_physical_column_ids.insert(columns_iter->second.begin(),
                                                columns_iter->second.end());
        }
    }
    std::shared_ptr<arrow::DataType> physical_type =
        MapSharedShreddingUtils::BuildSpecificPhysicalStructType(
            logical_map_type->item_type(), selected_physical_column_ids,
            logical_map_type->item_field()->nullable(), include_overflow);
    auto physical_read_field = logical_map_field->WithType(physical_type);
    std::unique_ptr<MapFieldReadPlan> read_plan = std::make_unique<FullMapReadPlan>(
        logical_map_field, physical_read_field, ResolveSelectedKeyIds(meta, selected_keys));
    return read_plan;
}

Result<std::unique_ptr<MapFieldReadPlan>> MapFieldReadPlanFactory::CreateSharedSelectedKeysReadPlan(
    const std::shared_ptr<arrow::Field>& selected_keys_field,
    const MapSharedShreddingFieldMeta& meta) {
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::string> selected_keys,
        NestedProjectionUtils::ValidateMapSharedShreddingAccessField(selected_keys_field));
    auto selected_keys_type =
        arrow::internal::checked_pointer_cast<arrow::StructType>(selected_keys_field->type());
    const auto& value_field = selected_keys_type->field(0);

    std::set<int32_t> selected_physical_column_ids;
    bool include_overflow = false;
    std::vector<SharedSelectedKeysReadPlan::SelectedKey> selected_key_plans;
    selected_key_plans.reserve(selected_keys.size());
    for (const auto& selected_key : selected_keys) {
        SharedSelectedKeysReadPlan::SelectedKey selected_key_plan;
        auto field_id_iter = meta.name_to_id.find(selected_key);
        if (field_id_iter != meta.name_to_id.end()) {
            selected_key_plan.field_id = field_id_iter->second;
            auto columns_iter = meta.field_to_columns.find(selected_key_plan.field_id);
            if (columns_iter != meta.field_to_columns.end()) {
                selected_key_plan.candidate_columns = columns_iter->second;
                selected_physical_column_ids.insert(columns_iter->second.begin(),
                                                    columns_iter->second.end());
            }
            selected_key_plan.may_use_overflow =
                meta.overflow_field_set.count(selected_key_plan.field_id) > 0;
            include_overflow = include_overflow || selected_key_plan.may_use_overflow;
        }
        selected_key_plans.push_back(std::move(selected_key_plan));
    }
    std::shared_ptr<arrow::DataType> physical_type =
        MapSharedShreddingUtils::BuildSpecificPhysicalStructType(
            value_field->type(), selected_physical_column_ids, value_field->nullable(),
            include_overflow);
    auto physical_read_field = selected_keys_field->WithType(physical_type);
    std::unique_ptr<MapFieldReadPlan> read_plan = std::make_unique<SharedSelectedKeysReadPlan>(
        selected_keys_field, physical_read_field, std::move(selected_key_plans));
    return read_plan;
}

Result<std::unique_ptr<MapFieldReadPlan>>
MapFieldReadPlanFactory::CreateDefaultSelectedKeysReadPlan(
    const std::shared_ptr<arrow::Field>& file_map_field,
    const std::shared_ptr<arrow::Field>& selected_keys_field) {
    if (file_map_field->type()->id() != arrow::Type::MAP) {
        return Status::Invalid(
            fmt::format("selected-key MAP projection {} requires MAP file field, got {}",
                        selected_keys_field->name(), file_map_field->type()->ToString()));
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::string> selected_keys,
        NestedProjectionUtils::ValidateMapSharedShreddingAccessField(selected_keys_field));
    auto physical_read_field = selected_keys_field->WithType(file_map_field->type());
    std::unique_ptr<MapFieldReadPlan> read_plan = std::make_unique<DefaultSelectedKeysReadPlan>(
        selected_keys_field, physical_read_field, selected_keys);
    return read_plan;
}

MapSharedShreddingFileReader::MapSharedShreddingFileReader(
    std::unique_ptr<FileBatchReader>&& reader,
    std::map<std::string, std::unique_ptr<MapFieldReadPlan>>&& field_read_plans,
    const std::shared_ptr<MemoryPool>& pool)
    : arrow_pool_(GetArrowPool(pool)),
      reader_(std::move(reader)),
      field_read_plans_(std::move(field_read_plans)) {}

Result<std::unique_ptr<::ArrowSchema>> MapSharedShreddingFileReader::GetFileSchema() const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> physical_schema,
                           reader_->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> physical_arrow_schema,
                                      arrow::ImportSchema(physical_schema.get()));

    arrow::FieldVector logical_fields = physical_arrow_schema->fields();
    for (int32_t i = 0; i < physical_arrow_schema->num_fields(); ++i) {
        const auto& field = physical_arrow_schema->field(i);
        std::shared_ptr<arrow::KeyValueMetadata> metadata =
            std::const_pointer_cast<arrow::KeyValueMetadata>(field->metadata());
        if (!MapSharedShreddingUtils::HasShreddingMetadata(metadata)) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(logical_fields[i], ToLogicalMapField(field));
    }

    auto logical_schema = arrow::schema(std::move(logical_fields));
    auto c_logical_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*logical_schema, c_logical_schema.get()));
    return c_logical_schema;
}

Result<std::shared_ptr<arrow::Field>> MapSharedShreddingFileReader::ToLogicalMapField(
    const std::shared_ptr<arrow::Field>& physical_field) {
    auto physical_type =
        arrow::internal::checked_pointer_cast<arrow::StructType>(physical_field->type());
    if (!physical_type) {
        return Status::Invalid(fmt::format("shared-shredding field {} is not a physical struct",
                                           physical_field->name()));
    }
    std::shared_ptr<arrow::DataType> value_type;
    bool value_nullable = true;
    for (const auto& child : physical_type->fields()) {
        if (child->name() == MapSharedShreddingDefine::kFieldMapping ||
            child->name() == MapSharedShreddingDefine::kOverflow) {
            continue;
        }
        value_type = child->type();
        value_nullable = child->nullable();
        break;
    }
    if (!value_type) {
        return Status::Invalid(fmt::format("cannot infer shared-shredding value type for field {}",
                                           physical_field->name()));
    }
    return arrow::field(
        physical_field->name(),
        arrow::map(arrow::utf8(), arrow::field("value", value_type, value_nullable)),
        physical_field->nullable());
}

Status MapSharedShreddingFileReader::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!read_schema) {
        return Status::Invalid(
            "invalid read schema in MapSharedShreddingFileReader, cannot be null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> logical_read_schema,
                                      arrow::ImportSchema(read_schema));
    bool converted = false;
    arrow::FieldVector physical_read_fields = logical_read_schema->fields();
    for (size_t i = 0; i < logical_read_schema->fields().size(); ++i) {
        const auto& field = logical_read_schema->field(i);
        auto plan_iter = field_read_plans_.find(field->name());
        if (plan_iter != field_read_plans_.end()) {
            physical_read_fields[i] = plan_iter->second->PhysicalReadField();
            converted = true;
        }
    }
    if (!converted) {
        return Status::Invalid("suppose not fall into MapSharedShreddingFileReader");
    }
    auto physical_read_schema = arrow::schema(std::move(physical_read_fields));
    std::unique_ptr<ArrowSchema> c_physical_read_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportSchema(*physical_read_schema, c_physical_read_schema.get()));
    return reader_->SetReadSchema(c_physical_read_schema.get(), predicate, selection_bitmap);
}

Result<BatchReader::ReadBatch> MapSharedShreddingFileReader::NextBatch() {
    return Status::Invalid(
        "paimon inner reader MapSharedShreddingFileReader should use NextBatchWithBitmap");
}

Result<BatchReader::ReadBatchWithBitmap> MapSharedShreddingFileReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           reader_->NextBatchWithBitmap());
    if (BatchReader::IsEofBatch(batch_with_bitmap)) {
        return batch_with_bitmap;
    }

    auto& [batch, bitmap] = batch_with_bitmap;
    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(arrow_array);
    if (!struct_array) {
        return Status::Invalid("cannot cast batch to StructArray in MapSharedShreddingFileReader");
    }

    arrow::ArrayVector resolved_arrays = struct_array->fields();
    arrow::FieldVector resolved_fields = struct_array->struct_type()->fields();
    for (int32_t field_idx = 0; field_idx < struct_array->num_fields(); ++field_idx) {
        const auto& physical_field = struct_array->struct_type()->field(field_idx);
        auto plan_iter = field_read_plans_.find(physical_field->name());
        if (plan_iter == field_read_plans_.end()) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(
            resolved_arrays[field_idx],
            plan_iter->second->Materialize(struct_array->field(field_idx), arrow_pool_.get()));
        resolved_fields[field_idx] = plan_iter->second->LogicalField();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> new_struct_array,
                                      arrow::StructArray::Make(resolved_arrays, resolved_fields));
    auto new_c_array = std::make_unique<ArrowArray>();
    auto new_c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*new_struct_array, new_c_array.get(), new_c_schema.get()));
    batch = std::make_pair(std::move(new_c_array), std::move(new_c_schema));
    return batch_with_bitmap;
}

Result<std::shared_ptr<arrow::Array>> FullMapReadPlan::Materialize(
    const std::shared_ptr<arrow::Array>& physical_array, arrow::MemoryPool* arrow_pool) const {
    auto physical_struct_array =
        arrow::internal::checked_pointer_cast<arrow::StructArray>(physical_array);
    if (!physical_struct_array) {
        return Status::Invalid(fmt::format("cannot cast physical shredding field {} to StructArray",
                                           LogicalField()->name()));
    }
    const std::string& shredding_field_name = LogicalField()->name();

    auto field_mapping_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(
        physical_struct_array->GetFieldByName(MapSharedShreddingDefine::kFieldMapping));
    if (!field_mapping_array) {
        return Status::Invalid(
            fmt::format("cannot find __field_mapping for field {}", shredding_field_name));
    }
    auto field_mapping_values =
        arrow::internal::checked_pointer_cast<arrow::Int32Array>(field_mapping_array->values());
    if (!field_mapping_values) {
        return Status::Invalid("__field_mapping values is not an Int32Array");
    }

    std::map<std::string, std::shared_ptr<arrow::Array>> physical_column_name_to_array;
    std::shared_ptr<arrow::MapArray> overflow_array;
    CollectPhysicalColumns(physical_struct_array, &physical_column_name_to_array, &overflow_array);
    for (auto& [_, physical_column_array] : physical_column_name_to_array) {
        if (physical_column_array->type_id() == arrow::Type::DICTIONARY) {
            PAIMON_ASSIGN_OR_RAISE(
                physical_column_array,
                CastingUtils::Cast(physical_column_array, logical_map_type_->item_type(),
                                   arrow::compute::CastOptions::Safe(), arrow_pool));
        }
    }

    std::shared_ptr<arrow::Int32Array> overflow_keys;
    std::shared_ptr<arrow::Array> overflow_items;
    if (overflow_array) {
        overflow_keys =
            arrow::internal::checked_pointer_cast<arrow::Int32Array>(overflow_array->keys());
        overflow_items = overflow_array->items();
        if (!overflow_keys || !overflow_items) {
            return Status::Invalid("__overflow map has invalid key or item array");
        }
        if (overflow_items->type_id() == arrow::Type::DICTIONARY) {
            PAIMON_ASSIGN_OR_RAISE(
                overflow_items,
                CastingUtils::Cast(overflow_items, logical_map_type_->item_type(),
                                   arrow::compute::CastOptions::Safe(), arrow_pool));
        }
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> map_builder_base,
                                      arrow::MakeBuilder(logical_map_type_, arrow_pool));
    auto* map_builder = dynamic_cast<arrow::MapBuilder*>(map_builder_base.get());
    if (!map_builder) {
        return Status::Invalid(
            fmt::format("cannot create MapBuilder for field {}", shredding_field_name));
    }
    auto* key_builder = dynamic_cast<arrow::StringBuilder*>(map_builder->key_builder());
    if (!key_builder) {
        return Status::Invalid(fmt::format("map key builder is not a StringBuilder for field {}",
                                           shredding_field_name));
    }
    arrow::ArrayBuilder* item_builder = map_builder->item_builder();
    if (!item_builder) {
        return Status::Invalid(
            fmt::format("map item builder is null for field {}", shredding_field_name));
    }

    int64_t row_count = physical_struct_array->length();
    int64_t max_item_count = row_count * static_cast<int64_t>(selected_key_ids_.size());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->Reserve(row_count));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Reserve(max_item_count));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(item_builder->Reserve(max_item_count));

    for (int64_t row = 0; row < row_count; ++row) {
        if (physical_struct_array->IsNull(row)) {
            // null struct -> null map
            PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->AppendNull());
            continue;
        }
        if (field_mapping_array->IsNull(row)) {
            return Status::Invalid(fmt::format(
                "__field_mapping cannot be null in non-null shared-shredding row for field {}",
                shredding_field_name));
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->Append());
        int32_t mapping_offset = field_mapping_array->value_offset(row);
        int32_t mapping_length = field_mapping_array->value_length(row);
        // follow the sequence in paimon.map.selected-keys
        for (const auto& [selected_key, selected_field_id] : selected_key_ids_) {
            bool found = false;
            for (int32_t pos = 0; pos < mapping_length; ++pos) {
                int32_t mapping_index = mapping_offset + pos;
                if (field_mapping_values->IsNull(mapping_index)) {
                    return Status::Invalid("__field_mapping element cannot be null");
                }
                if (field_mapping_values->Value(mapping_index) != selected_field_id) {
                    continue;
                }
                std::string physical_column_name =
                    MapSharedShreddingDefine::PhysicalColumnName(pos);
                auto physical_column_iter =
                    physical_column_name_to_array.find(physical_column_name);
                if (physical_column_iter == physical_column_name_to_array.end()) {
                    return Status::Invalid(
                        fmt::format("cannot find selected physical column {} for field {}",
                                    physical_column_name, shredding_field_name));
                }
                PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Append(selected_key));
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    item_builder->AppendArraySlice(*physical_column_iter->second->data(), row, 1));
                found = true;
                break;
            }
            if (found || !overflow_array) {
                continue;
            }
            int32_t overflow_offset = overflow_array->value_offset(row);
            int32_t overflow_length = overflow_array->value_length(row);
            for (int32_t pos = 0; pos < overflow_length; ++pos) {
                int32_t overflow_index = overflow_offset + pos;
                if (!overflow_keys->IsNull(overflow_index) &&
                    overflow_keys->Value(overflow_index) == selected_field_id) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->Append(selected_key));
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(
                        item_builder->AppendArraySlice(*overflow_items->data(), overflow_index, 1));
                    break;
                }
            }
        }
    }
    std::shared_ptr<arrow::MapArray> map_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder->Finish(&map_array));
    return map_array;
}

Result<std::shared_ptr<arrow::Array>> SharedSelectedKeysReadPlan::Materialize(
    const std::shared_ptr<arrow::Array>& physical_array, arrow::MemoryPool* arrow_pool) const {
    auto physical_struct_array =
        arrow::internal::checked_pointer_cast<arrow::StructArray>(physical_array);
    if (!physical_struct_array) {
        return Status::Invalid(fmt::format("cannot cast physical shredding field {} to StructArray",
                                           LogicalField()->name()));
    }
    auto selected_keys_type =
        arrow::internal::checked_pointer_cast<arrow::StructType>(LogicalField()->type());

    auto field_mapping_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(
        physical_struct_array->GetFieldByName(MapSharedShreddingDefine::kFieldMapping));
    if (!field_mapping_array) {
        return Status::Invalid(
            fmt::format("cannot find __field_mapping for field {}", LogicalField()->name()));
    }
    auto field_mapping_values =
        arrow::internal::checked_pointer_cast<arrow::Int32Array>(field_mapping_array->values());
    if (!field_mapping_values) {
        return Status::Invalid("__field_mapping values is not an Int32Array");
    }

    std::shared_ptr<arrow::DataType> value_type = selected_keys_type->field(0)->type();
    std::map<std::string, std::shared_ptr<arrow::Array>> physical_column_name_to_array;
    std::shared_ptr<arrow::MapArray> overflow_array;
    CollectPhysicalColumns(physical_struct_array, &physical_column_name_to_array, &overflow_array);
    for (auto& [_, physical_column_array] : physical_column_name_to_array) {
        if (physical_column_array->type_id() == arrow::Type::DICTIONARY) {
            PAIMON_ASSIGN_OR_RAISE(
                physical_column_array,
                CastingUtils::Cast(physical_column_array, value_type,
                                   arrow::compute::CastOptions::Safe(), arrow_pool));
        }
    }

    std::shared_ptr<arrow::Int32Array> overflow_keys;
    std::shared_ptr<arrow::Array> overflow_items;
    if (overflow_array) {
        overflow_keys =
            arrow::internal::checked_pointer_cast<arrow::Int32Array>(overflow_array->keys());
        overflow_items = overflow_array->items();
        if (!overflow_keys || !overflow_items) {
            return Status::Invalid("__overflow map has invalid key or item array");
        }
        if (overflow_items->type_id() == arrow::Type::DICTIONARY) {
            PAIMON_ASSIGN_OR_RAISE(
                overflow_items,
                CastingUtils::Cast(overflow_items, value_type, arrow::compute::CastOptions::Safe(),
                                   arrow_pool));
        }
    }

    std::unique_ptr<arrow::ArrayBuilder> access_builder_base;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(access_builder_base,
                                      arrow::MakeBuilder(LogicalField()->type(), arrow_pool));
    auto* access_builder = dynamic_cast<arrow::StructBuilder*>(access_builder_base.get());
    if (!access_builder) {
        return Status::Invalid(
            fmt::format("selected-key MAP field {} is not a STRUCT", LogicalField()->name()));
    }
    PAIMON_RETURN_NOT_OK_FROM_ARROW(access_builder->Reserve(physical_struct_array->length()));

    for (int64_t row = 0; row < physical_struct_array->length(); ++row) {
        if (physical_struct_array->IsNull(row)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(access_builder->AppendNull());
            continue;
        }
        if (field_mapping_array->IsNull(row)) {
            return Status::Invalid(fmt::format(
                "__field_mapping cannot be null in non-null shared-shredding row for field {}",
                LogicalField()->name()));
        }
        int32_t mapping_offset = field_mapping_array->value_offset(row);

        PAIMON_RETURN_NOT_OK_FROM_ARROW(access_builder->Append());
        for (int32_t key_index = 0; key_index < selected_keys_type->num_fields(); ++key_index) {
            arrow::ArrayBuilder* value_builder = access_builder->field_builder(key_index);
            const SelectedKey& selected_key = selected_keys_[key_index];
            bool appended = false;
            if (selected_key.field_id >= 0) {
                for (int32_t physical_column_id : selected_key.candidate_columns) {
                    int32_t mapping_index = mapping_offset + physical_column_id;
                    if (field_mapping_values->IsNull(mapping_index)) {
                        return Status::Invalid("__field_mapping element cannot be null");
                    }
                    if (field_mapping_values->Value(mapping_index) != selected_key.field_id) {
                        continue;
                    }
                    std::string physical_column_name =
                        MapSharedShreddingDefine::PhysicalColumnName(physical_column_id);
                    auto physical_column_iter =
                        physical_column_name_to_array.find(physical_column_name);
                    if (physical_column_iter == physical_column_name_to_array.end()) {
                        return Status::Invalid(
                            fmt::format("cannot find selected physical column {} for field {}",
                                        physical_column_name, LogicalField()->name()));
                    }
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->AppendArraySlice(
                        *physical_column_iter->second->data(), row, 1));
                    appended = true;
                    break;
                }
            }

            if (!appended && selected_key.may_use_overflow && overflow_array &&
                !overflow_array->IsNull(row)) {
                int32_t overflow_offset = overflow_array->value_offset(row);
                int32_t overflow_length = overflow_array->value_length(row);
                for (int32_t pos = 0; pos < overflow_length; ++pos) {
                    int32_t overflow_index = overflow_offset + pos;
                    if (!overflow_keys->IsNull(overflow_index) &&
                        overflow_keys->Value(overflow_index) == selected_key.field_id) {
                        PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->AppendArraySlice(
                            *overflow_items->data(), overflow_index, 1));
                        appended = true;
                        break;
                    }
                }
            }
            if (!appended) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->AppendNull());
            }
        }
    }
    std::shared_ptr<arrow::Array> result;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(access_builder->Finish(&result));
    return result;
}

Result<std::shared_ptr<arrow::Array>> DefaultSelectedKeysReadPlan::Materialize(
    const std::shared_ptr<arrow::Array>& physical_array, arrow::MemoryPool* arrow_pool) const {
    auto map_array = arrow::internal::checked_pointer_cast<arrow::MapArray>(physical_array);
    if (!map_array) {
        return Status::Invalid(
            fmt::format("cannot cast default-layout selected-key field {} to "
                        "MapArray",
                        LogicalField()->name()));
    }
    auto selected_keys_type =
        arrow::internal::checked_pointer_cast<arrow::StructType>(LogicalField()->type());
    auto physical_map_type =
        arrow::internal::checked_pointer_cast<arrow::MapType>(PhysicalReadField()->type());

    std::shared_ptr<arrow::Array> items = map_array->items();
    if (items->type_id() == arrow::Type::DICTIONARY) {
        PAIMON_ASSIGN_OR_RAISE(items,
                               CastingUtils::Cast(items, physical_map_type->item_type(),
                                                  arrow::compute::CastOptions::Safe(), arrow_pool));
    }
    std::shared_ptr<arrow::Array> keys = map_array->keys();
    std::unique_ptr<arrow::ArrayBuilder> access_builder_base;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(access_builder_base,
                                      arrow::MakeBuilder(LogicalField()->type(), arrow_pool));
    auto* access_builder = dynamic_cast<arrow::StructBuilder*>(access_builder_base.get());
    if (!access_builder) {
        return Status::Invalid(
            fmt::format("selected-key MAP field {} is not a STRUCT", LogicalField()->name()));
    }
    PAIMON_RETURN_NOT_OK_FROM_ARROW(access_builder->Reserve(map_array->length()));

    for (int64_t row = 0; row < map_array->length(); ++row) {
        if (map_array->IsNull(row)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(access_builder->AppendNull());
            continue;
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(access_builder->Append());
        int64_t begin = map_array->value_offset(row);
        int64_t end = map_array->value_offset(row + 1);
        for (int32_t key_index = 0; key_index < selected_keys_type->num_fields(); ++key_index) {
            arrow::ArrayBuilder* value_builder = access_builder->field_builder(key_index);
            bool appended = false;
            for (int64_t entry = begin; entry < end; ++entry) {
                PAIMON_ASSIGN_OR_RAISE(std::string_view key,
                                       NestedProjectionUtils::GetMapKeyViewAt(keys, entry));
                if (key != selected_keys_[key_index]) {
                    continue;
                }
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    value_builder->AppendArraySlice(*items->data(), entry, 1));
                appended = true;
                break;
            }
            if (!appended) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->AppendNull());
            }
        }
    }
    std::shared_ptr<arrow::Array> result;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(access_builder->Finish(&result));
    return result;
}

std::shared_ptr<Metrics> MapSharedShreddingFileReader::GetReaderMetrics() const {
    return reader_->GetReaderMetrics();
}

void MapSharedShreddingFileReader::Close() {
    reader_->Close();
}

Result<uint64_t> MapSharedShreddingFileReader::GetPreviousBatchFileRowId(
    uint64_t batch_row_id) const {
    return reader_->GetPreviousBatchFileRowId(batch_row_id);
}

Result<uint64_t> MapSharedShreddingFileReader::GetNumberOfRows() const {
    return reader_->GetNumberOfRows();
}

bool MapSharedShreddingFileReader::SupportPreciseBitmapSelection() const {
    return reader_->SupportPreciseBitmapSelection();
}

}  // namespace paimon
