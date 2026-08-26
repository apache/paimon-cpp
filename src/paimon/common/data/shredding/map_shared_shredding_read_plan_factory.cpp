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

#include "paimon/common/data/shredding/map_shared_shredding_read_plan_factory.h"

#include <set>
#include <string_view>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
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
            *overflow_array =
                checked_pointer_cast<arrow::MapArray>(physical_struct_array->field(i));
            continue;
        }
        (*physical_column_name_to_array)[sub_field->name()] = physical_struct_array->field(i);
    }
}

class MapShreddingColumnReadPlan : public ShreddingColumnReadPlan {
 public:
    MapShreddingColumnReadPlan(std::shared_ptr<arrow::Field> logical_field,
                               std::shared_ptr<arrow::Field> physical_field)
        : logical_field_(std::move(logical_field)), physical_field_(std::move(physical_field)) {}

    const std::shared_ptr<arrow::Field>& LogicalField() const override {
        return logical_field_;
    }

    const std::shared_ptr<arrow::Field>& PhysicalField() const override {
        return physical_field_;
    }

 private:
    std::shared_ptr<arrow::Field> logical_field_;
    std::shared_ptr<arrow::Field> physical_field_;
};

class FullMapReadPlan : public MapShreddingColumnReadPlan {
 public:
    FullMapReadPlan(const std::shared_ptr<arrow::Field>& logical_field,
                    const std::shared_ptr<arrow::Field>& physical_read_field,
                    std::vector<std::pair<std::string, int32_t>>&& selected_key_ids)
        : MapShreddingColumnReadPlan(logical_field, physical_read_field),
          selected_key_ids_(std::move(selected_key_ids)),
          logical_map_type_(checked_pointer_cast<arrow::MapType>(logical_field->type())) {}

    Result<std::shared_ptr<arrow::Array>> Assemble(
        const std::shared_ptr<arrow::Array>& physical_array,
        arrow::MemoryPool* arrow_pool) const override;

 private:
    std::vector<std::pair<std::string, int32_t>> selected_key_ids_;
    std::shared_ptr<arrow::MapType> logical_map_type_;
};

class SharedSelectedKeysReadPlan : public MapShreddingColumnReadPlan {
 public:
    struct SelectedKey {
        int32_t field_id = -1;
        std::vector<int32_t> candidate_columns;
        bool may_use_overflow = false;
    };

    SharedSelectedKeysReadPlan(const std::shared_ptr<arrow::Field>& logical_field,
                               const std::shared_ptr<arrow::Field>& physical_read_field,
                               std::vector<SelectedKey>&& selected_keys)
        : MapShreddingColumnReadPlan(logical_field, physical_read_field),
          selected_keys_(std::move(selected_keys)) {}

    Result<std::shared_ptr<arrow::Array>> Assemble(
        const std::shared_ptr<arrow::Array>& physical_array,
        arrow::MemoryPool* arrow_pool) const override;

 private:
    std::vector<SelectedKey> selected_keys_;
};

Result<std::shared_ptr<arrow::Array>> MaskSinglePhysicalColumn(
    const std::shared_ptr<arrow::StructArray>& physical_struct_array,
    const std::shared_ptr<arrow::ListArray>& field_mapping_array,
    const std::shared_ptr<arrow::Int32Array>& field_mapping_values,
    const std::shared_ptr<arrow::Array>& physical_column_array, int32_t physical_column_id,
    int32_t field_id, const std::string& field_name, arrow::MemoryPool* arrow_pool) {
    int64_t row_count = physical_struct_array->length();
    if (physical_column_array->length() != row_count) {
        return Status::Invalid("shared-shredding physical column length does not match row count");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> validity,
                                      arrow::AllocateEmptyBitmap(row_count, arrow_pool));
    int64_t valid_count = 0;
    for (int64_t row = 0; row < row_count; ++row) {
        if (physical_struct_array->IsNull(row)) {
            continue;
        }
        if (field_mapping_array->IsNull(row)) {
            return Status::Invalid(fmt::format(
                "__field_mapping cannot be null in non-null shared-shredding row for field {}",
                field_name));
        }
        int32_t mapping_offset = field_mapping_array->value_offset(row);
        int32_t mapping_length = field_mapping_array->value_length(row);
        if (physical_column_id < 0 || physical_column_id >= mapping_length) {
            return Status::Invalid("physical column id is out of __field_mapping range");
        }
        int32_t mapping_index = mapping_offset + physical_column_id;
        if (field_mapping_values->IsNull(mapping_index)) {
            return Status::Invalid("__field_mapping element cannot be null");
        }
        if (field_mapping_values->Value(mapping_index) != field_id ||
            physical_column_array->IsNull(row)) {
            continue;
        }
        arrow::bit_util::SetBit(validity->mutable_data(), row);
        ++valid_count;
    }

    // Replace only the top-level validity; offsets, values, and nested children stay shared.
    std::shared_ptr<arrow::ArrayData> result_data = physical_column_array->data()->Copy();
    if (result_data->buffers.empty()) {
        return Status::Invalid("shared-shredding physical column has no validity buffer slot");
    }
    int64_t null_count = row_count - valid_count;
    result_data->buffers[0] = null_count == 0 ? nullptr : std::move(validity);
    result_data->SetNullCount(null_count);
    return arrow::MakeArray(std::move(result_data));
}

class DefaultSelectedKeysReadPlan : public MapShreddingColumnReadPlan {
 public:
    DefaultSelectedKeysReadPlan(const std::shared_ptr<arrow::Field>& logical_field,
                                const std::shared_ptr<arrow::Field>& physical_read_field,
                                const std::vector<std::string>& selected_keys)
        : MapShreddingColumnReadPlan(logical_field, physical_read_field),
          selected_keys_(selected_keys) {}

    Result<std::shared_ptr<arrow::Array>> Assemble(
        const std::shared_ptr<arrow::Array>& physical_array,
        arrow::MemoryPool* arrow_pool) const override;

 private:
    std::vector<std::string> selected_keys_;
};

}  // namespace

Result<std::shared_ptr<ShreddingColumnReadPlan>>
MapSharedShreddingReadPlanFactory::CreateMapReadPlan(
    const std::shared_ptr<arrow::Field>& logical_map_field,
    const MapSharedShreddingFieldMeta& meta) {
    if (logical_map_field->type()->id() != arrow::Type::MAP) {
        return Status::Invalid(fmt::format("full MAP read plan requires MAP field {}, got {}",
                                           logical_map_field->name(),
                                           logical_map_field->type()->ToString()));
    }
    auto logical_map_type = checked_pointer_cast<arrow::MapType>(logical_map_field->type());
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
    std::shared_ptr<ShreddingColumnReadPlan> read_plan = std::make_shared<FullMapReadPlan>(
        logical_map_field, physical_read_field, ResolveSelectedKeyIds(meta, selected_keys));
    return read_plan;
}

Result<std::shared_ptr<ShreddingColumnReadPlan>>
MapSharedShreddingReadPlanFactory::CreateSharedSelectedKeysReadPlan(
    const std::shared_ptr<arrow::Field>& selected_keys_field,
    const MapSharedShreddingFieldMeta& meta) {
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::string> selected_keys,
        NestedProjectionUtils::ValidateMapSharedShreddingAccessField(selected_keys_field));
    auto selected_keys_type = checked_pointer_cast<arrow::StructType>(selected_keys_field->type());
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
    std::shared_ptr<ShreddingColumnReadPlan> read_plan =
        std::make_shared<SharedSelectedKeysReadPlan>(selected_keys_field, physical_read_field,
                                                     std::move(selected_key_plans));
    return read_plan;
}

Result<std::shared_ptr<ShreddingColumnReadPlan>>
MapSharedShreddingReadPlanFactory::CreateDefaultSelectedKeysReadPlan(
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
    std::shared_ptr<ShreddingColumnReadPlan> read_plan =
        std::make_shared<DefaultSelectedKeysReadPlan>(selected_keys_field, physical_read_field,
                                                      selected_keys);
    return read_plan;
}

Result<std::shared_ptr<arrow::Array>> FullMapReadPlan::Assemble(
    const std::shared_ptr<arrow::Array>& physical_array, arrow::MemoryPool* arrow_pool) const {
    if (!physical_array || physical_array->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid(fmt::format("cannot cast physical shredding field {} to StructArray",
                                           LogicalField()->name()));
    }
    auto physical_struct_array = checked_pointer_cast<arrow::StructArray>(physical_array);
    const std::string& shredding_field_name = LogicalField()->name();

    auto field_mapping =
        physical_struct_array->GetFieldByName(MapSharedShreddingDefine::kFieldMapping);
    if (!field_mapping || field_mapping->type_id() != arrow::Type::LIST) {
        return Status::Invalid(
            fmt::format("cannot find __field_mapping for field {}", shredding_field_name));
    }
    auto field_mapping_array = checked_pointer_cast<arrow::ListArray>(field_mapping);
    auto mapping_values = field_mapping_array->values();
    if (!mapping_values || mapping_values->type_id() != arrow::Type::INT32) {
        return Status::Invalid("__field_mapping values is not an Int32Array");
    }
    auto field_mapping_values = checked_pointer_cast<arrow::Int32Array>(mapping_values);

    std::map<std::string, std::shared_ptr<arrow::Array>> physical_column_name_to_array;
    std::shared_ptr<arrow::MapArray> overflow_array;
    CollectPhysicalColumns(physical_struct_array, &physical_column_name_to_array, &overflow_array);
    for (auto& [_, physical_column_array] : physical_column_name_to_array) {
        PAIMON_ASSIGN_OR_RAISE(
            physical_column_array,
            NestedProjectionUtils::AlignArrayToReadType(
                physical_column_array, logical_map_type_->item_type(), arrow_pool));
    }

    std::shared_ptr<arrow::Int32Array> overflow_keys;
    std::shared_ptr<arrow::Array> overflow_items;
    if (overflow_array) {
        auto overflow_key_array = overflow_array->keys();
        if (!overflow_key_array || overflow_key_array->type_id() != arrow::Type::INT32) {
            return Status::Invalid("__overflow map keys is not an Int32Array");
        }
        overflow_keys = checked_pointer_cast<arrow::Int32Array>(overflow_key_array);
        overflow_items = overflow_array->items();
        if (!overflow_items) {
            return Status::Invalid("__overflow map item array is null");
        }
        PAIMON_ASSIGN_OR_RAISE(overflow_items,
                               NestedProjectionUtils::AlignArrayToReadType(
                                   overflow_items, logical_map_type_->item_type(), arrow_pool));
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> map_builder_base,
                                      arrow::MakeBuilder(logical_map_type_, arrow_pool));
    if (!map_builder_base || !map_builder_base->type() ||
        map_builder_base->type()->id() != arrow::Type::MAP) {
        return Status::Invalid(
            fmt::format("cannot create MapBuilder for field {}", shredding_field_name));
    }
    auto* map_builder = checked_cast<arrow::MapBuilder*>(map_builder_base.get());
    auto* key_builder_base = map_builder->key_builder();
    if (!key_builder_base || !key_builder_base->type() ||
        key_builder_base->type()->id() != arrow::Type::STRING) {
        return Status::Invalid(fmt::format("map key builder is not a StringBuilder for field {}",
                                           shredding_field_name));
    }
    auto* key_builder = checked_cast<arrow::StringBuilder*>(key_builder_base);
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

Result<std::shared_ptr<arrow::Array>> SharedSelectedKeysReadPlan::Assemble(
    const std::shared_ptr<arrow::Array>& physical_array, arrow::MemoryPool* arrow_pool) const {
    if (!physical_array || physical_array->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid(fmt::format("cannot cast physical shredding field {} to StructArray",
                                           LogicalField()->name()));
    }
    auto physical_struct_array = checked_pointer_cast<arrow::StructArray>(physical_array);
    auto selected_keys_type = checked_pointer_cast<arrow::StructType>(LogicalField()->type());

    auto field_mapping =
        physical_struct_array->GetFieldByName(MapSharedShreddingDefine::kFieldMapping);
    if (!field_mapping || field_mapping->type_id() != arrow::Type::LIST) {
        return Status::Invalid(
            fmt::format("cannot find __field_mapping for field {}", LogicalField()->name()));
    }
    auto field_mapping_array = checked_pointer_cast<arrow::ListArray>(field_mapping);
    auto mapping_values = field_mapping_array->values();
    if (!mapping_values || mapping_values->type_id() != arrow::Type::INT32) {
        return Status::Invalid("__field_mapping values is not an Int32Array");
    }
    auto field_mapping_values = checked_pointer_cast<arrow::Int32Array>(mapping_values);

    std::shared_ptr<arrow::DataType> value_type = selected_keys_type->field(0)->type();
    std::map<std::string, std::shared_ptr<arrow::Array>> physical_column_name_to_array;
    std::shared_ptr<arrow::MapArray> overflow_array;
    CollectPhysicalColumns(physical_struct_array, &physical_column_name_to_array, &overflow_array);
    for (auto& [_, physical_column_array] : physical_column_name_to_array) {
        PAIMON_ASSIGN_OR_RAISE(physical_column_array,
                               NestedProjectionUtils::AlignArrayToReadType(physical_column_array,
                                                                           value_type, arrow_pool));
    }

    std::shared_ptr<arrow::Int32Array> overflow_keys;
    std::shared_ptr<arrow::Array> overflow_items;
    if (overflow_array) {
        auto overflow_key_array = overflow_array->keys();
        if (!overflow_key_array || overflow_key_array->type_id() != arrow::Type::INT32) {
            return Status::Invalid("__overflow map keys is not an Int32Array");
        }
        overflow_keys = checked_pointer_cast<arrow::Int32Array>(overflow_key_array);
        overflow_items = overflow_array->items();
        if (!overflow_items) {
            return Status::Invalid("__overflow map item array is null");
        }
        PAIMON_ASSIGN_OR_RAISE(overflow_items, NestedProjectionUtils::AlignArrayToReadType(
                                                   overflow_items, value_type, arrow_pool));
    }

    int64_t row_count = physical_struct_array->length();
    arrow::ArrayVector selected_key_arrays;
    selected_key_arrays.reserve(selected_keys_.size());
    for (int32_t key_index = 0; key_index < selected_keys_type->num_fields(); ++key_index) {
        const SelectedKey& selected_key = selected_keys_[key_index];
        if (selected_key.field_id < 0) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> null_array,
                arrow::MakeArrayOfNull(selected_keys_type->field(key_index)->type(), row_count,
                                       arrow_pool));
            selected_key_arrays.push_back(std::move(null_array));
            continue;
        }

        if (selected_key.candidate_columns.size() == 1 && !selected_key.may_use_overflow) {
            int32_t physical_column_id = selected_key.candidate_columns[0];
            std::string physical_column_name =
                MapSharedShreddingDefine::PhysicalColumnName(physical_column_id);
            auto physical_column_iter = physical_column_name_to_array.find(physical_column_name);
            if (physical_column_iter == physical_column_name_to_array.end()) {
                return Status::Invalid(
                    fmt::format("cannot find selected physical column {} for field {}",
                                physical_column_name, LogicalField()->name()));
            }
            const std::shared_ptr<arrow::Array>& physical_column_array =
                physical_column_iter->second;
            if (physical_column_array->offset() == 0) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<arrow::Array> masked_array,
                    MaskSinglePhysicalColumn(physical_struct_array, field_mapping_array,
                                             field_mapping_values, physical_column_array,
                                             physical_column_id, selected_key.field_id,
                                             LogicalField()->name(), arrow_pool));
                selected_key_arrays.push_back(std::move(masked_array));
                continue;
            } else {
                return Status::Invalid("paimon only supports arrays with zero offset");
            }
        }

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::unique_ptr<arrow::ArrayBuilder> value_builder,
            arrow::MakeBuilder(selected_keys_type->field(key_index)->type(), arrow_pool));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Reserve(row_count));
        for (int64_t row = 0; row < row_count; ++row) {
            if (physical_struct_array->IsNull(row)) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->AppendNull());
                continue;
            }
            if (field_mapping_array->IsNull(row)) {
                return Status::Invalid(fmt::format(
                    "__field_mapping cannot be null in non-null shared-shredding row for field {}",
                    LogicalField()->name()));
            }
            int32_t mapping_offset = field_mapping_array->value_offset(row);
            bool appended = false;
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
                const std::shared_ptr<arrow::Array>& physical_column_array =
                    physical_column_iter->second;
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    value_builder->AppendArraySlice(*physical_column_array->data(), row, 1));
                appended = true;
                break;
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
        std::shared_ptr<arrow::Array> selected_key_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(value_builder->Finish(&selected_key_array));
        selected_key_arrays.push_back(std::move(selected_key_array));
    }

    std::shared_ptr<arrow::Buffer> parent_validity;
    int64_t parent_null_count = physical_struct_array->null_count();
    if (parent_null_count > 0) {
        if (physical_struct_array->offset() == 0) {
            parent_validity = physical_struct_array->null_bitmap();
        } else {
            return Status::Invalid("paimon only supports arrays with zero offset");
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> result,
        arrow::StructArray::Make(selected_key_arrays, selected_keys_type->fields(),
                                 std::move(parent_validity), parent_null_count));
    return result;
}

Result<std::shared_ptr<arrow::Array>> DefaultSelectedKeysReadPlan::Assemble(
    const std::shared_ptr<arrow::Array>& physical_array, arrow::MemoryPool* arrow_pool) const {
    if (!physical_array || physical_array->type_id() != arrow::Type::MAP) {
        return Status::Invalid(
            fmt::format("cannot cast default-layout selected-key field {} to "
                        "MapArray",
                        LogicalField()->name()));
    }
    auto map_array = checked_pointer_cast<arrow::MapArray>(physical_array);
    auto selected_keys_type = checked_pointer_cast<arrow::StructType>(LogicalField()->type());

    std::shared_ptr<arrow::Array> items = map_array->items();
    PAIMON_ASSIGN_OR_RAISE(items, NestedProjectionUtils::AlignArrayToReadType(
                                      items, selected_keys_type->field(0)->type(), arrow_pool));
    std::shared_ptr<arrow::Array> keys = map_array->keys();
    std::unique_ptr<arrow::ArrayBuilder> access_builder_base;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(access_builder_base,
                                      arrow::MakeBuilder(LogicalField()->type(), arrow_pool));
    if (!access_builder_base || !access_builder_base->type() ||
        access_builder_base->type()->id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            fmt::format("selected-key MAP field {} is not a STRUCT", LogicalField()->name()));
    }
    auto* access_builder = checked_cast<arrow::StructBuilder*>(access_builder_base.get());
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

}  // namespace paimon
