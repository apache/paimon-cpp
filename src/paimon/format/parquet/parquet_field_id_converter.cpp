/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/format/parquet/parquet_field_id_converter.h"

#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/geospatial_type.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon::parquet {

using arrow::Field;
using arrow::KeyValueMetadata;

const char ParquetFieldIdConverter::PARQUET_FIELD_ID[] = "PARQUET:field_id";

Result<std::shared_ptr<arrow::Schema>> ParquetFieldIdConverter::AddParquetIdsFromPaimonIds(
    const std::shared_ptr<arrow::Schema>& schema) {
    // Match Java's Parquet IDs for LIST elements and MAP keys and values.
    auto mode = GeospatialType::ContainsGeospatial(schema->fields())
                    ? IdConvertType::PAIMON_TO_PARQUET_WITH_ELEMENT_IDS
                    : IdConvertType::PAIMON_TO_PARQUET_ID;
    std::vector<std::shared_ptr<Field>> new_fields;
    for (const auto& field : schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<Field> new_field,
                                          ProcessField(field, mode));
        new_fields.push_back(new_field);
    }
    return arrow::schema(new_fields, schema->metadata());
}

Result<std::shared_ptr<arrow::Schema>> ParquetFieldIdConverter::GetPaimonIdsFromParquetIds(
    const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<std::shared_ptr<Field>> new_fields;
    for (const auto& field : schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<Field> new_field,
            ProcessField(field, ParquetFieldIdConverter::IdConvertType::PARQUET_TO_PAIMON_ID));
        new_fields.push_back(new_field);
    }
    return arrow::schema(new_fields, schema->metadata());
}

arrow::Result<std::shared_ptr<const KeyValueMetadata>> ParquetFieldIdConverter::CopyId(
    const std::shared_ptr<const KeyValueMetadata>& metadata,
    ParquetFieldIdConverter::IdConvertType convert_type) {
    auto copy =
        [&](const std::string& from,
            const std::string& to) -> arrow::Result<std::shared_ptr<const KeyValueMetadata>> {
        if (!metadata || !metadata->Contains(from)) {
            return metadata;
        }
        ARROW_ASSIGN_OR_RAISE(auto paimon_id, metadata->Get(from));
        std::vector<std::string> keys = {to};
        std::vector<std::string> values = {paimon_id};
        auto new_meta = KeyValueMetadata::Make(keys, values);
        return metadata->Merge(*new_meta);
    };

    if (convert_type == IdConvertType::PAIMON_TO_PARQUET_ID ||
        convert_type == IdConvertType::PAIMON_TO_PARQUET_WITH_ELEMENT_IDS) {
        // in write process
        return copy(DataField::FIELD_ID, PARQUET_FIELD_ID);
    } else if (convert_type == ParquetFieldIdConverter::IdConvertType::PARQUET_TO_PAIMON_ID) {
        // in read process
        return copy(PARQUET_FIELD_ID, DataField::FIELD_ID);
    }
    return arrow::Status::Invalid("only support PAIMON_TO_PARQUET_ID and PARQUET_TO_PAIMON_ID");
}

arrow::Result<std::shared_ptr<Field>> ParquetFieldIdConverter::ProcessField(
    const std::shared_ptr<Field>& field, IdConvertType convert_type,
    std::optional<int32_t> owner_id, int32_t depth) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const KeyValueMetadata> updated_metadata,
                          CopyId(field->metadata(), convert_type));
    auto type = field->type();
    if (convert_type == IdConvertType::PAIMON_TO_PARQUET_WITH_ELEMENT_IDS && depth == 0 &&
        type->num_fields() && updated_metadata && updated_metadata->Contains(PARQUET_FIELD_ID)) {
        ARROW_ASSIGN_OR_RAISE(auto id, updated_metadata->Get(PARQUET_FIELD_ID));
        owner_id = StringUtils::StringToValue<int32_t>(id);
    }
    auto repeated_child = [&](const std::shared_ptr<Field>& child,
                              bool map_key = false) -> arrow::Result<std::shared_ptr<Field>> {
        ARROW_ASSIGN_OR_RAISE(auto result, ProcessField(child, convert_type, owner_id, depth + 1));
        if (!owner_id || (result->metadata() && result->metadata()->Contains(PARQUET_FIELD_ID))) {
            return result;
        }
        int32_t id = map_key ? SpecialFields::GetMapKeyFieldId(*owner_id, depth + 1)
                             : SpecialFields::GetArrayElementFieldId(*owner_id, depth + 1);
        return result->WithMergedMetadata(
            KeyValueMetadata::Make({PARQUET_FIELD_ID}, {std::to_string(id)}));
    };
    if (type->id() == arrow::Type::STRUCT) {
        auto struct_type = checked_pointer_cast<arrow::StructType>(type);
        std::vector<std::shared_ptr<Field>> new_fields;
        for (const auto& child : struct_type->fields()) {
            ARROW_ASSIGN_OR_RAISE(auto new_child, ProcessField(child, convert_type));
            new_fields.push_back(new_child);
        }
        auto new_type = arrow::struct_(new_fields);
        return field->WithType(new_type)->WithMergedMetadata(updated_metadata);
    } else if (type->id() == arrow::Type::LIST) {
        auto list_type = checked_pointer_cast<arrow::ListType>(type);
        ARROW_ASSIGN_OR_RAISE(auto new_value_field, repeated_child(list_type->value_field()));
        auto new_type = arrow::list(new_value_field);
        return field->WithType(new_type)->WithMergedMetadata(updated_metadata);
    } else if (type->id() == arrow::Type::FIXED_SIZE_LIST) {
        auto vector_type = checked_pointer_cast<arrow::FixedSizeListType>(type);
        ARROW_ASSIGN_OR_RAISE(auto new_value_field, repeated_child(vector_type->value_field()));
        auto new_type = arrow::fixed_size_list(new_value_field, vector_type->list_size());
        return field->WithType(new_type)->WithMergedMetadata(updated_metadata);
    } else if (type->id() == arrow::Type::MAP) {
        auto map_type = checked_pointer_cast<arrow::MapType>(type);
        ARROW_ASSIGN_OR_RAISE(auto new_key_field, repeated_child(map_type->key_field(), true));
        ARROW_ASSIGN_OR_RAISE(auto new_item_field, repeated_child(map_type->item_field()));
        auto new_type = std::make_shared<arrow::MapType>(new_key_field, new_item_field,
                                                         map_type->keys_sorted());
        return field->WithType(new_type)->WithMergedMetadata(updated_metadata);
    }

    return field->WithMergedMetadata(updated_metadata);
}

}  // namespace paimon::parquet
