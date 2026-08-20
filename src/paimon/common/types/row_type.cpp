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

#include "paimon/common/types/row_type.h"

#include <cassert>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

RowType::RowType(const std::shared_ptr<arrow::DataType>& type, bool nullable,
                 const std::shared_ptr<const arrow::KeyValueMetadata>& metadata)
    : DataType(type, nullable, metadata) {}

rapidjson::Value RowType::ToJson(rapidjson::Document::AllocatorType* allocator) const
    noexcept(false) {
    rapidjson::Value obj(rapidjson::kObjectType);
    obj.AddMember(rapidjson::StringRef("type"),
                  RapidJsonUtil::SerializeValue(WithNullable(TYPE), allocator).Move(), *allocator);
    if (!type_ || type_->id() != arrow::Type::STRUCT) {
        throw std::invalid_argument("type failed to cast to StructType");
    }
    auto type = checked_cast<arrow::StructType*>(type_.get());

    std::vector<DataField> fields;
    for (const auto& field : type->fields()) {
        std::optional<std::string> description;
        int32_t field_id = -1;
        if (field->HasMetadata() && field->metadata()) {
            if (field->metadata()->Contains(DataField::FIELD_ID)) {
                auto field_id_result = field->metadata()->Get(DataField::FIELD_ID);
                if (!field_id_result.ok()) {
                    throw std::invalid_argument("get FIELD_ID from meta data failed");
                } else {
                    std::optional<int32_t> id =
                        StringUtils::StringToValue<int32_t>(field_id_result.ValueUnsafe());
                    if (id != std::nullopt) {
                        field_id = id.value();
                    } else {
                        assert(false);
                    }
                }
            }
            auto description_result = field->metadata()->Get(DataField::DESCRIPTION);
            if (description_result.ok()) {
                description = description_result.ValueUnsafe();
            }
        }
        fields.emplace_back(field_id, field, description);
    }

    obj.AddMember(rapidjson::StringRef("fields"),
                  RapidJsonUtil::SerializeValue(fields, allocator).Move(), *allocator);
    return obj;
}

}  // namespace paimon
