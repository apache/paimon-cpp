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

#pragma once

#include <memory>
#include <string>

#include "arrow/api.h"
#include "paimon/common/types/data_type.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/rapidjson_util.h"

namespace paimon {

/// Fixed-size VECTOR<T, N> logical type backed by Arrow FixedSizeList.
class VectorType : public DataType {
 public:
    static constexpr char TYPE[] = "VECTOR";

    VectorType(const std::shared_ptr<arrow::DataType>& type, bool nullable,
               const std::shared_ptr<const arrow::KeyValueMetadata>& metadata)
        : DataType(type, nullable, metadata) {}

    static bool IsValidElementType(const std::shared_ptr<arrow::DataType>& type) {
        switch (type->id()) {
            case arrow::Type::BOOL:
            case arrow::Type::INT8:
            case arrow::Type::INT16:
            case arrow::Type::INT32:
            case arrow::Type::INT64:
            case arrow::Type::FLOAT:
            case arrow::Type::DOUBLE:
                return true;
            default:
                return false;
        }
    }

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember(
            rapidjson::StringRef("type"),
            RapidJsonUtil::SerializeValue(WithNullable(std::string(TYPE)), allocator).Move(),
            *allocator);
        auto* type = checked_cast<arrow::FixedSizeListType*>(type_.get());
        auto value_field = type->value_field();
        std::shared_ptr<DataType> data_type =
            DataType::Create(value_field->type(), value_field->nullable(), value_field->metadata());
        obj.AddMember(rapidjson::StringRef("element"),
                      RapidJsonUtil::SerializeValue(*data_type, allocator).Move(), *allocator);
        obj.AddMember(rapidjson::StringRef("length"),
                      RapidJsonUtil::SerializeValue(type->list_size(), allocator).Move(),
                      *allocator);
        return obj;
    }
};

}  // namespace paimon
