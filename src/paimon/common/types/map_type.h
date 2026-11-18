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

class MapType : public DataType {
 public:
    static constexpr char TYPE[] = "MAP";
    // Java allows nullable MAP keys in table schemas. Arrow requires physical keys to be non-null.
    static constexpr char KEY_NULLABLE[] = "paimon.map_key_nullable";

    static bool HasNullableKey(const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
        int32_t index = metadata ? metadata->FindKey(KEY_NULLABLE) : -1;
        return index >= 0 && metadata->value(index) == "true";
    }

    MapType(const std::shared_ptr<arrow::DataType>& type, bool nullable,
            const std::shared_ptr<const arrow::KeyValueMetadata>& metadata)
        : DataType(type, nullable, metadata) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember(
            rapidjson::StringRef("type"),
            RapidJsonUtil::SerializeValue(WithNullable(std::string(TYPE)), allocator).Move(),
            *allocator);
        auto type = checked_cast<arrow::MapType*>(type_.get());
        auto key_field = type->key_field();
        bool key_nullable = key_field->nullable() || HasNullableKey(metadata_);
        // The key and value metadata is load-bearing: it is what marks an extension type such as
        // VARIANT.
        std::shared_ptr<DataType> key_data_type =
            DataType::Create(key_field->type(), key_nullable, key_field->metadata());
        obj.AddMember(rapidjson::StringRef("key"),
                      RapidJsonUtil::SerializeValue(*key_data_type, allocator).Move(), *allocator);
        auto value_field = type->item_field();
        std::shared_ptr<DataType> value_data_type =
            DataType::Create(value_field->type(), value_field->nullable(), value_field->metadata());
        obj.AddMember(rapidjson::StringRef("value"),
                      RapidJsonUtil::SerializeValue(*value_data_type, allocator).Move(),
                      *allocator);
        return obj;
    }
};

}  // namespace paimon
