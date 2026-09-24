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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "arrow/api.h"
#include "paimon/common/types/data_field.h"
#include "paimon/result.h"
#include "rapidjson/document.h"

namespace paimon {
class DataTypeJsonParser {
 public:
    DataTypeJsonParser() = delete;
    ~DataTypeJsonParser() = delete;

    /// Parses JSON into an Arrow field. If all ROW fields omit 'id', assigns IDs in preorder
    /// starting at 0. Preserves explicit IDs and rejects partially specified IDs.
    ///
    /// @param name The name of the field.
    /// @param type_json_value The JSON value representing the type.
    /// @return A Result containing the parsed Arrow field, or an error status if parsing fails.
    static Result<std::shared_ptr<arrow::Field>> ParseType(const std::string& name,
                                                           const rapidjson::Value& type_json_value);

    /// Parses a schema field, requiring explicit IDs on the field and all nested ROW fields.
    ///
    /// @param field_json_value The JSON value representing the field.
    /// @return A Result containing the parsed DataField, or an error status if parsing fails.
    static Result<DataField> ParseDataField(const rapidjson::Value& field_json_value);

 private:
    /// Shares ID assignment across a type tree and rejects partially specified IDs.
    class FieldIdAssigner {
     public:
        Result<int32_t> Assign(const std::optional<int32_t>& explicit_id);

     private:
        int32_t next_generated_id_ = 0;
        bool has_explicit_id_ = false;
    };

    /// Reuses the type tree's ID assigner; nullptr requires explicit IDs on all ROW fields.
    static Result<std::shared_ptr<arrow::Field>> ParseType(const std::string& name,
                                                           const rapidjson::Value& type_json_value,
                                                           FieldIdAssigner* field_id_assigner);

    static Result<DataField> ParseDataField(const rapidjson::Value& field_json_value,
                                            FieldIdAssigner* field_id_assigner);

    static Result<std::shared_ptr<arrow::Field>> ParseAtomicTypeField(
        const std::string& name, const rapidjson::Value& type_json_value);
    static Result<std::shared_ptr<arrow::Field>> ParseComplexTypeField(
        const std::string& name, const rapidjson::Value& type_json_value,
        FieldIdAssigner* field_id_assigner);

    static Result<std::shared_ptr<arrow::Field>> ParseArrayType(
        const std::string& name, const rapidjson::Value& type_json_value, bool nullable,
        FieldIdAssigner* field_id_assigner);
    static Result<std::shared_ptr<arrow::Field>> ParseVectorType(
        const std::string& name, const rapidjson::Value& type_json_value, bool nullable,
        FieldIdAssigner* field_id_assigner);
    static Result<std::shared_ptr<arrow::Field>> ParseMapType(
        const std::string& name, const rapidjson::Value& type_json_value, bool nullable,
        FieldIdAssigner* field_id_assigner);
    static Result<std::shared_ptr<arrow::Field>> ParseRowType(
        const std::string& name, const rapidjson::Value& type_json_value, bool nullable,
        FieldIdAssigner* field_id_assigner);
};

}  // namespace paimon
