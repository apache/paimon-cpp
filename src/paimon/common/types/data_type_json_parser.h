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
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/result.h"
#include "rapidjson/document.h"

namespace paimon {
class DataTypeJsonParser {
 public:
    DataTypeJsonParser() = delete;
    ~DataTypeJsonParser() = delete;

    /// Parses a data type from a JSON value and returns an Arrow field representation.
    ///
    /// @param name The name of the field.
    /// @param type_json_value The JSON value representing the type.
    /// @return A Result containing the parsed Arrow field, or an error status if parsing fails.
    static Result<std::shared_ptr<arrow::Field>> ParseType(const std::string& name,
                                                           const rapidjson::Value& type_json_value);

 private:
    static Result<std::shared_ptr<arrow::Field>> ParseAtomicTypeField(
        const std::string& name, const rapidjson::Value& type_json_value);
    static Result<std::shared_ptr<arrow::Field>> ParseComplexTypeField(
        const std::string& name, const rapidjson::Value& type_json_value);

    static Result<std::shared_ptr<arrow::Field>> ParseArrayType(
        const std::string& name, const rapidjson::Value& type_json_value, bool nullable);
    static Result<std::shared_ptr<arrow::Field>> ParseVectorType(
        const std::string& name, const rapidjson::Value& type_json_value, bool nullable);
    static Result<std::shared_ptr<arrow::Field>> ParseMapType(
        const std::string& name, const rapidjson::Value& type_json_value, bool nullable);
    static Result<std::shared_ptr<arrow::Field>> ParseRowType(
        const std::string& name, const rapidjson::Value& type_json_value, bool nullable);
};

}  // namespace paimon
