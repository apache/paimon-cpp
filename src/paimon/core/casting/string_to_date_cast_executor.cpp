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

#include "paimon/core/casting/string_to_date_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <string>
#include <utility>

#include "arrow/array/array_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/type.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/defs.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
Result<Literal> StringToDateCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::STRING);
    PAIMON_ASSIGN_OR_RAISE(FieldType target_field_type,
                           FieldTypeUtils::ConvertToFieldType(target_type->id()));
    assert(target_field_type == FieldType::DATE);
    if (literal.IsNull()) {
        return Literal(target_field_type);
    }
    auto value = literal.GetValue<std::string>();
    PAIMON_ASSIGN_OR_RAISE(int32_t date_value, StringUtils::StringToDate(value));
    return Literal(FieldType::DATE, date_value);
}

Result<std::shared_ptr<arrow::Array>> StringToDateCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    auto* string_array = checked_cast<arrow::StringArray*>(array.get());
    auto date_builder = std::make_shared<arrow::Date32Builder>(pool);
    for (int64_t i = 0; i < string_array->length(); ++i) {
        if (string_array->IsNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(date_builder->AppendNull());
        } else {
            PAIMON_ASSIGN_OR_RAISE(int32_t date_value,
                                   StringUtils::StringToDate(string_array->GetString(i)));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(date_builder->Append(date_value));
        }
    }
    std::shared_ptr<arrow::Array> casted_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(date_builder->Finish(&casted_array));
    return casted_array;
}

}  // namespace paimon
