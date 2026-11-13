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

#include "paimon/core/casting/string_to_time_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <string>

#include "arrow/array/array_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
Result<Literal> StringToTimeCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::STRING);
    if (!target_type->Equals(*arrow::time32(arrow::TimeUnit::MILLI))) {
        return Status::Invalid(
            fmt::format("Only millisecond TIME is supported: {}", target_type->ToString()));
    }
    if (literal.IsNull()) {
        return Literal(FieldType::TIME);
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t time,
                           StringUtils::StringToTime(literal.GetValue<std::string>()));
    return Literal(FieldType::TIME, time);
}

Result<std::shared_ptr<arrow::Array>> StringToTimeCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    if (!target_type->Equals(*arrow::time32(arrow::TimeUnit::MILLI))) {
        return Status::Invalid(
            fmt::format("Only millisecond TIME is supported: {}", target_type->ToString()));
    }
    auto* string_array = checked_cast<arrow::StringArray*>(array.get());
    arrow::Time32Builder time_builder(target_type, pool);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(time_builder.Reserve(string_array->length()));
    for (int64_t i = 0; i < string_array->length(); ++i) {
        if (string_array->IsNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(time_builder.AppendNull());
        } else {
            PAIMON_ASSIGN_OR_RAISE(int32_t time,
                                   StringUtils::StringToTime(string_array->GetString(i)));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(time_builder.Append(time));
        }
    }
    std::shared_ptr<arrow::Array> casted_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(time_builder.Finish(&casted_array));
    return casted_array;
}

}  // namespace paimon
