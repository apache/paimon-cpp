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

#include "paimon/core/casting/time_to_string_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <string>

#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
Result<Literal> TimeToStringCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::TIME);
    if (literal.IsNull()) {
        return Literal(FieldType::STRING);
    }
    std::string time = DateTimeUtils::FormatTimeMillis(literal.GetValue<int32_t>(), precision_);
    return Literal(FieldType::STRING, time.data(), time.size());
}

Result<std::shared_ptr<arrow::Array>> TimeToStringCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    if (!array->type()->Equals(*arrow::time32(arrow::TimeUnit::MILLI))) {
        return Status::Invalid(
            fmt::format("Only millisecond TIME is supported: {}", array->type()->ToString()));
    }
    const auto& time_array = checked_cast<const arrow::Time32Array&>(*array);
    arrow::StringBuilder string_builder(pool);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(string_builder.Reserve(time_array.length()));
    for (int64_t i = 0; i < time_array.length(); ++i) {
        if (time_array.IsNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(string_builder.AppendNull());
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(string_builder.Append(
                DateTimeUtils::FormatTimeMillis(time_array.Value(i), precision_)));
        }
    }
    std::shared_ptr<arrow::Array> casted_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(string_builder.Finish(&casted_array));
    return casted_array;
}

}  // namespace paimon
