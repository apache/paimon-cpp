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

#include "paimon/core/casting/time_to_timestamp_cast_executor.h"

#include <cassert>
#include <cstdint>

#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
Result<Literal> TimeToTimestampCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::TIME);
    if (target_type->id() != arrow::Type::type::TIMESTAMP) {
        return Status::Invalid(
            fmt::format("cannot cast TIME literal to {}", target_type->ToString()));
    }
    if (literal.IsNull()) {
        return Literal(FieldType::TIMESTAMP);
    }
    const auto& target_ts_type = checked_cast<const arrow::TimestampType&>(*target_type);
    int64_t millis = literal.GetValue<int32_t>();
    if (target_ts_type.unit() == arrow::TimeUnit::SECOND) {
        // Like the array cast, a second-precision target drops the milliseconds.
        millis = millis / 1000 * 1000;
    }
    Timestamp timestamp = Timestamp::FromEpochMillis(millis);
    if (target_ts_type.timezone().empty()) {
        return Literal(timestamp);
    }
    // Like Java, a TIMESTAMP_LTZ value is the time on 1970-01-01 in the local time zone.
    PAIMON_ASSIGN_OR_RAISE(Timestamp utc_timestamp, DateTimeUtils::ToUTCTimestamp(timestamp));
    return Literal(utc_timestamp);
}

Result<std::shared_ptr<arrow::Array>> TimeToTimestampCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    if (!array->type()->Equals(*arrow::time32(arrow::TimeUnit::MILLI))) {
        return Status::Invalid(
            fmt::format("Only millisecond TIME is supported: {}", array->type()->ToString()));
    }
    const auto& time_array = checked_cast<const arrow::Time32Array&>(*array);
    auto target_ts_type = checked_pointer_cast<arrow::TimestampType>(target_type);
    const int64_t units_per_second =
        DateTimeUtils::CONVERSION_FACTORS[static_cast<int32_t>(target_ts_type->unit())];
    // Like Java, the time of day becomes that time on 1970-01-01, in the local time zone for a
    // TIMESTAMP_LTZ target.
    auto local_type = std::make_shared<arrow::TimestampType>(target_ts_type->unit());
    arrow::TimestampBuilder ts_builder(local_type, pool);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(ts_builder.Reserve(time_array.length()));
    for (int64_t i = 0; i < time_array.length(); ++i) {
        if (time_array.IsNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(ts_builder.AppendNull());
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(ts_builder.Append(
                static_cast<int64_t>(time_array.Value(i)) * units_per_second / 1000));
        }
    }
    std::shared_ptr<arrow::Array> local_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(ts_builder.Finish(&local_array));
    if (target_ts_type->timezone().empty()) {
        return local_array;
    }
    return CastingUtils::TimestampToTimestampWithTimezone(local_array, target_ts_type, pool);
}

}  // namespace paimon
