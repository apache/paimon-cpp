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

#include "paimon/core/casting/timestamp_to_time_cast_executor.h"

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
Result<Literal> TimestampToTimeCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::TIMESTAMP);
    if (!target_type->Equals(*arrow::time32(arrow::TimeUnit::MILLI))) {
        return Status::Invalid(
            fmt::format("Only millisecond TIME is supported: {}", target_type->ToString()));
    }
    if (literal.IsNull()) {
        return Literal(FieldType::TIME);
    }
    auto timestamp = literal.GetValue<Timestamp>();
    if (src_with_timezone_) {
        // Like Java, a TIMESTAMP_LTZ value gives its time of day in the local time zone.
        PAIMON_ASSIGN_OR_RAISE(timestamp, DateTimeUtils::ToLocalTimestamp(timestamp));
    }
    // Like Java, the time is the timestamp floor-modulo one day, in milliseconds.
    int64_t millis_of_day = timestamp.GetMillisecond() % DateTimeUtils::MILLIS_PER_DAY;
    if (millis_of_day < 0) {
        millis_of_day += DateTimeUtils::MILLIS_PER_DAY;
    }
    return Literal(FieldType::TIME, static_cast<int32_t>(millis_of_day));
}

Result<std::shared_ptr<arrow::Array>> TimestampToTimeCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    if (!target_type->Equals(*arrow::time32(arrow::TimeUnit::MILLI))) {
        return Status::Invalid(
            fmt::format("Only millisecond TIME is supported: {}", target_type->ToString()));
    }
    auto src_ts_type = checked_pointer_cast<arrow::TimestampType>(array->type());
    std::shared_ptr<arrow::Array> local_array = array;
    if (!src_ts_type->timezone().empty()) {
        auto local_type = std::make_shared<arrow::TimestampType>(src_ts_type->unit());
        PAIMON_ASSIGN_OR_RAISE(local_array, CastingUtils::TimestampWithTimezoneToTimestamp(
                                                local_array, local_type, pool));
    }
    const auto& ts_array = checked_cast<const arrow::TimestampArray&>(*local_array);
    const int64_t units_per_second =
        DateTimeUtils::CONVERSION_FACTORS[static_cast<int32_t>(src_ts_type->unit())];
    const int64_t units_per_day = DateTimeUtils::SECONDS_PER_DAY * units_per_second;
    arrow::Time32Builder time_builder(target_type, pool);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(time_builder.Reserve(ts_array.length()));
    for (int64_t i = 0; i < ts_array.length(); ++i) {
        if (ts_array.IsNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(time_builder.AppendNull());
            continue;
        }
        int64_t units_of_day = ts_array.Value(i) % units_per_day;
        if (units_of_day < 0) {
            units_of_day += units_per_day;
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            time_builder.Append(static_cast<int32_t>(units_of_day * 1000 / units_per_second)));
    }
    std::shared_ptr<arrow::Array> casted_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(time_builder.Finish(&casted_array));
    return casted_array;
}

}  // namespace paimon
