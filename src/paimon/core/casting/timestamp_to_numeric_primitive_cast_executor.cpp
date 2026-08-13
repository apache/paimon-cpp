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

#include "paimon/core/casting/timestamp_to_numeric_primitive_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_base.h"
#include "arrow/type.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/core/casting/timestamp_to_timestamp_cast_executor.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
template <typename T>
class NumericBuilder;
}  // namespace arrow

namespace paimon {
Result<Literal> TimestampToNumericPrimitiveCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    return Status::Invalid("do not support cast literal from timestamp to numeric primitive");
}

Result<std::shared_ptr<arrow::Array>> TimestampToNumericPrimitiveCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    auto timestamp_type = checked_pointer_cast<arrow::TimestampType>(array->type());
    assert(timestamp_type);
    assert(target_type->id() == arrow::Type::type::INT32 ||
           target_type->id() == arrow::Type::type::INT64);
    auto timestamp_to_timestamp_cast_executor =
        std::make_shared<TimestampToTimestampCastExecutor>();
    auto target_array = array;
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    auto ts_with_sec_tz = arrow::timestamp(arrow::TimeUnit::SECOND, timezone);
    // 1. timestamp array cast to timestamp(second, tz)
    PAIMON_ASSIGN_OR_RAISE(target_array, timestamp_to_timestamp_cast_executor->Cast(
                                             target_array, ts_with_sec_tz, pool));
    // 2. timestamp(second, tz) array cast to int32/int64 array, as output integer indicates
    // second
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(target_array, target_array->View(arrow::int64()));
    if (target_type->id() == arrow::Type::type::INT32) {
        arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
        options.allow_int_overflow = true;
        PAIMON_ASSIGN_OR_RAISE(target_array,
                               CastingUtils::Cast(target_array, target_type, options, pool));
    }
    return target_array;
}

}  // namespace paimon
