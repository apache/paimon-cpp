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

#include "paimon/core/casting/numeric_primitive_to_timestamp_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <limits>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_dict.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/core/casting/timestamp_to_timestamp_cast_executor.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
template <typename TYPE>
class NumericArray;
}  // namespace arrow

namespace paimon {
Result<Literal> NumericPrimitiveToTimestampCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    return Status::Invalid("do not support cast literal from numeric primitive to timestamp");
}

Result<std::shared_ptr<arrow::Array>> NumericPrimitiveToTimestampCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    auto src_type = array->type();
    assert(src_type->id() == arrow::Type::type::INT32 ||
           src_type->id() == arrow::Type::type::INT64);
    auto target_array = array;
    // 1. int32/int64 array to int64 array
    if (src_type->id() == arrow::Type::type::INT32) {
        arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
        PAIMON_ASSIGN_OR_RAISE(target_array,
                               CastingUtils::Cast(target_array, arrow::int64(), options, pool));
    }
    // 2. int64 array to timestamp(second, tz) array
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    auto ts_second_tz = arrow::timestamp(arrow::TimeUnit::SECOND, timezone);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(target_array, target_array->View(ts_second_tz));
    // 3. timestamp(second, tz) array to target ts array
    auto timestamp_to_timestamp_cast_executor =
        std::make_shared<TimestampToTimestampCastExecutor>();
    PAIMON_ASSIGN_OR_RAISE(
        target_array, timestamp_to_timestamp_cast_executor->Cast(target_array, target_type, pool));
    return target_array;
}

}  // namespace paimon
