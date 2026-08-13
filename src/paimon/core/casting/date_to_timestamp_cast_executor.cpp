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

#include "paimon/core/casting/date_to_timestamp_cast_executor.h"

#include <cassert>
#include <string>
#include <utility>

#include "arrow/compute/cast.h"
#include "arrow/type.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {

Result<Literal> DateToTimestampCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    return Status::Invalid("do not support cast literal from date to timestamp");
}

Result<std::shared_ptr<arrow::Array>> DateToTimestampCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    auto target_ts_type = checked_pointer_cast<arrow::TimestampType>(target_type);
    assert(target_ts_type);
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();

    auto target_ts_type_no_tz = arrow::timestamp(target_ts_type->unit());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> target_array,
                           CastingUtils::Cast(array, target_ts_type_no_tz, options, pool));
    if (target_ts_type->timezone().empty()) {
        return target_array;
    }
    // handle timezone
    return CastingUtils::TimestampToTimestampWithTimezone(target_array, target_ts_type, pool);
}

}  // namespace paimon
