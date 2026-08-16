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

#include "paimon/core/casting/timestamp_to_date_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <utility>

#include "arrow/compute/cast.h"
#include "arrow/type.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
Result<Literal> TimestampToDateCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    return Status::Invalid("do not support cast literal from timestamp to date");
}

Result<std::shared_ptr<arrow::Array>> TimestampToDateCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    auto target_array = array;
    auto src_ts_type = checked_pointer_cast<arrow::TimestampType>(array->type());
    if (!src_ts_type->timezone().empty()) {
        auto target_type_no_tz = std::make_shared<arrow::TimestampType>(src_ts_type->unit());
        PAIMON_ASSIGN_OR_RAISE(target_array, CastingUtils::TimestampWithTimezoneToTimestamp(
                                                 target_array, target_type_no_tz, pool));
    }
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    return CastingUtils::Cast(target_array, target_type, options, pool);
}

}  // namespace paimon
