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

#include "arrow/array/array_base.h"
#include "paimon/core/casting/cast_executor.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
class MemoryPool;
}  // namespace arrow

namespace paimon {
/// Casts TIMESTAMP and TIMESTAMP_LTZ to TIME like Java `TimestampToTimeCastRule`.
class TimestampToTimeCastExecutor : public CastExecutor {
 public:
    /// @param src_with_timezone Whether the source is a TIMESTAMP_LTZ. A literal does not carry
    ///                          its type, so a TIMESTAMP_LTZ literal is cast in the local time
    ///                          zone only when this is set. The array cast reads the source time
    ///                          zone from the array type instead.
    explicit TimestampToTimeCastExecutor(bool src_with_timezone = false)
        : src_with_timezone_(src_with_timezone) {}

    Result<Literal> Cast(const Literal& literal,
                         const std::shared_ptr<arrow::DataType>& target_type) const override;

    Result<std::shared_ptr<arrow::Array>> Cast(const std::shared_ptr<arrow::Array>& array,
                                               const std::shared_ptr<arrow::DataType>& target_type,
                                               arrow::MemoryPool* pool) const override;

 private:
    bool src_with_timezone_;
};

}  // namespace paimon
