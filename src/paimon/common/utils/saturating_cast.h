/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace paimon {

/// Converts a double to int32_t or int64_t with Java's float-to-int or float-to-long saturation
/// policy: NaN converts to 0 and an out-of-range value saturates at the bounds of TargetType.
/// Narrower Java integer conversions require a subsequent narrowing step and are not supported by
/// this helper. A bare static_cast of an unrepresentable double is undefined behavior and diverges
/// across architectures (x86 cvttsd2si yields the "integer indefinite" value, while aarch64 fcvtzs
/// saturates), so doubles that are not provably in range must go through this helper.
template <typename TargetType>
inline TargetType SaturatingDoubleToInteger(double value) {
    static_assert(std::is_same_v<TargetType, int32_t> || std::is_same_v<TargetType, int64_t>,
                  "TargetType must be int32_t or int64_t");
    if (std::isnan(value)) {
        return 0;
    }
    // Comparing against the bounds converted to double keeps the final truncation defined:
    // (double)INT64_MAX rounds up to 2^63, so every value that reaches the truncation is
    // representable in TargetType.
    if (value >= static_cast<double>(std::numeric_limits<TargetType>::max())) {
        return std::numeric_limits<TargetType>::max();
    }
    if (value <= static_cast<double>(std::numeric_limits<TargetType>::lowest())) {
        return std::numeric_limits<TargetType>::lowest();
    }
    return static_cast<TargetType>(value);
}

}  // namespace paimon
