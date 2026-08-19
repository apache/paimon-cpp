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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>

#include "paimon/visibility.h"

namespace paimon {

/// A left-closed, right-open offset range `[begin, end)`.
struct PAIMON_EXPORT OffsetRange {
    OffsetRange(int64_t begin, int64_t end) : begin(begin), end(end) {}

    /// Returns the number of offsets covered by this range.
    int64_t Count() const {
        return end - begin;
    }

    /// Returns whether this range contains no offsets.
    bool Empty() const {
        return begin == end;
    }

    bool operator==(const OffsetRange& other) const {
        return begin == other.begin && end == other.end;
    }

    bool operator!=(const OffsetRange& other) const {
        return !(*this == other);
    }

    /// Inclusive first offset.
    int64_t begin;
    /// Exclusive end offset.
    int64_t end;
};

}  // namespace paimon
