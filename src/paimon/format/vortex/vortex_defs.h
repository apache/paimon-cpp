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

namespace paimon::vortex {

inline constexpr char kVortexReadInPushdownMaxValues[] = "vortex.read.in-pushdown.max-values";
inline constexpr uint32_t kDefaultVortexReadInPushdownMaxValues = 32;

/// Limits applied independently to each IN or NOT IN predicate.
struct VortexPredicateOptions {
    /// Maximum number of input literals, including duplicates; zero disables membership pushdown.
    uint32_t max_in_values = kDefaultVortexReadInPushdownMaxValues;
};

inline constexpr char kVortexReadIoCoalesceMaxGap[] = "vortex.read.io-coalesce.max-gap";
inline constexpr char kVortexReadIoCoalesceMaxSize[] = "vortex.read.io-coalesce.max-size";

// Preserve Vortex's object-storage defaults when no explicit values are supplied.
inline constexpr uint64_t kDefaultVortexReadIoCoalesceMaxGap = 1ULL << 20;
inline constexpr uint64_t kDefaultVortexReadIoCoalesceMaxSize = 16ULL << 20;

/// I/O range coalescing limits, measured in bytes.
struct VortexIoCoalesceOptions {
    /// Maximum gap between ranges considered for merging.
    uint64_t max_gap = kDefaultVortexReadIoCoalesceMaxGap;
    /// Maximum merged span; a single original request may exceed this threshold.
    uint64_t max_size = kDefaultVortexReadIoCoalesceMaxSize;
};

/// Names for bitmap-selection counters set after a successful read-schema update.
class VortexMetrics {
 public:
    VortexMetrics() = delete;
    ~VortexMetrics() = delete;

    /// Rows selected by the bitmap, before predicate filtering; no bitmap selects all rows.
    static inline const char kSelectionRowCount[] = "vortex.selection-row-count";
    /// Stored selected or excluded row ids, using whichever side is smaller.
    static inline const char kSelectionBufferRowCount[] = "vortex.selection-buffer-row-count";
};

}  // namespace paimon::vortex
