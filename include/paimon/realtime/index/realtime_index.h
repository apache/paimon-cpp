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

#include <cstdint>
#include <memory>

#include "paimon/realtime/offset_range.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowArray;

namespace paimon {
class RealtimeIndexReader;

/// Append-only in-memory index for one field, supporting one writer and concurrent readers.
class PAIMON_EXPORT RealtimeIndex {
 public:
    virtual ~RealtimeIndex() = default;

    /// Consumes [_REALTIME_OFFSET, field] with increasing offsets.
    virtual Status AddBatch(::ArrowArray* batch) = 0;

    /// Creates a reader for [begin, end), sharing postings without copying.
    virtual Result<std::shared_ptr<RealtimeIndexReader>> CreateReader(
        const OffsetRange& offset_range) const = 0;

    virtual uint64_t GetMemoryUsage() const = 0;
};

}  // namespace paimon
