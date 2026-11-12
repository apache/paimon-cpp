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

#include <memory>
#include <string>
#include <vector>

#include "paimon/realtime/index/realtime_index_reader.h"
#include "paimon/realtime/realtime_store.h"

namespace paimon {

/// One immutable query source. Readers and index readers must share its pinned data snapshot.
class PAIMON_EXPORT RealtimeIndexReadSource {
 public:
    virtual ~RealtimeIndexReadSource() = default;

    virtual OffsetRange GetOffsetRange() const = 0;

    /// Builds a candidate superset from offset bounds and pinned deletion metadata without
    /// reading data. Gaps may be included; index and data readers only return existing rows.
    /// Deleted offsets must be excluded before full-text or vector TopK executes.
    virtual Result<RoaringBitmap64> CreateCandidates() const = 0;

    /// Returns readers bound to this source's offset range. A missing field has no readers.
    /// Readers must not expose writes published after the pinned snapshot.
    virtual Result<std::vector<std::shared_ptr<RealtimeIndexReader>>> CreateIndexReaders(
        const std::string& field_name) const = 0;
};

/// Optional capability for precise real-time filtering and search. Ordinary query reads do not
/// require it. Each visible row in the requested range of the pinned snapshot
/// must belong to exactly one source. Source offset ranges must not overlap, so data readers
/// can use the same pinned view with source-local offset bounds.
class PAIMON_EXPORT RealtimeIndexedReadView : public RealtimeReadView {
 public:
    ~RealtimeIndexedReadView() override = default;

    /// Creates sources restricted to the intersection of each segment's range and the query range.
    /// @param offset_range Query bounds, which may be narrower than GetOffsetRange() to exclude
    /// offsets already covered by the disk snapshot selected after this view was pinned.
    virtual Result<std::vector<std::shared_ptr<RealtimeIndexReadSource>>> CreateIndexReadSources(
        const OffsetRange& offset_range) const = 0;
};

}  // namespace paimon
