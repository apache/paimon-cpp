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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/result.h"

namespace paimon {
class Bytes;
class InternalRow;
class MemoryPool;
class MemorySegment;

/// Live manifest entries retained for one bucket across multiple snapshots.
///
/// Snapshot retention wraps the same indexed rows used by the manifest-file cache.
/// This value object owns merged live manifest entries by snapshot id. It does not own or access a
/// cache; callers are responsible for storing the serialized bytes in the cache layer.
class SnapshotLiveManifestEntries {
 public:
    struct Selection {
        std::vector<ManifestEntry> entries;
        uint64_t candidate_entries = 0;
    };

    explicit SnapshotLiveManifestEntries(int32_t max_snapshots);

    void Put(int64_t snapshot_id, std::vector<ManifestEntry>&& entries);
    size_t Size() const;

    Result<std::shared_ptr<Bytes>> Serialize(const std::shared_ptr<MemoryPool>& pool) const;
    static Result<SnapshotLiveManifestEntries> Deserialize(const MemorySegment& segment,
                                                           int32_t max_snapshots,
                                                           const std::shared_ptr<MemoryPool>& pool);

    /// Select an exact retained snapshot, applying partition and row filters before constructing
    /// file metadata. A partition filter runs once per partition in the indexed snapshot.
    /// The reader validates the entry version before invoking the filter, as in
    /// ManifestFile::ReadSelectedEntries. The serialized cache value is unchanged. A Selection is
    /// deliberately not a cache value: query-filtered entries must never replace the complete
    /// bucket's cached entries.
    static Result<std::optional<Selection>> ReadSnapshot(
        const MemorySegment& segment, int64_t snapshot_id, int32_t max_snapshots,
        const std::function<Result<bool>(const InternalRow&)>& row_filter,
        const std::shared_ptr<MemoryPool>& pool,
        const std::function<Result<bool>(const BinaryRow&)>& partition_filter = nullptr);

 private:
    void EvictIfNeeded();

    std::map<int64_t, std::shared_ptr<const std::vector<ManifestEntry>>> entries_by_snapshot_;
    int32_t max_snapshots_;
};

}  // namespace paimon
