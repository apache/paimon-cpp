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

#include "paimon/core/manifest/snapshot_live_manifest_entries.h"

#include <algorithm>
#include <utility>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/core/manifest/indexed_manifest_entries.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"

namespace paimon {
namespace {
constexpr int32_t kMagic = 0x534d4501;  // Indexed snapshot cache format version 1

size_t NormalizeMaxSnapshots(int32_t max_snapshots) {
    return static_cast<size_t>(std::max(0, max_snapshots));
}

// Length-delimited snapshots let an exact hit skip other snapshots without visiting their rows.
Status VisitSnapshots(const MemorySegment& segment, int32_t max_snapshots,
                      const std::function<Status(int64_t, const MemorySegment&)>& visitor) {
    auto stream = std::make_shared<ByteArrayInputStream>(segment.Data(), segment.Size());
    DataInputStream in(stream);
    PAIMON_ASSIGN_OR_RAISE(int32_t magic, in.ReadValue<int32_t>());
    if (magic != kMagic) {
        return Status::Invalid("invalid snapshot live manifest entries magic");
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t snapshot_count, in.ReadValue<int32_t>());
    if (snapshot_count < 0) {
        return Status::Invalid("snapshot live manifest entries snapshot count is negative");
    }
    std::optional<int64_t> previous_id;
    for (int32_t i = 0; i < snapshot_count; ++i) {
        PAIMON_ASSIGN_OR_RAISE(int64_t id, in.ReadValue<int64_t>());
        if (previous_id && id <= *previous_id) {
            return Status::Invalid("snapshot live manifest entries are not ordered");
        }
        previous_id = id;
        PAIMON_ASSIGN_OR_RAISE(int32_t length, in.ReadValue<int32_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
        if (length < 0 || length > segment.Size() - pos) {
            return Status::Invalid("invalid snapshot live manifest entries block length");
        }
        if (i >= snapshot_count - std::max(0, max_snapshots)) {
            PAIMON_RETURN_NOT_OK(
                visitor(id, MemorySegment::WrapView(segment.Data() + pos, length)));
        }
        PAIMON_RETURN_NOT_OK(in.Seek(pos + length));
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t end, in.GetPos());
    if (end != segment.Size()) {
        return Status::Invalid("trailing snapshot live manifest entries bytes");
    }
    return Status::OK();
}
}  // namespace

SnapshotLiveManifestEntries::SnapshotLiveManifestEntries(int32_t max_snapshots)
    : max_snapshots_(max_snapshots) {}

void SnapshotLiveManifestEntries::Put(int64_t snapshot_id, std::vector<ManifestEntry>&& entries) {
    if (NormalizeMaxSnapshots(max_snapshots_) == 0) {
        return;
    }
    entries_by_snapshot_[snapshot_id] =
        std::make_shared<const std::vector<ManifestEntry>>(std::move(entries));
    EvictIfNeeded();
}

size_t SnapshotLiveManifestEntries::Size() const {
    return entries_by_snapshot_.size();
}

Result<std::shared_ptr<Bytes>> SnapshotLiveManifestEntries::Serialize(
    const std::shared_ptr<MemoryPool>& pool) const {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.WriteValue<int32_t>(kMagic);
    out.WriteValue<int32_t>(entries_by_snapshot_.size());
    for (const auto& [snapshot_id, entries] : entries_by_snapshot_) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes,
                               IndexedManifestEntries::Serialize(*entries, pool));
        out.WriteValue<int64_t>(snapshot_id);
        out.WriteValue<int32_t>(bytes->size());
        out.WriteBytes(bytes);
    }
    return IndexedManifestEntries::CopyBytes(out, pool);
}

Result<SnapshotLiveManifestEntries> SnapshotLiveManifestEntries::Deserialize(
    const MemorySegment& segment, int32_t max_snapshots, const std::shared_ptr<MemoryPool>& pool) {
    SnapshotLiveManifestEntries snapshots(max_snapshots);
    if (!segment.Data() || segment.Size() == 0) {
        return snapshots;
    }
    ManifestEntrySerializer serializer(pool);
    PAIMON_RETURN_NOT_OK(VisitSnapshots(
        segment, max_snapshots, [&](int64_t id, const MemorySegment& block) -> Status {
            std::vector<ManifestEntry> entries;
            PAIMON_RETURN_NOT_OK(
                IndexedManifestEntries::Read(block, {}, [&](const InternalRow& row) -> Status {
                    PAIMON_ASSIGN_OR_RAISE(ManifestEntry entry, serializer.FromRow(row));
                    entries.push_back(std::move(entry));
                    return Status::OK();
                }));
            snapshots.Put(id, std::move(entries));
            return Status::OK();
        }));
    return snapshots;
}

void SnapshotLiveManifestEntries::EvictIfNeeded() {
    while (entries_by_snapshot_.size() > NormalizeMaxSnapshots(max_snapshots_)) {
        entries_by_snapshot_.erase(entries_by_snapshot_.begin());
    }
}

Result<std::optional<SnapshotLiveManifestEntries::Selection>>
SnapshotLiveManifestEntries::ReadSnapshot(
    const MemorySegment& segment, int64_t snapshot_id, int32_t max_snapshots,
    const std::function<Result<bool>(const InternalRow&)>& row_filter,
    const std::shared_ptr<MemoryPool>& pool,
    const std::function<Result<bool>(const BinaryRow&)>& partition_filter) {
    std::optional<Selection> selected;
    if (!segment.Data() || segment.Size() == 0 || max_snapshots <= 0) {
        return selected;
    }
    ManifestEntrySerializer serializer(pool);
    IndexedManifestEntries::Filter filter;
    filter.partition = partition_filter;
    PAIMON_RETURN_NOT_OK(VisitSnapshots(
        segment, max_snapshots, [&](int64_t id, const MemorySegment& block) -> Status {
            if (id != snapshot_id) {
                return Status::OK();
            }
            selected.emplace();
            PAIMON_ASSIGN_OR_RAISE(
                selected->candidate_entries,
                IndexedManifestEntries::Read(block, filter, [&](const InternalRow& row) -> Status {
                    if (row_filter) {
                        PAIMON_ASSIGN_OR_RAISE(bool keep, row_filter(row));
                        if (!keep) {
                            return Status::OK();
                        }
                    }
                    PAIMON_ASSIGN_OR_RAISE(ManifestEntry entry, serializer.FromRow(row));
                    selected->entries.push_back(std::move(entry));
                    return Status::OK();
                }));
            return Status::OK();
        }));
    return selected;
}
}  // namespace paimon
