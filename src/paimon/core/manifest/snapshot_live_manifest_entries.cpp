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
#include <cstring>
#include <utility>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/memory/memory_segment.h"

namespace paimon {
namespace {

constexpr int32_t kMagic = 0x534d4544;  // SMED: includes per-snapshot total file counts

size_t NormalizeMaxSnapshots(int32_t max_snapshots) {
    return static_cast<size_t>(std::max(0, max_snapshots));
}

std::shared_ptr<Bytes> ToBytes(const MemorySegmentOutputStream& out,
                               const std::shared_ptr<MemoryPool>& pool) {
    auto bytes = Bytes::AllocateBytes(static_cast<size_t>(out.CurrentSize()), pool.get());
    int64_t offset = 0;
    for (const auto& segment : out.Segments()) {
        int64_t copy_size =
            std::min<int64_t>(segment.Size(), static_cast<int64_t>(bytes->size()) - offset);
        if (copy_size <= 0) {
            break;
        }
        std::memcpy(bytes->data() + offset, segment.Data(), static_cast<size_t>(copy_size));
        offset += copy_size;
    }
    return bytes;
}

}  // namespace

SnapshotLiveManifestEntries::SnapshotLiveManifestEntries(int32_t max_snapshots)
    : max_snapshots_(max_snapshots) {}

void SnapshotLiveManifestEntries::Put(int64_t snapshot_id, std::vector<ManifestEntry>&& entries,
                                      int64_t total_data_files) {
    if (NormalizeMaxSnapshots(max_snapshots_) == 0) {
        return;
    }
    entries_by_snapshot_[snapshot_id] =
        std::make_shared<const std::vector<ManifestEntry>>(std::move(entries));
    total_data_files_by_snapshot_[snapshot_id] = total_data_files;
    EvictIfNeeded();
}

size_t SnapshotLiveManifestEntries::Size() const {
    return entries_by_snapshot_.size();
}

Result<std::shared_ptr<Bytes>> SnapshotLiveManifestEntries::Serialize(
    const std::shared_ptr<MemoryPool>& pool) const {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.WriteValue<int32_t>(kMagic);
    out.WriteValue<int32_t>(static_cast<int32_t>(entries_by_snapshot_.size()));

    ManifestEntrySerializer serializer(pool);
    for (const auto& [snapshot_id, entries] : entries_by_snapshot_) {
        out.WriteValue<int64_t>(snapshot_id);
        out.WriteValue<int64_t>(total_data_files_by_snapshot_.at(snapshot_id));
        PAIMON_RETURN_NOT_OK(serializer.SerializeList(*entries, &out));
    }
    return ToBytes(out, pool);
}

Result<SnapshotLiveManifestEntries> SnapshotLiveManifestEntries::Deserialize(
    const MemorySegment& segment, int32_t max_snapshots, const std::shared_ptr<MemoryPool>& pool) {
    SnapshotLiveManifestEntries snapshot_live_manifest_entries(max_snapshots);
    if (segment.Data() == nullptr || segment.Size() == 0) {
        return snapshot_live_manifest_entries;
    }

    auto bytes = segment.GetOrCreateHeapMemory(pool.get());
    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    DataInputStream in(input_stream);

    PAIMON_ASSIGN_OR_RAISE(int32_t magic, in.ReadValue<int32_t>());
    if (magic != kMagic) {
        return Status::Invalid("invalid snapshot live manifest entries magic");
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t snapshot_count, in.ReadValue<int32_t>());
    if (snapshot_count < 0) {
        return Status::Invalid("snapshot live manifest entries snapshot count is negative");
    }

    ManifestEntrySerializer serializer(pool);
    for (int32_t i = 0; i < snapshot_count; i++) {
        PAIMON_ASSIGN_OR_RAISE(int64_t snapshot_id, in.ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t total_data_files, in.ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries, serializer.DeserializeList(&in));
        snapshot_live_manifest_entries.total_data_files_by_snapshot_[snapshot_id] =
            total_data_files;
        snapshot_live_manifest_entries.entries_by_snapshot_[snapshot_id] =
            std::make_shared<const std::vector<ManifestEntry>>(std::move(entries));
    }
    snapshot_live_manifest_entries.EvictIfNeeded();
    return snapshot_live_manifest_entries;
}

void SnapshotLiveManifestEntries::EvictIfNeeded() {
    size_t max_snapshots = NormalizeMaxSnapshots(max_snapshots_);
    while (entries_by_snapshot_.size() > max_snapshots) {
        total_data_files_by_snapshot_.erase(entries_by_snapshot_.begin()->first);
        entries_by_snapshot_.erase(entries_by_snapshot_.begin());
    }
}

Result<std::optional<SnapshotLiveManifestEntries::Selection>>
SnapshotLiveManifestEntries::ReadSnapshot(
    const MemorySegment& segment, int64_t snapshot_id, int32_t max_snapshots,
    const std::function<Result<bool>(const InternalRow&)>& row_filter,
    const std::shared_ptr<MemoryPool>& pool) {
    std::optional<Selection> selected;
    if (segment.Data() == nullptr || segment.Size() == 0 || max_snapshots <= 0) {
        return selected;
    }
    auto bytes = segment.GetOrCreateHeapMemory(pool.get());
    auto stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    DataInputStream in(stream);
    PAIMON_ASSIGN_OR_RAISE(int32_t magic, in.ReadValue<int32_t>());
    if (magic != kMagic) {
        return Status::Invalid("invalid snapshot live manifest entries magic");
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t snapshot_count, in.ReadValue<int32_t>());
    if (snapshot_count < 0) {
        return Status::Invalid("snapshot live manifest entries snapshot count is negative");
    }
    ManifestEntrySerializer serializer(pool);
    BinaryRowSerializer row_serializer(serializer.NumFields(), pool);
    std::optional<int64_t> previous_id;
    for (int32_t i = 0; i < snapshot_count; ++i) {
        PAIMON_ASSIGN_OR_RAISE(int64_t id, in.ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t total_data_files, in.ReadValue<int64_t>());
        // Serialize emits snapshots in ascending order. Validate this before using the position
        // to honor a retention limit smaller than the one used when the cache was populated.
        if (previous_id && id <= previous_id.value()) {
            return Status::Invalid("snapshot live manifest entries are not ordered");
        }
        previous_id = id;
        const bool keep = id == snapshot_id && i >= snapshot_count - max_snapshots;
        PAIMON_ASSIGN_OR_RAISE(int32_t entry_count, in.ReadValue<int32_t>());
        if (entry_count < 0) {
            return Status::Invalid("snapshot live manifest entries entry count is negative");
        }
        if (keep) {
            selected.emplace();
            selected->candidate_entries = entry_count;
            selected->total_data_files = total_data_files;
            if (!row_filter) {
                selected->entries.reserve(entry_count);
            }
        }
        for (int32_t j = 0; j < entry_count; ++j) {
            if (!keep) {
                PAIMON_RETURN_NOT_OK(BinaryRowSerializer::Skip(&in));
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(BinaryRow row, row_serializer.Deserialize(&in));
            if (row_filter) {
                PAIMON_RETURN_NOT_OK(ManifestEntrySerializer::ValidateVersion(row.GetInt(0)));
                PAIMON_ASSIGN_OR_RAISE(bool matches, row_filter(row));
                if (!matches) {
                    continue;
                }
            }
            PAIMON_ASSIGN_OR_RAISE(ManifestEntry entry, serializer.FromRow(row));
            selected->entries.emplace_back(std::move(entry));
        }
    }
    return selected;
}

}  // namespace paimon
