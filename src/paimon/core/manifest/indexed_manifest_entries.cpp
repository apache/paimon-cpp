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

#include "paimon/core/manifest/indexed_manifest_entries.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"

namespace paimon {
namespace {
constexpr int32_t kMagic = 0x494d4501;  // IME, cache format version 1
constexpr int32_t kNumFields = 6;

Result<MemorySegment> ReadSlice(DataInputStream* in, const MemorySegment& segment) {
    PAIMON_ASSIGN_OR_RAISE(int32_t size, in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, in->GetPos());
    if (size < 0 || size > segment.Size() - pos) {
        return Status::Invalid("invalid indexed manifest block length");
    }
    PAIMON_RETURN_NOT_OK(in->Seek(pos + size));
    return MemorySegment::WrapView(segment.Data() + pos, size);
}
}  // namespace

Status IndexedManifestEntries::Add(const BinaryRow& row) {
    if (row.GetFieldCount() != kNumFields || entry_count_ == std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("invalid indexed manifest row or entry count");
    }
    if (ManifestEntrySerializer::ValidateVersion(entry_version_).ok()) {
        entry_version_ = row.GetInt(0);
    }
    // Validate the partition before publishing an index that can skip an entire group.
    PAIMON_ASSIGN_OR_RAISE(BinaryRow partition, ManifestEntrySerializer::GetPartitionView(row));
    (void)partition;
    GroupKey key(std::string(row.GetStringView(2)),
                 row.IsNullAt(3) ? std::nullopt : std::optional<int32_t>(row.GetInt(3)),
                 row.IsNullAt(4) ? std::nullopt : std::optional<int32_t>(row.GetInt(4)));
    auto [iter, inserted] = groups_.try_emplace(std::move(key), pool_);
    auto& group = iter->second;
    group.rows.WriteValue<int32_t>(entry_count_++);
    group.rows.WriteValue<int32_t>(row.GetSizeInBytes());
    group.rows.Write(row.GetSegment(), row.GetOffset(), row.GetSizeInBytes());
    ++group.count;
    return Status::OK();
}

Result<std::shared_ptr<Bytes>> IndexedManifestEntries::CopyBytes(
    const MemorySegmentOutputStream& out, const std::shared_ptr<MemoryPool>& pool) {
    if (out.CurrentSize() > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("indexed manifest cache exceeds maximum segment size");
    }
    struct OwnedBytes {
        std::shared_ptr<MemoryPool> pool;
        std::shared_ptr<Bytes> bytes;
    };
    auto owner = std::make_shared<OwnedBytes>();
    owner->pool = pool;
    owner->bytes = Bytes::AllocateBytes(out.CurrentSize(), pool.get());
    int64_t offset = 0;
    for (const auto& segment : out.Segments()) {
        const int64_t length = std::min<int64_t>(segment.Size(), out.CurrentSize() - offset);
        if (length == 0) {
            break;
        }
        std::memcpy(owner->bytes->data() + offset, segment.Data(), length);
        offset += length;
    }
    auto* bytes = owner->bytes.get();
    return std::shared_ptr<Bytes>(std::move(owner), bytes);
}

Result<std::shared_ptr<Bytes>> IndexedManifestEntries::Serialize(
    const std::shared_ptr<MemoryPool>& pool) const {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.WriteValue<int32_t>(kMagic);
    out.WriteValue<int32_t>(entry_version_);
    out.WriteValue<int32_t>(entry_count_);
    out.WriteValue<int32_t>(groups_.size());
    for (const auto& [key, group] : groups_) {
        const auto& [partition, bucket, total_buckets] = key;
        out.WriteValue<int32_t>(partition.size());
        out.Write(partition.data(), partition.size());
        out.WriteValue<int8_t>((bucket ? 1 : 0) | (total_buckets ? 2 : 0));
        out.WriteValue<int32_t>(bucket.value_or(0));
        out.WriteValue<int32_t>(total_buckets.value_or(0));
        out.WriteValue<int32_t>(group.count);
        const int64_t block_size = group.rows.CurrentSize();
        if (block_size > std::numeric_limits<int32_t>::max() ||
            out.CurrentSize() + sizeof(int32_t) + block_size >
                std::numeric_limits<int32_t>::max()) {
            return Status::Invalid("indexed manifest cache exceeds maximum segment size");
        }
        out.WriteValue<int32_t>(block_size);
        int64_t remaining = block_size;
        for (const auto& segment : group.rows.Segments()) {
            const int32_t length = std::min<int64_t>(remaining, segment.Size());
            out.Write(segment, 0, length);
            remaining -= length;
        }
    }
    return CopyBytes(out, pool);
}

Result<std::shared_ptr<Bytes>> IndexedManifestEntries::Serialize(
    const std::vector<ManifestEntry>& entries, const std::shared_ptr<MemoryPool>& pool) {
    IndexedManifestEntries index(pool);
    ManifestEntrySerializer serializer(pool);
    for (const auto& entry : entries) {
        if (entry.File()->creation_time.GetNanoOfMillisecond() != 0) {
            return Status::Invalid("manifest cache requires millisecond creation_time");
        }
        PAIMON_ASSIGN_OR_RAISE(BinaryRow row, serializer.ToRow(entry));
        PAIMON_RETURN_NOT_OK(index.Add(row));
    }
    return index.Serialize(pool);
}

Result<uint64_t> IndexedManifestEntries::Read(
    const MemorySegment& segment, const Filter& filter,
    const std::function<Status(const InternalRow&)>& consumer, bool* consumer_or_version_error) {
    if (consumer_or_version_error) {
        *consumer_or_version_error = false;
    }
    if (!segment.Data() || segment.Size() < 4 * static_cast<int32_t>(sizeof(int32_t))) {
        return Status::Invalid("truncated indexed manifest entries header");
    }
    auto semantic_status = [consumer_or_version_error](const Status& status) -> Status {
        if (!status.ok() && consumer_or_version_error) {
            *consumer_or_version_error = true;
        }
        return status;
    };
    auto stream = std::make_shared<ByteArrayInputStream>(segment.Data(), segment.Size());
    DataInputStream in(stream);
    PAIMON_ASSIGN_OR_RAISE(int32_t magic, in.ReadValue<int32_t>());
    if (magic != kMagic) {
        return Status::Invalid("invalid indexed manifest entries magic");
    }
    // Preserve version validation even for groups excluded by bucket or partition pruning.
    PAIMON_ASSIGN_OR_RAISE(int32_t entry_version, in.ReadValue<int32_t>());
    PAIMON_RETURN_NOT_OK(semantic_status(ManifestEntrySerializer::ValidateVersion(entry_version)));
    PAIMON_ASSIGN_OR_RAISE(int32_t entry_count, in.ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t group_count, in.ReadValue<int32_t>());
    if (entry_count < 0 || group_count < 0 || group_count > entry_count) {
        return Status::Invalid("invalid indexed manifest entry or group count");
    }
    std::vector<std::pair<int32_t, BinaryRow>> selected;
    std::optional<std::string_view> previous_partition;
    bool partition_matches = true;
    int64_t counted = 0;
    for (int32_t i = 0; i < group_count; ++i) {
        PAIMON_ASSIGN_OR_RAISE(MemorySegment partition_bytes, ReadSlice(&in, segment));
        std::string_view partition(partition_bytes.Data(), partition_bytes.Size());
        PAIMON_ASSIGN_OR_RAISE(BinaryRow partition_row,
                               SerializationUtils::DeserializeBinaryRowView(partition));
        PAIMON_ASSIGN_OR_RAISE(int8_t flags, in.ReadValue<int8_t>());
        PAIMON_ASSIGN_OR_RAISE(int32_t bucket, in.ReadValue<int32_t>());
        PAIMON_ASSIGN_OR_RAISE(int32_t total_buckets, in.ReadValue<int32_t>());
        PAIMON_ASSIGN_OR_RAISE(int32_t count, in.ReadValue<int32_t>());
        PAIMON_ASSIGN_OR_RAISE(MemorySegment block, ReadSlice(&in, segment));
        if (flags < 0 || flags > 3 || count <= 0 || count > block.Size() / 8 ||
            counted + count > entry_count) {
            return Status::Invalid("invalid indexed manifest group");
        }
        counted += count;
        const bool historical =
            filter.total_buckets && (flags != 3 || total_buckets != *filter.total_buckets);
        if (filter.bucket && !historical && bucket != *filter.bucket) {
            continue;
        }
        // Groups are ordered by partition first; test each selected partition only once.
        if (filter.partition && (!previous_partition || *previous_partition != partition)) {
            auto matches = filter.partition(partition_row);
            PAIMON_RETURN_NOT_OK(semantic_status(matches.status()));
            partition_matches = matches.value();
            previous_partition = partition;
        }
        if (!partition_matches) {
            continue;
        }
        auto row_stream = std::make_shared<ByteArrayInputStream>(block.Data(), block.Size());
        DataInputStream rows(row_stream);
        for (int32_t j = 0; j < count; ++j) {
            PAIMON_ASSIGN_OR_RAISE(int32_t ordinal, rows.ReadValue<int32_t>());
            PAIMON_ASSIGN_OR_RAISE(MemorySegment bytes, ReadSlice(&rows, block));
            if (ordinal < 0 || ordinal >= entry_count ||
                bytes.Size() <
                    BinaryRow::CalculateBitSetWidthInBytes(kNumFields) + kNumFields * 8) {
                return Status::Invalid("invalid indexed manifest row");
            }
            BinaryRow row(kNumFields);
            row.PointTo(bytes, 0, bytes.Size());
            PAIMON_RETURN_NOT_OK(
                semantic_status(ManifestEntrySerializer::ValidateVersion(row.GetInt(0))));
            selected.emplace_back(ordinal, std::move(row));
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t end, rows.GetPos());
        if (end != block.Size()) {
            return Status::Invalid("trailing indexed manifest group bytes");
        }
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t end, in.GetPos());
    if (counted != entry_count || end != segment.Size()) {
        return Status::Invalid("invalid indexed manifest entries length or count");
    }
    std::sort(selected.begin(), selected.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    int32_t previous_ordinal = -1;
    for (const auto& [ordinal, row] : selected) {
        if (ordinal == previous_ordinal) {
            return Status::Invalid("duplicate indexed manifest ordinal");
        }
        previous_ordinal = ordinal;
    }
    for (const auto& [ordinal, row] : selected) {
        PAIMON_RETURN_NOT_OK(semantic_status(consumer(row)));
    }
    return static_cast<uint64_t>(entry_count);
}
}  // namespace paimon
