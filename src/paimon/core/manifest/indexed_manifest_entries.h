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

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/result.h"

namespace paimon {
class Bytes;
class MemoryPool;

/// Query-independent serialized manifest rows, shared by file and snapshot-live caches.
/// Groups can be skipped without decoding their rows. Ordinals preserve manifest ordering,
/// including interleaved ADD/DELETE records. This is an internal cache format, not a file format.
/// The header records the format, entry version, entry count, and group count. Each group stores
/// its partition/bucket/totalBuckets, row count, and byte length, followed by (ordinal, row) pairs.
/// Unsupported entry versions are retained in the header and rejected before any selection.
class IndexedManifestEntries {
 public:
    struct Filter {
        std::optional<int32_t> bucket;
        std::optional<int32_t> total_buckets;
        std::function<Result<bool>(const BinaryRow&)> partition;
    };

    explicit IndexedManifestEntries(const std::shared_ptr<MemoryPool>& pool) : pool_(pool) {}

    // Rows must already use the current ManifestEntrySerializer schema. No DataFileMeta objects
    // are constructed while building the index. The builder copies the used bytes of each row.
    Status Add(const BinaryRow& row);
    Result<std::shared_ptr<Bytes>> Serialize(const std::shared_ptr<MemoryPool>& pool) const;
    static Result<std::shared_ptr<Bytes>> Serialize(const std::vector<ManifestEntry>& entries,
                                                    const std::shared_ptr<MemoryPool>& pool);

    // The consumer borrows rows only for the duration of this call. Returns the complete entry
    // count (before selection), which snapshot scan metrics use as their candidate count.
    // The optional error flag distinguishes predicate/data-version errors from a damaged cache
    // envelope, so callers do not retry a failed predicate or hide an unsupported manifest.
    static Result<uint64_t> Read(const MemorySegment& segment, const Filter& filter,
                                 const std::function<Status(const InternalRow&)>& consumer,
                                 bool* consumer_or_version_error = nullptr);

    // Flatten a cache stream while retaining its allocator for the lifetime of the returned bytes.
    static Result<std::shared_ptr<Bytes>> CopyBytes(const MemorySegmentOutputStream& out,
                                                    const std::shared_ptr<MemoryPool>& pool);

 private:
    using GroupKey = std::tuple<std::string, std::optional<int32_t>, std::optional<int32_t>>;
    struct Group {
        explicit Group(const std::shared_ptr<MemoryPool>& pool) : rows(1024, pool) {}
        MemorySegmentOutputStream rows;
        int32_t count = 0;
    };
    std::shared_ptr<MemoryPool> pool_;
    std::map<GroupKey, Group> groups_;
    int32_t entry_count_ = 0;
    int32_t entry_version_ = 2;
};
}  // namespace paimon
