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
#include <unordered_set>
#include <utility>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/operation/commit/commit_changes_provider.h"

namespace paimon {

/// One (partition, bucket) a commit materializes deletion vectors for.
struct MaterializedBucket {
    BinaryRow partition;
    int32_t bucket;

    bool operator==(const MaterializedBucket& other) const {
        return bucket == other.bucket && partition == other.partition;
    }
};

struct MaterializedBucketHash {
    size_t operator()(const MaterializedBucket& key) const {
        return std::hash<BinaryRow>()(key.partition) ^ (std::hash<int32_t>()(key.bucket) << 1);
    }
};

using MaterializedBuckets = std::unordered_set<MaterializedBucket, MaterializedBucketHash>;

/// Supplies the changes of a commit that materializes deletion vectors, re-deciding which
/// global indexes it drops on every attempt.
///
/// Materializing gives the surviving rows new row ids, which invalidates every global index of
/// the buckets it rewrites. The deletions prepared before the commit only cover the indexes
/// that existed when the compaction was planned; an index committed by another writer after
/// that would survive against row ids that no longer exist. Since `Provide` is called again for
/// every optimistic-commit attempt, re-reading the latest snapshot's index manifest here means
/// such an index is either dropped by this attempt or makes it retry and is dropped by the next
/// one.
class MaterializedIndexChangesProvider final : public CommitChangesProvider {
 public:
    /// Reads the index manifest of a snapshot. It is a callback so the provider stays free of
    /// the manifest reader's dependencies, the way `OverwriteChangesProvider` does it.
    using IndexScan =
        std::function<Result<std::vector<IndexManifestEntry>>(const Snapshot& snapshot)>;

    MaterializedIndexChangesProvider(std::vector<ManifestEntry> delta_files,
                                     std::vector<ManifestEntry> changelog_files,
                                     std::vector<IndexManifestEntry> index_entries,
                                     MaterializedBuckets materialized_buckets, IndexScan index_scan)
        : delta_files_(std::move(delta_files)),
          changelog_files_(std::move(changelog_files)),
          index_entries_(std::move(index_entries)),
          materialized_buckets_(std::move(materialized_buckets)),
          index_scan_(std::move(index_scan)) {}

    Result<std::shared_ptr<CommitChanges>> Provide(
        const std::optional<Snapshot>& latest_snapshot) const override;

 private:
    std::vector<ManifestEntry> delta_files_;
    std::vector<ManifestEntry> changelog_files_;
    std::vector<IndexManifestEntry> index_entries_;
    MaterializedBuckets materialized_buckets_;
    IndexScan index_scan_;
};

}  // namespace paimon
