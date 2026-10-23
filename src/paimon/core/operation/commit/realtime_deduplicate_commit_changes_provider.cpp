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

#include "paimon/core/operation/commit/realtime_deduplicate_commit_changes_provider.h"

#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/utils/file_store_path_factory.h"

namespace paimon {

RealtimeDeduplicateCommitChangesProvider::RealtimeDeduplicateCommitChangesProvider(
    ManifestEntryChanges changes,
    std::map<RealtimePartitionBucket, RoaringBitmap64> deleted_offsets,
    std::shared_ptr<TableSchema> table_schema, std::shared_ptr<FileStorePathFactory> path_factory,
    std::shared_ptr<CommitScanner> scanner, const CoreOptions& options,
    std::shared_ptr<MemoryPool> pool)
    : changes_(std::move(changes)),
      deleted_offsets_(std::move(deleted_offsets)),
      path_factory_(std::move(path_factory)),
      materializer_(std::move(table_schema), path_factory_, std::move(scanner), options,
                    std::move(pool)) {}

Result<std::shared_ptr<CommitChanges>> RealtimeDeduplicateCommitChangesProvider::Provide(
    const std::optional<Snapshot>& latest_snapshot) const {
    // Example: F1 offsets [10, 11, 12], baseline DV {0}; deleting offset 11 yields DV {0, 1}.
    // If a retry sees offset 11 moved to F2 row 0, update F2's DV instead of F1's.
    // Resolve against latest files and this commit's new files, preserving other baseline DVs.
    if (changes_.HasCompactChanges()) {
        return Status::Invalid(
            "realtime deduplicate progress must not contain physical compaction changes");
    }
    auto result = std::make_shared<CommitChanges>(
        changes_.append_table_files, changes_.append_changelog, changes_.append_index_files);
    ScopeGuard cleanup([&]() { materializer_.CleanUpUnpublishedFiles(*result); });

    // 1. Group nonempty logical deletions by partition and bucket.
    using BucketOffsets = std::map<int32_t, const RoaringBitmap64*>;
    std::unordered_map<BinaryRow, BucketOffsets> buckets;
    std::vector<BinaryRow> partitions;
    for (const auto& [partition_bucket, offsets] : deleted_offsets_) {
        if (offsets.IsEmpty()) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(BinaryRow partition,
                               path_factory_->ToBinaryRow(partition_bucket.partition));
        buckets[partition].emplace(partition_bucket.bucket, &offsets);
        partitions.push_back(partition);
    }
    if (partitions.empty()) {
        cleanup.Release();
        return result;
    }
    // 2. Group latest-snapshot files with this commit's deltas, including all supplied prepares.
    PAIMON_ASSIGN_OR_RAISE(RealtimeDeduplicateDvMaterializer::SnapshotBuckets snapshot_buckets,
                           materializer_.LoadSnapshot(latest_snapshot, partitions));
    for (const ManifestEntry& entry : result->delta_files) {
        const auto partition_iter = buckets.find(entry.Partition());
        if (partition_iter != buckets.end() && partition_iter->second.count(entry.Bucket()) != 0) {
            snapshot_buckets[entry.Partition()][entry.Bucket()].files.push_back(entry);
        }
    }
    for (const auto& [partition, bucket_offsets] : buckets) {
        for (const auto& [bucket, offsets] : bucket_offsets) {
            const auto& snapshot = snapshot_buckets[partition][bucket];
            // 3. Apply ADD/DELETE entries to obtain this bucket's final active files.
            std::map<std::string, std::shared_ptr<DataFileMeta>> active_files;
            for (const ManifestEntry& entry : snapshot.files) {
                if (entry.Kind() == FileKind::Add()) {
                    active_files[entry.FileName()] = entry.File();
                } else {
                    active_files.erase(entry.FileName());
                }
            }
            RealtimeDeduplicateDvMaterializer::BucketUpdate update;
            for (const auto& [name, file] : active_files) {
                update.final_file_names.insert(name);
                update.lookup_files.push_back(file);
            }
            update.deleted_offsets = *offsets;
            // 4. Map logical deleted offsets to physical row positions in the active files.
            PAIMON_ASSIGN_OR_RAISE(RealtimeDeduplicateDvMaterializer::FilePositions positions,
                                   materializer_.LookupFilePositions(partition, bucket, update));
            // Preserve the write-side no-hit fast path: do not read, prune or rewrite baseline
            // DVs when none of the logical deletions maps to a current physical row.
            if (positions.empty()) {
                continue;
            }
            // 5. Load latest baseline DVs, merge deletions and emit index changes.
            PAIMON_ASSIGN_OR_RAISE(RealtimeDeduplicateDvMaterializer::DeletionVectors baseline,
                                   materializer_.ReadDeletionVectors(partition, bucket, snapshot));
            PAIMON_RETURN_NOT_OK(materializer_.MaterializeBucket(
                partition, bucket, snapshot, std::move(baseline), update, positions, result.get()));
        }
    }
    cleanup.Release();
    return result;
}

}  // namespace paimon
