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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/core/core_options.h"
#include "paimon/core/snapshot.h"
#include "paimon/status.h"

namespace paimon {

class ManifestEntry;
struct IndexManifestEntry;
class CommitScanner;
class FileStorePathFactory;
class ManifestFile;
class ManifestList;
class RowIdConflictChecker;
class SnapshotManager;
class TableSchema;

/// Util class for detecting conflicts between base and delta files.
class ConflictDetection {
 public:
    ConflictDetection(std::shared_ptr<TableSchema> table_schema, const CoreOptions& options,
                      std::shared_ptr<SnapshotManager> snapshot_manager,
                      std::shared_ptr<ManifestList> manifest_list,
                      std::shared_ptr<ManifestFile> manifest_file,
                      std::shared_ptr<CommitScanner> commit_scanner, const std::string& commit_user,
                      const std::string& table_name,
                      const std::shared_ptr<FileStorePathFactory>& path_factory);

    /// @param base_index_entries The index entries of `latest_snapshot`, needed only by
    ///     `CheckDeletionVectorsNotBypassed` for the one commit shape that pairs data files
    ///     with their deletion vectors. `BaseIndexEntryFilter` says whether to read them and
    ///     which ones; every other caller passes none.
    Status CheckConflicts(
        const Snapshot& latest_snapshot, const std::vector<ManifestEntry>& base_entries,
        const std::vector<ManifestEntry>& delta_entries,
        const std::vector<IndexManifestEntry>& delta_index_entries,
        const std::optional<std::shared_ptr<RowIdConflictChecker>>& row_id_conflict_checker,
        const Snapshot::CommitKind& commit_kind,
        const std::vector<IndexManifestEntry>& base_index_entries = {}) const;

    /// Selects the base snapshot's index entries `CheckConflicts` needs for this commit, or
    /// returns an empty function when it needs none and the read can be skipped entirely.
    ///
    /// Only the entries that can decide the deletion-vector migration are kept, so a round of a
    /// batched compaction retains what its own work touches rather than the table's whole
    /// deletion-vector index.
    std::function<Result<bool>(const IndexManifestEntry&)> BaseIndexEntryFilter(
        const std::vector<ManifestEntry>& delta_entries,
        const std::vector<IndexManifestEntry>& delta_index_entries,
        const Snapshot::CommitKind& commit_kind) const;

    /// Which rule the row id conflict check follows. The commit that enables the check picks
    /// it, the way it picks the snapshot to check from.
    enum class RowIdCheckStrategy {
        /// A partial-column update: a historical file conflicts only when it wrote the same
        /// columns over the same rows.
        kDataEvolutionDml,
        /// A commit that materializes deletion vectors: it takes the rows of the ranges it
        /// rewrites away, so any historical file over those ranges conflicts.
        kMaterializeDeletionVectors,
    };

    void SetRowIdCheckFromSnapshot(
        const std::optional<int64_t>& row_id_check_from_snapshot,
        RowIdCheckStrategy strategy = RowIdCheckStrategy::kDataEvolutionDml);

    bool HasRowIdCheckFromSnapshot() const;

    /// Whether the configured check runs for this commit kind. The materialize rule only ever
    /// applies to a compaction commit, which is the only shape that reassigns row ids.
    bool RowIdCheckAppliesTo(const Snapshot::CommitKind& commit_kind) const;

    RowIdCheckStrategy GetRowIdCheckStrategy() const {
        return row_id_check_strategy_;
    }

    bool ShouldBeOverwriteCommit(const std::vector<ManifestEntry>& append_table_files,
                                 const std::vector<IndexManifestEntry>& append_index_files) const;

    Status CollectUncheckedBucketPartitions(
        const std::vector<ManifestEntry>& delta_entries,
        std::unordered_map<BinaryRow, int32_t>* total_buckets) const;

    Status CheckSameBucketByTotalBuckets(
        const std::unordered_map<BinaryRow, int32_t>& expected_total_buckets,
        const std::unordered_map<BinaryRow, int32_t>& previous_total_buckets) const;

 private:
    /// Refuses a delta that drops data files from a bucket-unaware table with deletion vectors.
    ///
    /// Such a table keeps its vectors in the index manifest, which the data file entries
    /// compared here do not reference. Pairing the two, so that dropping a data file conflicts
    /// with a concurrent commit rewriting that file's vector, is not implemented, so the shape
    /// needing it is refused rather than let through unchecked. Adding files cannot orphan a
    /// vector and stays allowed, as does replacing one, which travels through the delta index
    /// entries this check does not inspect.
    ///
    /// The exception is a data-evolution compaction, which drops data files *and* carries the
    /// migration of their vectors: it re-keys them onto the rewritten file and removes the index
    /// files that held them in the same commit. For that shape the pairing is checked directly
    /// against `base_index_entries`, so a vector left behind by another engine still fails the
    /// commit rather than being orphaned together with its data file.
    Status CheckDeletionVectorsNotBypassed(
        const std::vector<ManifestEntry>& delta_entries,
        const std::vector<IndexManifestEntry>& delta_index_entries,
        const std::vector<IndexManifestEntry>& base_index_entries,
        const Snapshot::CommitKind& commit_kind) const;

    /// Whether `commit_kind` is the data-evolution compaction that migrates its own deletion
    /// vectors, on a table whose vectors are keyed by data file alone.
    bool IsDataEvolutionCompaction(const Snapshot::CommitKind& commit_kind) const;

    /// Verifies that the deletions a data-evolution compaction inherits really do end up on the
    /// files it writes, given the vectors the base snapshot holds and the index changes the
    /// commit carries. Each dropped file is attributed to the output whose row range covers it,
    /// so the accounting is per compact group rather than per partition; the four ways it can
    /// fail are numbered in the implementation.
    ///
    /// Everything is decided from index metadata: which index file holds a vector and how many
    /// rows it deletes. No vector is read, so a vector deleting the right number of *wrong*
    /// rows passes — reading every vector back to prove otherwise is the cost this check exists
    /// to avoid.
    Status CheckDeletionVectorMigrationIsComplete(
        const std::vector<ManifestEntry>& delta_entries,
        const std::vector<IndexManifestEntry>& delta_index_entries,
        const std::vector<IndexManifestEntry>& base_index_entries) const;

    Status CheckBucketKeepSame(const std::vector<ManifestEntry>& all_entries,
                               const Snapshot::CommitKind& commit_kind,
                               const std::string& base_commit_user,
                               const std::vector<ManifestEntry>& base_entries,
                               const std::vector<ManifestEntry>& delta_entries) const;

    Status BucketNumMismatch(const BinaryRow& partition, int32_t num_buckets,
                             int32_t previous_num_buckets) const;

    Status TotalBucketsChanged(const BinaryRow& partition, int32_t num_buckets,
                               int32_t previous_num_buckets, const std::string& base_commit_user,
                               const std::vector<ManifestEntry>& base_entries,
                               const std::vector<ManifestEntry>& delta_entries) const;

    std::string BuildConflictMessage(const std::string& message,
                                     const std::string& base_commit_user,
                                     const std::vector<ManifestEntry>& base_entries,
                                     const std::vector<ManifestEntry>& delta_entries,
                                     const std::string& cause = "") const;

    void MarkBucketCheckedPartitions(
        const std::unordered_map<BinaryRow, int32_t>& total_buckets) const;

    Status CheckDeleteInEntries(const std::vector<ManifestEntry>& merged_entries,
                                const std::string& base_commit_user,
                                const std::vector<ManifestEntry>& base_entries,
                                const std::vector<ManifestEntry>& delta_entries) const;

    Status CheckKeyRange(const std::vector<ManifestEntry>& merged_entries,
                         const std::string& base_commit_user,
                         const std::vector<ManifestEntry>& base_entries,
                         const std::vector<ManifestEntry>& delta_entries) const;

    /// Checks that every row id range this commit adds is one the table already holds.
    ///
    /// A compaction rewrites rows that exist, so its outputs are checked against the ranges of
    /// their own partition and bucket; every other commit kind assigns fresh row ids, so only
    /// the files reusing a row id below `next_row_id` are checked, and against the exact range
    /// they claim to replace.
    Status CheckRowIdExistence(const std::vector<ManifestEntry>& base_entries,
                               const std::vector<ManifestEntry>& delta_entries,
                               const std::optional<int64_t>& next_row_id,
                               const Snapshot::CommitKind& commit_kind) const;

    Status CheckCompactRowIdExistence(const std::vector<const ManifestEntry*>& existing_data_files,
                                      const std::vector<ManifestEntry>& delta_entries) const;

    Status CheckNonCompactRowIdExistence(
        const std::vector<const ManifestEntry*>& existing_data_files,
        const std::vector<ManifestEntry>& delta_entries,
        const std::optional<int64_t>& next_row_id) const;

    Status CheckRowIdRangeConflicts(const Snapshot::CommitKind& commit_kind,
                                    const std::vector<ManifestEntry>& merged_entries) const;

    Status CheckDataFileRowIdRangeConflicts(RangeHelper<ManifestEntry>& range_helper,
                                            const std::vector<ManifestEntry>& data_files) const;

    Status CheckDedicatedFileRowIdRangeConflicts(
        const std::vector<ManifestEntry>& data_files,
        const std::vector<ManifestEntry>& dedicated_files) const;

    Status CheckForRowIdFromSnapshot(
        const Snapshot& latest_snapshot, const std::vector<ManifestEntry>& delta_entries,
        const std::vector<IndexManifestEntry>& delta_index_entries,
        const std::optional<std::shared_ptr<RowIdConflictChecker>>& row_id_conflict_checker) const;

    Status CheckGlobalIndexRowIdExistence(
        const std::vector<ManifestEntry>& base_entries,
        const std::vector<IndexManifestEntry>& delta_index_entries) const;

 private:
    static constexpr size_t kSameBucketCheckCacheMaxSize = 1000;

    std::shared_ptr<TableSchema> table_schema_;
    CoreOptions options_;
    std::optional<int64_t> row_id_check_from_snapshot_;
    RowIdCheckStrategy row_id_check_strategy_ = RowIdCheckStrategy::kDataEvolutionDml;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<ManifestList> manifest_list_;
    std::shared_ptr<ManifestFile> manifest_file_;
    std::shared_ptr<CommitScanner> commit_scanner_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::string commit_user_;
    std::string table_name_;
    mutable LinkedHashMap<BinaryRow, bool> same_bucket_checked_partitions_;
};

}  // namespace paimon
