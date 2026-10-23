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
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "paimon/common/options/memory_size.h"
#include "paimon/core/catalog/snapshot_commit.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/partition_entry.h"
#include "paimon/core/operation/commit/commit_scanner.h"
#include "paimon/core/operation/commit/conflict_detection.h"
#include "paimon/core/operation/commit/manifest_entry_changes.h"
#include "paimon/core/operation/commit/retry_waiter.h"
#include "paimon/core/operation/commit/row_id_column_conflict_checker.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/file_store_commit.h"
#include "paimon/logging.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
class Schema;
}  // namespace arrow

namespace paimon {

class CommitContext;
class CommitMessageImpl;
struct DataFileMeta;
class ExpireSnapshots;
class FileKind;
class FileStorePathFactory;
class ManifestEntry;
class ManifestCommittable;
class ManifestFile;
class IndexManifestFile;
struct IndexManifestEntry;
class ManifestList;
class ManifestFileMeta;
class SchemaManager;
class TableSchema;
class BinaryRowPartitionComputer;
class CommitChangesProvider;
class CommitMessage;
class Executor;
class FileSystem;
class Logger;
class MemoryPool;
class Metrics;
class PartitionEntry;
class SnapshotCommit;

/// Commit operation which provides commit and overwrite.
class FileStoreCommitImpl : public FileStoreCommit {
 public:
    /// Loads the current catalog schema ID; unset to use `schema_manager_`.
    using SchemaIdLoader = std::function<Result<int64_t>()>;

    static Status ValidateCommitOptions(const CoreOptions& options);

    FileStoreCommitImpl(const std::shared_ptr<MemoryPool>& pool,
                        const std::shared_ptr<Executor>& executor,
                        const std::shared_ptr<arrow::Schema>& schema, const std::string& root_path,
                        const std::string& commit_user, const CoreOptions& options,
                        const std::shared_ptr<FileStorePathFactory>& path_factory,
                        std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
                        const std::shared_ptr<SnapshotManager>& snapshot_manager,
                        const std::shared_ptr<SnapshotCommit>& snapshot_commit,
                        bool ignore_empty_commit, bool append_commit_check_conflict,
                        const std::shared_ptr<TableSchema>& table_schema,
                        SchemaIdLoader schema_id_loader,
                        const std::shared_ptr<ManifestFile>& manifest_file,
                        const std::shared_ptr<ManifestList>& manifest_list,
                        const std::shared_ptr<IndexManifestFile>& index_manifest_file,
                        const std::shared_ptr<ExpireSnapshots>& expire_snapshots,
                        const std::shared_ptr<SchemaManager>& schema_manager,
                        CommitScanner::ScanSupplier scan_supplier);
    ~FileStoreCommitImpl() override;

    Status Commit(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                  int64_t commit_identifier,
                  std::optional<int64_t> watermark = std::nullopt) override;

    Result<int64_t> CommitWithProgress(const std::vector<RealtimeCommitProgress>& realtime_commits,
                                       int64_t commit_identifier,
                                       std::optional<int64_t> watermark) override;

    Result<int32_t> FilterAndCommit(
        const std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>>&
            commit_identifier_and_messages,
        std::optional<int64_t> watermark = std::nullopt) override;

    Status Overwrite(const std::map<std::string, std::string>& partition,
                     const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                     int64_t commit_identifier,
                     std::optional<int64_t> watermark = std::nullopt) override;

    Result<int32_t> FilterAndOverwrite(
        const std::map<std::string, std::string>& partition,
        const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
        int64_t commit_identifier, std::optional<int64_t> watermark = std::nullopt) override;

    Result<std::string> GetLastCommitTableRequest() override;

    Result<int32_t> Expire() override;

    Status DropPartition(const std::vector<std::map<std::string, std::string>>& partitions,
                         int64_t commit_identifier) override;

    Status TruncateTable(int64_t commit_identifier) override;

    Status Abort(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages) override;

    Result<bool> RollbackToAsLatest(int64_t target_snapshot_id) override;

    FileStoreCommit& RowIdCheckConflict(std::optional<int64_t> row_id_check_from_snapshot) override;

    std::shared_ptr<Metrics> GetCommitMetrics() const override {
        return metrics_;
    }

    Status Init(std::unique_ptr<CommitContext> ctx);

 private:
    struct TryCommitResult {
        int32_t attempts;
        bool already_committed;
    };

    Status Commit(const std::shared_ptr<ManifestCommittable>& manifest_committable,
                  bool check_append_files, bool retry_on_conflict,
                  const std::map<RealtimePartitionBucket, OffsetRange>& realtime_ranges);

    Result<int32_t> TryOverwrite(const std::vector<std::map<std::string, std::string>>& partition,
                                 const std::vector<ManifestEntry>& changes,
                                 const std::vector<IndexManifestEntry>& index_entries,
                                 int64_t commit_identifier, std::optional<int64_t> watermark,
                                 const std::map<std::string, std::string>& properties);

    Status ExecuteOverwrite(const std::vector<std::map<std::string, std::string>>& partitions,
                            ManifestEntryChanges* changes, int64_t identifier,
                            std::optional<int64_t> watermark,
                            const std::shared_ptr<ManifestCommittable>& committable,
                            int32_t* generated_snapshot, int32_t* attempt);

    Result<std::vector<ManifestEntry>> GetAllFiles(
        const Snapshot& snapshot,
        const std::vector<std::map<std::string, std::string>>& partitions) const;

    Result<std::map<std::string, std::string>> PartitionToMap(const BinaryRow& partition) const;

    Result<std::vector<ManifestEntry>> TryUpgrade(
        const std::vector<ManifestEntry>& append_files) const;

    Result<std::vector<std::shared_ptr<ManifestCommittable>>> FilterCommitted(
        const std::vector<std::shared_ptr<ManifestCommittable>>& committables);

    /// Pass the source flag returned with `latest_snapshot` to apply the correct history boundary.
    Result<std::optional<Snapshot>> LatestSnapshotOfCommitUserAtOrBefore(
        const std::optional<Snapshot>& latest_snapshot, bool latest_from_catalog) const;

    /// Returns the containing snapshot id when the commit is already complete, or nullopt when
    /// neither its identifier nor offset ranges have been committed.
    Result<std::optional<int64_t>> ResolveRealtimeCommit(
        const SnapshotManager::LatestSnapshotResult& latest, int64_t identifier,
        const std::map<RealtimePartitionBucket, OffsetRange>& realtime_ranges) const;

    std::shared_ptr<ManifestCommittable> CreateManifestCommittable(
        int64_t identifier, const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
        std::optional<int64_t> watermark, const std::map<std::string, std::string>& properties);

    Result<ManifestEntryChanges> CollectChanges(
        const std::vector<std::shared_ptr<CommitMessage>>& commit_messages);

    void ReportCommit(const ManifestEntryChanges& changes, int64_t commit_duration,
                      int32_t generated_snapshot, int32_t attempt);

    Result<TryCommitResult> TryCommit(
        const std::vector<ManifestEntry>& delta_files,
        const std::vector<ManifestEntry>& changelog_files,
        const std::vector<IndexManifestEntry>& index_entries, int64_t identifier,
        std::optional<int64_t> watermark, const std::map<std::string, std::string>& properties,
        const std::map<RealtimePartitionBucket, OffsetRange>& realtime_ranges,
        Snapshot::CommitKind commit_kind, bool reset_all_realtime_progress,
        const std::vector<std::map<std::string, std::string>>& removed_realtime_partitions,
        bool detect_conflicts, bool retry_on_conflict);

    Result<TryCommitResult> TryCommit(
        const std::shared_ptr<CommitChangesProvider>& changes_provider, int64_t identifier,
        std::optional<int64_t> watermark, const std::map<std::string, std::string>& properties,
        const std::map<RealtimePartitionBucket, OffsetRange>& realtime_ranges,
        Snapshot::CommitKind commit_kind, bool reset_all_realtime_progress,
        const std::vector<std::map<std::string, std::string>>& removed_realtime_partitions,
        bool detect_conflicts, bool retry_on_conflict);

    Result<bool> TryCommitOnce(const std::vector<ManifestEntry>& delta_files,
                               const std::vector<ManifestEntry>& changelog_files,
                               const std::vector<IndexManifestEntry>& index_entries,
                               int64_t commit_identifier, std::optional<int64_t> watermark,
                               const std::map<std::string, std::string>& properties,
                               Snapshot::CommitKind commit_kind,
                               const std::optional<Snapshot>& latest_snapshot,
                               bool detect_conflicts);

    /// Requires schemas for files carrying row IDs to be available in the table directory.
    Status CheckRowIdSchemasArePublished(
        const std::vector<std::shared_ptr<DataFileMeta>>& delta_files) const;

    std::string CommitTargetSuffix() const;

    static std::string FormatBaseSnapshotUuid(const std::optional<Snapshot>& snapshot);

    Result<bool> CommitSnapshotImpl(const std::optional<Snapshot>& base_snapshot,
                                    const Snapshot& new_snapshot,
                                    const std::vector<PartitionEntry>& delta_statistics);

    Result<std::vector<ManifestEntry>> ReadAddManifestEntries(const Snapshot& snapshot) const;

    void CleanUpTmpManifests(const std::string& previous_changes_list_name,
                             const std::string& new_changes_list_name,
                             const std::vector<ManifestFileMeta>& old_metas,
                             const std::vector<ManifestFileMeta>& new_metas,
                             const std::optional<std::string>& old_index_manifest,
                             const std::optional<std::string>& new_index_manifest);

    Status CheckSameBucketFromSnapshot(const std::vector<ManifestEntry>& delta_entries,
                                       const std::optional<Snapshot>& latest_snapshot) const;

    bool ShouldCheckSameBucket(const Snapshot::CommitKind& commit_kind) const;

    bool IsUnorderedWriteOnlyAppend() const;

    bool IsWriteOnlySnapshotSequenceAppend() const;

    Result<std::optional<int64_t>> MaxSequenceNumber(
        const std::vector<ManifestFileMeta>& manifests) const;

    Status CheckFilesExistence(
        const std::vector<std::shared_ptr<ManifestCommittable>>& committables) const;

 private:
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<arrow::Schema> schema_;
    std::string root_path_;
    std::string table_name_;
    std::string commit_user_;
    CoreOptions options_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::shared_ptr<FileSystem> fs_;

    std::unique_ptr<BinaryRowPartitionComputer> partition_computer_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<SnapshotCommit> snapshot_commit_;
    bool ignore_empty_commit_ = true;
    bool append_commit_check_conflict_ = false;
    RetryWaiter retry_waiter_;
    int32_t num_bucket_ = 0;
    BucketMode bucket_mode_ = BucketMode::BUCKET_UNAWARE;
    std::shared_ptr<TableSchema> table_schema_;
    SchemaIdLoader schema_id_loader_;
    std::shared_ptr<CommitScanner> commit_scanner_;
    ConflictDetection conflict_detection_;

    std::shared_ptr<ManifestFile> manifest_file_;
    std::shared_ptr<ManifestList> manifest_list_;
    std::shared_ptr<IndexManifestFile> index_manifest_file_;

    std::shared_ptr<ExpireSnapshots> expire_snapshots_;
    std::shared_ptr<SchemaManager> schema_manager_;

    std::shared_ptr<Metrics> metrics_;
    std::shared_ptr<Logger> logger_;
    int64_t last_committed_snapshot_id_ = -1;
};

}  // namespace paimon
