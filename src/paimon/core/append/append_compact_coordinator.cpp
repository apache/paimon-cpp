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

#include "paimon/append/append_compact_coordinator.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/core/append/append_compact_task.h"
#include "paimon/core/append/data_evolution_compact_deletion_vector_rewriter.h"
#include "paimon/core/append/data_evolution_compact_global_index_dropper.h"
#include "paimon/core/append/data_evolution_compact_planner.h"
#include "paimon/core/append/data_evolution_materialize_deletion_compact_task.h"
#include "paimon/core/append/data_evolution_normal_compact_task.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/operation/append_only_file_store_write.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/file_store_commit.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/utils/range.h"
#include "paimon/utils/row_range_index.h"
namespace paimon {

namespace {

/// A bin for packing small files into compaction groups.
class FileBin {
 public:
    FileBin(int64_t target_file_size, int64_t open_file_cost, int32_t min_file_num)
        : target_file_size_(target_file_size),
          open_file_cost_(open_file_cost),
          min_file_num_(min_file_num) {}

    void AddFile(const std::shared_ptr<DataFileMeta>& file) {
        total_file_size_ += file->file_size + open_file_cost_;
        bin_.push_back(file);
    }

    bool EnoughContent() const {
        return bin_.size() > 1 && total_file_size_ >= target_file_size_ * 2;
    }

    bool EnoughInputFiles() const {
        return static_cast<int32_t>(bin_.size()) >= min_file_num_;
    }

    std::vector<std::shared_ptr<DataFileMeta>> Drain() {
        std::vector<std::shared_ptr<DataFileMeta>> result = std::move(bin_);
        bin_.clear();
        total_file_size_ = 0;
        return result;
    }

    bool IsEmpty() const {
        return bin_.empty();
    }

 private:
    int64_t target_file_size_;
    int64_t open_file_cost_;
    int32_t min_file_num_;
    std::vector<std::shared_ptr<DataFileMeta>> bin_;
    int64_t total_file_size_ = 0;
};

/// Pack small files into compaction groups using a bin-packing algorithm.
/// Files are sorted by size ascending, then greedily packed into bins.
/// A bin is flushed when its total size >= targetFileSize * 2 (and has > 1 file),
/// or when it has >= minFileNum files.
std::vector<std::vector<std::shared_ptr<DataFileMeta>>> PackFiles(
    const std::vector<std::shared_ptr<DataFileMeta>>& files, int64_t target_file_size,
    int64_t open_file_cost, int32_t min_file_num) {
    // Sort by file size ascending for better packing
    std::vector<std::shared_ptr<DataFileMeta>> sorted_files(files.begin(), files.end());
    std::sort(
        sorted_files.begin(), sorted_files.end(),
        [](const std::shared_ptr<DataFileMeta>& left, const std::shared_ptr<DataFileMeta>& right) {
            return left->file_size < right->file_size;
        });

    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> result;
    FileBin file_bin(target_file_size, open_file_cost, min_file_num);

    for (const auto& file_meta : sorted_files) {
        file_bin.AddFile(file_meta);
        if (file_bin.EnoughContent()) {
            result.push_back(file_bin.Drain());
        }
    }

    if (file_bin.EnoughInputFiles()) {
        result.push_back(file_bin.Drain());
    }
    // else: skip small files that are too few to compact

    return result;
}

/// Create a FileStoreScan for scanning manifest entries.
/// Mirrors the logic in `AppendOnlyFileStoreWrite::CreateFileStoreScan`.
Result<std::unique_ptr<FileStoreScan>> CreateFileStoreScan(
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& core_options,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<ScanFilter>& scan_filter, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                             core_options.GetManifestCompression(), path_factory,
                             core_options.GetCache(), pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                             core_options.GetManifestCompression(), path_factory,
                             core_options.GetManifestTargetFileSize(), pool, core_options,
                             partition_schema));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<AppendOnlyFileStoreScan> scan,
        AppendOnlyFileStoreScan::Create(snapshot_manager, schema_manager, manifest_list,
                                        manifest_file, table_schema, arrow_schema, scan_filter,
                                        core_options, executor, pool));
    return std::unique_ptr<FileStoreScan>(std::move(scan));
}

/// Create an AppendOnlyFileStoreWrite for executing compaction rewrites.
std::unique_ptr<AppendOnlyFileStoreWrite> CreateFileStoreWrite(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager, const std::string& table_path,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& core_options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    return std::make_unique<AppendOnlyFileStoreWrite>(
        path_factory, snapshot_manager, schema_manager,
        /*commit_user=*/"compact-coordinator",
        /*root_path=*/table_path, table_schema, arrow_schema,
        /*write_schema=*/arrow_schema, partition_schema,
        /*dv_maintainer_factory=*/nullptr,
        /*io_manager=*/nullptr, core_options,
        /*ignore_previous_files=*/true,
        /*is_streaming_mode=*/false,
        /*ignore_num_bucket_check=*/false,
        /*realtime_context=*/nullptr, executor, pool);
}

/// Rejects caller options that change an immutable table option: they decide which
/// compaction path runs and the physical layout the rewrite must preserve, so overriding
/// them could route a data-evolution table into the plain append rewrite (reordering row
/// ids) or slip a deletion-vector table past its rejection. Compared on the parsed effective
/// values, so passing an explicit default is still allowed.
Status ValidateImmutableOptions(const CoreOptions& schema_options,
                                const CoreOptions& merged_options) {
    std::vector<std::string> changed_options;
    if (schema_options.RowTrackingEnabled() != merged_options.RowTrackingEnabled()) {
        changed_options.emplace_back(Options::ROW_TRACKING_ENABLED);
    }
    if (schema_options.DataEvolutionEnabled() != merged_options.DataEvolutionEnabled()) {
        changed_options.emplace_back(Options::DATA_EVOLUTION_ENABLED);
    }
    if (schema_options.DeletionVectorsEnabled() != merged_options.DeletionVectorsEnabled()) {
        changed_options.emplace_back(Options::DELETION_VECTORS_ENABLED);
    }
    // The two deletion vector kinds serialize differently and refuse to merge into one
    // another, so flipping this at the compaction entry point would either fail the rewrite
    // or quietly replace a table's vectors with the other kind.
    if (schema_options.DeletionVectorsBitmap64() != merged_options.DeletionVectorsBitmap64()) {
        changed_options.emplace_back(Options::DELETION_VECTOR_BITMAP64);
    }
    if (schema_options.GetBucket() != merged_options.GetBucket()) {
        changed_options.emplace_back(Options::BUCKET);
    }
    if (schema_options.GetBlobFields() != merged_options.GetBlobFields()) {
        changed_options.emplace_back(Options::BLOB_FIELD);
    }
    if (schema_options.GetBlobDescriptorFields() != merged_options.GetBlobDescriptorFields()) {
        changed_options.emplace_back(Options::BLOB_DESCRIPTOR_FIELD);
    }
    if (schema_options.GetBlobViewFields() != merged_options.GetBlobViewFields()) {
        changed_options.emplace_back(Options::BLOB_VIEW_FIELD);
    }
    if (!changed_options.empty()) {
        return Status::Invalid(
            fmt::format("Compaction options must not change immutable table options [{}]: they "
                        "decide the compaction path and the physical layout the rewrite must "
                        "preserve.",
                        fmt::join(changed_options, ", ")));
    }
    return Status::OK();
}

/// Load schema from table path and merge user options with schema options.
Result<std::pair<std::shared_ptr<TableSchema>, CoreOptions>> LoadSchemaAndOptions(
    const std::string& table_path, const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options, CoreOptions::FromMap(options, file_system));
    SchemaManager schema_manager(tmp_options.GetFileSystem(), table_path);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_table_schema,
                           schema_manager.Latest());
    if (latest_table_schema == std::nullopt) {
        return Status::Invalid("not found latest schema");
    }
    const auto& table_schema = latest_table_schema.value();

    auto final_options = table_schema->Options();
    for (const auto& [key, value] : options) {
        final_options[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(final_options, file_system));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions schema_core_options,
                           CoreOptions::FromMap(table_schema->Options(), file_system));
    PAIMON_RETURN_NOT_OK(ValidateImmutableOptions(schema_core_options, core_options));
    return std::make_pair(table_schema, std::move(core_options));
}

/// Validate that the table is an append-only unaware-bucket table this coordinator can compact.
Status ValidateTable(const std::shared_ptr<TableSchema>& table_schema,
                     const CoreOptions& core_options) {
    if (!table_schema->PrimaryKeys().empty() || core_options.GetBucket() != -1) {
        return Status::Invalid(
            "AppendCompactCoordinator only supports append-only tables "
            "with UNAWARE_BUCKET mode");
    }
    if (core_options.DeletionVectorsEnabled()) {
        // A data-evolution rewrite preserves row ids, so its deletions can be re-keyed onto
        // the rewritten files by DataEvolutionCompactDeletionVectorRewriter. The plain append
        // rewrite reorders rows and has no such migration, so it still refuses.
        if (!core_options.DataEvolutionEnabled()) {
            return Status::NotImplemented(
                "AppendCompactCoordinator does not support deletion vectors in UNAWARE_BUCKET "
                "mode");
        }
    }
    return Status::OK();
}

/// Build the index file handler used to read and rewrite a table's deletion-vector index.
Result<std::shared_ptr<IndexFileHandler>> BuildIndexFileHandler(
    const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexManifestFile> index_manifest_file,
        IndexManifestFile::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                                  core_options.GetManifestCompression(), path_factory,
                                  core_options.GetBucket(), pool, core_options));
    return std::make_shared<IndexFileHandler>(
        core_options.GetFileSystem(), std::move(index_manifest_file),
        std::make_shared<IndexFilePathFactories>(path_factory),
        core_options.DeletionVectorsBitmap64(), pool);
}

/// Build FileStorePathFactory from core options and table schema.
Result<std::shared_ptr<FileStorePathFactory>> BuildPathFactory(
    const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           core_options.CreateGlobalIndexExternalPath());
    return FileStorePathFactory::Create(
        table_path, arrow_schema, table_schema->PartitionKeys(),
        core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
        core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(), external_paths,
        global_index_external_path, core_options.IndexFileInDataFileDir(), pool);
}

/// Scan the latest snapshot and collect files grouped by partition. When `small_files_only` is
/// set, files at or above the compaction trigger size are dropped; a data-evolution plan needs
/// every live file instead, since whether a file takes part depends on its field group, not
/// only on its size.
///
/// `row_id_window`, when set, restricts the scan to the files whose rows fall in that row id
/// window, which is how a batched data-evolution run keeps one round's file metadata bounded.
/// `snapshot_out` receives the snapshot that was scanned, so a later deletion-vector rewrite
/// reads the very same table state.
Result<LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>>> ScanFiles(
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& core_options,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::vector<std::map<std::string, std::string>>& partitions, bool small_files_only,
    const std::optional<Range>& row_id_window, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool, std::optional<Snapshot>* snapshot_out) {
    auto scan_filter = std::make_shared<ScanFilter>(
        /*predicate=*/nullptr, partitions, /*bucket_filter=*/std::nullopt);

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan,
                           CreateFileStoreScan(snapshot_manager, schema_manager, table_schema,
                                               arrow_schema, partition_schema, core_options,
                                               path_factory, scan_filter, executor, pool));
    if (core_options.ManifestDeleteFileDropStats()) {
        scan->EnableDropStats();
    }
    if (row_id_window.has_value()) {
        PAIMON_ASSIGN_OR_RAISE(RowRangeIndex row_range_index,
                               RowRangeIndex::Create({row_id_window.value()}));
        scan->WithRowRangeIndex(row_range_index);
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan, scan->CreatePlan());
    if (snapshot_out != nullptr) {
        *snapshot_out = plan->GetSnapshot();
    }
    std::vector<ManifestEntry> add_entries = plan->Files(FileKind::Add());

    int64_t compaction_file_size = core_options.GetCompactionFileSize(/*has_primary_key=*/false);
    LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>> partition_files;

    for (const auto& entry : add_entries) {
        const auto& file = entry.File();
        if (!small_files_only || file->file_size < compaction_file_size) {
            partition_files[entry.Partition()].push_back(file);
        }
    }
    return partition_files;
}

/// Generate compact tasks from partitioned small files via bin-packing.
std::vector<AppendCompactTask> GenerateCompactTasks(
    const LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>>& partition_files,
    const CoreOptions& core_options) {
    int64_t target_file_size = core_options.GetTargetFileSize(/*has_primary_key=*/false);
    int64_t open_file_cost = core_options.GetSourceSplitOpenFileCost();
    int32_t min_file_num = core_options.GetCompactionMinFileNum();

    std::vector<AppendCompactTask> tasks;
    for (const auto& [partition, files] : partition_files) {
        auto packed_groups = PackFiles(files, target_file_size, open_file_cost, min_file_num);
        for (const auto& group : packed_groups) {
            tasks.emplace_back(partition, group);
        }
    }
    return tasks;
}

/// Cleans up the rewritten output files of already finished tasks, best effort. Used when a
/// later task fails: the collected commit messages are discarded, so their outputs would
/// otherwise linger until an orphan clean. Failures are logged and never mask the original
/// compaction error the caller returns.
void CleanupCompactOutputs(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                           const std::shared_ptr<FileStorePathFactory>& path_factory,
                           const CoreOptions& core_options) {
    auto logger = Logger::GetLogger("AppendCompactCoordinator");
    for (const auto& message : commit_messages) {
        auto message_impl = std::dynamic_pointer_cast<CommitMessageImpl>(message);
        if (!message_impl) {
            // The log macros require at least one format argument.
            PAIMON_LOG_WARN(logger, "%s",
                            "Skipping cleanup of a compact output: unexpected commit message "
                            "type");
            continue;
        }
        Result<std::shared_ptr<DataFilePathFactory>> data_file_path_factory =
            path_factory->CreateDataFilePathFactory(message_impl->Partition(),
                                                    message_impl->Bucket());
        if (!data_file_path_factory.ok()) {
            PAIMON_LOG_WARN(logger,
                            "Skipping cleanup of compact outputs in partition %s bucket %d: %s",
                            message_impl->Partition().ToString().c_str(), message_impl->Bucket(),
                            data_file_path_factory.status().ToString().c_str());
            continue;
        }
        for (const auto& file : message_impl->GetCompactIncrement().CompactAfter()) {
            for (const auto& path : data_file_path_factory.value()->CollectFiles(file)) {
                auto status = core_options.GetFileSystem()->Delete(path);
                if (!status.ok()) {
                    PAIMON_LOG_WARN(logger, "Failed to delete compact output %s: %s", path.c_str(),
                                    status.ToString().c_str());
                }
            }
        }
    }
}

/// Execute data-evolution compact tasks synchronously and collect commit messages.
Result<std::vector<std::shared_ptr<CommitMessage>>> ExecuteDataEvolutionNormalCompactTasks(
    std::vector<DataEvolutionNormalCompactTask>&& tasks, const std::string& table_path,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& core_options,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    DataEvolutionCompactContext context{table_path,   table_schema, arrow_schema, core_options,
                                        path_factory, executor,     pool};
    auto logger = Logger::GetLogger("AppendCompactCoordinator");
    PAIMON_LOG_DEBUG(logger, "Executing %zu data-evolution compact tasks for table %s",
                     tasks.size(), table_path.c_str());

    std::vector<std::shared_ptr<CommitMessage>> commit_messages;
    commit_messages.reserve(tasks.size());
    for (auto& task : tasks) {
        Result<std::shared_ptr<CommitMessage>> message = task.DoCompact(context);
        if (!message.ok()) {
            PAIMON_LOG_WARN(logger, "Data-evolution compact task failed: %s, status: %s",
                            task.ToString().c_str(), message.status().ToString().c_str());
            // The failed task aborted its own output; the finished tasks' outputs will never
            // be committed anymore, so remove them too.
            CleanupCompactOutputs(commit_messages, path_factory, core_options);
            return message.status();
        }
        commit_messages.push_back(std::move(message).value());
    }
    return commit_messages;
}

/// Execute compact tasks synchronously and collect commit messages.
Result<std::vector<std::shared_ptr<CommitMessage>>> ExecuteCompactTasks(
    std::vector<AppendCompactTask>&& tasks,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager, const std::string& table_path,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& core_options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    auto write = CreateFileStoreWrite(path_factory, snapshot_manager, schema_manager, table_path,
                                      table_schema, arrow_schema, partition_schema, core_options,
                                      executor, pool);

    std::vector<std::shared_ptr<CommitMessage>> commit_messages;
    commit_messages.reserve(tasks.size());

    for (auto& task : tasks) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CommitMessage> message,
                               task.DoCompact(core_options, write.get()));
        commit_messages.push_back(std::move(message));
    }
    return commit_messages;
}

/// Commits one round's messages under `commit_user`.
///
/// The compaction options are passed through so the commit reaches the table through the same
/// file system and manifest settings the rest of the run used.
///
/// `materialize_row_id_check_from_snapshot` carries the snapshot a materialization was planned
/// against. Such a commit drops the rows of the ranges it rewrites and has the commit assign
/// new row ids to the survivors, so anything another writer added over those ranges since then
/// has to fail the commit rather than be left addressing rows that moved.
Status CommitRound(
    const std::string& table_path, const std::string& commit_user, const CoreOptions& core_options,
    const std::vector<std::shared_ptr<CommitMessage>>& messages,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool,
    const std::optional<int64_t>& materialize_row_id_check_from_snapshot = std::nullopt) {
    CommitContextBuilder builder(table_path, commit_user);
    builder.SetOptions(core_options.ToMap())
        .WithFileSystem(core_options.GetFileSystem())
        .WithExecutor(executor)
        .WithMemoryPool(pool);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context, builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> file_store_commit,
                           FileStoreCommit::Create(std::move(commit_context)));
    if (materialize_row_id_check_from_snapshot) {
        file_store_commit->RowIdCheckConflictForMaterializeDeletionVectors(
            materialize_row_id_check_from_snapshot);
    }
    return file_store_commit->Commit(messages);
}

/// Builds the deletion file of each data file of one partition from the index metas the
/// snapshot records for it, which already say where every vector sits.
Result<std::map<std::string, DeletionFile>> CollectDeletionFiles(
    const std::shared_ptr<IndexFileHandler>& index_file_handler, const BinaryRow& partition,
    const std::vector<std::shared_ptr<IndexFileMeta>>& index_metas) {
    std::map<std::string, DeletionFile> result;
    for (const auto& index_meta : index_metas) {
        const std::optional<LinkedHashMap<std::string, DeletionVectorMeta>>& dv_metas =
            index_meta->DvRanges();
        if (dv_metas == std::nullopt) {
            return Status::Invalid(
                fmt::format("Deletion vector index file {} has no deletion vector metas.",
                            index_meta->FileName()));
        }
        PAIMON_ASSIGN_OR_RAISE(std::string path,
                               index_file_handler->FilePath(partition, /*bucket=*/0, index_meta));
        for (const auto& [data_file_name, dv_meta] : dv_metas.value()) {
            auto inserted = result.emplace(
                data_file_name, DeletionFile(path, dv_meta.GetOffset(), dv_meta.GetLength(),
                                             dv_meta.GetCardinality()));
            if (!inserted.second) {
                return Status::Invalid(fmt::format(
                    "Data file {} has a deletion vector in more than one index file of the same "
                    "partition.",
                    data_file_name));
            }
        }
    }
    return result;
}

/// Plans one materialize task per contiguous row id run that carries deletions.
///
/// Runs are not packed into bins by their estimated size after the deletions are applied: the
/// run is streamed through the reader and the output is split by the table's ordinary rolling
/// rules, so a long run costs bounded memory either way, and a run without a single deletion
/// is skipped rather than rewritten.
Result<std::vector<DataEvolutionMaterializeDeletionCompactTask>> PlanMaterializeTasks(
    const LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>>& partition_files,
    const std::shared_ptr<IndexFileHandler>& index_file_handler, const Snapshot& snapshot) {
    // One scan for every partition of the round rather than one per partition: the index
    // manifest is a single file, and scanning it per partition would read it once per
    // partition the round touched.
    std::unordered_set<BinaryRow> partitions;
    for (const auto& [partition, files] : partition_files) {
        partitions.insert(partition);
    }
    IndexFileHandler::IndexFileMetaGroups index_metas_by_group;
    PAIMON_ASSIGN_OR_RAISE(
        index_metas_by_group,
        index_file_handler->Scan(snapshot, DeletionVectorsIndexFile::DELETION_VECTORS_INDEX,
                                 partitions));

    std::vector<DataEvolutionMaterializeDeletionCompactTask> tasks;
    for (const auto& [partition, files] : partition_files) {
        auto index_metas = index_metas_by_group.find({partition, /*bucket=*/0});
        if (index_metas == index_metas_by_group.end()) {
            continue;
        }
        std::map<std::string, DeletionFile> deletion_files;
        PAIMON_ASSIGN_OR_RAISE(deletion_files, CollectDeletionFiles(index_file_handler, partition,
                                                                    index_metas->second));
        if (deletion_files.empty()) {
            continue;
        }
        // Contiguous runs, the same adjacency the compaction planner uses: files whose ranges
        // touch belong to one run and have to be rewritten together, because a run is what a
        // reassignment of row ids can be kept self-consistent over.
        std::vector<std::shared_ptr<DataFileMeta>> run_candidates = files;
        RangeHelper<std::shared_ptr<DataFileMeta>> adjacency_helper(
            [](const std::shared_ptr<DataFileMeta>& file) -> Result<int64_t> {
                return file->NonNullFirstRowId();
            },
            [](const std::shared_ptr<DataFileMeta>& file) -> Result<int64_t> {
                PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file->NonNullFirstRowId());
                return first_row_id + file->row_count;
            });
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> runs,
                               adjacency_helper.MergeOverlappingRanges(std::move(run_candidates)));
        for (auto& run : runs) {
            std::vector<std::optional<DeletionFile>> aligned;
            aligned.reserve(run.size());
            bool has_deletions = false;
            for (const auto& file : run) {
                auto deletion_file = deletion_files.find(file->file_name);
                if (deletion_file == deletion_files.end()) {
                    aligned.push_back(std::nullopt);
                } else {
                    aligned.push_back(deletion_file->second);
                    has_deletions = true;
                }
            }
            if (!has_deletions) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(
                DataEvolutionMaterializeDeletionCompactTask task,
                DataEvolutionMaterializeDeletionCompactTask::Create(partition, run, aligned));
            tasks.push_back(std::move(task));
        }
    }
    return tasks;
}

/// Runs one data-evolution compaction round over `row_id_window`: scan, plan, rewrite the
/// files, then move the deletion vectors of the rewritten row range groups onto the new
/// files.
///
/// The returned messages belong to one atomic commit: the index-only messages re-key the
/// deletions of exactly the files the data messages replace, so committing the data without
/// them would resurrect deleted rows.
Result<std::vector<std::shared_ptr<CommitMessage>>> RunDataEvolutionRound(
    const std::string& table_path, const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& core_options,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::vector<std::map<std::string, std::string>>& partitions,
    const std::optional<Range>& row_id_window, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool) {
    std::optional<Snapshot> snapshot;
    LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>> partition_files;
    PAIMON_ASSIGN_OR_RAISE(
        partition_files,
        ScanFiles(snapshot_manager, schema_manager, table_schema, arrow_schema, partition_schema,
                  core_options, path_factory, partitions, /*small_files_only=*/false, row_id_window,
                  executor, pool, &snapshot));
    if (partition_files.empty()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }

    // Data-evolution tables compact across evolved field groups while preserving row ids; the
    // plain append rewrite would reorder rows and drop the column merge, so it must not run
    // here.
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<DataEvolutionNormalCompactTask> tasks,
        DataEvolutionCompactPlanner::PlanCompactTasks(partition_files, core_options));
    if (tasks.empty()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> messages,
                           ExecuteDataEvolutionNormalCompactTasks(
                               std::move(tasks), table_path, table_schema, arrow_schema,
                               core_options, path_factory, executor, pool));
    // Short-circuit before building the index handler. The rewriter re-checks the option
    // itself, so this is an optimization rather than the safety net.
    if (!core_options.DeletionVectorsEnabled() || messages.empty()) {
        return messages;
    }

    // The rewrite kept every input row, so the deletions of the replaced files still apply —
    // they only have to be re-keyed onto the file that now holds those rows.
    if (!snapshot.has_value()) {
        return Status::Invalid(
            "Data-evolution compaction planned files without a snapshot to rewrite deletion "
            "vectors against.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexFileHandler> index_file_handler,
                           BuildIndexFileHandler(path_factory, core_options, pool));
    Result<std::vector<std::shared_ptr<CommitMessage>>> index_messages =
        DataEvolutionCompactDeletionVectorRewriter::RewriteDeletionVectors(
            messages, snapshot.value(), core_options, index_file_handler);
    if (!index_messages.ok()) {
        // The rewritten data files can never be committed without their deletions, so drop
        // them instead of leaving them for an orphan clean.
        CleanupCompactOutputs(messages, path_factory, core_options);
        return index_messages.status();
    }
    for (auto& index_message : index_messages.value()) {
        messages.push_back(std::move(index_message));
    }
    return messages;
}

/// Runs one materialization round over `row_id_window`.
///
/// Scans the live files the window covers, rewrites every row range in it that carries
/// deletions, and returns the commit messages of that rewrite together with the index changes
/// it needs: the vectors that go away with the rows they deleted, and the global indexes the
/// new row ids invalidate. Empty when the window has nothing to materialize.
///
/// `planned_snapshot_id` receives the snapshot the round planned against, which the commit
/// checks concurrent writers against.
Result<std::vector<std::shared_ptr<CommitMessage>>> RunMaterializeRound(
    const std::string& table_path, const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& core_options,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::vector<std::map<std::string, std::string>>& partitions,
    const std::optional<Range>& row_id_window, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool, Logger* logger,
    std::optional<int64_t>* planned_snapshot_id) {
    // Every live file of the window is needed, not only the small ones: a range is materialized
    // because it carries deletions, whatever its files weigh.
    std::optional<Snapshot> snapshot;
    LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>> partition_files;
    PAIMON_ASSIGN_OR_RAISE(
        partition_files,
        ScanFiles(snapshot_manager, schema_manager, table_schema, arrow_schema, partition_schema,
                  core_options, path_factory, partitions, /*small_files_only=*/false, row_id_window,
                  executor, pool, &snapshot));
    if (partition_files.empty() || !snapshot.has_value()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }
    *planned_snapshot_id = snapshot.value().Id();

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexFileHandler> index_file_handler,
                           BuildIndexFileHandler(path_factory, core_options, pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<DataEvolutionMaterializeDeletionCompactTask> tasks,
        PlanMaterializeTasks(partition_files, index_file_handler, snapshot.value()));
    if (tasks.empty()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }
    PAIMON_LOG_DEBUG(logger, "Materializing deletion vectors of table %s in %zu task(s)",
                     table_path.c_str(), tasks.size());

    DataEvolutionCompactContext context{table_path,   table_schema, arrow_schema, core_options,
                                        path_factory, executor,     pool};
    std::vector<std::shared_ptr<CommitMessage>> messages;
    messages.reserve(tasks.size());
    for (auto& task : tasks) {
        Result<std::shared_ptr<CommitMessage>> message = task.DoCompact(context);
        if (!message.ok()) {
            PAIMON_LOG_WARN(logger, "Materialize deletion task failed: %s, status: %s",
                            task.ToString().c_str(), message.status().ToString().c_str());
            CleanupCompactOutputs(messages, path_factory, core_options);
            return message.status();
        }
        messages.push_back(std::move(message).value());
    }

    // The old vectors are removed rather than moved, and the global indexes of the touched
    // partitions go with them: their row ids no longer exist. Both travel in the same commit as
    // the rewritten data, so no reader ever sees the rows without their deletions applied.
    Result<std::vector<std::shared_ptr<CommitMessage>>> index_messages =
        DataEvolutionCompactDeletionVectorRewriter::RewriteDeletionVectors(
            messages, snapshot.value(), core_options, index_file_handler);
    if (!index_messages.ok()) {
        CleanupCompactOutputs(messages, path_factory, core_options);
        return index_messages.status();
    }
    // Scanned at the newest snapshot rather than the planned one, and the commit re-derives the
    // deletions on every attempt, so an index another engine commits while this runs is dropped
    // by this commit or by the retry it forces.
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                           snapshot_manager->LatestSnapshot());
    Result<std::vector<std::shared_ptr<CommitMessage>>> dropped_indexes =
        DataEvolutionCompactGlobalIndexDropper::DropGlobalIndexes(
            messages, latest_snapshot.has_value() ? latest_snapshot.value() : snapshot.value(),
            index_file_handler);
    if (!dropped_indexes.ok()) {
        CleanupCompactOutputs(messages, path_factory, core_options);
        return dropped_indexes.status();
    }
    for (auto& index_message : index_messages.value()) {
        messages.push_back(std::move(index_message));
    }
    for (auto& dropped_index : dropped_indexes.value()) {
        messages.push_back(std::move(dropped_index));
    }
    return messages;
}

}  // namespace

Result<std::vector<std::shared_ptr<CommitMessage>>> AppendCompactCoordinator::Run(
    const std::string& table_path, const std::map<std::string, std::string>& options,
    const std::vector<std::map<std::string, std::string>>& partitions,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& input_pool) {
    auto pool = input_pool ? input_pool : GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();

    // Load schema and merge options
    std::pair<std::shared_ptr<TableSchema>, CoreOptions> schema_and_options;
    PAIMON_ASSIGN_OR_RAISE(schema_and_options,
                           LoadSchemaAndOptions(table_path, options, file_system));
    const auto& [table_schema, core_options] = schema_and_options;

    // Validate table type
    PAIMON_RETURN_NOT_OK(ValidateTable(table_schema, core_options));

    // Build shared objects
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));

    auto snapshot_manager =
        std::make_shared<SnapshotManager>(core_options.GetFileSystem(), table_path);
    auto schema_manager = std::make_shared<SchemaManager>(core_options.GetFileSystem(), table_path);

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        BuildPathFactory(table_path, table_schema, arrow_schema, core_options, pool));

    // The legacy row-id rewriting mode is gone; a table whose options still carry it fails
    // instead of having the option silently ignored. Checked before any scanning, so even an
    // empty table fails loudly.
    bool data_evolution = core_options.DataEvolutionEnabled();
    if (data_evolution && core_options.DataEvolutionCompactionRewriteRowIds()) {
        return Status::Invalid(fmt::format(
            "'{}' is no longer supported: normal data-evolution compaction preserves row ids "
            "and logical deletions.",
            Options::DATA_EVOLUTION_COMPACTION_REWRITE_ROW_IDS));
    }

    if (data_evolution) {
        // One round over the whole snapshot: Run hands its messages back for the caller to
        // commit, so it cannot split the work across commits. RunAndCommit does that.
        return RunDataEvolutionRound(table_path, snapshot_manager, schema_manager, table_schema,
                                     arrow_schema, partition_schema, core_options, path_factory,
                                     partitions, /*row_id_window=*/std::nullopt, executor, pool);
    }

    LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>> partition_files;
    PAIMON_ASSIGN_OR_RAISE(
        partition_files,
        ScanFiles(snapshot_manager, schema_manager, table_schema, arrow_schema, partition_schema,
                  core_options, path_factory, partitions, /*small_files_only=*/true,
                  /*row_id_window=*/std::nullopt, executor, pool, /*snapshot_out=*/nullptr));

    if (partition_files.empty()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }

    // Generate compact tasks via bin-packing
    std::vector<AppendCompactTask> tasks = GenerateCompactTasks(partition_files, core_options);
    if (tasks.empty()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }

    // Execute compact tasks synchronously
    return ExecuteCompactTasks(std::move(tasks), path_factory, snapshot_manager, schema_manager,
                               table_path, table_schema, arrow_schema, partition_schema,
                               core_options, executor, pool);
}

Result<int32_t> AppendCompactCoordinator::RunAndCommit(
    const std::string& table_path, const std::map<std::string, std::string>& options,
    const std::vector<std::map<std::string, std::string>>& partitions,
    const std::string& commit_user, const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& input_pool, int64_t candidate_files_per_round) {
    if (commit_user.empty()) {
        return Status::Invalid("AppendCompactCoordinator::RunAndCommit needs a commit user.");
    }
    if (candidate_files_per_round <= 0) {
        return Status::Invalid(fmt::format(
            "candidate_files_per_round must be positive, but was {}.", candidate_files_per_round));
    }
    auto pool = input_pool ? input_pool : GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto logger = Logger::GetLogger("AppendCompactCoordinator");

    std::pair<std::shared_ptr<TableSchema>, CoreOptions> schema_and_options;
    PAIMON_ASSIGN_OR_RAISE(schema_and_options,
                           LoadSchemaAndOptions(table_path, options, file_system));
    const auto& [table_schema, core_options] = schema_and_options;
    PAIMON_RETURN_NOT_OK(ValidateTable(table_schema, core_options));

    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
    auto snapshot_manager =
        std::make_shared<SnapshotManager>(core_options.GetFileSystem(), table_path);
    auto schema_manager = std::make_shared<SchemaManager>(core_options.GetFileSystem(), table_path);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        BuildPathFactory(table_path, table_schema, arrow_schema, core_options, pool));

    if (!core_options.DataEvolutionEnabled()) {
        // A plain append table has no row id space to split on, so it keeps the single-round
        // behaviour and only gains the commit.
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> messages,
                               Run(table_path, options, partitions, file_system, pool));
        if (messages.empty()) {
            return 0;
        }
        PAIMON_RETURN_NOT_OK(
            CommitRound(table_path, commit_user, core_options, messages, executor, pool));
        return 1;
    }
    if (core_options.DataEvolutionCompactionRewriteRowIds()) {
        return Status::Invalid(fmt::format(
            "'{}' is no longer supported: normal data-evolution compaction preserves row ids "
            "and logical deletions.",
            Options::DATA_EVOLUTION_COMPACTION_REWRITE_ROW_IDS));
    }

    // The windows are planned once, against the snapshot the run starts from; every round
    // then re-scans and sees the rounds committed before it. That re-scan re-reads the
    // snapshot and the manifest list, but not most of the manifest files: a manifest is
    // pruned by its recorded row id range, and because windows are cut only at coverage gaps
    // it belongs to exactly one round. `DataEvolutionCompactPlanner::PlanRowIdWindows`
    // documents the one exception, a delete-only manifest without row id statistics.
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, snapshot_manager->LatestSnapshot());
    if (!snapshot.has_value()) {
        return 0;
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                             core_options.GetManifestCompression(), path_factory,
                             core_options.GetCache(), pool));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<PredicateFilter> partition_filter,
                           FileStoreScan::CreatePartitionPredicate(
                               table_schema->PartitionKeys(),
                               core_options.GetPartitionDefaultName(), arrow_schema, partitions));
    PAIMON_ASSIGN_OR_RAISE(std::vector<Range> windows,
                           DataEvolutionCompactPlanner::PlanRowIdWindows(
                               manifest_list, snapshot.value(), partition_filter, partition_schema,
                               candidate_files_per_round, logger.get()));
    if (windows.empty()) {
        // Nothing to split on; one unbounded round still commits correctly.
        windows.emplace_back(0, std::numeric_limits<int64_t>::max());
    }
    PAIMON_LOG_DEBUG(logger, "Compacting table %s in %zu round(s)", table_path.c_str(),
                     windows.size());

    int32_t committed_rounds = 0;
    for (const auto& window : windows) {
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::shared_ptr<CommitMessage>> messages,
            RunDataEvolutionRound(table_path, snapshot_manager, schema_manager, table_schema,
                                  arrow_schema, partition_schema, core_options, path_factory,
                                  partitions, window, executor, pool));
        if (messages.empty()) {
            continue;
        }
        Status commit_status =
            CommitRound(table_path, commit_user, core_options, messages, executor, pool);
        if (!commit_status.ok()) {
            // The round's rewritten data files are unreachable once its commit failed, and the
            // rounds already committed stay committed: compaction is idempotent, a later run
            // re-plans whatever is left. A deletion-vector index file the round may have
            // written is left to orphan cleaning, which is what collects unreferenced index
            // files.
            CleanupCompactOutputs(messages, path_factory, core_options);
            return commit_status;
        }
        committed_rounds++;
    }
    return committed_rounds;
}

Result<int32_t> AppendCompactCoordinator::MaterializeDeletionVectors(
    const std::string& table_path, const std::map<std::string, std::string>& options,
    const std::vector<std::map<std::string, std::string>>& partitions,
    const std::string& commit_user, const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& input_pool) {
    if (commit_user.empty()) {
        return Status::Invalid(
            "AppendCompactCoordinator::MaterializeDeletionVectors needs a commit user.");
    }
    auto pool = input_pool ? input_pool : GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto logger = Logger::GetLogger("AppendCompactCoordinator");

    std::pair<std::shared_ptr<TableSchema>, CoreOptions> schema_and_options;
    PAIMON_ASSIGN_OR_RAISE(schema_and_options,
                           LoadSchemaAndOptions(table_path, options, file_system));
    const auto& [table_schema, core_options] = schema_and_options;
    PAIMON_RETURN_NOT_OK(ValidateTable(table_schema, core_options));
    if (!core_options.DataEvolutionEnabled()) {
        return Status::Invalid(
            "Materializing deletion vectors is only supported for data-evolution tables.");
    }
    if (!core_options.DeletionVectorsEnabled()) {
        return Status::Invalid(
            "Materializing deletion vectors needs 'deletion-vectors.enabled' to be set.");
    }

    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
    auto snapshot_manager =
        std::make_shared<SnapshotManager>(core_options.GetFileSystem(), table_path);
    auto schema_manager = std::make_shared<SchemaManager>(core_options.GetFileSystem(), table_path);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        BuildPathFactory(table_path, table_schema, arrow_schema, core_options, pool));

    // The windows are planned once, against the snapshot this call starts from, and every
    // round then re-scans so it observes the rounds committed before it. Materialized rows are
    // renumbered from the table's next row id, past every row id the commits handed out, so a
    // later round never re-plans what an earlier one rewrote.
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> start_snapshot,
                           snapshot_manager->LatestSnapshot());
    if (!start_snapshot.has_value()) {
        return 0;
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                             core_options.GetManifestCompression(), path_factory,
                             core_options.GetCache(), pool));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<PredicateFilter> partition_filter,
                           FileStoreScan::CreatePartitionPredicate(
                               table_schema->PartitionKeys(),
                               core_options.GetPartitionDefaultName(), arrow_schema, partitions));
    PAIMON_ASSIGN_OR_RAISE(std::vector<Range> windows,
                           DataEvolutionCompactPlanner::PlanRowIdWindows(
                               manifest_list, start_snapshot.value(), partition_filter,
                               partition_schema, kDefaultCandidateFilesPerRound, logger.get()));
    if (windows.empty()) {
        // Nothing to split on; one unbounded round still materializes correctly.
        windows.emplace_back(0, std::numeric_limits<int64_t>::max());
    }
    PAIMON_LOG_DEBUG(logger, "Materializing deletion vectors of table %s in %zu round(s)",
                     table_path.c_str(), windows.size());

    int32_t committed_rounds = 0;
    for (const auto& window : windows) {
        std::optional<int64_t> planned_snapshot_id;
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::shared_ptr<CommitMessage>> messages,
            RunMaterializeRound(table_path, snapshot_manager, schema_manager, table_schema,
                                arrow_schema, partition_schema, core_options, path_factory,
                                partitions, window, executor, pool, logger.get(),
                                &planned_snapshot_id));
        if (messages.empty()) {
            continue;
        }
        Status commit_status =
            CommitRound(table_path, commit_user, core_options, messages, executor, pool,
                        /*materialize_row_id_check_from_snapshot=*/planned_snapshot_id);
        if (!commit_status.ok()) {
            // The round's rewritten files are unreachable once its commit failed, and the
            // rounds already committed stay committed: a later call re-plans whatever is left.
            CleanupCompactOutputs(messages, path_factory, core_options);
            return commit_status;
        }
        committed_rounds++;
    }
    if (committed_rounds == 0) {
        PAIMON_LOG_DEBUG(
            logger, "Nothing to materialize for table %s: no row range carries a deletion vector.",
            table_path.c_str());
    }
    return committed_rounds;
}

}  // namespace paimon
