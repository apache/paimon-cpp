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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/append/append_compact_task.h"
#include "paimon/core/append/data_evolution_compact_coordinator.h"
#include "paimon/core/append/data_evolution_normal_compact_task.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/manifest/file_kind.h"
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
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"
#include "paimon/memory/memory_pool.h"
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
/// ids) or slip a deletion-vector table past its rejection. Mirrors the immutable-option
/// check of Java's AbstractFileStoreTable#copy, compared on the parsed effective values so
/// that passing an explicit default is still allowed.
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

/// Validate that the table is an append-only unaware-bucket table without DV.
Status ValidateTable(const std::shared_ptr<TableSchema>& table_schema,
                     const CoreOptions& core_options) {
    if (!table_schema->PrimaryKeys().empty() || core_options.GetBucket() != -1) {
        return Status::Invalid(
            "AppendCompactCoordinator only supports append-only tables "
            "with UNAWARE_BUCKET mode");
    }
    if (core_options.DeletionVectorsEnabled()) {
        return Status::NotImplemented(
            "AppendCompactCoordinator does not support deletion vectors in UNAWARE_BUCKET mode");
    }
    return Status::OK();
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
Result<LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>>> ScanFiles(
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& core_options,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::vector<std::map<std::string, std::string>>& partitions, bool small_files_only,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    auto scan_filter = std::make_shared<ScanFilter>(
        /*predicate=*/nullptr, partitions, /*bucket_filter=*/std::nullopt);

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan,
                           CreateFileStoreScan(snapshot_manager, schema_manager, table_schema,
                                               arrow_schema, partition_schema, core_options,
                                               path_factory, scan_filter, executor, pool));
    if (core_options.ManifestDeleteFileDropStats()) {
        scan->EnableDropStats();
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan, scan->CreatePlan());
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

    // The legacy row-id rewriting mode was removed in Java; honor the same contract for
    // tables whose options still carry it, instead of silently ignoring the option. Checked
    // before any scanning, so even an empty table fails loudly.
    bool data_evolution = core_options.DataEvolutionEnabled();
    if (data_evolution && core_options.DataEvolutionCompactionRewriteRowIds()) {
        return Status::Invalid(fmt::format(
            "'{}' is no longer supported: normal data-evolution compaction preserves row ids "
            "and logical deletions.",
            Options::DATA_EVOLUTION_COMPACTION_REWRITE_ROW_IDS));
    }

    // Scan files from the latest snapshot. A data-evolution plan considers every live file,
    // the plain append plan only small ones.
    LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>> partition_files;
    PAIMON_ASSIGN_OR_RAISE(partition_files,
                           ScanFiles(snapshot_manager, schema_manager, table_schema, arrow_schema,
                                     partition_schema, core_options, path_factory, partitions,
                                     /*small_files_only=*/!data_evolution, executor, pool));

    if (partition_files.empty()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }

    if (data_evolution) {
        // Data-evolution tables compact across evolved field groups while preserving row ids;
        // the plain append rewrite would reorder rows and drop the column merge, so it must
        // not run here.
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<DataEvolutionNormalCompactTask> tasks,
            DataEvolutionCompactCoordinator::PlanCompactTasks(partition_files, core_options));
        if (tasks.empty()) {
            return std::vector<std::shared_ptr<CommitMessage>>{};
        }
        return ExecuteDataEvolutionNormalCompactTasks(std::move(tasks), table_path, table_schema,
                                                      arrow_schema, core_options, path_factory,
                                                      executor, pool);
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

}  // namespace paimon
