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

#include "paimon/append/append_file_compaction_planner.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <unordered_map>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/file_index/file_index_format_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"

namespace paimon {
namespace {

// Keep the pool alive until the last embedded-index reference releases its bytes.
struct EmbeddedIndexOwner {
    std::shared_ptr<MemoryPool> pool;
    std::shared_ptr<Bytes> bytes;
};

Result<std::optional<FileIndexMeta>> ReadFileIndexMeta(
    const std::shared_ptr<DataFileMeta>& file,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool) {
    FileIndexMeta meta;
    std::shared_ptr<InputStream> input;
    if (file->embedded_index) {
        auto owner =
            std::make_shared<EmbeddedIndexOwner>(EmbeddedIndexOwner{pool, file->embedded_index});
        meta.embedded_index = std::shared_ptr<const Bytes>(owner, owner->bytes.get());
        input = std::make_shared<ByteArrayInputStream>(meta.embedded_index->data(),
                                                       meta.embedded_index->size());
    } else {
        for (const auto& extra_file : file->extra_files) {
            if (!extra_file || !StringUtils::EndsWith(extra_file.value(),
                                                      DataFilePathFactory::INDEX_PATH_SUFFIX)) {
                continue;
            }
            if (meta.index_file_path) {
                return Status::Invalid("multiple file index containers for ", file->file_name);
            }
            meta.index_file_path = path_factory->ToAlignedPath(extra_file.value(), file);
        }
        if (!meta.index_file_path) {
            return std::optional<FileIndexMeta>();
        }
        PAIMON_ASSIGN_OR_RAISE(input, file_system->Open(meta.index_file_path.value()));
    }
    ScopeGuard close_guard([&input]() { (void)input->Close(); });
    PAIMON_ASSIGN_OR_RAISE(FileIndexFormatUtils::FileIndexEntries entries,
                           FileIndexFormatUtils::ReadIndexEntries(input, pool));
    PAIMON_ASSIGN_OR_RAISE(int64_t container_length, input->Length());
    for (const auto& [column, indexes] : entries) {
        auto& ranges = meta.indexes[column];
        for (const auto& [type, range] : indexes) {
            const auto& [offset, length] = range;
            if (offset == FileIndexFormat::EMPTY_INDEX_FLAG) {
                if (length != 0) {
                    return Status::Invalid("invalid empty index range for ", file->file_name);
                }
            } else if (offset < 0 || length < 0 || offset > container_length ||
                       length > container_length - offset) {
                return Status::Invalid("index range is outside container for ", file->file_name);
            }
            ranges.emplace(type, FileIndexRange{range.first, range.second});
        }
    }
    close_guard.Release();
    PAIMON_RETURN_NOT_OK(input->Close());
    return std::optional<FileIndexMeta>(std::move(meta));
}

Status BuildRowIdMapping(const std::shared_ptr<DeletionVector>& dv, int64_t target_start,
                         FileCompactionInfo* info) {
    if (info->source_row_count < 0) {
        return Status::Invalid("negative row count for ", info->file_name);
    }
    int64_t deleted_count = 0;
    if (dv) {
        PAIMON_ASSIGN_OR_RAISE(deleted_count, dv->GetCardinality());
    }
    if (deleted_count < 0 || deleted_count > info->source_row_count) {
        return Status::Invalid("invalid deletion vector cardinality for ", info->file_name);
    }
    info->live_row_count = info->source_row_count - deleted_count;
    if (info->live_row_count > std::numeric_limits<int64_t>::max() - target_start) {
        return Status::Invalid("compacted row count exceeds int64 range");
    }

    int64_t source_start = 0;
    int64_t mapped_count = 0;
    int64_t visited_deletions = 0;
    auto append_range = [&](int64_t end) -> bool {
        if (end > source_start) {
            int64_t length = end - source_start;
            if (length > info->live_row_count - mapped_count) {
                return false;
            }
            info->row_id_mapping.push_back({source_start, target_start + mapped_count, length});
            mapped_count += length;
        }
        return true;
    };
    if (dv) {
        bool invalid_position = false;
        // Both supported DV encodings enumerate deleted positions in ascending order.
        PAIMON_RETURN_NOT_OK(dv->ForEachDeletedPosition([&](int64_t position) {
            if (invalid_position) {
                return;
            }
            if (position < source_start || position >= info->source_row_count) {
                invalid_position = true;
                return;
            }
            if (!append_range(position)) {
                invalid_position = true;
                return;
            }
            source_start = position + 1;
            ++visited_deletions;
        }));
        if (invalid_position || visited_deletions != deleted_count) {
            return Status::Invalid("invalid deletion vector positions for ", info->file_name);
        }
    }
    if (!append_range(info->source_row_count) || mapped_count != info->live_row_count) {
        return Status::Invalid("deletion vector cardinality does not match positions for ",
                               info->file_name);
    }
    return Status::OK();
}

struct PlanningContext {
    Snapshot snapshot;
    std::shared_ptr<SnapshotManager> snapshot_manager;
    std::shared_ptr<SchemaManager> schema_manager;
    std::shared_ptr<TableSchema> table_schema;
    CoreOptions core_options;
};

using DeletionVectors = std::map<std::string, std::shared_ptr<DeletionVector>>;

Status ValidateInputs(const std::string& table_path, const std::set<std::string>& file_names,
                      const std::optional<int64_t>& snapshot_id) {
    if (table_path.empty()) {
        return Status::Invalid("table path is empty");
    }
    if (file_names.empty()) {
        return Status::Invalid("compaction planning requires at least one input file");
    }
    for (const auto& name : file_names) {
        PAIMON_RETURN_NOT_OK(PathUtil::CheckSinglePathComponent("data file", name));
    }
    if (snapshot_id && snapshot_id.value() < Snapshot::FIRST_SNAPSHOT_ID) {
        return Status::Invalid("snapshot id must be greater than or equal to 1");
    }
    return Status::OK();
}

Result<PlanningContext> LoadPlanningContext(const std::string& table_path,
                                            const std::string& normalized_branch,
                                            const std::optional<int64_t>& snapshot_id,
                                            const std::map<std::string, std::string>& options,
                                            const std::shared_ptr<FileSystem>& file_system) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions temporary_options,
                           CoreOptions::FromMap(options, file_system));
    SchemaManager temporary_schema_manager(temporary_options.GetFileSystem(), table_path,
                                           normalized_branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           temporary_schema_manager.Latest());
    if (!latest_schema) {
        return Status::Invalid("not found latest schema");
    }
    std::map<std::string, std::string> dynamic_options = options;
    dynamic_options[Options::BRANCH] = normalized_branch;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> merged_schema,
                           latest_schema.value()->MergeOptions(dynamic_options));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(merged_schema->Options(), file_system));
    auto snapshot_manager = std::make_shared<SnapshotManager>(core_options.GetFileSystem(),
                                                              table_path, normalized_branch);
    auto schema_manager = std::make_shared<SchemaManager>(core_options.GetFileSystem(), table_path,
                                                          normalized_branch);
    std::optional<Snapshot> snapshot;
    if (snapshot_id) {
        PAIMON_ASSIGN_OR_RAISE(Snapshot loaded_snapshot,
                               snapshot_manager->LoadSnapshot(snapshot_id.value()));
        snapshot = std::move(loaded_snapshot);
    } else {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager->LatestSnapshot());
    }
    if (!snapshot) {
        return Status::Invalid("compaction planning requires a committed snapshot");
    }
    return PlanningContext{std::move(snapshot.value()), std::move(snapshot_manager),
                           std::move(schema_manager), std::move(merged_schema),
                           std::move(core_options)};
}

Status ValidateTable(const PlanningContext& context,
                     const std::map<std::string, std::string>& partition, int32_t bucket) {
    const auto& table_schema = context.table_schema;
    const auto& core_options = context.core_options;
    if (!table_schema->PrimaryKeys().empty() || core_options.GetBucket() <= 0 ||
        core_options.DataEvolutionEnabled() || core_options.RealtimeEnabled() ||
        !core_options.GetRealtimeDeduplicateKeyFields().empty()) {
        return Status::NotImplemented(
            "compaction planning only supports fixed-bucket append tables without real-time "
            "mode or data evolution");
    }
    if (bucket < 0 || bucket >= core_options.GetBucket()) {
        return Status::Invalid("compaction planning bucket is out of range");
    }
    if (partition.size() != table_schema->PartitionKeys().size()) {
        return Status::Invalid("compaction planning requires exactly the table partition keys");
    }
    return Status::OK();
}

Result<std::shared_ptr<FileStorePathFactory>> CreatePathFactory(
    const std::string& table_path, const PlanningContext& context,
    const std::shared_ptr<arrow::Schema>& arrow_schema, const std::shared_ptr<MemoryPool>& pool) {
    const auto& table_schema = context.table_schema;
    const auto& core_options = context.core_options;
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> external_path_weights,
                           core_options.GetExternalPathWeights());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> global_index_external_paths,
                           core_options.CreateGlobalIndexExternalPaths());
    return FileStorePathFactory::Create(
        table_path, arrow_schema, table_schema->PartitionKeys(),
        core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
        core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(), external_paths,
        global_index_external_paths, core_options.IndexFileInDataFileDir(), external_path_weights,
        pool);
}

Result<std::vector<std::shared_ptr<DataFileMeta>>> SelectInputFiles(
    const std::vector<ManifestEntry>& entries, const std::set<std::string>& file_names,
    int64_t snapshot_id) {
    std::unordered_map<std::string, const ManifestEntry*> selected;
    for (const auto& entry : entries) {
        if (file_names.count(entry.FileName()) == 0) {
            continue;
        }
        if (!selected.emplace(entry.FileName(), &entry).second) {
            return Status::Invalid("ambiguous compaction input file: ", entry.FileName());
        }
    }
    std::vector<std::shared_ptr<DataFileMeta>> files;
    files.reserve(file_names.size());
    for (const auto& name : file_names) {
        auto iter = selected.find(name);
        if (iter == selected.end()) {
            return Status::Invalid(fmt::format(
                "input file {} is not active in the selected partition/bucket of snapshot {}", name,
                snapshot_id));
        }
        const ManifestEntry* entry = iter->second;
        if (entry->Bucket() < 0 || entry->Bucket() >= entry->TotalBuckets()) {
            return Status::Invalid("invalid bucket for input file: ", name);
        }
        files.push_back(entry->File());
    }
    return files;
}

Result<std::vector<ManifestEntry>> ScanFiles(
    const PlanningContext& context, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::map<std::string, std::string>& partition, int32_t bucket,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor) {
    const auto& core_options = context.core_options;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                             core_options.GetManifestCompression(), path_factory,
                             core_options.GetCache(), pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, context.table_schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                             core_options.GetManifestCompression(), path_factory,
                             core_options.GetManifestTargetFileSize(), pool, core_options,
                             partition_schema));
    auto scan_filter = std::make_shared<ScanFilter>(
        /*predicate=*/nullptr, std::vector<std::map<std::string, std::string>>{partition}, bucket);
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<AppendOnlyFileStoreScan> scan,
        AppendOnlyFileStoreScan::Create(context.snapshot_manager, context.schema_manager,
                                        manifest_list, manifest_file, context.table_schema,
                                        arrow_schema, scan_filter, core_options, executor, pool));
    scan->WithSnapshot(context.snapshot);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> raw_plan, scan->CreatePlan());
    return raw_plan->Files();
}

Result<DeletionVectors> ReadDeletionVectors(
    const PlanningContext& context, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const BinaryRow& part, int32_t bucket, const std::shared_ptr<MemoryPool>& pool) {
    const auto& core_options = context.core_options;
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexManifestFile> index_manifest_file,
        IndexManifestFile::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                                  core_options.GetManifestCompression(), path_factory,
                                  ResolveBucketMode(core_options.GetBucket(), context.table_schema),
                                  pool, core_options));
    IndexFileHandler index_handler(core_options.GetFileSystem(), std::move(index_manifest_file),
                                   std::make_shared<IndexFilePathFactories>(path_factory),
                                   core_options.DeletionVectorsBitmap64(), pool);
    // Apply committed deletions regardless of read options.
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::shared_ptr<IndexFileMeta>> dv_files,
        index_handler.Scan(context.snapshot, DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, part,
                           bucket));
    return index_handler.ReadAllDeletionVectors(part, bucket, dv_files);
}

Status ValidateIndexedRowCounts(const AppendFileCompactionPlan& plan) {
    if (std::none_of(plan.files.begin(), plan.files.end(),
                     [](const FileCompactionInfo& file) { return file.index_meta.has_value(); })) {
        return Status::OK();
    }
    for (const auto& file : plan.files) {
        if (file.source_row_count > std::numeric_limits<int32_t>::max()) {
            return Status::Invalid("source row count exceeds int32 range for ", file.file_name);
        }
    }
    if (plan.output_row_count > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("compacted row count exceeds int32 range with file indexes");
    }
    return Status::OK();
}

Result<AppendFileCompactionPlan> BuildCompactionPlan(
    int64_t snapshot_id, const std::vector<std::shared_ptr<DataFileMeta>>& files,
    const DeletionVectors& deletion_vectors,
    const std::shared_ptr<DataFilePathFactory>& data_path_factory,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool) {
    AppendFileCompactionPlan plan{snapshot_id, 0, {}};
    plan.files.reserve(files.size());
    for (const auto& file : files) {
        const auto& name = file->file_name;
        FileCompactionInfo info{name, file->schema_id, file->row_count, 0, std::nullopt, {}};
        auto dv_iter = deletion_vectors.find(name);
        PAIMON_RETURN_NOT_OK(
            BuildRowIdMapping(dv_iter == deletion_vectors.end() ? nullptr : dv_iter->second,
                              plan.output_row_count, &info));
        PAIMON_ASSIGN_OR_RAISE(info.index_meta,
                               ReadFileIndexMeta(file, data_path_factory, file_system, pool));
        plan.output_row_count += info.live_row_count;
        plan.files.push_back(std::move(info));
    }
    PAIMON_RETURN_NOT_OK(ValidateIndexedRowCounts(plan));
    return plan;
}

}  // namespace

Result<std::optional<int64_t>> FileCompactionInfo::MapRowId(int64_t source_row_id) const {
    if (source_row_id < 0 || source_row_id >= source_row_count) {
        return Status::Invalid("source row id is out of range for ", file_name);
    }
    auto iter = std::upper_bound(
        row_id_mapping.begin(), row_id_mapping.end(), source_row_id,
        [](int64_t row_id, const RowIdMappingRange& range) { return row_id < range.source_start; });
    if (iter == row_id_mapping.begin()) {
        return std::optional<int64_t>();
    }
    const RowIdMappingRange& range = *std::prev(iter);
    int64_t offset = source_row_id - range.source_start;
    if (offset >= range.length) {
        return std::optional<int64_t>();
    }
    return std::optional<int64_t>(range.target_start + offset);
}

Result<AppendFileCompactionPlan> AppendFileCompactionPlanner::CreatePlan(
    const std::string& table_path, const std::string& branch,
    const std::map<std::string, std::string>& partition, int32_t bucket,
    const std::set<std::string>& file_names, const std::optional<int64_t>& snapshot_id,
    const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor) {
    PAIMON_RETURN_NOT_OK(ValidateInputs(table_path, file_names, snapshot_id));
    std::string normalized_branch = BranchManager::NormalizeBranch(branch);
    PAIMON_RETURN_NOT_OK(BranchManager::CheckValidBranch(normalized_branch));
    std::shared_ptr<MemoryPool> final_pool = pool ? pool : GetDefaultPool();
    std::shared_ptr<Executor> final_executor = executor ? executor : CreateDefaultExecutor();
    PAIMON_ASSIGN_OR_RAISE(
        PlanningContext context,
        LoadPlanningContext(table_path, normalized_branch, snapshot_id, options, file_system));
    PAIMON_RETURN_NOT_OK(ValidateTable(context, partition, bucket));
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(context.table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                           CreatePathFactory(table_path, context, arrow_schema, final_pool));
    PAIMON_ASSIGN_OR_RAISE(BinaryRow part, path_factory->ToBinaryRow(partition));
    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> entries,
                           ScanFiles(context, path_factory, arrow_schema, partition, bucket,
                                     final_pool, final_executor));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> files,
                           SelectInputFiles(entries, file_names, context.snapshot.Id()));
    PAIMON_ASSIGN_OR_RAISE(DeletionVectors deletion_vectors,
                           ReadDeletionVectors(context, path_factory, part, bucket, final_pool));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_path_factory,
                           path_factory->CreateDataFilePathFactory(part, bucket));
    return BuildCompactionPlan(context.snapshot.Id(), files, deletion_vectors, data_path_factory,
                               context.core_options.GetFileSystem(), final_pool);
}

}  // namespace paimon
