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

#include "paimon/snapshot/snapshot_file_scan.h"

#include <future>
#include <iterator>
#include <set>
#include <unordered_set>
#include <utility>

#include "paimon/common/executor/future.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"

namespace paimon {
namespace {

struct ManifestReadResult {
    ManifestFileMeta meta;
    std::vector<ManifestEntry> entries;
};

class SnapshotFileCollector {
 public:
    SnapshotFileCollector(const std::shared_ptr<FileStorePathFactory>& path_factory,
                          const std::shared_ptr<ManifestList>& manifest_list,
                          const std::shared_ptr<ManifestFile>& manifest_file,
                          std::unique_ptr<IndexFileHandler>&& index_file_handler,
                          const std::shared_ptr<SchemaManager>& schema_manager,
                          int64_t latest_schema_id,
                          const std::shared_ptr<SnapshotManager>& snapshot_manager,
                          const std::shared_ptr<Executor>& executor,
                          const std::shared_ptr<arrow::Schema>& partition_schema,
                          const std::shared_ptr<PredicateFilter>& partition_filter,
                          const std::optional<int32_t>& bucket_id)
        : path_factory_(path_factory),
          manifest_list_(manifest_list),
          manifest_file_(manifest_file),
          index_file_handler_(std::move(index_file_handler)),
          schema_manager_(schema_manager),
          latest_schema_id_(latest_schema_id),
          snapshot_manager_(snapshot_manager),
          executor_(executor),
          partition_schema_(partition_schema),
          partition_filter_(partition_filter),
          bucket_id_(bucket_id) {}

    Result<std::set<std::string>> Collect(const Snapshot& snapshot) {
        AddPath(snapshot_manager_->SnapshotPath(snapshot.Id()));
        schema_ids_.insert(snapshot.SchemaId());
        schema_ids_.insert(latest_schema_id_);

        AddManifestList(snapshot.BaseManifestList());
        AddManifestList(snapshot.DeltaManifestList());
        if (snapshot.ChangelogManifestList()) {
            AddManifestList(snapshot.ChangelogManifestList().value());
        }
        if (snapshot.IndexManifest()) {
            AddPath(path_factory_->ToManifestFilePath(snapshot.IndexManifest().value()));
        }
        if (snapshot.Statistics()) {
            AddPath(path_factory_->ToStatsFilePath(snapshot.Statistics().value()));
        }
        std::optional<std::string> offsets_path =
            RealtimeCommitProperties::GetOffsetsPath(snapshot);
        if (offsets_path) {
            AddPath(offsets_path.value());
        }

        PAIMON_RETURN_NOT_OK(CollectDataFiles(snapshot));
        PAIMON_RETURN_NOT_OK(CollectChangelogFiles(snapshot));
        PAIMON_RETURN_NOT_OK(CollectIndexFiles(snapshot));
        for (int64_t schema_id : schema_ids_) {
            AddPath(PathUtil::JoinPath(schema_manager_->SchemaDirectory(),
                                       "schema-" + std::to_string(schema_id)));
        }

        return std::move(paths_);
    }

 private:
    void AddPath(const std::string& path) {
        if (!path.empty()) {
            paths_.insert(path);
        }
    }

    void AddManifestList(const std::string& file_name) {
        if (!file_name.empty()) {
            AddPath(path_factory_->ToManifestListPath(file_name));
        }
    }

    bool MayContainBucket(const ManifestFileMeta& meta) const {
        if (!bucket_id_) {
            return true;
        }
        const std::optional<int32_t>& min_bucket = meta.MinBucket();
        const std::optional<int32_t>& max_bucket = meta.MaxBucket();
        return !min_bucket || !max_bucket ||
               (bucket_id_.value() >= min_bucket.value() &&
                bucket_id_.value() <= max_bucket.value());
    }

    Result<bool> ShouldReadManifest(const ManifestFileMeta& meta) const {
        if (!MayContainBucket(meta)) {
            return false;
        }
        if (!partition_filter_) {
            return true;
        }
        SimpleStats stats = meta.PartitionStats();
        return partition_filter_->Test(partition_schema_,
                                       /*row_count=*/meta.NumAddedFiles() + meta.NumDeletedFiles(),
                                       stats.MinValues(), stats.MaxValues(), stats.NullCounts());
    }

    Status ApplyPartitionFilter(std::vector<ManifestEntry>* entries) const {
        if (!partition_filter_) {
            return Status::OK();
        }
        std::vector<ManifestEntry> filtered_entries;
        filtered_entries.reserve(entries->size());
        for (ManifestEntry& entry : *entries) {
            PAIMON_ASSIGN_OR_RAISE(bool saved,
                                   partition_filter_->Test(partition_schema_, entry.Partition()));
            if (saved) {
                filtered_entries.push_back(std::move(entry));
            }
        }
        *entries = std::move(filtered_entries);
        return Status::OK();
    }

    Result<std::vector<ManifestReadResult>> ReadManifests(
        const std::vector<ManifestFileMeta>& metas) const {
        std::unordered_set<std::string> submitted_files;
        std::vector<std::future<Result<ManifestReadResult>>> futures;
        for (const ManifestFileMeta& meta : metas) {
            PAIMON_ASSIGN_OR_RAISE(bool should_read, ShouldReadManifest(meta));
            if (!should_read || !submitted_files.insert(meta.FileName()).second) {
                continue;
            }
            futures.push_back(Via(executor_.get(), [this, meta]() -> Result<ManifestReadResult> {
                std::vector<ManifestEntry> entries;
                if (bucket_id_) {
                    PAIMON_RETURN_NOT_OK(manifest_file_->ReadBucketEntries(
                        meta.FileName(), bucket_id_.value(), &entries));
                } else {
                    PAIMON_RETURN_NOT_OK(
                        manifest_file_->Read(meta.FileName(), /*filter=*/nullptr, &entries));
                }
                PAIMON_RETURN_NOT_OK(ApplyPartitionFilter(&entries));
                return ManifestReadResult{meta, std::move(entries)};
            }));
        }

        std::vector<ManifestReadResult> results;
        std::vector<Result<ManifestReadResult>> read_results = CollectAll(futures);
        results.reserve(read_results.size());
        for (Result<ManifestReadResult>& result : read_results) {
            PAIMON_RETURN_NOT_OK(result);
            results.push_back(std::move(result).value());
        }
        return results;
    }

    Status CollectDataFiles(const Snapshot& snapshot) {
        std::vector<ManifestFileMeta> metas;
        PAIMON_RETURN_NOT_OK(manifest_list_->ReadDataManifests(snapshot, &metas));
        PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestReadResult> manifest_results,
                               ReadManifests(metas));

        std::vector<ManifestEntry> unmerged_entries;
        for (ManifestReadResult& result : manifest_results) {
            AddPath(path_factory_->ToManifestFilePath(result.meta.FileName()));
            unmerged_entries.insert(unmerged_entries.end(),
                                    std::make_move_iterator(result.entries.begin()),
                                    std::make_move_iterator(result.entries.end()));
        }

        std::vector<ManifestEntry> live_entries;
        PAIMON_RETURN_NOT_OK(FileStoreScan::MergeLiveEntries(unmerged_entries, &live_entries));
        return CollectEntryFiles(live_entries, /*only_add=*/false);
    }

    Status CollectChangelogFiles(const Snapshot& snapshot) {
        if (!snapshot.ChangelogManifestList()) {
            return Status::OK();
        }
        std::vector<ManifestFileMeta> metas;
        PAIMON_RETURN_NOT_OK(manifest_list_->ReadChangelogManifests(snapshot, &metas));
        PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestReadResult> manifest_results,
                               ReadManifests(metas));

        for (ManifestReadResult& result : manifest_results) {
            AddPath(path_factory_->ToManifestFilePath(result.meta.FileName()));
            PAIMON_RETURN_NOT_OK(CollectEntryFiles(result.entries, /*only_add=*/true));
        }
        return Status::OK();
    }

    Status CollectEntryFiles(const std::vector<ManifestEntry>& entries, bool only_add) {
        for (const ManifestEntry& entry : entries) {
            if (only_add && !(entry.Kind() == FileKind::Add())) {
                continue;
            }
            schema_ids_.insert(entry.File()->schema_id);
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                path_factory_->CreateDataFilePathFactory(entry.Partition(), entry.Bucket()));
            for (const std::string& path : data_file_path_factory->CollectFiles(entry.File())) {
                AddPath(path);
            }
        }
        return Status::OK();
    }

    Status CollectIndexFiles(const Snapshot& snapshot) {
        if (!snapshot.IndexManifest()) {
            return Status::OK();
        }
        auto filter = [this](const IndexManifestEntry& entry) -> Result<bool> {
            if (!(entry.kind == FileKind::Add()) ||
                (bucket_id_ && entry.bucket != bucket_id_.value())) {
                return false;
            }
            if (!partition_filter_) {
                return true;
            }
            return partition_filter_->Test(partition_schema_, entry.partition);
        };
        PAIMON_ASSIGN_OR_RAISE(std::vector<IndexManifestEntry> entries,
                               index_file_handler_->Scan(snapshot, filter));
        for (const IndexManifestEntry& entry : entries) {
            PAIMON_ASSIGN_OR_RAISE(
                std::string path,
                index_file_handler_->FilePath(entry.partition, entry.bucket, entry.index_file));
            AddPath(path);
        }
        return Status::OK();
    }

 private:
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::shared_ptr<ManifestList> manifest_list_;
    std::shared_ptr<ManifestFile> manifest_file_;
    std::unique_ptr<IndexFileHandler> index_file_handler_;
    std::shared_ptr<SchemaManager> schema_manager_;
    int64_t latest_schema_id_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<arrow::Schema> partition_schema_;
    std::shared_ptr<PredicateFilter> partition_filter_;
    std::optional<int32_t> bucket_id_;
    std::set<int64_t> schema_ids_;
    std::set<std::string> paths_;
};

}  // namespace

Result<std::set<std::string>> SnapshotFileScan::ListFiles(
    const std::string& table_path, const std::string& branch,
    const std::optional<int64_t>& snapshot_id, const std::shared_ptr<ScanFilter>& scan_filter,
    const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (table_path.empty()) {
        return Status::Invalid("table path is empty");
    }
    if (snapshot_id && snapshot_id.value() < Snapshot::FIRST_SNAPSHOT_ID) {
        return Status::Invalid("snapshot id must be greater than or equal to 1");
    }
    if (scan_filter && scan_filter->GetPredicate()) {
        return Status::Invalid("snapshot file scan does not support predicate filter");
    }

    std::shared_ptr<MemoryPool> pool = memory_pool ? memory_pool : GetDefaultPool();
    std::shared_ptr<Executor> final_executor = executor;
    if (!final_executor) {
        final_executor = CreateDefaultExecutor();
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions temporary_options,
                           CoreOptions::FromMap(options, file_system));
    std::string normalized_branch = BranchManager::NormalizeBranch(branch);
    auto snapshot_manager = std::make_shared<SnapshotManager>(temporary_options.GetFileSystem(),
                                                              table_path, normalized_branch);

    std::optional<Snapshot> snapshot;
    if (snapshot_id) {
        PAIMON_ASSIGN_OR_RAISE(Snapshot loaded_snapshot,
                               snapshot_manager->LoadSnapshot(snapshot_id.value()));
        snapshot = std::move(loaded_snapshot);
    } else {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager->LatestSnapshot());
    }
    if (!snapshot) {
        return std::set<std::string>();
    }

    auto schema_manager = std::make_shared<SchemaManager>(temporary_options.GetFileSystem(),
                                                          table_path, normalized_branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           schema_manager->Latest());
    if (!latest_schema) {
        return Status::Invalid("not found latest schema");
    }

    std::map<std::string, std::string> final_options = latest_schema.value()->Options();
    for (const auto& [key, value] : options) {
        final_options[key] = value;
    }
    final_options[Options::BRANCH] = normalized_branch;
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(final_options, file_system));

    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(latest_schema.value()->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           core_options.CreateGlobalIndexExternalPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            table_path, arrow_schema, latest_schema.value()->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            pool));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                             core_options.GetManifestCompression(), path_factory,
                             core_options.GetCache(), pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, latest_schema.value()->PartitionKeys()));
    std::shared_ptr<PredicateFilter> partition_filter;
    std::optional<int32_t> bucket_id;
    if (scan_filter) {
        bucket_id = scan_filter->GetBucketFilter();
        PAIMON_ASSIGN_OR_RAISE(
            partition_filter,
            FileStoreScan::CreatePartitionPredicate(
                latest_schema.value()->PartitionKeys(), core_options.GetPartitionDefaultName(),
                arrow_schema, scan_filter->GetPartitionFilters()));
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                             core_options.GetManifestCompression(), path_factory,
                             core_options.GetManifestTargetFileSize(), pool, core_options,
                             partition_schema));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexManifestFile> index_manifest_file,
        IndexManifestFile::Create(core_options.GetFileSystem(), core_options.GetManifestFormat(),
                                  core_options.GetManifestCompression(), path_factory,
                                  core_options.GetBucket(), pool, core_options));
    auto index_file_handler = std::make_unique<IndexFileHandler>(
        core_options.GetFileSystem(), std::move(index_manifest_file),
        std::make_shared<IndexFilePathFactories>(path_factory),
        core_options.DeletionVectorsBitmap64(), pool);

    SnapshotFileCollector collector(path_factory, manifest_list, manifest_file,
                                    std::move(index_file_handler), schema_manager,
                                    latest_schema.value()->Id(), snapshot_manager, final_executor,
                                    partition_schema, partition_filter, bucket_id);
    return collector.Collect(snapshot.value());
}

}  // namespace paimon
