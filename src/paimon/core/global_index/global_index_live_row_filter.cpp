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

#include "paimon/core/global_index/global_index_live_row_filter.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/operation/data_evolution_file_store_scan.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/source/append_only_split_generator.h"
#include "paimon/core/table/source/data_evolution_split_generator.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/scan_mode.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/core/table/source/split_generator.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/plan.h"
#include "paimon/utils/range.h"

namespace paimon {
namespace {
/// Creates a reader over the data files of an append table in the partitions accepted by
/// `partition_filter`, as Java `FileStoreTable#newSnapshotReader` with `withPartitionFilter` does.
Result<std::unique_ptr<SnapshotReader>> CreateSnapshotReader(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<PredicateFilter>& partition_filter, const CoreOptions& options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    std::shared_ptr<FileSystem> fs = options.GetFileSystem();
    std::string branch = BranchManager::NormalizeBranch(options.GetBranch());
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            root_path, arrow_schema, table_schema->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_path, options.IndexFileInDataFileDir(), pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(fs, options.GetManifestFormat(), options.GetManifestCompression(),
                             path_factory, options.GetCache(), pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(fs, options.GetManifestFormat(), options.GetManifestCompression(),
                             path_factory, options.GetManifestTargetFileSize(), pool, options,
                             partition_schema));
    auto snapshot_manager = std::make_shared<SnapshotManager>(fs, root_path, branch);
    auto schema_manager = std::make_shared<SchemaManager>(fs, root_path, branch);
    auto scan_filter = std::make_shared<ScanFilter>(
        /*predicate=*/nullptr,
        /*partition_filters=*/std::vector<std::map<std::string, std::string>>(),
        /*bucket_filter=*/std::nullopt);

    std::unique_ptr<FileStoreScan> scan;
    std::unique_ptr<SplitGenerator> split_generator;
    if (options.DataEvolutionEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(
            scan, DataEvolutionFileStoreScan::Create(
                      snapshot_manager, schema_manager, manifest_list, manifest_file, table_schema,
                      arrow_schema, scan_filter, options, executor, pool));
        split_generator = std::make_unique<DataEvolutionSplitGenerator>(
            options.GetSourceSplitTargetSize(), options.GetSourceSplitOpenFileCost(),
            options.BlobSplitByFileSize());
    } else {
        PAIMON_ASSIGN_OR_RAISE(
            scan, AppendOnlyFileStoreScan::Create(snapshot_manager, schema_manager, manifest_list,
                                                  manifest_file, table_schema, arrow_schema,
                                                  scan_filter, options, executor, pool));
        split_generator = std::make_unique<AppendOnlySplitGenerator>(
            options.GetSourceSplitTargetSize(), options.GetSourceSplitOpenFileCost(),
            BucketMode::BUCKET_UNAWARE);
    }
    scan->WithTablePath(root_path)->WithPartitionFilter(partition_filter, partition_schema);

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexManifestFile> index_manifest_file,
        IndexManifestFile::Create(fs, options.GetManifestFormat(), options.GetManifestCompression(),
                                  path_factory, options.GetBucket(), pool, options));
    auto index_file_handler = std::make_unique<IndexFileHandler>(
        fs, std::move(index_manifest_file), std::make_shared<IndexFilePathFactories>(path_factory),
        options.DeletionVectorsBitmap64(), pool);
    return std::make_unique<SnapshotReader>(
        std::move(scan), path_factory, std::move(split_generator), std::move(index_file_handler));
}

/// Removes the rows deleted by the deletion vectors of `split` from `live_rows`. Each deletion
/// vector is keyed by the data file whose first row id its positions are relative to.
Status SubtractDeletedRows(const DataSplitImpl& split, const std::string& root_path,
                           const Snapshot& snapshot, const std::shared_ptr<FileSystem>& fs,
                           const std::shared_ptr<MemoryPool>& pool, RoaringBitmap64* live_rows) {
    const std::vector<std::shared_ptr<DataFileMeta>>& files = split.DataFiles();
    DeletionVector::Factory deletion_vector_factory = DeletionVector::CreateFactory(
        fs, DeletionVector::CreateDeletionFileMap(files, split.DeletionFiles()), pool);
    for (const auto& file : files) {
        if (!file->first_row_id) {
            continue;
        }
        int64_t first_row_id = file->first_row_id.value();
        Result<std::shared_ptr<DeletionVector>> deletion_vector_result =
            deletion_vector_factory(file->file_name);
        if (!deletion_vector_result.ok()) {
            const Status& status = deletion_vector_result.status();
            return status.WithMessage(fmt::format(
                "failed to read deletion vector of data file {} in snapshot {} of table {}: {}",
                file->file_name, snapshot.Id(), root_path, status.message()));
        }
        std::shared_ptr<DeletionVector> deletion_vector = std::move(deletion_vector_result).value();
        if (!deletion_vector || deletion_vector->IsEmpty()) {
            continue;
        }
        RoaringBitmap64 deleted_rows;
        PAIMON_RETURN_NOT_OK(deletion_vector->ForEachDeletedPosition(
            [&deleted_rows, first_row_id](int64_t position) {
                deleted_rows.Add(first_row_id + position);
            }));
        *live_rows -= deleted_rows;
    }
    return Status::OK();
}
}  // namespace

Result<std::optional<RoaringBitmap64>> GlobalIndexLiveRowFilter::LiveRows(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const Snapshot& snapshot, const std::shared_ptr<PredicateFilter>& partition_filter,
    const CoreOptions& options, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool) {
    // Without row tracking, as in primary-key tables, data files carry no row ids to filter.
    if (!options.DeletionVectorsEnabled() || !options.RowTrackingEnabled()) {
        return std::optional<RoaringBitmap64>();
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<SnapshotReader> snapshot_reader,
        CreateSnapshotReader(root_path, table_schema, partition_filter, options, executor, pool));
    snapshot_reader->WithMode(ScanMode::ALL)->WithSnapshot(snapshot);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, snapshot_reader->Read());

    std::vector<std::shared_ptr<DataSplitImpl>> splits;
    splits.reserve(plan->Splits().size());
    for (const auto& split : plan->Splits()) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        if (!data_split) {
            return Status::Invalid("cannot cast split to DataSplitImpl when reading live rows");
        }
        splits.push_back(std::move(data_split));
    }

    RoaringBitmap64 live_rows;
    // Phase 1: union the row id range of every data file.
    for (const auto& split : splits) {
        for (const auto& file : split->DataFiles()) {
            if (file->first_row_id && file->row_count > 0) {
                PAIMON_ASSIGN_OR_RAISE(Range row_id_range, file->NonNullRowIdRange());
                // AddRange is half-open and `row_id_range.to` may be the largest int64 value.
                live_rows.AddRange(row_id_range.from, row_id_range.to);
                live_rows.Add(row_id_range.to);
            }
        }
    }
    // Phase 2: subtract each deletion vector. Every range is added first, so a row deleted by one
    // file's deletion vector is not added back by another file covering it, and only one deletion
    // vector is held at a time.
    for (const auto& split : splits) {
        PAIMON_RETURN_NOT_OK(SubtractDeletedRows(*split, root_path, snapshot,
                                                 options.GetFileSystem(), pool, &live_rows));
    }
    return std::optional<RoaringBitmap64>(std::move(live_rows));
}

}  // namespace paimon
