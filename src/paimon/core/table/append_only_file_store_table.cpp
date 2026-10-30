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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/core/table/append_only_file_store_table.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/operation/append_only_file_store_write.h"
#include "paimon/core/operation/commit/commit_scanner.h"
#include "paimon/core/operation/data_evolution_file_store_scan.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/source/append_only_split_generator.h"
#include "paimon/core/table/source/data_evolution_split_generator.h"
#include "paimon/core/table/source/split_generator.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/write_context.h"

namespace paimon {

AppendOnlyFileStoreTable::AppendOnlyFileStoreTable(const std::shared_ptr<FileStore>& store,
                                                   const std::shared_ptr<TableSchema>& schema,
                                                   const Identifier& identifier,
                                                   const std::string& uuid)
    : FileStoreTable(store, schema, identifier, uuid) {}

Result<std::unique_ptr<FileStoreWrite>> AppendOnlyFileStoreTable::CreateWrite(
    const WriteContext& ctx, const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& options,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<IOManager>& io_manager, bool ignore_previous_files) const {
    const bool realtime_deduplicate = !options.GetRealtimeDeduplicateKeyFields().empty();
    std::shared_ptr<TableSchema> schema = std::static_pointer_cast<TableSchema>(schema_);
    bool need_dv_maintainer_factory = options.DeletionVectorsEnabled();
    if (options.GetBucket() == -1) {
        need_dv_maintainer_factory = false;
        ignore_previous_files = true;
    } else if (options.GetBucket() <= 0) {
        return Status::Invalid(
            fmt::format("not support bucket {} in append table", options.GetBucket()));
    }
    if (ctx.GetRealtimeContext()) {
        if (!ctx.GetWriteSchema().empty()) {
            return Status::Invalid("real-time append write does not support a custom schema");
        }
        if (!realtime_deduplicate) {
            PAIMON_RETURN_NOT_OK(RestoreRealtimeCommittedProgress(ctx.GetRealtimeContext(),
                                                                  snapshot_manager, options));
        }
    } else if (realtime_deduplicate) {
        return Status::Invalid("real-time deduplicate write requires a real-time context");
    }
    std::shared_ptr<arrow::Schema> write_schema = arrow_schema;
    const auto& write_field_names = ctx.GetWriteSchema();
    if (!write_field_names.empty()) {
        arrow::FieldVector write_fields;
        write_fields.reserve(write_field_names.size());
        for (const auto& field_name : write_field_names) {
            auto field = arrow_schema->GetFieldByName(field_name);
            if (!field) {
                // TODO(xinyu.lxy): support _ROW_ID and _SEQUENCE_NUMBER
                return Status::Invalid(
                    fmt::format("write field {} does not exist in table schema", field_name));
            }
            write_fields.push_back(field);
        }
        write_schema = arrow::schema(write_fields);
    }

    std::shared_ptr<RealtimeSchemaLayout> realtime_schema_layout;
    if (ctx.GetRealtimeContext()) {
        const RealtimeStoreMode mode =
            realtime_deduplicate ? RealtimeStoreMode::DEDUPLICATE : RealtimeStoreMode::APPEND_ONLY;
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RealtimeSchemaLayout> layout,
                               RealtimeSchemaLayout::Create(mode, write_schema));
        realtime_schema_layout = std::move(layout);
    }

    std::shared_ptr<BucketedDvMaintainer::Factory> dv_maintainer_factory;
    if (need_dv_maintainer_factory) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<IndexFileHandler> index_file_handler,
            store_->NewIndexFileHandler(options, path_factory, ctx.GetMemoryPool()));
        dv_maintainer_factory =
            std::make_shared<BucketedDvMaintainer::Factory>(std::move(index_file_handler));
    }

    auto file_store_write = std::make_unique<AppendOnlyFileStoreWrite>(
        path_factory, snapshot_manager, GetSchemaManager(), ctx.GetCommitUser(), root_path_, schema,
        arrow_schema, write_schema, realtime_schema_layout, partition_schema, dv_maintainer_factory,
        io_manager, options, ignore_previous_files, ctx.IsStreamingMode(),
        ctx.IgnoreNumBucketCheck(), ctx.GetRealtimeContext(), ctx.GetExecutor(),
        ctx.GetMemoryPool());
    if (ctx.GetRealtimeContext() && realtime_deduplicate) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager->LatestSnapshot());
        if (latest_snapshot) {
            PAIMON_RETURN_NOT_OK(file_store_write->RefreshCommittedSnapshot(latest_snapshot->Id()));
        }
    }
    return std::unique_ptr<FileStoreWrite>(std::move(file_store_write));
}

Result<std::unique_ptr<FileStoreScan>> AppendOnlyFileStoreTable::NewFileStoreScan(
    const std::shared_ptr<ManifestList>& manifest_list,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& core_options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& memory_pool,
    const ScanContext* context) const {
    // TODO(liancheng.lsz): support fallback branch in scan
    std::shared_ptr<TableSchema> table_schema = std::static_pointer_cast<TableSchema>(schema_);
    std::unique_ptr<FileStoreScan> scan;
    if (core_options.DataEvolutionEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(
            scan, DataEvolutionFileStoreScan::Create(GetSnapshotManager(), GetSchemaManager(),
                                                     manifest_list, manifest_file, table_schema,
                                                     arrow_schema, context->GetScanFilters(),
                                                     core_options, executor, memory_pool));
    } else {
        PAIMON_ASSIGN_OR_RAISE(
            scan, AppendOnlyFileStoreScan::Create(GetSnapshotManager(), GetSchemaManager(),
                                                  manifest_list, manifest_file, table_schema,
                                                  arrow_schema, context->GetScanFilters(),
                                                  core_options, executor, memory_pool));
    }
    return WithTablePath(std::move(scan), root_path_);
}

Result<std::unique_ptr<SplitGenerator>> AppendOnlyFileStoreTable::NewSplitGenerator(
    const CoreOptions& core_options) const {
    auto source_split_target_size = core_options.GetSourceSplitTargetSize();
    auto source_split_open_file_cost = core_options.GetSourceSplitOpenFileCost();
    if (core_options.DataEvolutionEnabled()) {
        return std::make_unique<DataEvolutionSplitGenerator>(source_split_target_size,
                                                             source_split_open_file_cost,
                                                             core_options.BlobSplitByFileSize());
    }
    BucketMode bucket_mode =
        (core_options.GetBucket() == -1 ? BucketMode::BUCKET_UNAWARE : BucketMode::HASH_FIXED);
    return std::make_unique<AppendOnlySplitGenerator>(source_split_target_size,
                                                      source_split_open_file_cost, bucket_mode);
}

CommitScanner::ScanSupplier AppendOnlyFileStoreTable::NewCommitScanSupplier(
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<ManifestList>& manifest_list,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) const {
    const std::shared_ptr<SchemaManager>& schema_manager = GetSchemaManager();
    std::shared_ptr<TableSchema> table_schema = std::static_pointer_cast<TableSchema>(schema_);
    return [snapshot_manager, schema_manager, manifest_list, manifest_file, table_schema,
            arrow_schema, options, executor, pool](const std::shared_ptr<ScanFilter>& scan_filter)
               -> Result<std::unique_ptr<FileStoreScan>> {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<AppendOnlyFileStoreScan> scan,
            AppendOnlyFileStoreScan::Create(snapshot_manager, schema_manager, manifest_list,
                                            manifest_file, table_schema, arrow_schema, scan_filter,
                                            options, executor, pool));
        return std::unique_ptr<FileStoreScan>(std::move(scan));
    };
}

}  // namespace paimon
