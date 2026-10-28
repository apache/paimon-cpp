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

#include "paimon/core/table/primary_key_file_store_table.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/global_index/btree/btree_defs.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/index/pk/bucketed_primary_key_index_maintainer.h"
#include "paimon/core/index/pk/primary_key_index_definitions.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/mergetree/compact/lookup_merge_function.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/operation/commit/commit_scanner.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/key_value_file_store_scan.h"
#include "paimon/core/operation/key_value_file_store_write.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/postpone/postpone_bucket_file_store_write.h"
#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/source/merge_tree_split_generator.h"
#include "paimon/core/table/source/split_generator.h"
#include "paimon/core/utils/primary_key_table_utils.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/write_context.h"

namespace paimon {

struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

namespace {

Result<bool> HasHistoricalPrimaryKeyBTreeDefinition(
    const std::shared_ptr<SchemaManager>& schema_manager, int64_t current_schema_id) {
    if (current_schema_id <= TableSchema::FIRST_SCHEMA_ID) {
        return false;
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> schema_ids, schema_manager->ListAllIds());
    for (int64_t schema_id : schema_ids) {
        if (schema_id >= current_schema_id) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> historical_schema,
                               schema_manager->ReadSchema(schema_id));
        const auto& historical_options = historical_schema->Options();
        auto option = historical_options.find(Options::PK_BTREE_INDEX_COLUMNS);
        if (option != historical_options.end() &&
            !StringUtils::IsNullOrWhitespaceOnly(option->second)) {
            return true;
        }
    }
    return false;
}

}  // namespace

PrimaryKeyFileStoreTable::PrimaryKeyFileStoreTable(const std::shared_ptr<FileStore>& store,
                                                   const std::shared_ptr<TableSchema>& schema,
                                                   const Identifier& identifier,
                                                   const std::string& uuid)
    : FileStoreTable(store, schema, identifier, uuid) {}

Result<std::unique_ptr<FileStoreWrite>> PrimaryKeyFileStoreTable::CreateWrite(
    const WriteContext& ctx, const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& options,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<IOManager>& io_manager, bool ignore_previous_files) const {
    std::shared_ptr<TableSchema> schema = std::static_pointer_cast<TableSchema>(schema_);
    if (ctx.GetRealtimeContext()) {
        PAIMON_RETURN_NOT_OK(PrimaryKeyTableUtils::ValidateRealtimeOptions(options, *schema));
        if (ignore_previous_files) {
            return Status::NotImplemented("PK realtime requires restore from the latest snapshot");
        }
        if (!ctx.GetWriteSchema().empty()) {
            return Status::NotImplemented("PK realtime does not support a custom write schema");
        }
        PAIMON_RETURN_NOT_OK(
            RestoreRealtimeCommittedProgress(ctx.GetRealtimeContext(), snapshot_manager, options));
    }
    if (options.GetBucket() == BucketModeDefine::POSTPONE_BUCKET) {
        return PostponeBucketFileStoreWrite::Create(
            snapshot_manager, GetSchemaManager(), ctx.GetCommitUser(), root_path_, schema,
            arrow_schema, partition_schema, io_manager, options, ctx.IsStreamingMode(),
            ctx.IgnoreNumBucketCheck(), ctx.GetWriteId(), ctx.GetFileSystemSchemeToIdentifierMap(),
            ctx.GetExecutor(), ctx.GetMemoryPool(), file_system_);
    }
    if (options.GetBucket() <= 0) {
        return Status::Invalid(
            fmt::format("not support bucket {} in key value table", options.GetBucket()));
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           schema->TrimmedPrimaryKeys());
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> trimmed_primary_key_fields,
                           schema->GetFields(trimmed_primary_keys));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                           FieldsComparator::Create(trimmed_primary_key_fields,
                                                    options.SequenceFieldSortOrderIsAscending()));
    auto primary_keys = schema->PrimaryKeys();
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MergeFunction> merge_function,
                           PrimaryKeyTableUtils::CreateMergeFunction(arrow_schema, primary_keys,
                                                                     options, ctx.GetMemoryPool()));
    if (options.NeedLookup() && options.GetMergeEngine() != MergeEngine::FIRST_ROW) {
        merge_function = std::make_unique<LookupMergeFunction>(std::move(merge_function));
    }
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper =
        std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FieldsComparator> sequence_fields_comparator,
        PrimaryKeyTableUtils::CreateSequenceFieldsComparator(schema->Fields(), options));

    std::shared_ptr<BucketedDvMaintainer::Factory> dv_maintainer_factory;
    PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexDefinitions primary_key_index_definitions,
                           PrimaryKeyIndexDefinitions::Create(*schema));
    bool has_btree_index = false;
    for (const PrimaryKeyIndexDefinition& definition :
         primary_key_index_definitions.Definitions()) {
        if (definition.GetFamily() == PrimaryKeyIndexDefinition::Family::BTREE) {
            has_btree_index = true;
            break;
        }
    }
    std::optional<Snapshot> latest_snapshot;
    bool has_existing_index_manifest = false;
    if (!has_btree_index) {
        PAIMON_ASSIGN_OR_RAISE(
            bool has_historical_btree_index,
            HasHistoricalPrimaryKeyBTreeDefinition(GetSchemaManager(), schema->Id()));
        if (has_historical_btree_index) {
            PAIMON_ASSIGN_OR_RAISE(latest_snapshot, snapshot_manager->LatestSnapshot());
            has_existing_index_manifest =
                latest_snapshot.has_value() && latest_snapshot->IndexManifest().has_value();
        }
    }
    std::shared_ptr<IndexFileHandler> index_file_handler;
    if (options.DeletionVectorsEnabled() || has_btree_index || has_existing_index_manifest) {
        PAIMON_ASSIGN_OR_RAISE(index_file_handler, store_->NewIndexFileHandler(
                                                       options, path_factory, ctx.GetMemoryPool()));
    }
    bool has_restored_btree_index = false;
    if (!has_btree_index && has_existing_index_manifest) {
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<IndexManifestEntry> restored_btree_entries,
            index_file_handler->Scan(
                latest_snapshot.value(), [](const IndexManifestEntry& entry) -> bool {
                    return entry.index_file->IndexType() == BtreeDefs::kIdentifier &&
                           IndexFileHandler::IsPrimaryKeySourceIndex(*entry.index_file);
                }));
        has_restored_btree_index = !restored_btree_entries.empty();
    }
    if (options.DeletionVectorsEnabled()) {
        dv_maintainer_factory = std::make_shared<BucketedDvMaintainer::Factory>(index_file_handler);
    }
    std::shared_ptr<BucketedPrimaryKeyIndexMaintainer::Factory>
        primary_key_index_maintainer_factory;
    if (has_btree_index || has_restored_btree_index) {
        primary_key_index_maintainer_factory =
            std::make_shared<BucketedPrimaryKeyIndexMaintainer::Factory>(
                root_path_, CurrentBranch(), schema, primary_key_index_definitions.Definitions(),
                path_factory, index_file_handler, options, io_manager, ctx.EnableMultiThreadSpill(),
                ctx.GetExecutor(), ctx.GetMemoryPool());
    }

    std::shared_ptr<RealtimeSchemaLayout> realtime_schema_layout;
    if (ctx.GetRealtimeContext()) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<RealtimeSchemaLayout> layout,
            RealtimeSchemaLayout::Create(RealtimeStoreMode::PRIMARY_KEY, arrow_schema));
        realtime_schema_layout = std::move(layout);
    }

    return std::make_unique<KeyValueFileStoreWrite>(
        path_factory, snapshot_manager, GetSchemaManager(), ctx.GetCommitUser(), root_path_, schema,
        arrow_schema, realtime_schema_layout, partition_schema, dv_maintainer_factory,
        primary_key_index_maintainer_factory, io_manager, key_comparator,
        sequence_fields_comparator, merge_function_wrapper, options, ignore_previous_files,
        ctx.IsStreamingMode(), ctx.IgnoreNumBucketCheck(), ctx.EnableMultiThreadSpill(),
        ctx.GetRealtimeContext(), ctx.GetExecutor(), ctx.GetMemoryPool());
}

Result<std::unique_ptr<FileStoreScan>> PrimaryKeyFileStoreTable::NewFileStoreScan(
    const std::shared_ptr<ManifestList>& manifest_list,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& core_options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& memory_pool,
    const ScanContext* context) const {
    // TODO(liancheng.lsz): support fallback branch in scan
    std::shared_ptr<TableSchema> table_schema = std::static_pointer_cast<TableSchema>(schema_);
    std::unique_ptr<FileStoreScan> scan;
    PAIMON_ASSIGN_OR_RAISE(
        scan, KeyValueFileStoreScan::Create(GetSnapshotManager(), GetSchemaManager(), manifest_list,
                                            manifest_file, table_schema, arrow_schema,
                                            context->GetScanFilters(), core_options, executor,
                                            memory_pool));
    return WithTablePath(std::move(scan), root_path_);
}

Result<std::unique_ptr<SplitGenerator>> PrimaryKeyFileStoreTable::NewSplitGenerator(
    const CoreOptions& core_options) const {
    auto source_split_target_size = core_options.GetSourceSplitTargetSize();
    auto source_split_open_file_cost = core_options.GetSourceSplitOpenFileCost();
    // TODO(liancheng.lsz): support evolution
    std::shared_ptr<TableSchema> table_schema = std::static_pointer_cast<TableSchema>(schema_);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema->TrimmedPrimaryKeys());
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> trimmed_pk_fields,
                           table_schema->GetFields(trimmed_primary_keys));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FieldsComparator> key_comparator,
        FieldsComparator::Create(trimmed_pk_fields, /*is_ascending_order=*/true));
    return std::make_unique<MergeTreeSplitGenerator>(
        source_split_target_size, source_split_open_file_cost,
        core_options.DeletionVectorsEnabled(), core_options.GetMergeEngine(), key_comparator);
}

CommitScanner::ScanSupplier PrimaryKeyFileStoreTable::NewCommitScanSupplier(
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
            std::unique_ptr<KeyValueFileStoreScan> scan,
            KeyValueFileStoreScan::Create(snapshot_manager, schema_manager, manifest_list,
                                          manifest_file, table_schema, arrow_schema, scan_filter,
                                          options, executor, pool));
        return std::unique_ptr<FileStoreScan>(std::move(scan));
    };
}

}  // namespace paimon
