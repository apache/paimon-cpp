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

#include "paimon/catalog/table.h"

#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/commit_context.h"
#include "paimon/common/global_index/btree/btree_defs.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/catalog/catalog_snapshot_commit.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/catalog/commit_lock.h"
#include "paimon/core/catalog/renaming_snapshot_commit.h"
#include "paimon/core/catalog/snapshot_commit.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/index/pk/bucketed_primary_key_index_maintainer.h"
#include "paimon/core/index/pk/primary_key_index_definitions.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/mergetree/compact/lookup_merge_function.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/operation/append_only_file_store_write.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/operation/data_evolution_file_store_scan.h"
#include "paimon/core/operation/expire_snapshots.h"
#include "paimon/core/operation/file_store_commit_impl.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/key_value_file_store_scan.h"
#include "paimon/core/operation/key_value_file_store_write.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/postpone/postpone_bucket_file_store_write.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/format/format_table_file_store_commit.h"
#include "paimon/core/table/format/format_table_file_store_write.h"
#include "paimon/core/table/format/format_table_loader.h"
#include "paimon/core/table/format/format_table_read.h"
#include "paimon/core/table/format/format_table_scan.h"
#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/append_only_split_generator.h"
#include "paimon/core/table/source/append_only_table_read.h"
#include "paimon/core/table/source/data_evolution_batch_scan.h"
#include "paimon/core/table/source/data_evolution_split_generator.h"
#include "paimon/core/table/source/data_table_batch_scan.h"
#include "paimon/core/table/source/data_table_stream_scan.h"
#include "paimon/core/table/source/fallback_table_read.h"
#include "paimon/core/table/source/key_value_table_read.h"
#include "paimon/core/table/source/merge_tree_split_generator.h"
#include "paimon/core/table/source/primary_key_index_batch_scan.h"
#include "paimon/core/table/source/read_optimized_scan_options.h"
#include "paimon/core/table/source/realtime_table_scan.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/core/table/source/split_generator.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/primary_key_table_utils.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/read_context.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/write_context.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class DataSplit;
class Executor;
struct KeyValue;
class MemoryPool;
template <typename T>
class MergeFunctionWrapper;

namespace {

Status RestoreRealtimeCommittedProgress(const std::shared_ptr<RealtimeContext>& realtime_context,
                                        const std::shared_ptr<SnapshotManager>& snapshot_manager,
                                        const CoreOptions& options) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                           RealtimeContextImpl::Cast(realtime_context));
    PAIMON_RETURN_NOT_OK(realtime_context_impl->CheckUsable());
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                           snapshot_manager->LatestSnapshot());
    if (latest_snapshot) {
        PAIMON_ASSIGN_OR_RAISE(
            RealtimeOffsetMap realtime_committed_offsets,
            RealtimeCommitProperties::ReadOffsets(latest_snapshot, options.GetFileSystem()));
        PAIMON_RETURN_NOT_OK(realtime_context_impl->AdvanceCommittedProgress(
            latest_snapshot->Id(), realtime_committed_offsets, /*committed_data_files=*/{}));
    }
    return Status::OK();
}

/// Maps a `WriteContext` onto `FormatTableFileStoreWrite`, refusing by name what a format table
/// cannot honour rather than silently dropping it.
Result<std::unique_ptr<FileStoreWrite>> NewFormatTableWrite(
    const std::shared_ptr<FormatTable>& table, const WriteContext& ctx) {
    if (ctx.GetCatalog() != nullptr) {
        return Status::Invalid(
            "WithCatalog() requires a native table; use WriteContextBuilder(FormatTable) for a "
            "format table");
    }
    if (ctx.IsStreamingMode()) {
        return Status::NotImplemented(
            "a format table has no snapshots, so there is nothing a streaming write could "
            "commit against");
    }
    if (ctx.GetRealtimeContext() != nullptr) {
        return Status::NotImplemented("a format table has no real-time store to write into");
    }
    if (!ctx.GetWriteSchema().empty()) {
        return Status::NotImplemented(
            "a format table write takes the table's own columns; a write schema naming a "
            "subset of them is not supported yet");
    }
    // A write id prefixes a postpone-bucket writer's files so that one compaction reader can
    // put them back in order. A format table has no buckets, so it would identify nothing.
    if (ctx.GetWriteId().has_value()) {
        return Status::NotImplemented(
            "a format table has no buckets, so a write id would name nothing");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableFileStoreWrite> format_write,
                           FormatTableFileStoreWrite::Create(table, ctx.GetMemoryPool()));
    return std::unique_ptr<FileStoreWrite>(std::move(format_write));
}

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

Table::Table(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
             const std::shared_ptr<Schema>& schema, const Identifier& identifier,
             const std::string& uuid)
    : file_system_(file_system),
      root_path_(root_path),
      schema_(schema),
      identifier_(identifier),
      uuid_(uuid) {}

Result<std::unique_ptr<TableScan>> Table::NewScan(std::unique_ptr<ScanContext> context) const {
    return TableScan::Create(std::move(context));
}

Result<std::unique_ptr<TableRead>> Table::NewRead(std::unique_ptr<ReadContext> context) const {
    return TableRead::Create(std::move(context));
}

Result<std::unique_ptr<FileStoreWrite>> Table::NewWrite(
    std::unique_ptr<WriteContext> context) const {
    return FileStoreWrite::Create(std::move(context));
}

Result<std::unique_ptr<FileStoreCommit>> Table::NewCommit(
    std::unique_ptr<CommitContext> context) const {
    return FileStoreCommit::Create(std::move(context));
}

Result<std::unique_ptr<FileStoreWrite>> FileStoreWrite::Create(std::unique_ptr<WriteContext> ctx) {
    if (ctx == nullptr) {
        return Status::Invalid("write context is null pointer");
    }
    if (ctx->GetMemoryPool() == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    if (ctx->GetExecutor() == nullptr) {
        return Status::Invalid("executor is null pointer");
    }

    // A table the caller already loaded says what it is, so nothing is read to find out.
    if (ctx->GetFormatTable() != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> given_table,
                               FormatTable::Copy(ctx->GetFormatTable(), ctx->GetOptions()));
        return NewFormatTableWrite(given_table, *ctx);
    }

    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx->GetCatalog()));
    std::shared_ptr<FileSystem> specific_fs = ctx->GetSpecificFileSystem();
    if (specific_fs == nullptr && ctx->GetFileSystemSchemeToIdentifierMap().empty() &&
        ctx->GetCatalog() != nullptr) {
        if (!ctx->GetIdentifier()) {
            return Status::Invalid("a catalog write requires a table identifier");
        }
        // A catalog issuing per-table temporary credentials only hands them out through
        // GetTableFileSystem, so the write uses the credentials of this table.
        PAIMON_ASSIGN_OR_RAISE(specific_fs,
                               ctx->GetCatalog()->GetTableFileSystem(ctx->GetIdentifier().value()));
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx->GetOptions(), specific_fs,
                                                ctx->GetFileSystemSchemeToIdentifierMap()));
    std::string branch = ctx->GetBranch();
    std::optional<Identifier> catalog_identifier;
    if (ctx->GetIdentifier()) {
        PAIMON_ASSIGN_OR_RAISE(
            Identifier branch_identifier,
            CatalogUtils::BranchIdentifier(ctx->GetIdentifier().value(), branch));
        catalog_identifier.emplace(std::move(branch_identifier));
    }
    // Only the main branch may have a current schema the catalog alone holds. A branch publishes
    // its own under `branch/branch-<name>`, which is where the load below and a read of the branch
    // both read it, so a catalog answering for no branch schema does not fail a write to one.
    std::optional<std::string> catalog_table_schema;
    if (ctx->GetCatalog() != nullptr) {
        if (!catalog_identifier) {
            return Status::Invalid("a catalog write requires a table identifier");
        }
        if (BranchManager::IsMainBranch(branch)) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> schema,
                                   ctx->GetCatalog()->LoadTableSchema(catalog_identifier.value()));
            PAIMON_ASSIGN_OR_RAISE(catalog_table_schema, schema->GetJsonSchema());
        }
    }
    // A format table writes plain data files into a directory, so it never reaches the manifest
    // path below.
    auto schema_manager =
        std::make_shared<SchemaManager>(tmp_options.GetFileSystem(), ctx->GetRootPath(), branch);
    std::shared_ptr<TableSchema> latest_schema;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FormatTable> format_table,
        FormatTableLoader::TryLoad(tmp_options.GetFileSystem(), ctx->GetRootPath(), branch,
                                   ctx->GetOptions(), catalog_table_schema, schema_manager.get(),
                                   &latest_schema));
    if (format_table != nullptr) {
        return NewFormatTableWrite(format_table, *ctx);
    }
    // The schema the dispatch above already read through `schema_manager`, rather than a second
    // read of the same file.
    if (latest_schema == nullptr) {
        return Status::Invalid(fmt::format("not found latest schema in branch {}", branch));
    }
    const std::shared_ptr<TableSchema>& schema = latest_schema;
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    auto opts = schema->Options();
    for (const auto& [key, value] : ctx->GetOptions()) {
        opts[key] = value;
    }
    // The metadata paths above are built for this branch, so what reads through these options,
    // whose cache keys are scoped by it, has to read the same one.
    opts[Options::BRANCH] = branch;
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions options,
        CoreOptions::FromMap(opts, specific_fs, ctx->GetFileSystemSchemeToIdentifierMap()));
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*schema, options));
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateVectorStore(*schema, options));
    if (options.GetVectorFileFormat() && !ctx->GetWriteSchema().empty()) {
        return Status::NotImplemented(
            "Dedicated VECTOR storage does not yet support custom write schemas or partial writes");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> partition_schema,
                           FieldMapping::GetPartitionSchema(arrow_schema, schema->PartitionKeys()));

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> file_store_path_factory,
        FileStorePathFactory::Create(ctx->GetRootPath(), arrow_schema, schema->PartitionKeys(),
                                     options.GetPartitionDefaultName(),
                                     options.GetWriteFileFormat(/*level=*/0)->Identifier(),
                                     options.DataFilePrefix(), options.LegacyPartitionNameEnabled(),
                                     external_paths, global_index_external_path,
                                     options.IndexFileInDataFileDir(), ctx->GetMemoryPool()));
    auto snapshot_manager =
        std::make_shared<SnapshotManager>(options.GetFileSystem(), ctx->GetRootPath(), branch);
    if (VersionManagedCatalog* versioned = AsVersionManaged(ctx->GetCatalog())) {
        std::shared_ptr<Catalog> catalog = ctx->GetCatalog();
        Identifier identifier = catalog_identifier.value();
        // Keep the catalog alive while the writer uses its snapshot loader.
        snapshot_manager->SetSnapshotLoader(
            [catalog, versioned, identifier]() -> Result<std::optional<Snapshot>> {
                return versioned->LoadSnapshot(identifier);
            });
    }
    std::shared_ptr<IOManager> io_manager;
    const auto& io_temp_dir = ctx->GetTempDirectory();
    if (!io_temp_dir.empty()) {
        io_manager = std::make_shared<IOManager>(io_temp_dir, options.GetFileSystem());
    }

    bool ignore_previous_files = ctx->IgnorePreviousFiles();
    if (ctx->GetRealtimeContext() && !options.RealtimeEnabled()) {
        return Status::Invalid("real-time write requires realtime.enabled=true");
    }
    if (ctx->GetRealtimeContext()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                               RealtimeContextImpl::Cast(ctx->GetRealtimeContext()));
        PAIMON_RETURN_NOT_OK(realtime_context_impl->CheckUsable());
    }
    if (schema->PrimaryKeys().empty()) {
        // append table
        const bool realtime_deduplicate = !options.GetRealtimeDeduplicateKeyFields().empty();
        bool need_dv_maintainer_factory = options.DeletionVectorsEnabled();
        if (options.GetBucket() == -1) {
            need_dv_maintainer_factory = false;
            ignore_previous_files = true;
        } else if (options.GetBucket() <= 0) {
            return Status::Invalid(
                fmt::format("not support bucket {} in append table", options.GetBucket()));
        }
        if (ctx->GetRealtimeContext()) {
            if (!ctx->GetWriteSchema().empty()) {
                return Status::Invalid("real-time append write does not support a custom schema");
            }
            if (!realtime_deduplicate) {
                PAIMON_RETURN_NOT_OK(RestoreRealtimeCommittedProgress(ctx->GetRealtimeContext(),
                                                                      snapshot_manager, options));
            }
        } else if (realtime_deduplicate) {
            if (!ctx->GetWriteSchema().empty() || ignore_previous_files || options.WriteOnly()) {
                return Status::Invalid(
                    "deduplicate compaction requires the full schema, restored files and "
                    "write-only=false");
            }
        }
        std::shared_ptr<arrow::Schema> write_schema = arrow_schema;
        const auto& write_field_names = ctx->GetWriteSchema();
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
        if (ctx->GetRealtimeContext()) {
            const RealtimeStoreMode mode = realtime_deduplicate ? RealtimeStoreMode::DEDUPLICATE
                                                                : RealtimeStoreMode::APPEND_ONLY;
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RealtimeSchemaLayout> layout,
                                   RealtimeSchemaLayout::Create(mode, write_schema));
            realtime_schema_layout = std::move(layout);
        }

        std::shared_ptr<BucketedDvMaintainer::Factory> dv_maintainer_factory;
        if (need_dv_maintainer_factory) {
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<IndexManifestFile> index_manifest_file,
                IndexManifestFile::Create(options.GetFileSystem(), options.GetManifestFormat(),
                                          options.GetManifestCompression(), file_store_path_factory,
                                          options.GetBucket(), ctx->GetMemoryPool(), options));
            auto index_file_handler = std::make_shared<IndexFileHandler>(
                options.GetFileSystem(), std::move(index_manifest_file),
                std::make_shared<IndexFilePathFactories>(file_store_path_factory),
                options.DeletionVectorsBitmap64(), ctx->GetMemoryPool());
            dv_maintainer_factory =
                std::make_shared<BucketedDvMaintainer::Factory>(index_file_handler);
        }

        auto file_store_write = std::make_unique<AppendOnlyFileStoreWrite>(
            file_store_path_factory, snapshot_manager, schema_manager, ctx->GetCommitUser(),
            ctx->GetRootPath(), schema, arrow_schema, write_schema, realtime_schema_layout,
            partition_schema, dv_maintainer_factory, io_manager, options, ignore_previous_files,
            ctx->IsStreamingMode(), ctx->IgnoreNumBucketCheck(), ctx->GetRealtimeContext(),
            ctx->GetExecutor(), ctx->GetMemoryPool());
        if (ctx->GetRealtimeContext() && realtime_deduplicate) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                                   snapshot_manager->LatestSnapshot());
            if (latest_snapshot) {
                PAIMON_RETURN_NOT_OK(
                    file_store_write->RefreshCommittedSnapshot(latest_snapshot->Id()));
            }
        }
        return std::unique_ptr<FileStoreWrite>(std::move(file_store_write));
    } else {
        // pk table
        if (ctx->GetRealtimeContext()) {
            PAIMON_RETURN_NOT_OK(PrimaryKeyTableUtils::ValidateRealtimeOptions(options, *schema));
            if (ignore_previous_files) {
                return Status::NotImplemented(
                    "PK realtime requires restore from the latest snapshot");
            }
            if (!ctx->GetWriteSchema().empty()) {
                return Status::NotImplemented("PK realtime does not support a custom write schema");
            }
            PAIMON_RETURN_NOT_OK(RestoreRealtimeCommittedProgress(ctx->GetRealtimeContext(),
                                                                  snapshot_manager, options));
        }
        if (options.GetBucket() == BucketModeDefine::POSTPONE_BUCKET) {
            return PostponeBucketFileStoreWrite::Create(
                snapshot_manager, schema_manager, ctx->GetCommitUser(), ctx->GetRootPath(), schema,
                arrow_schema, partition_schema, io_manager, options, ctx->IsStreamingMode(),
                ctx->IgnoreNumBucketCheck(), ctx->GetWriteId(),
                ctx->GetFileSystemSchemeToIdentifierMap(), ctx->GetExecutor(), ctx->GetMemoryPool(),
                specific_fs);
        }
        if (options.GetBucket() <= 0) {
            return Status::Invalid(
                fmt::format("not support bucket {} in key value table", options.GetBucket()));
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                               schema->TrimmedPrimaryKeys());
        PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> trimmed_primary_key_fields,
                               schema->GetFields(trimmed_primary_keys));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FieldsComparator> key_comparator,
            FieldsComparator::Create(trimmed_primary_key_fields,
                                     options.SequenceFieldSortOrderIsAscending()));
        auto primary_keys = schema->PrimaryKeys();
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MergeFunction> merge_function,
                               PrimaryKeyTableUtils::CreateMergeFunction(
                                   arrow_schema, primary_keys, options, ctx->GetMemoryPool()));
        if (options.NeedLookup() && options.GetMergeEngine() != MergeEngine::FIRST_ROW) {
            // don't wrap first row, it is already OK
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
                HasHistoricalPrimaryKeyBTreeDefinition(schema_manager, schema->Id()));
            if (has_historical_btree_index) {
                PAIMON_ASSIGN_OR_RAISE(latest_snapshot, snapshot_manager->LatestSnapshot());
                has_existing_index_manifest =
                    latest_snapshot.has_value() && latest_snapshot->IndexManifest().has_value();
            }
        }
        std::shared_ptr<IndexFileHandler> index_file_handler;
        if (options.DeletionVectorsEnabled() || has_btree_index || has_existing_index_manifest) {
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<IndexManifestFile> index_manifest_file,
                IndexManifestFile::Create(options.GetFileSystem(), options.GetManifestFormat(),
                                          options.GetManifestCompression(), file_store_path_factory,
                                          options.GetBucket(), ctx->GetMemoryPool(), options));
            index_file_handler = std::make_shared<IndexFileHandler>(
                options.GetFileSystem(), std::move(index_manifest_file),
                std::make_shared<IndexFilePathFactories>(file_store_path_factory),
                options.DeletionVectorsBitmap64(), ctx->GetMemoryPool());
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
            dv_maintainer_factory =
                std::make_shared<BucketedDvMaintainer::Factory>(index_file_handler);
        }
        std::shared_ptr<BucketedPrimaryKeyIndexMaintainer::Factory>
            primary_key_index_maintainer_factory;
        if (has_btree_index || has_restored_btree_index) {
            primary_key_index_maintainer_factory =
                std::make_shared<BucketedPrimaryKeyIndexMaintainer::Factory>(
                    ctx->GetRootPath(), branch, schema, primary_key_index_definitions.Definitions(),
                    file_store_path_factory, index_file_handler, options, io_manager,
                    ctx->EnableMultiThreadSpill(), ctx->GetExecutor(), ctx->GetMemoryPool());
        }

        std::shared_ptr<RealtimeSchemaLayout> realtime_schema_layout;
        if (ctx->GetRealtimeContext()) {
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<RealtimeSchemaLayout> layout,
                RealtimeSchemaLayout::Create(RealtimeStoreMode::PRIMARY_KEY, arrow_schema));
            realtime_schema_layout = std::move(layout);
        }

        return std::make_unique<KeyValueFileStoreWrite>(
            file_store_path_factory, snapshot_manager, schema_manager, ctx->GetCommitUser(),
            ctx->GetRootPath(), schema, arrow_schema, realtime_schema_layout, partition_schema,
            dv_maintainer_factory, primary_key_index_maintainer_factory, io_manager, key_comparator,
            sequence_fields_comparator, merge_function_wrapper, options, ignore_previous_files,
            ctx->IsStreamingMode(), ctx->IgnoreNumBucketCheck(), ctx->EnableMultiThreadSpill(),
            ctx->GetRealtimeContext(), ctx->GetExecutor(), ctx->GetMemoryPool());
    }
}

namespace {

class TableScanImpl {
 public:
    static Result<std::unique_ptr<FileStoreScan>> CreateFileStoreScan(
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<TableSchema>& table_schema, const CoreOptions& core_options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& memory_pool,
        const ScanContext* context) {
        auto fs = core_options.GetFileSystem();
        auto manifest_file_format = core_options.GetManifestFormat();
        std::string branch = BranchManager::NormalizeBranch(core_options.GetBranch());
        auto snapshot_manager = std::make_shared<SnapshotManager>(fs, context->GetPath(), branch);
        // TODO(liancheng.lsz): support fallback branch in scan
        auto schema_manager = std::make_shared<SchemaManager>(fs, context->GetPath(), branch);
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<ManifestList> manifest_list,
            ManifestList::Create(fs, manifest_file_format, core_options.GetManifestCompression(),
                                 path_factory, core_options.GetCache(), memory_pool));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Schema> partition_schema,
            FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<ManifestFile> manifest_file,
            ManifestFile::Create(fs, manifest_file_format, core_options.GetManifestCompression(),
                                 path_factory, core_options.GetManifestTargetFileSize(),
                                 memory_pool, core_options, partition_schema));
        std::unique_ptr<FileStoreScan> scan;
        if (table_schema->PrimaryKeys().empty()) {
            if (core_options.DataEvolutionEnabled()) {
                PAIMON_ASSIGN_OR_RAISE(
                    scan, DataEvolutionFileStoreScan::Create(
                              snapshot_manager, schema_manager, manifest_list, manifest_file,
                              table_schema, arrow_schema, context->GetScanFilters(), core_options,
                              executor, memory_pool));
            } else {
                PAIMON_ASSIGN_OR_RAISE(
                    scan, AppendOnlyFileStoreScan::Create(
                              snapshot_manager, schema_manager, manifest_list, manifest_file,
                              table_schema, arrow_schema, context->GetScanFilters(), core_options,
                              executor, memory_pool));
            }
        } else {
            PAIMON_ASSIGN_OR_RAISE(
                scan, KeyValueFileStoreScan::Create(snapshot_manager, schema_manager, manifest_list,
                                                    manifest_file, table_schema, arrow_schema,
                                                    context->GetScanFilters(), core_options,
                                                    executor, memory_pool));
        }
        return WithTablePath(std::move(scan), context);
    }

    static std::unique_ptr<FileStoreScan> WithTablePath(std::unique_ptr<FileStoreScan>&& scan,
                                                        const ScanContext* context) {
        scan->WithTablePath(context->GetPath());
        return std::move(scan);
    }

    static Result<std::unique_ptr<SplitGenerator>> CreateSplitGenerator(
        const std::shared_ptr<TableSchema>& table_schema, const CoreOptions& core_options,
        const ScanContext* context) {
        auto source_split_target_size = core_options.GetSourceSplitTargetSize();
        auto source_split_open_file_cost = core_options.GetSourceSplitOpenFileCost();
        if (table_schema->PrimaryKeys().empty()) {
            if (core_options.DataEvolutionEnabled()) {
                return std::make_unique<DataEvolutionSplitGenerator>(
                    source_split_target_size, source_split_open_file_cost,
                    core_options.BlobSplitByFileSize());
            }
            BucketMode bucket_mode = (core_options.GetBucket() == -1 ? BucketMode::BUCKET_UNAWARE
                                                                     : BucketMode::HASH_FIXED);
            return std::make_unique<AppendOnlySplitGenerator>(
                source_split_target_size, source_split_open_file_cost, bucket_mode);
        } else {
            // TODO(liancheng.lsz): support evolution
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                                   table_schema->TrimmedPrimaryKeys());
            PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> trimmed_pk_fields,
                                   table_schema->GetFields(trimmed_primary_keys));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<FieldsComparator> key_comparator,
                FieldsComparator::Create(trimmed_pk_fields, /*is_ascending_order=*/true));
            return std::make_unique<MergeTreeSplitGenerator>(
                source_split_target_size, source_split_open_file_cost,
                core_options.DeletionVectorsEnabled(), core_options.GetMergeEngine(),
                key_comparator);
        }
    }

    static Result<std::unique_ptr<IndexFileHandler>> CreateIndexFileHandler(
        const CoreOptions& core_options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexManifestFile> index_manifest_file,
                               IndexManifestFile::Create(
                                   core_options.GetFileSystem(), core_options.GetManifestFormat(),
                                   core_options.GetManifestCompression(), path_factory,
                                   core_options.GetBucket(), memory_pool, core_options));
        return std::make_unique<IndexFileHandler>(
            core_options.GetFileSystem(), std::move(index_manifest_file),
            std::make_shared<IndexFilePathFactories>(path_factory),
            core_options.DeletionVectorsBitmap64(), memory_pool);
    }
};

/// @param loaded_schema The table's schema, when the caller already read it, or null when it has
///        to be read here. `TableScan::Create()` reads one to dispatch on its table type.
Result<std::unique_ptr<TableScan>> NewDataTableScan(
    const std::shared_ptr<ScanContext>& context, const std::shared_ptr<TableSchema>& loaded_schema);

/// Maps a `ScanContext` onto `FormatTableScan`, which takes one partition filter and a limit and
/// nothing else.
///
/// A format table has no buckets, so a bucket filter has nothing to act on. A predicate is
/// refused too, which is narrower than Java: `FormatReadBuilder.newScan()` hands the partition
/// half of one to the scan to prune directories with, which this does not do yet.
Result<std::unique_ptr<TableScan>> NewFormatTableScan(const std::shared_ptr<FormatTable>& table,
                                                      const std::shared_ptr<ScanContext>& context) {
    // Anything a `ScanContext` carries that a format table cannot honour is refused rather than
    // silently dropped.
    if (context->IsStreamingMode()) {
        return Status::NotImplemented(
            "a format table has no snapshots, so there is nothing for a streaming scan to follow");
    }
    if (context->GetRealtimeContext() != nullptr) {
        return Status::NotImplemented(
            "a format table has no real-time store to union with what is on disk");
    }
    if (context->GetGlobalIndexResult() != nullptr) {
        return Status::NotImplemented("a format table carries no global index");
    }
    std::map<std::string, std::string> partition_filter;
    std::shared_ptr<ScanFilter> filters = context->GetScanFilters();
    if (filters != nullptr) {
        if (filters->GetPredicate() != nullptr) {
            return Status::NotImplemented(
                "a format table scan does not take a predicate: its files carry no statistics to "
                "skip by, and pruning by the partition columns a predicate names is not "
                "implemented yet; give the partition values as a partition filter instead");
        }
        if (filters->GetBucketFilter()) {
            return Status::NotImplemented("a format table has no buckets to filter by");
        }
        const std::vector<std::map<std::string, std::string>>& partition_filters =
            filters->GetPartitionFilters();
        if (partition_filters.size() > 1) {
            return Status::NotImplemented("a format table scan takes at most one partition filter");
        }
        if (partition_filters.size() == 1) {
            partition_filter = partition_filters.front();
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableScan> scan,
                           FormatTableScan::Create(table, partition_filter, context->GetLimit()));
    return std::unique_ptr<TableScan>(std::move(scan));
}

}  // namespace

Result<std::unique_ptr<TableScan>> TableScan::Create(std::unique_ptr<ScanContext> context) {
    if (context == nullptr) {
        return Status::Invalid("scan context is null pointer");
    }
    if (context->GetMemoryPool() == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    if (context->GetExecutor() == nullptr) {
        return Status::Invalid("executor is null pointer");
    }

    std::shared_ptr<ScanContext> shared_context = std::move(context);
    // A table the caller already loaded says what it is, so nothing is read to find out.
    if (shared_context->GetFormatTable() != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FormatTable> format_table,
            FormatTable::Copy(shared_context->GetFormatTable(), shared_context->GetOptions()));
        return NewFormatTableScan(format_table, shared_context);
    }
    // load schema
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(shared_context->GetOptions(),
                                                shared_context->GetSpecificFileSystem(), {}));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(shared_context->GetPath()));
    if (system_table_path) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<SystemTable> system_table,
            SystemTableLoader::LoadFromPath(tmp_options.GetFileSystem(), shared_context->GetPath(),
                                            shared_context->GetOptions()));
        return system_table->NewScan(shared_context);
    }
    // A format table is planned by listing directories, so it never reaches the manifest path
    // below. One `TableScan` interface serves both, as Java Paimon serves both through one
    // `ReadBuilder`.
    std::shared_ptr<TableSchema> latest_schema;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FormatTable> format_table,
        FormatTableLoader::TryLoad(tmp_options.GetFileSystem(), shared_context->GetPath(),
                                   BranchManager::NormalizeBranch(tmp_options.GetBranch()),
                                   shared_context->GetOptions(),
                                   shared_context->GetSpecificTableSchema(),
                                   /*schema_manager=*/nullptr, &latest_schema));
    if (format_table != nullptr) {
        return NewFormatTableScan(format_table, shared_context);
    }
    // With the schema the dispatch already read, so the managed path does not read it again.
    return NewDataTableScan(shared_context, latest_schema);
}

namespace {

Status ValidateRealtimeScan(const TableSchema& table_schema, const CoreOptions& core_options,
                            const ScanContext& context, bool read_optimized) {
    if (!context.GetRealtimeContext()) {
        return Status::OK();
    }
    if (!core_options.RealtimeEnabled()) {
        return Status::Invalid("real-time scan requires realtime.enabled=true");
    }
    if (core_options.GetBucket() <= 0) {
        return Status::Invalid("real-time union read requires fixed bucket mode");
    }
    if (core_options.DataEvolutionEnabled()) {
        return Status::Invalid("real-time union read does not support data evolution");
    }
    if (!table_schema.PrimaryKeys().empty()) {
        if (read_optimized) {
            return Status::NotImplemented(
                "PK real-time union read does not support read-optimized scans");
        }
        PAIMON_RETURN_NOT_OK(
            PrimaryKeyTableUtils::ValidateRealtimeOptions(core_options, table_schema));
    }
    if (context.IsStreamingMode()) {
        return Status::Invalid("real-time union read currently supports batch scans only");
    }
    if (context.GetLimit()) {
        return Status::Invalid("real-time union read does not support scan limit pushdown");
    }
    if (context.GetGlobalIndexResult()) {
        return Status::Invalid("real-time union read does not support global index splits");
    }
    StartupMode startup_mode = core_options.GetStartupMode();
    const bool specified_snapshot =
        startup_mode == StartupMode::FromSnapshot() && core_options.GetScanSnapshotId().has_value();
    if (!(startup_mode == StartupMode::LatestFull() || startup_mode == StartupMode::Latest() ||
          specified_snapshot)) {
        return Status::Invalid(
            "real-time union read requires the latest snapshot or an explicit snapshot id");
    }
    return Status::OK();
}

Result<std::unique_ptr<TableScan>> NewDataTableScan(
    const std::shared_ptr<ScanContext>& context,
    const std::shared_ptr<TableSchema>& loaded_schema) {
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions tmp_options,
        CoreOptions::FromMap(context->GetOptions(), context->GetSpecificFileSystem(), {}));
    std::string branch = BranchManager::NormalizeBranch(tmp_options.GetBranch());
    std::shared_ptr<TableSchema> table_schema;
    const auto& specific_table_schema = context->GetSpecificTableSchema();
    if (loaded_schema != nullptr) {
        table_schema = loaded_schema;
    } else if (branch == BranchManager::DEFAULT_MAIN_BRANCH && specific_table_schema) {
        PAIMON_ASSIGN_OR_RAISE(table_schema,
                               TableSchema::CreateFromJson(specific_table_schema.value()));
    } else {
        SchemaManager schema_manager(tmp_options.GetFileSystem(), context->GetPath(), branch);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_table_schema,
                               schema_manager.Latest());
        if (latest_table_schema == std::nullopt) {
            return Status::Invalid("not found latest schema");
        }
        table_schema = latest_table_schema.value();
    }
    PAIMON_ASSIGN_OR_RAISE(bool read_optimized,
                           OptionsUtils::GetValueFromMap<bool>(context->GetOptions(),
                                                               kReadOptimizedScanOption, false));
    // merge options
    auto options = table_schema->Options();
    for (const auto& [key, value] : context->GetOptions()) {
        options[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(options, context->GetSpecificFileSystem(), {}));
    core_options.WithCache(context->GetCache());

    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*table_schema, core_options));

    PAIMON_RETURN_NOT_OK(
        ValidateRealtimeScan(*table_schema, core_options, *context, read_optimized));
    // validate options
    if (core_options.GetBucket() == -1) {
        if (!table_schema->PrimaryKeys().empty()) {
            return Status::NotImplemented(fmt::format(
                "do not support pk table bucket={} in scan process", core_options.GetBucket()));
        }
    } else if (core_options.GetBucket() < 1 &&
               !SchemaValidation::IsPostponeBucketTable(*table_schema, core_options.GetBucket())) {
        return Status::Invalid(
            fmt::format("do not support bucket={} in scan process", core_options.GetBucket()));
    }

    // validate schema and scan filter
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    if (context->GetScanFilters() && context->GetScanFilters()->GetPredicate()) {
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
            *arrow_schema, context->GetScanFilters()->GetPredicate(),
            /*validate_field_idx=*/false));
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           core_options.CreateGlobalIndexExternalPath());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            context->GetPath(), arrow_schema, table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            context->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan> file_store_scan,
                           TableScanImpl::CreateFileStoreScan(
                               path_factory, arrow_schema, table_schema, core_options,
                               context->GetExecutor(), context->GetMemoryPool(), context.get()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<SplitGenerator> split_generator,
        TableScanImpl::CreateSplitGenerator(table_schema, core_options, context.get()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexFileHandler> index_file_handler,
                           TableScanImpl::CreateIndexFileHandler(core_options, path_factory,
                                                                 context->GetMemoryPool()));
    auto snapshot_reader = std::make_shared<SnapshotReader>(
        file_store_scan, path_factory, std::move(split_generator), std::move(index_file_handler));
    const bool pk_table = !table_schema->PrimaryKeys().empty();
    if (read_optimized && pk_table && context->IsStreamingMode()) {
        return Status::NotImplemented(
            "read-optimized system table does not support streaming scan for primary key table");
    }
    if (context->IsStreamingMode()) {
        return std::make_unique<DataTableStreamScan>(core_options, snapshot_reader);
    }
    auto batch_scan = std::make_unique<DataTableBatchScan>(
        /*pk_table=*/pk_table, core_options, snapshot_reader, read_optimized, context->GetLimit(),
        /*realtime_pk_scan=*/pk_table && context->GetRealtimeContext() != nullptr);
    if (context->GetRealtimeContext()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context,
                               RealtimeContextImpl::Cast(context->GetRealtimeContext()));
        return std::make_unique<RealtimeTableScan>(
            std::move(batch_scan), pk_table, realtime_context, path_factory,
            snapshot_reader->GetSnapshotManager(), core_options.GetFileSystem(),
            context->GetScanFilters(), core_options.GetRealtimeReadViewTtlMillis());
    }
    if (core_options.DataEvolutionEnabled()) {
        return std::make_unique<DataEvolutionBatchScan>(
            context->GetPath(), snapshot_reader, std::move(batch_scan), table_schema,
            context->GetGlobalIndexResult(), core_options, context->GetMemoryPool(),
            context->GetExecutor());
    }
    if (pk_table && !read_optimized && core_options.GlobalIndexEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexDefinitions definitions,
                               PrimaryKeyIndexDefinitions::Create(*table_schema));
        if (!definitions.ScalarDefinitions().empty()) {
            return PrimaryKeyIndexBatchScan::Create(snapshot_reader, std::move(batch_scan),
                                                    table_schema, path_factory, core_options,
                                                    context->GetMemoryPool());
        }
    }
    return batch_scan;
}

}  // namespace

namespace {

/// @param loaded_schema The schema of `branch`, when the caller already read it, or null when it
///        has to be read here: `TableRead::Create()` reads one to dispatch on the table type, and
///        reading it again would cost a second listing and read.
Result<std::unique_ptr<InternalReadContext>> CreateInternalReadContext(
    const std::shared_ptr<ReadContext>& context, const std::string& branch,
    const std::shared_ptr<TableSchema>& loaded_schema) {
    std::map<std::string, std::string> tmp_options = context->GetOptions();
    std::shared_ptr<TableSchema> table_schema;
    const auto& specific_table_schema = context->GetSpecificTableSchema();
    if (loaded_schema != nullptr) {
        table_schema = loaded_schema;
    } else if (branch == BranchManager::DEFAULT_MAIN_BRANCH && specific_table_schema) {
        PAIMON_ASSIGN_OR_RAISE(table_schema,
                               TableSchema::CreateFromJson(specific_table_schema.value()));
    } else {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_core_options,
                               CoreOptions::FromMap(tmp_options, context->GetSpecificFileSystem(),
                                                    context->GetFileSystemSchemeToIdentifierMap()));
        SchemaManager schema_manager(tmp_core_options.GetFileSystem(), context->GetPath(), branch);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                               schema_manager.Latest());
        if (!latest_schema) {
            return Status::Invalid(fmt::format("schema file not found in path {}, branch {}",
                                               context->GetPath(), branch));
        }
        table_schema = latest_schema.value();
    }
    assert(table_schema);

    // merge options
    auto options = table_schema->Options();
    for (const auto& [key, value] : tmp_options) {
        options[key] = value;
    }
    if (branch != BranchManager::DEFAULT_MAIN_BRANCH) {
        options[Options::BRANCH] = branch;
    }
    return InternalReadContext::Create(context, table_schema, options);
}

Result<std::unique_ptr<TableRead>> CreateTableRead(
    const std::shared_ptr<InternalReadContext>& internal_context,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor) {
    if (PredicateUtils::ContainsFullTextMatch(internal_context->GetPredicate())) {
        return Status::NotImplemented("FullTextMatch predicate execution is not implemented yet");
    }
    if (!internal_context->GetIndexScoreBindings().empty()) {
        return Status::NotImplemented("Per-field index score output is not implemented yet");
    }
    const auto& core_options = internal_context->GetCoreOptions();
    const auto& table_schema = internal_context->GetTableSchema();
    if (internal_context->GetRealtimeContext() && !core_options.RealtimeEnabled()) {
        return Status::Invalid("real-time read requires realtime.enabled=true");
    }
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*table_schema, core_options));
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           core_options.CreateGlobalIndexExternalPath());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            internal_context->GetPath(), arrow_schema, table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            memory_pool));

    if (internal_context->GetPrimaryKeys().empty()) {
        return std::make_unique<AppendOnlyTableRead>(path_factory, internal_context, memory_pool,
                                                     executor);
    }
    return KeyValueTableRead::Create(path_factory, internal_context, memory_pool, executor);
}

Result<std::unique_ptr<TableRead>> NewDataTableRead(
    const std::shared_ptr<ReadContext>& context,
    const std::shared_ptr<TableSchema>& loaded_schema) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> internal_context,
                           CreateInternalReadContext(context, context->GetBranch(), loaded_schema));

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<TableRead> table_read,
        CreateTableRead(internal_context, context->GetMemoryPool(), context->GetExecutor()));

    std::optional<std::string> scan_fallback_branch =
        internal_context->GetCoreOptions().GetScanFallbackBranch();
    if (!scan_fallback_branch ||
        StringUtils::IsNullOrWhitespaceOnly(scan_fallback_branch.value())) {
        return std::move(table_read);
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> fallback_context,
        CreateInternalReadContext(context, /*branch=*/scan_fallback_branch.value(),
                                  /*loaded_schema=*/nullptr));

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<TableRead> fallback_table_read,
        CreateTableRead(fallback_context, context->GetMemoryPool(), context->GetExecutor()));
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool = internal_context->GetArrowMemoryPool();
    return std::make_unique<FallbackTableRead>(std::move(table_read),
                                               std::move(fallback_table_read), arrow_pool);
}

/// Maps a `ReadContext` onto `FormatTableRead`, which reads everything it needs out of the context
/// itself: the columns, the predicate, the pool and executor, and what a file is opened with. It
/// refuses by name what a format table cannot honour.
Result<std::unique_ptr<TableRead>> NewFormatTableRead(const std::shared_ptr<FormatTable>& table,
                                                      const std::shared_ptr<ReadContext>& context) {
    if (PredicateUtils::ContainsFullTextMatch(context->GetPredicate())) {
        return Status::NotImplemented("FullTextMatch predicate execution is not implemented yet");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableRead> read,
                           FormatTableRead::Create(table, context));
    return std::unique_ptr<TableRead>(std::move(read));
}

}  // namespace

Result<std::unique_ptr<TableRead>> TableRead::Create(std::unique_ptr<ReadContext> ctx) {
    std::shared_ptr<ReadContext> context = std::move(ctx);
    if (context == nullptr) {
        return Status::Invalid("read context is null pointer");
    }
    if (context->GetMemoryPool() == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    if (context->GetExecutor() == nullptr) {
        return Status::Invalid("executor is null pointer");
    }
    // A table the caller already loaded says what it is, so nothing is read to find out.
    if (context->GetFormatTable() != nullptr) {
        if (context->HasVectorOrFullTextSearch()) {
            return Status::NotImplemented(
                "VectorSearch or FullTextSearch is not supported for format tables");
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> format_table,
                               FormatTable::Copy(context->GetFormatTable(), context->GetOptions()));
        return NewFormatTableRead(format_table, context);
    }
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions tmp_core_options,
        CoreOptions::FromMap(context->GetOptions(), context->GetSpecificFileSystem(),
                             context->GetFileSystemSchemeToIdentifierMap()));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(context->GetPath()));
    if (system_table_path) {
        if (PredicateUtils::ContainsFullTextMatch(context->GetPredicate())) {
            return Status::NotImplemented(
                "FullTextMatch predicate execution is not implemented yet");
        }
        if (context->HasVectorOrFullTextSearch()) {
            return Status::NotImplemented(
                "VectorSearch or FullTextSearch is not supported for system tables");
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<SystemTable> system_table,
            SystemTableLoader::LoadFromPath(tmp_core_options.GetFileSystem(), context->GetPath(),
                                            context->GetOptions()));
        return system_table->NewRead(context);
    }

    // A format table has no manifest and a file another engine wrote carries no field ids, so
    // which files there are and how a row is put back together is its own. How a file is opened
    // is not: both paths go through `DataFileReaderFactory`.
    std::shared_ptr<TableSchema> latest_schema;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FormatTable> format_table,
        FormatTableLoader::TryLoad(tmp_core_options.GetFileSystem(), context->GetPath(),
                                   context->GetBranch(), context->GetOptions(),
                                   context->GetSpecificTableSchema(),
                                   /*schema_manager=*/nullptr, &latest_schema));
    if (format_table != nullptr) {
        if (context->HasVectorOrFullTextSearch()) {
            return Status::NotImplemented(
                "VectorSearch or FullTextSearch is not supported for format tables");
        }
        return NewFormatTableRead(format_table, context);
    }

    // With the schema the dispatch already read, so the managed path does not read it again.
    return NewDataTableRead(context, latest_schema);
}

namespace {

CommitScanner::ScanSupplier CreateAppendScanSupplier(
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<ManifestList>& manifest_list,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
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

CommitScanner::ScanSupplier CreatePkScanSupplier(
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<ManifestList>& manifest_list,
    const std::shared_ptr<ManifestFile>& manifest_file,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
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

bool IsCatalogCommit(const CommitContext& ctx) {
    return AsVersionManaged(ctx.GetCatalog()) != nullptr;
}

Result<std::shared_ptr<SnapshotCommit>> NewSnapshotCommit(
    const CommitContext& ctx, const std::optional<Identifier>& catalog_identifier,
    const CoreOptions& options, const std::shared_ptr<SnapshotManager>& snapshot_manager) {
    if (IsCatalogCommit(ctx)) {
        if (!catalog_identifier) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitLock> lock,
                               AsVersionManaged(ctx.GetCatalog())
                                   ->CreateCommitLock(catalog_identifier.value(), ctx.GetTableId(),
                                                      ctx.GetCommitUser(), options));
        // Reuse the caller's UUID so table recreation cannot redirect this commit.
        return std::shared_ptr<SnapshotCommit>(std::make_shared<CatalogSnapshotCommit>(
            ctx.GetCatalog(), catalog_identifier.value(), ctx.GetTableId(), std::move(lock)));
    }
    if (ctx.UseRESTCatalogCommit()) {
        if (options.RestCommitAcquireLock()) {
            return Status::Invalid(fmt::format(
                "'{}' needs a commit through a REST catalog with WithCatalog(); a commit table "
                "request built for the caller to send cannot hold a commit lease",
                Options::REST_COMMIT_ACQUIRE_LOCK));
        }
        return std::shared_ptr<SnapshotCommit>(
            std::make_shared<CatalogSnapshotCommit>(ctx.GetTableId()));
    }
    // Catalogs without version management may still supply a table ID.
    if (ctx.GetTableId() && ctx.GetCatalog() == nullptr) {
        return Status::Invalid(
            "a commit which publishes the snapshot by writing it to the table directory sends no "
            "commit table request, so there is nowhere to put a table id; commit through a "
            "catalog with WithCatalog() or ask for the request with UseRESTCatalogCommit()");
    }
    return std::shared_ptr<SnapshotCommit>(
        std::make_shared<RenamingSnapshotCommit>(options.GetFileSystem(), snapshot_manager));
}

/// Creates a format-table commit, rejecting non-default options that require snapshots.
Result<std::unique_ptr<FileStoreCommit>> NewFormatTableCommit(
    const std::shared_ptr<FormatTable>& table, const CommitContext& ctx) {
    if (!ctx.IgnoreEmptyCommit()) {
        return Status::NotImplemented(
            "a format table cannot record an empty commit: keeping one means writing a "
            "snapshot that adds no files, and there are no snapshots here");
    }
    if (ctx.UseRESTCatalogCommit()) {
        return Status::NotImplemented(
            "a format table commits by renaming files into place, not by sending a snapshot "
            "to a rest catalog");
    }
    if (ctx.GetTableId()) {
        return Status::NotImplemented(
            "a format table sends no commit table request, so there is nowhere to put a table id");
    }
    if (ctx.GetCatalog() != nullptr) {
        return Status::NotImplemented(
            "a format table keeps no snapshot for a catalog to take, so it commits by renaming "
            "files into place rather than through the catalog it was loaded from");
    }
    if (ctx.AppendCommitCheckConflict()) {
        return Status::NotImplemented(
            "a format table has no manifests to check a concurrent commit against");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableFileStoreCommit> format_commit,
                           FormatTableFileStoreCommit::Create(table));
    return std::unique_ptr<FileStoreCommit>(std::move(format_commit));
}

}  // namespace

Result<std::unique_ptr<FileStoreCommit>> FileStoreCommit::Create(
    std::unique_ptr<CommitContext> ctx) {
    if (ctx == nullptr) {
        return Status::Invalid("commit context is null pointer");
    }
    if (ctx->GetMemoryPool() == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    if (ctx->GetExecutor() == nullptr) {
        return Status::Invalid("executor is null pointer");
    }

    // A table the caller already loaded says what it is, so nothing is read to find out.
    if (ctx->GetFormatTable() != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> given_table,
                               FormatTable::Copy(ctx->GetFormatTable(), ctx->GetOptions()));
        return NewFormatTableCommit(given_table, *ctx);
    }

    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx->GetCatalog()));
    // Use catalog credentials unless the caller supplied a file system.
    std::shared_ptr<FileSystem> specific_fs = ctx->GetSpecificFileSystem();
    if (specific_fs == nullptr && ctx->GetCatalog() != nullptr) {
        if (!ctx->GetIdentifier()) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        // Use the credentials of this table, which a catalog issuing per-table temporary
        // ones only hands out through GetTableFileSystem.
        PAIMON_ASSIGN_OR_RAISE(specific_fs,
                               ctx->GetCatalog()->GetTableFileSystem(ctx->GetIdentifier().value()));
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx->GetOptions(), specific_fs));
    const std::string& root_path = ctx->GetRootPath();
    // The branch the context was built for, already validated by `CommitContextBuilder`.
    PAIMON_ASSIGN_OR_RAISE(std::string branch, BranchManager::ResolveBranch(
                                                   ctx->GetIdentifier(), ctx->GetOptions(),
                                                   /*explicit_branch=*/std::nullopt, "commit"));
    std::optional<Identifier> catalog_identifier;
    if (ctx->GetIdentifier()) {
        PAIMON_ASSIGN_OR_RAISE(
            Identifier branch_identifier,
            CatalogUtils::BranchIdentifier(ctx->GetIdentifier().value(), branch));
        catalog_identifier.emplace(std::move(branch_identifier));
    }
    // Catalog-managed schemas may be absent from the table directory, but only on the main branch:
    // a branch publishes its own under `branch/branch-<name>`, which is where the load below and a
    // read of the branch both read it, so a catalog answering for no branch schema does not fail
    // a commit to one.
    std::optional<std::string> catalog_table_schema;
    FileStoreCommitImpl::SchemaIdLoader schema_id_loader;
    if (ctx->GetCatalog() != nullptr) {
        if (!catalog_identifier) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        if (BranchManager::IsMainBranch(branch)) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> loaded_schema,
                                   ctx->GetCatalog()->LoadTableSchema(catalog_identifier.value()));
            PAIMON_ASSIGN_OR_RAISE(std::string schema_json, loaded_schema->GetJsonSchema());
            catalog_table_schema = std::move(schema_json);
        }
        std::shared_ptr<Catalog> catalog = ctx->GetCatalog();
        Identifier identifier = catalog_identifier.value();
        schema_id_loader = [catalog, identifier]() -> Result<int64_t> {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> schema,
                                   catalog->LoadTableSchema(identifier));
            std::shared_ptr<DataSchema> data_schema = std::dynamic_pointer_cast<DataSchema>(schema);
            if (data_schema == nullptr) {
                return Status::Invalid(
                    "the catalog did not hand back a data table schema, which has the id a "
                    "snapshot records");
            }
            return data_schema->Id();
        };
    }
    // A format table commits by renaming files into place, so it never reaches the snapshot path
    // below. The managed path here reads the commit's branch, so this reads the same one: the two
    // must not dispatch on different schemas.
    auto schema_manager =
        std::make_shared<SchemaManager>(tmp_options.GetFileSystem(), root_path, branch);
    std::shared_ptr<TableSchema> latest_schema;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> format_table,
                           FormatTableLoader::TryLoad(
                               tmp_options.GetFileSystem(), root_path, branch, ctx->GetOptions(),
                               catalog_table_schema, schema_manager.get(), &latest_schema));
    if (format_table != nullptr) {
        return NewFormatTableCommit(format_table, *ctx);
    }
    if (latest_schema == nullptr) {
        return Status::Invalid(fmt::format("not found latest schema in branch {}", branch));
    }
    const std::shared_ptr<TableSchema>& schema = latest_schema;
    auto opts = schema->Options();
    for (const auto& [key, value] : ctx->GetOptions()) {
        opts[key] = value;
    }
    // The metadata paths below are built for this branch, so what reads through these options,
    // whose cache keys are scoped by it, has to read the same one.
    opts[Options::BRANCH] = branch;
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(opts, specific_fs));
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*schema, options));
    assert(options.GetFileSystem());
    assert(options.GetFileFormat());
    PAIMON_RETURN_NOT_OK(FileStoreCommitImpl::ValidateCommitOptions(options));
    PAIMON_ASSIGN_OR_RAISE(bool is_object_store, FileSystem::IsObjectStore(root_path));
    // Catalog commits do not require atomic rename on the object store.
    if (is_object_store && !ctx->UseRESTCatalogCommit() && !IsCatalogCommit(*ctx) &&
        opts.find("enable-object-store-commit-in-inte-test") == opts.end()) {
        return Status::NotImplemented(
            "commit operation does not support object store file system for now");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
                           BinaryRowPartitionComputer::Create(schema->PartitionKeys(), arrow_schema,
                                                              options.GetPartitionDefaultName(),
                                                              options.LegacyPartitionNameEnabled(),
                                                              ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            root_path, arrow_schema, schema->PartitionKeys(), options.GetPartitionDefaultName(),
            options.GetFileFormat()->Identifier(), options.DataFilePrefix(),
            options.LegacyPartitionNameEnabled(), external_paths, global_index_external_path,
            options.IndexFileInDataFileDir(), ctx->GetMemoryPool()));

    auto snapshot_manager =
        std::make_shared<SnapshotManager>(options.GetFileSystem(), root_path, branch);
    if (IsCatalogCommit(*ctx)) {
        std::shared_ptr<Catalog> catalog = ctx->GetCatalog();
        VersionManagedCatalog* versioned = AsVersionManaged(catalog);
        Identifier identifier = catalog_identifier.value();
        // Capture the catalog to keep the versioned interface alive.
        snapshot_manager->SetSnapshotLoader(
            [catalog, versioned, identifier]() -> Result<std::optional<Snapshot>> {
                return versioned->LoadSnapshot(identifier);
            });
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(options.GetFileSystem(), options.GetManifestFormat(),
                             options.GetManifestCompression(), path_factory, options.GetCache(),
                             ctx->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> partition_schema,
                           FieldMapping::GetPartitionSchema(arrow_schema, schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(options.GetFileSystem(), options.GetManifestFormat(),
                             options.GetManifestCompression(), path_factory,
                             options.GetManifestTargetFileSize(), ctx->GetMemoryPool(), options,
                             partition_schema));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<IndexManifestFile> index_manifest_file,
        IndexManifestFile::Create(options.GetFileSystem(), options.GetManifestFormat(),
                                  options.GetManifestCompression(), path_factory,
                                  options.GetBucket(), ctx->GetMemoryPool(), options));

    auto expire_snapshots = std::make_shared<ExpireSnapshots>(
        snapshot_manager, path_factory, manifest_list, manifest_file, index_manifest_file,
        options.GetFileSystem(), options.GetExpireConfig(), options.RealtimeEnabled(),
        ctx->GetExecutor());

    CommitScanner::ScanSupplier scan_supplier;
    if (schema->PrimaryKeys().empty()) {
        scan_supplier = CreateAppendScanSupplier(snapshot_manager, schema_manager, manifest_list,
                                                 manifest_file, schema, arrow_schema, options,
                                                 ctx->GetExecutor(), ctx->GetMemoryPool());
    } else {
        scan_supplier = CreatePkScanSupplier(snapshot_manager, schema_manager, manifest_list,
                                             manifest_file, schema, arrow_schema, options,
                                             ctx->GetExecutor(), ctx->GetMemoryPool());
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SnapshotCommit> snapshot_commit,
                           NewSnapshotCommit(*ctx, catalog_identifier, options, snapshot_manager));

    return std::make_unique<FileStoreCommitImpl>(
        ctx->GetMemoryPool(), ctx->GetExecutor(), arrow_schema, root_path, ctx->GetCommitUser(),
        options, path_factory, std::move(partition_computer), snapshot_manager, snapshot_commit,
        ctx->IgnoreEmptyCommit(), ctx->AppendCommitCheckConflict(), schema,
        std::move(schema_id_loader), manifest_file, manifest_list, index_manifest_file,
        expire_snapshots, schema_manager, std::move(scan_supplier));
}

}  // namespace paimon
