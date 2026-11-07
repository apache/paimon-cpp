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

#include "paimon/core/table/file_store_table.h"

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/sensitive_config_utils.h"
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
#include "paimon/core/index/pk/primary_key_index_definitions.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/operation/expire_snapshots.h"
#include "paimon/core/operation/file_store_commit_impl.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/append_only_file_store_table.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/primary_key_file_store_table.h"
#include "paimon/core/table/source/append_only_table_read.h"
#include "paimon/core/table/source/data_evolution_batch_scan.h"
#include "paimon/core/table/source/data_table_batch_scan.h"
#include "paimon/core/table/source/data_table_stream_scan.h"
#include "paimon/core/table/source/fallback_table_read.h"
#include "paimon/core/table/source/key_value_table_read.h"
#include "paimon/core/table/source/primary_key_index_batch_scan.h"
#include "paimon/core/table/source/read_optimized_scan_options.h"
#include "paimon/core/table/source/realtime_table_scan.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/context_utils.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/primary_key_table_utils.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/read_context.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/write_context.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

namespace {

/// Allows object-store commits in integration tests.
constexpr const char kObjectStoreCommitInteTestOption[] = "enable-object-store-commit-in-inte-test";

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

}  // namespace

class Executor;
class MemoryPool;

FileStoreTable::FileStoreTable(std::unique_ptr<FileStore> store,
                               const std::shared_ptr<TableSchema>& schema,
                               const Identifier& identifier, const std::string& uuid)
    : Table(store->GetFileSystem(), store->GetRootPath(), schema, identifier, uuid),
      store_(std::move(store)) {}

Result<std::shared_ptr<FileStoreTable>> FileStoreTable::Create(
    const std::shared_ptr<FileSystem>& specified_file_system, const std::string& root_path,
    const std::shared_ptr<TableSchema>& schema, const Identifier& identifier,
    const std::string& uuid, const std::string& branch,
    const std::map<std::string, std::string>& fs_scheme_map) {
    if (schema == nullptr) {
        return Status::Invalid(
            fmt::format("not found latest schema of {}", identifier.GetFullName()));
    }
    PAIMON_RETURN_NOT_OK(
        CatalogUtils::CheckManagedTableType(identifier, schema, "FileStoreTable::Create"));
    PAIMON_RETURN_NOT_OK(BranchManager::CheckValidBranch(branch));
    std::map<std::string, std::string> options = schema->Options();
    const std::string normalized_branch = BranchManager::NormalizeBranch(branch);
    if (BranchManager::IsMainBranch(normalized_branch)) {
        options.erase(Options::BRANCH);
    } else {
        options[Options::BRANCH] = normalized_branch;
    }
    std::shared_ptr<TableSchema> effective_schema = schema;
    if (options != schema->Options()) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> normalized_schema,
                               schema->Copy(options));
        effective_schema = std::shared_ptr<TableSchema>(std::move(normalized_schema));
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(options, specified_file_system, fs_scheme_map));
    std::unique_ptr<FileStore> store =
        std::make_unique<FileStore>(root_path, std::move(core_options));
    return Create(std::move(store), effective_schema, identifier, uuid, fs_scheme_map);
}

Result<std::shared_ptr<FileStoreTable>> FileStoreTable::Create(
    std::unique_ptr<FileStore> store, const std::shared_ptr<TableSchema>& schema,
    const Identifier& identifier, const std::string& uuid,
    const std::map<std::string, std::string>& fs_scheme_map) {
    if (!store || !schema) {
        return Status::Invalid("managed table requires a store and schema");
    }
    PAIMON_RETURN_NOT_OK(
        CatalogUtils::CheckManagedTableType(identifier, schema, "FileStoreTable::Create"));
    PAIMON_RETURN_NOT_OK(BranchManager::CheckValidBranch(store->CurrentBranch()));
    std::shared_ptr<FileStoreTable> table;
    if (!schema->PrimaryKeys().empty()) {
        table =
            std::make_shared<PrimaryKeyFileStoreTable>(std::move(store), schema, identifier, uuid);
    } else {
        table =
            std::make_shared<AppendOnlyFileStoreTable>(std::move(store), schema, identifier, uuid);
    }
    table->fs_scheme_map_ = fs_scheme_map;
    return table;
}

Result<CoreOptions> FileStoreTable::ResolveOptions(
    const std::map<std::string, std::string>& overrides) const {
    if (overrides.empty()) {
        return store_->GetOptions();
    }
    auto options = store_->GetOptions().ToMap();
    for (const auto& [key, value] : overrides) {
        options[key] = value;
    }
    auto branch = overrides.find(Options::BRANCH);
    if (branch != overrides.end() &&
        BranchManager::NormalizeBranch(branch->second) != CurrentBranch()) {
        return Status::Invalid("context branch contradicts the loaded table");
    }
    PAIMON_ASSIGN_OR_RAISE(TableType type, TableTypeDefine::FromOptions(options));
    PAIMON_ASSIGN_OR_RAISE(TableType bound_type,
                           TableTypeDefine::FromOptions(store_->GetOptions().ToMap()));
    if (type != bound_type) {
        return Status::Invalid("context type contradicts the loaded table");
    }
    return CoreOptions::FromMap(options, file_system_, fs_scheme_map_);
}

Status FileStoreTable::RestoreRealtimeCommittedProgress(
    const std::shared_ptr<RealtimeContext>& realtime_context,
    const std::shared_ptr<SnapshotManager>& snapshot_manager, const CoreOptions& options) {
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

Status FileStoreTable::CheckContextNamesThisTable(
    const std::string& kind, const std::string& context_root_path,
    const std::optional<Identifier>& context_identifier,
    const std::optional<std::string>& context_table_id) const {
    if (!root_path_.empty()) {
        PAIMON_ASSIGN_OR_RAISE(std::string context_path,
                               PathUtil::NormalizePath(context_root_path));
        PAIMON_ASSIGN_OR_RAISE(std::string table_path, PathUtil::NormalizePath(root_path_));
        if (context_path != table_path) {
            return Status::Invalid(fmt::format(
                "the {} context names root path '{}' but this table serves '{}': the store below "
                "reads and writes this table's files, so a context naming another path would "
                "leave this table's metadata there and publish it as the other's",
                kind, context_root_path, root_path_));
        }
    }
    if (context_identifier) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> context_branch,
                               context_identifier.value().GetBranchName());
        if (context_branch) {
            PAIMON_ASSIGN_OR_RAISE(context_branch,
                                   context_identifier.value().GetBranchNameOrDefault());
        }
        if (context_branch && context_branch.value() != CurrentBranch()) {
            return Status::Invalid(fmt::format(
                "the {} context names branch '{}' of '{}' but this table serves branch '{}', "
                "which is the one a {} of this table writes and publishes",
                kind, context_branch.value(), context_identifier.value().ToString(),
                CurrentBranch(), kind));
        }
        PAIMON_ASSIGN_OR_RAISE(
            Identifier context_table,
            CatalogUtils::BranchIdentifier(context_identifier.value(), CurrentBranch()));
        PAIMON_ASSIGN_OR_RAISE(Identifier served_table,
                               CatalogUtils::BranchIdentifier(identifier_, CurrentBranch()));
        const bool names_this_table =
            (served_table.GetDatabaseName() == Identifier::kUnknownDatabase ||
             context_table.GetDatabaseName() == served_table.GetDatabaseName()) &&
            context_table.GetObjectName() == served_table.GetObjectName();
        if (!names_this_table) {
            return Status::Invalid(fmt::format(
                "the {} context names table '{}' but this table is '{}': the store below serves "
                "this table's files, so a context naming another table would write this table's "
                "metadata and publish it as the other's",
                kind, context_identifier.value().ToString(), identifier_.ToString()));
        }
    }
    if (context_table_id && context_table_id != CatalogUuid()) {
        return Status::Invalid(fmt::format(
            "the {} context carries table id '{}' but this table's is '{}': the id tells a "
            "recreated table from the one the changes were prepared for, so one this table does "
            "not carry would publish this table's metadata as the recreation's",
            kind, context_table_id.value(), CatalogUuid().value_or("none")));
    }
    return Status::OK();
}

std::unique_ptr<FileStoreScan> FileStoreTable::WithTablePath(std::unique_ptr<FileStoreScan>&& scan,
                                                             const std::string& root_path) {
    scan->WithTablePath(root_path);
    return std::move(scan);
}

Result<std::unique_ptr<FileStoreWrite>> FileStoreTable::NewWrite(
    std::unique_ptr<WriteContext> ctx) const {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "write"));
    PAIMON_RETURN_NOT_OK(CheckContextNamesThisTable("write", ctx->GetRootPath(),
                                                    ctx->GetIdentifier(),
                                                    /*context_table_id=*/std::nullopt));
    if (AsVersionManaged(ctx->GetCatalog()) != nullptr) {
        PAIMON_RETURN_NOT_OK(BranchManager::CheckCatalogAddressableBranch(CurrentBranch()));
    }
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx->GetCatalog()));
    std::shared_ptr<FileSystem> specific_fs = ctx->GetSpecificFileSystem();
    if (specific_fs == nullptr && ctx->GetFileSystemSchemeToIdentifierMap().empty() &&
        ctx->GetCatalog() != nullptr && !ctx->GetIdentifier()) {
        return Status::Invalid("a catalog write requires a table identifier");
    }
    std::optional<Identifier> catalog_identifier;
    if (ctx->GetIdentifier()) {
        PAIMON_ASSIGN_OR_RAISE(Identifier identifier, CatalogUtils::BranchIdentifier(
                                                          *ctx->GetIdentifier(), CurrentBranch()));
        catalog_identifier.emplace(std::move(identifier));
    }
    if (ctx->GetRealtimeContext()) {
        PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateBranch(CurrentBranch()));
    }
    std::shared_ptr<TableSchema> schema = std::static_pointer_cast<TableSchema>(schema_);
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, ResolveOptions(ctx->GetOptions()));
    FileStore file_store(root_path_, options);
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*schema, options));
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateVectorStore(*schema, options));
    if (options.GetVectorFileFormat() && !ctx->GetWriteSchema().empty()) {
        return Status::NotImplemented(
            "Dedicated VECTOR storage does not yet support custom write schemas or partial writes");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> partition_schema,
                           FieldMapping::GetPartitionSchema(arrow_schema, schema->PartitionKeys()));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> file_store_path_factory,
        file_store.NewPathFactory(arrow_schema, schema->PartitionKeys(),
                                  options.GetWriteFileFormat(/*level=*/0)->Identifier(),
                                  ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SnapshotManager> snapshot_manager,
                           file_store.NewSnapshotManager(ctx->GetCatalog(), catalog_identifier));
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
    return CreateWrite(*ctx, arrow_schema, partition_schema, options, file_store_path_factory,
                       snapshot_manager, io_manager, ignore_previous_files);
}

Result<std::unique_ptr<TableScan>> FileStoreTable::NewScan(
    std::unique_ptr<ScanContext> context) const {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "scan"));
    if (context->GetRealtimeContext()) {
        PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateBranch(CurrentBranch()));
    }
    std::shared_ptr<TableSchema> table_schema = std::static_pointer_cast<TableSchema>(schema_);
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, ResolveOptions(context->GetOptions()));
    PAIMON_ASSIGN_OR_RAISE(
        bool read_optimized,
        OptionsUtils::GetValueFromMap<bool>(core_options.ToMap(), kReadOptimizedScanOption, false));
    core_options.WithCache(context->GetCache());
    FileStore file_store(root_path_, core_options);

    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*table_schema, core_options));

    PAIMON_RETURN_NOT_OK(
        ValidateRealtimeScan(*table_schema, core_options, *context, read_optimized));
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

    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    if (context->GetScanFilters() && context->GetScanFilters()->GetPredicate()) {
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
            *arrow_schema, context->GetScanFilters()->GetPredicate(),
            /*validate_field_idx=*/false));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                           file_store.NewPathFactory(arrow_schema, table_schema->PartitionKeys(),
                                                     core_options.GetFileFormat()->Identifier(),
                                                     context->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ManifestList> manifest_list,
                           file_store.NewManifestList(path_factory, context->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        file_store.NewManifestFile(path_factory, partition_schema, context->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStoreScan> file_store_scan,
        NewFileStoreScan(manifest_list, manifest_file, arrow_schema, core_options,
                         context->GetExecutor(), context->GetMemoryPool(), context.get()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SplitGenerator> split_generator,
                           NewSplitGenerator(core_options));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexFileHandler> index_file_handler,
        file_store.NewIndexFileHandler(ResolveBucketMode(core_options.GetBucket(), table_schema),
                                       path_factory, context->GetMemoryPool()));
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
            root_path_, snapshot_reader, std::move(batch_scan), table_schema,
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

Result<std::unique_ptr<TableRead>> FileStoreTable::CreateTableRead(
    const std::shared_ptr<InternalReadContext>& internal_context,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<Executor>& executor) const {
    const auto& core_options = internal_context->GetCoreOptions();
    FileStore file_store(root_path_, core_options);
    const auto& table_schema = internal_context->GetTableSchema();
    if (internal_context->GetRealtimeContext() && !core_options.RealtimeEnabled()) {
        return Status::Invalid("real-time read requires realtime.enabled=true");
    }
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*table_schema, core_options));
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        file_store.NewPathFactory(arrow_schema, table_schema->PartitionKeys(),
                                  core_options.GetFileFormat()->Identifier(), memory_pool));
    if (internal_context->GetPrimaryKeys().empty()) {
        return std::make_unique<AppendOnlyTableRead>(path_factory, internal_context, memory_pool,
                                                     executor);
    }
    return KeyValueTableRead::Create(path_factory, internal_context, memory_pool, executor);
}

Result<std::unique_ptr<TableRead>> FileStoreTable::NewRead(
    std::unique_ptr<ReadContext> context) const {
    std::shared_ptr<ReadContext> read_context = std::move(context);
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(read_context, "read"));
    if (read_context->GetRealtimeContext()) {
        PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateBranch(CurrentBranch()));
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, ResolveOptions(read_context->GetOptions()));
    auto fs_scheme_map = fs_scheme_map_;
    for (const auto& [scheme, identifier] : read_context->GetFileSystemSchemeToIdentifierMap()) {
        fs_scheme_map[scheme] = identifier;
    }
    std::shared_ptr<TableSchema> loaded_schema = std::static_pointer_cast<TableSchema>(schema_);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> internal_context,
        InternalReadContext::CreateForTable(read_context, loaded_schema, std::move(options),
                                            root_path_, fs_scheme_map));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           CreateTableRead(internal_context, read_context->GetMemoryPool(),
                                           read_context->GetExecutor()));

    std::optional<std::string> scan_fallback_branch =
        internal_context->GetCoreOptions().GetScanFallbackBranch();
    if (!scan_fallback_branch ||
        StringUtils::IsNullOrWhitespaceOnly(scan_fallback_branch.value())) {
        return std::move(table_read);
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> fallback_context,
                           InternalReadContext::CreateForFallbackBranch(
                               read_context, /*branch=*/scan_fallback_branch.value(), file_system_,
                               root_path_, fs_scheme_map));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> fallback_table_read,
                           CreateTableRead(fallback_context, read_context->GetMemoryPool(),
                                           read_context->GetExecutor()));
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool = internal_context->GetArrowMemoryPool();
    return std::make_unique<FallbackTableRead>(std::move(table_read),
                                               std::move(fallback_table_read), arrow_pool);
}

Result<std::unique_ptr<FileStoreCommit>> FileStoreTable::NewCommit(
    std::unique_ptr<CommitContext> ctx) const {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "commit"));
    PAIMON_RETURN_NOT_OK(CheckContextNamesThisTable("commit", ctx->GetRootPath(),
                                                    ctx->GetIdentifier(), ctx->GetTableId()));
    if (ctx->GetCatalog() != nullptr || ctx->UseRESTCatalogCommit()) {
        PAIMON_RETURN_NOT_OK(BranchManager::CheckCatalogAddressableBranch(CurrentBranch()));
    }
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx->GetCatalog()));
    std::shared_ptr<FileSystem> specific_fs = ctx->GetSpecificFileSystem();
    if (specific_fs == nullptr && ctx->GetCatalog() != nullptr && !ctx->GetIdentifier()) {
        return Status::Invalid("a catalog commit requires a table identifier");
    }
    std::optional<Identifier> catalog_identifier;
    if (ctx->GetIdentifier()) {
        PAIMON_ASSIGN_OR_RAISE(Identifier identifier, CatalogUtils::BranchIdentifier(
                                                          *ctx->GetIdentifier(), CurrentBranch()));
        catalog_identifier.emplace(std::move(identifier));
    }
    FileStoreCommitImpl::SchemaIdLoader schema_id_loader;
    if (ctx->GetCatalog() != nullptr) {
        if (!catalog_identifier) {
            return Status::Invalid("a catalog commit requires a table identifier");
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
    std::shared_ptr<TableSchema> schema = std::static_pointer_cast<TableSchema>(schema_);
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, ResolveOptions(ctx->GetOptions()));
    FileStore file_store(root_path_, options);
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*schema, options));
    assert(options.GetFileSystem());
    assert(options.GetFileFormat());
    PAIMON_RETURN_NOT_OK(FileStoreCommitImpl::ValidateCommitOptions(options));
    PAIMON_ASSIGN_OR_RAISE(bool is_object_store, FileSystem::IsObjectStore(root_path_));
    if (is_object_store && !ctx->UseRESTCatalogCommit() &&
        AsVersionManaged(ctx->GetCatalog()) == nullptr &&
        options.ToMap().count(kObjectStoreCommitInteTestOption) == 0) {
        return Status::NotImplemented(
            "commit operation does not support object store file system for now");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
                           file_store.NewPartitionComputer(arrow_schema, schema->PartitionKeys(),
                                                           ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        file_store.NewPathFactory(arrow_schema, schema->PartitionKeys(),
                                  options.GetFileFormat()->Identifier(), ctx->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SnapshotManager> snapshot_manager,
                           file_store.NewSnapshotManager(ctx->GetCatalog(), catalog_identifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ManifestList> manifest_list,
                           file_store.NewManifestList(path_factory, ctx->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> partition_schema,
                           FieldMapping::GetPartitionSchema(arrow_schema, schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        file_store.NewManifestFile(path_factory, partition_schema, ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<IndexManifestFile> index_manifest_file,
        file_store.NewIndexManifestFile(ResolveBucketMode(options.GetBucket(), schema),
                                        path_factory, ctx->GetMemoryPool()));

    auto expire_snapshots = std::make_shared<ExpireSnapshots>(
        snapshot_manager, path_factory, manifest_list, manifest_file, index_manifest_file,
        options.GetFileSystem(), options.GetExpireConfig(), options.RealtimeEnabled(),
        ctx->GetExecutor());

    CommitScanner::ScanSupplier scan_supplier =
        NewCommitScanSupplier(snapshot_manager, manifest_list, manifest_file, arrow_schema, options,
                              ctx->GetExecutor(), ctx->GetMemoryPool());

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SnapshotCommit> snapshot_commit,
                           NewSnapshotCommit(*ctx, catalog_identifier, options, snapshot_manager));

    return std::make_unique<FileStoreCommitImpl>(
        ctx->GetMemoryPool(), ctx->GetExecutor(), arrow_schema, root_path_, ctx->GetCommitUser(),
        options, path_factory, std::move(partition_computer), snapshot_manager, snapshot_commit,
        ctx->IgnoreEmptyCommit(), ctx->AppendCommitCheckConflict(), schema,
        std::move(schema_id_loader), manifest_file, manifest_list, index_manifest_file,
        expire_snapshots, GetSchemaManager(), std::move(scan_supplier));
}

}  // namespace paimon
