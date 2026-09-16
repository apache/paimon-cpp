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

#include "paimon/file_store_commit.h"

#include <cassert>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "paimon/catalog/catalog.h"
#include "paimon/commit_context.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/core/catalog/catalog_snapshot_commit.h"
#include "paimon/core/catalog/renaming_snapshot_commit.h"
#include "paimon/core/catalog/snapshot_commit.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/operation/expire_snapshots.h"
#include "paimon/core/operation/file_store_commit_impl.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/key_value_file_store_scan.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/format/format_table_file_store_commit.h"
#include "paimon/core/table/format/format_table_loader.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/schema/schema.h"
#include "paimon/table/format/format_table.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

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
    const CommitContext& ctx, const std::shared_ptr<FileSystem>& fs,
    const std::shared_ptr<SnapshotManager>& snapshot_manager) {
    if (IsCatalogCommit(ctx)) {
        if (!ctx.GetIdentifier()) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        // Reuse the caller's UUID so table recreation cannot redirect this commit.
        return std::shared_ptr<SnapshotCommit>(std::make_shared<CatalogSnapshotCommit>(
            ctx.GetCatalog(), ctx.GetIdentifier().value(), ctx.GetTableId()));
    }
    if (ctx.UseRESTCatalogCommit()) {
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
        std::make_shared<RenamingSnapshotCommit>(fs, snapshot_manager));
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
        specific_fs = ctx->GetCatalog()->GetFileSystem();
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx->GetOptions(), specific_fs));
    const std::string& root_path = ctx->GetRootPath();
    // Catalog-managed schemas may be absent from the table directory.
    std::optional<std::string> catalog_table_schema;
    FileStoreCommitImpl::SchemaIdLoader schema_id_loader;
    if (ctx->GetCatalog() != nullptr) {
        if (!ctx->GetIdentifier()) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> loaded_schema,
                               ctx->GetCatalog()->LoadTableSchema(ctx->GetIdentifier().value()));
        PAIMON_ASSIGN_OR_RAISE(std::string schema_json, loaded_schema->GetJsonSchema());
        catalog_table_schema = std::move(schema_json);
        std::shared_ptr<Catalog> catalog = ctx->GetCatalog();
        Identifier identifier = ctx->GetIdentifier().value();
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
    // below. The managed path here reads the main branch, so this reads the same one: the two
    // must not dispatch on different schemas.
    auto schema_manager = std::make_shared<SchemaManager>(tmp_options.GetFileSystem(), root_path);
    std::shared_ptr<TableSchema> latest_schema;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FormatTable> format_table,
        FormatTableLoader::TryLoad(tmp_options.GetFileSystem(), root_path,
                                   BranchManager::DEFAULT_MAIN_BRANCH, ctx->GetOptions(),
                                   catalog_table_schema, schema_manager.get(), &latest_schema));
    if (format_table != nullptr) {
        return NewFormatTableCommit(format_table, *ctx);
    }
    if (latest_schema == nullptr) {
        return Status::Invalid("not found latest schema");
    }
    const std::shared_ptr<TableSchema>& schema = latest_schema;
    auto opts = schema->Options();
    for (const auto& [key, value] : ctx->GetOptions()) {
        opts[key] = value;
    }
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(opts, specific_fs));
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

    auto snapshot_manager = std::make_shared<SnapshotManager>(options.GetFileSystem(), root_path);
    if (IsCatalogCommit(*ctx)) {
        std::shared_ptr<Catalog> catalog = ctx->GetCatalog();
        VersionManagedCatalog* versioned = AsVersionManaged(catalog);
        Identifier identifier = ctx->GetIdentifier().value();
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
        snapshot_manager, path_factory, manifest_list, manifest_file, options.GetFileSystem(),
        options.GetExpireConfig(), options.RealtimeEnabled(), ctx->GetExecutor());

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
                           NewSnapshotCommit(*ctx, options.GetFileSystem(), snapshot_manager));

    return std::make_unique<FileStoreCommitImpl>(
        ctx->GetMemoryPool(), ctx->GetExecutor(), arrow_schema, root_path, ctx->GetCommitUser(),
        options, path_factory, std::move(partition_computer), snapshot_manager, snapshot_commit,
        ctx->IgnoreEmptyCommit(), ctx->AppendCommitCheckConflict(), schema,
        std::move(schema_id_loader), manifest_file, manifest_list, index_manifest_file,
        expire_snapshots, schema_manager, std::move(scan_supplier));
}

}  // namespace paimon
