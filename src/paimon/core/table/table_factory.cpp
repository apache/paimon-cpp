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

#include "paimon/core/table/table_factory.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/table.h"
#include "paimon/commit_context.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/core/realtime/realtime_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/format/format_table_loader.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/context_utils.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/write_context.h"

namespace paimon {
namespace {
Result<std::shared_ptr<Table>> ManagedTable(
    const std::string& path, const std::string& branch, const std::shared_ptr<TableSchema>& schema,
    CoreOptions options, const std::optional<Identifier>& identifier,
    const std::optional<std::string>& table_id,
    const std::map<std::string, std::string>& fs_scheme_map) {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(std::make_unique<FileStore>(path, std::move(options)), schema,
                               identifier.value_or(CatalogUtils::IdentifierFromPath(path, branch)),
                               table_id.value_or(""), fs_scheme_map));
    return std::shared_ptr<Table>(std::move(table));
}
}  // namespace

Result<std::shared_ptr<Table>> TableFactory::Create(const WriteContext* ctx) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "write"));

    // A table the caller already loaded says what it is, so nothing is read to find out.
    if (ctx->GetFormatTable() != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> given_table,
                               FormatTable::Copy(ctx->GetFormatTable(), ctx->GetOptions()));
        return std::shared_ptr<Table>(given_table);
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
    if (ctx->GetRealtimeContext()) {
        PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateBranch(branch));
    }
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
        return std::shared_ptr<Table>(format_table);
    }
    // The schema the dispatch above already read through `schema_manager`, rather than a second
    // read of the same file.
    if (latest_schema == nullptr) {
        return Status::Invalid(fmt::format("not found latest schema in branch {}", branch));
    }
    const std::shared_ptr<TableSchema>& schema = latest_schema;
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
    return ManagedTable(ctx->GetRootPath(), branch, schema, std::move(options),
                        ctx->GetIdentifier(), std::nullopt,
                        ctx->GetFileSystemSchemeToIdentifierMap());
}

Result<std::shared_ptr<Table>> TableFactory::Create(const CommitContext* ctx) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "commit"));

    // A table the caller already loaded says what it is, so nothing is read to find out.
    if (ctx->GetFormatTable() != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> given_table,
                               FormatTable::Copy(ctx->GetFormatTable(), ctx->GetOptions()));
        return std::shared_ptr<Table>(given_table);
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
        return std::shared_ptr<Table>(format_table);
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
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(opts, specific_fs));
    return ManagedTable(ctx->GetRootPath(), branch, schema, std::move(options),
                        ctx->GetIdentifier(), ctx->GetTableId(), {});
}

Result<std::shared_ptr<Table>> TableFactory::Create(const ScanContext* context) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "scan"));

    // A table the caller already loaded says what it is, so nothing is read to find out.
    if (context->GetFormatTable() != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> format_table,
                               FormatTable::Copy(context->GetFormatTable(), context->GetOptions()));
        return std::shared_ptr<Table>(format_table);
    }
    // load schema
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions tmp_options,
        CoreOptions::FromMap(context->GetOptions(), context->GetSpecificFileSystem(), {}));
    if (context->GetRealtimeContext()) {
        PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateBranch(tmp_options.GetBranch()));
    }
    std::map<std::string, std::string> read_options;
    PAIMON_ASSIGN_OR_RAISE(
        read_options,
        CatalogUtils::ResolveQueryAuthReadOptions(
            context->GetCatalog(), context->GetIdentifier(), context->GetOptions(),
            BranchManager::NormalizeBranch(tmp_options.GetBranch()), tmp_options.GetFileSystem(),
            context->GetPath(), context->GetSpecificTableSchema().has_value(), "scan"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(context->GetPath()));
    if (system_table_path) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SystemTable> system_table,
                               SystemTableLoader::LoadFromPath(tmp_options.GetFileSystem(),
                                                               context->GetPath(), read_options));
        return std::shared_ptr<Table>(system_table);
    }
    // A format table is planned by listing directories, so it never reaches the manifest path
    // below. One `TableScan` interface serves both, as Java Paimon serves both through one
    // `ReadBuilder`.
    std::shared_ptr<TableSchema> latest_schema;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FormatTable> format_table,
        FormatTableLoader::TryLoad(tmp_options.GetFileSystem(), context->GetPath(),
                                   BranchManager::NormalizeBranch(tmp_options.GetBranch()),
                                   read_options, context->GetSpecificTableSchema(),
                                   /*schema_manager=*/nullptr, &latest_schema));
    if (format_table != nullptr) {
        return std::shared_ptr<Table>(format_table);
    }
    if (!latest_schema) {
        return Status::Invalid("not found latest schema");
    }
    auto options_map = latest_schema->Options();
    for (const auto& [key, value] : read_options) {
        options_map[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::string branch,
        BranchManager::ResolveBranch(std::nullopt, options_map, std::nullopt, "scan"));
    if (options_map.count(Options::BRANCH) != 0) {
        options_map[Options::BRANCH] = branch;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                           CoreOptions::FromMap(options_map, context->GetSpecificFileSystem(), {}));
    return ManagedTable(context->GetPath(), branch, latest_schema, std::move(options),
                        context->GetIdentifier(), std::nullopt, {});
}

Result<std::shared_ptr<Table>> TableFactory::Create(const ReadContext* context) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "read"));

    // A table the caller already loaded says what it is, so nothing is read to find out.
    if (context->GetFormatTable() != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> format_table,
                               FormatTable::Copy(context->GetFormatTable(), context->GetOptions()));
        return std::shared_ptr<Table>(format_table);
    }
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions tmp_core_options,
        CoreOptions::FromMap(context->GetOptions(), context->GetSpecificFileSystem(),
                             context->GetFileSystemSchemeToIdentifierMap()));
    std::map<std::string, std::string> read_options;
    PAIMON_ASSIGN_OR_RAISE(
        read_options,
        CatalogUtils::ResolveQueryAuthReadOptions(
            context->GetCatalog(), context->GetIdentifier(), context->GetOptions(),
            context->GetBranch(), tmp_core_options.GetFileSystem(), context->GetPath(),
            context->GetSpecificTableSchema().has_value(), "read"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(context->GetPath()));
    if (system_table_path) {
        if (context->EnablePreciseFileIndexEvaluate()) {
            return Status::NotImplemented(
                "Precise file-index evaluation is not supported for system tables");
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SystemTable> system_table,
                               SystemTableLoader::LoadFromPath(
                                   tmp_core_options.GetFileSystem(), context->GetPath(),
                                   read_options, context->GetFileSystemSchemeToIdentifierMap()));
        return std::shared_ptr<Table>(system_table);
    }

    // A format table has no manifest and a file another engine wrote carries no field ids, so
    // which files there are and how a row is put back together is its own. How a file is opened
    // is not: both paths go through `DataFileReaderFactory`.
    std::shared_ptr<TableSchema> latest_schema;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FormatTable> format_table,
        FormatTableLoader::TryLoad(tmp_core_options.GetFileSystem(), context->GetPath(),
                                   context->GetBranch(), read_options,
                                   context->GetSpecificTableSchema(),
                                   /*schema_manager=*/nullptr, &latest_schema));
    if (format_table != nullptr) {
        return std::shared_ptr<Table>(format_table);
    }

    if (!latest_schema) {
        return Status::Invalid("not found latest schema");
    }
    auto options_map = latest_schema->Options();
    for (const auto& [key, value] : read_options) {
        options_map[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::string branch,
        BranchManager::ResolveBranch(std::nullopt, options_map, std::nullopt, "read"));
    if (options_map.count(Options::BRANCH) != 0) {
        options_map[Options::BRANCH] = branch;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                           CoreOptions::FromMap(options_map, context->GetSpecificFileSystem(),
                                                context->GetFileSystemSchemeToIdentifierMap()));
    return ManagedTable(context->GetPath(), branch, latest_schema, std::move(options),
                        context->GetIdentifier(), std::nullopt,
                        context->GetFileSystemSchemeToIdentifierMap());
}

Result<std::unique_ptr<TableScan>> TableScan::Create(std::unique_ptr<ScanContext> context) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "scan"));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, TableFactory::Create(context.get()));
    return table->NewScan(std::move(context));
}

Result<std::unique_ptr<TableRead>> TableRead::Create(std::unique_ptr<ReadContext> context) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "read"));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, TableFactory::Create(context.get()));
    return table->NewRead(std::move(context));
}

Result<std::unique_ptr<FileStoreWrite>> FileStoreWrite::Create(
    std::unique_ptr<WriteContext> context) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "write"));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, TableFactory::Create(context.get()));
    return table->NewWrite(std::move(context));
}

Result<std::unique_ptr<FileStoreCommit>> FileStoreCommit::Create(
    std::unique_ptr<CommitContext> context) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "commit"));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, TableFactory::Create(context.get()));
    return table->NewCommit(std::move(context));
}
}  // namespace paimon
