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

#include "paimon/core/table/file_store_table_factory.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/catalog/table.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/read_context.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/table/format/format_table.h"
#include "paimon/write_context.h"

namespace paimon {

namespace {

/// Recognises a format table behind `table_path`, or returns null when it is not one. The scan,
/// read, write and commit entry points below all dispatch on the same question, so one place
/// answers it and they cannot disagree. Null is also the answer when no schema was found under
/// the path at all: what to say about a table that is not there is the managed path's to decide.
///
/// The table type is read from the schema, never from `options`: `type` is structural, and one
/// call's options must not decide what kind of table this is.
Result<std::shared_ptr<FormatTable>> TryLoadFormatTable(
    const std::shared_ptr<FileSystem>& file_system, const std::string& table_path,
    const std::string& branch, const std::map<std::string, std::string>& options,
    const std::shared_ptr<TableSchema>& table_schema) {
    if (table_schema == nullptr) {
        return std::shared_ptr<FormatTable>();
    }
    // From the schema alone: see `FormatTable::Create()`.
    PAIMON_ASSIGN_OR_RAISE(bool is_format_table,
                           TableTypeDefine::IsFormatTable(table_schema->Options()));
    if (!is_format_table) {
        return std::shared_ptr<FormatTable>();
    }
    // A format table has no branches, so it is refused rather than served from the table path
    // under another branch's name. See `SchemaValidation::ValidateFormatTableSchema()`.
    if (BranchManager::NormalizeBranch(branch) != BranchManager::DEFAULT_MAIN_BRANCH) {
        return Status::Invalid(
            fmt::format("a format table has no branches, so {} cannot be read on branch '{}'",
                        table_path, branch));
    }
    Identifier identifier = CatalogUtils::IdentifierFromPath(table_path, branch);
    return FormatTable::Create(file_system, table_path, identifier,
                               checked_pointer_cast<DataSchema>(table_schema),
                               /*location_carries_paimon_metadata=*/true, options);
}

/// The options a context carries, merged into the table the load hands back: a table is the
/// product of the load, so the options speak there - the way `Copy` merges them - and `NewXxx` on
/// the product serves them without merging again. A caller holding a table already changes it
/// through `Copy` itself, and an option that contradicts the table that way is refused.
Result<std::shared_ptr<Table>> WithContextOptions(
    const std::shared_ptr<Table>& table, const std::shared_ptr<TableSchema>& schema,
    const std::map<std::string, std::string>& options) {
    if (table == nullptr || schema == nullptr || options.empty()) {
        return table;
    }
    const std::map<std::string, std::string>& schema_options = schema->Options();
    for (const auto& [key, value] : options) {
        auto schema_option = schema_options.find(key);
        // A key the schema does not carry, or carries differently: the merge has something to do.
        if (schema_option == schema_options.end() || schema_option->second != value) {
            return table->Copy(options);
        }
    }
    return table;
}

/// The tail every `Create` ends with: the schema one of them read says what the path names. A
/// format table is handed back as the `FormatTable` it is, everything else as the `FileStoreTable`
/// a managed table is, carrying the schema for its `NewXxx` to build from. A null schema is handed
/// over as it is - see `TryLoadFormatTable`.
///
/// @param file_system The file system the schema above was read through, which a format table
///        is served by - it has no schema options of its own to name another one.
/// @param explicit_file_system The file system the context supplied, which a managed table is
///        opened through when there is one. A null one leaves the table to resolve from its
///        schema's own access configuration rather than being pinned to the file system this load
///        happened to read the schema through.
Result<std::shared_ptr<Table>> TableOfLoadedSchema(
    const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<FileSystem>& explicit_file_system, const std::string& path,
    const std::string& branch, const std::map<std::string, std::string>& options,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<TableSchema>& latest_schema, const std::optional<Identifier>& identifier,
    const std::string& uuid = "", const std::map<std::string, std::string>& fs_scheme_map = {}) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> format_table,
                           TryLoadFormatTable(file_system, path, branch, options, latest_schema));
    if (format_table != nullptr) {
        return std::shared_ptr<Table>(std::move(format_table));
    }
    // The branch the schema was read on is folded into the object name.
    Identifier table_identifier =
        identifier.value_or(CatalogUtils::IdentifierFromPath(path, branch));
    // The manager was built over the file system the context's own options resolve, while the
    // store resolves the schema's: a schema carrying access configuration names a backend the
    // context's options never did, and a manager kept through that disagreement would read the
    // table's metadata through the wrong one. The schema is in hand already, so dropping the
    // manager costs its cache alone. A context that supplied a file system wins on both sides and
    // keeps it; a scheme map only the store's resolution takes part in is the same disagreement.
    std::shared_ptr<SchemaManager> manager = schema_manager;
    if (manager != nullptr && latest_schema != nullptr && explicit_file_system == nullptr &&
        (!fs_scheme_map.empty() ||
         !FileStore::FileSystemResolutionAgrees(latest_schema->Options(), options))) {
        manager = nullptr;
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreTable> table,
                           FileStoreTable::Create(explicit_file_system, path, latest_schema,
                                                  table_identifier, uuid, branch, manager,
                                                  /*snapshot_manager=*/nullptr, fs_scheme_map));
    return WithContextOptions(table, latest_schema, options);
}

/// The managed table a context names behind a path: the schema is read through a manager of the
/// context's branch - handed over instead when the caller already read it - and what the schema
/// says is there decides the table `TableOfLoadedSchema` hands back.
///
/// @param schema_json The schema of the table, when the caller already read it, or null when it
///        has to be read here. It is honoured on the main branch only; a branch publishes its own
///        under `branch/branch-<name>`, which is where the read below goes.
/// @param explicit_file_system The file system the context supplied, which the managed table the
///        schema describes is opened through - see `TableOfLoadedSchema`.
Result<std::shared_ptr<Table>> ManagedTableOf(
    const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<FileSystem>& explicit_file_system, const std::string& path,
    const std::string& branch, const std::map<std::string, std::string>& options,
    const std::optional<std::string>& schema_json, const std::optional<Identifier>& identifier,
    const std::string& uuid = "", const std::map<std::string, std::string>& fs_scheme_map = {}) {
    auto schema_manager = std::make_shared<SchemaManager>(file_system, path, branch);
    std::shared_ptr<TableSchema> latest_schema;
    if (branch == BranchManager::DEFAULT_MAIN_BRANCH && schema_json) {
        // Handing the schema over only saves reading it. A context always names a table path and
        // this library writes the metadata under it, so `schema` and `branch` stay metadata.
        PAIMON_ASSIGN_OR_RAISE(latest_schema, TableSchema::CreateFromJson(schema_json.value()));
    } else {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest,
                               schema_manager->Latest());
        if (latest) {
            latest_schema = *latest;
        }
    }
    return TableOfLoadedSchema(file_system, explicit_file_system, path, branch, options,
                               schema_manager, latest_schema, identifier, uuid, fs_scheme_map);
}

/// The schema a catalog may hold for the table, read from it on the main branch only and null
/// otherwise: a branch publishes its own under `branch/branch-<name>`, which is where
/// `ManagedTableOf` reads instead, so a catalog answering for no branch schema does not fail the
/// `kind` operation to one. `kind` also names the operator the messages speak of.
Result<std::optional<std::string>> CatalogMainBranchSchemaJson(
    const std::shared_ptr<Catalog>& catalog, const std::optional<Identifier>& catalog_identifier,
    const std::string& branch, const char* kind) {
    if (catalog == nullptr) {
        return std::optional<std::string>();
    }
    if (!catalog_identifier) {
        return Status::Invalid(fmt::format("a catalog {} requires a table identifier", kind));
    }
    if (BranchManager::IsMainBranch(branch)) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> schema,
                               catalog->LoadTableSchema(catalog_identifier.value()));
        PAIMON_ASSIGN_OR_RAISE(std::string schema_json, schema->GetJsonSchema());
        return std::optional<std::string>(std::move(schema_json));
    }
    return std::optional<std::string>();
}

}  // namespace

/// The table a scan context names: the format table the builder was handed, a system table behind
/// a `T$system` path, or the table the schema under the context's path says is there.
Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const ScanContext& ctx) {
    if (ctx.GetFormatTable() != nullptr) {
        return std::shared_ptr<Table>(ctx.GetFormatTable());
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx.GetOptions(), ctx.GetSpecificFileSystem(), {}));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(ctx.GetPath()));
    if (system_table_path) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SystemTable> system_table,
                               SystemTableLoader::LoadFromPath(tmp_options.GetFileSystem(),
                                                               ctx.GetPath(), ctx.GetOptions()));
        return std::shared_ptr<Table>(std::move(system_table));
    }
    // One `TableScan` interface serves a format table too, the way Java Paimon serves both
    // through one `ReadBuilder`; a format table is planned by listing directories instead.
    const std::string branch = BranchManager::NormalizeBranch(tmp_options.GetBranch());
    return ManagedTableOf(tmp_options.GetFileSystem(), ctx.GetSpecificFileSystem(), ctx.GetPath(),
                          branch, ctx.GetOptions(), ctx.GetSpecificTableSchema(),
                          ctx.GetIdentifier());
}

/// The table a read context names: the format table the builder was handed, a system table behind
/// a `T$system` path, or the table the schema under the context's path says is there.
Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const ReadContext& ctx) {
    if (ctx.GetFormatTable() != nullptr) {
        return std::shared_ptr<Table>(ctx.GetFormatTable());
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_core_options,
                           CoreOptions::FromMap(ctx.GetOptions(), ctx.GetSpecificFileSystem(),
                                                ctx.GetFileSystemSchemeToIdentifierMap()));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(ctx.GetPath()));
    if (system_table_path) {
        if (PredicateUtils::ContainsFullTextMatch(ctx.GetPredicate())) {
            return Status::NotImplemented(
                "FullTextMatch predicate execution is not implemented yet");
        }
        if (ctx.HasVectorOrFullTextSearch()) {
            return Status::NotImplemented(
                "VectorSearch or FullTextSearch is not supported for system tables");
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SystemTable> system_table,
                               SystemTableLoader::LoadFromPath(tmp_core_options.GetFileSystem(),
                                                               ctx.GetPath(), ctx.GetOptions()));
        return std::shared_ptr<Table>(std::move(system_table));
    }

    // A format table has no manifest and a file another engine wrote carries no field ids, so how
    // a row is put back together is its own; how a file is opened is not - both paths go through
    // `DataFileReaderFactory`.
    const std::string branch = ctx.GetBranch();
    // The scheme map the schema was read through, so the table's data resolves custom schemes the
    // same way its metadata does.
    return ManagedTableOf(tmp_core_options.GetFileSystem(), ctx.GetSpecificFileSystem(),
                          ctx.GetPath(), branch, ctx.GetOptions(), ctx.GetSpecificTableSchema(),
                          ctx.GetIdentifier(), /*uuid=*/"",
                          ctx.GetFileSystemSchemeToIdentifierMap());
}

/// The table a write context names: the format table the builder was handed, or the table the
/// schema under the context's root path says is there.
Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const WriteContext& ctx) {
    if (ctx.GetFormatTable() != nullptr) {
        return std::shared_ptr<Table>(ctx.GetFormatTable());
    }
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx.GetCatalog()));
    std::shared_ptr<FileSystem> specific_fs = ctx.GetSpecificFileSystem();
    if (specific_fs == nullptr && ctx.GetFileSystemSchemeToIdentifierMap().empty() &&
        ctx.GetCatalog() != nullptr) {
        if (!ctx.GetIdentifier()) {
            return Status::Invalid("a catalog write requires a table identifier");
        }
        // A catalog issuing per-table temporary credentials only hands them out through
        // GetTableFileSystem, so the write uses this table's.
        PAIMON_ASSIGN_OR_RAISE(specific_fs,
                               ctx.GetCatalog()->GetTableFileSystem(ctx.GetIdentifier().value()));
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx.GetOptions(), specific_fs,
                                                ctx.GetFileSystemSchemeToIdentifierMap()));
    std::string branch = ctx.GetBranch();
    PAIMON_ASSIGN_OR_RAISE(std::optional<Identifier> catalog_identifier,
                           CatalogUtils::BranchIdentifier(ctx.GetIdentifier(), branch));
    // Only the main branch may have a current schema the catalog alone holds: a branch publishes
    // its own under `branch/branch-<name>`, which is where the load below reads it instead.
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<std::string> catalog_table_schema,
        CatalogMainBranchSchemaJson(ctx.GetCatalog(), catalog_identifier, branch, "write"));
    return ManagedTableOf(tmp_options.GetFileSystem(), specific_fs, ctx.GetRootPath(), branch,
                          ctx.GetOptions(), catalog_table_schema, ctx.GetIdentifier(), /*uuid=*/"",
                          ctx.GetFileSystemSchemeToIdentifierMap());
}

/// The table a commit context names: the format table the builder was handed, or the table the
/// schema under the context's root path says is there.
Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const CommitContext& ctx) {
    if (ctx.GetFormatTable() != nullptr) {
        return std::shared_ptr<Table>(ctx.GetFormatTable());
    }
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx.GetCatalog()));
    // Use this table's catalog credentials unless the caller supplied a file system: a catalog
    // issuing per-table temporary ones only hands them out through GetTableFileSystem.
    std::shared_ptr<FileSystem> specific_fs = ctx.GetSpecificFileSystem();
    if (specific_fs == nullptr && ctx.GetCatalog() != nullptr) {
        if (!ctx.GetIdentifier()) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        PAIMON_ASSIGN_OR_RAISE(specific_fs,
                               ctx.GetCatalog()->GetTableFileSystem(ctx.GetIdentifier().value()));
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx.GetOptions(), specific_fs));
    const std::string& root_path = ctx.GetRootPath();
    // The branch the context was built for, already validated by `CommitContextBuilder`.
    PAIMON_ASSIGN_OR_RAISE(std::string branch, BranchManager::ResolveBranch(
                                                   ctx.GetIdentifier(), ctx.GetOptions(),
                                                   /*explicit_branch=*/std::nullopt, "commit"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<Identifier> catalog_identifier,
                           CatalogUtils::BranchIdentifier(ctx.GetIdentifier(), branch));
    // Only the main branch may have a current schema the catalog alone holds: a branch publishes
    // its own under `branch/branch-<name>`, which is where the load below reads it instead.
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<std::string> catalog_table_schema,
        CatalogMainBranchSchemaJson(ctx.GetCatalog(), catalog_identifier, branch, "commit"));
    // The managed path below reads the commit's branch, so this reads the same one: the two must
    // not dispatch on different schemas.
    return ManagedTableOf(tmp_options.GetFileSystem(), specific_fs, root_path, branch,
                          ctx.GetOptions(), catalog_table_schema, ctx.GetIdentifier(),
                          ctx.GetTableId().value_or(""));
}

}  // namespace paimon
