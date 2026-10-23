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
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/sensitive_config_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/catalog/catalog_snapshot_commit.h"
#include "paimon/core/catalog/catalog_utils.h"
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
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/append_only_file_store_table.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/primary_key_file_store_table.h"
#include "paimon/core/table/source/data_evolution_batch_scan.h"
#include "paimon/core/table/source/data_table_batch_scan.h"
#include "paimon/core/table/source/data_table_stream_scan.h"
#include "paimon/core/table/source/fallback_table_read.h"
#include "paimon/core/table/source/primary_key_index_batch_scan.h"
#include "paimon/core/table/source/read_optimized_scan_options.h"
#include "paimon/core/table/source/realtime_table_scan.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/primary_key_table_utils.h"
#include "paimon/core/utils/snapshot_manager.h"
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

class Executor;
class MemoryPool;

namespace {

/// Recognises a format table behind `table_path`, or returns null when it is not one.
///
/// The scan, read, write and commit entry points below all take a path and have to answer the
/// same question before they can dispatch, so one place answers it and they cannot disagree.
/// Null is also the answer when no schema was found under the path at all: what to say about a
/// table that is not there is the managed path's to decide, and it says it in more detail.
///
/// The table type is read from the schema, never from `options`: `type` is structural, and one
/// call's options must not decide what kind of table this is. `branch` names the branch the
/// schema was read from: anything but the main branch is refused once the schema turns out to be
/// a format table's, which has no branches to read.
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
    // A context names a path, never a catalog identifier, so the table is known by the directory
    // it sits in.
    Identifier identifier(Identifier::kUnknownDatabase, PathUtil::GetName(table_path));
    return FormatTable::Create(file_system, table_path, identifier,
                               checked_pointer_cast<DataSchema>(table_schema),
                               /*location_carries_paimon_metadata=*/true, options);
}

/// Whether an option of a copy names the file system its store resolves: the backend
/// `file-system` picks, or the access configuration a `fs.` key carries. A copy whose dynamic
/// options change one of these resolves to another file system, so the managers of the table it
/// was copied from - built over that table's resolution - are not its to inherit: they would
/// keep reading metadata through the backend the copy just left.
bool FileSystemResolutionMayChange(const TableSchema& schema,
                                   const std::map<std::string, std::string>& dynamic_options) {
    const std::map<std::string, std::string>& schema_options = schema.Options();
    for (const auto& [key, value] : dynamic_options) {
        if (key != Options::FILE_SYSTEM && key.rfind("fs.", 0) != 0) {
            continue;
        }
        auto schema_option = schema_options.find(key);
        if (schema_option == schema_options.end() || schema_option->second != value) {
            return true;
        }
    }
    return false;
}

/// The options a context carries, merged into the table the load hands back: a table is the
/// product of the load, so the options speak there - the way `Copy` merges them, over the ones
/// the schema carries - and `NewXxx` on the product serves them without merging again. A
/// caller holding a table already - a load that is over - changes it through `Copy` itself,
/// and an option that contradicts the table that way is refused.
Result<std::shared_ptr<Table>> WithContextOptions(
    const std::shared_ptr<Table>& table, const std::shared_ptr<TableSchema>& schema,
    const std::map<std::string, std::string>& options) {
    if (table == nullptr || schema == nullptr || options.empty()) {
        return table;
    }
    const std::map<std::string, std::string>& schema_options = schema->Options();
    for (const auto& [key, value] : options) {
        auto schema_option = schema_options.find(key);
        // A key the schema does not carry, or carries differently: the merge has something to
        // do. Every key agreeing with the schema leaves the table the load built as it is.
        if (schema_option == schema_options.end() || schema_option->second != value) {
            return table->Copy(options);
        }
    }
    return table;
}

/// The tail every `TableOf` ends with: the schema one of them read says what the path names. A
/// format table is handed back as the `FormatTable` it is, everything else as the
/// `FileStoreTable` a managed table is, carrying the schema for its `NewXxx` to build from. A
/// null schema is handed over as it is: what to say about a table that is not there is the
/// managed path's to decide, and it says it in more detail.
///
/// @param file_system The file system the schema above was read through, which a format table
///        is served by - it has no schema options of its own to name another one.
/// @param explicit_file_system The file system the context supplied, which a managed table is
///        opened through when there is one. A null one leaves the table to resolve from the
///        options its schema carries: access configuration the schema was stored with then
///        names the files it serves, rather than being pinned to the one this load happened to
///        read the schema through.
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
    // A context names a path, never a catalog identifier, so the table is known by the directory
    // it sits in - the same convention `TryLoadFormatTable` follows above.
    Identifier table_identifier =
        identifier.value_or(Identifier(Identifier::kUnknownDatabase, PathUtil::GetName(path)));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreTable> table,
                           FileStoreTable::Create(explicit_file_system, path, latest_schema,
                                                  table_identifier, uuid, branch, schema_manager,
                                                  /*snapshot_manager=*/nullptr, fs_scheme_map));
    return WithContextOptions(table, latest_schema, options);
}

/// The managed table a context names behind a path: the schema is read through a manager of the
/// context's branch - handed over instead when the caller already read it - and what the schema
/// says is there decides the table `TableOfLoadedSchema` hands back.
///
/// @param schema_json The schema of the table, when the caller already read it, or null when it
///        has to be read here. It is honoured on the main branch only: a branch publishes its own
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

/// The identifier a catalog knows the table by, on `branch`: a non-main branch is folded into
/// the identifier, so the catalog is asked for the table on that branch rather than the main one.
Result<std::optional<Identifier>> MakeBranchCatalogIdentifier(
    const std::optional<Identifier>& identifier, const std::string& branch) {
    if (!identifier) {
        return std::optional<Identifier>();
    }
    PAIMON_ASSIGN_OR_RAISE(Identifier branch_identifier,
                           CatalogUtils::BranchIdentifier(identifier.value(), branch));
    return std::optional<Identifier>(std::move(branch_identifier));
}

/// The schema a catalog may hold for the table, read from it on the main branch only and null
/// otherwise. A branch publishes its own schema under `branch/branch-<name>`, which is where
/// `ManagedTableOf` reads instead, so a catalog answering for no branch schema does not fail the
/// `kind` operation to one. `kind` also names the operator the missing-identifier message speaks
/// of.
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

/// The table a scan context names, read from what the context carries: a format table the
/// builder was handed, a system table behind a `T$system` path, or the table - format or
/// managed - the schema under the context's path says is there.
Result<std::shared_ptr<Table>> TableOf(const ScanContext& ctx) {
    // A table the caller already loaded says what it is, so nothing is read to find out.
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
    // A format table is planned by listing directories, so it never reaches the manifest path
    // below. One `TableScan` interface serves both, as Java Paimon serves both through one
    // `ReadBuilder`.
    const std::string branch = BranchManager::NormalizeBranch(tmp_options.GetBranch());
    return ManagedTableOf(tmp_options.GetFileSystem(), ctx.GetSpecificFileSystem(), ctx.GetPath(),
                          branch, ctx.GetOptions(), ctx.GetSpecificTableSchema(),
                          ctx.GetIdentifier());
}

/// The table a read context names, read from what the context carries: a format table the
/// builder was handed, a system table behind a `T$system` path, or the table - format or
/// managed - the schema under the context's path says is there.
Result<std::shared_ptr<Table>> TableOf(const ReadContext& ctx) {
    // A table the caller already loaded says what it is, so nothing is read to find out.
    if (ctx.GetFormatTable() != nullptr) {
        return std::shared_ptr<Table>(ctx.GetFormatTable());
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_core_options,
                           CoreOptions::FromMap(ctx.GetOptions(), ctx.GetSpecificFileSystem(),
                                                ctx.GetFileSystemSchemeToIdentifierMap()));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(ctx.GetPath()));
    if (system_table_path) {
        if (PredicateUtils::ContainsFullTextMatch(context->GetPredicate())) {
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

    // A format table has no manifest and a file another engine wrote carries no field ids, so
    // which files there are and how a row is put back together is its own. How a file is opened
    // is not: both paths go through `DataFileReaderFactory`.
    const std::string branch = ctx.GetBranch();
    // The scheme map the schema was read through, so the table it describes resolves custom
    // schemes the same way - the data belongs to the metadata, both sit behind the map.
    return ManagedTableOf(tmp_core_options.GetFileSystem(), ctx.GetSpecificFileSystem(),
                          ctx.GetPath(), branch, ctx.GetOptions(), ctx.GetSpecificTableSchema(),
                          ctx.GetIdentifier(), /*uuid=*/"",
                          ctx.GetFileSystemSchemeToIdentifierMap());
}

/// The table a write context names, read from what the context carries: a format table the
/// builder was handed, or the table - format or managed - the schema under the context's root
/// path says is there.
Result<std::shared_ptr<Table>> TableOf(const WriteContext& ctx) {
    // A table the caller already loaded says what it is, so nothing is read to find out.
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
        // GetTableFileSystem, so the write uses the credentials of this table.
        PAIMON_ASSIGN_OR_RAISE(specific_fs,
                               ctx.GetCatalog()->GetTableFileSystem(ctx.GetIdentifier().value()));
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx.GetOptions(), specific_fs,
                                                ctx.GetFileSystemSchemeToIdentifierMap()));
    std::string branch = ctx.GetBranch();
    PAIMON_ASSIGN_OR_RAISE(std::optional<Identifier> catalog_identifier,
                           MakeBranchCatalogIdentifier(ctx.GetIdentifier(), branch));
    // Only the main branch may have a current schema the catalog alone holds. A branch publishes
    // its own under `branch/branch-<name>`, which is where the load below and a read of the branch
    // both read it, so a catalog answering for no branch schema does not fail a write to one.
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<std::string> catalog_table_schema,
        CatalogMainBranchSchemaJson(ctx.GetCatalog(), catalog_identifier, branch, "write"));
    // A format table writes plain data files into a directory, so it never reaches the manifest
    // path below.
    return ManagedTableOf(tmp_options.GetFileSystem(), specific_fs, ctx.GetRootPath(), branch,
                          ctx.GetOptions(), catalog_table_schema, ctx.GetIdentifier(), /*uuid=*/"",
                          ctx.GetFileSystemSchemeToIdentifierMap());
}

/// The table a commit context names, read from what the context carries: a format table the
/// builder was handed, or the table - format or managed - the schema under the context's root
/// path says is there.
Result<std::shared_ptr<Table>> TableOf(const CommitContext& ctx) {
    // A table the caller already loaded says what it is, so nothing is read to find out.
    if (ctx.GetFormatTable() != nullptr) {
        return std::shared_ptr<Table>(ctx.GetFormatTable());
    }
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx.GetCatalog()));
    // Use catalog credentials unless the caller supplied a file system.
    std::shared_ptr<FileSystem> specific_fs = ctx.GetSpecificFileSystem();
    if (specific_fs == nullptr && ctx.GetCatalog() != nullptr) {
        if (!ctx.GetIdentifier()) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        // Use the credentials of this table, which a catalog issuing per-table temporary
        // ones only hands out through GetTableFileSystem.
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
                           MakeBranchCatalogIdentifier(ctx.GetIdentifier(), branch));
    // Catalog-managed schemas may be absent from the table directory, but only on the main branch:
    // a branch publishes its own under `branch/branch-<name>`, which is where the load below and a
    // read of the branch both read it, so a catalog answering for no branch schema does not fail a
    // commit to one.
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<std::string> catalog_table_schema,
        CatalogMainBranchSchemaJson(ctx.GetCatalog(), catalog_identifier, branch, "commit"));
    // A format table commits by renaming files into place, so it never reaches the snapshot path
    // below. The managed path here reads the commit's branch, so this reads the same one: the two
    // must not dispatch on different schemas.
    return ManagedTableOf(tmp_options.GetFileSystem(), specific_fs, root_path, branch,
                          ctx.GetOptions(), catalog_table_schema, ctx.GetIdentifier(),
                          ctx.GetTableId().value_or(""));
}

/// The options a context carries against the ones the table was loaded with. A table serves the
/// options it was loaded with, so an option the context contradicts is refused rather than
/// honoured - a caller that means to change them copies the table. A table a load hands back
/// carries the context's own options merged in, so it agrees with the context by construction;
/// the refusal belongs to a table the caller held already, whose load is over. A key the table
/// was not loaded with is no contradiction: it names a runtime setting - a scan mode, a batch
/// size - which is none of the table's business, and is served as given.
Status CheckOptionsAgreeWithTable(const TableSchema& table_schema,
                                  const std::map<std::string, std::string>& context_options) {
    const std::map<std::string, std::string>& table_options = table_schema.Options();
    for (const auto& [key, value] : context_options) {
        auto table_option = table_options.find(key);
        if (table_option != table_options.end() && table_option->second != value) {
            // A value may be a credential, and the error travels into logs: it is redacted the
            // way every surface that exposes options redacts them.
            return Status::Invalid(
                fmt::format("option '{}' is '{}' in the context but '{}' on the table", key,
                            SensitiveConfigUtils::RedactValue(key, value),
                            SensitiveConfigUtils::RedactValue(key, table_option->second)));
        }
    }
    return Status::OK();
}

/// The checks every operator entry point opens with: a context, a memory pool and an executor
/// must all be there for the operator to be built at all. `kind` names the operator the first
/// message speaks of, which is all that sets the entry points' messages apart.
template <typename ContextPointer>
Status CheckOperatorContext(const ContextPointer& context, const char* kind) {
    if (context == nullptr) {
        return Status::Invalid(fmt::format("{} context is null pointer", kind));
    }
    if (context->GetMemoryPool() == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    if (context->GetExecutor() == nullptr) {
        return Status::Invalid("executor is null pointer");
    }
    return Status::OK();
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

FileStoreTable::FileStoreTable(const std::shared_ptr<FileStore>& store,
                               const Identifier& identifier, const std::string& uuid)
    : Table(store->file_system(), store->root_path(), store->schema(), identifier, uuid),
      store_(store) {}

Result<std::shared_ptr<FileStoreTable>> FileStoreTable::Create(
    const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
    const std::shared_ptr<Schema>& schema, const Identifier& identifier, const std::string& uuid,
    const std::string& branch, const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::map<std::string, std::string>& fs_scheme_map) {
    // The kind the schema's primary keys name, which no schema change can add or drop, so the
    // kind the table is built as is the kind every schema its root path ever held describes. A
    // caller without a schema gets the append-only kind, whose operators then report that no
    // schema was found. A schema that is not a table schema - the read-only one a system table
    // exposes, say - is none to serve a managed table through, so it is refused rather than cast.
    std::shared_ptr<TableSchema> table_schema;
    if (schema != nullptr) {
        table_schema = std::dynamic_pointer_cast<TableSchema>(schema);
        if (table_schema == nullptr) {
            return Status::Invalid(
                fmt::format("{} is not a managed table schema", identifier.ToString()));
        }
    }
    // The store parses the schema's options, so a schema whose options do not parse is refused
    // here: the table it names cannot be served, not by one operator more than another.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStore> store,
                           FileStore::Create(file_system, root_path, table_schema, branch,
                                             schema_manager, snapshot_manager, fs_scheme_map));
    if (table_schema != nullptr && !table_schema->PrimaryKeys().empty()) {
        return std::make_shared<PrimaryKeyFileStoreTable>(store, identifier, uuid);
    }
    return std::make_shared<AppendOnlyFileStoreTable>(store, identifier, uuid);
}

Result<std::shared_ptr<Table>> FileStoreTable::Copy(
    const std::map<std::string, std::string>& dynamic_options) const {
    std::shared_ptr<TableSchema> loaded_schema = std::static_pointer_cast<TableSchema>(schema_);
    if (loaded_schema == nullptr) {
        return Status::Invalid(fmt::format("not found latest schema of {}", FullName()));
    }
    // `type` is structural and read from the schema alone, the promise `Copy` makes: an option
    // given at the call must not restyle the table. `branch` is the same kind of promise: the
    // copy is the table it was copied from, on the branch that table serves, so a branch option
    // given at the call moves neither - it would only claim a branch the copy does not serve -
    // and a caller that means to move it has `SwitchToBranch`. Both are dropped before the
    // merge, so the copy's options never speak of a branch its operators do not.
    std::map<std::string, std::string> overridable = dynamic_options;
    overridable.erase(Options::TYPE);
    overridable.erase(Options::BRANCH);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> merged_schema,
                           loaded_schema->MergeOptions(overridable));
    // The managers are handed over, not rebuilt, when the copy resolves the same file system
    // this table does: its operators then read the schema cache this table already warmed. The
    // file system is the one the caller opened this table through, not the one it resolved to:
    // the copy resolves from its own merged options again, so a file-system option among the
    // ones it was given speaks rather than being pinned to this table's resolution - and when
    // one does change the resolution, fresh managers are built over it instead, so the metadata
    // is read through the same backend the data is. The scheme map travels the same way, so the
    // copy resolves custom schemes as this table does.
    const bool fs_resolution_may_change =
        FileSystemResolutionMayChange(*loaded_schema, overridable);
    return FileStoreTable::Create(store_->specified_file_system(), root_path_,
                                  std::shared_ptr<TableSchema>(std::move(merged_schema)),
                                  identifier_, CatalogUuid().value_or(""), CurrentBranch(),
                                  fs_resolution_may_change ? nullptr : GetSchemaManager(),
                                  fs_resolution_may_change ? nullptr : GetSnapshotManager(),
                                  store_->fs_scheme_map());
}

Result<std::shared_ptr<Table>> FileStoreTable::SwitchToBranch(const std::string& branch) const {
    const std::string target_branch = BranchManager::NormalizeBranch(branch);
    if (target_branch == CurrentBranch()) {
        // The table already serves this branch: a copy of it - schema and managers shared - is
        // the same table to the caller.
        return FileStoreTable::Create(store_->specified_file_system(), root_path_, schema_,
                                      identifier_, CatalogUuid().value_or(""), CurrentBranch(),
                                      GetSchemaManager(), GetSnapshotManager(),
                                      store_->fs_scheme_map());
    }
    // The schema of the branch switched to, read from its own directory, which a branch without
    // one does not have.
    auto schema_manager = std::make_shared<SchemaManager>(file_system_, root_path_, target_branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest,
                           schema_manager->Latest());
    if (!latest) {
        return Status::Invalid(fmt::format("branch {} does not exist", target_branch));
    }
    // A branch other than main is named in the options the schema carries - the way a read
    // context names it - so what reads through them serves the branch the managers point at.
    std::map<std::string, std::string> overridable;
    if (target_branch != BranchManager::DEFAULT_MAIN_BRANCH) {
        overridable[Options::BRANCH] = target_branch;
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> branch_schema,
                           (*latest)->MergeOptions(overridable));
    return FileStoreTable::Create(store_->specified_file_system(), root_path_,
                                  std::shared_ptr<TableSchema>(std::move(branch_schema)),
                                  identifier_, CatalogUuid().value_or(""), target_branch,
                                  schema_manager, /*snapshot_manager=*/nullptr,
                                  store_->fs_scheme_map());
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

std::unique_ptr<FileStoreScan> FileStoreTable::WithTablePath(std::unique_ptr<FileStoreScan>&& scan,
                                                             const std::string& root_path) {
    scan->WithTablePath(root_path);
    return std::move(scan);
}

Result<std::unique_ptr<FileStoreWrite>> FileStoreTable::NewWrite(
    std::unique_ptr<WriteContext> ctx) const {
    PAIMON_RETURN_NOT_OK(CheckOperatorContext(ctx, "write"));
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx->GetCatalog()));
    std::shared_ptr<FileSystem> specific_fs = ctx->GetSpecificFileSystem();
    // A catalog write names its table; the file system itself comes from the table, which its
    // catalog's credentials already opened.
    if (specific_fs == nullptr && ctx->GetFileSystemSchemeToIdentifierMap().empty() &&
        ctx->GetCatalog() != nullptr && !ctx->GetIdentifier()) {
        return Status::Invalid("a catalog write requires a table identifier");
    }
    // The write goes to the branch the table serves: a table is the branch it was loaded for,
    // so the context's own branch is not consulted.
    PAIMON_ASSIGN_OR_RAISE(std::optional<Identifier> catalog_identifier,
                           MakeBranchCatalogIdentifier(ctx->GetIdentifier(), CurrentBranch()));
    // The schema the table was constructed with, so a write serves the table as it was loaded
    // rather than reading the schema again.
    const std::shared_ptr<TableSchema>& schema = store_->schema();
    if (schema == nullptr) {
        return Status::Invalid(
            fmt::format("not found latest schema in branch {}", CurrentBranch()));
    }
    // The options the store was built with are the ones this write serves: an option the
    // context contradicts is refused rather than honoured, and a key the table was not loaded
    // with names a runtime setting the write does not take - `Copy` derives the table that one
    // belongs to.
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(*schema, ctx->GetOptions()));
    CoreOptions options = store_->options();
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
        store_->NewPathFactory(arrow_schema, options,
                               options.GetWriteFileFormat(/*level=*/0)->Identifier(),
                               ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SnapshotManager> snapshot_manager,
        store_->NewSnapshotManager(options, ctx->GetCatalog(), catalog_identifier));
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

Result<std::unique_ptr<FileStoreWrite>> FileStoreWrite::Create(std::unique_ptr<WriteContext> ctx) {
    PAIMON_RETURN_NOT_OK(CheckOperatorContext(ctx, "write"));
    // First the table the context names, then the context handed to that table's `NewWrite`.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, TableOf(*ctx));
    return table->NewWrite(std::move(ctx));
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

}  // namespace

Result<std::unique_ptr<TableScan>> FileStoreTable::NewScan(
    std::unique_ptr<ScanContext> context) const {
    PAIMON_RETURN_NOT_OK(CheckOperatorContext(context, "scan"));
    // The schema the table was constructed with, so a scan serves the table as it was loaded
    // rather than reading the schema again.
    const std::shared_ptr<TableSchema>& table_schema = store_->schema();
    if (table_schema == nullptr) {
        return Status::Invalid("not found latest schema");
    }
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(*table_schema, context->GetOptions()));
    PAIMON_ASSIGN_OR_RAISE(bool read_optimized,
                           OptionsUtils::GetValueFromMap<bool>(context->GetOptions(),
                                                               kReadOptimizedScanOption, false));
    // The options the store was built with are the ones this scan serves: an option the context
    // contradicts is refused above, and a key the table was not loaded with names a runtime
    // setting the scan does not take - `Copy` derives the table that one belongs to. The branch
    // in them is the table's, not the context's: a table is the branch it was loaded for, so a
    // branch the context carries is not honoured.
    CoreOptions core_options = store_->options();
    // The cache is a runtime setting of this scan, not of the table: the copy the scan serves
    // carries it.
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
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                           store_->NewPathFactory(arrow_schema, core_options,
                                                  core_options.GetFileFormat()->Identifier(),
                                                  context->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        store_->NewManifestList(core_options, path_factory, context->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ManifestFile> manifest_file,
                           store_->NewManifestFile(core_options, path_factory, partition_schema,
                                                   context->GetMemoryPool()));

    // The managers the table carries, whose schema cache the dispatch already warmed.
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStoreScan> file_store_scan,
        NewFileStoreScan(manifest_list, manifest_file, arrow_schema, core_options,
                         context->GetExecutor(), context->GetMemoryPool(), context.get()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SplitGenerator> split_generator,
                           NewSplitGenerator(core_options));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexFileHandler> index_file_handler,
        store_->NewIndexFileHandler(core_options, path_factory, context->GetMemoryPool()));
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

Result<std::unique_ptr<TableScan>> TableScan::Create(std::unique_ptr<ScanContext> context) {
    PAIMON_RETURN_NOT_OK(CheckOperatorContext(context, "scan"));
    // First the table the context names, then the context handed to that table's `NewScan`.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, TableOf(*context));
    return table->NewScan(std::move(context));
}

namespace {

/// The read context of a fallback branch, whose schema is not the table's own: it is read
/// through a manager of that branch, and the options the read serves are the ones that schema
/// was loaded with - the same way the table's own read serves the store's - with the branch it
/// was read from in them. A branch the context carries is not honoured here.
/// @param file_system The file system the table was opened with, which the fallback reads
///        through in place of one resolved from the context.
/// @param root_path The root path the table was opened with, which the fallback reads from in
///        place of the one named by the context.
Result<std::unique_ptr<InternalReadContext>> CreateFallbackReadContext(
    const std::shared_ptr<ReadContext>& context, const std::string& branch,
    const std::shared_ptr<FileSystem>& file_system, const std::string& root_path) {
    std::shared_ptr<TableSchema> table_schema;
    const auto& specific_table_schema = context->GetSpecificTableSchema();
    if (branch == BranchManager::DEFAULT_MAIN_BRANCH && specific_table_schema) {
        PAIMON_ASSIGN_OR_RAISE(table_schema,
                               TableSchema::CreateFromJson(specific_table_schema.value()));
    } else {
        auto schema_manager = std::make_shared<SchemaManager>(file_system, root_path, branch);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                               schema_manager->Latest());
        if (!latest_schema) {
            return Status::Invalid(
                fmt::format("schema file not found in path {}, branch {}", root_path, branch));
        }
        table_schema = latest_schema.value();
    }
    assert(table_schema);
    auto options = table_schema->Options();
    // The dynamic options the load merged into the table this read serves - a sequence-number
    // flag, a batch size - speak for the read, and the fallback branch serves the same read:
    // they are merged over the fallback branch's own schema options the way the load merged
    // them over the table's, so the fallback reader recognises what the main one does. `type`
    // stays the branch's own: a copy never restyles the table, and neither does a fallback.
    for (const auto& [key, value] : context->GetOptions()) {
        if (key != Options::TYPE) {
            options[key] = value;
        }
    }
    if (branch != BranchManager::DEFAULT_MAIN_BRANCH) {
        options[Options::BRANCH] = branch;
    } else {
        options.erase(Options::BRANCH);
    }
    return InternalReadContext::Create(context, table_schema, options, file_system, &root_path);
}

}  // namespace

Result<std::unique_ptr<TableRead>> FileStoreTable::CreateTableRead(
    const std::shared_ptr<InternalReadContext>& internal_context,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<Executor>& executor) const {
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
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        store_->NewPathFactory(arrow_schema, core_options,
                               core_options.GetFileFormat()->Identifier(), memory_pool));
    return NewTableRead(path_factory, internal_context, memory_pool, executor);
}

Result<std::unique_ptr<TableRead>> FileStoreTable::NewRead(
    std::unique_ptr<ReadContext> context) const {
    std::shared_ptr<ReadContext> read_context = std::move(context);
    PAIMON_RETURN_NOT_OK(CheckOperatorContext(read_context, "read"));
    // The schema the table was constructed with, so a read serves the table as it was loaded
    // rather than reading the schema again. Its options are the ones the read serves: an option
    // the context would have changed is refused rather than honoured, and a key the table was
    // not loaded with names a runtime setting the read does not take - `Copy` derives the table
    // that one belongs to.
    const std::shared_ptr<TableSchema>& loaded_schema = store_->schema();
    if (loaded_schema == nullptr) {
        return Status::Invalid("not found latest schema");
    }
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(*loaded_schema, read_context->GetOptions()));
    // The options the store was built with, which the read reads through on the branch the
    // table serves: a table is the branch it was loaded for, so the context's own branch is
    // not consulted.
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> internal_context,
        InternalReadContext::Create(read_context, loaded_schema, store_->options(), &root_path_));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           CreateTableRead(internal_context, read_context->GetMemoryPool(),
                                           read_context->GetExecutor()));

    std::optional<std::string> scan_fallback_branch =
        internal_context->GetCoreOptions().GetScanFallbackBranch();
    if (!scan_fallback_branch ||
        StringUtils::IsNullOrWhitespaceOnly(scan_fallback_branch.value())) {
        return std::move(table_read);
    }

    // The fallback branch is not the table's, so its schema is read through a manager of its
    // own rather than the table's.
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> fallback_context,
        CreateFallbackReadContext(read_context, /*branch=*/scan_fallback_branch.value(),
                                  file_system_, root_path_));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> fallback_table_read,
                           CreateTableRead(fallback_context, read_context->GetMemoryPool(),
                                           read_context->GetExecutor()));
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool = internal_context->GetArrowMemoryPool();
    return std::make_unique<FallbackTableRead>(std::move(table_read),
                                               std::move(fallback_table_read), arrow_pool);
}

Result<std::unique_ptr<TableRead>> TableRead::Create(std::unique_ptr<ReadContext> ctx) {
    PAIMON_RETURN_NOT_OK(CheckOperatorContext(ctx, "read"));
    // First the table the context names, then the context handed to that table's `NewRead`.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, TableOf(*ctx));
    return table->NewRead(std::move(ctx));
}

namespace {

bool IsCatalogCommit(const CommitContext& ctx) {
    return AsVersionManaged(ctx.GetCatalog()) != nullptr;
}

Result<std::shared_ptr<SnapshotCommit>> NewSnapshotCommit(
    const CommitContext& ctx, const std::optional<Identifier>& catalog_identifier,
    const std::shared_ptr<FileSystem>& fs,
    const std::shared_ptr<SnapshotManager>& snapshot_manager) {
    if (IsCatalogCommit(ctx)) {
        if (!catalog_identifier) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        // Reuse the caller's UUID so table recreation cannot redirect this commit.
        return std::shared_ptr<SnapshotCommit>(std::make_shared<CatalogSnapshotCommit>(
            ctx.GetCatalog(), catalog_identifier.value(), ctx.GetTableId()));
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

}  // namespace

Result<std::unique_ptr<FileStoreCommit>> FileStoreTable::NewCommit(
    std::unique_ptr<CommitContext> ctx) const {
    PAIMON_RETURN_NOT_OK(CheckOperatorContext(ctx, "commit"));
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx->GetCatalog()));
    std::shared_ptr<FileSystem> specific_fs = ctx->GetSpecificFileSystem();
    // A catalog commit names its table; the file system itself comes from the table, which its
    // catalog's credentials already opened.
    if (specific_fs == nullptr && ctx->GetCatalog() != nullptr && !ctx->GetIdentifier()) {
        return Status::Invalid("a catalog commit requires a table identifier");
    }
    // A commit commits to the root path the table was opened with and the branch the table
    // serves, in place of the ones named by the context: a table is the branch it was loaded
    // for, so the context's own branch is not consulted.
    PAIMON_ASSIGN_OR_RAISE(std::optional<Identifier> catalog_identifier,
                           MakeBranchCatalogIdentifier(ctx->GetIdentifier(), CurrentBranch()));
    // The id a snapshot records the catalog's current schema by, read through the loader below
    // rather than trusted from a schema this commit will not re-read.
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
    // The schema the table was constructed with, so a commit serves the table as it was loaded
    // rather than reading the schema again.
    const std::shared_ptr<TableSchema>& schema = store_->schema();
    if (schema == nullptr) {
        return Status::Invalid(
            fmt::format("not found latest schema in branch {}", CurrentBranch()));
    }
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    // The options the store was built with are the ones this commit serves: an option the
    // context contradicts is refused rather than honoured, and a key the table was not loaded
    // with names a runtime setting the commit does not take - `Copy` derives the table that one
    // belongs to.
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(*schema, ctx->GetOptions()));
    CoreOptions options = store_->options();
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*schema, options));
    assert(options.GetFileSystem());
    assert(options.GetFileFormat());
    PAIMON_RETURN_NOT_OK(FileStoreCommitImpl::ValidateCommitOptions(options));
    PAIMON_ASSIGN_OR_RAISE(bool is_object_store, FileSystem::IsObjectStore(root_path_));
    // Catalog commits do not require atomic rename on the object store. The inte-test marker is
    // not a table option, so it is read from the context the commit was handed.
    if (is_object_store && !ctx->UseRESTCatalogCommit() && !IsCatalogCommit(*ctx) &&
        ctx->GetOptions().find("enable-object-store-commit-in-inte-test") ==
            ctx->GetOptions().end()) {
        return Status::NotImplemented(
            "commit operation does not support object store file system for now");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
        store_->NewPartitionComputer(arrow_schema, options, ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        store_->NewPathFactory(arrow_schema, options, options.GetFileFormat()->Identifier(),
                               ctx->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SnapshotManager> snapshot_manager,
        store_->NewSnapshotManager(options, ctx->GetCatalog(), catalog_identifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ManifestList> manifest_list,
                           store_->NewManifestList(options, path_factory, ctx->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> partition_schema,
                           FieldMapping::GetPartitionSchema(arrow_schema, schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        store_->NewManifestFile(options, path_factory, partition_schema, ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<IndexManifestFile> index_manifest_file,
        store_->NewIndexManifestFile(options, path_factory, ctx->GetMemoryPool()));

    auto expire_snapshots = std::make_shared<ExpireSnapshots>(
        snapshot_manager, path_factory, manifest_list, manifest_file, index_manifest_file,
        options.GetFileSystem(), options.GetExpireConfig(), options.RealtimeEnabled(),
        ctx->GetExecutor());

    CommitScanner::ScanSupplier scan_supplier =
        NewCommitScanSupplier(snapshot_manager, manifest_list, manifest_file, arrow_schema, options,
                              ctx->GetExecutor(), ctx->GetMemoryPool());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SnapshotCommit> snapshot_commit,
        NewSnapshotCommit(*ctx, catalog_identifier, options.GetFileSystem(), snapshot_manager));

    return std::make_unique<FileStoreCommitImpl>(
        ctx->GetMemoryPool(), ctx->GetExecutor(), arrow_schema, root_path_, ctx->GetCommitUser(),
        options, path_factory, std::move(partition_computer), snapshot_manager, snapshot_commit,
        ctx->IgnoreEmptyCommit(), ctx->AppendCommitCheckConflict(), schema,
        std::move(schema_id_loader), manifest_file, manifest_list, index_manifest_file,
        expire_snapshots, GetSchemaManager(), std::move(scan_supplier));
}

Result<std::unique_ptr<FileStoreCommit>> FileStoreCommit::Create(
    std::unique_ptr<CommitContext> ctx) {
    PAIMON_RETURN_NOT_OK(CheckOperatorContext(ctx, "commit"));
    // First the table the context names, then the context handed to that table's `NewCommit`.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, TableOf(*ctx));
    return table->NewCommit(std::move(ctx));
}

}  // namespace paimon
