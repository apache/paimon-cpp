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
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/global_index/btree/btree_defs.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/path_util.h"
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
#include "paimon/core/options/table_type.h"
#include "paimon/core/postpone/postpone_bucket_file_store_write.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/file_store_table.h"
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

/// The tail every `TableOf` ends with: the schema one of them read says what the path names. A
/// format table is handed back as the `FormatTable` it is, everything else as the
/// `FileStoreTable` a managed table is, carrying the schema for its `NewXxx` to build from. A
/// null schema is handed over as it is: what to say about a table that is not there is the
/// managed path's to decide, and it says it in more detail.
Result<std::shared_ptr<Table>> TableOfLoadedSchema(
    const std::shared_ptr<FileSystem>& file_system, const std::string& path,
    const std::string& branch, const std::map<std::string, std::string>& options,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<TableSchema>& latest_schema, const std::optional<Identifier>& identifier,
    const std::string& uuid = "") {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> format_table,
                           TryLoadFormatTable(file_system, path, branch, options, latest_schema));
    if (format_table != nullptr) {
        return std::shared_ptr<Table>(std::move(format_table));
    }
    // A context names a path, never a catalog identifier, so the table is known by the directory
    // it sits in - the same convention `TryLoadFormatTable` follows above.
    Identifier table_identifier =
        identifier.value_or(Identifier(Identifier::kUnknownDatabase, PathUtil::GetName(path)));
    return std::make_shared<FileStoreTable>(file_system, path, latest_schema, table_identifier,
                                            uuid, branch, schema_manager);
}

/// The managed table a context names behind a path: the schema is read through a manager of the
/// context's branch - handed over instead when the caller already read it - and what the schema
/// says is there decides the table `TableOfLoadedSchema` hands back.
///
/// @param schema_json The schema of the table, when the caller already read it, or null when it
///        has to be read here. It is honoured on the main branch only: a branch publishes its own
///        under `branch/branch-<name>`, which is where the read below goes.
Result<std::shared_ptr<Table>> ManagedTableOf(const std::shared_ptr<FileSystem>& file_system,
                                              const std::string& path, const std::string& branch,
                                              const std::map<std::string, std::string>& options,
                                              const std::optional<std::string>& schema_json,
                                              const std::optional<Identifier>& identifier,
                                              const std::string& uuid = "") {
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
    return TableOfLoadedSchema(file_system, path, branch, options, schema_manager, latest_schema,
                               identifier, uuid);
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
    return ManagedTableOf(tmp_options.GetFileSystem(), ctx.GetPath(), branch, ctx.GetOptions(),
                          ctx.GetSpecificTableSchema(), ctx.GetIdentifier());
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
    return ManagedTableOf(tmp_core_options.GetFileSystem(), ctx.GetPath(), branch, ctx.GetOptions(),
                          ctx.GetSpecificTableSchema(), ctx.GetIdentifier());
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
    return ManagedTableOf(tmp_options.GetFileSystem(), ctx.GetRootPath(), branch, ctx.GetOptions(),
                          catalog_table_schema, ctx.GetIdentifier());
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
    // read of the branch both read it, so a catalog answering for no branch schema does not fail
    // a commit to one.
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<std::string> catalog_table_schema,
        CatalogMainBranchSchemaJson(ctx.GetCatalog(), catalog_identifier, branch, "commit"));
    // A format table commits by renaming files into place, so it never reaches the snapshot path
    // below. The managed path here reads the commit's branch, so this reads the same one: the two
    // must not dispatch on different schemas.
    return ManagedTableOf(tmp_options.GetFileSystem(), root_path, branch, ctx.GetOptions(),
                          catalog_table_schema, ctx.GetIdentifier(), ctx.GetTableId().value_or(""));
}

/// The options a context carries against the ones the table was loaded with. A table serves the
/// options it was loaded with, so an option the context contradicts is refused rather than
/// honoured - a caller that means to change them copies the table. A key the table was not loaded
/// with is no contradiction: it names a runtime setting - a scan mode, a batch size - which is
/// none of the table's business, and is served as given.
Status CheckOptionsAgreeWithTable(const TableSchema& table_schema,
                                  const std::map<std::string, std::string>& context_options) {
    const std::map<std::string, std::string>& table_options = table_schema.Options();
    for (const auto& [key, value] : context_options) {
        auto table_option = table_options.find(key);
        if (table_option != table_options.end() && table_option->second != value) {
            return Status::Invalid(
                fmt::format("option '{}' is '{}' in the context but '{}' on the table", key, value,
                            table_option->second));
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

/// The schema the table was constructed with, which a write or a commit serves as the table was
/// loaded rather than reading the schema again.
Result<std::shared_ptr<TableSchema>> LatestTableSchemaOfBranch(
    const std::shared_ptr<Schema>& schema, const std::string& branch) {
    std::shared_ptr<TableSchema> latest_schema = std::static_pointer_cast<TableSchema>(schema);
    if (latest_schema == nullptr) {
        return Status::Invalid(fmt::format("not found latest schema in branch {}", branch));
    }
    return latest_schema;
}

/// The options a write or a commit serves: the ones the table was loaded with - an option the
/// context contradicts is refused by `CheckOptionsAgreeWithTable` above - the ones the context
/// adds on top, and the table's branch, which the metadata paths below are built for: what reads
/// through these options, whose cache keys are scoped by it, has to read the same one.
Result<std::map<std::string, std::string>> MergedOperatorOptions(
    const TableSchema& table_schema, const std::map<std::string, std::string>& context_options,
    const std::string& branch) {
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(table_schema, context_options));
    auto opts = table_schema.Options();
    for (const auto& [key, value] : context_options) {
        opts[key] = value;
    }
    opts[Options::BRANCH] = branch;
    return opts;
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

FileStoreTable::FileStoreTable(const std::shared_ptr<FileSystem>& file_system,
                               const std::string& root_path, const std::shared_ptr<Schema>& schema,
                               const Identifier& identifier, const std::string& uuid,
                               const std::string& branch,
                               const std::shared_ptr<SchemaManager>& schema_manager,
                               const std::shared_ptr<SnapshotManager>& snapshot_manager)
    : Table(file_system, root_path, schema, identifier, uuid),
      store_(std::make_shared<FileStore>(file_system, root_path,
                                         std::static_pointer_cast<TableSchema>(schema), branch,
                                         schema_manager, snapshot_manager)) {}

Result<std::shared_ptr<Table>> FileStoreTable::Copy(
    const std::map<std::string, std::string>& dynamic_options) const {
    std::shared_ptr<TableSchema> loaded_schema = std::static_pointer_cast<TableSchema>(schema_);
    if (loaded_schema == nullptr) {
        return Status::Invalid(fmt::format("not found latest schema of {}", FullName()));
    }
    // `type` is structural and read from the schema alone, the promise `Copy` makes: an option
    // given at the call must not restyle the table.
    std::map<std::string, std::string> overridable = dynamic_options;
    overridable.erase(Options::TYPE);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> merged_schema,
                           loaded_schema->MergeOptions(overridable));
    // The managers are handed over, not rebuilt: the copy serves the same root path and branch,
    // so its operators read the schema cache this table already warmed.
    return std::shared_ptr<Table>(std::make_shared<FileStoreTable>(
        file_system_, root_path_, std::shared_ptr<TableSchema>(std::move(merged_schema)),
        identifier_, CatalogUuid().value_or(""), CurrentBranch(), GetSchemaManager(),
        GetSnapshotManager()));
}

Result<std::shared_ptr<Table>> FileStoreTable::SwitchToBranch(const std::string& branch) const {
    const std::string target_branch = BranchManager::NormalizeBranch(branch);
    if (target_branch == CurrentBranch()) {
        // The table already serves this branch: a copy of it - schema and managers shared - is
        // the same table to the caller.
        return std::shared_ptr<Table>(std::make_shared<FileStoreTable>(
            file_system_, root_path_, schema_, identifier_, CatalogUuid().value_or(""),
            CurrentBranch(), GetSchemaManager(), GetSnapshotManager()));
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
    return std::shared_ptr<Table>(std::make_shared<FileStoreTable>(
        file_system_, root_path_, std::shared_ptr<TableSchema>(std::move(branch_schema)),
        identifier_, CatalogUuid().value_or(""), target_branch, schema_manager));
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
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema,
                           LatestTableSchemaOfBranch(schema_, CurrentBranch()));
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    // The options the table was loaded with, with a runtime setting the context adds on top: an
    // option the context would have changed is refused rather than honoured, and the branch is
    // the table's - the metadata paths below are built for it, so what reads through these
    // options, whose cache keys are scoped by it, has to read the same one.
    std::map<std::string, std::string> opts;
    PAIMON_ASSIGN_OR_RAISE(opts,
                           MergedOperatorOptions(*schema, ctx->GetOptions(), CurrentBranch()));
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions options,
        CoreOptions::FromMap(opts, file_system_, ctx->GetFileSystemSchemeToIdentifierMap()));
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
            return Status::Invalid("real-time deduplicate write requires a real-time context");
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
            file_store_path_factory, snapshot_manager, GetSchemaManager(), ctx->GetCommitUser(),
            root_path_, schema, arrow_schema, write_schema, realtime_schema_layout,
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
                snapshot_manager, GetSchemaManager(), ctx->GetCommitUser(), root_path_, schema,
                arrow_schema, partition_schema, io_manager, options, ctx->IsStreamingMode(),
                ctx->IgnoreNumBucketCheck(), ctx->GetWriteId(),
                ctx->GetFileSystemSchemeToIdentifierMap(), ctx->GetExecutor(), ctx->GetMemoryPool(),
                file_system_);
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
                HasHistoricalPrimaryKeyBTreeDefinition(GetSchemaManager(), schema->Id()));
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
                    root_path_, CurrentBranch(), schema,
                    primary_key_index_definitions.Definitions(), file_store_path_factory,
                    index_file_handler, options, io_manager, ctx->EnableMultiThreadSpill(),
                    ctx->GetExecutor(), ctx->GetMemoryPool());
        }

        std::shared_ptr<RealtimeSchemaLayout> realtime_schema_layout;
        if (ctx->GetRealtimeContext()) {
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<RealtimeSchemaLayout> layout,
                RealtimeSchemaLayout::Create(RealtimeStoreMode::PRIMARY_KEY, arrow_schema));
            realtime_schema_layout = std::move(layout);
        }

        return std::make_unique<KeyValueFileStoreWrite>(
            file_store_path_factory, snapshot_manager, GetSchemaManager(), ctx->GetCommitUser(),
            root_path_, schema, arrow_schema, realtime_schema_layout, partition_schema,
            dv_maintainer_factory, primary_key_index_maintainer_factory, io_manager, key_comparator,
            sequence_fields_comparator, merge_function_wrapper, options, ignore_previous_files,
            ctx->IsStreamingMode(), ctx->IgnoreNumBucketCheck(), ctx->EnableMultiThreadSpill(),
            ctx->GetRealtimeContext(), ctx->GetExecutor(), ctx->GetMemoryPool());
    }
}

Result<std::unique_ptr<FileStoreWrite>> FileStoreWrite::Create(std::unique_ptr<WriteContext> ctx) {
    PAIMON_RETURN_NOT_OK(CheckOperatorContext(ctx, "write"));
    // First the table the context names, then the context handed to that table's `NewWrite`.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, TableOf(*ctx));
    return table->NewWrite(std::move(ctx));
}

namespace {

class TableScanImpl {
 public:
    /// @param snapshot_manager The snapshot manager the scan reads through - the table's own.
    /// @param schema_manager The schema manager the scan reads through, likewise.
    static Result<std::unique_ptr<FileStoreScan>> CreateFileStoreScan(
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<TableSchema>& table_schema, const CoreOptions& core_options,
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<SchemaManager>& schema_manager,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& memory_pool,
        const ScanContext* context, const std::string& root_path) {
        auto fs = core_options.GetFileSystem();
        auto manifest_file_format = core_options.GetManifestFormat();
        // TODO(liancheng.lsz): support fallback branch in scan
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
        return WithTablePath(std::move(scan), root_path);
    }

    static std::unique_ptr<FileStoreScan> WithTablePath(std::unique_ptr<FileStoreScan>&& scan,
                                                        const std::string& root_path) {
        scan->WithTablePath(root_path);
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
    std::shared_ptr<TableSchema> loaded_schema = std::static_pointer_cast<TableSchema>(schema_);
    if (loaded_schema == nullptr) {
        return Status::Invalid("not found latest schema");
    }
    const std::shared_ptr<TableSchema>& table_schema = loaded_schema;
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(*table_schema, context->GetOptions()));
    PAIMON_ASSIGN_OR_RAISE(bool read_optimized,
                           OptionsUtils::GetValueFromMap<bool>(context->GetOptions(),
                                                               kReadOptimizedScanOption, false));
    // The options the table was loaded with, with a runtime setting the context adds on top: an
    // option the context would have changed is refused above, so the table's own stand as they
    // are. The branch is the table's, not the context's: a table is the branch it was loaded
    // for, so a branch the context carries is not honoured.
    auto options = table_schema->Options();
    for (const auto& [key, value] : context->GetOptions()) {
        if (key != Options::BRANCH) {
            options[key] = value;
        }
    }
    if (BranchManager::IsMainBranch(CurrentBranch())) {
        options.erase(Options::BRANCH);
    } else {
        options[Options::BRANCH] = CurrentBranch();
    }
    // The file system and root path the table was opened with, so a scan serves the table as it
    // was loaded rather than resolving them from the context.
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(options, file_system_, {}));
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

    // The managers the table carries, whose schema cache the dispatch already warmed.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan> file_store_scan,
                           TableScanImpl::CreateFileStoreScan(
                               path_factory, arrow_schema, table_schema, core_options,
                               GetSnapshotManager(), GetSchemaManager(), context->GetExecutor(),
                               context->GetMemoryPool(), context.get(), root_path_));
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

/// @param schema_manager The schema manager of `branch`, from which the latest schema is read
///        when `loaded_schema` is null.
/// @param loaded_schema The schema of `branch`, when the caller already read it, or null when it
///        has to be read here: `FileStoreTable::NewRead()` hands the table's own schema over, and
///        reading it again would cost a second listing and read.
/// @param file_system The file system the table was opened with, which a read reads through in
///        place of one resolved from the context.
/// @param root_path The root path the table was opened with, which a read reads from in place of
///        the one named by the context.
Result<std::unique_ptr<InternalReadContext>> CreateInternalReadContext(
    const std::shared_ptr<ReadContext>& context, const std::string& branch,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<TableSchema>& loaded_schema,
    const std::shared_ptr<FileSystem>& file_system, const std::string& root_path) {
    std::shared_ptr<TableSchema> table_schema;
    const auto& specific_table_schema = context->GetSpecificTableSchema();
    if (loaded_schema != nullptr) {
        table_schema = loaded_schema;
    } else if (branch == BranchManager::DEFAULT_MAIN_BRANCH && specific_table_schema) {
        PAIMON_ASSIGN_OR_RAISE(table_schema,
                               TableSchema::CreateFromJson(specific_table_schema.value()));
    } else {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                               schema_manager->Latest());
        if (!latest_schema) {
            return Status::Invalid(
                fmt::format("schema file not found in path {}, branch {}", root_path, branch));
        }
        table_schema = latest_schema.value();
    }
    assert(table_schema);

    // The options the schema was loaded with, with a runtime setting the context adds on top -
    // and the fallback below serves the ones its own schema was loaded with. The branch is the
    // one named below, not the context's: a branch the context carries is not honoured here.
    auto options = table_schema->Options();
    for (const auto& [key, value] : context->GetOptions()) {
        if (key != Options::BRANCH) {
            options[key] = value;
        }
    }
    if (branch != BranchManager::DEFAULT_MAIN_BRANCH) {
        options[Options::BRANCH] = branch;
    }
    return InternalReadContext::Create(context, table_schema, options, file_system, &root_path);
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
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStore::MakePathFactory(internal_context->GetPath(), arrow_schema,
                                   table_schema->PartitionKeys(), core_options,
                                   core_options.GetFileFormat()->Identifier(), memory_pool));

    if (internal_context->GetPrimaryKeys().empty()) {
        return std::make_unique<AppendOnlyTableRead>(path_factory, internal_context, memory_pool,
                                                     executor);
    }
    return KeyValueTableRead::Create(path_factory, internal_context, memory_pool, executor);
}

}  // namespace

Result<std::unique_ptr<TableRead>> FileStoreTable::NewRead(
    std::unique_ptr<ReadContext> context) const {
    std::shared_ptr<ReadContext> read_context = std::move(context);
    PAIMON_RETURN_NOT_OK(CheckOperatorContext(read_context, "read"));
    // The schema the table was constructed with, so a read serves the table as it was loaded
    // rather than reading the schema again. Its options are the ones the read serves: an option
    // the context would have changed is refused rather than honoured.
    std::shared_ptr<TableSchema> loaded_schema = std::static_pointer_cast<TableSchema>(schema_);
    if (loaded_schema == nullptr) {
        return Status::Invalid("not found latest schema");
    }
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(*loaded_schema, read_context->GetOptions()));
    // The branch the table serves, which the read reads on: a table is the branch it was loaded
    // for, so the context's own branch is not consulted.
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> internal_context,
        CreateInternalReadContext(read_context, CurrentBranch(), GetSchemaManager(), loaded_schema,
                                  file_system_, root_path_));

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
        CreateInternalReadContext(
            read_context, /*branch=*/scan_fallback_branch.value(),
            std::make_shared<SchemaManager>(file_system_, root_path_, scan_fallback_branch.value()),
            /*loaded_schema=*/nullptr, file_system_, root_path_));

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
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema,
                           LatestTableSchemaOfBranch(schema_, CurrentBranch()));
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    // The options the table was loaded with, with a runtime setting the context adds on top: an
    // option the context would have changed is refused rather than honoured, and the branch is
    // the table's - the metadata paths below are built for it, so what reads through these
    // options, whose cache keys are scoped by it, has to read the same one.
    std::map<std::string, std::string> opts;
    PAIMON_ASSIGN_OR_RAISE(opts,
                           MergedOperatorOptions(*schema, ctx->GetOptions(), CurrentBranch()));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(opts, file_system_));
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
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
                           BinaryRowPartitionComputer::Create(schema->PartitionKeys(), arrow_schema,
                                                              options.GetPartitionDefaultName(),
                                                              options.LegacyPartitionNameEnabled(),
                                                              ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        store_->NewPathFactory(arrow_schema, options, options.GetFileFormat()->Identifier(),
                               ctx->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SnapshotManager> snapshot_manager,
        store_->NewSnapshotManager(options, ctx->GetCatalog(), catalog_identifier));
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
        scan_supplier = CreateAppendScanSupplier(snapshot_manager, GetSchemaManager(),
                                                 manifest_list, manifest_file, schema, arrow_schema,
                                                 options, ctx->GetExecutor(), ctx->GetMemoryPool());
    } else {
        scan_supplier = CreatePkScanSupplier(snapshot_manager, GetSchemaManager(), manifest_list,
                                             manifest_file, schema, arrow_schema, options,
                                             ctx->GetExecutor(), ctx->GetMemoryPool());
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SnapshotCommit> snapshot_commit,
                           NewSnapshotCommit(*ctx, catalog_identifier, options, snapshot_manager));

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
