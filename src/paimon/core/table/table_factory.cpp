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
#include "paimon/common/utils/path_util.h"
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

Status CheckCatalogTable(const TableLoadOptions& load, const std::shared_ptr<Table>& table) {
    if (load.table_id && load.table_id != table->CatalogUuid()) {
        return Status::Invalid("the expected table id does not match the catalog table id");
    }
    if (!load.path.empty() && !table->GetRootPath().empty()) {
        PAIMON_ASSIGN_OR_RAISE(std::string supplied_path, PathUtil::NormalizePath(load.path));
        PAIMON_ASSIGN_OR_RAISE(std::string catalog_path,
                               PathUtil::NormalizePath(table->GetRootPath()));
        if (supplied_path != catalog_path) {
            return Status::Invalid("the table path does not match the catalog table location");
        }
    }
    if (std::dynamic_pointer_cast<FileStoreTable>(table) == nullptr &&
        (load.table_schema_json || load.file_system || !load.fs_scheme_to_identifier_map.empty())) {
        return Status::Invalid(
            "a catalog format or system table carries its schema and file system");
    }
    return Status::OK();
}

}  // namespace

Result<std::shared_ptr<Table>> TableFactory::Create(
    const TableLoadOptions& load, const std::map<std::string, std::string>& overrides,
    const char* operation) {
    if (load.catalog == nullptr && load.identifier) {
        return Status::Invalid("a table identifier requires a non-null catalog");
    }
    if (load.catalog != nullptr && !load.identifier) {
        return Status::Invalid("a catalog load requires a table identifier");
    }
    if (load.file_system && !load.fs_scheme_to_identifier_map.empty()) {
        return Status::Invalid("a file system and URI scheme routing cannot be specified together");
    }
    auto fs_scheme_map = load.fs_scheme_to_identifier_map;
    const bool writing = std::string(operation) == "write" || std::string(operation) == "commit";
    if (writing && load.identifier) {
        PAIMON_RETURN_NOT_OK(CatalogUtils::CheckNotSystemTable(*load.identifier, operation));
    }
    std::optional<SystemTablePath> named_system_path;
    if (!load.path.empty()) {
        PAIMON_ASSIGN_OR_RAISE(named_system_path, SystemTableLoader::TryParsePath(load.path));
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::string branch,
        BranchManager::ResolveBranch(load.identifier, overrides,
                                     named_system_path ? named_system_path->branch : std::nullopt,
                                     operation));
    std::optional<Identifier> identifier = load.identifier;
    if (load.catalog) {
        PAIMON_RETURN_NOT_OK(BranchManager::CheckCatalogAddressableBranch(branch));
        PAIMON_ASSIGN_OR_RAISE(Identifier resolved_identifier,
                               CatalogUtils::ResolvedBranchIdentifier(*identifier, branch));
        identifier.emplace(std::move(resolved_identifier));
    }
    auto dynamic_options = overrides;
    if (load.identifier || dynamic_options.count(Options::BRANCH) ||
        (named_system_path && named_system_path->branch)) {
        dynamic_options[Options::BRANCH] = branch;
    }
    if (writing && load.catalog) {
        PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(load.catalog));
        PAIMON_RETURN_NOT_OK(BranchManager::CheckCatalogAddressableBranch(branch));
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> named_branch,
                               load.identifier->GetBranchName());
        if (!BranchManager::IsMainBranch(branch) && !named_branch) {
            return Status::Invalid(
                "a catalog write or commit must name its branch in the table identifier");
        }
    }
    std::string path = load.path;
    std::optional<std::string> table_id = load.table_id;
    std::optional<std::string> schema_json = load.table_schema_json;
    std::shared_ptr<FileSystem> file_system = load.file_system;
    std::shared_ptr<TableSchema> catalog_schema;
    // A catalog-only load obtains identity, schema and location from one table generation.
    if (load.catalog && (path.empty() || load.table_id)) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, load.catalog->GetTable(*identifier));
        PAIMON_RETURN_NOT_OK(CheckCatalogTable(load, table));
        if (std::dynamic_pointer_cast<FileStoreTable>(table) == nullptr) {
            return table;
        }
        if (path.empty()) {
            path = table->GetRootPath();
            if (!schema_json || !BranchManager::IsMainBranch(branch)) {
                // The catalog has resolved this branch's schema. table_schema_json is a
                // main-only input and must not discard it or substitute another branch's schema.
                catalog_schema = std::dynamic_pointer_cast<TableSchema>(table->GetSchema());
            }
        }
        if (!file_system && fs_scheme_map.empty()) {
            file_system = table->GetFileSystem();
        }
        if (auto managed = std::dynamic_pointer_cast<FileStoreTable>(table)) {
            auto inherited_map = managed->GetFileSystemSchemeToIdentifierMap();
            for (const auto& [scheme, identifier] : fs_scheme_map) {
                inherited_map[scheme] = identifier;
            }
            fs_scheme_map = std::move(inherited_map);
        }
        table_id = table->CatalogUuid();
    }
    PAIMON_ASSIGN_OR_RAISE(path, PathUtil::NormalizePath(path));
    if (path.empty()) {
        return Status::Invalid("table path is empty");
    }
    if (load.catalog && !file_system && fs_scheme_map.empty()) {
        PAIMON_ASSIGN_OR_RAISE(file_system, load.catalog->GetTableFileSystem(*identifier));
    }
    if (writing && load.catalog && BranchManager::IsMainBranch(branch) && !schema_json &&
        !catalog_schema) {
        PAIMON_ASSIGN_OR_RAISE(Identifier identifier,
                               CatalogUtils::BranchIdentifier(*load.identifier, branch));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> schema,
                               load.catalog->LoadTableSchema(identifier));
        PAIMON_ASSIGN_OR_RAISE(schema_json, schema->GetJsonSchema());
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions temporary,
                           CoreOptions::FromMap(dynamic_options, file_system, fs_scheme_map));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_path,
                           SystemTableLoader::TryParsePath(path));
    if (system_path) {
        if (writing) {
            return Status::Invalid("system tables are read-only");
        }
        if (!writing) {
            PAIMON_ASSIGN_OR_RAISE(dynamic_options, CatalogUtils::ResolveQueryAuthReadOptions(
                                                        load.catalog, identifier, dynamic_options,
                                                        branch, temporary.GetFileSystem(), path,
                                                        schema_json.has_value(), operation));
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<SystemTable> table,
            SystemTableLoader::LoadFromPath(temporary.GetFileSystem(), path, dynamic_options,
                                            fs_scheme_map, schema_json));
        return std::shared_ptr<Table>(std::move(table));
    }
    std::shared_ptr<TableSchema> schema = catalog_schema;
    const std::string metadata_branch = branch;
    auto schema_manager = std::make_shared<SchemaManager>(temporary.GetFileSystem(), path, branch);
    Result<std::shared_ptr<FormatTable>> format_result = std::shared_ptr<FormatTable>();
    if (!schema) {
        format_result =
            FormatTableLoader::TryLoad(temporary.GetFileSystem(), path, branch, dynamic_options,
                                       schema_json, schema_manager.get(), &schema);
    }
    if (load.catalog && BranchManager::IsMainBranch(branch) &&
        (!format_result.ok() || format_result.value() != nullptr || !schema)) {
        // An external format table may have real data under `schema`, including file names
        // that look like schema versions. Its catalog determines the directory semantics.
        Result<std::shared_ptr<Table>> catalog_result = load.catalog->GetTable(*load.identifier);
        if (catalog_result.ok() &&
            std::dynamic_pointer_cast<FormatTable>(catalog_result.value()) != nullptr) {
            PAIMON_RETURN_NOT_OK(CheckCatalogTable(load, catalog_result.value()));
            return catalog_result;
        }
        PAIMON_RETURN_NOT_OK(format_result.status());
        // Preserve catalog lookup failures (including a missing upstream table) when there
        // is no usable schema at the supplied location.
        PAIMON_RETURN_NOT_OK(catalog_result.status());
        if (format_result.value() != nullptr) {
            return Status::Invalid("the catalog did not return a format table");
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> format_table, std::move(format_result));
    if (format_table) {
        if (load.table_id) {
            return Status::Invalid("a format table has no catalog commit table id");
        }
        return std::shared_ptr<Table>(std::move(format_table));
    }
    if (!schema) {
        return Status::Invalid(fmt::format("not found latest schema in branch {}", branch));
    }
    if (!writing) {
        PAIMON_ASSIGN_OR_RAISE(
            dynamic_options,
            CatalogUtils::ResolveQueryAuthReadOptions(
                load.catalog, identifier, dynamic_options, branch, temporary.GetFileSystem(), path,
                schema_json.has_value() || catalog_schema, operation));
    }
    auto merged = schema->Options();
    for (const auto& [key, value] : dynamic_options) {
        merged[key] = value;
    }
    if (!writing) {
        PAIMON_ASSIGN_OR_RAISE(
            branch, BranchManager::ResolveBranch(load.identifier, merged, std::nullopt, operation));
    }
    if (writing || merged.count(Options::BRANCH) != 0) {
        merged[Options::BRANCH] = branch;
    }
    if (metadata_branch != branch) {
        // A supplied main schema may select another branch for reads. Its metadata manager
        // must not become the schema cache of that other branch.
        schema_manager.reset();
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                           CoreOptions::FromMap(merged, file_system, fs_scheme_map));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(std::make_unique<FileStore>(path, std::move(options)), schema,
                               identifier.value_or(CatalogUtils::IdentifierFromPath(path, branch)),
                               table_id.value_or(""), fs_scheme_map, load.catalog, dynamic_options,
                               schema_manager));
    table->supplied_main_schema_ = load.table_schema_json;
    table->fallback_file_system_ = file_system;
    return std::shared_ptr<Table>(std::move(table));
}

Result<std::unique_ptr<TableRead>> TableRead::Create(
    const TableLoadOptions& load_options, std::unique_ptr<ReadContext> context,
    const std::map<std::string, std::string>& dynamic_options) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "read"));
    if (load_options.table_id && !load_options.catalog) {
        return Status::Invalid("a table id requires a catalog or an external REST commit request");
    }
    if (context->GetRealtimeContext()) {
        PAIMON_ASSIGN_OR_RAISE(std::string branch,
                               BranchManager::ResolveBranch(load_options.identifier,
                                                            dynamic_options, std::nullopt, "read"));
        PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateBranch(branch));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                           TableFactory::Create(load_options, dynamic_options, "read"));
    PAIMON_ASSIGN_OR_RAISE(table, table->Copy(dynamic_options));
    return table->NewRead(std::move(context));
}

Result<std::unique_ptr<TableScan>> TableScan::Create(
    const TableLoadOptions& load_options, std::unique_ptr<ScanContext> context,
    const std::map<std::string, std::string>& dynamic_options) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "scan"));
    if (load_options.table_id && !load_options.catalog) {
        return Status::Invalid("a table id requires a catalog or an external REST commit request");
    }
    if (context->GetRealtimeContext()) {
        PAIMON_ASSIGN_OR_RAISE(std::string branch,
                               BranchManager::ResolveBranch(load_options.identifier,
                                                            dynamic_options, std::nullopt, "scan"));
        PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateBranch(branch));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                           TableFactory::Create(load_options, dynamic_options, "scan"));
    PAIMON_ASSIGN_OR_RAISE(table, table->Copy(dynamic_options));
    return table->NewScan(std::move(context));
}

Result<std::unique_ptr<FileStoreWrite>> FileStoreWrite::Create(
    const TableLoadOptions& load_options, std::unique_ptr<WriteContext> context,
    const std::map<std::string, std::string>& dynamic_options) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "write"));
    if (load_options.table_id && !load_options.catalog) {
        return Status::Invalid("a table id requires a catalog or an external REST commit request");
    }
    if (context->GetRealtimeContext()) {
        PAIMON_ASSIGN_OR_RAISE(std::string branch, BranchManager::ResolveBranch(
                                                       load_options.identifier, dynamic_options,
                                                       std::nullopt, "write"));
        PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateBranch(branch));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                           TableFactory::Create(load_options, dynamic_options, "write"));
    PAIMON_ASSIGN_OR_RAISE(table, table->Copy(dynamic_options));
    return table->NewWrite(std::move(context));
}

Result<std::unique_ptr<FileStoreCommit>> FileStoreCommit::Create(
    const TableLoadOptions& load_options, std::unique_ptr<CommitContext> context,
    const std::map<std::string, std::string>& dynamic_options) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "commit"));
    if (load_options.catalog && context->UseRESTCatalogCommit()) {
        return Status::Invalid(
            "a commit cannot both use a catalog and build an external REST request");
    }
    if (load_options.table_id && !load_options.catalog && !context->UseRESTCatalogCommit()) {
        return Status::Invalid("a table id requires a catalog or an external REST commit request");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                           TableFactory::Create(load_options, dynamic_options, "commit"));
    PAIMON_ASSIGN_OR_RAISE(table, table->Copy(dynamic_options));
    return table->NewCommit(std::move(context));
}

}  // namespace paimon
