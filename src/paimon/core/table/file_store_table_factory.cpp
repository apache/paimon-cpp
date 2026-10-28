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
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/read_context.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/write_context.h"

namespace paimon {

namespace {

Result<std::shared_ptr<Table>> WithContextOptions(
    const std::shared_ptr<Table>& table, const std::shared_ptr<TableSchema>& schema,
    const std::map<std::string, std::string>& options) {
    if (table == nullptr || schema == nullptr || options.empty()) {
        return table;
    }
    const std::map<std::string, std::string>& schema_options = schema->Options();
    for (const auto& [key, value] : options) {
        auto schema_option = schema_options.find(key);
        if (schema_option == schema_options.end() || schema_option->second != value) {
            return table->Copy(options);
        }
    }
    return table;
}

Result<std::shared_ptr<Table>> TableOfLoadedSchema(
    const std::shared_ptr<FileSystem>& explicit_file_system, const std::string& path,
    const std::string& branch, const std::map<std::string, std::string>& options,
    const std::shared_ptr<TableSchema>& latest_schema, const std::optional<Identifier>& identifier,
    const std::string& uuid, const std::map<std::string, std::string>& fs_scheme_map) {
    if (latest_schema != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(bool is_format_table,
                               TableTypeDefine::IsFormatTable(latest_schema->Options()));
        if (is_format_table) {
            return Status::Invalid(fmt::format(
                "the schema under {} names a format table, which is not loaded from a path: a "
                "catalog hands the table back through GetTable, and its operators come from "
                "table->NewScan/NewRead/NewWrite/NewCommit on it",
                path));
        }
    }
    Identifier table_identifier =
        identifier.value_or(CatalogUtils::IdentifierFromPath(path, branch));
    // The schema was read through a file system the context's options resolved, while the table is
    // served through the one the schema's own options resolve: the store builds its managers over
    // that resolution rather than taking over the reader's.
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(explicit_file_system, path, latest_schema, table_identifier, uuid,
                               branch, /*schema_manager=*/nullptr, /*snapshot_manager=*/nullptr,
                               fs_scheme_map, /*dynamic_options=*/{}));
    return WithContextOptions(table, latest_schema, options);
}

Result<std::shared_ptr<Table>> ManagedTableOf(
    const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<FileSystem>& explicit_file_system, const std::string& path,
    const std::string& branch, const std::map<std::string, std::string>& options,
    const std::optional<std::string>& schema_json, const std::optional<Identifier>& identifier,
    const std::string& uuid, const std::map<std::string, std::string>& fs_scheme_map) {
    SchemaManager schema_manager(file_system, path, branch);
    std::shared_ptr<TableSchema> latest_schema;
    if (branch == BranchManager::DEFAULT_MAIN_BRANCH && schema_json) {
        PAIMON_ASSIGN_OR_RAISE(latest_schema, TableSchema::CreateFromJson(schema_json.value()));
    } else {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest,
                               schema_manager.Latest());
        if (latest) {
            latest_schema = *latest;
        }
    }
    return TableOfLoadedSchema(explicit_file_system, path, branch, options, latest_schema,
                               identifier, uuid, fs_scheme_map);
}

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

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const ScanContext& ctx) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx.GetOptions(), ctx.GetSpecificFileSystem(), {}));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(ctx.GetPath()));
    if (system_table_path) {
        return Status::Invalid(fmt::format(
            "the path {} names a system table, which is not loaded from a path: a catalog "
            "hands the table back through GetTable, and its operators come from "
            "table->NewScan/NewRead on it",
            ctx.GetPath()));
    }
    const std::string branch = BranchManager::NormalizeBranch(tmp_options.GetBranch());
    return ManagedTableOf(tmp_options.GetFileSystem(), ctx.GetSpecificFileSystem(), ctx.GetPath(),
                          branch, ctx.GetOptions(), ctx.GetSpecificTableSchema(),
                          ctx.GetIdentifier(),
                          /*uuid=*/"", /*fs_scheme_map=*/{});
}

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const ReadContext& ctx) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_core_options,
                           CoreOptions::FromMap(ctx.GetOptions(), ctx.GetSpecificFileSystem(),
                                                ctx.GetFileSystemSchemeToIdentifierMap()));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(ctx.GetPath()));
    if (system_table_path) {
        return Status::Invalid(fmt::format(
            "the path {} names a system table, which is not loaded from a path: a catalog "
            "hands the table back through GetTable, and its operators come from "
            "table->NewScan/NewRead on it",
            ctx.GetPath()));
    }

    const std::string branch = ctx.GetBranch();
    return ManagedTableOf(tmp_core_options.GetFileSystem(), ctx.GetSpecificFileSystem(),
                          ctx.GetPath(), branch, ctx.GetOptions(), ctx.GetSpecificTableSchema(),
                          ctx.GetIdentifier(), /*uuid=*/"",
                          ctx.GetFileSystemSchemeToIdentifierMap());
}

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const WriteContext& ctx) {
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx.GetCatalog()));
    std::shared_ptr<FileSystem> specific_fs = ctx.GetSpecificFileSystem();
    if (specific_fs == nullptr && ctx.GetFileSystemSchemeToIdentifierMap().empty() &&
        ctx.GetCatalog() != nullptr) {
        if (!ctx.GetIdentifier()) {
            return Status::Invalid("a catalog write requires a table identifier");
        }
        PAIMON_ASSIGN_OR_RAISE(specific_fs,
                               ctx.GetCatalog()->GetTableFileSystem(ctx.GetIdentifier().value()));
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx.GetOptions(), specific_fs,
                                                ctx.GetFileSystemSchemeToIdentifierMap()));
    std::string branch = ctx.GetBranch();
    PAIMON_ASSIGN_OR_RAISE(std::optional<Identifier> catalog_identifier,
                           CatalogUtils::BranchIdentifier(ctx.GetIdentifier(), branch));
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<std::string> catalog_table_schema,
        CatalogMainBranchSchemaJson(ctx.GetCatalog(), catalog_identifier, branch, "write"));
    return ManagedTableOf(tmp_options.GetFileSystem(), specific_fs, ctx.GetRootPath(), branch,
                          ctx.GetOptions(), catalog_table_schema, ctx.GetIdentifier(), /*uuid=*/"",
                          ctx.GetFileSystemSchemeToIdentifierMap());
}

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const CommitContext& ctx) {
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx.GetCatalog()));
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
    PAIMON_ASSIGN_OR_RAISE(std::string branch, BranchManager::ResolveBranch(
                                                   ctx.GetIdentifier(), ctx.GetOptions(),
                                                   /*explicit_branch=*/std::nullopt, "commit"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<Identifier> catalog_identifier,
                           CatalogUtils::BranchIdentifier(ctx.GetIdentifier(), branch));
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<std::string> catalog_table_schema,
        CatalogMainBranchSchemaJson(ctx.GetCatalog(), catalog_identifier, branch, "commit"));
    return ManagedTableOf(tmp_options.GetFileSystem(), specific_fs, root_path, branch,
                          ctx.GetOptions(), catalog_table_schema, ctx.GetIdentifier(),
                          ctx.GetTableId().value_or(""), /*fs_scheme_map=*/{});
}

}  // namespace paimon
