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
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/context_utils.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/read_context.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/write_context.h"

namespace paimon {

namespace {

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
    if (latest_schema == nullptr) {
        return Status::Invalid(fmt::format("not found latest schema in branch {}",
                                           BranchManager::NormalizeBranch(branch)));
    }
    PAIMON_ASSIGN_OR_RAISE(bool is_format_table,
                           TableTypeDefine::IsFormatTable(latest_schema->Options()));
    if (is_format_table) {
        return Status::Invalid(fmt::format(
            "the schema under {} names a format table, which is not loaded from a path: a "
            "catalog hands the table back through GetTable, and its operators come from "
            "table->NewScan/NewRead/NewWrite/NewCommit on it",
            path));
    }
    Identifier table_identifier =
        identifier.value_or(CatalogUtils::IdentifierFromPath(path, branch));

    std::map<std::string, std::string> dynamic_options = options;
    dynamic_options.erase(Options::TYPE);
    dynamic_options.erase(Options::BRANCH);
    PAIMON_RETURN_NOT_OK(SchemaValidation::CheckImmutableOptions(*latest_schema, dynamic_options));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> merged_schema,
                           latest_schema->MergeOptions(dynamic_options));
    return FileStoreTable::Create(explicit_file_system, path,
                                  std::shared_ptr<TableSchema>(std::move(merged_schema)),
                                  table_identifier, uuid, branch, fs_scheme_map, dynamic_options);
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

template <typename Context>
Result<std::shared_ptr<Table>> ManagedTable(const Context& ctx,
                                            const std::map<std::string, std::string>& options,
                                            const char* kind, bool catalog_write) {
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx.GetCatalog()));
    if (ctx.GetCatalog() && !ctx.GetIdentifier()) {
        return Status::Invalid(fmt::format("a catalog {} requires a table identifier", kind));
    }
    PAIMON_ASSIGN_OR_RAISE(std::string branch,
                           BranchManager::ResolveBranch(ctx.GetIdentifier(), options, kind));
    if (catalog_write && ctx.GetCatalog() && !BranchManager::IsMainBranch(branch)) {
        PAIMON_RETURN_NOT_OK(BranchManager::CheckCatalogAddressableBranch(branch));
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> identifier_branch,
                               ctx.GetIdentifier()->GetBranchName());
        if (!identifier_branch) {
            return Status::Invalid(
                fmt::format("a {} through a catalog addresses a branch by the table identifier; "
                            "name branch '{}' in the identifier",
                            kind, branch));
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> file_system,
                           ContextUtils::ResolveFileSystemForLoad(ctx));
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions tmp_options,
        CoreOptions::FromMap(options, file_system, ctx.GetFileSystemSchemeToIdentifierMap()));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(ctx.GetPath()));
    if (system_table_path) {
        return Status::Invalid(fmt::format(
            "the path {} names a system table, which is not loaded from a path: a catalog "
            "hands the table back through GetTable, and its operators come from "
            "table->NewScan/NewRead/NewWrite/NewCommit on it",
            ctx.GetPath()));
    }
    std::optional<std::string> catalog_table_schema = ctx.GetSpecificTableSchema();
    if (!catalog_table_schema) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<Identifier> catalog_identifier,
                               CatalogUtils::BranchIdentifier(ctx.GetIdentifier(), branch));
        PAIMON_ASSIGN_OR_RAISE(
            catalog_table_schema,
            CatalogMainBranchSchemaJson(ctx.GetCatalog(), catalog_identifier, branch, kind));
    }
    return ManagedTableOf(tmp_options.GetFileSystem(), file_system, ctx.GetPath(), branch, options,
                          catalog_table_schema, ctx.GetIdentifier(), ctx.GetTableId().value_or(""),
                          ctx.GetFileSystemSchemeToIdentifierMap());
}

}  // namespace

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(
    const ScanContext& ctx, const std::map<std::string, std::string>& options) {
    return ManagedTable(ctx, options, "scan", false);
}

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(
    const ReadContext& ctx, const std::map<std::string, std::string>& options) {
    return ManagedTable(ctx, options, "read", false);
}

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(
    const WriteContext& ctx, const std::map<std::string, std::string>& options) {
    return ManagedTable(ctx, options, "write", true);
}

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(
    const CommitContext& ctx, const std::map<std::string, std::string>& options) {
    return ManagedTable(ctx, options, "commit", true);
}

}  // namespace paimon
