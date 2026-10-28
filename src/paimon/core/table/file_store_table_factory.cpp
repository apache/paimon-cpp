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
    // Fold the context options into the schema before the store resolves them: resolving the
    // schema's own options first would fail on a format plugin the context overrides away,
    // before the override has a chance to take effect. Only options that differ from the
    // schema's own restyle the load, the way `Copy` decides; the rest leave the schema and the
    // dynamic options as they are.
    std::shared_ptr<Schema> loaded_schema = latest_schema;
    std::map<std::string, std::string> dynamic_options;
    if (latest_schema != nullptr && !options.empty()) {
        const std::map<std::string, std::string>& schema_options = latest_schema->Options();
        bool options_differ = false;
        for (const auto& [key, value] : options) {
            auto schema_option = schema_options.find(key);
            if (schema_option == schema_options.end() || schema_option->second != value) {
                options_differ = true;
                break;
            }
        }
        if (options_differ) {
            dynamic_options = options;
            dynamic_options.erase(Options::TYPE);
            dynamic_options.erase(Options::BRANCH);
            PAIMON_RETURN_NOT_OK(
                SchemaValidation::CheckImmutableOptions(*latest_schema, dynamic_options));
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> merged_schema,
                                   latest_schema->MergeOptions(dynamic_options));
            loaded_schema = std::shared_ptr<TableSchema>(std::move(merged_schema));
        }
    }
    // The schema was read through a file system the context's options resolved, while the table is
    // served through the one the schema's own options resolve: the store builds its managers over
    // that resolution rather than taking over the reader's.
    return FileStoreTable::Create(explicit_file_system, path, loaded_schema, table_identifier, uuid,
                                  branch, /*schema_manager=*/nullptr,
                                  /*snapshot_manager=*/nullptr, fs_scheme_map, dynamic_options);
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
Result<std::shared_ptr<Table>> ManagedTable(const Context& ctx, const char* kind) {
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx.GetCatalog()));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                           CoreOptions::FromMap(ctx.GetOptions(), ctx.GetSpecificFileSystem(),
                                                ctx.GetFileSystemSchemeToIdentifierMap()));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(ctx.GetPath()));
    if (system_table_path) {
        return Status::Invalid(fmt::format(
            "the path {} names a system table, which is not loaded from a path: a catalog "
            "hands the table back through GetTable, and its operators come from "
            "table->NewScan/NewRead/NewWrite/NewCommit on it",
            ctx.GetPath()));
    }
    // A schema the caller gave replaces the catalog's; the caller answers for its correctness.
    std::optional<std::string> catalog_table_schema = ctx.GetSpecificTableSchema();
    if (!catalog_table_schema) {
        PAIMON_ASSIGN_OR_RAISE(
            std::optional<Identifier> catalog_identifier,
            CatalogUtils::BranchIdentifier(ctx.GetIdentifier(), ctx.GetBranch()));
        PAIMON_ASSIGN_OR_RAISE(catalog_table_schema,
                               CatalogMainBranchSchemaJson(ctx.GetCatalog(), catalog_identifier,
                                                           ctx.GetBranch(), kind));
    }
    return ManagedTableOf(tmp_options.GetFileSystem(), ctx.GetSpecificFileSystem(), ctx.GetPath(),
                          ctx.GetBranch(), ctx.GetOptions(), catalog_table_schema,
                          ctx.GetIdentifier(), ctx.GetTableId().value_or(""),
                          ctx.GetFileSystemSchemeToIdentifierMap());
}

}  // namespace

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const ScanContext& ctx) {
    return ManagedTable(ctx, "scan");
}

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const ReadContext& ctx) {
    return ManagedTable(ctx, "read");
}

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const WriteContext& ctx) {
    return ManagedTable(ctx, "write");
}

Result<std::shared_ptr<Table>> FileStoreTableFactory::Create(const CommitContext& ctx) {
    return ManagedTable(ctx, "commit");
}

}  // namespace paimon
