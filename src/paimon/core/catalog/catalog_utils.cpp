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

#include "paimon/core/catalog/catalog_utils.h"

#include <map>
#include <memory>
#include <optional>
#include <string>

#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/catalog/query_auth_catalog.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/result.h"
#include "paimon/schema/schema.h"
#include "paimon/table/format/format_table.h"

namespace paimon {

namespace {

Status SystemTableError(const Identifier& identifier, const std::string& action) {
    return Status::Invalid(fmt::format("Cannot '{}' for system table '{}', please use data table.",
                                       action, identifier.ToString()));
}

bool SameLocation(const Path& left, const Path& right) {
    auto scheme = [](const Path& path) {
        std::string lower = StringUtils::ToLowerCase(path.scheme);
        return lower.empty() ? std::string("file") : lower;
    };
    return scheme(left) == scheme(right) && left.authority == right.authority &&
           left.path == right.path;
}

}  // namespace

bool CatalogUtils::IsSystemDatabase(const std::string& db_name) {
    return db_name == Catalog::SYSTEM_DATABASE_NAME;
}

Status CatalogUtils::CheckNotSystemDatabase(const std::string& db_name, const std::string& action) {
    if (IsSystemDatabase(db_name)) {
        return Status::Invalid(
            fmt::format("Cannot '{}' for system database '{}'.", action, db_name));
    }
    return Status::OK();
}

Status CatalogUtils::CheckNotSystemTable(const Identifier& identifier, const std::string& action) {
    // The system database is checked first so that an identifier of "sys" is rejected
    // without being parsed as a table name.
    if (IsSystemDatabase(identifier.GetDatabaseName())) {
        return SystemTableError(identifier, action);
    }
    PAIMON_ASSIGN_OR_RAISE(bool is_system_table, identifier.IsSystemTable());
    if (is_system_table) {
        return SystemTableError(identifier, action);
    }
    return Status::OK();
}

Result<Identifier> CatalogUtils::BranchIdentifier(const Identifier& identifier,
                                                  const std::string& branch) {
    PAIMON_ASSIGN_OR_RAISE(std::string data_table_name, identifier.GetDataTableName());
    return Identifier(identifier.GetDatabaseName(), data_table_name, branch);
}

Result<Identifier> CatalogUtils::ResolvedBranchIdentifier(const Identifier& identifier,
                                                          const std::string& branch) {
    if (IsSystemDatabase(identifier.GetDatabaseName())) {
        return identifier;
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> identifier_branch,
                           identifier.GetBranchName());
    if (identifier_branch) {
        return identifier;
    }
    PAIMON_ASSIGN_OR_RAISE(Identifier branch_identifier, BranchIdentifier(identifier, branch));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> system_table,
                           identifier.GetSystemTableName());
    if (!system_table) {
        return branch_identifier;
    }
    return Identifier(branch_identifier.GetDatabaseName(),
                      fmt::format("{}{}{}", branch_identifier.GetObjectName(),
                                  Identifier::kSystemTableSplitter, system_table.value()));
}

Result<bool> CatalogUtils::SameTableLocation(const std::string& left, const std::string& right) {
    PAIMON_ASSIGN_OR_RAISE(Path left_path, PathUtil::ToPath(left));
    PAIMON_ASSIGN_OR_RAISE(Path right_path, PathUtil::ToPath(right));
    return SameLocation(left_path, right_path);
}

Status CatalogUtils::CheckPathIsTableLocation(const Identifier& identifier,
                                              const Result<std::string>& location,
                                              const std::string& path,
                                              const std::string& operation) {
    if (!location.ok()) {
        return location.status().WithMessage(fmt::format(
            "Cannot check that {} is the location of table {}, whose query auth settings the {} "
            "applies: {}",
            path, identifier.GetFullName(), operation, location.status().message()));
    }
    PAIMON_ASSIGN_OR_RAISE(bool same_location, SameTableLocation(location.value(), path));
    if (!same_location) {
        return Status::Invalid(fmt::format(
            "The {} targets {}, but the catalog keeps table {}, whose query auth settings it would "
            "apply, at {}. Use the location the catalog returns for the table.",
            operation, path, identifier.GetFullName(), location.value()));
    }
    return Status::OK();
}

Status CatalogUtils::CheckNotBranch(const Identifier& identifier, const std::string& action) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> branch, identifier.GetBranchName());
    if (branch) {
        return Status::Invalid(fmt::format(
            "Cannot '{}' for branch table '{}', please modify the table with the default branch.",
            action, identifier.ToString()));
    }
    return Status::OK();
}

Status CatalogUtils::CheckValidDatabaseName(const std::string& db_name) {
    return PathUtil::CheckSinglePathComponent("database", db_name);
}

Status CatalogUtils::CheckValidTableName(const Identifier& identifier) {
    PAIMON_ASSIGN_OR_RAISE(std::string data_table_name, identifier.GetDataTableName());
    PAIMON_RETURN_NOT_OK(PathUtil::CheckSinglePathComponent("table", data_table_name));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> branch, identifier.GetBranchName());
    if (branch) {
        // The branch of an identifier selects the same directory as the `branch` option, so both
        // go through the same check.
        PAIMON_RETURN_NOT_OK(BranchManager::CheckValidBranch(branch.value()));
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> system_table,
                           identifier.GetSystemTableName());
    if (system_table) {
        PAIMON_RETURN_NOT_OK(
            PathUtil::CheckSinglePathComponent("system table", system_table.value()));
    }
    return Status::OK();
}

Status CatalogUtils::CheckManagedTableType(const Identifier& identifier,
                                           const std::shared_ptr<Schema>& schema,
                                           const std::string& action) {
    std::shared_ptr<DataSchema> data_schema = std::dynamic_pointer_cast<DataSchema>(schema);
    if (data_schema == nullptr) {
        // Only a data table carries table options, so nothing else declares a table type.
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(TableType table_type,
                           TableTypeDefine::FromOptions(data_schema->Options()));
    if (table_type == TableType::FORMAT_TABLE) {
        return Status::Invalid(
            fmt::format("Cannot open format table '{}' as a Table in '{}', please use "
                        "'Catalog::GetFormatTable' or 'FormatTable::Create'.",
                        identifier.ToString(), action));
    }
    // A materialized table is a managed table that also carries the SQL it materializes.
    if (table_type != TableType::TABLE && table_type != TableType::MATERIALIZED_TABLE) {
        const std::map<std::string, std::string>& options = data_schema->Options();
        auto type_iter = options.find(Options::TYPE);
        return Status::NotImplemented(fmt::format(
            "Cannot open table '{}' in '{}': its '{}' is '{}', a table type paimon-cpp does not "
            "implement, and a managed table would promise snapshots it never had.",
            identifier.ToString(), action, Options::TYPE,
            type_iter == options.end() ? std::string() : type_iter->second));
    }
    return Status::OK();
}

Result<std::shared_ptr<FormatTable>> CatalogUtils::LoadFormatTableInTwoRequests(
    const Catalog& catalog, const Identifier& identifier, bool location_carries_paimon_metadata) {
    PAIMON_ASSIGN_OR_RAISE(std::string location, catalog.GetTableLocation(identifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> schema, catalog.LoadTableSchema(identifier));
    std::shared_ptr<DataSchema> data_schema = std::dynamic_pointer_cast<DataSchema>(schema);
    if (data_schema == nullptr) {
        return Status::Invalid(fmt::format("{} is not a data table, so it cannot be a format table",
                                           identifier.GetFullName()));
    }
    return FormatTable::Create(catalog.GetFileSystem(), location, identifier, data_schema,
                               location_carries_paimon_metadata, /*dynamic_options=*/{});
}

Identifier CatalogUtils::IdentifierFromPath(const std::string& table_path,
                                            const std::string& branch) {
    const std::string name = PathUtil::GetName(table_path);
    const std::string parent = PathUtil::GetName(PathUtil::GetParentDirPath(table_path));
    const std::string suffix = Catalog::DB_SUFFIX;
    if (StringUtils::EndsWith(parent, suffix)) {
        return Identifier(parent.substr(0, parent.size() - suffix.size()), name, branch);
    }
    return Identifier(Identifier::kUnknownDatabase, name, branch);
}

/// Returns `options` with `query-auth.enabled` turned on when the current settings of `branch`
/// enable it: the options of the schema the catalog holds for that branch, as Java loads the table
/// of the branch it reads, or, when the caller hands in a cached schema, those of the latest schema
/// of the branch under `path`, so a stale cache cannot skip the rules. A system table path naming
/// a branch, as `tbl$branch_dev$files`, reads that branch, so its settings are the ones looked up.
/// @param operation The operation reading `path`, as "scan", named in the error messages.
Result<std::map<std::string, std::string>> CatalogUtils::ResolveQueryAuthReadOptions(
    const std::shared_ptr<Catalog>& catalog, const std::optional<Identifier>& identifier,
    const std::map<std::string, std::string>& options, const std::string& branch,
    const std::shared_ptr<FileSystem>& file_system, const std::string& path,
    bool has_specific_table_schema, const std::string& operation) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(path));
    std::string read_branch = branch;
    std::string table_path = path;
    if (system_table_path && !system_table_path->is_global) {
        table_path = system_table_path->table_path;
        if (system_table_path->branch) {
            // Resolved as `SystemTableLoader::LoadFromPath()` resolves the branch it loads.
            std::map<std::string, std::string> named_branch;
            if (!BranchManager::IsMainBranch(branch)) {
                named_branch[Options::BRANCH] = branch;
            }
            PAIMON_ASSIGN_OR_RAISE(read_branch,
                                   BranchManager::ResolveBranch(
                                       /*identifier=*/std::nullopt, named_branch,
                                       system_table_path->branch, "system table read"));
        }
    }
    bool enabled = false;
    if (catalog != nullptr) {
        PAIMON_RETURN_NOT_OK(BranchManager::CheckCatalogAddressableBranch(read_branch));
        if (!identifier) {
            return Status::Invalid("a catalog read requires a table identifier");
        }
        if (CatalogUtils::IsSystemDatabase(identifier.value().GetDatabaseName())) {
            // Only a global system table has no data table policy to load from the catalog.
            if (!system_table_path || !system_table_path->is_global) {
                return Status::Invalid(fmt::format(
                    "Catalog identifier {} names a global system table, but the {} targets {}. "
                    "Use the identifier of the data table being read.",
                    identifier.value().ToString(), operation, path));
            }
        } else {
            PAIMON_ASSIGN_OR_RAISE(Identifier table_identifier,
                                   CatalogUtils::BranchIdentifier(identifier.value(), read_branch));
            // A catalog that serves query auth hands the location back with the schema, so the
            // two cannot come from different generations of a recreated table.
            const QueryAuthCatalog* auth_catalog = AsQueryAuthCatalog(catalog);
            std::shared_ptr<Schema> schema;
            std::optional<std::string> location;
            if (auth_catalog != nullptr) {
                PAIMON_ASSIGN_OR_RAISE(schema, auth_catalog->LoadTableSchemaAndLocation(
                                                   table_identifier, &location.emplace()));
            } else {
                PAIMON_ASSIGN_OR_RAISE(schema, catalog->LoadTableSchema(table_identifier));
            }
            std::shared_ptr<DataSchema> data_schema = std::dynamic_pointer_cast<DataSchema>(schema);
            if (data_schema == nullptr) {
                return Status::Invalid(
                    fmt::format("the catalog did not hand back a data table schema for {}",
                                table_identifier.ToString()));
            }
            PAIMON_ASSIGN_OR_RAISE(
                enabled, SchemaValidation::QueryAuthReadEnabled(data_schema->Options(), options));
            // An enabled setting needs no check here: the query auth scan checks the location and
            // the read takes only its splits. A disabled one would leave another table unguarded.
            if (!enabled) {
                PAIMON_RETURN_NOT_OK(CatalogUtils::CheckPathIsTableLocation(
                    table_identifier,
                    location ? Result<std::string>(location.value())
                             : catalog->GetTableLocation(table_identifier),
                    table_path, operation));
            }
        }
    }
    if (!enabled && has_specific_table_schema && !system_table_path) {
        SchemaManager schema_manager(file_system, path, read_branch);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                               schema_manager.Latest());
        if (latest_schema) {
            PAIMON_ASSIGN_OR_RAISE(enabled, SchemaValidation::QueryAuthReadEnabled(
                                                latest_schema.value()->Options(), options));
        }
    }
    if (!enabled) {
        return options;
    }
    std::map<std::string, std::string> read_options = options;
    read_options[Options::QUERY_AUTH_ENABLED] = "true";
    return read_options;
}

}  // namespace paimon
