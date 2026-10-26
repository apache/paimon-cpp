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
#include <vector>

#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/result.h"
#include "paimon/schema/schema.h"

namespace paimon {

namespace {

Status SystemTableError(const Identifier& identifier, const std::string& action) {
    return Status::Invalid(fmt::format("Cannot '{}' for system table '{}', please use data table.",
                                       action, identifier.ToString()));
}

}  // namespace

Identifier CatalogUtils::IdentifierFromPath(const std::string& table_path,
                                            const std::string& branch) {
    const std::vector<std::string> paths = StringUtils::Split(table_path, "/");
    if (paths.size() < 2) {
        return Identifier(Identifier::kUnknownDatabase,
                          paths.empty() ? std::string() : paths.front());
    }
    const std::string& database = paths[paths.size() - 2];
    const std::string::size_type index = database.rfind(Catalog::DB_SUFFIX);
    if (index == std::string::npos) {
        return Identifier(Identifier::kUnknownDatabase, paths.back(), branch);
    }
    return Identifier(database.substr(0, index), paths.back(), branch);
}

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

Result<std::optional<Identifier>> CatalogUtils::BranchIdentifier(
    const std::optional<Identifier>& identifier, const std::string& branch) {
    if (!identifier) {
        return std::optional<Identifier>();
    }
    PAIMON_ASSIGN_OR_RAISE(Identifier branch_identifier,
                           BranchIdentifier(identifier.value(), branch));
    return std::optional<Identifier>(std::move(branch_identifier));
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

}  // namespace paimon
