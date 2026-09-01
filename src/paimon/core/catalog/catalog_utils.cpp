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

#include <optional>

#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/result.h"

namespace paimon {

namespace {

Status SystemTableError(const Identifier& identifier, const std::string& action) {
    return Status::Invalid(fmt::format("Cannot '{}' for system table '{}', please use data table.",
                                       action, identifier.ToString()));
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

}  // namespace paimon
