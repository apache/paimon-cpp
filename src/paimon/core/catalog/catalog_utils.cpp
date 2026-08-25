/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
#include "paimon/core/options/table_type.h"
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
    const Catalog& catalog, const Identifier& identifier, bool metadata_under_table_path) {
    PAIMON_ASSIGN_OR_RAISE(std::string location, catalog.GetTableLocation(identifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> schema, catalog.LoadTableSchema(identifier));
    std::shared_ptr<DataSchema> data_schema = std::dynamic_pointer_cast<DataSchema>(schema);
    if (data_schema == nullptr) {
        return Status::Invalid(fmt::format("{} is not a data table, so it cannot be a format table",
                                           identifier.GetFullName()));
    }
    return FormatTable::Create(catalog.GetFileSystem(), location, identifier, data_schema,
                               metadata_under_table_path);
}

}  // namespace paimon
