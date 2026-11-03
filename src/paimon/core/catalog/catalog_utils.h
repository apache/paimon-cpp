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

#pragma once

#include <memory>
#include <optional>
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class Catalog;
class FormatTable;
class Schema;

/// Identifier utilities shared by catalog implementations.
class CatalogUtils {
 public:
    CatalogUtils() = delete;
    ~CatalogUtils() = delete;

    /// Derives the database from the parent directory's .db suffix; otherwise leaves it unknown.
    static Identifier IdentifierFromPath(const std::string& table_path, const std::string& branch);

    /// Returns whether `db_name` is the reserved system database "sys".
    static bool IsSystemDatabase(const std::string& db_name);

    /// Fails when `db_name` is the system database.
    static Status CheckNotSystemDatabase(const std::string& db_name, const std::string& action);

    /// Fails when `identifier` denotes a system table or any table of the system database.
    static Status CheckNotSystemTable(const Identifier& identifier, const std::string& action);

    /// Fails when `identifier` carries a "$branch_" suffix.
    static Status CheckNotBranch(const Identifier& identifier, const std::string& action);

    /// Returns the identifier a catalog is told `branch` of the data table of `identifier` by:
    /// the bare data table for the main branch, and the branch object name, as `tbl$branch_dev`,
    /// for any other.
    /// Every call a commit or a write makes to the catalog goes to this one identifier, so that
    /// what is read and what is committed name one object.
    static Result<Identifier> BranchIdentifier(const Identifier& identifier,
                                               const std::string& branch);

    /// Returns an identifier qualified with branch, or nullopt if no identifier was supplied.
    static Result<std::optional<Identifier>> BranchIdentifier(
        const std::optional<Identifier>& identifier, const std::string& branch);

    /// Fails when `db_name` cannot be used as a single path component, which is required to
    /// keep the database path under the warehouse.
    static Status CheckValidDatabaseName(const std::string& db_name);

    /// Fails when any component parsed out of the identifier's table name (data table name,
    /// branch name, system table name) cannot be used as a single path component.
    static Status CheckValidTableName(const Identifier& identifier);

    /// Rejects unsupported managed-table types; system-table schemas pass through.
    /// Format tables must be handled by the caller.
    /// @param action Operation name used in the error message, e.g. Catalog::GetTable.
    static Status CheckManagedTableType(const Identifier& identifier,
                                        const std::shared_ptr<Schema>& schema,
                                        const std::string& action);
};

}  // namespace paimon
