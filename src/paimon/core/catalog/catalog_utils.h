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

/// The identifier tools and the checks shared by the catalog implementations. Every check takes an
/// `action` naming the rejected operation in the error message, e.g. "dropTable".
class CatalogUtils {
 public:
    CatalogUtils() = delete;
    ~CatalogUtils() = delete;

    /// The identifier a table path names when no catalog name goes with it, the C++ reading of the
    /// Java `SchemaManagerUtils#identifierFromPath` over `ignoreIfUnknownDatabase = true`: the
    /// directory above the table's names the database when it carries the `.db` suffix a catalog
    /// lays its databases out with, and a path that names no database leaves it unknown.
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

    /// The identifier a catalog knows the table by, on `branch`, when there is one to fold a
    /// branch into: a caller without an identifier has none to ask the catalog about, and gets
    /// none back rather than an error.
    static Result<std::optional<Identifier>> BranchIdentifier(
        const std::optional<Identifier>& identifier, const std::string& branch);

    /// Fails when `db_name` cannot be used as a single path component, which is required to
    /// keep the database path under the warehouse.
    static Status CheckValidDatabaseName(const std::string& db_name);

    /// Fails when any component parsed out of the identifier's table name (data table name,
    /// branch name, system table name) cannot be used as a single path component.
    static Status CheckValidTableName(const Identifier& identifier);

    /// Fails when `schema` is not a table a managed `Table` can describe.
    ///
    /// Only a managed table is one: the remaining table types are stored differently and are
    /// not implemented here, so handing one back as a `Table` would promise snapshots it never
    /// had. A schema that is not a data table's, such as a system table's, passes. A format
    /// table is served by the caller - `GetTable()` hands one back as the `FormatTable` it is -
    /// so its schema only reaches this check by mistake and falls to the refusal below.
    ///
    /// @param action The call being refused, qualified as a caller would write it, e.g.
    ///        `Catalog::GetTable`. It names the entry point in the message.
    static Status CheckManagedTableType(const Identifier& identifier,
                                        const std::shared_ptr<Schema>& schema,
                                        const std::string& action);
};

}  // namespace paimon
