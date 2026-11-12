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

#include <map>
#include <memory>
#include <optional>
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class Catalog;
class FileSystem;
class FormatTable;
class Schema;

/// Checks shared by the catalog implementations. Every check takes an `action` naming
/// the rejected operation in the error message, e.g. "dropTable".
class CatalogUtils {
 public:
    CatalogUtils() = delete;
    ~CatalogUtils() = delete;

    /// Derives an identifier for a table loaded directly from a path.
    static Identifier IdentifierFromPath(const std::string& path, const std::string& branch);
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

    /// Returns `identifier` naming `branch`, the branch resolved for an operation on it, so that
    /// every catalog request of the operation goes to that branch: an identifier of the system
    /// database or one already naming a branch is returned as it is, and any other keeps its
    /// system table suffix, as `tbl$branch_dev$files`.
    static Result<Identifier> ResolvedBranchIdentifier(const Identifier& identifier,
                                                       const std::string& branch);

    /// Compares scheme, authority and normalized path, treating a missing scheme as "file".
    static Result<bool> SameTableLocation(const std::string& left, const std::string& right);

    /// Fails unless `path` is `location`, where the catalog keeps `identifier`, so the query auth
    /// settings the catalog serves for one table are never applied to the files of another.
    /// @param location The location the catalog returned for `identifier`, or the error it failed
    ///        with.
    /// @param operation The operation reading `path`, as "scan", named in the error message.
    static Status CheckPathIsTableLocation(const Identifier& identifier,
                                           const Result<std::string>& location,
                                           const std::string& path, const std::string& operation);

    /// Preserves current query-auth settings when a caller supplies a cached schema.
    static Result<std::map<std::string, std::string>> ResolveQueryAuthReadOptions(
        const std::shared_ptr<Catalog>& catalog, const std::optional<Identifier>& identifier,
        const std::map<std::string, std::string>& options, const std::string& branch,
        const std::shared_ptr<FileSystem>& file_system, const std::string& path,
        bool has_specific_table_schema, const std::string& operation);

    /// Fails when `db_name` cannot be used as a single path component, which is required to
    /// keep the database path under the warehouse.
    static Status CheckValidDatabaseName(const std::string& db_name);

    /// Fails when any component parsed out of the identifier's table name (data table name,
    /// branch name, system table name) cannot be used as a single path component.
    static Status CheckValidTableName(const Identifier& identifier);

    /// Fails when `schema` is not a table a `Table` can describe.
    ///
    /// Only a managed table is one: a format table is loaded through `GetFormatTable()`, and the
    /// remaining table types are stored differently and are not implemented here, so handing one
    /// back as a `Table` would promise snapshots it never had. A schema that is not a data
    /// table's, such as a system table's, passes.
    ///
    /// @param action The call being refused, qualified as a caller would write it, e.g.
    ///        `Catalog::GetTable`. It names the entry point in the message.
    static Status CheckManagedTableType(const Identifier& identifier,
                                        const std::shared_ptr<Schema>& schema,
                                        const std::string& action);

    /// Loads `identifier` as a format table by reading its location and its schema separately.
    ///
    /// What every catalog can do, so it is what `Catalog::GetFormatTable()` falls back to. A
    /// catalog that can get both from one response should not use it, since two requests can
    /// disagree.
    ///
    /// @param location_carries_paimon_metadata Whether this catalog put the table's own metadata
    ///        under the table path, which is what tells a `schema` or `branch` directory below the
    ///        location from a partition directory of the same name. See
    ///        `FormatTable::LocationCarriesPaimonMetadata()`.
    static Result<std::shared_ptr<FormatTable>> LoadFormatTableInTwoRequests(
        const Catalog& catalog, const Identifier& identifier,
        bool location_carries_paimon_metadata);
};

}  // namespace paimon
