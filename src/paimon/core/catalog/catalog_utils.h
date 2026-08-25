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

#pragma once

#include <memory>
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class Catalog;
class FormatTable;
class Schema;

/// Checks shared by the catalog implementations. Every check takes an `action` naming
/// the rejected operation in the error message, e.g. "dropTable".
class CatalogUtils {
 public:
    CatalogUtils() = delete;
    ~CatalogUtils() = delete;

    /// Returns whether `db_name` is the reserved system database "sys".
    static bool IsSystemDatabase(const std::string& db_name);

    /// Fails when `db_name` is the system database.
    static Status CheckNotSystemDatabase(const std::string& db_name, const std::string& action);

    /// Fails when `identifier` denotes a system table or any table of the system database.
    static Status CheckNotSystemTable(const Identifier& identifier, const std::string& action);

    /// Fails when `identifier` carries a "$branch_" suffix.
    static Status CheckNotBranch(const Identifier& identifier, const std::string& action);

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
    /// @param metadata_under_table_path Whether this catalog put the table's own metadata under
    ///        the table path, which is what tells a `schema` or `branch` directory below the
    ///        location from a partition directory of the same name.
    static Result<std::shared_ptr<FormatTable>> LoadFormatTableInTwoRequests(
        const Catalog& catalog, const Identifier& identifier, bool metadata_under_table_path);
};

}  // namespace paimon
