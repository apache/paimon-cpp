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
#include <vector>

#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/core/catalog/table_query_auth_result.h"
#include "paimon/result.h"

namespace paimon {

/// Serves the row filters and column masks of tables with `query-auth.enabled=true`, mirroring
/// Java `Catalog.authTableQuery`.
///
/// Kept internal because `TableQueryAuthResult` is not an installed API type.
class QueryAuthCatalog {
 public:
    virtual ~QueryAuthCatalog() = default;

    /// Loads the rules one query of a table is subject to.
    ///
    /// @param identifier The table the query reads, a branch of it included. A system table is
    ///        refused.
    /// @param select The columns the query reads, or null for all of them.
    /// @return The parsed rules; a rule this library cannot apply fails the call.
    virtual Result<std::shared_ptr<TableQueryAuthResult>> AuthTableQuery(
        const Identifier& identifier,
        const std::optional<std::vector<std::string>>& select) const = 0;
};

/// Returns the catalog as a `QueryAuthCatalog`, or null when it does not support the interface.
inline const QueryAuthCatalog* AsQueryAuthCatalog(const std::shared_ptr<Catalog>& catalog) {
    return dynamic_cast<const QueryAuthCatalog*>(catalog.get());
}

}  // namespace paimon
