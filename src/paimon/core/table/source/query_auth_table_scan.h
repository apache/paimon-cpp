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

#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "paimon/catalog/catalog.h"
#include "paimon/core/catalog/table_query_auth_result.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_scan.h"

namespace paimon {
class Metrics;
class QueryAuthCatalog;

/// Plans a table with `query-auth.enabled=true`, mirroring Java `AbstractDataTableScan.authQuery`:
/// every plan loads and checks the rules and wraps each split in a `QueryAuthSplit`. Only the row
/// filter and the predicate conjuncts on unmasked columns are pushed down, and the limit only when
/// the predicate has no conjunct on a masked column.
class QueryAuthTableScan : public TableScan {
 public:
    /// Builds the scan the rules are applied around from the context it is given.
    using InnerScanFactory =
        std::function<Result<std::unique_ptr<TableScan>>(const std::shared_ptr<ScanContext>&)>;

    /// @param context The scan as the caller asked for it; it must name the catalog and the table.
    static Result<std::unique_ptr<TableScan>> Create(
        const std::shared_ptr<ScanContext>& context,
        const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<FileSystem>& file_system, const std::string& branch,
        InnerScanFactory create_inner);

    Result<std::shared_ptr<Plan>> CreatePlan() override;

    std::shared_ptr<Metrics> GetMetrics() const override;

 private:
    QueryAuthTableScan(const std::shared_ptr<ScanContext>& context,
                       const QueryAuthCatalog* auth_catalog, Identifier auth_identifier,
                       const std::shared_ptr<TableSchema>& table_schema,
                       const std::shared_ptr<FileSystem>& file_system, const std::string& branch,
                       InnerScanFactory create_inner,
                       std::optional<std::vector<std::string>> authorization_columns,
                       std::set<std::string> user_filter_fields,
                       std::set<std::string> partition_filter_fields);

    /// Adds the table, branch and schema of this scan to the message of `status`.
    Status AddErrorContext(const Status& status) const;

    /// Checks both the latest published schema and the catalog's current schema. Fails when a
    /// column the catalog is asked to authorize is another field than the scan plans with, when
    /// `rules` do not fit either schema, when they reference a column the planned schema does
    /// not hold or holds as another field, or when the catalog no longer keeps the table at the
    /// scan path.
    Status ValidateAgainstCurrentSchemas(const TableQueryAuthResult& rules) const;

    /// Asks the catalog for the rules of this query and checks them with
    /// `ValidateAgainstCurrentSchemas()`.
    Result<std::shared_ptr<TableQueryAuthResult>> LoadAndValidateRules() const;

    /// Builds the scan the rules are applied around, with what `rules` let it push down.
    Result<std::unique_ptr<TableScan>> CreateInnerScan(const TableQueryAuthResult& rules) const;

    /// Fails when `rules` would need pruning the inner scan already pushed down to be undone.
    Status ValidatePushdownCompatibility(const TableQueryAuthResult& rules) const;

    /// Holds the catalog `auth_catalog_` points to.
    std::shared_ptr<ScanContext> context_;
    const QueryAuthCatalog* auth_catalog_;
    /// The table the rules are asked for, naming the branch planned.
    Identifier auth_identifier_;
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<FileSystem> file_system_;
    std::string branch_;
    InnerScanFactory create_inner_;
    /// The columns the catalog is asked to authorize: the projection plus the filter operands, or
    /// null for every column.
    std::optional<std::vector<std::string>> authorization_columns_;
    std::set<std::string> user_filter_fields_;
    std::set<std::string> partition_filter_fields_;
    /// Built by the first plan, from the rules it loaded.
    std::unique_ptr<TableScan> inner_;
    std::shared_ptr<TableQueryAuthResult> pushed_rules_;
    std::shared_ptr<Logger> logger_ = Logger::GetLogger("QueryAuthTableScan");
};

}  // namespace paimon
