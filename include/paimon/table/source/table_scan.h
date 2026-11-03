/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/macros.h"
#include "paimon/result.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/scan_metrics.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class ScanContext;

/// A scanner interface for reading table's meta and create a plan.
class PAIMON_EXPORT TableScan {
 public:
    /// Creates a managed-table scan from the context and table options.
    /// System and format tables require Table::NewScan().
    /// @param context Scan context.
    /// @param options Overrides for the stored schema options. The branch option must agree with
    ///                any branch in the identifier.
    /// @return The scan, or an error status.
    /// @deprecated Use Table::NewScan().
    PAIMON_DEPRECATED("Use Table::NewScan() instead")
    static Result<std::unique_ptr<TableScan>> Create(
        std::unique_ptr<ScanContext> context, const std::map<std::string, std::string>& options);

    virtual ~TableScan() = default;

    /// Create a scan plan.
    ///
    /// @return A Result containing a shared pointer to the created `Plan` or an error status.
    virtual Result<std::shared_ptr<Plan>> CreatePlan() = 0;

    /// Retrieve metrics related to scan planning operations.
    ///
    /// @return A point-in-time snapshot of scan metrics. Mutating the returned object does not
    /// affect metrics collected by this scan.
    virtual std::shared_ptr<Metrics> GetMetrics() const;

    /// Lists the partitions the scan can see, each as its partition values keyed by field name,
    /// in a stable order. A table with no partition keys has none.
    ///
    /// Only a format table answers it today: its partitions are the directories a scan descends,
    /// so listing them costs no more than planning. Every other table type returns
    /// `NotImplemented`.
    ///
    /// @return A Result containing the partitions, or an error status.
    virtual Result<std::vector<std::map<std::string, std::string>>> ListPartitions() const;
};
}  // namespace paimon
