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

#include <memory>

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
    /// Create an instance of `TableScan`.
    ///
    /// @param context A unique pointer to the `ScanContext` used for scan operations.
    /// @return A Result containing a unique pointer to the `TableScan` instance.
    static Result<std::unique_ptr<TableScan>> Create(std::unique_ptr<ScanContext> context);

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
};
}  // namespace paimon
