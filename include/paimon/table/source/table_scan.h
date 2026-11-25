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

#include <cstddef>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/scan_metrics.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class ScanContext;

/// Per-plan limits for an uncached managed append-only batch scan of a full snapshot with Avro
/// manifests. Row ranges are unsupported. A default limit imposes no count restriction.
/// Set explicit finite limits to bound objects. Each zero allows no such objects.
///
/// These counts exclude table and snapshot discovery, schema and index/statistics buffers,
/// decoding buffers (including Arrow and Avro), variable-length field bytes, and container
/// capacity.
struct PlanningLimits {
    /// Total rows in the base and delta manifest lists, charged before conversion to objects.
    std::size_t max_manifest_list_rows = std::numeric_limits<std::size_t>::max();
    /// Rows charged across all selected manifests. With lazy entry decoding, only rows that
    /// pass the early partition row filter are charged. Otherwise each decoded Arrow batch
    /// is charged in full. Data-column predicates run after these charges.
    std::size_t max_manifest_entry_rows = std::numeric_limits<std::size_t>::max();
    /// Actual split count before applying the row limit, checked after a bucket is packed and
    /// before creating its split objects. Packing groups are bounded by the admitted entries.
    std::size_t max_splits = std::numeric_limits<std::size_t>::max();
};

/// A scanner interface for reading table's meta and create a plan.
class PAIMON_EXPORT TableScan {
 public:
    /// Create an instance of `TableScan`.
    ///
    /// @param context A unique pointer to the `ScanContext` used for scan operations.
    /// @return A Result containing a unique pointer to the `TableScan` instance.
    static Result<std::unique_ptr<TableScan>> Create(std::unique_ptr<ScanContext> context);

    /// Opt in to planning object limits using the existing executor.
    /// Unsupported routes return `NotImplemented`. A limit refusal returns `CapacityError`.
    static Result<std::unique_ptr<TableScan>> Create(std::unique_ptr<ScanContext> context,
                                                     const PlanningLimits& limits);

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
