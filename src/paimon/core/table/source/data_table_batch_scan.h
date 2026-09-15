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
#include <cstdint>
#include <memory>
#include <optional>

#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/snapshot/starting_scanner.h"
#include "paimon/logging.h"
#include "paimon/result.h"
#include "paimon/table/source/plan.h"

namespace paimon {
class CoreOptions;
class SnapshotReader;

/// `TableScan` implementation for batch planning.
class DataTableBatchScan : public AbstractTableScan {
 public:
    DataTableBatchScan(bool pk_table, const CoreOptions& core_options,
                       const std::shared_ptr<SnapshotReader>& snapshot_reader, bool read_optimized,
                       std::optional<int32_t> push_down_limit, bool realtime_pk_scan);

    Result<std::shared_ptr<Plan>> CreatePlan() override;

    std::shared_ptr<PredicateFilter> GetNonPartitionPredicate() const {
        return snapshot_reader_->GetNonPartitionPredicate();
    }
    std::shared_ptr<PredicateFilter> GetPartitionPredicate() const {
        return snapshot_reader_->GetPartitionPredicate();
    }

    DataTableBatchScan* WithRowRangeIndex(const RowRangeIndex& row_range_index) {
        snapshot_reader_->WithRowRangeIndex(row_range_index);
        return this;
    }

 private:
    /// Drops the trailing splits once the kept ones already hold `push_down_limit_` rows.
    ///
    /// A split's row count comes from metadata, so this is only safe when every row it reports
    /// is actually returned. Anything that drops rows after planning (a non-partition filter, a
    /// row range index) turns the count into an upper bound, so the push down is skipped there.
    /// The limit is still enforced downstream; skipping only costs the pruning.
    Result<std::shared_ptr<Plan>> ApplyPushDownLimit(
        const std::shared_ptr<StartingScanner::ScanResult>& scan_result) const;

    /// Whether a limit is set and nothing drops rows after planning. Whether a given split's
    /// own count is exact is decided per split.
    bool CanPushDownLimit() const;

 private:
    std::shared_ptr<StartingScanner> starting_scanner_;
    bool has_next_ = true;
    std::optional<int32_t> push_down_limit_;
    std::unique_ptr<Logger> logger_;
};
}  // namespace paimon
