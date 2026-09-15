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

#include "paimon/core/table/source/data_table_batch_scan.h"

#include <functional>
#include <utility>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/options/merge_engine.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/status.h"

namespace paimon {
class DataSplit;

DataTableBatchScan::DataTableBatchScan(bool pk_table, const CoreOptions& core_options,
                                       const std::shared_ptr<SnapshotReader>& snapshot_reader,
                                       bool read_optimized, std::optional<int32_t> push_down_limit,
                                       bool realtime_pk_scan)
    : AbstractTableScan(core_options, snapshot_reader),
      push_down_limit_(push_down_limit),
      logger_(Logger::GetLogger("DataTableBatchScan")) {
    if (pk_table && read_optimized) {
        int32_t top_level = core_options.GetNumLevels() - 1;
        snapshot_reader_->WithLevelFilter(
            [top_level](int32_t level) -> bool { return level == top_level; });
        snapshot_reader_->EnableValueFilter();
    } else if (pk_table && (core_options.DeletionVectorsEnabled() ||
                            core_options.GetMergeEngine() == MergeEngine::FIRST_ROW)) {
        if (realtime_pk_scan) {
            snapshot_reader_->EnableValueFilterForLevels(
                [](int32_t level) -> bool { return level > 0; });
        } else {
            auto level_filter = [](int32_t level) -> bool { return level > 0; };
            snapshot_reader_->WithLevelFilter(level_filter);
            snapshot_reader_->EnableValueFilter();
        }
    }
    if (core_options.GetBucket() == BucketModeDefine::POSTPONE_BUCKET) {
        snapshot_reader_->OnlyReadRealBuckets();
    }
}

Result<std::shared_ptr<Plan>> DataTableBatchScan::CreatePlan() {
    if (starting_scanner_ == nullptr) {
        PAIMON_ASSIGN_OR_RAISE(starting_scanner_, CreateStartingScanner(/*is_streaming=*/false));
    }
    if (has_next_) {
        has_next_ = false;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<StartingScanner::ScanResult> scan_result,
                               starting_scanner_->Scan(snapshot_reader_));
        return ApplyPushDownLimit(scan_result);
    }
    return Status::Invalid("end of scan");
}

bool DataTableBatchScan::CanPushDownLimit() const {
    if (push_down_limit_ == std::nullopt) {
        return false;
    }
    if (snapshot_reader_->GetNonPartitionPredicate()) {
        // a non-partition filter runs while reading, so the metadata count is an upper bound
        return false;
    }
    if (snapshot_reader_->GetRowRangeIndex()) {
        // a row range index selects a subset of each split's rows, likewise an upper bound
        return false;
    }
    return true;
}

Result<std::shared_ptr<Plan>> DataTableBatchScan::ApplyPushDownLimit(
    const std::shared_ptr<StartingScanner::ScanResult>& scan_result) const {
    auto current_scan_result =
        std::dynamic_pointer_cast<StartingScanner::CurrentSnapshot>(scan_result);
    if (!current_scan_result) {
        // NoSnapshot
        return PlanImpl::EmptyPlan();
    }
    if (!CanPushDownLimit()) {
        return current_scan_result->GetPlan();
    }
    std::vector<std::shared_ptr<Split>> splits = current_scan_result->Splits();
    std::vector<std::shared_ptr<Split>> limited_data_splits;
    limited_data_splits.reserve(splits.size());
    int64_t scanned_row_count = 0;
    for (const auto& split : splits) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        if (!data_split) {
            return Status::Invalid("DataSplit cannot cast to DataSplitImpl");
        }
        // A split the read has to merge by key, or whose deletion file carries no cardinality,
        // cannot be counted from metadata and would have to be pruned blindly, so the push down
        // is abandoned. A data-evolution split merges by column over a known row id range and
        // still counts.
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> merged_row_count,
                               data_split->MergedRowCount());
        if (!merged_row_count.has_value()) {
            PAIMON_LOG_DEBUG(logger_,
                             "Limit push down abandoned: split %zu of %zu cannot be counted from "
                             "metadata.",
                             limited_data_splits.size() + 1, splits.size());
            return current_scan_result->GetPlan();
        }
        limited_data_splits.emplace_back(data_split);
        scanned_row_count += merged_row_count.value();
        if (scanned_row_count >= push_down_limit_.value()) {
            PAIMON_ASSIGN_OR_RAISE(int64_t snapshot_id, current_scan_result->SnapshotId());
            PAIMON_LOG_DEBUG(logger_,
                             "Limit push down kept %zu of %zu splits for limit %d, holding %ld "
                             "rows.",
                             limited_data_splits.size(), splits.size(), push_down_limit_.value(),
                             scanned_row_count);
            return std::make_shared<PlanImpl>(snapshot_id, limited_data_splits);
        }
    }
    return current_scan_result->GetPlan();
}

}  // namespace paimon
