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

#include "paimon/core/table/source/data_evolution_batch_scan.h"

#include <algorithm>
#include <limits>

#include "paimon/core/global_index/global_index_scan_impl.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/snapshot/static_from_snapshot_starting_scanner.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/global_index/bitmap_global_index_result.h"

namespace paimon {
namespace {

bool UsesUnsupportedTimeTravel(const CoreOptions& core_options) {
    const StartupMode startup_mode = core_options.GetStartupMode();
    if (startup_mode == StartupMode::FromTimestamp()) {
        return core_options.GetScanTimestampMillis().has_value();
    }
    return startup_mode == StartupMode::FromSnapshot() &&
           !core_options.GetScanSnapshotId().has_value() &&
           core_options.GetScanTagName().has_value();
}

Result<std::optional<Snapshot>> ResolveGlobalIndexScanSnapshot(
    const CoreOptions& core_options, const std::shared_ptr<SnapshotManager>& snapshot_manager) {
    const StartupMode startup_mode = core_options.GetStartupMode();
    if (startup_mode == StartupMode::FromSnapshot() ||
        startup_mode == StartupMode::FromSnapshotFull()) {
        if (const std::optional<int64_t>& snapshot_id = core_options.GetScanSnapshotId()) {
            return StaticFromSnapshotStartingScanner::ResolveSnapshot(snapshot_manager,
                                                                      snapshot_id.value());
        }
        if (startup_mode == StartupMode::FromSnapshotFull()) {
            return Status::Invalid(
                "scan.snapshot-id must be set when startup mode is FROM_SNAPSHOT_FULL");
        }
        if (!core_options.GetScanTagName()) {
            return Status::Invalid(
                "scan.snapshot-id or scan.tag-name must be set when startup mode is "
                "FROM_SNAPSHOT");
        }
    } else if (startup_mode == StartupMode::FromTimestamp() &&
               !core_options.GetScanTimestampMillis()) {
        return Status::Invalid(
            "scan.timestamp-millis or scan.timestamp must be set when startup mode is "
            "FROM_TIMESTAMP");
    }

    // Tag and timestamp scans are rejected only when the predicate uses a Global Index. Use the
    // latest snapshot here to check whether an applicable index exists.
    return snapshot_manager->LatestSnapshot();
}

}  // namespace

DataEvolutionBatchScan::DataEvolutionBatchScan(
    const std::string& table_path, const std::shared_ptr<SnapshotReader>& snapshot_reader,
    std::unique_ptr<DataTableBatchScan>&& batch_scan,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<GlobalIndexResult>& global_index_result, const CoreOptions& core_options,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor)
    : AbstractTableScan(core_options, snapshot_reader),
      pool_(pool),
      table_path_(table_path),
      batch_scan_(std::move(batch_scan)),
      table_schema_(table_schema),
      global_index_result_(global_index_result),
      executor_(executor) {}

Result<std::shared_ptr<Plan>> DataEvolutionBatchScan::CreatePlan() {
    std::optional<int64_t> global_index_snapshot_id;
    std::shared_ptr<GlobalIndexResult> final_global_index_result = global_index_result_;
    if (!final_global_index_result) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<EvaluatedGlobalIndex> evaluated_index,
                               EvalGlobalIndex());
        if (evaluated_index) {
            final_global_index_result = evaluated_index->result;
            global_index_snapshot_id = evaluated_index->snapshot_id;
        }
    }
    if (!final_global_index_result) {
        return batch_scan_->CreatePlan();
    }
    if (UsesUnsupportedTimeTravel(core_options_)) {
        return Status::NotImplemented("Global index scan does not support time travel");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<Range> row_ranges, final_global_index_result->ToRanges());
    if (row_ranges.empty()) {
        if (!global_index_snapshot_id) {
            const std::shared_ptr<SnapshotManager>& snapshot_manager =
                snapshot_reader_->GetSnapshotManager();
            PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                                   ResolveGlobalIndexScanSnapshot(core_options_, snapshot_manager));
            if (!snapshot) {
                return PlanImpl::EmptyPlan();
            }
            global_index_snapshot_id = snapshot->Id();
        }
        return std::make_shared<PlanImpl>(global_index_snapshot_id,
                                          std::vector<std::shared_ptr<Split>>());
    }
    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex row_range_index, RowRangeIndex::Create(row_ranges));
    batch_scan_->WithRowRangeIndex(row_range_index);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> data_plan, batch_scan_->CreatePlan());
    if (global_index_snapshot_id && data_plan->SnapshotId() != global_index_snapshot_id) {
        return Status::Invalid("Global index and data scan resolved different snapshots");
    }
    std::map<int64_t, float> id_to_score;
    if (auto scored_result =
            std::dynamic_pointer_cast<ScoredGlobalIndexResult>(final_global_index_result)) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScoredGlobalIndexResult::ScoredIterator> scored_iter,
                               scored_result->CreateScoredIterator());
        while (scored_iter->HasNext()) {
            auto [id, score] = scored_iter->NextWithScore();
            id_to_score[id] = score;
        }
    }
    return WrapToIndexedSplits(data_plan, row_range_index, id_to_score);
}

Result<std::shared_ptr<Plan>> DataEvolutionBatchScan::WrapToIndexedSplits(
    const std::shared_ptr<Plan>& data_plan, const RowRangeIndex& row_range_index,
    const std::map<int64_t, float>& id_to_score) {
    // TODO(lisizhuo.lsz): add executor here
    auto data_splits = data_plan->Splits();
    std::vector<std::shared_ptr<Split>> indexed_splits;
    indexed_splits.reserve(data_splits.size());
    for (const auto& split : data_splits) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        if (!data_split) {
            return Status::Invalid("Cannot cast split to DataSplit when create IndexedSplit");
        }
        const auto& files = data_split->DataFiles();
        if (files.empty()) {
            return Status::Invalid("Empty data files in WrapToIndexedSplits");
        }
        // The row-id ranges of the files in a split may be unordered, discontiguous, or
        // overlapping, so intersect the index with each file's range separately, then sort and
        // merge the intersected ranges.
        std::vector<Range> intersected;
        int64_t min = std::numeric_limits<int64_t>::max();
        int64_t max = std::numeric_limits<int64_t>::min();
        for (const auto& file : files) {
            PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file->NonNullFirstRowId());
            int64_t last_row_id = first_row_id + file->row_count - 1;
            min = std::min(min, first_row_id);
            max = std::max(max, last_row_id);
            std::vector<Range> file_ranges =
                row_range_index.IntersectedRanges(first_row_id, last_row_id);
            intersected.insert(intersected.end(), file_ranges.begin(), file_ranges.end());
        }

        std::vector<Range> expected = Range::SortAndMergeOverlap(intersected, /*adjacent=*/true);
        if (expected.empty()) {
            return Status::Invalid(
                fmt::format("There should be intersected ranges for split with min row id {} and "
                            "max row id {}.",
                            min, max));
        }

        std::vector<float> scores;
        if (!id_to_score.empty()) {
            for (const auto& range : expected) {
                for (int64_t i = range.from; i <= range.to; i++) {
                    auto iter = id_to_score.find(i);
                    if (iter != id_to_score.end()) {
                        scores.push_back(iter->second);
                    } else {
                        return Status::Invalid(fmt::format("cannot find score for row {}", i));
                    }
                }
            }
        }
        indexed_splits.push_back(std::make_shared<IndexedSplitImpl>(data_split, expected, scores));
    }
    return std::make_shared<PlanImpl>(data_plan->SnapshotId(), indexed_splits);
}

Result<std::optional<DataEvolutionBatchScan::EvaluatedGlobalIndex>>
DataEvolutionBatchScan::EvalGlobalIndex() const {
    auto predicate = batch_scan_->GetNonPartitionPredicate();
    if (!predicate) {
        return std::optional<EvaluatedGlobalIndex>();
    }
    if (!core_options_.GlobalIndexEnabled()) {
        return std::optional<EvaluatedGlobalIndex>();
    }
    auto partition_filter = batch_scan_->GetPartitionPredicate();
    const std::shared_ptr<SnapshotManager>& snapshot_manager =
        snapshot_reader_->GetSnapshotManager();
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                           ResolveGlobalIndexScanSnapshot(core_options_, snapshot_manager));
    if (!snapshot) {
        return std::optional<EvaluatedGlobalIndex>();
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<GlobalIndexScanImpl> index_scan,
        GlobalIndexScanImpl::Create(table_path_, table_schema_, snapshot.value(), partition_filter,
                                    core_options_, executor_, pool_));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result, index_scan->Scan(predicate));
    return std::optional<EvaluatedGlobalIndex>(EvaluatedGlobalIndex{result, snapshot->Id()});
}

}  // namespace paimon
