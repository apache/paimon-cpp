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
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/snapshot_read_view_impl.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/global_index/bitmap_global_index_result.h"

namespace paimon {
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
    std::shared_ptr<const SnapshotReadView> snapshot_read_view =
        snapshot_reader_->GetSnapshotReadView();
    if (snapshot_read_view && !snapshot_read_view->SnapshotId()) {
        return PlanImpl::EmptyPlan(snapshot_read_view);
    }
    std::optional<std::vector<Range>> row_ranges;
    std::shared_ptr<GlobalIndexResult> final_global_index_result = global_index_result_;
    if (!final_global_index_result) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> index_result, EvalGlobalIndex());
        if (index_result) {
            final_global_index_result = index_result;
            PAIMON_ASSIGN_OR_RAISE(row_ranges, index_result->ToRanges());
        }
        // EvalGlobalIndex captures the latest snapshot before consulting its index. Refresh the
        // local copy so both negative and positive results publish and reuse that exact view.
        snapshot_read_view = snapshot_reader_->GetSnapshotReadView();
    } else {
        PAIMON_ASSIGN_OR_RAISE(row_ranges, final_global_index_result->ToRanges());
    }
    if (!row_ranges) {
        return batch_scan_->CreatePlan();
    }
    if (row_ranges.value().empty()) {
        if (snapshot_read_view && snapshot_read_view->SnapshotId()) {
            return std::make_shared<PlanImpl>(snapshot_read_view->SnapshotId(),
                                              std::vector<std::shared_ptr<Split>>(),
                                              snapshot_read_view);
        }
        return PlanImpl::EmptyPlan(snapshot_read_view);
    }
    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex row_range_index,
                           RowRangeIndex::Create(row_ranges.value()));
    batch_scan_->WithRowRangeIndex(row_range_index);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> data_plan, batch_scan_->CreatePlan());
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
    return std::make_shared<PlanImpl>(data_plan->SnapshotId(), indexed_splits,
                                      data_plan->GetSnapshotReadView());
}

Result<std::shared_ptr<GlobalIndexResult>> DataEvolutionBatchScan::EvalGlobalIndex() const {
    auto predicate = batch_scan_->GetNonPartitionPredicate();
    if (!predicate) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    if (!core_options_.GlobalIndexEnabled()) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    auto partition_filter = batch_scan_->GetPartitionPredicate();
    StartupMode startup_mode = core_options_.GetStartupMode();
    std::shared_ptr<const Snapshot> snapshot;
    if (!(startup_mode == StartupMode::LatestFull() || startup_mode == StartupMode::Latest())) {
        // Snapshot read views deliberately support latest scans only. Preserve main's non-latest
        // planning path, including an explicitly selected snapshot and the caller-owned context.
        // TODO(lisizhuo.lsz): support tag/timestamp time travel.
        std::optional<Snapshot> loaded_snapshot;
        const std::shared_ptr<SnapshotManager>& snapshot_manager =
            snapshot_reader_->GetSnapshotManager();
        if (const std::optional<int64_t>& snapshot_id = core_options_.GetScanSnapshotId()) {
            PAIMON_ASSIGN_OR_RAISE(Snapshot selected_snapshot,
                                   snapshot_manager->LoadSnapshot(snapshot_id.value()));
            loaded_snapshot = std::move(selected_snapshot);
        } else {
            PAIMON_ASSIGN_OR_RAISE(loaded_snapshot, snapshot_manager->LatestSnapshot());
        }
        if (!loaded_snapshot) {
            return Status::Invalid("not found latest snapshot");
        }
        snapshot = std::make_shared<const Snapshot>(std::move(loaded_snapshot).value());
    } else {
        // Capture the latest snapshot through the shared SnapshotReader before evaluating the
        // index. The subsequent data scan then consumes the same immutable view, avoiding both a
        // second latest-snapshot lookup and a row-id/snapshot race.
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const SnapshotReadView> snapshot_read_view,
                               snapshot_reader_->CaptureLatestSnapshotReadView());
        PAIMON_ASSIGN_OR_RAISE(snapshot, SnapshotReadViewImpl::GetSnapshot(snapshot_read_view));
        if (!snapshot) {
            return std::shared_ptr<GlobalIndexResult>(nullptr);
        }
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<GlobalIndexScanImpl> index_scan,
        GlobalIndexScanImpl::Create(table_path_, table_schema_, *snapshot, partition_filter,
                                    core_options_, executor_, pool_));
    return index_scan->Scan(predicate);
}

}  // namespace paimon
