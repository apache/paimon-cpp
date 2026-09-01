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

#include "paimon/core/table/source/data_table_stream_scan.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/core/core_options.h"
#include "paimon/core/options/changelog_producer.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/core/table/source/snapshot/changelog_follow_up_scanner.h"
#include "paimon/core/table/source/snapshot/delta_follow_up_scanner.h"
#include "paimon/core/table/source/snapshot/follow_up_scanner.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/core/table/source/snapshot/starting_scanner.h"
#include "paimon/core/utils/snapshot_manager.h"

namespace paimon {
DataTableStreamScan::DataTableStreamScan(const CoreOptions& core_options,
                                         const std::shared_ptr<SnapshotReader>& snapshot_reader)
    : AbstractTableScan(core_options, snapshot_reader) {
    if (core_options.GetBucket() == BucketModeDefine::POSTPONE_BUCKET &&
        core_options.GetChangelogProducer() != ChangelogProducer::NONE) {
        snapshot_reader_->OnlyReadRealBuckets();
    }
}

Result<std::shared_ptr<Plan>> DataTableStreamScan::CreatePlan() {
    if (!starting_scanner_) {
        PAIMON_RETURN_NOT_OK(InitScanner());
    }
    if (next_snapshot_id_ == std::nullopt) {
        return TryFirstPlan();
    } else {
        return NextPlan();
    }
}

Result<std::shared_ptr<Plan>> DataTableStreamScan::TryFirstPlan() {
    std::shared_ptr<StartingScanner::ScanResult> scan_result;
    if (core_options_.GetChangelogProducer() == ChangelogProducer::FULL_COMPACTION) {
        return Status::NotImplemented("do not support full compaction changelog producer");
    } else if (core_options_.GetChangelogProducer() == ChangelogProducer::LOOKUP) {
        // Level-0 files will be compacted later to produce changelog records. Exclude them from
        // the initial full scan so that the same changes are not emitted both in the full phase
        // and again in the incremental changelog phase.
        snapshot_reader_->WithLevelFilter([](int32_t level) -> bool { return level > 0; });
        PAIMON_ASSIGN_OR_RAISE(scan_result, starting_scanner_->Scan(snapshot_reader_));
        snapshot_reader_->WithLevelFilter([](int32_t) -> bool { return true; });
    } else {
        PAIMON_ASSIGN_OR_RAISE(scan_result, starting_scanner_->Scan(snapshot_reader_));
    }
    if (auto current_snapshot =
            std::dynamic_pointer_cast<StartingScanner::CurrentSnapshot>(scan_result)) {
        PAIMON_ASSIGN_OR_RAISE(int64_t current_snapshot_id, current_snapshot->SnapshotId());
        next_snapshot_id_ = current_snapshot_id + 1;
        return current_snapshot->GetPlan();
    } else if (auto next_snapshot =
                   std::dynamic_pointer_cast<StartingScanner::NextSnapshot>(scan_result)) {
        next_snapshot_id_ = next_snapshot->NextSnapshotId();
    }
    return PlanImpl::EmptyPlan();
}

Result<std::shared_ptr<Plan>> DataTableStreamScan::NextPlan() {
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                               GetNextSnapshot(next_snapshot_id_.value()));
        if (snapshot == std::nullopt) {
            return PlanImpl::EmptyPlan();
        }
        if (follow_up_scanner_->NeedScanSnapshot(snapshot.value())) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan,
                                   follow_up_scanner_->Scan(snapshot.value(), snapshot_reader_));
            next_snapshot_id_.value()++;
            if (plan->Splits().empty()) {
                continue;
            }
            return plan;
        } else {
            next_snapshot_id_.value()++;
        }
    }
}

Result<std::optional<Snapshot>> DataTableStreamScan::GetNextSnapshot(
    int64_t next_snapshot_id) const {
    auto snapshot_manager = snapshot_reader_->GetSnapshotManager();
    PAIMON_ASSIGN_OR_RAISE(bool exists, snapshot_manager->SnapshotExists(next_snapshot_id));
    if (exists) {
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, snapshot_manager->LoadSnapshot(next_snapshot_id));
        return std::optional<Snapshot>(snapshot);
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> earliest, snapshot_manager->EarliestSnapshotId());
    PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> latest, snapshot_manager->LatestSnapshotId());
    // No snapshot now
    if (earliest == std::nullopt || earliest.value() <= next_snapshot_id) {
        if ((earliest == std::nullopt && next_snapshot_id > 1) ||
            (latest != std::nullopt && next_snapshot_id > latest.value() + 1)) {
            return Status::Invalid(fmt::format(
                "The next expected snapshot is too big! Most possible cause might be the table had "
                "been recreated. The next snapshot id is {}, while the latest snapshot id is {}",
                next_snapshot_id, latest.value()));
        }
        return std::optional<Snapshot>();
    }
    return Status::Invalid(
        fmt::format("The snapshot with id {} has expired. You can: 1. increase the snapshot or "
                    "changelog expiration time. 2. use consumer-id to ensure that unconsumed "
                    "snapshots will not be expired.",
                    next_snapshot_id));
}

Status DataTableStreamScan::InitScanner() {
    PAIMON_ASSIGN_OR_RAISE(starting_scanner_, CreateStartingScanner(/*is_streaming=*/true));
    switch (core_options_.GetChangelogProducer()) {
        case ChangelogProducer::NONE:
            follow_up_scanner_ = std::make_shared<DeltaFollowUpScanner>();
            return Status::OK();
        case ChangelogProducer::INPUT:
        case ChangelogProducer::LOOKUP:
            follow_up_scanner_ = std::make_shared<ChangelogFollowUpScanner>();
            return Status::OK();
        case ChangelogProducer::FULL_COMPACTION:
            return Status::NotImplemented("do not support full compaction changelog producer");
        default:
            return Status::NotImplemented("unknown changelog producer");
    }
}

}  // namespace paimon
