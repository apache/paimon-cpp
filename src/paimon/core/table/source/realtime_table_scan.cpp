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

#include "paimon/core/table/source/realtime_table_scan.h"

#include <map>
#include <optional>
#include <utility>
#include <vector>

#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"

namespace paimon {

int64_t RealtimeTableScan::GetCommittedOffset(const RealtimeOffsetMap& committed_offsets,
                                              const RealtimePartitionBucket& partition_bucket) {
    auto iter = committed_offsets.find(partition_bucket);
    return iter == committed_offsets.end() ? -1 : iter->second;
}

bool RealtimeTableScan::MatchPartition(const std::map<std::string, std::string>& partition) const {
    const auto& filters = scan_filter_->GetPartitionFilters();
    if (filters.empty()) {
        return true;
    }
    for (const auto& filter : filters) {
        bool matched = true;
        for (const auto& [key, raw_value] : filter) {
            auto partition_iter = partition.find(key);
            if (partition_iter == partition.end() || partition_iter->second != raw_value) {
                matched = false;
                break;
            }
        }
        if (matched) {
            return true;
        }
    }
    return false;
}

Result<RealtimeOffsetMap> RealtimeTableScan::LoadCommittedOffsets(
    const std::shared_ptr<Plan>& disk_plan) const {
    if (!disk_plan->SnapshotId()) {
        return RealtimeOffsetMap{};
    }
    PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot,
                           snapshot_manager_->LoadSnapshot(disk_plan->SnapshotId().value()));
    return RealtimeCommitProperties::ReadOffsets(std::optional<Snapshot>(std::move(snapshot)),
                                                 file_system_);
}

Result<RealtimeTableScan::MemoryViewMap> RealtimeTableScan::CollectActiveMemoryViews(
    std::vector<RealtimePartitionBucketView>&& memory_views,
    const RealtimeOffsetMap& committed_offsets) const {
    MemoryViewMap active_memory;
    for (RealtimePartitionBucketView& memory : memory_views) {
        if (scan_filter_->GetBucketFilter() &&
            memory.partition_bucket.bucket != scan_filter_->GetBucketFilter().value()) {
            continue;
        }
        if (!MatchPartition(memory.partition_bucket.partition)) {
            continue;
        }
        const std::optional<Range> memory_range = memory.read_view->GetOffsetRange();
        if (!memory_range ||
            memory_range->to <= GetCommittedOffset(committed_offsets, memory.partition_bucket)) {
            continue;
        }
        RealtimePartitionBucket partition_bucket = memory.partition_bucket;
        active_memory.emplace(std::move(partition_bucket), std::move(memory));
    }
    return active_memory;
}

Result<std::vector<std::shared_ptr<Split>>> RealtimeTableScan::CreateRealtimeSplits(
    const std::vector<std::shared_ptr<Split>>& disk_splits, MemoryViewMap&& active_memory,
    const RealtimeOffsetMap& committed_offsets, const std::optional<int64_t>& snapshot_id) const {
    std::map<RealtimePartitionBucket, std::vector<std::shared_ptr<Split>>> disk_by_partition_bucket;
    for (const std::shared_ptr<Split>& split : disk_splits) {
        std::shared_ptr<DataSplitImpl> data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        if (!data_split) {
            return Status::Invalid("real-time append scan requires process-local data splits");
        }
        std::vector<std::pair<std::string, std::string>> partition_values;
        PAIMON_ASSIGN_OR_RAISE(partition_values,
                               path_factory_->GeneratePartitionVector(data_split->Partition()));
        std::map<std::string, std::string> partition(partition_values.begin(),
                                                     partition_values.end());
        disk_by_partition_bucket[RealtimePartitionBucket(std::move(partition),
                                                         data_split->Bucket())]
            .push_back(split);
    }

    // TODO(xinyu.lxy): Support splitting one partition-bucket into multiple real-time splits.
    std::vector<std::shared_ptr<Split>> result;
    std::vector<std::string> pinned_tickets;
    ScopeGuard ticket_guard([this, &pinned_tickets]() {
        for (const std::string& ticket : pinned_tickets) {
            realtime_context_->ReleaseReadView(ticket);
        }
    });
    auto create_realtime_split =
        [this, &committed_offsets, &snapshot_id, &pinned_tickets](
            const RealtimePartitionBucket& key,
            std::vector<std::shared_ptr<Split>>&& grouped_disk_splits,
            const RealtimePartitionBucketView& memory) -> Result<std::shared_ptr<Split>> {
        const std::optional<Range> memory_range = memory.read_view->GetOffsetRange();
        if (!memory_range) {
            return Status::Invalid("real-time split cannot reference an empty memory view");
        }
        PAIMON_ASSIGN_OR_RAISE(std::string opaque_ticket,
                               realtime_context_->PinReadView(memory, read_view_ttl_millis_));
        pinned_tickets.push_back(opaque_ticket);
        return std::make_shared<RealtimeSplit>(
            RealtimeSplit::kCurrentVersion, snapshot_id, key.partition, key.bucket,
            std::move(grouped_disk_splits), GetCommittedOffset(committed_offsets, key),
            memory_range->to, std::move(opaque_ticket));
    };
    for (auto& [key, grouped_disk_splits] : disk_by_partition_bucket) {
        auto memory_iter = active_memory.find(key);
        if (memory_iter == active_memory.end()) {
            result.insert(result.end(), grouped_disk_splits.begin(), grouped_disk_splits.end());
            continue;
        }
        RealtimePartitionBucketView& memory = memory_iter->second;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Split> realtime_split,
                               create_realtime_split(key, std::move(grouped_disk_splits), memory));
        result.push_back(std::move(realtime_split));
        active_memory.erase(memory_iter);
    }

    for (auto& [key, memory] : active_memory) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<Split> realtime_split,
            create_realtime_split(key, std::vector<std::shared_ptr<Split>>(), memory));
        result.push_back(std::move(realtime_split));
    }
    ticket_guard.Release();
    return result;
}

RealtimeTableScan::RealtimeTableScan(std::unique_ptr<TableScan>&& disk_scan,
                                     const std::shared_ptr<RealtimeContext>& realtime_context,
                                     const std::shared_ptr<FileStorePathFactory>& path_factory,
                                     const std::shared_ptr<SnapshotManager>& snapshot_manager,
                                     const std::shared_ptr<FileSystem>& file_system,
                                     const std::shared_ptr<ScanFilter>& scan_filter,
                                     int64_t read_view_ttl_millis)
    : disk_scan_(std::move(disk_scan)),
      realtime_context_(realtime_context),
      path_factory_(path_factory),
      snapshot_manager_(snapshot_manager),
      file_system_(file_system),
      scan_filter_(scan_filter),
      read_view_ttl_millis_(read_view_ttl_millis) {}

Result<std::shared_ptr<Plan>> RealtimeTableScan::CreatePlan() {
    // Memory is pinned first. If a commit and reclaim happens before disk planning, the old memory
    // remains alive in these views and the selected snapshot offset removes its covered prefix.
    PAIMON_ASSIGN_OR_RAISE(std::vector<RealtimePartitionBucketView> memory_views,
                           realtime_context_->AcquireReadViews());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> disk_plan, disk_scan_->CreatePlan());
    PAIMON_ASSIGN_OR_RAISE(RealtimeOffsetMap committed_offsets, LoadCommittedOffsets(disk_plan));
    PAIMON_ASSIGN_OR_RAISE(MemoryViewMap active_memory,
                           CollectActiveMemoryViews(std::move(memory_views), committed_offsets));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Split>> splits,
                           CreateRealtimeSplits(disk_plan->Splits(), std::move(active_memory),
                                                committed_offsets, disk_plan->SnapshotId()));
    return std::make_shared<PlanImpl>(disk_plan->SnapshotId(), splits);
}

}  // namespace paimon
