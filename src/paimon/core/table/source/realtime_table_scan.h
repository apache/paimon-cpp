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

#include "paimon/realtime/realtime_context.h"
#include "paimon/result.h"
#include "paimon/table/source/table_scan.h"

namespace paimon {

class FileStorePathFactory;
class FileSystem;
class ScanFilter;
class SnapshotManager;

/// Adds process-local memory splits to a normal append-table batch scan.
class RealtimeTableScan : public TableScan {
 public:
    RealtimeTableScan(std::unique_ptr<TableScan>&& disk_scan,
                      const std::shared_ptr<RealtimeContext>& realtime_context,
                      const std::shared_ptr<FileStorePathFactory>& path_factory,
                      const std::shared_ptr<SnapshotManager>& snapshot_manager,
                      const std::shared_ptr<FileSystem>& file_system,
                      const std::shared_ptr<ScanFilter>& scan_filter, int64_t read_view_ttl_millis);

    Result<std::shared_ptr<Plan>> CreatePlan() override;

 private:
    using MemoryViewMap = std::map<RealtimePartitionBucket, RealtimePartitionBucketView>;

    static int64_t GetCommittedOffset(const RealtimeOffsetMap& committed_offsets,
                                      const RealtimePartitionBucket& partition_bucket);

    bool MatchPartition(const std::map<std::string, std::string>& partition) const;

    Result<RealtimeOffsetMap> LoadCommittedOffsets(const std::shared_ptr<Plan>& disk_plan) const;

    Result<MemoryViewMap> CollectActiveMemoryViews(
        std::vector<RealtimePartitionBucketView>&& memory_views,
        const RealtimeOffsetMap& committed_offsets) const;

    Result<std::vector<std::shared_ptr<Split>>> CreateRealtimeSplits(
        const std::vector<std::shared_ptr<Split>>& disk_splits, MemoryViewMap&& active_memory,
        const RealtimeOffsetMap& committed_offsets,
        const std::optional<int64_t>& snapshot_id) const;

    std::unique_ptr<TableScan> disk_scan_;
    std::shared_ptr<RealtimeContext> realtime_context_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<ScanFilter> scan_filter_;
    int64_t read_view_ttl_millis_;
};

}  // namespace paimon
