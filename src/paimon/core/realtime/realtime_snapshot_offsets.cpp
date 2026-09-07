/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/realtime/realtime_snapshot_offsets.h"

#include <map>
#include <optional>
#include <string>
#include <utility>

#include "paimon/core/core_options.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/macros.h"
#include "paimon/status.h"

namespace paimon {

Result<RealtimeOffsetMap> RealtimeSnapshotOffsets::ReadAll(
    const std::string& table_path, const std::string& branch, int64_t snapshot_id,
    const std::map<std::string, std::string>& options_map,
    const std::shared_ptr<FileSystem>& file_system) {
    if (table_path.empty()) {
        return Status::Invalid("table path is empty");
    }
    if (snapshot_id < Snapshot::FIRST_SNAPSHOT_ID) {
        return Status::Invalid("snapshot id must be greater than or equal to 1");
    }
    PAIMON_RETURN_NOT_OK(BranchManager::CheckValidBranch(branch));

    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_map, file_system));
    SnapshotManager snapshot_manager(options.GetFileSystem(), table_path,
                                     BranchManager::NormalizeBranch(branch));
    PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, snapshot_manager.LoadSnapshot(snapshot_id));
    return RealtimeCommitProperties::ReadOffsets(std::optional<Snapshot>(std::move(snapshot)),
                                                 options.GetFileSystem());
}

Result<int64_t> RealtimeSnapshotOffsets::ReadOffset(
    const std::string& table_path, const std::string& branch, int64_t snapshot_id,
    const RealtimePartitionBucket& partition_bucket,
    const std::map<std::string, std::string>& options_map,
    const std::shared_ptr<FileSystem>& file_system) {
    if (partition_bucket.bucket < 0) {
        return Status::Invalid("real-time recovery bucket must not be negative");
    }
    PAIMON_ASSIGN_OR_RAISE(RealtimeOffsetMap offsets,
                           ReadAll(table_path, branch, snapshot_id, options_map, file_system));
    auto iter = offsets.find(partition_bucket);
    return iter == offsets.end() ? -1 : iter->second;
}

}  // namespace paimon
