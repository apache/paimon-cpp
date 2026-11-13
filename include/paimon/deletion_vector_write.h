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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/commit_message.h"
#include "paimon/result.h"
#include "paimon/table/source/file_row_selection.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class FileSystem;
class MemoryPool;

/// Merges append-table DV updates. Different buckets may run concurrently.
/// Serialize updates, compaction and overwrite within each partition/bucket,
/// from snapshot selection through commit.
class PAIMON_EXPORT DeletionVectorWrite {
 public:
    DeletionVectorWrite() = delete;
    ~DeletionVectorWrite() = delete;

    /// Uses one snapshot for the batch; std::nullopt selects latest.
    /// Requires a managed fixed/unaware append table with bitmap32 DV.
    /// Commit returned messages with append messages; callers handle recovery and orphan cleanup.
    static Result<std::vector<std::shared_ptr<CommitMessage>>> PrepareCommit(
        const std::string& table_path, const std::string& branch,
        const std::map<std::string, std::string>& options,
        const std::optional<int64_t>& base_snapshot_id,
        const std::vector<FileRowSelection>& deletions,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<Executor>& executor);
};

}  // namespace paimon
