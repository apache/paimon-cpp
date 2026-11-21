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
#include <string>
#include <vector>

#include "paimon/commit_message.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class FileSystem;
class MemoryPool;

/// Rewrites explicitly selected files in a fixed-bucket append table.
class PAIMON_EXPORT AppendFileCompaction {
 public:
    AppendFileCompaction() = delete;
    ~AppendFileCompaction() = delete;

    /// Synchronously compacts files and returns messages for FileStoreCommit::Commit or Abort.
    /// Each call creates a dedicated batch writer and restores the latest snapshot for the bucket.
    /// Supports deletion vectors; real-time mode and data evolution are unsupported.
    /// Requires bucket-append-ordered=false and write-only=false.
    /// With deletion vectors enabled, callers must serialize compactions within each
    /// partition/bucket, even for disjoint inputs. Do not start another compaction, update deletion
    /// vectors or overwrite this partition/bucket until the messages are committed or discarded.
    /// Without deletion vectors, compactions of disjoint input files may run concurrently in the
    /// same partition/bucket. Overlapping compaction or overwrite may cause commit conflicts.
    /// Failures may leave orphan files for later cleanup.
    ///
    /// @param table_path The root path of the table.
    /// @param branch The branch to use; an empty string selects main.
    /// @param partition Complete partition values; empty for an unpartitioned table.
    /// @param bucket The bucket containing the input files.
    /// @param file_names Nonempty, distinct data file basenames. Files need not be contiguous.
    /// @param options Options overriding the table options.
    /// @param file_system The file system; nullptr selects one from options.
    /// @param pool The memory pool; nullptr uses the default pool.
    /// @param executor The executor; nullptr uses the default executor.
    static Result<std::vector<std::shared_ptr<CommitMessage>>> PrepareCommit(
        const std::string& table_path, const std::string& branch,
        const std::map<std::string, std::string>& partition, int32_t bucket,
        const std::vector<std::string>& file_names,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<Executor>& executor);
};

}  // namespace paimon
