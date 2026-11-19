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
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class FileSystem;
class InputStream;
class MemoryPool;

/// Serialized index payload using the data file's original field name.
struct PAIMON_EXPORT FileIndexInput {
    std::string field_name;
    std::string index_type;
    /// Raw payload read from [0, Length()); nullptr denotes an empty index.
    /// Non-null inputs must be nonempty. Borrowed synchronously, never closed or retained.
    std::shared_ptr<InputStream> input;
};

/// Index updates for one live data file in the base snapshot.
struct PAIMON_EXPORT DataFileIndexInput {
    std::string data_file_name;
    std::vector<FileIndexInput> indexes;
};

/// Prepares file-index updates for fixed-bucket append tables.
/// Serialize backfill commits with compaction/overwrite per partition/bucket;
/// keep base metadata valid through commit.
class PAIMON_EXPORT FileIndexWrite {
 public:
    FileIndexWrite() = delete;
    ~FileIndexWrite() = delete;

    /// Writes one external container per file, preserving indexes except supplied field/type pairs.
    /// Ignores the embedding threshold; preserves other extra files and historical index files.
    /// Payloads must use physical row positions, including DV-deleted rows. Do not use input
    /// streams concurrently. Null file system, pool or executor uses the corresponding default.
    ///
    /// @param branch Target branch; empty selects main.
    /// @param options Table option overrides; bucket count cannot change.
    /// @param base_snapshot_id Base snapshot; nullopt pins latest once for the batch.
    /// @param files Unique live file names and field/type pairs; empty index lists are skipped.
    /// @return Messages grouped by partition/bucket for commit to the same table and branch.
    /// New files are cleaned up best-effort on failure; callers handle commit recovery and orphans.
    static Result<std::vector<std::shared_ptr<CommitMessage>>> PrepareCommit(
        const std::string& table_path, const std::string& branch,
        const std::map<std::string, std::string>& options,
        const std::optional<int64_t>& base_snapshot_id,
        const std::vector<DataFileIndexInput>& files,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<Executor>& executor);
};

}  // namespace paimon
