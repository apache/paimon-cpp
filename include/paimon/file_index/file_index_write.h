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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/commit_message.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class FileSystem;
class InputStream;
class MemoryPool;

/// Serialized index payload using the data file's original field name.
/// @note IMPORTANT: Callers must ensure fields and payloads match the file's original schema.
/// This is not validated.
struct PAIMON_EXPORT FileIndexInput {
    std::string field_name;
    std::string index_type;
    /// Raw payload read from [0, Length()); nullptr denotes an empty index.
    /// Non-null inputs must be nonempty. Borrowed synchronously, never closed or retained.
    std::shared_ptr<InputStream> input;
};

/// Index updates for one data file added by the supplied commit messages.
struct PAIMON_EXPORT DataFileIndexInput {
    std::string data_file_name;
    std::vector<FileIndexInput> indexes;
};

/// Attaches file indexes to pending commits for fixed-bucket append tables.
class PAIMON_EXPORT FileIndexWrite {
 public:
    FileIndexWrite() = delete;
    ~FileIndexWrite() = delete;

    /// Writes one external container per file, preserving indexes except supplied field/type pairs.
    /// Ignores the embedding threshold; preserves other extra files and historical index files.
    /// Reuses existing targets without reading or writing index payloads; still prepares messages.
    /// Otherwise publishes UUID temporary files via Rename. If the target appears meanwhile,
    /// reuses it and cleans up the temporary file best-effort. Existing contents are not validated.
    /// Concurrent publication requires atomic rename without overwrite from the file system.
    /// Payloads must use physical row positions, including DV-deleted rows. Do not use input
    /// streams concurrently. Null file system or pool uses the corresponding default.
    ///
    /// @param branch Target branch; empty selects main.
    /// @param options Table option overrides; bucket count cannot change.
    /// @param messages Pending messages from the same table and branch; no snapshot is required.
    /// @param files Unique names of NewFiles or CompactAfter files and field/type pairs.
    /// Empty index lists leave the corresponding files unchanged.
    /// @return Messages in input order with updated file metadata; inputs are not modified.
    /// Commit the returned messages instead of the originals. Other increments are preserved.
    /// New index files are cleaned up best-effort on failure; callers handle commit recovery.
    static Result<std::vector<std::shared_ptr<CommitMessage>>> PrepareCommit(
        const std::string& table_path, const std::string& branch,
        const std::map<std::string, std::string>& options,
        const std::vector<std::shared_ptr<CommitMessage>>& messages,
        const std::vector<DataFileIndexInput>& files,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
