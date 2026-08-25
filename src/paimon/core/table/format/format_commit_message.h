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
#include <string>

#include "paimon/commit_message.h"

namespace paimon {

/// One file a `FormatTableWrite` has written but not yet published.
///
/// The file is complete on disk under `temp_file_path`, which a scan skips; committing renames it
/// to `file_path`, which is what makes it part of the table.
///
/// It is a `CommitMessage` so that a format table can be written and committed through
/// `FileStoreWrite` and `FileStoreCommit` like any other table, as Java Paimon's
/// `TwoPhaseCommitMessage` is. It names a staged path rather than files to record in a manifest,
/// so `CommitMessage::Serialize()` refuses it: there is no cross-runtime encoding for one, and a
/// write and its commit belong to the same process.
struct FormatCommitMessage : public CommitMessage {
    FormatCommitMessage(const std::string& _temp_file_path, const std::string& _file_path,
                        const std::map<std::string, std::string>& _partition, int64_t _record_count,
                        int64_t _file_size)
        : temp_file_path(_temp_file_path),
          file_path(_file_path),
          partition(_partition),
          record_count(_record_count),
          file_size(_file_size) {}

    ~FormatCommitMessage() override = default;

    std::string ToString() const;

    /// Path the data was written to: a hidden file a scan skips.
    std::string temp_file_path;
    /// Path the file is renamed to when the write is committed.
    std::string file_path;
    /// Partition the file belongs to, empty when the table is not partitioned.
    std::map<std::string, std::string> partition;
    /// Rows written to the file.
    int64_t record_count;
    /// Size of the written file in bytes.
    int64_t file_size;
};

}  // namespace paimon
