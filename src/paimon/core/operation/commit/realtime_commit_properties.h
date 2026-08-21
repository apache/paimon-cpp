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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/snapshot.h"
#include "paimon/realtime/realtime_commit_progress.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/result.h"

namespace paimon {

class FileSystem;

class RealtimeCommitProperties {
 public:
    RealtimeCommitProperties() = delete;

    static constexpr const char* kOffsetsKey = "realtime.offsets";
    static constexpr int32_t kOffsetsVersion = 1;

    static void Sort(std::vector<RealtimeCommitProgress>* commits);

    static std::string OffsetsDirectory(const std::string& table_root, const std::string& branch);

    /// Returns the offset file referenced by `snapshot`, if present.
    static std::optional<std::string> GetOffsetsPath(const Snapshot& snapshot);

    static Result<RealtimeOffsetMap> ReadOffsets(const std::optional<Snapshot>& snapshot,
                                                 const std::shared_ptr<FileSystem>& file_system);

    /// Returns whether all ranges are already covered by committed offsets.
    ///
    /// Ranges must either all immediately follow committed offsets or all be fully covered.
    /// Mixed states, gaps, and partial overlaps are rejected.
    static Result<bool> AreRangesCommitted(
        const RealtimeOffsetMap& committed_offsets,
        const std::map<RealtimePartitionBucket, OffsetRange>& realtime_ranges);

    static Result<std::string> SerializeOffsets(const RealtimeOffsetMap& offsets);

    /// Builds snapshot properties against `latest_snapshot` and applies real-time progress.
    ///
    /// A full-table replacement sets `reset_all_realtime_progress`. A partition overwrite or
    /// drop lists only the affected partition specs in `removed_realtime_partitions`; offsets for
    /// all other partitions are retained. The two reset forms are independent of the snapshot's
    /// commit kind because an ordinary commit may also use `OVERWRITE` for conflict handling.
    static Result<std::map<std::string, std::string>> Build(
        const std::map<std::string, std::string>& properties,
        const std::optional<Snapshot>& latest_snapshot,
        const std::map<RealtimePartitionBucket, OffsetRange>& realtime_ranges,
        bool reset_all_realtime_progress,
        const std::vector<std::map<std::string, std::string>>& removed_realtime_partitions,
        const std::shared_ptr<FileSystem>& file_system, const std::string& table_root,
        const std::string& branch);

 private:
    static Result<std::string> WriteOffsets(const RealtimeOffsetMap& offsets,
                                            const std::shared_ptr<FileSystem>& file_system,
                                            const std::string& offsets_directory);

    static Result<RealtimeOffsetMap> ParseOffsets(const std::string& value);
};

}  // namespace paimon
