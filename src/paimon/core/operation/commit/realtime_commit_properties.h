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

    static Result<RealtimeOffsetMap> ReadOffsets(const std::optional<Snapshot>& snapshot,
                                                 const std::shared_ptr<FileSystem>& file_system);

    static Result<std::string> SerializeOffsets(const RealtimeOffsetMap& offsets);

    /// Builds snapshot properties against `latest_snapshot` and applies real-time progress.
    static Result<std::map<std::string, std::string>> Build(
        const std::map<std::string, std::string>& properties,
        const std::optional<Snapshot>& latest_snapshot,
        const std::map<RealtimePartitionBucket, Range>& realtime_ranges,
        const std::shared_ptr<FileSystem>& file_system, const std::string& table_root,
        const std::string& branch);

 private:
    static Result<std::string> WriteOffsets(const RealtimeOffsetMap& offsets,
                                            const std::shared_ptr<FileSystem>& file_system,
                                            const std::string& offsets_directory);

    static Result<RealtimeOffsetMap> ParseOffsets(const std::string& value);
};

}  // namespace paimon
