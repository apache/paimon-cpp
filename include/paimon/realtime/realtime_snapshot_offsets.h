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
#include <string>

#include "paimon/realtime/realtime_context.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

class FileSystem;

/// Reads durable real-time recovery offsets recorded by an immutable table snapshot.
class PAIMON_EXPORT RealtimeSnapshotOffsets {
 public:
    RealtimeSnapshotOffsets() = delete;
    ~RealtimeSnapshotOffsets() = delete;

    /// Loads every partition-bucket offset recorded by an exact snapshot.
    ///
    /// An empty branch selects the main branch. A non-null `file_system` takes precedence;
    /// otherwise it is resolved by `CoreOptions` from `options`. A snapshot without real-time
    /// progress returns an empty map.
    static Result<RealtimeOffsetMap> ReadAll(const std::string& table_path,
                                             const std::string& branch, int64_t snapshot_id,
                                             const std::map<std::string, std::string>& options,
                                             const std::shared_ptr<FileSystem>& file_system);

    /// Loads the exclusive durable recovery offset for one partition-bucket.
    ///
    /// A non-null `file_system` takes precedence; otherwise it is resolved by `CoreOptions` from
    /// `options`. Partition values are logical values rather than escaped partition-path
    /// components. Returns `-1` when the snapshot contains no progress for `partition_bucket`.
    static Result<int64_t> ReadOffset(const std::string& table_path, const std::string& branch,
                                      int64_t snapshot_id,
                                      const RealtimePartitionBucket& partition_bucket,
                                      const std::map<std::string, std::string>& options,
                                      const std::shared_ptr<FileSystem>& file_system);
};

}  // namespace paimon
