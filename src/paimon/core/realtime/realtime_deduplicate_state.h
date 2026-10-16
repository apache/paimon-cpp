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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "paimon/realtime/offset_range.h"
#include "paimon/result.h"

namespace paimon {

class RealtimeStore;
struct DataFileMeta;

class RealtimeOffsetFileIndexLookup;

/// Coordinates the two lookup views derived from the same committed snapshot.
/// `RealtimeOffsetFileIndexLookup` is used by PrepareCommit to map deleted offsets to physical
/// data-file row positions for deletion-vector generation. Its `KeyOffsetLookup` is installed in
/// `ArrowDeduplicateRealtimeStore` so writes can find the previously committed offset for a key.
/// Access is synchronized here so attaching and replacing the committed lookups cannot race.
/// Prepared data files are retained with the shared context across writer handoff until their
/// offset ranges are committed, so later prepares can generate deletion vectors for those files.
class RealtimeDeduplicateState {
 public:
    Result<std::shared_ptr<RealtimeOffsetFileIndexLookup>> AttachFileIndexLookup(
        const std::shared_ptr<RealtimeOffsetFileIndexLookup>& file_lookup,
        const std::shared_ptr<RealtimeStore>& store);

    /// Pins the committed and still-pending prepared files together for deletion-vector lookup.
    Result<std::shared_ptr<RealtimeOffsetFileIndexLookup>> AcquireFileLookup() const;

    /// Retains a completed prepared file for later writers sharing this context.
    Status RegisterPreparedDataFile(const std::shared_ptr<DataFileMeta>& data_file,
                                    const OffsetRange& offset_range);

    Status InstallCommittedSnapshot(
        int64_t committed_end_offset,
        const std::vector<std::shared_ptr<DataFileMeta>>& active_data_files,
        const std::shared_ptr<RealtimeStore>& store);

 private:
    struct PreparedDataFile {
        std::shared_ptr<DataFileMeta> data_file;
        OffsetRange offset_range;
    };

    mutable std::mutex mutex_;
    std::shared_ptr<RealtimeOffsetFileIndexLookup> committed_file_lookup_;
    std::map<std::string, PreparedDataFile> prepared_data_files_;
    int64_t committed_end_offset_ = 0;
};

}  // namespace paimon
