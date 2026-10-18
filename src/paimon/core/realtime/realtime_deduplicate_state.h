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

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "paimon/result.h"

namespace paimon {

class RealtimeStore;
struct DataFileMeta;

class RealtimeOffsetFileIndexLookup;

/// Maintains the committed key lookup shared by successive writers of one partition-bucket.
/// Refresh installs the new snapshot's key lookup together with committed offset advancement.
/// Pending deletions stay as logical offsets in the store's immutable sealed checkpoints;
/// RealtimeCommitProgress carries them to commit, which materializes snapshot-specific DVs.
class RealtimeDeduplicateState {
 public:
    Status AttachFileIndexLookup(const std::shared_ptr<RealtimeOffsetFileIndexLookup>& file_lookup,
                                 const std::shared_ptr<RealtimeStore>& store);

    Status InstallCommittedSnapshot(
        int64_t committed_end_offset,
        const std::vector<std::shared_ptr<DataFileMeta>>& active_data_files,
        const std::shared_ptr<RealtimeStore>& store);

 private:
    mutable std::mutex mutex_;
    std::shared_ptr<RealtimeOffsetFileIndexLookup> committed_file_lookup_;
};

}  // namespace paimon
