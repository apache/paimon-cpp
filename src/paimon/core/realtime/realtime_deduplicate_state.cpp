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

#include "paimon/core/realtime/realtime_deduplicate_state.h"

#include <utility>

#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/realtime/arrow_deduplicate_realtime_store.h"
#include "paimon/core/realtime/realtime_offset_file_index_lookup.h"
#include "paimon/macros.h"

namespace paimon {

Result<std::shared_ptr<RealtimeOffsetFileIndexLookup>>
RealtimeDeduplicateState::AttachFileIndexLookup(
    const std::shared_ptr<RealtimeOffsetFileIndexLookup>& file_lookup,
    const std::shared_ptr<RealtimeStore>& store) {
    if (!file_lookup || !store) {
        return Status::Invalid("cannot attach a null deduplicate file lookup or store");
    }
    std::shared_ptr<ArrowDeduplicateRealtimeStore> deduplicate_store =
        std::dynamic_pointer_cast<ArrowDeduplicateRealtimeStore>(store);
    if (!deduplicate_store) {
        return Status::Invalid("deduplicate state requires ArrowDeduplicateRealtimeStore");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const KeyOffsetLookup> key_lookup,
                           file_lookup->CreateKeyOffsetLookup());
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (committed_file_lookup_) {
            return committed_file_lookup_;
        }
        PAIMON_RETURN_NOT_OK(deduplicate_store->AttachCommittedKeyOffsetLookup(key_lookup));
        committed_file_lookup_ = file_lookup;
    }
    return file_lookup;
}

Result<std::shared_ptr<RealtimeOffsetFileIndexLookup>>
RealtimeDeduplicateState::AcquireCommittedFileLookup() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!committed_file_lookup_) {
        return Status::Invalid("deduplicate file lookup is not attached");
    }
    return committed_file_lookup_;
}

Status RealtimeDeduplicateState::InstallCommittedSnapshot(
    int64_t committed_end_offset,
    const std::vector<std::shared_ptr<DataFileMeta>>& active_data_files,
    const std::shared_ptr<RealtimeStore>& store) {
    if (committed_end_offset < 0 || !store) {
        return Status::Invalid("invalid deduplicate committed advancement");
    }
    std::shared_ptr<ArrowDeduplicateRealtimeStore> deduplicate_store =
        std::dynamic_pointer_cast<ArrowDeduplicateRealtimeStore>(store);
    if (!deduplicate_store) {
        return Status::Invalid("deduplicate state requires ArrowDeduplicateRealtimeStore");
    }
    std::shared_ptr<RealtimeOffsetFileIndexLookup> current_file_lookup;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!committed_file_lookup_) {
            return Status::Invalid("deduplicate file lookup is not attached");
        }
        current_file_lookup = committed_file_lookup_;
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeOffsetFileIndexLookup> next_file_lookup,
                           current_file_lookup->WithDataFiles(active_data_files));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const KeyOffsetLookup> next_key_lookup,
                           next_file_lookup->CreateKeyOffsetLookup());
    {
        std::lock_guard<std::mutex> lock(mutex_);
        PAIMON_RETURN_NOT_OK(deduplicate_store->AdvanceCommittedOffsetAndLookup(
            committed_end_offset, next_key_lookup));
        committed_file_lookup_ = std::move(next_file_lookup);
    }
    return Status::OK();
}

}  // namespace paimon
