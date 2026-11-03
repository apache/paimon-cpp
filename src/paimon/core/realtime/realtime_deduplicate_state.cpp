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

#include "paimon/core/realtime/arrow_deduplicate_realtime_store.h"
#include "paimon/core/realtime/realtime_offset_file_index_lookup.h"
#include "paimon/macros.h"

namespace paimon {

Status RealtimeDeduplicateState::AttachFileIndexLookup(
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
            return Status::OK();
        }
        PAIMON_RETURN_NOT_OK(deduplicate_store->AttachCommittedKeyOffsetLookup(key_lookup));
        committed_file_lookup_ = file_lookup;
    }
    return Status::OK();
}

Result<RealtimeDeduplicateState::PreparedSnapshot>
RealtimeDeduplicateState::PrepareCommittedSnapshot(
    const std::vector<std::shared_ptr<DataFileMeta>>& active_data_files) const {
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
    return PreparedSnapshot{std::move(current_file_lookup), std::move(next_file_lookup),
                            std::move(next_key_lookup)};
}

Result<std::shared_ptr<RealtimeReadView>> RealtimeDeduplicateState::InstallPreparedSnapshot(
    int64_t committed_end_offset, const PreparedSnapshot& prepared,
    const std::shared_ptr<RealtimeStore>& store) {
    if (committed_end_offset < 0 || !store || !prepared.previous_file_lookup ||
        !prepared.file_lookup || !prepared.key_lookup) {
        return Status::Invalid("invalid deduplicate committed advancement");
    }
    std::shared_ptr<ArrowDeduplicateRealtimeStore> deduplicate_store =
        std::dynamic_pointer_cast<ArrowDeduplicateRealtimeStore>(store);
    if (!deduplicate_store) {
        return Status::Invalid("deduplicate state requires ArrowDeduplicateRealtimeStore");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (committed_file_lookup_ != prepared.previous_file_lookup) {
        return Status::Invalid("deduplicate committed lookup changed during refresh preparation");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeReadView> retired,
                           deduplicate_store->RetireCommittedOffsetAndLookup(committed_end_offset,
                                                                             prepared.key_lookup));
    committed_file_lookup_ = prepared.file_lookup;
    return retired;
}

}  // namespace paimon
