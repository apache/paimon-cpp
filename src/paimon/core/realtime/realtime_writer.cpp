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

#include "paimon/core/realtime/realtime_writer.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include "arrow/c/bridge.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/utils/commit_increment.h"

namespace paimon {

Result<RealtimeStoreState> RealtimeWriter::GetOrCreateStore(
    const std::shared_ptr<RealtimeContextImpl>& context,
    const RealtimePartitionBucket& partition_bucket,
    const std::shared_ptr<arrow::Schema>& write_schema, RealtimeStoreMode mode,
    const CoreOptions& options, const std::string& temp_directory,
    const std::vector<std::string>& deduplicate_key_fields,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    auto c_write_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*write_schema, c_write_schema.get()));
    RealtimeStoreCreateRequest request{std::move(c_write_schema), options.ToMap(), memory_pool,
                                       mode, options.GetRealtimeStoreStatisticsMode()};
    request.deduplicate_key_fields = deduplicate_key_fields;
    if (options.RealtimeSpillEnabled()) {
        request.temp_directory = temp_directory;
        request.file_system = options.GetFileSystem();
    }
    return context->GetOrCreateRealtimeStore(std::move(request), partition_bucket);
}

RealtimeWriter::RealtimeWriter(const std::shared_ptr<RealtimeStore>& realtime_store,
                               int64_t next_offset, const std::string& store_name)
    : realtime_store_(realtime_store), next_offset_(next_offset), store_name_(store_name) {}

Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> RealtimeWriter::SealStore(
    bool allow_spill) {
    auto* two_phase = dynamic_cast<TwoPhaseSealableRealtimeStore*>(realtime_store_.get());
    return !allow_spill && two_phase ? two_phase->FreezeBuildingSegment()
                                     : realtime_store_->SealForCommit();
}

Status RealtimeWriter::SealCurrentSegment(bool allow_spill) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                           SealStore(allow_spill));
    std::lock_guard<std::mutex> lock(realtime_store_mutex_);
    if (!segment) {
        return Status::OK();
    }
    if (!segment.value()) {
        return Status::Invalid(store_name_, " real-time store sealed a null segment");
    }
    has_building_data_ = next_offset_ > segment.value()->GetOffsetRange().end;
    sealed_segments_.push_back(std::move(segment.value()));
    return Status::OK();
}

Status RealtimeWriter::Seal() {
    std::lock_guard<std::mutex> lock(prepare_mutex_);
    return SealCurrentSegment(/*allow_spill=*/true);
}

Result<CommitIncrement> RealtimeWriter::PrepareCommit(bool wait_compaction) {
    std::lock_guard<std::mutex> prepare_lock(prepare_mutex_);
    PAIMON_RETURN_NOT_OK(SealCurrentSegment(/*allow_spill=*/false));
    std::vector<std::shared_ptr<RealtimeSegmentHandle>> segments;
    {
        std::lock_guard<std::mutex> store_lock(realtime_store_mutex_);
        segments.swap(sealed_segments_);
    }
    PAIMON_ASSIGN_OR_RAISE(CommitIncrement increment, PrepareCommitImpl(segments, wait_compaction));
    if (!segments.empty()) {
        increment.SetRealtimeOffsetRange(OffsetRange(segments.front()->GetOffsetRange().begin,
                                                     segments.back()->GetOffsetRange().end));
    }
    return increment;
}

bool RealtimeWriter::HasUnpreparedRealtimeData() const {
    std::lock_guard<std::mutex> lock(realtime_store_mutex_);
    return has_building_data_ || !sealed_segments_.empty();
}

void RealtimeWriter::ReleaseCommittedRealtimeSegments(int64_t committed_end_offset) {
    std::vector<std::shared_ptr<RealtimeSegmentHandle>> retired_segments;
    {
        std::lock_guard<std::mutex> prepare_lock(prepare_mutex_);
        std::lock_guard<std::mutex> store_lock(realtime_store_mutex_);
        auto retained_end = std::stable_partition(
            sealed_segments_.begin(), sealed_segments_.end(),
            [committed_end_offset](const std::shared_ptr<RealtimeSegmentHandle>& segment) {
                return segment->GetOffsetRange().end > committed_end_offset;
            });
        retired_segments.assign(std::make_move_iterator(retained_end),
                                std::make_move_iterator(sealed_segments_.end()));
        sealed_segments_.erase(retained_end, sealed_segments_.end());
    }
    // Release Arrow buffers and spill files without holding the writer locks.
}

}  // namespace paimon
