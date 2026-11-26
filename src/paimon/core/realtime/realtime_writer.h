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
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/utils/batch_writer.h"
#include "paimon/realtime/realtime_store.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class CoreOptions;
class MemoryPool;
class RealtimeContextImpl;
struct RealtimePartitionBucket;
struct RealtimeStoreState;

/// Common store creation and segment lifecycle for real-time writers.
class RealtimeWriter : public BatchWriter {
 public:
    static Result<RealtimeStoreState> GetOrCreateStore(
        const std::shared_ptr<RealtimeContextImpl>& context,
        const RealtimePartitionBucket& partition_bucket,
        const std::shared_ptr<arrow::Schema>& write_schema, RealtimeStoreMode mode,
        const CoreOptions& options, const std::string& temp_directory,
        const std::vector<std::string>& deduplicate_key_fields,
        const std::shared_ptr<MemoryPool>& memory_pool);

    Status Seal() final;
    Result<CommitIncrement> PrepareCommit(bool wait_compaction) final;
    bool HasUnpreparedRealtimeData() const final;
    void ReleaseCommittedRealtimeSegments(int64_t committed_end_offset) final;

 protected:
    RealtimeWriter(const std::shared_ptr<RealtimeStore>& realtime_store, int64_t next_offset,
                   const std::string& store_name);

    /// Called under prepare_mutex_, without holding realtime_store_mutex_.
    virtual Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealStore(
        bool allow_spill);
    virtual Result<CommitIncrement> PrepareCommitImpl(
        const std::vector<std::shared_ptr<RealtimeSegmentHandle>>& segments,
        bool wait_compaction) = 0;

    std::shared_ptr<RealtimeStore> realtime_store_;
    // Protects store writes and the offset/building state shared with seal.
    mutable std::mutex realtime_store_mutex_;
    int64_t next_offset_;
    bool has_building_data_ = false;

 private:
    Status SealCurrentSegment(bool allow_spill);

    std::string store_name_;
    std::vector<std::shared_ptr<RealtimeSegmentHandle>> sealed_segments_;
    std::mutex prepare_mutex_;
};

}  // namespace paimon
