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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "paimon/realtime/realtime_context.h"
#include "paimon/realtime/realtime_store.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class RealtimeStore;
class RealtimeReadView;
class MemoryPool;

struct RealtimeStoreState {
    std::shared_ptr<RealtimeStore> store;
    int64_t initial_offset;
};

struct RealtimePartitionBucketView {
    RealtimePartitionBucket partition_bucket;
    std::shared_ptr<RealtimeStore> store;
    std::shared_ptr<RealtimeReadView> read_view;
};

struct RealtimeReadState {
    std::vector<RealtimePartitionBucketView> views;
    RealtimeOffsetMap committed_offsets;
};

class PAIMON_EXPORT RealtimeContextImpl final : public RealtimeContext {
 public:
    static Result<std::shared_ptr<RealtimeContextImpl>> Create(
        const std::shared_ptr<RealtimeStoreFactory>& factory);

    ~RealtimeContextImpl() override;

    static Result<std::shared_ptr<RealtimeContextImpl>> Cast(
        const std::shared_ptr<RealtimeContext>& context);

    Result<RealtimeStoreState> GetOrCreateRealtimeStore(
        RealtimeStoreCreateRequest&& request, const RealtimePartitionBucket& partition_bucket);

    Result<int64_t> AdvanceMaterializedMaxSequenceNumber(
        const RealtimePartitionBucket& partition_bucket, int64_t max_sequence_number);

    Result<RealtimeReadState> AcquireReadState();

    Result<std::string> PinReadView(const RealtimePartitionBucketView& view, int64_t ttl_millis);

    Result<RealtimePartitionBucketView> ResolveReadView(const std::string& opaque_ticket);

    Status ReleaseReadView(const std::string& opaque_ticket);

    // Returns an error requiring a new context if a newer snapshot removes or moves committed
    // progress backwards for a store created by this context. Progress for inactive stores is
    // only reference state and can be replaced in place.
    Status AdvanceCommittedProgress(int64_t snapshot_id,
                                    const RealtimeOffsetMap& committed_offsets);

 private:
    static constexpr std::chrono::milliseconds kReadViewReleaseCheckInterval{100};

    struct PinnedReadView {
        RealtimePartitionBucketView view;
        std::chrono::steady_clock::time_point expire_at;
    };

    struct StoreEntry {
        std::shared_ptr<RealtimeStore> store;
        std::shared_ptr<arrow::Schema> write_schema;
        RealtimeStoreMode mode;
        int64_t materialized_max_sequence_number = -1;
    };

    explicit RealtimeContextImpl(const std::shared_ptr<RealtimeStoreFactory>& factory);

    Status Start();

    void CleanupReadViews();

    std::shared_ptr<RealtimeStoreFactory> factory_;
    std::mutex mutex_;
    std::mutex progress_mutex_;
    std::map<RealtimePartitionBucket, StoreEntry> stores_;
    // Full-table progress used as the initial offset when a store is created lazily.
    RealtimeOffsetMap committed_offsets_;
    // Progress already reflected in stores owned by this context.
    RealtimeOffsetMap reclaimed_offsets_;
    std::optional<int64_t> last_refreshed_snapshot_id_;
    std::mutex read_views_mutex_;
    std::condition_variable read_views_cv_;
    std::map<std::string, PinnedReadView> pinned_read_views_;
    std::deque<RealtimePartitionBucketView> read_view_release_queue_;
    bool stopping_ = false;
    std::thread read_view_cleanup_thread_;
};

}  // namespace paimon
