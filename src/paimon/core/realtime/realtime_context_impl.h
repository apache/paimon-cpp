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
#include "paimon/result.h"

struct ArrowSchema;

namespace paimon {

class MemIndexer;
class MemReadView;
class MemoryPool;

struct RealtimeMemIndexerState {
    std::shared_ptr<MemIndexer> indexer;
    int64_t initial_offset;
};

struct RealtimePartitionBucketView {
    RealtimePartitionBucket partition_bucket;
    std::shared_ptr<MemIndexer> indexer;
    std::shared_ptr<MemReadView> read_view;
};

class RealtimeContextImpl final : public RealtimeContext {
 public:
    static Result<std::shared_ptr<RealtimeContextImpl>> Create(
        const std::shared_ptr<MemIndexerFactory>& factory);

    ~RealtimeContextImpl() override;

    static Result<std::shared_ptr<RealtimeContextImpl>> Cast(
        const std::shared_ptr<RealtimeContext>& context);

    Result<RealtimeMemIndexerState> GetOrCreateMemIndexer(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        std::unique_ptr<::ArrowSchema> write_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool);

    Result<std::vector<RealtimePartitionBucketView>> AcquireReadViews();

    Result<std::string> PinReadView(const RealtimePartitionBucketView& view, int64_t ttl_millis);

    Result<RealtimePartitionBucketView> ResolveReadView(const std::string& opaque_ticket);

    Status ReleaseReadView(const std::string& opaque_ticket);

    Status AdvanceCommittedProgress(int64_t snapshot_id,
                                    const RealtimeOffsetMap& committed_offsets);

 private:
    static constexpr std::chrono::milliseconds kReadViewReleaseCheckInterval{100};

    struct PinnedReadView {
        RealtimePartitionBucketView view;
        std::chrono::steady_clock::time_point expire_at;
    };

    explicit RealtimeContextImpl(const std::shared_ptr<MemIndexerFactory>& factory);

    Status Start();

    void CleanupReadViews();

    std::shared_ptr<MemIndexerFactory> factory_;
    std::mutex mutex_;
    std::mutex progress_mutex_;
    std::map<RealtimePartitionBucket, std::shared_ptr<MemIndexer>> indexers_;
    RealtimeOffsetMap committed_offsets_;
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
