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

#include "paimon/realtime/realtime_context.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/c/helpers.h"
#include "paimon/arrow/abi.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/macros.h"
#include "paimon/realtime/arrow_mem_indexer_factory.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/status.h"

namespace paimon {

class RealtimeContext::Impl {
 public:
    static constexpr std::chrono::milliseconds kReadViewReleaseCheckInterval{100};

    explicit Impl(const std::shared_ptr<MemIndexerFactory>& factory) : factory_(factory) {
        read_view_cleanup_thread_ = std::thread([this]() { CleanupReadViews(); });
    }

    ~Impl() {
        {
            std::lock_guard<std::mutex> lock(read_views_mutex_);
            stopping_ = true;
        }
        read_views_cv_.notify_all();
        if (read_view_cleanup_thread_.joinable()) {
            read_view_cleanup_thread_.join();
        }
    }

    Result<RealtimeMemIndexerState> GetOrCreateMemIndexer(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        std::unique_ptr<ArrowSchema> write_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool) {
        std::lock_guard<std::mutex> progress_lock(progress_mutex_);
        std::lock_guard<std::mutex> registry_lock(mutex_);
        const RealtimePartitionBucket key(partition, bucket);
        int64_t initial_offset = 0;
        auto offset_iter = committed_offsets_.find(key);
        if (offset_iter != committed_offsets_.end()) {
            if (offset_iter->second == std::numeric_limits<int64_t>::max()) {
                if (write_schema) {
                    ArrowSchemaRelease(write_schema.get());
                }
                return Status::Invalid("real-time offset has reached INT64_MAX");
            }
            initial_offset = offset_iter->second + 1;
        }
        auto iter = indexers_.find(key);
        if (iter != indexers_.end()) {
            if (write_schema) {
                ArrowSchemaRelease(write_schema.get());
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemReadView> read_view,
                                   iter->second->AcquireReadView());
            if (!read_view) {
                return Status::Invalid("mem indexer returned a null read view");
            }
            const std::optional<Range> memory_range = read_view->GetOffsetRange();
            if (memory_range) {
                if (memory_range->to == std::numeric_limits<int64_t>::max()) {
                    return Status::Invalid("real-time offset has reached INT64_MAX");
                }
                // A reused indexer may retain sealed-but-uncommitted rows beyond the committed
                // offset. Continue after all retained rows instead of restarting across a seal.
                if (memory_range->to >= initial_offset) {
                    initial_offset = memory_range->to + 1;
                }
            }
            return RealtimeMemIndexerState{iter->second, initial_offset};
        }
        Result<std::shared_ptr<MemIndexer>> indexer_result =
            factory_->Create(std::move(write_schema), options, memory_pool);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemIndexer> indexer, std::move(indexer_result));
        indexers_.emplace(key, indexer);
        if (offset_iter != committed_offsets_.end()) {
            reclaimed_offsets_.emplace(key, offset_iter->second);
        }
        return RealtimeMemIndexerState{std::move(indexer), initial_offset};
    }

    Result<std::vector<RealtimePartitionBucketView>> AcquireReadViews() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<RealtimePartitionBucketView> result;
        result.reserve(indexers_.size());
        for (const auto& [partition_bucket, indexer] : indexers_) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MemReadView> read_view,
                                   indexer->AcquireReadView());
            result.push_back(
                RealtimePartitionBucketView{partition_bucket, indexer, std::move(read_view)});
        }
        return result;
    }

    Result<std::string> PinReadView(const RealtimePartitionBucketView& view, int64_t ttl_millis) {
        if (!view.indexer || !view.read_view) {
            return Status::Invalid("cannot pin an incomplete real-time read view");
        }
        if (ttl_millis <= 0) {
            return Status::Invalid("real-time read-view TTL must be greater than zero");
        }
        const auto now = std::chrono::steady_clock::now();
        const auto ttl = std::chrono::milliseconds(ttl_millis);
        if (ttl > std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::time_point::max() - now)) {
            return Status::Invalid("real-time read-view TTL is too large");
        }

        while (true) {
            std::string opaque_ticket;
            if (!UUID::Generate(&opaque_ticket)) {
                return Status::IOError("failed to generate a real-time read-view ticket");
            }
            {
                std::lock_guard<std::mutex> lock(read_views_mutex_);
                bool inserted =
                    pinned_read_views_.emplace(opaque_ticket, PinnedReadView{view, now + ttl})
                        .second;
                if (!inserted) {
                    continue;
                }
            }
            read_views_cv_.notify_all();
            return opaque_ticket;
        }
    }

    Result<RealtimePartitionBucketView> ResolveReadView(const std::string& opaque_ticket) {
        bool expired = false;
        {
            std::lock_guard<std::mutex> lock(read_views_mutex_);
            auto iter = pinned_read_views_.find(opaque_ticket);
            if (iter == pinned_read_views_.end()) {
                return Status::Invalid("real-time read-view ticket does not exist or has expired");
            }
            if (iter->second.expire_at <= std::chrono::steady_clock::now()) {
                read_view_release_queue_.push_back(std::move(iter->second.view));
                pinned_read_views_.erase(iter);
                expired = true;
            } else {
                return iter->second.view;
            }
        }
        if (expired) {
            read_views_cv_.notify_one();
        }
        return Status::Invalid("real-time read-view ticket does not exist or has expired");
    }

    Status ReleaseReadView(const std::string& opaque_ticket) {
        bool released = false;
        {
            std::lock_guard<std::mutex> lock(read_views_mutex_);
            auto iter = pinned_read_views_.find(opaque_ticket);
            if (iter == pinned_read_views_.end()) {
                return Status::OK();
            }
            read_view_release_queue_.push_back(std::move(iter->second.view));
            pinned_read_views_.erase(iter);
            released = true;
        }
        if (released) {
            read_views_cv_.notify_one();
        }
        return Status::OK();
    }

    Status AdvanceCommittedProgress(int64_t snapshot_id,
                                    const RealtimeOffsetMap& committed_offsets) {
        if (snapshot_id < 0) {
            return Status::Invalid("real-time refresh snapshot id must not be negative");
        }
        std::lock_guard<std::mutex> progress_lock(progress_mutex_);
        if (last_refreshed_snapshot_id_ && snapshot_id < last_refreshed_snapshot_id_.value()) {
            return Status::Invalid("real-time committed snapshot cannot move backwards");
        }
        if (!last_refreshed_snapshot_id_ || snapshot_id > last_refreshed_snapshot_id_.value()) {
            for (const auto& [partition_bucket, committed_offset] : committed_offsets) {
                if (partition_bucket.bucket < 0 || committed_offset < 0) {
                    return Status::Invalid("invalid partition-bucket committed offset");
                }
                auto previous_iter = committed_offsets_.find(partition_bucket);
                if (previous_iter != committed_offsets_.end()) {
                    if (committed_offset < previous_iter->second) {
                        return Status::Invalid(
                            "real-time partition-bucket committed offset cannot move backwards");
                    }
                }
            }
            committed_offsets_ = committed_offsets;
            last_refreshed_snapshot_id_ = snapshot_id;
        }

        std::vector<std::tuple<RealtimePartitionBucket, std::shared_ptr<MemIndexer>, int64_t>>
            notifications;
        {
            std::lock_guard<std::mutex> registry_lock(mutex_);
            for (const auto& [partition_bucket, committed_offset] : committed_offsets_) {
                auto reclaimed_iter = reclaimed_offsets_.find(partition_bucket);
                if (reclaimed_iter != reclaimed_offsets_.end() &&
                    reclaimed_iter->second >= committed_offset) {
                    continue;
                }
                auto indexer_iter = indexers_.find(partition_bucket);
                if (indexer_iter != indexers_.end()) {
                    notifications.emplace_back(partition_bucket, indexer_iter->second,
                                               committed_offset);
                }
            }
        }
        // Reclaim independent indexers on a best-effort basis, then report the first error.
        Status first_error = Status::OK();
        for (const auto& [partition_bucket, indexer, committed_offset] : notifications) {
            Status status = indexer->AdvanceCommittedOffset(committed_offset);
            if (status.ok()) {
                reclaimed_offsets_[partition_bucket] = committed_offset;
            } else if (first_error.ok()) {
                first_error = std::move(status);
            }
        }
        return first_error;
    }

 private:
    struct PinnedReadView {
        RealtimePartitionBucketView view;
        std::chrono::steady_clock::time_point expire_at;
    };

    void CleanupReadViews() {
        while (true) {
            std::vector<RealtimePartitionBucketView> released_views;
            bool should_stop = false;
            {
                std::unique_lock<std::mutex> lock(read_views_mutex_);
                if (pinned_read_views_.empty() && read_view_release_queue_.empty() && !stopping_) {
                    read_views_cv_.wait(lock, [this]() {
                        return stopping_ || !pinned_read_views_.empty() ||
                               !read_view_release_queue_.empty();
                    });
                }
                if (stopping_) {
                    for (auto& entry : pinned_read_views_) {
                        released_views.push_back(std::move(entry.second.view));
                    }
                    pinned_read_views_.clear();
                    while (!read_view_release_queue_.empty()) {
                        released_views.push_back(std::move(read_view_release_queue_.front()));
                        read_view_release_queue_.pop_front();
                    }
                    should_stop = true;
                } else {
                    const auto now = std::chrono::steady_clock::now();
                    for (auto iter = pinned_read_views_.begin();
                         iter != pinned_read_views_.end();) {
                        if (iter->second.expire_at <= now) {
                            released_views.push_back(std::move(iter->second.view));
                            iter = pinned_read_views_.erase(iter);
                        } else {
                            ++iter;
                        }
                    }
                    for (auto iter = read_view_release_queue_.begin();
                         iter != read_view_release_queue_.end();) {
                        if (iter->read_view.use_count() == 1) {
                            released_views.push_back(std::move(*iter));
                            iter = read_view_release_queue_.erase(iter);
                        } else {
                            ++iter;
                        }
                    }
                    if (released_views.empty()) {
                        std::optional<std::chrono::steady_clock::time_point> next_check;
                        if (!read_view_release_queue_.empty()) {
                            next_check = now + kReadViewReleaseCheckInterval;
                        }
                        if (!pinned_read_views_.empty()) {
                            auto earliest = std::min_element(
                                pinned_read_views_.begin(), pinned_read_views_.end(),
                                [](const auto& left, const auto& right) {
                                    return left.second.expire_at < right.second.expire_at;
                                });
                            if (!next_check || earliest->second.expire_at < next_check.value()) {
                                next_check = earliest->second.expire_at;
                            }
                        }
                        if (next_check) {
                            read_views_cv_.wait_until(lock, next_check.value());
                        }
                        continue;
                    }
                }
            }
            // Destroy plugin views outside the registry lock because they may own large segments.
            released_views.clear();
            if (should_stop) {
                return;
            }
        }
    }

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

Result<std::shared_ptr<RealtimeContext>> RealtimeContext::Create() {
    return Create(std::make_shared<ArrowMemIndexerFactory>());
}

Result<std::shared_ptr<RealtimeContext>> RealtimeContext::Create(
    const std::shared_ptr<MemIndexerFactory>& factory) {
    if (!factory) {
        return Status::Invalid("mem indexer factory is null");
    }
    return std::shared_ptr<RealtimeContext>(new RealtimeContext(std::make_unique<Impl>(factory)));
}

RealtimeContext::RealtimeContext(std::unique_ptr<Impl>&& impl) : impl_(std::move(impl)) {}

RealtimeContext::~RealtimeContext() = default;

Result<RealtimeMemIndexerState> RealtimeContext::GetOrCreateMemIndexer(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    std::unique_ptr<ArrowSchema> write_schema, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    return impl_->GetOrCreateMemIndexer(partition, bucket, std::move(write_schema), options,
                                        memory_pool);
}

Result<std::vector<RealtimePartitionBucketView>> RealtimeContext::AcquireReadViews() {
    return impl_->AcquireReadViews();
}

Result<std::string> RealtimeContext::PinReadView(const RealtimePartitionBucketView& view,
                                                 int64_t ttl_millis) {
    return impl_->PinReadView(view, ttl_millis);
}

Result<RealtimePartitionBucketView> RealtimeContext::ResolveReadView(
    const std::string& opaque_ticket) {
    return impl_->ResolveReadView(opaque_ticket);
}

Status RealtimeContext::ReleaseReadView(const std::string& opaque_ticket) {
    return impl_->ReleaseReadView(opaque_ticket);
}

Status RealtimeContext::AdvanceCommittedProgress(int64_t snapshot_id,
                                                 const RealtimeOffsetMap& committed_offsets) {
    return impl_->AdvanceCommittedProgress(snapshot_id, committed_offsets);
}

}  // namespace paimon
