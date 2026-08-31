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

#include "paimon/core/realtime/realtime_context_impl.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/arrow/abi.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/macros.h"
#include "paimon/realtime/realtime_store.h"
#include "paimon/status.h"

namespace paimon {
namespace {

std::string PartitionToString(const std::map<std::string, std::string>& partition) {
    std::string result = "{";
    for (auto iter = partition.begin(); iter != partition.end(); ++iter) {
        if (iter != partition.begin()) {
            result += ", ";
        }
        result += iter->first + "=" + iter->second;
    }
    return result + "}";
}

}  // namespace

RealtimeContextImpl::RealtimeContextImpl(const std::shared_ptr<RealtimeStoreFactory>& factory)
    : factory_(factory) {}

RealtimeContextImpl::~RealtimeContextImpl() {
    {
        std::lock_guard<std::mutex> lock(read_views_mutex_);
        stopping_ = true;
    }
    read_views_cv_.notify_all();
    if (read_view_cleanup_thread_.joinable()) {
        read_view_cleanup_thread_.join();
    }
}

Result<std::shared_ptr<RealtimeContextImpl>> RealtimeContextImpl::Create(
    const std::shared_ptr<RealtimeStoreFactory>& factory) {
    if (!factory) {
        return Status::Invalid("real-time store factory is null");
    }
    std::shared_ptr<RealtimeContextImpl> context(new RealtimeContextImpl(factory));
    PAIMON_RETURN_NOT_OK(context->Start());
    return context;
}

Status RealtimeContextImpl::Start() {
    try {
        read_view_cleanup_thread_ = std::thread([this]() { CleanupReadViews(); });
    } catch (const std::system_error& e) {
        return Status::UnknownError(
            std::string("failed to start real-time read-view cleanup thread: ") + e.what());
    }
    return Status::OK();
}

Result<RealtimeStoreState> RealtimeContextImpl::GetOrCreateRealtimeStore(
    RealtimeStoreCreateRequest&& request, const RealtimePartitionBucket& partition_bucket) {
    if (!request.write_schema || !request.write_schema->release) {
        return Status::Invalid("real-time store write schema is null");
    }
    ScopeGuard schema_guard(
        [schema = request.write_schema.get()]() { ArrowSchemaRelease(schema); });
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> requested_schema,
                                      arrow::ImportSchema(request.write_schema.get()));
    schema_guard.Release();
    std::lock_guard<std::mutex> progress_lock(progress_mutex_);
    std::lock_guard<std::mutex> registry_lock(mutex_);
    auto iter = stores_.find(partition_bucket);
    int64_t initial_offset = 0;
    auto offset_iter = committed_offsets_.find(partition_bucket);
    if (offset_iter != committed_offsets_.end()) {
        if (offset_iter->second == std::numeric_limits<int64_t>::max()) {
            return Status::Invalid("real-time offset has reached INT64_MAX");
        }
        initial_offset = offset_iter->second;
    }
    if (iter != stores_.end()) {
        if (iter->second.mode != request.mode ||
            !iter->second.write_schema->Equals(*requested_schema, /*check_metadata=*/true)) {
            return Status::Invalid(fmt::format(
                "real-time store schema or mode mismatch for partition {}, bucket {}; recreate "
                "the RealtimeContext",
                PartitionToString(partition_bucket.partition), partition_bucket.bucket));
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeReadView> read_view,
                               iter->second.store->AcquireReadView());
        if (!read_view) {
            return Status::Invalid("real-time store returned a null read view");
        }
        const std::optional<OffsetRange> memory_range = read_view->GetOffsetRange();
        if (memory_range) {
            if (memory_range->end == std::numeric_limits<int64_t>::max()) {
                return Status::Invalid("real-time offset has reached INT64_MAX");
            }
            // A reused store may retain sealed-but-uncommitted rows beyond the committed
            // offset. Continue after all retained rows instead of restarting across a seal.
            if (memory_range->end > initial_offset) {
                initial_offset = memory_range->end;
            }
        }
        return RealtimeStoreState{iter->second.store, initial_offset};
    }
    if (!request.memory_pool) {
        return Status::Invalid("real-time store memory pool is null");
    }
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportSchema(*requested_schema, request.write_schema.get()));
    RealtimeStoreMode mode = request.mode;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeStore> store,
                           factory_->Create(std::move(request)));
    if (!store) {
        return Status::Invalid("real-time store factory returned a null store");
    }
    stores_.emplace(partition_bucket, StoreEntry{store, requested_schema, mode});
    if (offset_iter != committed_offsets_.end()) {
        reclaimed_offsets_.emplace(partition_bucket, offset_iter->second);
    }
    return RealtimeStoreState{std::move(store), initial_offset};
}

Result<int64_t> RealtimeContextImpl::AdvanceMaterializedMaxSequenceNumber(
    const RealtimePartitionBucket& partition_bucket, int64_t max_sequence_number) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = stores_.find(partition_bucket);
    if (iter == stores_.end()) {
        return Status::KeyError(fmt::format("real-time store not found for partition {}, bucket {}",
                                            PartitionToString(partition_bucket.partition),
                                            partition_bucket.bucket));
    }
    StoreEntry& entry = iter->second;
    if (max_sequence_number > entry.materialized_max_sequence_number) {
        entry.materialized_max_sequence_number = max_sequence_number;
    }
    return entry.materialized_max_sequence_number;
}

Result<std::vector<RealtimePartitionBucketView>> RealtimeContextImpl::AcquireReadViews() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<RealtimePartitionBucketView> result;
    result.reserve(stores_.size());
    for (const auto& [partition_bucket, store] : stores_) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeReadView> read_view,
                               store.store->AcquireReadView());
        if (!read_view) {
            return Status::Invalid("real-time store returned a null read view");
        }
        result.push_back(
            RealtimePartitionBucketView{partition_bucket, store.store, std::move(read_view)});
    }
    return result;
}

Result<std::string> RealtimeContextImpl::PinReadView(const RealtimePartitionBucketView& view,
                                                     int64_t ttl_millis) {
    if (!view.store || !view.read_view) {
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
                pinned_read_views_.emplace(opaque_ticket, PinnedReadView{view, now + ttl}).second;
            if (!inserted) {
                continue;
            }
        }
        read_views_cv_.notify_all();
        return opaque_ticket;
    }
}

Result<RealtimePartitionBucketView> RealtimeContextImpl::ResolveReadView(
    const std::string& opaque_ticket) {
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

Status RealtimeContextImpl::ReleaseReadView(const std::string& opaque_ticket) {
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

Status RealtimeContextImpl::AdvanceCommittedProgress(int64_t snapshot_id,
                                                     const RealtimeOffsetMap& committed_offsets) {
    if (snapshot_id < 0) {
        return Status::Invalid("real-time refresh snapshot id must not be negative");
    }
    std::lock_guard<std::mutex> progress_lock(progress_mutex_);
    if (last_refreshed_snapshot_id_ && snapshot_id < last_refreshed_snapshot_id_.value()) {
        return Status::Invalid("real-time committed snapshot cannot move backwards");
    }
    if (!last_refreshed_snapshot_id_ || snapshot_id > last_refreshed_snapshot_id_.value()) {
        for (const auto& [partition_bucket, committed_end_offset] : committed_offsets) {
            if (partition_bucket.bucket < 0 || committed_end_offset < 0) {
                return Status::Invalid("invalid partition-bucket committed offset");
            }
        }
        // Only stores created by this context can contain state which cannot be restored in
        // place. Offsets for other partition-buckets are reference state for lazy store creation
        // and may be removed or rolled back without rebuilding the context.
        std::lock_guard<std::mutex> registry_lock(mutex_);
        for (const auto& store_entry : stores_) {
            const RealtimePartitionBucket& partition_bucket = store_entry.first;
            auto previous_iter = committed_offsets_.find(partition_bucket);
            if (previous_iter == committed_offsets_.end()) {
                continue;
            }

            auto current_iter = committed_offsets.find(partition_bucket);
            if (current_iter == committed_offsets.end()) {
                return Status::Invalid(
                    "real-time committed progress removed an active partition-bucket; recreate "
                    "RealtimeContext");
            }
            if (current_iter->second < previous_iter->second) {
                return Status::Invalid(
                    "real-time committed offset moved backwards for an active partition-bucket; "
                    "recreate RealtimeContext");
            }
        }
        committed_offsets_ = committed_offsets;
        last_refreshed_snapshot_id_ = snapshot_id;
    }

    std::vector<std::tuple<RealtimePartitionBucket, std::shared_ptr<RealtimeStore>, int64_t>>
        notifications;
    {
        std::lock_guard<std::mutex> registry_lock(mutex_);
        for (const auto& [partition_bucket, committed_end_offset] : committed_offsets_) {
            auto reclaimed_iter = reclaimed_offsets_.find(partition_bucket);
            if (reclaimed_iter != reclaimed_offsets_.end() &&
                reclaimed_iter->second >= committed_end_offset) {
                continue;
            }
            auto store_iter = stores_.find(partition_bucket);
            if (store_iter != stores_.end()) {
                notifications.emplace_back(partition_bucket, store_iter->second.store,
                                           committed_end_offset);
            }
        }
    }
    // Reclaim independent stores on a best-effort basis, then report the first error.
    Status first_error = Status::OK();
    for (const auto& [partition_bucket, store, committed_end_offset] : notifications) {
        Status status = store->AdvanceCommittedOffset(committed_end_offset);
        if (status.ok()) {
            reclaimed_offsets_[partition_bucket] = committed_end_offset;
        } else if (first_error.ok()) {
            first_error = std::move(status);
        }
    }
    return first_error;
}

void RealtimeContextImpl::CleanupReadViews() {
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
                for (auto iter = pinned_read_views_.begin(); iter != pinned_read_views_.end();) {
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

Result<std::shared_ptr<RealtimeContextImpl>> RealtimeContextImpl::Cast(
    const std::shared_ptr<RealtimeContext>& context) {
    std::shared_ptr<RealtimeContextImpl> impl =
        std::dynamic_pointer_cast<RealtimeContextImpl>(context);
    if (!impl) {
        return Status::Invalid("invalid real-time context implementation");
    }
    return impl;
}

}  // namespace paimon
