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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/scan_context.h"

#include <map>
#include <utility>

#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
class Predicate;

ScanContext::ScanContext(bool is_streaming_mode, std::optional<int32_t> limit,
                         const std::shared_ptr<ScanFilter>& scan_filter,
                         const std::shared_ptr<GlobalIndexResult>& global_index_result,
                         const std::shared_ptr<RealtimeContext>& realtime_context,
                         const std::shared_ptr<MemoryPool>& memory_pool,
                         const std::shared_ptr<Executor>& executor,
                         const std::shared_ptr<Cache>& cache)
    : is_streaming_mode_(is_streaming_mode),
      limit_(limit),
      scan_filters_(scan_filter),
      global_index_result_(global_index_result),
      realtime_context_(realtime_context),
      memory_pool_(memory_pool),
      executor_(executor),
      cache_(cache) {}

ScanContext::~ScanContext() = default;

class ScanContextBuilder::Impl {
 public:
    friend class ScanContextBuilder;

    void Reset() {
        is_streaming_mode_ = false;
        limit_ = std::nullopt;
        bucket_filter_ = std::nullopt;
        partition_filters_.clear();
        predicates_.reset();
        global_index_result_.reset();
        realtime_context_.reset();
        memory_pool_ = GetDefaultPool();
        executor_.reset();
        cache_.reset();
    }

 private:
    bool is_streaming_mode_ = false;
    std::optional<int32_t> limit_;
    std::optional<int32_t> bucket_filter_;
    std::vector<std::map<std::string, std::string>> partition_filters_;
    std::shared_ptr<Predicate> predicates_;
    std::shared_ptr<GlobalIndexResult> global_index_result_;
    std::shared_ptr<RealtimeContext> realtime_context_;
    std::shared_ptr<MemoryPool> memory_pool_ = GetDefaultPool();
    // Resolved in Finish(); a builder never owns an executor of its own.
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<Cache> cache_;
};

ScanContextBuilder::ScanContextBuilder() : impl_(std::make_unique<Impl>()) {}

ScanContextBuilder::~ScanContextBuilder() = default;
ScanContextBuilder& ScanContextBuilder::WithStreamingMode(bool is_streaming_mode) {
    impl_->is_streaming_mode_ = is_streaming_mode;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetLimit(int32_t limit) {
    impl_->limit_ = limit;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetBucketFilter(int32_t bucket_filter) {
    impl_->bucket_filter_ = bucket_filter;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetPartitionFilter(
    const std::vector<std::map<std::string, std::string>>& partition_filters) {
    impl_->partition_filters_ = partition_filters;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetPredicate(const std::shared_ptr<Predicate>& predicate) {
    impl_->predicates_ = predicate;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::SetGlobalIndexResult(
    const std::shared_ptr<GlobalIndexResult>& global_index_result) {
    impl_->global_index_result_ = global_index_result;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::WithRealtimeContext(
    const std::shared_ptr<RealtimeContext>& realtime_context) {
    impl_->realtime_context_ = realtime_context;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::WithMemoryPool(
    const std::shared_ptr<MemoryPool>& memory_pool) {
    impl_->memory_pool_ = memory_pool;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::WithExecutor(const std::shared_ptr<Executor>& executor) {
    impl_->executor_ = executor;
    return *this;
}

ScanContextBuilder& ScanContextBuilder::WithCache(const std::shared_ptr<Cache>& cache) {
    impl_->cache_ = cache;
    return *this;
}

Result<std::unique_ptr<ScanContext>> ScanContextBuilder::Finish() {
    std::shared_ptr<Executor> executor =
        impl_->executor_ ? impl_->executor_ : CreateDefaultExecutor();
    auto ctx = std::make_unique<ScanContext>(
        impl_->is_streaming_mode_, impl_->limit_,
        std::make_shared<ScanFilter>(impl_->predicates_, impl_->partition_filters_,
                                     impl_->bucket_filter_),
        impl_->global_index_result_, impl_->realtime_context_, impl_->memory_pool_, executor,
        impl_->cache_);
    impl_->Reset();
    return ctx;
}

}  // namespace paimon
