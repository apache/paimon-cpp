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

// Adapted from Apache ORC
// https://github.com/apache/orc/blob/main/c%2B%2B/src/io/Cache.cc

#include "paimon/common/utils/read_ahead_cache.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <functional>
#include <future>
#include <shared_mutex>
#include <string>

#include "paimon/common/memory/bytes_utils.h"
#include "paimon/common/metrics/atomic_counter_pair.h"
#include "paimon/common/utils/byte_range_combiner.h"
#include "paimon/common/utils/file_block_cache.h"
#include "paimon/common/utils/io_trace.h"
#include "paimon/common/utils/math.h"
#include "paimon/memory/bytes.h"
#include "paimon/metrics.h"

namespace paimon {

struct RangeCacheEntry {
    ByteRange range;
    std::shared_ptr<Bytes> buffer;
    std::shared_future<Status> future;  // use shared_future in case of multiple get calls

    RangeCacheEntry() = default;
    RangeCacheEntry(const ByteRange& range, std::shared_ptr<Bytes> buffer,
                    std::future<Status> future)
        : range(range), buffer(std::move(buffer)), future(std::move(future).share()) {}

    friend bool operator<(const RangeCacheEntry& left, const RangeCacheEntry& right) {
        return left.range.offset < right.range.offset;
    }
};

// Everything needed to dispatch the prefetch IO of an entry AFTER the entry
// has been published into entries_: the promise resolves the entry's future
// and the buffer capture keeps the destination alive for the async IO.
struct PendingFetch {
    ByteRange range;
    std::shared_ptr<Bytes> buffer;
    std::shared_ptr<std::promise<Status>> promise;
};

namespace {

// Copy the requested window out of the covering entries into dest. The
// entries must fully cover the range and their futures must be resolved.
void CopyRangeFromEntries(const std::vector<RangeCacheEntry>& covering, const ByteRange& range,
                          char* dest) {
    size_t pos = 0;
    for (const auto& entry : covering) {
        const uint64_t entry_end = entry.range.offset + entry.range.length;
        const uint64_t copy_begin = std::max(range.offset, entry.range.offset);
        const uint64_t copy_end = std::min(range.offset + range.length, entry_end);
        const auto copy_len = static_cast<size_t>(copy_end - copy_begin);
        std::memcpy(dest + pos, entry.buffer->data() + (copy_begin - entry.range.offset), copy_len);
        pos += copy_len;
    }
}

}  // namespace

class ReadAheadCache::Impl {
 public:
    Impl(const std::shared_ptr<InputStream>& stream, const CacheConfig& config, uint64_t file_size,
         const std::shared_ptr<MemoryPool>& memory_pool);
    ~Impl();

    Status Init(std::vector<ByteRange>&& ranges);
    Result<bool> Read(const ByteRange& range, char* dest);
    void ReadAsync(const ByteRange& range, char* dest, std::function<void(Status, bool)> callback);
    void Reset();
    void ReleaseBuffers();
    void Warmup();
    void CollectMetrics(std::shared_ptr<Metrics>* metrics) const;

 private:
    /// Dispatch the prefetch IOs for entries that have already been published
    /// into entries_.
    void DispatchFetches(const std::vector<PendingFetch>& fetches);
    /// Find the entries fully covering the given range under the read lock.
    /// Returns an empty vector on miss. Entries are copied (shared buffers)
    /// so the caller may use them after releasing the lock.
    std::vector<RangeCacheEntry> FindCoveringEntries(const ByteRange& range);

    /// The front half of every read, shared by Read() and ReadAsync(): count the
    /// request, prefetch from the offset it reads at and look up the registered
    /// ranges covering it.
    /// @return the covering entries, empty when no registered range covers the
    /// read.
    std::vector<RangeCacheEntry> PrepareRead(const ByteRange& range);

    /// The back half of a read that registered ranges cover, shared by Read() and
    /// ReadAsync(): wait for the covering prefetches, copy the requested window
    /// out of them and count the hit.
    ///
    /// Waits synchronously: the prefetches have already been dispatched, so
    /// waiting for them is the latency hiding this cache exists for.
    /// @return true when the range was served and `dest` was filled.
    /// @param status Set to the failure of the covering prefetch when false is
    /// returned, which the caller must report: the bytes were expected to be in
    /// the cache and reading them again would fail the same way.
    bool ServeCovering(const std::vector<RangeCacheEntry>& covering, const ByteRange& range,
                       char* dest, Status* status);
    void PreBuffer(uint64_t offset);

    /// Mark, publish and fetch the pending ranges at the given indices.
    ///
    /// Marking is_cached_ and publishing the promise-backed entries happen
    /// atomically under the write lock, before any IO is dispatched, so a
    /// reader racing the prefetch waits on the in-flight entries instead of
    /// re-fetching the same bytes.
    void Cache(std::vector<size_t> pending_indices);

    /// Clear the prefetch state, waiting for the fetches still writing into the
    /// entry buffers. Leaves the block cache untouched.
    void ReleasePrefetchBuffers();

    std::shared_ptr<InputStream> stream_;
    CacheConfig config_;
    // Ordered by offset (so as to find a matching region by binary search)
    std::vector<RangeCacheEntry> entries_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_mutex rw_mutex_;
    std::vector<std::atomic<bool>> is_cached_;
    std::vector<ByteRange> pending_ranges_;
    bool is_initialized_ = false;
    // Caches the reads that no registered range covers, or null when the block
    // cache is disabled. Owns its own locking and counters.
    std::unique_ptr<FileBlockCache> block_cache_;
    // The Read() requests issued to the cache and how they were served,
    // aggregated over all the streams sharing this cache. A read is counted
    // either as a hit, a block cache hit or a miss. The misses are shared: the
    // continuation of an asynchronous read counts them from the thread resolving
    // the block fetch, which can outlive this cache.
    AtomicCounterPair reads_;
    AtomicCounterPair hits_;
    std::shared_ptr<AtomicCounterPair> misses_;
    // The prefetch IO actually issued to the underlying stream. The block cache
    // counts its own fetches, which CollectMetrics() adds to these.
    AtomicCounterPair ios_;
    // TEMPORARY: the file the traced IOs read, resolved once, empty when the
    // tracing is off. See io_trace.h.
    std::string trace_uri_;
};

void ReadAheadCache::Impl::Cache(std::vector<size_t> pending_indices) {
    std::vector<RangeCacheEntry> new_entries;
    std::vector<PendingFetch> fetches;
    // Mark is_cached_, publish the promise-backed entries and only then
    // dispatch the IOs. The mark and the publication happen atomically under
    // the write lock: a reader racing the prefetch observes is_cached_=true
    // only once the covering entries are already visible, so it waits on
    // their futures instead of issuing a duplicate underlying read.
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        for (size_t idx : pending_indices) {
            if (is_cached_[idx].exchange(true)) {
                continue;
            }
            const ByteRange& range = pending_ranges_[idx];
            auto promise = std::make_shared<std::promise<Status>>();
            auto future = promise->get_future();
            auto buffer = AllocateBytesKeepingPoolAlive(range.length, memory_pool_);
            fetches.push_back({range, buffer, promise});
            new_entries.emplace_back(range, std::move(buffer), std::move(future));
        }
        if (!new_entries.empty()) {
            // Entries are never evicted: the cache holds every published
            // range until ReleaseBuffers()/Reset(), so an in-flight fetch
            // always keeps its entry and thus its future reachable.
            std::vector<RangeCacheEntry> merged(entries_.size() + new_entries.size());
            std::merge(entries_.begin(), entries_.end(), new_entries.begin(), new_entries.end(),
                       merged.begin());
            entries_ = std::move(merged);
        }
    }
    DispatchFetches(fetches);
}

Status ReadAheadCache::Impl::Init(std::vector<ByteRange>&& ranges) {
    if (is_initialized_) {
        return Status::Invalid("Cache has already been initialized");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<ByteRange> pending_ranges,
        ByteRangeCombiner::CoalesceByteRanges(std::move(ranges), config_.GetHoleSizeLimit(),
                                              config_.GetRangeSizeLimit()));
    for (const auto& pending_range : pending_ranges) {
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int64_t>(pending_range.offset, "range offset"));
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int64_t>(pending_range.length, "range length"));
    }
    pending_ranges_ = pending_ranges;
    is_cached_ = std::vector<std::atomic<bool>>(pending_ranges_.size());
    for (auto& is_cached : is_cached_) {
        is_cached.store(false);
    }
    is_initialized_ = true;
    return Status::OK();
}

void ReadAheadCache::Impl::PreBuffer(uint64_t offset) {
    auto it = std::lower_bound(pending_ranges_.begin(), pending_ranges_.end(), offset,
                               [](const ByteRange& range, uint64_t offset) {
                                   return range.offset + range.length <= offset;
                               });
    if (it == pending_ranges_.end() || it->offset > offset) {
        return;
    }

    size_t start_idx = std::distance(pending_ranges_.begin(), it);
    std::vector<size_t> pending_indices;
    size_t total_bytes = 0;
    for (size_t i = start_idx; i < pending_ranges_.size(); ++i) {
        total_bytes += pending_ranges_[i].length;
        if (total_bytes > config_.GetPreBufferLimit()) {
            break;
        }
        pending_indices.push_back(i);
    }

    if (!pending_indices.empty()) {
        Cache(std::move(pending_indices));
    }
}

ReadAheadCache::Impl::Impl(const std::shared_ptr<InputStream>& stream, const CacheConfig& config,
                           uint64_t file_size, const std::shared_ptr<MemoryPool>& memory_pool)
    : stream_(stream),
      config_(config),
      memory_pool_(memory_pool),
      misses_(std::make_shared<AtomicCounterPair>()),
      trace_uri_(io_trace::Uri(stream)) {
    // An unknown file size cannot be aligned to, and a zero limit or block size
    // means the block cache is turned off: leave it null in those cases.
    if (file_size > 0 && config_.GetBlockSize() > 0 && config_.GetBlockCacheLimit() > 0) {
        block_cache_ = std::make_unique<FileBlockCache>(stream, file_size, config_.GetBlockSize(),
                                                        config_.GetBlockCacheLimit(), memory_pool);
    }
}

ReadAheadCache::Impl::~Impl() {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    for (auto& entry : entries_) {
        entry.future.wait();
    }
    // The block cache waits for its own fetches when it is destroyed.
}

void ReadAheadCache::Impl::Reset() {
    ReleasePrefetchBuffers();
    reads_.Reset();
    hits_.Reset();
    misses_->Reset();
    ios_.Reset();
    if (block_cache_ != nullptr) {
        // Only the counters: the blocks cache the file, not the registered
        // ranges, and a reader resetting the cache reads the same file again.
        block_cache_->ResetCounters();
    }
}

void ReadAheadCache::Impl::ReleaseBuffers() {
    ReleasePrefetchBuffers();
    if (block_cache_ != nullptr) {
        block_cache_->Release();
    }
}

void ReadAheadCache::Impl::ReleasePrefetchBuffers() {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    // Entries are never evicted, so waiting on entries_ covers every
    // dispatched fetch: no fetch is still writing into an entry buffer when the
    // buffers go away. The buffers keep the memory pool alive themselves, for
    // the callbacks that a stream destroys later than it resolves them.
    for (auto& entry : entries_) {
        entry.future.wait();
    }
    entries_.clear();
    is_cached_.clear();
    pending_ranges_.clear();
    is_initialized_ = false;
    // The read/io counters are deliberately kept: a reader closed at EOF must
    // still be able to report them through CollectMetrics().
}

void ReadAheadCache::Impl::CollectMetrics(std::shared_ptr<Metrics>* metrics) const {
    if (metrics == nullptr || !*metrics) {
        return;
    }
    auto& m = *metrics;
    m->SetCounter(ReadAheadCacheMetrics::READ_COUNT, reads_.Count());
    m->SetCounter(ReadAheadCacheMetrics::READ_BYTES, reads_.Bytes());
    m->SetCounter(ReadAheadCacheMetrics::READ_HITS, hits_.Count());
    m->SetCounter(ReadAheadCacheMetrics::READ_HIT_BYTES, hits_.Bytes());
    m->SetCounter(ReadAheadCacheMetrics::READ_MISSES, misses_->Count());
    m->SetCounter(ReadAheadCacheMetrics::READ_MISS_BYTES, misses_->Bytes());
    // The block cache keeps its own counters. Its fetches also go to the
    // underlying stream, so they are part of the io counters too.
    const FileBlockCache::Counters blocks =
        block_cache_ != nullptr ? block_cache_->GetCounters() : FileBlockCache::Counters{};
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_HITS, blocks.hits);
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_HIT_BYTES, blocks.hit_bytes);
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_FETCHES, blocks.fetches);
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_FETCH_BYTES, blocks.fetch_bytes);
    m->SetCounter(ReadAheadCacheMetrics::IO_COUNT, ios_.Count() + blocks.fetches);
    m->SetCounter(ReadAheadCacheMetrics::IO_BYTES, ios_.Bytes() + blocks.fetch_bytes);
}

void ReadAheadCache::Impl::Warmup() {
    // Init() only registers the pending ranges; without this the first fetch
    // starts when the first Read() arrives, racing the reader's own miss fetch.
    if (!pending_ranges_.empty()) {
        PreBuffer(pending_ranges_.front().offset);
    }
}

std::vector<RangeCacheEntry> ReadAheadCache::Impl::FindCoveringEntries(const ByteRange& range) {
    std::vector<RangeCacheEntry> covering;
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    // Find the entry holding the start of the range: the first entry whose
    // end is beyond range.offset (entries are disjoint and sorted by offset).
    auto it = std::lower_bound(entries_.begin(), entries_.end(), range.offset,
                               [](const RangeCacheEntry& e, uint64_t offset) {
                                   return e.range.offset + e.range.length <= offset;
                               });
    if (it == entries_.end() || it->range.offset > range.offset) {
        return covering;
    }
    if (it->range.Contains(range)) {
        covering.push_back(*it);
        return covering;
    }
    // The request spans several adjacent entries (a column chunk larger than
    // one coalesced range): collect the contiguous run and check it covers
    // the whole request. Entries are published before their fetch is
    // dispatched, so a reader racing the prefetch waits for the in-flight
    // fetch instead of issuing a second one for the same bytes.
    uint64_t covered_end = it->range.offset + it->range.length;
    covering.push_back(*it);
    auto next = std::next(it);
    while (covered_end < range.offset + range.length && next != entries_.end() &&
           next->range.offset == covered_end) {
        covered_end = next->range.offset + next->range.length;
        covering.push_back(*next);
        ++next;
    }
    if (covered_end < range.offset + range.length) {
        covering.clear();
    }
    return covering;
}

std::vector<RangeCacheEntry> ReadAheadCache::Impl::PrepareRead(const ByteRange& range) {
    reads_.Add(range.length);
    PreBuffer(range.offset);
    return FindCoveringEntries(range);
}

bool ReadAheadCache::Impl::ServeCovering(const std::vector<RangeCacheEntry>& covering,
                                         const ByteRange& range, char* dest, Status* status) {
    // TEMPORARY: trace how long the reader waits for the prefetches covering its
    // read, which is the latency this cache did not manage to hide. See
    // io_trace.h.
    const bool trace = io_trace::Enabled();
    const io_trace::Instant wait_started_at = trace ? io_trace::Now() : io_trace::Instant{};
    // Wait OUTSIDE the lock: the futures resolve when the prefetch stream's
    // async reads complete, and holding rw_mutex_ would block Cache().
    for (const auto& entry : covering) {
        const Status prefetch_status = entry.future.get();
        if (!prefetch_status.ok()) {
            if (trace) {
                io_trace::Emit("prefetch-wait", trace_uri_, range.offset, range.length,
                               wait_started_at, io_trace::ElapsedMicros(wait_started_at),
                               io_trace::kNotApplicable, prefetch_status.ToString().c_str());
            }
            *status = prefetch_status;
            return false;
        }
    }
    if (trace) {
        io_trace::Emit("prefetch-wait", trace_uri_, range.offset, range.length, wait_started_at,
                       io_trace::ElapsedMicros(wait_started_at), io_trace::kNotApplicable, "ok");
    }
    // The data copy runs OUTSIDE the lock for the same reason.
    CopyRangeFromEntries(covering, range, dest);
    hits_.Add(range.length);
    return true;
}

Result<bool> ReadAheadCache::Impl::Read(const ByteRange& range, char* dest) {
    if (range.length == 0) {
        return true;
    }
    std::vector<RangeCacheEntry> covering = PrepareRead(range);
    if (covering.empty()) {
        // No registered range covers this read: the block cache can still serve
        // it, and then serve the readers of the other streams sharing this cache
        // that are about to read the same bytes.
        if (block_cache_ != nullptr && block_cache_->Read(range, dest)) {
            // The block cache counts its own hits, see CollectMetrics().
            return true;
        }
        misses_->Add(range.length);
        // TEMPORARY: trace the reads this cache declines, which the caller then
        // reads itself: IO this cache did not manage to fold into a prefetch.
        if (io_trace::Enabled()) {
            io_trace::Emit("cache-miss", trace_uri_, range.offset, range.length, io_trace::Now(),
                           io_trace::kNotApplicable, io_trace::kNotApplicable, nullptr);
        }
        return false;
    }
    Status status;
    if (!ServeCovering(covering, range, dest, &status)) {
        return status;
    }
    return true;
}

void ReadAheadCache::Impl::ReadAsync(const ByteRange& range, char* dest,
                                     std::function<void(Status, bool)> callback) {
    if (range.length == 0) {
        callback(Status::OK(), true);
        return;
    }
    std::vector<RangeCacheEntry> covering = PrepareRead(range);
    if (!covering.empty()) {
        Status status;
        if (!ServeCovering(covering, range, dest, &status)) {
            callback(status, false);
            return;
        }
        callback(Status::OK(), true);
        return;
    }
    if (block_cache_ == nullptr) {
        misses_->Add(range.length);
        // TEMPORARY: see the miss trace in Read().
        if (io_trace::Enabled()) {
            io_trace::Emit("cache-miss", trace_uri_, range.offset, range.length, io_trace::Now(),
                           io_trace::kNotApplicable, io_trace::kNotApplicable, nullptr);
        }
        callback(Status::OK(), false);
        return;
    }
    // The continuation runs on the thread resolving the block fetch, which may
    // outlive this cache: it counts into the shared counters and touches only the
    // block buffer, never `this`.
    std::shared_ptr<AtomicCounterPair> misses = misses_;
    const uint64_t length = range.length;
    // TEMPORARY: the uri is copied for the same reason, see io_trace.h.
    block_cache_->ReadAsync(range, dest,
                            [misses, length, offset = range.offset, uri = trace_uri_,
                             callback = std::move(callback)](bool served) {
                                if (!served) {
                                    misses->Add(length);
                                    if (io_trace::Enabled()) {
                                        io_trace::Emit("cache-miss", uri, offset, length,
                                                       io_trace::Now(), io_trace::kNotApplicable,
                                                       io_trace::kNotApplicable, nullptr);
                                    }
                                }
                                callback(Status::OK(), served);
                            });
}

void ReadAheadCache::Impl::DispatchFetches(const std::vector<PendingFetch>& fetches) {
    for (const auto& fetch : fetches) {
        auto promise = fetch.promise;
        auto buffer = fetch.buffer;
        auto read_size = static_cast<int64_t>(buffer->size());
        auto read_offset = static_cast<int64_t>(fetch.range.offset);
        // TEMPORARY: trace the IO this cache issues, see io_trace.h. The uri is
        // copied into the callback rather than reached for through `this`, which
        // the thread resolving the fetch may outlive.
        const bool trace = io_trace::Enabled();
        io_trace::Instant dispatched_at;
        if (trace) {
            dispatched_at = io_trace::Now();
            io_trace::Emit("prefetch-dispatch", trace_uri_, fetch.range.offset, fetch.range.length,
                           dispatched_at, io_trace::kNotApplicable, io_trace::EnterInflight(),
                           nullptr);
        }
        stream_->ReadAsync(buffer->data(), read_size, read_offset,
                           [promise, buffer, trace, dispatched_at, range = fetch.range,
                            uri = trace_uri_](Status status) mutable {
                               if (trace) {
                                   const int64_t elapsed = io_trace::ElapsedMicros(dispatched_at);
                                   io_trace::Emit("prefetch-done", uri, range.offset, range.length,
                                                  dispatched_at, elapsed, io_trace::LeaveInflight(),
                                                  status.ok() ? "ok" : status.ToString().c_str());
                               }
                               promise->set_value(status);
                           });
        ios_.Add(fetch.range.length);
    }
}

ReadAheadCache::ReadAheadCache(const std::shared_ptr<InputStream>& stream,
                               const CacheConfig& config, uint64_t file_size,
                               const std::shared_ptr<MemoryPool>& memory_pool)
    : impl_(std::make_unique<Impl>(stream, config, file_size, memory_pool)) {}

ReadAheadCache::~ReadAheadCache() = default;

Status ReadAheadCache::Init(std::vector<ByteRange>&& ranges) {
    return impl_->Init(std::move(ranges));
}

Result<bool> ReadAheadCache::Read(const ByteRange& range, char* dest) {
    return impl_->Read(range, dest);
}

void ReadAheadCache::ReadAsync(const ByteRange& range, char* dest,
                               std::function<void(Status status, bool served)> callback) {
    impl_->ReadAsync(range, dest, std::move(callback));
}

void ReadAheadCache::Reset() {
    return impl_->Reset();
}

void ReadAheadCache::ReleaseBuffers() {
    return impl_->ReleaseBuffers();
}

void ReadAheadCache::Warmup() {
    impl_->Warmup();
}

void ReadAheadCache::CollectMetrics(std::shared_ptr<Metrics>* metrics) const {
    impl_->CollectMetrics(metrics);
}

}  // namespace paimon
