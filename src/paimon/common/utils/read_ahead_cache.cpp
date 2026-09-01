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
#include <future>
#include <shared_mutex>

#include "paimon/common/utils/byte_range_combiner.h"
#include "paimon/common/utils/file_block_cache.h"
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

CacheConfig::CacheConfig(uint64_t range_size_limit, uint64_t hole_size_limit,
                         uint64_t pre_buffer_limit)
    : range_size_limit_(range_size_limit),
      hole_size_limit_(hole_size_limit),
      pre_buffer_limit_(pre_buffer_limit) {}

CacheConfig::CacheConfig()
    // Aligned with the reader's request granularity and with realistic data
    // file sizes:
    // - range_size_limit matches the parquet reader's 32 MiB request blocks
    //   (Arrow ReadRangeCache's own range limit); a smaller limit cuts entries
    //   below the request size, so a request can never be served from one piece.
    // - pre_buffer_limit must exceed the LARGEST single read a reader issues
    //   (coalesced column-chunk reads of ~128 MiB were observed): fetches are
    //   only dispatched up to this window, so a request reaching past it can
    //   never be served and falls back to a second fetch of the same bytes.
    : CacheConfig(/*range_size_limit=*/32 * 1024 * 1024,
                  /*hole_size_limit=*/8 * 1024,
                  /*pre_buffer_limit=*/256 * 1024 * 1024) {}

class ReadAheadCache::Impl {
 public:
    Impl(const std::shared_ptr<InputStream>& stream, const CacheConfig& config, uint64_t file_size,
         const std::shared_ptr<MemoryPool>& memory_pool);
    ~Impl();

    Status Init(std::vector<ByteRange>&& ranges);
    Result<bool> Read(const ByteRange& range, char* dest);
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
    void PreBuffer(uint64_t offset);
    void CountHit(uint64_t size) {
        hits_.fetch_add(1, std::memory_order_relaxed);
        hit_bytes_.fetch_add(size, std::memory_order_relaxed);
    }
    void CountMiss(uint64_t size) {
        misses_.fetch_add(1, std::memory_order_relaxed);
        miss_bytes_.fetch_add(size, std::memory_order_relaxed);
    }

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
    // Statistics of the Read() requests issued to the cache, aggregated over
    // all streams sharing this cache.
    std::atomic<uint64_t> read_count_{0};
    std::atomic<uint64_t> read_bytes_{0};
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> hit_bytes_{0};
    std::atomic<uint64_t> misses_{0};
    std::atomic<uint64_t> miss_bytes_{0};
    // Prefetch IO statistics: how many requests and bytes were actually issued
    // to the underlying stream.
    std::atomic<uint64_t> io_count_{0};
    std::atomic<uint64_t> io_bytes_{0};
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
            auto buffer = std::make_shared<Bytes>(range.length, memory_pool_.get());
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
    : stream_(stream), config_(config), memory_pool_(memory_pool) {
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
    read_count_.store(0, std::memory_order_relaxed);
    read_bytes_.store(0, std::memory_order_relaxed);
    hits_.store(0, std::memory_order_relaxed);
    hit_bytes_.store(0, std::memory_order_relaxed);
    misses_.store(0, std::memory_order_relaxed);
    miss_bytes_.store(0, std::memory_order_relaxed);
    io_count_.store(0, std::memory_order_relaxed);
    io_bytes_.store(0, std::memory_order_relaxed);
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
    // dispatched fetch: no async callback can outlive the stream or the
    // memory pool its buffer belongs to.
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
    m->SetCounter(ReadAheadCacheMetrics::READ_COUNT, read_count_.load(std::memory_order_relaxed));
    m->SetCounter(ReadAheadCacheMetrics::READ_BYTES, read_bytes_.load(std::memory_order_relaxed));
    m->SetCounter(ReadAheadCacheMetrics::READ_HITS, hits_.load(std::memory_order_relaxed));
    m->SetCounter(ReadAheadCacheMetrics::READ_HIT_BYTES,
                  hit_bytes_.load(std::memory_order_relaxed));
    m->SetCounter(ReadAheadCacheMetrics::READ_MISSES, misses_.load(std::memory_order_relaxed));
    m->SetCounter(ReadAheadCacheMetrics::READ_MISS_BYTES,
                  miss_bytes_.load(std::memory_order_relaxed));
    // The block cache keeps its own counters. Its fetches also go to the
    // underlying stream, so they are part of the io counters too.
    const FileBlockCache::Counters blocks =
        block_cache_ != nullptr ? block_cache_->GetCounters() : FileBlockCache::Counters{};
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_HITS, blocks.hits);
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_HIT_BYTES, blocks.hit_bytes);
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_FETCHES, blocks.fetches);
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_FETCH_BYTES, blocks.fetch_bytes);
    m->SetCounter(ReadAheadCacheMetrics::IO_COUNT,
                  io_count_.load(std::memory_order_relaxed) + blocks.fetches);
    m->SetCounter(ReadAheadCacheMetrics::IO_BYTES,
                  io_bytes_.load(std::memory_order_relaxed) + blocks.fetch_bytes);
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

Result<bool> ReadAheadCache::Impl::Read(const ByteRange& range, char* dest) {
    if (range.length == 0) {
        return true;
    }
    read_count_.fetch_add(1, std::memory_order_relaxed);
    read_bytes_.fetch_add(range.length, std::memory_order_relaxed);
    PreBuffer(range.offset);
    std::vector<RangeCacheEntry> covering = FindCoveringEntries(range);
    if (covering.empty()) {
        // No registered range covers this read: the block cache can still serve
        // it, and then serve the readers of the other streams sharing this cache
        // that are about to read the same bytes.
        if (block_cache_ != nullptr && block_cache_->Read(range, dest)) {
            // The block cache counts its own hits, see CollectMetrics().
            return true;
        }
        CountMiss(range.length);
        return false;
    }
    // Wait OUTSIDE the lock: the futures resolve when the prefetch stream's
    // async reads complete, and holding rw_mutex_ would block Cache().
    for (const auto& entry : covering) {
        PAIMON_RETURN_NOT_OK(entry.future.get());
    }
    // The data copy runs OUTSIDE the lock for the same reason.
    CopyRangeFromEntries(covering, range, dest);
    CountHit(range.length);
    return true;
}

void ReadAheadCache::Impl::DispatchFetches(const std::vector<PendingFetch>& fetches) {
    for (const auto& fetch : fetches) {
        auto promise = fetch.promise;
        auto buffer = fetch.buffer;
        auto read_size = static_cast<int64_t>(buffer->size());
        auto read_offset = static_cast<int64_t>(fetch.range.offset);
        stream_->ReadAsync(
            buffer->data(), read_size, read_offset,
            [promise, buffer](Status status) mutable { promise->set_value(status); });
        io_count_.fetch_add(1, std::memory_order_relaxed);
        io_bytes_.fetch_add(fetch.range.length, std::memory_order_relaxed);
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
