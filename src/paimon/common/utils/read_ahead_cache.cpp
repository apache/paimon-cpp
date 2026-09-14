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
#include <optional>
#include <shared_mutex>
#include <utility>

#include "paimon/common/memory/bytes_utils.h"
#include "paimon/common/metrics/atomic_counter_pair.h"
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

// No registration round is open: Init() never ran, or the round it opened has ended.
constexpr uint64_t kNoRegistrationRound = 0;

class ReadAheadCache::Impl {
 public:
    Impl(const std::shared_ptr<InputStream>& stream, const CacheConfig& config, uint64_t file_size,
         const std::shared_ptr<MemoryPool>& memory_pool);
    ~Impl();

    Status Init(std::vector<ByteRange>&& ranges);
    uint64_t RegistrationRound() const;
    Result<std::optional<uint64_t>> AddRanges(std::vector<ByteRange>&& ranges,
                                              uint64_t expected_round);
    Result<bool> Read(const ByteRange& range, char* dest);
    void Reset();
    void ReleaseBuffers();
    void Warmup();
    void Warmup(uint64_t from_offset);
    void CollectMetrics(std::shared_ptr<Metrics>* metrics) const;

 private:
    /// Dispatch the prefetch IOs for entries that have already been published
    /// into entries_.
    void DispatchFetches(const std::vector<PendingFetch>& fetches);
    /// Find the entries fully covering the given range under the read lock.
    /// Returns an empty vector on miss. Entries are copied (shared buffers)
    /// so the caller may use them after releasing the lock.
    std::vector<RangeCacheEntry> FindCoveringEntries(const ByteRange& range);

    /// Mark, publish and fetch the pending ranges from the given offset forward, bounded by the
    /// pre-buffer limit.
    ///
    /// Selecting the ranges, marking is_cached_ and publishing the promise-backed entries all
    /// happen atomically under the write lock, before any IO is dispatched: AddRanges() rewrites
    /// pending_ranges_ and is_cached_ from another thread, and a reader racing the prefetch must
    /// observe is_cached_ set only once the covering entries are already visible, so it waits on
    /// the in-flight entries instead of re-fetching the same bytes.
    void PreBuffer(uint64_t offset);

    /// Clear the prefetch state, waiting for the fetches still writing into the
    /// entry buffers. Leaves the block cache untouched.
    void ReleasePrefetchBuffers();

    std::shared_ptr<InputStream> stream_;
    CacheConfig config_;
    // Ordered by offset (so as to find a matching region by binary search)
    std::vector<RangeCacheEntry> entries_;
    std::shared_ptr<MemoryPool> memory_pool_;
    mutable std::shared_mutex rw_mutex_;
    std::vector<std::atomic<bool>> is_cached_;
    std::vector<ByteRange> pending_ranges_;
    // The round the currently registered ranges belong to, or kNoRegistrationRound when no round
    // is open. Every Init() opens a new one, so ranges reported by a pass that outlived its round
    // can be told apart from the ranges of the open round.
    uint64_t registration_round_ = kNoRegistrationRound;
    uint64_t last_registration_round_ = kNoRegistrationRound;
    // Caches the reads that no registered range covers, or null when the block
    // cache is disabled. Owns its own locking and counters.
    std::unique_ptr<FileBlockCache> block_cache_;
    // The Read() requests issued to the cache and how they were served,
    // aggregated over all the streams sharing this cache. A read is counted
    // either as a hit, a block cache hit or a miss.
    AtomicCounterPair reads_;
    AtomicCounterPair hits_;
    AtomicCounterPair misses_;
    // The prefetch IO actually issued to the underlying stream. The block cache
    // counts its own fetches, which CollectMetrics() adds to these.
    AtomicCounterPair ios_;
    // The ranges registered mid-read: the ones that were registered and the ones that were
    // dropped, either as already registered or as belonging to a round that has ended.
    AtomicCounterPair late_registered_;
    AtomicCounterPair late_dropped_;
};

Status ReadAheadCache::Impl::Init(std::vector<ByteRange>&& ranges) {
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<ByteRange> pending_ranges,
        ByteRangeCombiner::CoalesceByteRanges(std::move(ranges), config_.GetHoleSizeLimit(),
                                              config_.GetRangeSizeLimit()));
    for (const auto& pending_range : pending_ranges) {
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int64_t>(pending_range.offset, "range offset"));
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int64_t>(pending_range.length, "range length"));
    }
    // Locked like AddRanges(), which extends this very registration round from another thread.
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    if (registration_round_ != kNoRegistrationRound) {
        return Status::Invalid("Cache has already been initialized");
    }
    pending_ranges_ = std::move(pending_ranges);
    is_cached_ = std::vector<std::atomic<bool>>(pending_ranges_.size());
    for (auto& is_cached : is_cached_) {
        is_cached.store(false);
    }
    // Monotonic across Reset(), so that a round is never confused with an earlier one.
    registration_round_ = ++last_registration_round_;
    return Status::OK();
}

uint64_t ReadAheadCache::Impl::RegistrationRound() const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    return registration_round_;
}

Result<std::optional<uint64_t>> ReadAheadCache::Impl::AddRanges(std::vector<ByteRange>&& ranges,
                                                                uint64_t expected_round) {
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<ByteRange> new_ranges,
        ByteRangeCombiner::CoalesceByteRanges(std::move(ranges), config_.GetHoleSizeLimit(),
                                              config_.GetLateRangeSizeLimit()));
    for (const auto& new_range : new_ranges) {
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int64_t>(new_range.offset, "range offset"));
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int64_t>(new_range.length, "range length"));
    }

    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    if (expected_round == kNoRegistrationRound || expected_round != registration_round_) {
        // The round these ranges belong to is not the open one: the cache was never initialized,
        // its buffers were released, or it was reset for a new read-range generation. The reads
        // keep issuing their own IO, as they did before these ranges became known.
        for (const auto& new_range : new_ranges) {
            late_dropped_.Add(new_range.length);
        }
        return std::optional<uint64_t>{};
    }
    // Merge the two sorted, internally disjoint lists, keeping the result disjoint so that
    // FindCoveringEntries() may keep walking it: of a new range, only the parts no registered
    // range covers are registered. is_cached_ is rebuilt alongside: it is indexed by
    // pending_ranges_, and an already fetched range must not be fetched again.
    std::vector<ByteRange> merged;
    std::vector<char> merged_is_cached;
    merged.reserve(pending_ranges_.size() + new_ranges.size());
    merged_is_cached.reserve(pending_ranges_.size() + new_ranges.size());
    std::optional<uint64_t> first_added;
    size_t old_idx = 0;
    auto keep_registered = [&]() {
        merged.push_back(pending_ranges_[old_idx]);
        merged_is_cached.push_back(is_cached_[old_idx].load() ? 1 : 0);
        ++old_idx;
    };
    auto register_new = [&](uint64_t offset, uint64_t length) {
        if (!first_added.has_value()) {
            first_added = offset;
        }
        merged.emplace_back(offset, length);
        merged_is_cached.push_back(0);
        late_registered_.Add(length);
    };
    for (const ByteRange& candidate : new_ranges) {
        const uint64_t candidate_end = candidate.offset + candidate.length;
        // Start of the part of the candidate that is neither registered nor dropped yet.
        uint64_t cursor = candidate.offset;
        uint64_t dropped_bytes = 0;
        while (cursor < candidate_end) {
            if (old_idx == pending_ranges_.size()) {
                register_new(cursor, candidate_end - cursor);
                break;
            }
            const ByteRange registered = pending_ranges_[old_idx];
            const uint64_t registered_end = registered.offset + registered.length;
            if (registered_end <= cursor) {
                // Entirely before what is left of the candidate, so its place in the merged list
                // is settled.
                keep_registered();
                continue;
            }
            if (registered.offset >= candidate_end) {
                // The candidate ends before this one starts: the rest of it is new.
                register_new(cursor, candidate_end - cursor);
                break;
            }
            // Overlap: the hole before the registered range is new, and the overlapping part is
            // dropped because the round that registered it is already fetching those bytes. The
            // registered range is left for the next iteration to settle, as a later candidate may
            // still start before it.
            if (registered.offset > cursor) {
                register_new(cursor, registered.offset - cursor);
            }
            dropped_bytes +=
                std::min(candidate_end, registered_end) - std::max(cursor, registered.offset);
            cursor = registered_end;
        }
        if (dropped_bytes > 0) {
            late_dropped_.Add(dropped_bytes);
        }
    }
    while (old_idx < pending_ranges_.size()) {
        keep_registered();
    }
    if (!first_added.has_value()) {
        return std::optional<uint64_t>{};
    }
    pending_ranges_ = std::move(merged);
    // std::atomic<bool> is neither copyable nor movable, so the inherited flags are carried over
    // one by one instead of resizing in place.
    is_cached_ = std::vector<std::atomic<bool>>(pending_ranges_.size());
    for (size_t i = 0; i < is_cached_.size(); ++i) {
        is_cached_[i].store(merged_is_cached[i] != 0);
    }
    return first_added;
}

void ReadAheadCache::Impl::PreBuffer(uint64_t offset) {
    std::vector<PendingFetch> fetches;
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        auto it = std::lower_bound(pending_ranges_.begin(), pending_ranges_.end(), offset,
                                   [](const ByteRange& range, uint64_t offset) {
                                       return range.offset + range.length <= offset;
                                   });
        if (it == pending_ranges_.end() || it->offset > offset) {
            return;
        }

        std::vector<RangeCacheEntry> new_entries;
        size_t total_bytes = 0;
        for (size_t i = std::distance(pending_ranges_.begin(), it); i < pending_ranges_.size();
             ++i) {
            total_bytes += pending_ranges_[i].length;
            if (total_bytes > config_.GetPreBufferLimit()) {
                break;
            }
            if (is_cached_[i].exchange(true)) {
                continue;
            }
            const ByteRange& range = pending_ranges_[i];
            auto promise = std::make_shared<std::promise<Status>>();
            auto future = promise->get_future();
            auto buffer = AllocateBytesKeepingPoolAlive(range.length, memory_pool_);
            fetches.push_back({range, buffer, promise});
            new_entries.emplace_back(range, std::move(buffer), std::move(future));
        }
        if (!new_entries.empty()) {
            // Entries are never evicted: the cache holds every published range until
            // ReleaseBuffers()/Reset(), so an in-flight fetch always keeps its entry and thus
            // its future reachable.
            std::vector<RangeCacheEntry> merged(entries_.size() + new_entries.size());
            std::merge(entries_.begin(), entries_.end(), new_entries.begin(), new_entries.end(),
                       merged.begin());
            entries_ = std::move(merged);
        }
    }
    // Dispatched OUTSIDE the lock, so that a fetch completing inline does not deadlock against a
    // Read() waiting for it.
    DispatchFetches(fetches);
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
    reads_.Reset();
    hits_.Reset();
    misses_.Reset();
    ios_.Reset();
    late_registered_.Reset();
    late_dropped_.Reset();
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
    // Ends the registration round: the ranges a pass reports from here on belong to a round that
    // is over, and AddRanges() drops them instead of registering bytes nobody reads.
    registration_round_ = kNoRegistrationRound;
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
    m->SetCounter(ReadAheadCacheMetrics::READ_MISSES, misses_.Count());
    m->SetCounter(ReadAheadCacheMetrics::READ_MISS_BYTES, misses_.Bytes());
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
    m->SetCounter(ReadAheadCacheMetrics::LATE_REGISTERED, late_registered_.Count());
    m->SetCounter(ReadAheadCacheMetrics::LATE_REGISTERED_BYTES, late_registered_.Bytes());
    m->SetCounter(ReadAheadCacheMetrics::LATE_DROPPED, late_dropped_.Count());
    m->SetCounter(ReadAheadCacheMetrics::LATE_DROPPED_BYTES, late_dropped_.Bytes());
}

void ReadAheadCache::Impl::Warmup() {
    // Init() only registers the pending ranges; without this the first fetch
    // starts when the first Read() arrives, racing the reader's own miss fetch.
    uint64_t from_offset = 0;
    {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        if (pending_ranges_.empty()) {
            return;
        }
        from_offset = pending_ranges_.front().offset;
    }
    PreBuffer(from_offset);
}

void ReadAheadCache::Impl::Warmup(uint64_t from_offset) {
    PreBuffer(from_offset);
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
    reads_.Add(range.length);
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
        misses_.Add(range.length);
        return false;
    }
    // Wait OUTSIDE the lock: the futures resolve when the prefetch stream's
    // async reads complete, and holding rw_mutex_ would block Cache().
    for (const auto& entry : covering) {
        PAIMON_RETURN_NOT_OK(entry.future.get());
    }
    // The data copy runs OUTSIDE the lock for the same reason.
    CopyRangeFromEntries(covering, range, dest);
    hits_.Add(range.length);
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

Result<std::optional<uint64_t>> ReadAheadCache::AddRanges(std::vector<ByteRange>&& ranges,
                                                          uint64_t expected_round) {
    return impl_->AddRanges(std::move(ranges), expected_round);
}

uint64_t ReadAheadCache::RegistrationRound() const {
    return impl_->RegistrationRound();
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

void ReadAheadCache::Warmup(uint64_t from_offset) {
    impl_->Warmup(from_offset);
}

void ReadAheadCache::CollectMetrics(std::shared_ptr<Metrics>* metrics) const {
    impl_->CollectMetrics(metrics);
}

}  // namespace paimon
