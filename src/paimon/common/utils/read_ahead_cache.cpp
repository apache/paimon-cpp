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

// Everything needed to dispatch the prefetch IO of an entry AFTER the entry
// has been published into ranges_: the promise resolves the entry's future
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

Result<std::optional<uint64_t>> ReadAheadCache::AddRanges(std::vector<ByteRange>&& ranges) {
    // Both an up-front registration and a mid-read one are cut at the plain size limit.
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<ByteRange> new_ranges,
        ByteRangeCombiner::CoalesceByteRanges(std::move(ranges), config_.GetHoleSizeLimit(),
                                              config_.GetRangeSizeLimit()));
    for (const auto& new_range : new_ranges) {
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int64_t>(new_range.offset, "range offset"));
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int64_t>(new_range.length, "range length"));
    }

    // Locked: a registration rewrites the very ranges_ a concurrent Read() serves from, so it
    // must not race the read finding its covering entries.
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    return MergeRangesLocked(std::move(new_ranges));
}

std::optional<uint64_t> ReadAheadCache::MergeRangesLocked(std::vector<ByteRange>&& new_ranges) {
    // Merge the two sorted, internally disjoint lists, keeping the result disjoint so that
    // FindCoveringEntries() may keep walking it: of a new range, only the parts no registered
    // range covers are registered. Already registered entries are moved over as-is, so a range
    // that was published keeps its buffer and future and is never fetched again.
    std::vector<RangeCacheEntry> merged;
    merged.reserve(ranges_.size() + new_ranges.size());
    std::optional<uint64_t> first_added;
    size_t old_idx = 0;
    auto keep_registered = [&]() {
        merged.push_back(std::move(ranges_[old_idx]));
        ++old_idx;
    };
    auto register_new = [&](uint64_t offset, uint64_t length) {
        if (!first_added.has_value()) {
            first_added = offset;
        }
        merged.emplace_back(ByteRange(offset, length));
    };
    for (const ByteRange& candidate : new_ranges) {
        const uint64_t candidate_end = candidate.offset + candidate.length;
        // Start of the part of the candidate that is not registered yet.
        uint64_t cursor = candidate.offset;
        while (cursor < candidate_end) {
            if (old_idx == ranges_.size()) {
                register_new(cursor, candidate_end - cursor);
                break;
            }
            const ByteRange registered = ranges_[old_idx].range;
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
            // dropped because it is already being fetched. The registered range is left for the
            // next iteration to settle, as a later candidate may still start before it.
            if (registered.offset > cursor) {
                register_new(cursor, registered.offset - cursor);
            }
            cursor = registered_end;
        }
    }
    while (old_idx < ranges_.size()) {
        keep_registered();
    }
    if (!first_added.has_value()) {
        return std::optional<uint64_t>{};
    }
    ranges_ = std::move(merged);
    return first_added;
}

bool ReadAheadCache::WindowHasUnpublishedLocked(uint64_t offset) const {
    auto it = std::lower_bound(ranges_.begin(), ranges_.end(), offset,
                               [](const RangeCacheEntry& entry, uint64_t offset) {
                                   return entry.range.offset + entry.range.length <= offset;
                               });
    if (it == ranges_.end() || it->range.offset > offset) {
        return false;
    }
    size_t total_bytes = 0;
    for (auto entry = it; entry != ranges_.end(); ++entry) {
        total_bytes += entry->range.length;
        if (total_bytes > config_.GetPreBufferLimit()) {
            break;
        }
        if (!entry->Published()) {
            return true;
        }
    }
    return false;
}

void ReadAheadCache::PreBuffer(uint64_t offset) {
    // Fast path under the read lock: once the whole prefetch window is published there is nothing
    // left to fetch, and returning here keeps the hot Read() path off the write lock, so concurrent
    // readers' lookups are not serialized behind a publish that has nothing to publish.
    {
        std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
        if (!WindowHasUnpublishedLocked(offset)) {
            return;
        }
    }
    std::vector<PendingFetch> fetches;
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        auto it = std::lower_bound(ranges_.begin(), ranges_.end(), offset,
                                   [](const RangeCacheEntry& entry, uint64_t offset) {
                                       return entry.range.offset + entry.range.length <= offset;
                                   });
        if (it == ranges_.end() || it->range.offset > offset) {
            return;
        }

        size_t total_bytes = 0;
        for (auto entry = it; entry != ranges_.end(); ++entry) {
            total_bytes += entry->range.length;
            if (total_bytes > config_.GetPreBufferLimit()) {
                break;
            }
            if (entry->Published()) {
                continue;
            }
            auto promise = std::make_shared<std::promise<Status>>();
            auto future = promise->get_future();
            auto buffer = AllocateBytesKeepingPoolAlive(entry->range.length, memory_pool_);
            fetches.push_back({entry->range, buffer, promise});
            // Published IN PLACE under the write lock, before the IO is dispatched below: a
            // reader racing the prefetch then finds the entry and waits on its future instead of
            // issuing a second fetch for the same bytes. Entries are never evicted, so an
            // in-flight fetch always keeps its entry and thus its future reachable.
            entry->buffer = std::move(buffer);
            entry->future = std::move(future).share();
        }
    }
    // Dispatched OUTSIDE the lock, so that a fetch completing inline does not deadlock against a
    // Read() waiting for it.
    DispatchFetches(fetches);
}

ReadAheadCache::ReadAheadCache(const std::shared_ptr<InputStream>& stream,
                               const CacheConfig& config, uint64_t file_size,
                               const std::shared_ptr<MemoryPool>& memory_pool)
    : stream_(stream), config_(config), memory_pool_(memory_pool) {
    // An unknown file size cannot be aligned to, and a zero limit or block size
    // means the block cache is turned off: leave it null in those cases.
    if (file_size > 0 && config_.GetBlockSize() > 0 && config_.GetBlockCacheLimit() > 0) {
        block_cache_ = std::make_unique<FileBlockCache>(stream, file_size, config_.GetBlockSize(),
                                                        config_.GetBlockCacheLimit(), memory_pool);
    }
}

ReadAheadCache::~ReadAheadCache() {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    for (auto& entry : ranges_) {
        if (entry.Published()) {
            entry.future.wait();
        }
    }
    // The block cache waits for its own fetches when it is destroyed.
}

void ReadAheadCache::Reset() {
    ReleasePrefetchBuffers();
    read_metrics_.Reset();
    hit_metrics_.Reset();
    miss_metrics_.Reset();
    io_metrics_.Reset();
    if (block_cache_ != nullptr) {
        // Only the counters: the blocks cache the file, not the registered
        // ranges, and a reader resetting the cache reads the same file again.
        block_cache_->ResetCounters();
    }
}

void ReadAheadCache::ReleaseBuffers() {
    ReleasePrefetchBuffers();
    if (block_cache_ != nullptr) {
        block_cache_->Release();
    }
}

void ReadAheadCache::ReleasePrefetchBuffers() {
    std::vector<RangeCacheEntry> to_release;
    {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        // Swap the entries out under the lock instead of waiting on them in place: an in-flight
        // fetch may take arbitrarily long, and holding the write lock while waiting would stall
        // every concurrent reader. Once swapped, the entries are owned locally and no other thread
        // can observe or mutate them. Entries are never evicted, so waiting on the published ones
        // covers every dispatched fetch: no fetch is still writing into an entry buffer when the
        // buffers go away. The buffers keep the memory pool alive themselves, for the callbacks
        // that a stream destroys later than it resolves them.
        to_release.swap(ranges_);
    }
    // Wait OUTSIDE the lock; the buffers are released when to_release destructs.
    for (auto& entry : to_release) {
        if (entry.Published()) {
            entry.future.wait();
        }
    }
    // The read/io counters are deliberately kept: a reader closed at EOF must
    // still be able to report them through CollectMetrics().
}

void ReadAheadCache::CollectMetrics(std::shared_ptr<Metrics>* metrics) const {
    if (metrics == nullptr || !*metrics) {
        return;
    }
    auto& m = *metrics;
    m->SetCounter(ReadAheadCacheMetrics::READ_COUNT, read_metrics_.Count());
    m->SetCounter(ReadAheadCacheMetrics::READ_BYTES, read_metrics_.Bytes());
    m->SetCounter(ReadAheadCacheMetrics::READ_HITS, hit_metrics_.Count());
    m->SetCounter(ReadAheadCacheMetrics::READ_HIT_BYTES, hit_metrics_.Bytes());
    m->SetCounter(ReadAheadCacheMetrics::READ_MISSES, miss_metrics_.Count());
    m->SetCounter(ReadAheadCacheMetrics::READ_MISS_BYTES, miss_metrics_.Bytes());
    // The block cache keeps its own counters. Its fetches also go to the
    // underlying stream, so they are part of the io counters too.
    const FileBlockCache::Counters blocks =
        block_cache_ != nullptr ? block_cache_->GetCounters() : FileBlockCache::Counters{};
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_HITS, blocks.hits);
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_HIT_BYTES, blocks.hit_bytes);
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_FETCHES, blocks.fetches);
    m->SetCounter(ReadAheadCacheMetrics::BLOCK_FETCH_BYTES, blocks.fetch_bytes);
    m->SetCounter(ReadAheadCacheMetrics::IO_COUNT, io_metrics_.Count() + blocks.fetches);
    m->SetCounter(ReadAheadCacheMetrics::IO_BYTES, io_metrics_.Bytes() + blocks.fetch_bytes);
}

void ReadAheadCache::Warmup() {
    // AddRanges() only registers the pending ranges; without this the first fetch
    // starts when the first Read() arrives, racing the reader's own miss fetch.
    uint64_t from_offset = 0;
    {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        if (ranges_.empty()) {
            return;
        }
        from_offset = ranges_.front().range.offset;
    }
    PreBuffer(from_offset);
}

void ReadAheadCache::Warmup(uint64_t from_offset) {
    PreBuffer(from_offset);
}

std::vector<RangeCacheEntry> ReadAheadCache::FindCoveringEntries(const ByteRange& range) {
    std::vector<RangeCacheEntry> covering;
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    // Find the entry holding the start of the range: the first entry whose
    // end is beyond range.offset (entries are disjoint and sorted by offset).
    auto it = std::lower_bound(ranges_.begin(), ranges_.end(), range.offset,
                               [](const RangeCacheEntry& e, uint64_t offset) {
                                   return e.range.offset + e.range.length <= offset;
                               });
    // An unpublished covering entry means its prefetch has not been dispatched
    // yet, so the bytes are not there to serve: a miss.
    if (it == ranges_.end() || it->range.offset > range.offset || !it->Published()) {
        return covering;
    }
    if (it->range.Contains(range)) {
        covering.push_back(*it);
        return covering;
    }
    // The request spans several adjacent entries (a column chunk larger than
    // one coalesced range): collect the contiguous run of published entries and
    // check it covers the whole request. Entries are published before their
    // fetch is dispatched, so a reader racing the prefetch waits for the
    // in-flight fetch instead of issuing a second one for the same bytes.
    uint64_t covered_end = it->range.offset + it->range.length;
    covering.push_back(*it);
    auto next = std::next(it);
    while (covered_end < range.offset + range.length && next != ranges_.end() &&
           next->range.offset == covered_end && next->Published()) {
        covered_end = next->range.offset + next->range.length;
        covering.push_back(*next);
        ++next;
    }
    if (covered_end < range.offset + range.length) {
        covering.clear();
    }
    return covering;
}

Result<bool> ReadAheadCache::Read(const ByteRange& range, char* dest) {
    if (range.length == 0) {
        return true;
    }
    read_metrics_.Record(range.length);
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
        miss_metrics_.Record(range.length);
        return false;
    }
    // Wait OUTSIDE the lock: the futures resolve when the prefetch stream's
    // async reads complete, and holding rw_mutex_ would block Cache().
    for (const auto& entry : covering) {
        PAIMON_RETURN_NOT_OK(entry.future.get());
    }
    // The data copy runs OUTSIDE the lock for the same reason.
    CopyRangeFromEntries(covering, range, dest);
    hit_metrics_.Record(range.length);
    return true;
}

void ReadAheadCache::DispatchFetches(const std::vector<PendingFetch>& fetches) {
    for (const auto& fetch : fetches) {
        auto promise = fetch.promise;
        auto buffer = fetch.buffer;
        auto read_size = static_cast<int64_t>(buffer->size());
        auto read_offset = static_cast<int64_t>(fetch.range.offset);
        stream_->ReadAsync(
            buffer->data(), read_size, read_offset,
            [promise, buffer](Status status) mutable { promise->set_value(status); });
        io_metrics_.Record(fetch.range.length);
    }
}

}  // namespace paimon
