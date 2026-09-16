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
// https://github.com/apache/orc/blob/main/c%2B%2B/src/io/Cache.hh

#pragma once

#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "paimon/common/metrics/atomic_counter_pair.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/prefetch_cache_config.h"
#include "paimon/visibility.h"

namespace paimon {

class Metrics;
class FileBlockCache;
class Bytes;
struct PendingFetch;

/// Metric names for the read-ahead cache.
class PAIMON_EXPORT ReadAheadCacheMetrics {
 public:
    /// Number of non-zero-sized Read() requests issued to the cache.
    static inline const char READ_COUNT[] = "read-ahead-cache.read.count";
    /// Total bytes requested by the Read() requests issued to the cache.
    static inline const char READ_BYTES[] = "read-ahead-cache.read.bytes";
    static inline const char READ_HITS[] = "read-ahead-cache.read.hits";
    static inline const char READ_HIT_BYTES[] = "read-ahead-cache.read.hit-bytes";
    static inline const char READ_MISSES[] = "read-ahead-cache.read.misses";
    static inline const char READ_MISS_BYTES[] = "read-ahead-cache.read.miss-bytes";
    /// Number of Read() requests served by the block cache, and the bytes they
    /// copied out of it. A read is counted either as a hit, a block hit or a
    /// miss, so `read.count = read.hits + block.hits + read.misses` for the reads
    /// that complete; a read whose prefetch fetch failed is counted in read.count
    /// only, as it is served by neither.
    ///
    /// A block hit is a read served out of a block, not a read that avoided IO:
    /// the read that finds no block waits for the fetch it dispatches and is
    /// counted here too. Comparing with block.fetches tells the two apart.
    static inline const char BLOCK_HITS[] = "read-ahead-cache.block.hits";
    static inline const char BLOCK_HIT_BYTES[] = "read-ahead-cache.block.hit-bytes";
    /// Block fetches issued to the underlying stream, and their bytes. Both are
    /// a subset of the io counters below, so comparing them tells how many bytes
    /// the block granularity added on top of the requested ones.
    static inline const char BLOCK_FETCHES[] = "read-ahead-cache.block.fetches";
    static inline const char BLOCK_FETCH_BYTES[] = "read-ahead-cache.block.fetch-bytes";
    /// Number of IO requests the cache itself issued to the underlying stream:
    /// the prefetch fetches plus the block fetches. The `io.async.*` metrics of
    /// the prefetch reader count those same requests one layer below, but they
    /// also count the async reads a sub-reader falls back to after this cache
    /// declined them, so `io.async.requests >= io.count` rather than the two
    /// agreeing.
    static inline const char IO_COUNT[] = "read-ahead-cache.io.count";
    /// Total bytes requested by the IOs the cache itself issued to the
    /// underlying stream.
    static inline const char IO_BYTES[] = "read-ahead-cache.io.bytes";
};

/// A byte range with offset and length.
struct PAIMON_EXPORT ByteRange {
    uint64_t offset;
    uint64_t length;

    ByteRange() = default;
    ByteRange(uint64_t offset, uint64_t length) : offset(offset), length(length) {}

    friend bool operator==(const ByteRange& left, const ByteRange& right) {
        return (left.offset == right.offset && left.length == right.length);
    }
    friend bool operator!=(const ByteRange& left, const ByteRange& right) {
        return !(left == right);
    }

    /// @param other The other byte range to check.
    /// @return true if this range contains the other range
    bool Contains(const ByteRange& other) const {
        return (offset <= other.offset && offset + length >= other.offset + other.length);
    }
};

/// A registered range of the cache. `buffer` is null until the prefetch for the
/// range is published (dispatched); once published it holds the destination the
/// async IO writes into and `future` resolves when that IO completes. So
/// `Published()` distinguishes "registered, not fetched yet" from "fetched".
struct RangeCacheEntry {
    ByteRange range;
    std::shared_ptr<Bytes> buffer;
    std::shared_future<Status> future;  // use shared_future in case of multiple get calls

    RangeCacheEntry() = default;
    /// Register a range without publishing it: no buffer, no fetch yet.
    explicit RangeCacheEntry(const ByteRange& range) : range(range) {}
    RangeCacheEntry(const ByteRange& range, std::shared_ptr<Bytes> buffer,
                    std::future<Status> future)
        : range(range), buffer(std::move(buffer)), future(std::move(future).share()) {}

    /// True once the prefetch for this range has been published (buffer assigned).
    bool Published() const {
        return buffer != nullptr;
    }

    friend bool operator<(const RangeCacheEntry& left, const RangeCacheEntry& right) {
        return left.range.offset < right.range.offset;
    }
};

/// A read cache designed to hide IO latency when reading.
/// Prefetching strategy: a Read() (or Warmup()) at an offset publishes and fetches the registered
/// ranges from that offset forward until their cumulative size exceeds `pre_buffer_limit`, so a
/// sequential read runs ahead of the caller instead of fetching one range at a time.
///
/// The cache never evicts: every published range stays cached until
/// ReleaseBuffers() or Reset(). It is meant to hold the prefetched ranges of
/// a single data file, whose size is bounded by the reader's scan scope.
///
/// Reads that the prefetched ranges do not cover - a reader reads the metadata
/// of its file before any range is registered - are served by a FileBlockCache
/// instead of being left to the caller. That block cache is owned by this one
/// and shares its lifetime: it is configured from `block_size` and
/// `block_cache_limit`, it survives Reset() - the blocks belong to the file
/// rather than to the registered ranges - and it is released by ReleaseBuffers().
class PAIMON_EXPORT ReadAheadCache {
 public:
    /// Construct a read cache with given options
    /// @param stream The stream the cache fetches from.
    /// @param config The cache configuration.
    /// @param file_size Size of the file behind `stream`, used to align the
    /// block cache to the end of the file. Zero means unknown and disables the
    /// block cache.
    /// @param memory_pool The pool the cached buffers are allocated from.
    ReadAheadCache(const std::shared_ptr<InputStream>& stream, const CacheConfig& config,
                   uint64_t file_size, const std::shared_ptr<MemoryPool>& memory_pool);
    ~ReadAheadCache();

    /// Register byte ranges to prefetch, merging them into the ranges already registered.
    ///
    /// The cache's only registration entry point; safe to call repeatedly and concurrently with
    /// Read(). Ranges known up front are registered before any Read(); ranges that only become
    /// known while reading (a late-materialization payload pass learns its pages after the probe
    /// pass) are registered as they are discovered. The list stays disjoint and offset-ordered, so
    /// only the part of a new range that no registered range covers is added - the overlap is
    /// dropped, as those bytes are already being fetched - and each registration is cut into ranges
    /// of at most `range_size_limit` bytes (one prefetch IO each) so a large pass is fetched
    /// concurrently. Which ranges to register, and not reporting those of an ended read, is the
    /// caller's concern.
    ///
    /// @param ranges The byte ranges to register.
    /// @return The offset of the first newly registered range, or nullopt when nothing was added.
    Result<std::optional<uint64_t>> AddRanges(std::vector<ByteRange>&& ranges);

    /// Read a range previously registered through AddRanges(), copying the cached data
    /// directly into the given destination buffer.
    ///
    /// Multi-segment hits are copied into `dest` segment by segment, without
    /// an intermediate assembled buffer.
    ///
    /// A range that no registered range covers may still be served by the block
    /// cache, see the class documentation.
    /// @param range The byte range to read.
    /// @param dest Destination buffer with at least `range.length` bytes.
    /// @return true if the range was served by the cache and `dest` was
    /// filled; false on cache miss (`dest` is left untouched).
    Result<bool> Read(const ByteRange& range, char* dest);

    /// Start fetching from the first registered range forward immediately. AddRanges() only
    /// registers the ranges; without Warmup() the first fetch starts when the first Read() arrives,
    /// racing the caller's own miss fetch.
    void Warmup();

    /// Start fetching the registered ranges from `from_offset` forward, bounded by the
    /// pre-buffer limit. Warmup() is this call made from the first registered range.
    /// @param from_offset The offset to start fetching from, typically one AddRanges() returned.
    void Warmup(uint64_t from_offset);

    /// Collect the counters of the Read() calls, of the block cache and of the IOs into the given
    /// metrics as counters named after `ReadAheadCacheMetrics`. Only reads issued through Read()
    /// are counted as hits, block hits or misses; the fetches the cache dispatches are counted in
    /// the io counters instead.
    /// @param metrics The metrics to write the counters into. A null
    /// pointer or a null shared pointer is a no-op.
    void CollectMetrics(std::shared_ptr<Metrics>* metrics) const;

    /// Drop the prefetched entries and zero all counters, so a fresh set of ranges can be
    /// registered with AddRanges(). Waits for the in-flight fetches before their buffers go away.
    ///
    /// The block cache is kept - it caches the file rather than the registered ranges, and a reader
    /// reusing the cache reads the same file again - but its counters are zeroed too. Unlike
    /// ReleaseBuffers(), the counters do not survive.
    void Reset();

    /// Release all cached buffers and registered ranges while keeping the hit/miss
    /// counters intact.
    ///
    /// Unlike Reset(), the counters recorded by Read() remain readable through
    /// CollectMetrics() afterwards, so this is safe to call when the owning reader
    /// is closed while its metrics are still being aggregated. The block cache is
    /// released too, as the file is not read again.
    void ReleaseBuffers();

 private:
    /// Merge coalesced, validated `new_ranges` into the disjoint, offset-ordered ranges_, moving
    /// the already registered entries (and thus their published buffer/future) over as-is, and
    /// return the offset of the first newly registered range (nullopt when nothing was added).
    /// The caller holds the write lock.
    std::optional<uint64_t> MergeRangesLocked(std::vector<ByteRange>&& new_ranges);

    /// Dispatch the prefetch IOs for entries that have already been published into ranges_.
    void DispatchFetches(const std::vector<PendingFetch>& fetches);

    /// Find the published entries fully covering the given range under the read lock.
    /// Returns an empty vector on miss. Entries are copied (shared buffers)
    /// so the caller may use them after releasing the lock.
    std::vector<RangeCacheEntry> FindCoveringEntries(const ByteRange& range);

    /// True when the prefetch window starting at `offset` still holds an entry whose fetch has not
    /// been published. The caller holds either lock; PreBuffer() uses it under the read lock to
    /// skip the write lock entirely when there is nothing left to fetch.
    bool WindowHasUnpublishedLocked(uint64_t offset) const;

    /// Publish and fetch the registered ranges from the given offset forward, bounded by the
    /// pre-buffer limit.
    ///
    /// Selecting the ranges and publishing them (filling buffer/future in place in ranges_) all
    /// happen under the write lock, before any IO is dispatched: AddRanges() rewrites ranges_
    /// from another thread, and a reader racing the prefetch must observe an entry published only
    /// once it is already visible in ranges_, so it waits on the in-flight entry instead of
    /// re-fetching the same bytes.
    void PreBuffer(uint64_t offset);

    /// Clear the prefetch state, waiting for the fetches still writing into the
    /// entry buffers. Leaves the block cache untouched.
    void ReleasePrefetchBuffers();

    std::shared_ptr<InputStream> stream_;
    CacheConfig config_;
    // Every registered range, ordered by offset and disjoint (so a matching
    // region is found by binary search). An entry whose buffer is still null is
    // registered but not fetched yet; publishing fills buffer/future in place.
    std::vector<RangeCacheEntry> ranges_;
    std::shared_ptr<MemoryPool> memory_pool_;
    mutable std::shared_mutex rw_mutex_;
    // Caches the reads that no registered range covers, or null when the block
    // cache is disabled. Owns its own locking and counters.
    std::unique_ptr<FileBlockCache> block_cache_;
    // The Read() requests issued to the cache and how they were served,
    // aggregated over all the streams sharing this cache. A read is counted
    // either as a hit, a block cache hit or a miss.
    AtomicCounterPair read_metrics_;
    AtomicCounterPair hit_metrics_;
    AtomicCounterPair miss_metrics_;
    // The prefetch IO actually issued to the underlying stream. The block cache
    // counts its own fetches, which CollectMetrics() adds to these.
    AtomicCounterPair io_metrics_;
};

}  // namespace paimon
