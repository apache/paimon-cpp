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
#include <functional>
#include <memory>
#include <vector>

#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/prefetch_cache_config.h"
#include "paimon/visibility.h"

namespace paimon {

class Metrics;

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
    /// Number of prefetch IO requests actually issued to the underlying stream.
    /// These are the same requests that the `io.async.*` metrics of the prefetch
    /// reader observe one layer below, counted here per cache rather than per
    /// stream, so the two are expected to agree instead of adding up.
    static inline const char IO_COUNT[] = "read-ahead-cache.io.count";
    /// Total bytes requested by the prefetch IOs issued to the underlying stream.
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

/// A read cache designed to hide IO latencies when reading.
/// Prefetching strategy: When a range is read, the cache will prefetch up to
/// `pre_buffer_range_count` additional adjacent ranges ahead of the requested offset. This helps
/// hide I/O latency for sequential access. Example: If you read range [0, 100), and
/// pre_buffer_range_count=2, the next two configured ranges will also be prefetched.
///
/// The cache never evicts: every published range stays cached until
/// ReleaseBuffers() or Reset(). It is meant to hold the prefetched ranges of
/// a single data file, whose size is bounded by the reader's scan scope.
///
/// Reads that the prefetched ranges do not cover (the footer and the page index
/// of a parquet file are read before any range is registered) are served by a
/// FileBlockCache instead of being left to the caller. That block cache is owned
/// by this one and shares its lifetime: it is configured from `block_size` and
/// `block_cache_limit`, it survives Reset() - the blocks belong to the file
/// rather than to a registration round - and it is released by ReleaseBuffers().
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

    /// Initialize the cache with given byte ranges to be cached.
    /// @param ranges The byte ranges to be cached.
    /// @return Status of the operation.
    /// @note This method must be called before any Read() calls. Ranges will be coalesced based
    /// on the cache configuration.
    Status Init(std::vector<ByteRange>&& ranges);

    /// Read a range previously provided to Init(), copying the cached data
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

    /// Asynchronous variant of Read(), for a caller whose own interface is
    /// asynchronous and must not block a thread: the block cache fetch serving a
    /// range that no registered range covers is awaited through `callback`
    /// instead of waited for.
    ///
    /// The prefetch of a COVERING entry is still waited for synchronously: it has
    /// already been dispatched, so waiting for it is the latency hiding this cache
    /// exists for.
    ///
    /// `callback` is invoked exactly once: inline when the read is served out of
    /// an entry, out of an already cached block, or declined, and from the thread
    /// resolving the block fetch otherwise. A non-ok status means the read failed
    /// and must be reported to the caller; an ok status with `served == false`
    /// means the cache declined the read and the caller reads the bytes itself,
    /// exactly like Read() returning false.
    ///
    /// The caller must keep `dest` alive until `callback` has been invoked, the
    /// way InputStream::ReadAsync() requires of its own callers.
    /// @param range The byte range to read.
    /// @param dest Destination buffer with at least `range.length` bytes.
    /// @param callback Continuation receiving the outcome of the read and whether
    /// the range was served and `dest` was filled.
    void ReadAsync(const ByteRange& range, char* dest,
                   std::function<void(Status status, bool served)> callback);

    /// Start fetching the first batch of pending ranges immediately.
    /// Init() only registers the ranges; without Warmup() the first fetch starts
    /// when the first Read() arrives, racing the caller's own miss fetch.
    void Warmup();

    /// Collect hit/miss counters of Read() calls and the prefetch IO
    /// counters into the given metrics as counters named after
    /// `ReadAheadCacheMetrics`. Only reads issued through Read() are counted
    /// as hits/misses; prefetch fetches dispatched by the cache itself are
    /// counted in the fetch counters instead.
    /// @param metrics The metrics to write the counters into. A null
    /// pointer or a null shared pointer is a no-op.
    void CollectMetrics(std::shared_ptr<Metrics>* metrics) const;

    /// Reset the cache to its initial state, clearing all cached data and configuration.
    ///
    /// This method waits for all ongoing asynchronous read operations to complete,
    /// clears all cached entries, and resets the internal state so that Init() can be called again.
    /// After calling Reset, the cache can be safely re-initialized with new ranges.
    ///
    /// The block cache is kept: it caches the file rather than the registered
    /// ranges, and a reader reusing the cache reads the same file again.
    void Reset();

    /// Release all cached buffers and pending ranges while keeping the hit/miss
    /// counters intact.
    ///
    /// Unlike Reset(), the counters recorded by Read() remain readable through
    /// CollectMetrics() afterwards, so this is safe to call when the owning reader
    /// is closed while its metrics are still being aggregated. The block cache is
    /// released too, as the file is not read again.
    void ReleaseBuffers();

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
