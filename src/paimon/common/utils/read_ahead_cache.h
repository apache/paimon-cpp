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
    /// Number of prefetch IO requests actually issued to the underlying stream.
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
/// Eviction policy: The cache uses a simple FIFO eviction policy based on total cached byte size.
/// When adding new ranges would exceed `buffer_size_limit`, the oldest cached ranges are evicted
/// first until there is enough space for the new data.
class PAIMON_EXPORT ReadAheadCache {
 public:
    /// Construct a read cache with given options
    ReadAheadCache(const std::shared_ptr<InputStream>& stream, const CacheConfig& config,
                   const std::shared_ptr<MemoryPool>& memory_pool);
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
    /// @param range The byte range to read.
    /// @param dest Destination buffer with at least `range.length` bytes.
    /// @return true if the range was served from the cache and `dest` was
    /// filled; false on cache miss (`dest` is left untouched).
    Result<bool> Read(const ByteRange& range, char* dest);

    /// Start fetching the first batch of pending ranges immediately.
    /// Init() only registers the ranges; without Warmup() the first fetch starts
    /// when the first Read() arrives, racing the caller's own miss fetch.
    void Warmup();

    /// Collect hit/miss counters of Read() calls and the prefetch IO
    /// counters into the given metrics as counters named after
    /// `ReadAheadCacheMetrics`. Only reads issued through Read() are counted
    /// as hits/misses; prefetch fetches dispatched by the cache itself are
    /// counted in the fetch counters instead.
    /// @param metrics[out] The metrics to write the counters into. A null
    /// pointer or a null shared pointer is a no-op.
    void CollectMetrics(std::shared_ptr<Metrics>* metrics) const;

    /// Reset the cache to its initial state, clearing all cached data and configuration.
    ///
    /// This method waits for all ongoing asynchronous read operations to complete,
    /// clears all cached entries, and resets the internal state so that Init() can be called again.
    /// After calling Reset, the cache can be safely re-initialized with new ranges.
    void Reset();

    /// Release all cached buffers and pending ranges while keeping the hit/miss
    /// counters intact.
    ///
    /// Unlike Reset(), the counters recorded by Read() remain readable through
    /// CollectMetrics() afterwards, so this is safe to call when the owning reader
    /// is closed while its metrics are still being aggregated.
    void ReleaseBuffers();

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
