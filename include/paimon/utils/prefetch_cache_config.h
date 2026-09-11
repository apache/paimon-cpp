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

#include "paimon/visibility.h"

namespace paimon {

/// Configuration parameters for the read-ahead cache behavior.
///
/// This struct controls various limits and prefetching strategies used by
/// ReadAheadCache to balance memory usage, I/O efficiency, and latency hiding.
class PAIMON_EXPORT CacheConfig {
 public:
    /// Returns the maximum allowed size (in bytes) for a single cached range.
    /// Defaults to 32 MiB.
    uint64_t GetRangeSizeLimit() const {
        return range_size_limit_;
    }

    /// Sets the maximum allowed size (in bytes) for a single cached range.
    void SetRangeSizeLimit(uint64_t range_size_limit) {
        range_size_limit_ = range_size_limit;
    }

    /// Returns the maximum allowed size (in bytes) for a single range registered after the cache
    /// was initialized, i.e. for the ranges that only become known while reading.
    /// Defaults to 8 MiB.
    uint64_t GetLateRangeSizeLimit() const {
        return late_range_size_limit_;
    }

    /// Sets the maximum allowed size (in bytes) for a single range registered after the cache was
    /// initialized.
    void SetLateRangeSizeLimit(uint64_t late_range_size_limit) {
        late_range_size_limit_ = late_range_size_limit;
    }

    /// Returns the maximum gap size (in bytes) considered mergeable between
    /// adjacent ranges. Defaults to 512 KiB.
    uint64_t GetHoleSizeLimit() const {
        return hole_size_limit_;
    }

    /// Sets the maximum gap size (in bytes) considered mergeable between adjacent ranges.
    void SetHoleSizeLimit(uint64_t hole_size_limit) {
        hole_size_limit_ = hole_size_limit;
    }

    /// Returns the maximum size to pre-buffer ahead of the current read
    /// position. Defaults to 256 MiB.
    uint64_t GetPreBufferLimit() const {
        return pre_buffer_limit_;
    }

    /// Sets the maximum size to pre-buffer ahead of the current read position.
    void SetPreBufferLimit(uint64_t pre_buffer_limit) {
        pre_buffer_limit_ = pre_buffer_limit;
    }

    /// Returns the granularity (in bytes) of the block cache entries serving the
    /// small reads that the prefetched ranges do not cover. Defaults to 64 KiB.
    uint64_t GetBlockSize() const {
        return block_size_;
    }

    /// Sets the granularity (in bytes) of the block cache entries.
    void SetBlockSize(uint64_t block_size) {
        block_size_ = block_size;
    }

    /// Returns the maximum total size (in bytes) of the block cache entries of
    /// one file. Zero disables the block cache. Defaults to 1 MiB.
    uint64_t GetBlockCacheLimit() const {
        return block_cache_limit_;
    }

    /// Sets the maximum total size (in bytes) of the block cache entries of one
    /// file. Zero disables the block cache.
    void SetBlockCacheLimit(uint64_t block_cache_limit) {
        block_cache_limit_ = block_cache_limit;
    }

    /// Returns the alignment (in bytes) the adaptive range size is rounded up to, which is also
    /// the smallest size a range is cut to. Defaults to 4 MiB.
    uint64_t GetRangeSplitAlignment() const {
        return range_split_alignment_;
    }

    /// Sets the alignment the adaptive range size is rounded up to. Zero turns the adaptive
    /// sizing off, so the ranges are cut at the configured size limit instead.
    void SetRangeSplitAlignment(uint64_t range_split_alignment) {
        range_split_alignment_ = range_split_alignment;
    }

    /// Returns the number of requests the adaptive range size aims to spread one round of
    /// registered ranges over. Defaults to 10.
    uint64_t GetRangeSplitConcurrency() const {
        return range_split_concurrency_;
    }

    /// Sets the number of requests the adaptive range size aims to spread one round of ranges
    /// over. Zero turns the adaptive sizing off, so the ranges are cut at the configured size
    /// limit instead.
    void SetRangeSplitConcurrency(uint64_t range_split_concurrency) {
        range_split_concurrency_ = range_split_concurrency;
    }

 private:
    // The defaults are aligned with the reader's request granularity and with
    // realistic data file sizes:
    // - range_size_limit matches the parquet reader's 32 MiB request blocks
    //   (Arrow ReadRangeCache's own range limit); a smaller limit cuts entries
    //   below the request size, so a request can never be served from one piece.
    // - pre_buffer_limit must exceed the LARGEST single read a reader issues
    //   (coalesced column-chunk reads of ~128 MiB were observed): fetches are
    //   only dispatched up to this window, so a request reaching past it can
    //   never be served and falls back to a second fetch of the same bytes.
    // - late_range_size_limit bounds the ranges registered mid-read instead of
    //   at Init: those are fetched only just before they are read, and one range
    //   is one request, so they are cut smaller than range_size_limit to be
    //   fetched concurrently rather than in one long request. A read spanning
    //   several of them is still served, as they are adjacent. It is an upper
    //   bound only: the size such a round is actually cut at is derived from the
    //   bytes it registers, see range_split_alignment below.
    // - hole_size_limit trades bytes against requests: coalescing across a gap
    //   reads the gap too, but saves a request, and on remote storage a request
    //   costs a round trip whatever its size. The limit is therefore well above
    //   the page-sized gaps a filtered read leaves between the pages it keeps,
    //   which would otherwise each cost a request of their own.
    uint64_t range_size_limit_ = 32 * 1024 * 1024;
    uint64_t late_range_size_limit_ = 8 * 1024 * 1024;
    uint64_t hole_size_limit_ = 512 * 1024;
    uint64_t pre_buffer_limit_ = 256 * 1024 * 1024;
    // A fixed size limit cuts a small round into fewer requests than could be
    // fetched at once, leaving the storage idle while each of them runs. So the
    // size a round is cut at is derived from the round instead: its bytes are
    // spread over range_split_concurrency requests, rounded up to
    // range_split_alignment, and kept within the size limit above. A round
    // smaller than alignment * concurrency is therefore cut finer than that
    // limit and fetched in a single wave, while a larger one still stops at it.
    // The alignment is also the floor, so the derivation never cuts a round into
    // requests too small to amortize their own round trip.
    uint64_t range_split_alignment_ = 4 * 1024 * 1024;
    uint64_t range_split_concurrency_ = 10;
    // Blocks are aligned to the END of the file, so a block never reaches past
    // EOF. 64 KiB is the granularity the reads no prefetched range covers are
    // shared at: small enough that a metadata read at the tail of a file is
    // served by one block instead of straddling two, large enough that a block
    // fetch does not pull in much more than the reads ask for.
    uint64_t block_size_ = 64 * 1024;
    // One block is enough for the metadata tail of a file; the limit only
    // bounds the pathological case, as blocks are never evicted.
    uint64_t block_cache_limit_ = 1024 * 1024;
};

/// Controls how far a reader prepares the next file before that file is actually read.
///
/// Warmup overlaps remote-storage latency with the read of the current file. Each level takes the
/// next file one step further along the read pipeline: `RAW` covers the remote fetch, `DECODED`
/// adds the decode on top of it. Higher levels hide more latency, but commit more memory and
/// background I/O to files that a query may end up never reading (for example when a LIMIT stops
/// the scan early). Callers can trade latency against memory by picking a level.
enum class PAIMON_EXPORT WarmupLevel {
    /// Do not warm up. The next file's I/O starts only when it is actually read. This is the
    /// behavior from before warmup existed and uses no extra memory or background threads.
    NONE,
    /// Fetch only the next file's raw, still-compressed bytes into memory, and leave the decoder
    /// alone. Overlaps the remote fetch while keeping memory lower than `DECODED`, because no
    /// decoded batches are materialized ahead of the read. It fetches through the read-ahead
    /// cache, so it falls back to `NONE` when that cache is disabled.
    RAW,
    /// Fetch the raw bytes and start the background decode loop as well, so decoded batches are
    /// ready before the file is read. Hides the most latency but uses the most memory. This is the
    /// default.
    DECODED,
};

}  // namespace paimon
