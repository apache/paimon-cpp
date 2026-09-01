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
    CacheConfig();
    CacheConfig(uint64_t range_size_limit, uint64_t hole_size_limit, uint64_t pre_buffer_limit);

    /// Returns the maximum allowed size (in bytes) for a single cached range.
    uint64_t GetRangeSizeLimit() const {
        return range_size_limit_;
    }

    /// Sets the maximum allowed size (in bytes) for a single cached range.
    void SetRangeSizeLimit(uint64_t range_size_limit) {
        range_size_limit_ = range_size_limit;
    }

    /// Returns the maximum gap size (in bytes) considered mergeable between adjacent ranges.
    uint64_t GetHoleSizeLimit() const {
        return hole_size_limit_;
    }

    /// Sets the maximum gap size (in bytes) considered mergeable between adjacent ranges.
    void SetHoleSizeLimit(uint64_t hole_size_limit) {
        hole_size_limit_ = hole_size_limit;
    }

    /// Returns the maximum size to pre-buffer ahead of the current read position.
    uint64_t GetPreBufferLimit() const {
        return pre_buffer_limit_;
    }

    /// Sets the maximum size to pre-buffer ahead of the current read position.
    void SetPreBufferLimit(uint64_t pre_buffer_limit) {
        pre_buffer_limit_ = pre_buffer_limit;
    }

    /// Returns the granularity (in bytes) of the block cache entries serving the
    /// small reads that the prefetched ranges do not cover.
    uint64_t GetBlockSize() const {
        return block_size_;
    }

    /// Sets the granularity (in bytes) of the block cache entries.
    void SetBlockSize(uint64_t block_size) {
        block_size_ = block_size;
    }

    /// Returns the maximum total size (in bytes) of the block cache entries of
    /// one file. Zero disables the block cache.
    uint64_t GetBlockCacheLimit() const {
        return block_cache_limit_;
    }

    /// Sets the maximum total size (in bytes) of the block cache entries of one
    /// file. Zero disables the block cache.
    void SetBlockCacheLimit(uint64_t block_cache_limit) {
        block_cache_limit_ = block_cache_limit;
    }

 private:
    uint64_t range_size_limit_;
    uint64_t hole_size_limit_;
    uint64_t pre_buffer_limit_;
    // Blocks are aligned to the END of the file, so a block never reaches past
    // EOF. 64 KiB matches the footer read of the parquet reader (arrow's
    // kDefaultFooterReadSize), which then covers exactly one block instead of
    // straddling two.
    uint64_t block_size_ = 64 * 1024;
    // One block is enough for the metadata tail of a file; the limit only
    // bounds the pathological case, as blocks are never evicted.
    uint64_t block_cache_limit_ = 1024 * 1024;
};

}  // namespace paimon
