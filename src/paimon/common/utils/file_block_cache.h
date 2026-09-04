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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "paimon/common/metrics/atomic_counter_pair.h"
#include "paimon/common/utils/fetch_state.h"
#include "paimon/common/utils/read_ahead_cache.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {

/// A cache of fixed-size blocks of one file, serving the reads that no
/// prefetched range covers: a parquet reader reads the footer and the page index
/// before any range can be registered, and every reader of the file reads the
/// same bytes.
///
/// Blocks are aligned to the END of the file: block 0 is
/// [file_size - block_size, file_size). The metadata of a parquet/orc file lives
/// in its tail and arrow reads exactly the last 64 KiB as the footer, so an
/// end-aligned block matches that read instead of straddling two blocks. It also
/// keeps every block inside the file, as long as the given file size is the size
/// of the file: a block fetch failing because the file is shorter than that only
/// costs the caching of that block, see Read().
///
/// A block is published before its fetch is dispatched, so concurrent readers of
/// the same block wait for that one fetch instead of issuing their own. Readers
/// that must not block a thread wait for it through ReadAsync() instead.
///
/// Blocks are never evicted: once the capacity is reached Read() declines
/// instead of replacing a block. That keeps every dispatched fetch reachable
/// through its block, so Release() and the destructor can wait for the fetches
/// still writing into the block buffers.
class PAIMON_EXPORT FileBlockCache {
 public:
    /// Requests served by a block and fetches issued for the blocks themselves,
    /// reported by the owner of this cache through its own metrics. A snapshot:
    /// the counters keep being recorded while it is read.
    struct Counters {
        uint64_t hits = 0;
        uint64_t hit_bytes = 0;
        uint64_t fetches = 0;
        uint64_t fetch_bytes = 0;
    };

    /// @param stream The stream the blocks are fetched from.
    /// @param file_size Size of the file behind `stream`, which the blocks are
    /// aligned to the end of. Must not be zero.
    /// @param block_size Granularity of the blocks. Must not be zero.
    /// @param capacity Maximum total size of the cached blocks, in bytes.
    /// @param memory_pool The pool the block buffers are allocated from.
    FileBlockCache(const std::shared_ptr<InputStream>& stream, uint64_t file_size,
                   uint64_t block_size, uint64_t capacity,
                   const std::shared_ptr<MemoryPool>& memory_pool);
    ~FileBlockCache();

    /// Serve the given range out of its block, fetching that block first if it
    /// is not cached yet.
    /// @param range The byte range to read.
    /// @param dest Destination buffer with at least `range.length` bytes.
    /// @return true if the range was served and `dest` was filled; false when
    /// one block cannot serve the range (it straddles two blocks, is larger than
    /// a block or reaches past EOF), when the capacity is exhausted or when the
    /// fetch of the block failed, leaving `dest` untouched so the caller can read
    /// the bytes itself. A fetch failure is never reported to the caller: the
    /// block reads more than the caller asked for, so the caller reads its own
    /// bytes instead and reports the failure itself if they cannot be read
    /// either. A block whose fetch failed is not fetched again.
    bool Read(const ByteRange& range, char* dest);

    /// Asynchronous variant of Read(), for a caller whose own interface is
    /// asynchronous and must not block a thread: the fetch of a missing block is
    /// awaited through `callback` instead of waited for.
    ///
    /// `callback` is invoked exactly once, with the outcome Read() would have
    /// returned: inline when the block is already cached, when the read is
    /// declined and when the fetch of the block completes inline - the local
    /// filesystem reads inline - and from the thread resolving the fetch
    /// otherwise. A fetch failure is still not reported to the caller, see
    /// Read().
    ///
    /// The caller must keep `dest` alive until `callback` has been invoked, the
    /// way InputStream::ReadAsync() requires of its own callers.
    /// @param range The byte range to read.
    /// @param dest Destination buffer with at least `range.length` bytes.
    /// @param callback Continuation receiving whether the range was served and
    /// `dest` was filled.
    void ReadAsync(const ByteRange& range, char* dest, std::function<void(bool served)> callback);

    /// Drop all cached blocks, waiting for the fetches still writing into their
    /// buffers. The counters are kept readable for the owner's metrics.
    void Release();

    /// Zero the counters while keeping the cached blocks, which cache the file
    /// rather than a round of reads.
    void ResetCounters();

    Counters GetCounters() const;

 private:
    /// A cached block. Blocks are handed out as shared_ptr so that a reader
    /// keeps its block alive once it has released the lock.
    struct Block {
        ByteRange range;
        std::shared_ptr<Bytes> buffer;
        // The completion of the fetch, resolved by the stream callback and
        // waited for - or awaited through a continuation - by every reader of
        // the block.
        std::shared_ptr<FetchState> state;
    };

    /// Whether one block can serve the given range.
    bool CanServe(const ByteRange& range) const;
    /// Look up the block serving `range`, publishing it when it is missing and
    /// setting `*dispatch` when the caller must fetch it.
    ///
    /// Publishing the state-backed block under the lock before its fetch is
    /// dispatched is what makes concurrent readers of the same block wait for
    /// that one fetch instead of issuing their own.
    /// @return null when one block cannot serve the range or the capacity is
    /// exhausted, leaving the read to the caller.
    std::shared_ptr<Block> AcquireBlock(const ByteRange& range, bool* dispatch);
    /// Index of the block holding `offset`, counted from the END of the file, so
    /// that block 0 is the last block. `offset` must be inside the file.
    uint64_t IndexOf(uint64_t offset) const;
    /// Range of the block with the given index, clamped at the start of the file.
    ByteRange RangeOf(uint64_t index) const;
    /// Fetch the block into its buffer and resolve its state with the outcome.
    /// Must be called after the block has been published, and only by the reader
    /// that published it.
    void Fetch(const std::shared_ptr<Block>& block);
    /// Copy the requested window out of a block whose fetch has been resolved,
    /// and count the hit into `hits`.
    ///
    /// Static and taking the counters explicitly: the continuation of an
    /// asynchronous read runs on the thread resolving the fetch, which can
    /// outlive this cache, so it must not reach the counters through it.
    static bool Serve(const Block& block, const ByteRange& range, char* dest,
                      const std::shared_ptr<AtomicCounterPair>& hits);

    std::shared_ptr<InputStream> stream_;
    uint64_t file_size_;
    uint64_t block_size_;
    uint64_t capacity_;
    std::shared_ptr<MemoryPool> memory_pool_;
    // Blocks are aligned, so keying them by index keeps them disjoint by
    // construction and needs no ordering. A plain mutex is enough: only the
    // reads that no prefetched range covers touch the map, and they are few.
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<Block>> blocks_;
    // Bytes held by blocks_, guarded by mutex_ and bounded by capacity_.
    uint64_t cached_bytes_ = 0;
    // The requests served out of a block, and the block fetches issued to the
    // underlying stream. The hits are shared: the continuation of an
    // asynchronous read counts them from the thread resolving the fetch, which
    // can outlive this cache.
    std::shared_ptr<AtomicCounterPair> hits_;
    AtomicCounterPair fetches_;
};

}  // namespace paimon
