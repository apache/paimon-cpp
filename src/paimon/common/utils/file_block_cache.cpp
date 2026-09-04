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

#include "paimon/common/utils/file_block_cache.h"

#include <cstring>

#include "paimon/common/memory/bytes_utils.h"

namespace paimon {

FileBlockCache::FileBlockCache(const std::shared_ptr<InputStream>& stream, uint64_t file_size,
                               uint64_t block_size, uint64_t capacity,
                               const std::shared_ptr<MemoryPool>& memory_pool)
    : stream_(stream),
      file_size_(file_size),
      block_size_(block_size),
      capacity_(capacity),
      memory_pool_(memory_pool),
      hits_(std::make_shared<AtomicCounterPair>()) {}

FileBlockCache::~FileBlockCache() {
    // The fetches write into the block buffers, so they must not outlive the
    // stream they read from.
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& block : blocks_) {
        block.second->state->Wait();
    }
}

bool FileBlockCache::Read(const ByteRange& range, char* dest) {
    bool dispatch = false;
    std::shared_ptr<Block> block = AcquireBlock(range, &dispatch);
    if (block == nullptr) {
        return false;
    }
    if (dispatch) {
        Fetch(block);
    }
    // Wait OUTSIDE the lock, so that a reader waiting for a fetch does not keep
    // the other readers out of the map.
    if (!block->state->Wait().ok()) {
        // A block fetch reads more than the caller asked for, so its failure must
        // not fail the caller's read: the read goes back to the caller, which
        // reports the real error itself if its own bytes cannot be read either.
        // This is what keeps a block whose range the file does not have - the
        // file metadata records a size larger than the physical file - from
        // failing the reads of the region it covers.
        //
        // The block is left in place with its failed state, so the later reads of
        // that block are declined without fetching it again.
        return false;
    }
    return Serve(*block, range, dest, hits_);
}

void FileBlockCache::ReadAsync(const ByteRange& range, char* dest,
                               std::function<void(bool served)> callback) {
    bool dispatch = false;
    std::shared_ptr<Block> block = AcquireBlock(range, &dispatch);
    if (block == nullptr) {
        callback(false);
        return;
    }
    if (dispatch) {
        Fetch(block);
    }
    // The continuation runs on the thread resolving the fetch, which may outlive
    // this cache: it holds the block and the counters it touches, never `this`.
    // A fetch resolving inline - the local filesystem reads inline - makes the
    // state resolve before this point, and the continuation then runs here, the
    // way a synchronous read would have.
    block->state->OnComplete(
        [block, range, dest, hits = hits_, callback = std::move(callback)](Status status) mutable {
            // A failed fetch declines the read instead of reporting the failure, see
            // Read().
            if (!status.ok() || !Serve(*block, range, dest, hits)) {
                callback(false);
                return;
            }
            callback(true);
        });
}

void FileBlockCache::Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    // Blocks are never evicted, so waiting on blocks_ covers every dispatched
    // fetch before the buffers they write into go away.
    for (auto& block : blocks_) {
        block.second->state->Wait();
    }
    blocks_.clear();
    cached_bytes_ = 0;
}

void FileBlockCache::ResetCounters() {
    hits_->Reset();
    fetches_.Reset();
}

FileBlockCache::Counters FileBlockCache::GetCounters() const {
    Counters counters;
    counters.hits = hits_->Count();
    counters.hit_bytes = hits_->Bytes();
    counters.fetches = fetches_.Count();
    counters.fetch_bytes = fetches_.Bytes();
    return counters;
}

std::shared_ptr<FileBlockCache::Block> FileBlockCache::AcquireBlock(const ByteRange& range,
                                                                    bool* dispatch) {
    if (!CanServe(range)) {
        return nullptr;
    }
    const uint64_t index = IndexOf(range.offset);
    // Publishing the state-backed block under the lock before its fetch is
    // dispatched is what makes concurrent readers of the same block wait for
    // that one fetch instead of issuing their own.
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = blocks_.find(index);
    if (it != blocks_.end()) {
        return it->second;
    }
    const ByteRange block_range = RangeOf(index);
    // Blocks are never evicted, so an exhausted capacity means this read goes
    // back to the caller instead of replacing a cached block.
    if (cached_bytes_ + block_range.length > capacity_) {
        return nullptr;
    }
    auto block = std::make_shared<Block>();
    block->range = block_range;
    // The buffer keeps the pool alive, for the fetch callbacks that a stream
    // destroys later than it resolves them.
    block->buffer = AllocateBytesKeepingPoolAlive(block_range.length, memory_pool_);
    block->state = std::make_shared<FetchState>();
    blocks_.emplace(index, block);
    cached_bytes_ += block_range.length;
    *dispatch = true;
    return block;
}

bool FileBlockCache::Serve(const Block& block, const ByteRange& range, char* dest,
                           const std::shared_ptr<AtomicCounterPair>& hits) {
    std::memcpy(dest, block.buffer->data() + (range.offset - block.range.offset), range.length);
    hits->Add(range.length);
    return true;
}

bool FileBlockCache::CanServe(const ByteRange& range) const {
    if (capacity_ == 0 || block_size_ == 0 || file_size_ == 0) {
        return false;
    }
    if (range.length == 0 || range.length > block_size_) {
        return false;
    }
    // A read reaching past EOF is left to the caller: serving it would mean
    // short-reading into the block buffer.
    if (range.offset >= file_size_ || range.length > file_size_ - range.offset) {
        return false;
    }
    // A read straddling two blocks would need both of them to be present; it is
    // left to the caller instead, which keeps one block per served read.
    return IndexOf(range.offset) == IndexOf(range.offset + range.length - 1);
}

uint64_t FileBlockCache::IndexOf(uint64_t offset) const {
    // Counted from the end of the file, so that block 0 is the last block.
    return (file_size_ - 1 - offset) / block_size_;
}

ByteRange FileBlockCache::RangeOf(uint64_t index) const {
    const uint64_t end = file_size_ - index * block_size_;
    const uint64_t offset = end > block_size_ ? end - block_size_ : 0;
    return {offset, end - offset};
}

void FileBlockCache::Fetch(const std::shared_ptr<Block>& block) {
    fetches_.Add(block->range.length);
    auto state = block->state;
    auto buffer = block->buffer;
    // The buffer and the state are captured, so the async read keeps its
    // destination and the fetch it resolves alive. The buffer keeps the memory
    // pool alive as well, so a callback outliving this cache still frees the
    // buffer against a live pool.
    stream_->ReadAsync(buffer->data(), static_cast<int64_t>(buffer->size()),
                       static_cast<int64_t>(block->range.offset),
                       [state, buffer](Status status) { state->Complete(std::move(status)); });
}

}  // namespace paimon
