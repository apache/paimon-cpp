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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "paimon/common/memory/bytes_utils.h"
#include "paimon/macros.h"
#include "paimon/result.h"

namespace paimon {

/// A pool-backed hash map which grows by adding blocks without rehashing existing entries.
/// Adapted from indexlib/util/HashMap.h in https://github.com/alibaba/havenask.
/// Keys are already-hashed uint32_t or uint64_t values. Values must be trivially copyable and
/// default constructible. Mutation requires external synchronization; immutable maps may be shared.
template <typename Key, typename Value>
class HashMap {
    static_assert(std::is_same_v<Key, uint32_t> || std::is_same_v<Key, uint64_t>);
    static_assert(std::is_trivially_copyable_v<Value> && std::is_default_constructible_v<Value>);

 public:
    static Result<std::unique_ptr<HashMap>> Create(uint64_t initial_capacity,
                                                   const std::shared_ptr<MemoryPool>& pool) {
        if (!pool || initial_capacity == 0 || initial_capacity > kMaxBytes / 2) {
            return Status::Invalid("hash map requires a pool and a valid positive capacity");
        }
        auto map = std::unique_ptr<HashMap>(new HashMap(pool));
        PAIMON_RETURN_NOT_OK(map->AddBlock(initial_capacity * 2));
        return map;
    }

    HashMap(const HashMap&) = delete;
    HashMap& operator=(const HashMap&) = delete;

    std::optional<Value> Find(Key key) const {
        for (auto iter = blocks_.rbegin(); iter != blocks_.rend(); ++iter) {
            const Block& block = *iter;
            uint64_t position = key % block.bucket_count;
            for (uint64_t probe = 0; probe < block.bucket_count; ++probe) {
                if (!block.Occupied(position)) {
                    break;
                }
                if (block.ReadKey(position) == key) {
                    return block.ReadValue(position);
                }
                position = (position + 1) % block.bucket_count;
            }
        }
        return std::nullopt;
    }

    /// Inserts or updates a key across all blocks. Each key is stored exactly once.
    Status FindAndInsert(Key key, const Value& value) {
        for (auto iter = blocks_.rbegin(); iter != blocks_.rend(); ++iter) {
            Block& block = *iter;
            uint64_t position = key % block.bucket_count;
            for (uint64_t probe = 0; probe < block.bucket_count; ++probe) {
                if (!block.Occupied(position)) {
                    break;
                }
                if (block.ReadKey(position) == key) {
                    block.WriteValue(position, value);
                    return Status::OK();
                }
                position = (position + 1) % block.bucket_count;
            }
        }
        if (blocks_.empty()) {
            PAIMON_RETURN_NOT_OK(AddBlock(initial_bucket_count_));
        } else if (blocks_.back().size >= blocks_.back().bucket_count / 2) {
            if (blocks_.back().bucket_count > kMaxBytes / 2) {
                return Status::Invalid("hash map capacity overflow");
            }
            PAIMON_RETURN_NOT_OK(AddBlock(blocks_.back().bucket_count * 2));
        }
        Block& block = blocks_.back();
        uint64_t position = key % block.bucket_count;
        while (block.Occupied(position)) {
            position = (position + 1) % block.bucket_count;
        }
        block.WriteValue(position, value);
        std::memcpy(block.Entry(position), &key, sizeof(Key));
        block.bitmap[position / 8] |= static_cast<uint8_t>(1U << (position % 8));
        ++block.size;
        ++size_;
        return Status::OK();
    }

    /// Visits each key/value pair once. The callback must not mutate this map.
    template <typename Visitor>
    void ForEach(Visitor&& visitor) const {
        for (const Block& block : blocks_) {
            for (uint64_t position = 0; position < block.bucket_count; ++position) {
                if (block.Occupied(position)) {
                    visitor(block.ReadKey(position), block.ReadValue(position));
                }
            }
        }
    }

    /// Releases all bucket allocations. The next insertion reuses the initial capacity.
    void Clear() {
        blocks_.clear();
        size_ = 0;
        memory_usage_ = 0;
    }

    uint64_t Size() const {
        return size_;
    }

    /// Returns bucket and occupancy-bitmap bytes allocated from the pool.
    uint64_t MemoryUsage() const {
        return memory_usage_;
    }

 private:
    static constexpr uint64_t kEntrySize = sizeof(Key) + sizeof(Value);
    static constexpr uint64_t kMaxBytes = std::numeric_limits<int64_t>::max();

    struct Block {
        char* Entry(uint64_t position) const {
            return data->data() + position * kEntrySize;
        }

        bool Occupied(uint64_t position) const {
            return (bitmap[position / 8] & (1U << (position % 8))) != 0;
        }

        Key ReadKey(uint64_t position) const {
            Key key{};
            std::memcpy(&key, Entry(position), sizeof(Key));
            return key;
        }

        Value ReadValue(uint64_t position) const {
            Value value{};
            std::memcpy(&value, Entry(position) + sizeof(Key), sizeof(Value));
            return value;
        }

        void WriteValue(uint64_t position, const Value& value) {
            std::memcpy(Entry(position) + sizeof(Key), &value, sizeof(Value));
        }

        std::shared_ptr<Bytes> data;
        uint8_t* bitmap = nullptr;
        uint64_t bucket_count = 0;
        uint64_t size = 0;
    };

    explicit HashMap(std::shared_ptr<MemoryPool> pool) : pool_(std::move(pool)) {}

    Status AddBlock(uint64_t bucket_count) {
        const uint64_t bitmap_size = bucket_count / 8 + (bucket_count % 8 != 0);
        if (bucket_count > (kMaxBytes - bitmap_size) / kEntrySize) {
            return Status::Invalid("hash map allocation size overflow");
        }
        const uint64_t bytes = bucket_count * kEntrySize + bitmap_size;
        if (bytes > kMaxBytes - memory_usage_) {
            return Status::Invalid("hash map total allocation size overflow");
        }
        std::shared_ptr<Bytes> data = AllocateBytesKeepingPoolAlive(bytes, pool_);
        auto* bitmap = reinterpret_cast<uint8_t*>(data->data() + bucket_count * kEntrySize);
        std::memset(bitmap, 0, bitmap_size);
        blocks_.push_back(Block{std::move(data), bitmap, bucket_count, 0});
        memory_usage_ += bytes;
        if (initial_bucket_count_ == 0) {
            initial_bucket_count_ = bucket_count;
        }
        return Status::OK();
    }

    std::shared_ptr<MemoryPool> pool_;
    std::vector<Block> blocks_;
    uint64_t initial_bucket_count_ = 0;
    uint64_t size_ = 0;
    uint64_t memory_usage_ = 0;
};

}  // namespace paimon
