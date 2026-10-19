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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

#include "paimon/common/memory/bytes_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"
#include "paimon/result.h"

namespace paimon {

template <typename Key, typename Value>
class DenseHashTableFileReader;

/// A fixed-capacity, linear-probing hash table for already-hashed unsigned keys.
/// Adapted from:
/// https://github.com/alibaba/havenask/blob/main/aios/storage/indexlib/index/common/hash_table/DenseHashTable.h
/// All key and value bit patterns are supported.
/// Mutation requires external synchronization; mounted hash tables are read-only.
///
/// The native layout is [uint64 bucket_count, uint64 key_count], followed by fixed-size
/// [Key, Value] buckets and two special-key buckets. Integers use host byte order and fields have
/// no padding. Dumped buffers require matching key/value types and endianness when read. This is
/// a container layout, not a versioned Paimon index format.
template <typename Key, typename Value>
class DenseHashTable {
    static_assert(std::is_same_v<Key, uint32_t> || std::is_same_v<Key, uint64_t>);
    static_assert(std::is_integral_v<Value> && !std::is_same_v<Value, bool>);

 public:
    static Result<std::unique_ptr<DenseHashTable>> Create(uint64_t capacity, int32_t occupancy_pct,
                                                          const std::shared_ptr<MemoryPool>& pool) {
        if (!pool) {
            return Status::Invalid("dense hash table requires a memory pool");
        }
        PAIMON_ASSIGN_OR_RAISE(uint64_t bucket_count, BucketCount(capacity, occupancy_pct));
        PAIMON_ASSIGN_OR_RAISE(uint64_t bytes, RequiredBytes(bucket_count));
        auto hash_table = std::unique_ptr<DenseHashTable>(new DenseHashTable());
        hash_table->data_ = AllocateBytesKeepingPoolAlive(bytes, pool);
        hash_table->header_ = Header{bucket_count, 0};
        hash_table->capacity_ = capacity;
        hash_table->writable_ = true;
        hash_table->WriteHeader();
        for (uint64_t index = 0; index < bucket_count; ++index) {
            hash_table->WriteBucket(index, Bucket{kEmptyKey, Value{}});
        }
        hash_table->WriteBucket(bucket_count, Bucket{0, Value{}});
        hash_table->WriteBucket(bucket_count + 1, Bucket{0, Value{}});
        return hash_table;
    }

    /// Mounts a shared buffer without copying. Its bytes must not be mutated while mounted.
    /// The returned hash table retains the buffer; its allocator must also outlive the buffer.
    static Result<std::unique_ptr<DenseHashTable>> MountForRead(std::shared_ptr<const Bytes> data) {
        if (!data || data->size() < kHeaderSize) {
            return Status::Invalid("dense hash table buffer is shorter than its header");
        }
        Header header;
        std::memcpy(&header, data->data(), kHeaderSize);
        PAIMON_RETURN_NOT_OK(ValidateHeader(header, data->size()));
        auto hash_table = std::unique_ptr<DenseHashTable>(new DenseHashTable());
        hash_table->data_ = std::move(data);
        hash_table->header_ = header;
        hash_table->capacity_ = header.bucket_count + 2;
        for (uint64_t index = header.bucket_count; index < header.bucket_count + 2; ++index) {
            if (hash_table->ReadBucket(index).key > 1) {
                return Status::Invalid("invalid dense hash table special bucket");
            }
        }
        return hash_table;
    }

    DenseHashTable(const DenseHashTable&) = delete;
    DenseHashTable& operator=(const DenseHashTable&) = delete;

    /// New keys fail once capacity is reached. Updating an existing key still succeeds.
    Status Insert(Key key, Value value) {
        if (!writable_) {
            return Status::Invalid("cannot insert into a read-only dense hash table");
        }
        if (key == kEmptyKey || key == kOtherSpecialKey) {
            const uint64_t index = header_.bucket_count + (key == kOtherSpecialKey);
            return SetBucket(index, Bucket{1, value}, ReadBucket(index).key == 0);
        }
        uint64_t index = key % header_.bucket_count;
        for (uint64_t probe = 0; probe < header_.bucket_count; ++probe) {
            const Bucket bucket = ReadBucket(index);
            if (bucket.key == kEmptyKey || bucket.key == key) {
                return SetBucket(index, Bucket{key, value}, bucket.key == kEmptyKey);
            }
            index = (index + 1) % header_.bucket_count;
        }
        return Status::Invalid("dense hash table is full");
    }

    std::optional<Value> Find(Key key) const {
        if (key == kEmptyKey || key == kOtherSpecialKey) {
            const Bucket bucket = ReadBucket(header_.bucket_count + (key == kOtherSpecialKey));
            return bucket.key == 0 ? std::nullopt : std::optional<Value>(bucket.value);
        }
        uint64_t index = key % header_.bucket_count;
        for (uint64_t probe = 0; probe < header_.bucket_count; ++probe) {
            const Bucket bucket = ReadBucket(index);
            if (bucket.key == kEmptyKey) {
                return std::nullopt;
            }
            if (bucket.key == key) {
                return bucket.value;
            }
            index = (index + 1) % header_.bucket_count;
        }
        return std::nullopt;
    }

    /// Writes the container layout at the stream's current position. The caller owns flush/close.
    Status WriteTo(OutputStream* output) const {
        if (!output) {
            return Status::Invalid("dense hash table output stream is null");
        }
        const auto length = static_cast<int64_t>(data_->size());
        PAIMON_ASSIGN_OR_RAISE(int64_t written, output->Write(data_->data(), length));
        if (written != length) {
            return Status::IOError("short write for dense hash table");
        }
        return Status::OK();
    }

    uint64_t Size() const {
        return header_.key_count;
    }

    /// Returns the writable key limit, or the physical bucket limit for a read-only mount.
    uint64_t Capacity() const {
        return capacity_;
    }

    uint64_t MemoryUsage() const {
        return data_->size();
    }

 private:
    friend class DenseHashTableFileReader<Key, Value>;

    struct Header {
        uint64_t bucket_count;
        uint64_t key_count;
    };

    struct Bucket {
        Key key;
        Value value;
    };

    static constexpr Key kEmptyKey = static_cast<Key>(0xFFFFFFFFFFFFFEFFULL);
    static constexpr Key kOtherSpecialKey = static_cast<Key>(0xFFFFFFFFFFFFFDFFULL);
    static constexpr uint64_t kHeaderSize = sizeof(Header);
    static constexpr uint64_t kBucketSize = sizeof(Key) + sizeof(Value);
    static constexpr uint64_t kMaxBytes = std::numeric_limits<int64_t>::max();

    DenseHashTable() = default;

    static Result<uint64_t> BucketCount(uint64_t capacity, int32_t occupancy_pct) {
        if (occupancy_pct <= 0 || occupancy_pct > 80) {
            return Status::Invalid("dense hash table occupancy must be between 1 and 80");
        }
        const uint64_t quotient = capacity / occupancy_pct;
        if (quotient > kMaxBytes / 100) {
            return Status::Invalid("dense hash table capacity overflow");
        }
        const uint64_t remainder =
            (capacity % occupancy_pct * 100 + occupancy_pct - 1) / occupancy_pct;
        return std::max<uint64_t>(1, quotient * 100 + remainder);
    }

    static Result<uint64_t> RequiredBytes(uint64_t bucket_count) {
        if (bucket_count == 0 || bucket_count > (kMaxBytes - kHeaderSize) / kBucketSize - 2) {
            return Status::Invalid("invalid dense hash table bucket count");
        }
        return kHeaderSize + (bucket_count + 2) * kBucketSize;
    }

    static Status ValidateHeader(const Header& header, uint64_t length) {
        PAIMON_ASSIGN_OR_RAISE(uint64_t bytes, RequiredBytes(header.bucket_count));
        if (header.key_count > header.bucket_count + 2 || bytes != length) {
            return Status::Invalid(
                "dense hash table header does not match its length or key count");
        }
        return Status::OK();
    }

    static Bucket DecodeBucket(const char* data) {
        Bucket bucket{};
        std::memcpy(&bucket.key, data, sizeof(Key));
        std::memcpy(&bucket.value, data + sizeof(Key), sizeof(Value));
        return bucket;
    }

    Bucket ReadBucket(uint64_t index) const {
        return DecodeBucket(data_->data() + kHeaderSize + index * kBucketSize);
    }

    void WriteBucket(uint64_t index, const Bucket& bucket) {
        char* data = data_->data() + kHeaderSize + index * kBucketSize;
        std::memcpy(data, &bucket.key, sizeof(Key));
        std::memcpy(data + sizeof(Key), &bucket.value, sizeof(Value));
    }

    void WriteHeader() {
        std::memcpy(data_->data(), &header_, kHeaderSize);
    }

    Status SetBucket(uint64_t index, const Bucket& bucket, bool is_new) {
        if (is_new && Size() >= capacity_) {
            return Status::Invalid("dense hash table is full");
        }
        WriteBucket(index, bucket);
        if (is_new) {
            ++header_.key_count;
            WriteHeader();
        }
        return Status::OK();
    }

    std::shared_ptr<const Bytes> data_;
    Header header_{};
    uint64_t capacity_ = 0;
    bool writable_ = false;
};

}  // namespace paimon
