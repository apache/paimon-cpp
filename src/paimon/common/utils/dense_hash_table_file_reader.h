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
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "paimon/common/io/cache/cache_manager.h"
#include "paimon/common/utils/dense_hash_table.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"
#include "paimon/memory/memory_segment.h"
#include "paimon/result.h"

namespace paimon {

/// Reads the native DenseHashTable layout using positional reads and a shared CacheManager.
/// Opening reads only the header, not the bucket array. Find is safe for concurrent callers if
/// the input stream supports concurrent positional reads. Shared streams are not closed here.
/// File paths identify immutable contents and must be unique within the shared cache. Destroying
/// a reader does not invalidate cached pages, which retain their allocation's memory pool.
template <typename Key, typename Value>
class DenseHashTableFileReader {
 public:
    static Result<std::unique_ptr<DenseHashTableFileReader>> Create(
        uint32_t page_size, const std::shared_ptr<CacheManager>& cache_manager,
        const std::shared_ptr<FileSystem>& fs, const std::string& path,
        const std::shared_ptr<MemoryPool>& pool) {
        if (!fs) {
            return Status::Invalid("dense hash table file system is null");
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, fs->Open(path));
        return Create(page_size, cache_manager, path, input, pool);
    }

    static Result<std::unique_ptr<DenseHashTableFileReader>> Create(
        uint32_t page_size, const std::shared_ptr<CacheManager>& cache_manager,
        const std::string& file_path, const std::shared_ptr<InputStream>& input,
        const std::shared_ptr<MemoryPool>& pool) {
        if (!input || !pool || !cache_manager || file_path.empty() || page_size == 0 ||
            page_size > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
            return Status::Invalid("invalid dense hash table reader input, pool or cache options");
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t length, input->Length());
        if (length < static_cast<int64_t>(HashTable::kHeaderSize)) {
            return Status::Invalid("dense hash table file is shorter than its header");
        }
        typename HashTable::Header header;
        PAIMON_RETURN_NOT_OK(
            ReadAt(input.get(), reinterpret_cast<char*>(&header), HashTable::kHeaderSize, 0));
        PAIMON_RETURN_NOT_OK(HashTable::ValidateHeader(header, length));
        return std::unique_ptr<DenseHashTableFileReader>(new DenseHashTableFileReader(
            page_size, cache_manager, file_path, input, header, length, pool));
    }

    DenseHashTableFileReader(const DenseHashTableFileReader&) = delete;
    DenseHashTableFileReader& operator=(const DenseHashTableFileReader&) = delete;

    Result<std::optional<Value>> Find(Key key) const {
        std::optional<Page> page;
        if (key == HashTable::kEmptyKey || key == HashTable::kOtherSpecialKey) {
            PAIMON_ASSIGN_OR_RAISE(
                typename HashTable::Bucket bucket,
                ReadBucket(header_.bucket_count + (key == HashTable::kOtherSpecialKey), &page));
            if (bucket.key > 1) {
                return Status::Invalid("invalid dense hash table special bucket");
            }
            return bucket.key == 0 ? std::nullopt : std::optional<Value>(bucket.value);
        }
        uint64_t index = key % header_.bucket_count;
        for (uint64_t probe = 0; probe < header_.bucket_count; ++probe) {
            PAIMON_ASSIGN_OR_RAISE(typename HashTable::Bucket bucket, ReadBucket(index, &page));
            if (bucket.key == HashTable::kEmptyKey) {
                return std::optional<Value>();
            }
            if (bucket.key == HashTable::kOtherSpecialKey) {
                return Status::Invalid("invalid dense hash table ordinary bucket");
            }
            if (bucket.key == key) {
                return std::optional<Value>(bucket.value);
            }
            index = (index + 1) % header_.bucket_count;
        }
        return std::optional<Value>();
    }

    uint64_t Size() const {
        return header_.key_count;
    }

 private:
    using HashTable = DenseHashTable<Key, Value>;

    struct Page {
        uint64_t index;
        MemorySegment segment;
    };

    DenseHashTableFileReader(uint32_t page_size, std::shared_ptr<CacheManager> cache_manager,
                             std::string file_path, std::shared_ptr<InputStream> input,
                             typename HashTable::Header header, uint64_t length,
                             std::shared_ptr<MemoryPool> pool)
        : input_(std::move(input)),
          pool_(std::move(pool)),
          cache_manager_(std::move(cache_manager)),
          file_path_(std::move(file_path)),
          header_(header),
          length_(length),
          page_size_(page_size) {}

    static Status ReadAt(InputStream* input, char* data, int64_t size, int64_t offset) {
        PAIMON_ASSIGN_OR_RAISE(int64_t read, input->Read(data, size, offset));
        if (read != size) {
            return Status::IOError("short read for dense hash table");
        }
        return Status::OK();
    }

    Result<MemorySegment> ReadPage(uint64_t index) const {
        const uint64_t offset = index * page_size_;
        const int32_t size = static_cast<int32_t>(std::min<uint64_t>(page_size_, length_ - offset));
        std::shared_ptr<CacheKey> key =
            CacheKey::ForPosition(file_path_, offset, size, /*is_index=*/true);
        return cache_manager_->GetPage(
            key,
            [this, offset, size](const std::shared_ptr<CacheKey>&) -> Result<MemorySegment> {
                std::shared_ptr<Bytes> bytes = AllocateBytesKeepingPoolAlive(size, pool_);
                PAIMON_RETURN_NOT_OK(ReadAt(input_.get(), bytes->data(), size, offset));
                return MemorySegment::Wrap(bytes);
            },
            /*eviction_callback=*/nullptr);
    }

    Result<typename HashTable::Bucket> ReadBucket(uint64_t index, std::optional<Page>* page) const {
        uint64_t offset = HashTable::kHeaderSize + index * HashTable::kBucketSize;
        char data[HashTable::kBucketSize];
        uint64_t copied = 0;
        while (copied < HashTable::kBucketSize) {
            const uint64_t page_index = offset / page_size_;
            if (!*page || (*page)->index != page_index) {
                PAIMON_ASSIGN_OR_RAISE(MemorySegment segment, ReadPage(page_index));
                *page = Page{page_index, std::move(segment)};
            }
            const uint64_t position = offset % page_size_;
            const uint64_t size = std::min<uint64_t>((*page)->segment.Size() - position,
                                                     HashTable::kBucketSize - copied);
            std::memcpy(data + copied, (*page)->segment.Data() + position, size);
            copied += size;
            offset += size;
        }
        return HashTable::DecodeBucket(data);
    }

    std::shared_ptr<InputStream> input_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<CacheManager> cache_manager_;
    std::string file_path_;
    typename HashTable::Header header_;
    uint64_t length_;
    uint32_t page_size_;
};

}  // namespace paimon
