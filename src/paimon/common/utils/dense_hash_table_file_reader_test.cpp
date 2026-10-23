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

#include "paimon/common/utils/dense_hash_table_file_reader.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/cache/cache_manager.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/bytes_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/dense_hash_table.h"
#include "paimon/common/utils/hash_map.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

using HashTable = DenseHashTable<uint64_t, int64_t>;
using Reader = DenseHashTableFileReader<uint64_t, int64_t>;

int64_t CachedBytes(const std::shared_ptr<CacheManager>& cache_manager) {
    return checked_pointer_cast<LruCache>(cache_manager->IndexCache())->GetCurrentWeight();
}

class CountingInputStream : public ByteArrayInputStream {
 public:
    explicit CountingInputStream(std::shared_ptr<Bytes> bytes)
        : ByteArrayInputStream(bytes->data(), bytes->size()), bytes_(std::move(bytes)) {}

    using ByteArrayInputStream::Read;

    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        ++read_calls;
        if (fail_reads) {
            return Status::IOError("injected dense hash table read failure");
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t read, ByteArrayInputStream::Read(
                                                 buffer, short_reads ? size - 1 : size, offset));
        bytes_read += read;
        return read;
    }

    std::atomic<int64_t> read_calls{0};
    std::atomic<int64_t> bytes_read{0};
    bool short_reads = false;
    bool fail_reads = false;

 private:
    std::shared_ptr<Bytes> bytes_;
};

Result<std::shared_ptr<Bytes>> FlushHashTable(const HashTable& hash_table,
                                              const std::shared_ptr<MemoryPool>& pool) {
    ByteArrayOutputStream output(std::make_unique<MemorySegmentOutputStream>(1024, pool));
    PAIMON_RETURN_NOT_OK(hash_table.WriteTo(&output));
    PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> bytes, output.Finish(pool.get()));
    std::shared_ptr<Bytes> result = AllocateBytesKeepingPoolAlive(bytes->size(), pool);
    std::memcpy(result->data(), bytes->data(), bytes->size());
    return result;
}

Result<std::shared_ptr<Bytes>> MakeHashTableBytes(const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<HashTable> hash_table,
                           HashTable::Create(1000, /*occupancy_pct=*/65, pool));
    for (uint64_t key = 0; key < 998; ++key) {
        PAIMON_RETURN_NOT_OK(hash_table->Insert(key, key + 10));
    }
    PAIMON_RETURN_NOT_OK(hash_table->Insert(0xFFFFFFFFFFFFFEFFULL, -1));
    PAIMON_RETURN_NOT_OK(
        hash_table->Insert(0xFFFFFFFFFFFFFDFFULL, std::numeric_limits<int64_t>::min()));
    return FlushHashTable(*hash_table, pool);
}

}  // namespace

TEST(DenseHashTableFileReaderTest, TestOpenIsLazyAndRepeatedLookupsUseCache) {
    auto cache_manager = std::make_shared<CacheManager>(512, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, MakeHashTableBytes(GetDefaultPool()));
    auto input = std::make_shared<CountingInputStream>(bytes);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Reader> reader,
                         Reader::Create(/*page_size=*/256, cache_manager, "dense-hash-table", input,
                                        GetDefaultPool()));
    ASSERT_EQ(reader->Size(), 1000);
    ASSERT_EQ(input->read_calls.load(), 1);
    ASSERT_EQ(input->bytes_read.load(), 16);
    ASSERT_EQ(CachedBytes(cache_manager), 0);

    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(0));
    ASSERT_EQ(value, 10);
    ASSERT_EQ(input->read_calls.load(), 2);
    ASSERT_EQ(input->bytes_read.load(), 16 + 256);
    ASSERT_OK_AND_ASSIGN(value, reader->Find(0));
    ASSERT_EQ(value, 10);
    ASSERT_EQ(input->read_calls.load(), 2);
    ASSERT_EQ(CachedBytes(cache_manager), 256);
    ASSERT_OK_AND_ASSIGN(int64_t position, input->GetPos());
    ASSERT_EQ(position, 0);
}

TEST(DenseHashTableFileReaderTest, TestPageBoundariesSpecialKeysAndMissingKeys) {
    auto cache_manager = std::make_shared<CacheManager>(4 * 17, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, MakeHashTableBytes(GetDefaultPool()));
    auto input = std::make_shared<CountingInputStream>(bytes);
    // Seventeen-byte pages force buckets to span page boundaries.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Reader> reader,
        Reader::Create(17, cache_manager, "dense-hash-table", input, GetDefaultPool()));
    for (uint64_t key : {uint64_t{0}, uint64_t{1}, uint64_t{30}, uint64_t{997}}) {
        ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(key));
        ASSERT_EQ(value, static_cast<int64_t>(key + 10));
        ASSERT_LE(CachedBytes(cache_manager), 4 * 17);
    }
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(0xFFFFFFFFFFFFFEFFULL));
    ASSERT_EQ(value, -1);
    ASSERT_OK_AND_ASSIGN(value, reader->Find(0xFFFFFFFFFFFFFDFFULL));
    ASSERT_EQ(value, std::numeric_limits<int64_t>::min());
    ASSERT_OK_AND_ASSIGN(value, reader->Find(1000));
    ASSERT_FALSE(value);
    ASSERT_OK_AND_ASSIGN(value, reader->Find(std::numeric_limits<uint64_t>::max()));
    ASSERT_FALSE(value);
}

TEST(DenseHashTableFileReaderTest, TestProbingAcrossPagesAndWraparound) {
    auto cache_manager = std::make_shared<CacheManager>(32, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> hash_table,
                         HashTable::Create(3, /*occupancy_pct=*/50, GetDefaultPool()));
    ASSERT_OK(hash_table->Insert(5, 1));
    ASSERT_OK(hash_table->Insert(11, 2));
    ASSERT_OK(hash_table->Insert(17, 3));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes,
                         FlushHashTable(*hash_table, GetDefaultPool()));
    auto input = std::make_shared<CountingInputStream>(bytes);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Reader> reader,
        Reader::Create(16, cache_manager, "dense-hash-table", input, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(17));
    ASSERT_EQ(value, 3);
    ASSERT_OK_AND_ASSIGN(value, reader->Find(23));
    ASSERT_FALSE(value);
    ASSERT_OK_AND_ASSIGN(value, reader->Find(0xFFFFFFFFFFFFFEFFULL));
    ASSERT_FALSE(value);
    ASSERT_OK_AND_ASSIGN(value, reader->Find(0xFFFFFFFFFFFFFDFFULL));
    ASSERT_FALSE(value);
}

TEST(DenseHashTableFileReaderTest, TestEmptyHashTable) {
    auto cache_manager =
        std::make_shared<CacheManager>(8 * 1024 * 1024, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> hash_table,
                         HashTable::Create(0, /*occupancy_pct=*/65, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes,
                         FlushHashTable(*hash_table, GetDefaultPool()));
    auto input = std::make_shared<CountingInputStream>(bytes);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Reader> reader,
        Reader::Create(64 * 1024, cache_manager, "dense-hash-table", input, GetDefaultPool()));
    ASSERT_EQ(reader->Size(), 0);
    for (uint64_t key :
         {uint64_t{0}, uint64_t{0xFFFFFFFFFFFFFEFFULL}, uint64_t{0xFFFFFFFFFFFFFDFFULL}}) {
        ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(key));
        ASSERT_FALSE(value);
    }
}

TEST(DenseHashTableFileReaderTest, TestMixedIntegerWidths) {
    auto cache_manager = std::make_shared<CacheManager>(68, /*high_priority_pool_ratio=*/0.0);
    using MixedHashTable = DenseHashTable<uint32_t, int64_t>;
    using MixedReader = DenseHashTableFileReader<uint32_t, int64_t>;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MixedHashTable> hash_table,
                         MixedHashTable::Create(3, /*occupancy_pct=*/65, GetDefaultPool()));
    ASSERT_OK(hash_table->Insert(1, -10));
    ASSERT_OK(hash_table->Insert(0xFFFFFEFFU, 20));
    ASSERT_OK(hash_table->Insert(0xFFFFFDFFU, 30));
    ByteArrayOutputStream output(
        std::make_unique<MemorySegmentOutputStream>(1024, GetDefaultPool()));
    ASSERT_OK(hash_table->WriteTo(&output));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, output.Finish(GetDefaultPool().get()));
    auto input = std::make_shared<CountingInputStream>(bytes);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<MixedReader> reader,
        MixedReader::Create(17, cache_manager, "dense-hash-table", input, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(1));
    ASSERT_EQ(value, -10);
    ASSERT_OK_AND_ASSIGN(value, reader->Find(0xFFFFFEFFU));
    ASSERT_EQ(value, 20);
    ASSERT_OK_AND_ASSIGN(value, reader->Find(0xFFFFFDFFU));
    ASSERT_EQ(value, 30);
}

TEST(DenseHashTableFileReaderTest, TestCacheEvictionAndDisabledCache) {
    auto cache_manager = std::make_shared<CacheManager>(256, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, MakeHashTableBytes(GetDefaultPool()));
    auto input = std::make_shared<CountingInputStream>(bytes);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Reader> reader,
        Reader::Create(256, cache_manager, "dense-hash-table", input, GetDefaultPool()));
    for (uint64_t key : {uint64_t{0}, uint64_t{500}, uint64_t{0}}) {
        ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(key));
        ASSERT_EQ(value, static_cast<int64_t>(key + 10));
        ASSERT_EQ(CachedBytes(cache_manager), 256);
    }
    ASSERT_EQ(input->read_calls.load(), 4);
    reader.reset();
    cache_manager = std::make_shared<CacheManager>(0, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(
        reader, Reader::Create(256, cache_manager, "dense-hash-table", input, GetDefaultPool()));
    const int64_t before = input->read_calls.load();
    for (int32_t iteration = 0; iteration < 2; ++iteration) {
        ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(0));
        ASSERT_EQ(value, 10);
        ASSERT_EQ(CachedBytes(cache_manager), 0);
    }
    ASSERT_EQ(input->read_calls.load(), before + 2);
}

TEST(DenseHashTableFileReaderTest, TestSharedCacheSurvivesReaderDestruction) {
    auto cache_manager = std::make_shared<CacheManager>(512, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, MakeHashTableBytes(GetDefaultPool()));
    auto first_input = std::make_shared<CountingInputStream>(bytes);
    auto second_input = std::make_shared<CountingInputStream>(bytes);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Reader> first,
        Reader::Create(256, cache_manager, "same-file", first_input, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Reader> second,
        Reader::Create(256, cache_manager, "same-file", second_input, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, first->Find(0));
    ASSERT_EQ(value, 10);
    ASSERT_EQ(first_input->read_calls.load(), 2);
    first.reset();
    ASSERT_EQ(CachedBytes(cache_manager), 256);

    ASSERT_OK_AND_ASSIGN(value, second->Find(0));
    ASSERT_EQ(value, 10);
    ASSERT_EQ(second_input->read_calls.load(), 1);
    second.reset();
    ASSERT_OK_AND_ASSIGN(
        second, Reader::Create(256, cache_manager, "same-file", second_input, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(value, second->Find(0));
    ASSERT_EQ(value, 10);
    ASSERT_EQ(second_input->read_calls.load(), 2);
}

TEST(DenseHashTableFileReaderTest, TestSharedBudgetAndDistinctFiles) {
    auto cache_manager = std::make_shared<CacheManager>(256, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> first_bytes, MakeHashTableBytes(GetDefaultPool()));
    std::shared_ptr<Bytes> second_bytes =
        AllocateBytesKeepingPoolAlive(first_bytes->size(), GetDefaultPool());
    std::memcpy(second_bytes->data(), first_bytes->data(), first_bytes->size());
    // Change key zero's value in the second file; both lookups read the same byte range.
    const int64_t second_value = 1000;
    std::memcpy(second_bytes->data() + 16 + sizeof(uint64_t), &second_value, sizeof(second_value));
    auto first_input = std::make_shared<CountingInputStream>(first_bytes);
    auto second_input = std::make_shared<CountingInputStream>(second_bytes);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Reader> first,
        Reader::Create(256, cache_manager, "first-file", first_input, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Reader> second,
        Reader::Create(256, cache_manager, "second-file", second_input, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, first->Find(0));
    ASSERT_EQ(value, 10);
    ASSERT_OK_AND_ASSIGN(value, second->Find(0));
    ASSERT_EQ(value, second_value);
    ASSERT_EQ(CachedBytes(cache_manager), 256);
    ASSERT_EQ(cache_manager->IndexCache()->Size(), 1);
    ASSERT_OK_AND_ASSIGN(value, first->Find(0));
    ASSERT_EQ(value, 10);
    ASSERT_EQ(first_input->read_calls.load(), 3);
    ASSERT_EQ(second_input->read_calls.load(), 2);
    ASSERT_EQ(CachedBytes(cache_manager), 256);
}

TEST(DenseHashTableFileReaderTest, TestHashMapToDenseFileRoundTrip) {
    auto cache_manager =
        std::make_shared<CacheManager>(8 * 1024 * 1024, /*high_priority_pool_ratio=*/0.0);
    std::unique_ptr<UniqueTestDirectory> directory = UniqueTestDirectory::Create();
    ASSERT_NE(directory, nullptr);
    using Map = HashMap<uint64_t, int64_t>;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Map> map, Map::Create(1, GetDefaultPool()));
    for (uint64_t key = 0; key < 300; ++key) {
        ASSERT_OK(map->FindAndInsert(key, key));
    }
    ASSERT_OK(map->FindAndInsert(0, -1));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> hash_table,
                         HashTable::Create(map->Size(), /*occupancy_pct=*/65, GetDefaultPool()));
    Status inserted;
    map->ForEach([&](uint64_t key, int64_t value) {
        if (inserted.ok()) {
            inserted = hash_table->Insert(key, value);
        }
    });
    ASSERT_OK(inserted);
    const std::string path = directory->Str() + "/dense-hash-table";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output,
                         directory->GetFileSystem()->Create(path, /*overwrite=*/false));
    ASSERT_OK(hash_table->WriteTo(output.get()));
    ASSERT_OK(output->Flush());
    ASSERT_OK(output->Close());
    hash_table.reset();

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Reader> reader,
                         Reader::Create(64 * 1024, cache_manager, directory->GetFileSystem(), path,
                                        GetDefaultPool()));
    for (uint64_t key = 0; key < 300; ++key) {
        ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(key));
        ASSERT_EQ(value, map->Find(key));
    }
}

TEST(DenseHashTableFileReaderTest, TestConcurrentLookupsAndBufferLifetime) {
    auto cache_manager = std::make_shared<CacheManager>(512, /*high_priority_pool_ratio=*/0.0);
    std::shared_ptr<MemoryPool> pool = GetMemoryPool();
    std::weak_ptr<MemoryPool> weak_pool = pool;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, MakeHashTableBytes(pool));
    auto input = std::make_shared<CountingInputStream>(bytes);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Reader> reader,
                         Reader::Create(256, cache_manager, "dense-hash-table", input, pool));
    bytes.reset();
    input.reset();
    pool.reset();
    ASSERT_FALSE(weak_pool.expired());
    std::vector<std::thread> threads;
    for (int32_t thread = 0; thread < 4; ++thread) {
        threads.emplace_back([&reader]() {
            for (uint64_t key = 0; key < 998; ++key) {
                ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(key));
                ASSERT_EQ(value, static_cast<int64_t>(key + 10));
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    ASSERT_LE(CachedBytes(cache_manager), 512);
    reader.reset();
    ASSERT_FALSE(weak_pool.expired());
    cache_manager.reset();
    ASSERT_TRUE(weak_pool.expired());
}

TEST(DenseHashTableFileReaderTest, TestInvalidOptionsAndHeader) {
    auto cache_manager = std::make_shared<CacheManager>(512, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, MakeHashTableBytes(GetDefaultPool()));
    auto input = std::make_shared<CountingInputStream>(bytes);
    ASSERT_NOK(Reader::Create(256, cache_manager, "dense-hash-table",
                              std::shared_ptr<InputStream>(), GetDefaultPool()));
    ASSERT_NOK(Reader::Create(256, cache_manager, "dense-hash-table", input, nullptr));
    ASSERT_NOK(Reader::Create(0, cache_manager, "dense-hash-table", input, GetDefaultPool()));
    ASSERT_NOK(Reader::Create(std::numeric_limits<uint32_t>::max(), cache_manager,
                              "dense-hash-table", input, GetDefaultPool()));
    ASSERT_NOK(Reader::Create(16, nullptr, "dense-hash-table", input, GetDefaultPool()));
    ASSERT_NOK(Reader::Create(256, cache_manager, "", input, GetDefaultPool()));
    ASSERT_NOK(Reader::Create(256, cache_manager, std::shared_ptr<FileSystem>(), "unused",
                              GetDefaultPool()));

    auto short_input =
        std::make_shared<CountingInputStream>(AllocateBytesKeepingPoolAlive(15, GetDefaultPool()));
    ASSERT_NOK_WITH_MSG(
        Reader::Create(256, cache_manager, "dense-hash-table", short_input, GetDefaultPool()),
        "shorter than its header");
    const uint64_t zero = 0;
    std::memcpy(bytes->data(), &zero, sizeof(zero));
    ASSERT_NOK_WITH_MSG(
        Reader::Create(256, cache_manager, "dense-hash-table", input, GetDefaultPool()),
        "bucket count");
    const uint64_t overflow = std::numeric_limits<uint64_t>::max();
    std::memcpy(bytes->data(), &overflow, sizeof(overflow));
    ASSERT_NOK_WITH_MSG(
        Reader::Create(256, cache_manager, "dense-hash-table", input, GetDefaultPool()),
        "bucket count");
}

TEST(DenseHashTableFileReaderTest, TestReadErrorsArePropagatedAndNotCached) {
    auto cache_manager = std::make_shared<CacheManager>(512, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, MakeHashTableBytes(GetDefaultPool()));
    auto input = std::make_shared<CountingInputStream>(bytes);
    input->short_reads = true;
    ASSERT_NOK_WITH_MSG(
        Reader::Create(256, cache_manager, "dense-hash-table", input, GetDefaultPool()),
        "short read");
    input->short_reads = false;
    input->fail_reads = true;
    ASSERT_NOK_WITH_MSG(
        Reader::Create(256, cache_manager, "dense-hash-table", input, GetDefaultPool()),
        "injected dense hash table read failure");
    input->fail_reads = false;
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Reader> reader,
        Reader::Create(256, cache_manager, "dense-hash-table", input, GetDefaultPool()));
    input->short_reads = true;
    ASSERT_NOK_WITH_MSG(reader->Find(0), "short read");
    ASSERT_EQ(CachedBytes(cache_manager), 0);
    input->short_reads = false;
    input->fail_reads = true;
    ASSERT_NOK_WITH_MSG(reader->Find(0), "injected dense hash table read failure");
    ASSERT_EQ(CachedBytes(cache_manager), 0);
    input->fail_reads = false;
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> value, reader->Find(0));
    ASSERT_EQ(value, 10);
}

TEST(DenseHashTableFileReaderTest, TestRejectInvalidSpecialBucket) {
    auto cache_manager =
        std::make_shared<CacheManager>(8 * 1024 * 1024, /*high_priority_pool_ratio=*/0.0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, MakeHashTableBytes(GetDefaultPool()));
    const uint64_t invalid_state = 2;
    std::memcpy(bytes->data() + bytes->size() - 32, &invalid_state, sizeof(invalid_state));
    auto input = std::make_shared<CountingInputStream>(bytes);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Reader> reader,
        Reader::Create(64 * 1024, cache_manager, "dense-hash-table", input, GetDefaultPool()));
    ASSERT_NOK_WITH_MSG(reader->Find(0xFFFFFFFFFFFFFEFFULL), "special bucket");
}

}  // namespace paimon::test
