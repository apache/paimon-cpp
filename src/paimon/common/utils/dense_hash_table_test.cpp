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

#include "paimon/common/utils/dense_hash_table.h"

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/bytes_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

using HashTable = DenseHashTable<uint64_t, int64_t>;

template <typename Key, typename Value>
Result<std::shared_ptr<Bytes>> FlushHashTable(const DenseHashTable<Key, Value>& hash_table,
                                              const std::shared_ptr<MemoryPool>& pool) {
    ByteArrayOutputStream output(std::make_unique<MemorySegmentOutputStream>(1024, pool));
    PAIMON_RETURN_NOT_OK(hash_table.WriteTo(&output));
    PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> bytes, output.Finish(pool.get()));
    std::shared_ptr<Bytes> result = AllocateBytesKeepingPoolAlive(bytes->size(), pool);
    std::memcpy(result->data(), bytes->data(), bytes->size());
    return result;
}

class FailingOutputStream : public MockOutputStream {
 public:
    Result<int64_t> Write(const char*, int64_t) override {
        return Status::IOError("injected dense hash table write failure");
    }
};

}  // namespace

TEST(DenseHashTableTest, TestProbingWraparoundAndCapacity) {
    // Three entries at 50% occupancy give six buckets. These keys all start at bucket five.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> hash_table,
                         HashTable::Create(3, /*occupancy_pct=*/50, GetDefaultPool()));
    ASSERT_EQ(hash_table->Capacity(), 3);
    ASSERT_OK(hash_table->Insert(5, 10));
    ASSERT_OK(hash_table->Insert(11, 20));
    ASSERT_OK(hash_table->Insert(17, 30));
    ASSERT_EQ(hash_table->Find(5), 10);
    ASSERT_EQ(hash_table->Find(11), 20);
    ASSERT_EQ(hash_table->Find(17), 30);
    ASSERT_FALSE(hash_table->Find(23));
    ASSERT_NOK_WITH_MSG(hash_table->Insert(23, 40), "full");
    ASSERT_OK(hash_table->Insert(5, -1));
    ASSERT_EQ(hash_table->Find(5), -1);
    ASSERT_EQ(hash_table->Size(), 3);
}

TEST(DenseHashTableTest, TestAllKeyAndValueBitPatterns) {
    const std::vector<uint64_t> keys = {0,
                                        1,
                                        0xFFFFFFFFFFFFFEFFULL,
                                        0xFFFFFFFFFFFFFDFFULL,
                                        std::numeric_limits<uint64_t>::max(),
                                        uint64_t{1} << 63};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> hash_table,
                         HashTable::Create(keys.size(), /*occupancy_pct=*/65, GetDefaultPool()));
    for (size_t index = 0; index < keys.size(); ++index) {
        ASSERT_FALSE(hash_table->Find(keys[index]));
        ASSERT_OK(hash_table->Insert(keys[index], static_cast<int64_t>(index) - 3));
    }
    for (size_t index = 0; index < keys.size(); ++index) {
        ASSERT_EQ(hash_table->Find(keys[index]), static_cast<int64_t>(index) - 3);
    }
    ASSERT_OK(hash_table->Insert(keys[2], std::numeric_limits<int64_t>::min()));
    ASSERT_OK(hash_table->Insert(keys[3], std::numeric_limits<int64_t>::max()));
    ASSERT_EQ(hash_table->Find(keys[2]), std::numeric_limits<int64_t>::min());
    ASSERT_EQ(hash_table->Find(keys[3]), std::numeric_limits<int64_t>::max());
    ASSERT_EQ(hash_table->Size(), keys.size());
    ASSERT_NOK(hash_table->Insert(2, 4));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> full,
                         HashTable::Create(1, /*occupancy_pct=*/65, GetDefaultPool()));
    ASSERT_OK(full->Insert(1, 1));
    ASSERT_NOK(full->Insert(keys[2], 2));
    ASSERT_NOK(full->Insert(keys[3], 3));
    ASSERT_FALSE(full->Find(keys[2]));
    ASSERT_FALSE(full->Find(keys[3]));
}

TEST(DenseHashTableTest, TestEmptyAndInvalidArguments) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> hash_table,
                         HashTable::Create(0, /*occupancy_pct=*/65, GetDefaultPool()));
    ASSERT_EQ(hash_table->Size(), 0);
    ASSERT_FALSE(hash_table->Find(0));
    ASSERT_FALSE(hash_table->Find(0xFFFFFFFFFFFFFEFFULL));
    ASSERT_FALSE(hash_table->Find(0xFFFFFFFFFFFFFDFFULL));
    ASSERT_NOK_WITH_MSG(hash_table->Insert(0, 0), "full");
    ASSERT_NOK(HashTable::Create(1, 65, nullptr));
    ASSERT_NOK(HashTable::Create(1, 0, GetDefaultPool()));
    ASSERT_NOK(HashTable::Create(1, -1, GetDefaultPool()));
    ASSERT_NOK(HashTable::Create(1, 81, GetDefaultPool()));
    ASSERT_NOK(HashTable::Create(std::numeric_limits<uint64_t>::max(), 65, GetDefaultPool()));
    ASSERT_NOK(HashTable::Create(std::numeric_limits<int64_t>::max(), 80, GetDefaultPool()));
}

TEST(DenseHashTableTest, TestFlushAndReadOnlyMountKeepPoolAlive) {
    std::shared_ptr<MemoryPool> pool = GetMemoryPool();
    std::weak_ptr<MemoryPool> weak_pool = pool;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> hash_table,
                         HashTable::Create(5000, /*occupancy_pct=*/65, pool));
    for (uint64_t key = 0; key < 5000; ++key) {
        ASSERT_OK(hash_table->Insert(key, key + 10));
    }
    ASSERT_EQ(pool->CurrentUsage(), hash_table->MemoryUsage());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, FlushHashTable(*hash_table, pool));
    ASSERT_EQ(bytes->size(), hash_table->MemoryUsage());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> mounted, HashTable::MountForRead(bytes));
    hash_table.reset();
    bytes.reset();
    pool.reset();
    ASSERT_FALSE(weak_pool.expired());
    ASSERT_EQ(mounted->Size(), 5000);
    for (uint64_t key = 0; key < 5000; ++key) {
        ASSERT_EQ(mounted->Find(key), static_cast<int64_t>(key + 10));
    }
    ASSERT_FALSE(mounted->Find(5000));
    ASSERT_NOK_WITH_MSG(mounted->Insert(0, 0), "read-only");
    mounted.reset();
    ASSERT_TRUE(weak_pool.expired());
}

TEST(DenseHashTableTest, TestRejectMalformedBuffers) {
    ASSERT_NOK(HashTable::MountForRead(nullptr));
    ASSERT_NOK(HashTable::MountForRead(AllocateBytesKeepingPoolAlive(15, GetDefaultPool())));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> hash_table,
                         HashTable::Create(1, /*occupancy_pct=*/65, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes,
                         FlushHashTable(*hash_table, GetDefaultPool()));

    const uint64_t zero = 0;
    const uint64_t overflow = std::numeric_limits<uint64_t>::max();
    std::memcpy(bytes->data(), &zero, sizeof(zero));
    ASSERT_NOK_WITH_MSG(HashTable::MountForRead(bytes), "bucket count");
    std::memcpy(bytes->data(), &overflow, sizeof(overflow));
    ASSERT_NOK_WITH_MSG(HashTable::MountForRead(bytes), "bucket count");

    ASSERT_OK_AND_ASSIGN(bytes, FlushHashTable(*hash_table, GetDefaultPool()));
    std::memcpy(bytes->data() + sizeof(uint64_t), &overflow, sizeof(overflow));
    ASSERT_NOK_WITH_MSG(HashTable::MountForRead(bytes), "key count");

    ASSERT_OK_AND_ASSIGN(bytes, FlushHashTable(*hash_table, GetDefaultPool()));
    const uint64_t invalid_state = 2;
    std::memcpy(bytes->data() + bytes->size() - 32, &invalid_state, sizeof(invalid_state));
    ASSERT_NOK_WITH_MSG(HashTable::MountForRead(bytes), "special bucket");

    ASSERT_OK_AND_ASSIGN(bytes, FlushHashTable(*hash_table, GetDefaultPool()));
    std::shared_ptr<Bytes> truncated =
        AllocateBytesKeepingPoolAlive(bytes->size() - 1, GetDefaultPool());
    std::memcpy(truncated->data(), bytes->data(), truncated->size());
    ASSERT_NOK_WITH_MSG(HashTable::MountForRead(truncated), "length");
}

TEST(DenseHashTableTest, TestWriteErrors) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HashTable> hash_table,
                         HashTable::Create(1, /*occupancy_pct=*/65, GetDefaultPool()));
    ASSERT_NOK(hash_table->WriteTo(nullptr));
    MockOutputStream short_output;
    ASSERT_NOK_WITH_MSG(hash_table->WriteTo(&short_output), "short write");
    FailingOutputStream failed_output;
    ASSERT_NOK_WITH_MSG(hash_table->WriteTo(&failed_output),
                        "injected dense hash table write failure");
}

}  // namespace paimon::test
