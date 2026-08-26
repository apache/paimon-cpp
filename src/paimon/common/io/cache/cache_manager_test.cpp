/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/common/io/cache/cache_manager.h"

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/cache/cache.h"
#include "paimon/common/io/cache/cache_key.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/memory/memory_segment.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class CacheManagerTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    std::shared_ptr<CacheKey> MakeKey(int64_t position, bool is_index = false) const {
        return CacheKey::ForPosition("test_file", position, 64, is_index);
    }

    MemorySegment MakeSegment(int32_t size, char fill_byte) const {
        auto segment = MemorySegment::AllocateHeapMemory(size, pool_.get());
        std::memset(segment.MutableData(), fill_byte, size);
        return segment;
    }

    std::shared_ptr<LruCache> DataLru(const CacheManager& manager) const {
        return std::dynamic_pointer_cast<LruCache>(manager.DataCache());
    }

    std::shared_ptr<LruCache> IndexLru(const CacheManager& manager) const {
        return std::dynamic_pointer_cast<LruCache>(manager.IndexCache());
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

/// Regression test for the double->int64_t conversions in the CacheManager constructor:
/// (double)INT64_MAX rounds to 2^63, which is not representable as int64_t, so casting the
/// product back is undefined behavior (x86 cvttsd2si yields INT64_MIN, aarch64 fcvtzs
/// saturates to INT64_MAX). The conversion must saturate, keeping the capacity non-negative.
TEST_F(CacheManagerTest, TestCapacitySaturatesAtInt64Max) {
    CacheManager manager(std::numeric_limits<int64_t>::max(), /*high_priority_pool_ratio=*/0.0);

    std::shared_ptr<LruCache> data_lru = DataLru(manager);
    ASSERT_NE(data_lru, nullptr);
    ASSERT_GE(data_lru->GetMaxWeight(), 0);
    ASSERT_EQ(data_lru->GetMaxWeight(), std::numeric_limits<int64_t>::max());

    // A ratio of 0.0 means index and data share the same cache.
    ASSERT_EQ(manager.DataCache(), manager.IndexCache());

    // The saturated capacity accepts entries instead of rejecting every insert.
    std::shared_ptr<CacheKey> key = MakeKey(0);
    auto reader = [&](const std::shared_ptr<CacheKey>&) -> Result<MemorySegment> {
        return MakeSegment(64, 'A');
    };
    ASSERT_OK_AND_ASSIGN(MemorySegment segment, manager.GetPage(key, reader, {}));
    ASSERT_EQ(segment.Size(), 64);
    ASSERT_EQ(segment.Get(0), 'A');
}

/// Verifies the exact capacity split between the data and index caches for a normal
/// configuration, plus a Get/Invalidate smoke path through CacheManager::GetPage.
TEST_F(CacheManagerTest, TestNormalSplitAndSmokePath) {
    CacheManager manager(/*max_memory_bytes=*/1024, /*high_priority_pool_ratio=*/0.5);

    std::shared_ptr<LruCache> data_lru = DataLru(manager);
    std::shared_ptr<LruCache> index_lru = IndexLru(manager);
    ASSERT_NE(data_lru, nullptr);
    ASSERT_NE(index_lru, nullptr);
    ASSERT_EQ(data_lru->GetMaxWeight(), 512);
    ASSERT_EQ(index_lru->GetMaxWeight(), 512);

    std::shared_ptr<CacheKey> key = MakeKey(0);
    int32_t reader_calls = 0;
    auto reader = [&](const std::shared_ptr<CacheKey>&) -> Result<MemorySegment> {
        reader_calls++;
        return MakeSegment(128, 'B');
    };

    // The first GetPage is a miss and invokes the reader; the second is a cache hit.
    ASSERT_OK_AND_ASSIGN(MemorySegment first, manager.GetPage(key, reader, {}));
    ASSERT_EQ(first.Get(0), 'B');
    ASSERT_EQ(reader_calls, 1);
    ASSERT_OK_AND_ASSIGN(MemorySegment second, manager.GetPage(key, reader, {}));
    ASSERT_EQ(second.Get(0), 'B');
    ASSERT_EQ(reader_calls, 1);

    // After InvalidPage the reader is invoked again.
    manager.InvalidPage(key);
    ASSERT_OK_AND_ASSIGN(MemorySegment third, manager.GetPage(key, reader, {}));
    ASSERT_EQ(third.Get(0), 'B');
    ASSERT_EQ(reader_calls, 2);
}

/// Verifies weight-based eviction through GetPage: inserting beyond the data cache capacity
/// evicts the least recently used page and runs its eviction callback.
TEST_F(CacheManagerTest, TestGetPageEviction) {
    // The data cache capacity is 512 * (1.0 - 0.5) = 256 bytes.
    CacheManager manager(/*max_memory_bytes=*/512, /*high_priority_pool_ratio=*/0.5);

    std::vector<int64_t> evicted;
    auto callback_for = [&evicted](int64_t position) -> CacheCallback {
        return
            [&evicted, position](const std::shared_ptr<CacheKey>&) { evicted.push_back(position); };
    };
    auto reader = [&](const std::shared_ptr<CacheKey>&) -> Result<MemorySegment> {
        return MakeSegment(128, 'C');
    };

    std::shared_ptr<CacheKey> key0 = MakeKey(0);
    std::shared_ptr<CacheKey> key1 = MakeKey(1);
    std::shared_ptr<CacheKey> key2 = MakeKey(2);
    ASSERT_OK_AND_ASSIGN(MemorySegment segment0, manager.GetPage(key0, reader, callback_for(0)));
    ASSERT_EQ(segment0.Get(0), 'C');
    ASSERT_OK_AND_ASSIGN(MemorySegment segment1, manager.GetPage(key1, reader, callback_for(1)));
    ASSERT_EQ(segment1.Get(0), 'C');
    ASSERT_TRUE(evicted.empty());

    // 128 + 128 + 128 > 256: inserting key2 evicts key0, the least recently used page.
    ASSERT_OK_AND_ASSIGN(MemorySegment segment2, manager.GetPage(key2, reader, callback_for(2)));
    ASSERT_EQ(segment2.Get(0), 'C');
    ASSERT_EQ(evicted, std::vector<int64_t>({0}));
    ASSERT_EQ(manager.DataCache()->Size(), 2);
}

}  // namespace paimon::test
