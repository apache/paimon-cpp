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

#include "paimon/common/utils/hash_map.h"

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <random>

#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

using Map = HashMap<uint64_t, int64_t>;

}  // namespace

TEST(HashMapTest, TestGrowthUpdatesAndIteration) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Map> map, Map::Create(1, GetDefaultPool()));
    ASSERT_EQ(map->Size(), 0);
    ASSERT_FALSE(map->Find(0));
    const uint64_t initial_memory = map->MemoryUsage();
    std::map<uint64_t, int64_t> expected;
    for (uint64_t key = 0; key < 300; ++key) {
        ASSERT_OK(map->FindAndInsert(key, key + 10));
        expected[key] = key + 10;
    }
    ASSERT_GT(map->MemoryUsage(), initial_memory);
    const uint64_t memory_after_growth = map->MemoryUsage();
    for (uint64_t key = 0; key < 300; ++key) {
        const int64_t value = -static_cast<int64_t>(key) - 1;
        ASSERT_OK(map->FindAndInsert(key, value));
        expected[key] = value;
        ASSERT_EQ(map->Find(key), value);
    }
    ASSERT_EQ(map->Size(), expected.size());
    ASSERT_EQ(map->MemoryUsage(), memory_after_growth);
    ASSERT_FALSE(map->Find(300));

    std::map<uint64_t, int64_t> visited;
    map->ForEach(
        [&](uint64_t key, int64_t value) { ASSERT_TRUE(visited.emplace(key, value).second); });
    ASSERT_EQ(visited, expected);
}

TEST(HashMapTest, TestBucketCollisionsAndKeyBitPatterns) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Map> map, Map::Create(1, GetDefaultPool()));
    // Distinct hashes with identical low bits start in the same bucket in these blocks.
    for (uint64_t key = 0; key < 200; ++key) {
        ASSERT_OK(map->FindAndInsert(key << 32, key * 2));
    }
    const uint64_t max_key = std::numeric_limits<uint64_t>::max();
    ASSERT_OK(map->FindAndInsert(max_key, std::numeric_limits<int64_t>::min()));
    for (uint64_t key = 0; key < 200; ++key) {
        ASSERT_EQ(map->Find(key << 32), static_cast<int64_t>(key * 2));
    }
    ASSERT_EQ(map->Find(max_key), std::numeric_limits<int64_t>::min());
    ASSERT_OK(map->FindAndInsert(0, std::numeric_limits<int64_t>::max()));
    ASSERT_EQ(map->Find(0), std::numeric_limits<int64_t>::max());
    ASSERT_EQ(map->Size(), 201);
    ASSERT_FALSE(map->Find(uint64_t{201} << 32));
}

TEST(HashMapTest, TestDeterministicUpdatesMatchReference) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Map> map, Map::Create(4, GetDefaultPool()));
    std::map<uint64_t, int64_t> expected;
    std::mt19937_64 random(42);
    for (int32_t iteration = 0; iteration < 5000; ++iteration) {
        const uint64_t key = random() % 1000;
        const int64_t value = iteration - 2500;
        ASSERT_OK(map->FindAndInsert(key, value));
        expected[key] = value;
    }
    ASSERT_EQ(map->Size(), expected.size());
    for (const auto& [key, value] : expected) {
        ASSERT_EQ(map->Find(key), value);
    }
}

TEST(HashMapTest, TestClearAndPoolLifetime) {
    std::shared_ptr<MemoryPool> pool = GetMemoryPool();
    std::weak_ptr<MemoryPool> weak_pool = pool;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Map> map, Map::Create(2, pool));
    for (uint64_t key = 0; key < 100; ++key) {
        ASSERT_OK(map->FindAndInsert(key, key));
    }
    ASSERT_EQ(map->MemoryUsage(), pool->CurrentUsage());
    map->Clear();
    ASSERT_EQ(map->Size(), 0);
    ASSERT_EQ(map->MemoryUsage(), 0);
    ASSERT_EQ(pool->CurrentUsage(), 0);
    ASSERT_FALSE(map->Find(1));
    uint64_t visited = 0;
    map->ForEach([&](uint64_t, int64_t) { ++visited; });
    ASSERT_EQ(visited, 0);
    ASSERT_OK(map->FindAndInsert(7, 9));
    ASSERT_EQ(map->Find(7), 9);
    pool.reset();
    ASSERT_FALSE(weak_pool.expired());
    map.reset();
    ASSERT_TRUE(weak_pool.expired());
}

TEST(HashMapTest, TestInvalidArguments) {
    ASSERT_NOK_WITH_MSG(Map::Create(1, nullptr), "requires a pool");
    ASSERT_NOK_WITH_MSG(Map::Create(0, GetDefaultPool()), "positive capacity");
    ASSERT_NOK(Map::Create(std::numeric_limits<uint64_t>::max(), GetDefaultPool()));
    ASSERT_NOK(Map::Create(std::numeric_limits<int64_t>::max() / 2, GetDefaultPool()));
}

}  // namespace paimon::test
