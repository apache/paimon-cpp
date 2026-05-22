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

#include "paimon/common/utils/linked_hash_map.h"

#include <cstdint>
#include <string>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(LinkedHashMapTest, TestCopy) {
    LinkedHashMap<int32_t, std::string> hash_map;
    hash_map.insert(3, "c");
    hash_map.insert(1, "a");
    hash_map.insert(2, "b");
    ASSERT_EQ(hash_map.size(), 3);

    LinkedHashMap<int32_t, std::string> hash_map_2;
    hash_map_2 = hash_map;
    ASSERT_EQ(hash_map_2, hash_map);
    LinkedHashMap<int32_t, std::string> hash_map_3(hash_map);
    ASSERT_EQ(hash_map_3, hash_map);

    auto check = [](const LinkedHashMap<int32_t, std::string>& hash_map) {
        std::vector<std::pair<int32_t, std::string>> expected = {{3, "c"}, {1, "a"}, {2, "b"}};
        std::vector<std::pair<int32_t, std::string>> result;
        for (const auto& iter : hash_map) {
            result.push_back(iter);
        }
        ASSERT_EQ(result, expected);
        ASSERT_EQ(std::unordered_set<int32_t>({3, 1, 2}), hash_map.key_set());
        ASSERT_EQ(std::vector<int32_t>({3, 1, 2}), hash_map.key_vec());
    };
    check(hash_map);
    check(hash_map_2);
    check(hash_map_3);
}

TEST(LinkedHashMapTest, TestInsert) {
    LinkedHashMap<int32_t, std::string> hash_map;
    hash_map.insert(3, "c");
    hash_map.insert(1, "a");
    hash_map.insert(2, "b");
    ASSERT_EQ(hash_map.size(), 3);
    // test key exist
    auto iter = hash_map.find(1);
    ASSERT_TRUE(iter != hash_map.end());
    ASSERT_EQ(iter->first, 1);
    ASSERT_EQ(iter->second, "a");

    // insert the same key (will not modify)
    hash_map.insert(1, "bb");
    iter = hash_map.find(1);
    ASSERT_TRUE(iter != hash_map.end());
    ASSERT_EQ(iter->first, 1);
    ASSERT_EQ(iter->second, "a");

    // test iterator
    std::vector<std::pair<int32_t, std::string>> expected = {{3, "c"}, {1, "a"}, {2, "b"}};
    std::vector<std::pair<int32_t, std::string>> result;
    for (const auto& iter2 : hash_map) {
        result.push_back(iter2);
    }
    ASSERT_EQ(result, expected);
    ASSERT_EQ(std::unordered_set<int32_t>({3, 1, 2}), hash_map.key_set());
    ASSERT_EQ(std::vector<int32_t>({3, 1, 2}), hash_map.key_vec());
}

TEST(LinkedHashMapTest, TestErase) {
    LinkedHashMap<int32_t, std::string> hash_map;
    hash_map.insert(3, "c");
    hash_map.insert(1, "a");
    hash_map.insert(2, "b");
    ASSERT_EQ(hash_map.size(), 3);

    auto iter = hash_map.erase(1);
    ASSERT_TRUE(iter != hash_map.end());
    ASSERT_EQ(iter->first, 2);
    ASSERT_EQ(iter->second, "b");
    ASSERT_EQ(hash_map.size(), 2);

    iter = hash_map.erase(1);
    ASSERT_TRUE(iter == hash_map.end());

    iter = hash_map.erase(2);
    ASSERT_TRUE(iter == hash_map.end());
    ASSERT_EQ(hash_map.size(), 1);
}

TEST(LinkedHashMapTest, TestSimple) {
    LinkedHashMap<int32_t, std::string> hash_map;
    hash_map.insert_or_assign(3, "c");
    hash_map.insert_or_assign(1, "a");
    hash_map.insert_or_assign(2, "b");
    ASSERT_EQ(hash_map.size(), 3);
    // test key exist
    auto iter = hash_map.find(1);
    ASSERT_TRUE(iter != hash_map.end());
    ASSERT_EQ(iter->first, 1);
    ASSERT_EQ(iter->second, "a");

    // test key not exist
    iter = hash_map.find(100);
    ASSERT_TRUE(iter == hash_map.end());

    // test insert the same key with insert_or_assign()
    hash_map.insert_or_assign(1, "aa");
    ASSERT_EQ(hash_map.size(), 3);
    iter = hash_map.find(1);
    ASSERT_TRUE(iter != hash_map.end());
    ASSERT_EQ(iter->first, 1);
    ASSERT_EQ(iter->second, "aa");

    iter = hash_map.find(2);
    ASSERT_TRUE(iter != hash_map.end());
    ASSERT_EQ(iter->first, 2);
    ASSERT_EQ(iter->second, "b");

    hash_map.insert_or_assign(0, "d");
    ASSERT_EQ(hash_map.size(), 4);
    iter = hash_map.find(0);
    ASSERT_TRUE(iter != hash_map.end());
    ASSERT_EQ(iter->first, 0);
    ASSERT_EQ(iter->second, "d");

    // test iterator
    std::vector<std::pair<int32_t, std::string>> expected = {
        {3, "c"}, {1, "aa"}, {2, "b"}, {0, "d"}};
    std::vector<std::pair<int32_t, std::string>> result;
    for (const auto& iter2 : hash_map) {
        result.push_back(iter2);
    }
    ASSERT_EQ(result, expected);
    ASSERT_EQ(std::unordered_set<int32_t>({3, 1, 2, 0}), hash_map.key_set());
    ASSERT_EQ(std::vector<int32_t>({3, 1, 2, 0}), hash_map.key_vec());
}

TEST(LinkedHashMapTest, TestOperator) {
    LinkedHashMap<int32_t, std::string> hash_map;
    hash_map[10] = "abc";
    ASSERT_EQ(hash_map.size(), 1);
    // test key 10 exist
    auto iter = hash_map.find(10);
    ASSERT_TRUE(iter != hash_map.end());
    ASSERT_EQ(iter->first, 10);
    ASSERT_EQ(iter->second, "abc");

    // test key not exist
    iter = hash_map.find(100);
    ASSERT_TRUE(iter == hash_map.end());

    hash_map[20] = "aaa";
    ASSERT_EQ(hash_map.size(), 2);
    // test key 20 exist
    iter = hash_map.find(20);
    ASSERT_TRUE(iter != hash_map.end());
    ASSERT_EQ(iter->first, 20);
    ASSERT_EQ(iter->second, "aaa");

    hash_map[10] = "bbb";
    ASSERT_EQ(hash_map.size(), 2);
    iter = hash_map.find(10);
    ASSERT_TRUE(iter != hash_map.end());
    ASSERT_EQ(iter->first, 10);
    ASSERT_EQ(iter->second, "bbb");

    auto non_exist = hash_map[30];
    ASSERT_EQ(non_exist, "");
    ASSERT_EQ(hash_map.size(), 3);

    ASSERT_EQ(std::unordered_set<int32_t>({10, 20, 30}), hash_map.key_set());
    ASSERT_EQ(std::vector<int32_t>({10, 20, 30}), hash_map.key_vec());
}

TEST(LinkedHashMapTest, TestEqual) {
    LinkedHashMap<int32_t, std::string> hash_map1;
    hash_map1.insert(3, "c");
    hash_map1.insert(1, "a");
    hash_map1.insert(2, "b");
    ASSERT_EQ(hash_map1, hash_map1);

    LinkedHashMap<int32_t, std::string> hash_map2;
    hash_map2[3] = "c";
    hash_map2[1] = "a";
    hash_map2[2] = "b";
    ASSERT_EQ(hash_map1, hash_map2);

    LinkedHashMap<int32_t, std::string> hash_map3;
    hash_map3[1] = "a";
    hash_map3[2] = "b";
    hash_map3[3] = "c";
    ASSERT_NE(hash_map1, hash_map3);

    LinkedHashMap<int32_t, std::string> hash_map4;
    hash_map4[3] = "c";
    hash_map4[1] = "a";
    hash_map4[2] = "b";
    hash_map4[4] = "d";
    ASSERT_NE(hash_map1, hash_map4);
}

}  // namespace paimon::test
