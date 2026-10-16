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

#include "paimon/common/utils/bin_packing.h"

#include <map>
#include <string>
#include <utility>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(BinPackingTest, CheckedPackingPreservesOrderAndPropagatesRefusal) {
    const auto weight = [](const int32_t& value) -> int64_t { return value; };
    const std::vector<std::vector<int32_t>> expected = {{2, 3}, {7}, {1}};
    int32_t checks = 0;
    auto result = BinPacking::PackForOrdered<int32_t>({2, 3, 7, 1}, weight, 5, [&checks]() {
        ++checks;
        return Status::OK();
    });
    ASSERT_TRUE(result.ok());
    auto packed = std::move(result).value();
    ASSERT_EQ(packed, expected);
    ASSERT_EQ(checks, 10);
    ASSERT_EQ(BinPacking::PackForOrdered<int32_t>({2, 3, 7, 1}, weight, 5), expected);
    for (int32_t stop = 1; stop <= checks; ++stop) {
        SCOPED_TRACE(stop);
        int32_t visits = 0;
        auto result = BinPacking::PackForOrdered<int32_t>({2, 3, 7, 1}, weight, 5, [&]() {
            return ++visits == stop ? Status::Cancelled("packing cancelled") : Status::OK();
        });
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().IsCancelled());
        ASSERT_EQ(visits, stop);
    }
    auto empty = BinPacking::PackForOrdered<int32_t>(
        {}, weight, 5, []() { return Status::Cancelled("empty packing cancelled"); });
    ASSERT_FALSE(empty.ok());
    ASSERT_TRUE(empty.status().IsCancelled());
}

TEST(BinPackingTest, TestExactlyPack) {
    std::map<std::string, int64_t> items_map = {{"a", 1000ll}, {"b", 20ll}, {"c", 200ll}};
    auto weight_func = [&](const std::string& item) -> int64_t { return items_map[item]; };
    std::vector<std::string> items = {"a", "b", "c"};
    std::vector<std::vector<std::string>> expect_packed = {{"a"}, {"b", "c"}};
    auto packed = BinPacking::PackForOrdered<std::string>(std::move(items), weight_func,
                                                          /*target_weight=*/800);
    ASSERT_EQ(packed, expect_packed);
}

}  // namespace paimon::test
