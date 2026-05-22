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
