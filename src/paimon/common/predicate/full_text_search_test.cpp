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

#include "paimon/predicate/full_text_search.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::test {

TEST(FullTextSearchTest, TestOffsetRangeWithoutIncludeRowIds) {
    FullTextSearch search("f0", R"({"match":{"query":"paimon"}})", /*limit=*/5);
    ASSERT_FALSE(search.include_row_ids);

    std::shared_ptr<FullTextSearch> shard_search = search.OffsetRange(100, 199);
    ASSERT_TRUE(shard_search);
    ASSERT_EQ(shard_search->field_name, "f0");
    ASSERT_EQ(shard_search->query, R"({"match":{"query":"paimon"}})");
    ASSERT_EQ(shard_search->limit, 5);
    ASSERT_FALSE(shard_search->include_row_ids);
}

TEST(FullTextSearchTest, TestOffsetRangeWithIncludeRowIds) {
    FullTextSearch search("f0", R"({"match":{"query":"paimon"}})", /*limit=*/5,
                          RoaringBitmap64::From({5l, 99l, 100l, 150l, 199l, 200l}));

    std::shared_ptr<FullTextSearch> shard_search = search.OffsetRange(100, 199);
    ASSERT_EQ(shard_search->field_name, "f0");
    ASSERT_EQ(shard_search->query, R"({"match":{"query":"paimon"}})");
    ASSERT_EQ(shard_search->limit, 5);
    ASSERT_EQ(shard_search->include_row_ids, RoaringBitmap64::From({0l, 50l, 99l}));
    ASSERT_EQ(search.OffsetRange(0, 99)->include_row_ids, RoaringBitmap64::From({5l, 99l}));
    ASSERT_EQ(search.OffsetRange(150, 150)->include_row_ids, RoaringBitmap64::From({0l}));
    ASSERT_EQ(search.OffsetRange(300, 399)->include_row_ids, RoaringBitmap64());
    ASSERT_EQ(search.include_row_ids, RoaringBitmap64::From({5l, 99l, 100l, 150l, 199l, 200l}));

    const int64_t from = int64_t{1} << 40;
    FullTextSearch large_search("f0", R"({"match":{"query":"paimon"}})", /*limit=*/5,
                                RoaringBitmap64::From(std::vector<int64_t>{3, from + 3, from + 7}));
    ASSERT_EQ(large_search.OffsetRange(from, RoaringBitmap64::MAX_VALUE)->include_row_ids,
              RoaringBitmap64::From({3l, 7l}));
}

TEST(FullTextSearchTest, TestOffsetRangeWithIncludeRowIdRun) {
    auto row_ids_of = [](int64_t from, int64_t to) {
        RoaringBitmap64 row_ids;
        row_ids.AddRange(from, to);
        row_ids.Add(to);
        return row_ids;
    };
    FullTextSearch search("f0", R"({"match":{"query":"paimon"}})", /*limit=*/5,
                          row_ids_of(100, 199));
    ASSERT_EQ(search.OffsetRange(150, 299)->include_row_ids, row_ids_of(0, 49));
    ASSERT_EQ(search.OffsetRange(0, 149)->include_row_ids, row_ids_of(100, 149));
    ASSERT_EQ(search.OffsetRange(120, 130)->include_row_ids, row_ids_of(0, 10));
    ASSERT_EQ(search.OffsetRange(200, 299)->include_row_ids, RoaringBitmap64());
    ASSERT_EQ(search.include_row_ids, row_ids_of(100, 199));

    constexpr int64_t kMax = RoaringBitmap64::MAX_VALUE;
    FullTextSearch last_search("f0", R"({"match":{"query":"paimon"}})", /*limit=*/5,
                               row_ids_of(kMax - 9, kMax));
    ASSERT_EQ(last_search.OffsetRange(kMax - 5, kMax)->include_row_ids, row_ids_of(0, 5));
    FullTextSearch max_search("f0", R"({"match":{"query":"paimon"}})", /*limit=*/5,
                              RoaringBitmap64::From({kMax}));
    ASSERT_EQ(max_search.OffsetRange(kMax - 5, kMax)->include_row_ids, RoaringBitmap64::From({5l}));
}

}  // namespace paimon::test
