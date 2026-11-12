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

#include "paimon/common/global_index/global_index_utils.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::test {

class GlobalIndexUtilsTest : public ::testing::Test {
 public:
    /// Helper to create a valid ArrowArray with the given number of int32 elements.
    static ArrowArray CreateInt32Array(const std::vector<int32_t>& values) {
        arrow::Int32Builder builder;
        if (!values.empty()) {
            EXPECT_TRUE(builder.AppendValues(values).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder.Finish(&array).ok());
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        return c_array;
    }
};

TEST_F(GlobalIndexUtilsTest, TestUnionResults) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> unavailable,
                         GlobalIndexUtils::UnionResults({}));
    ASSERT_EQ(unavailable, nullptr);
    ASSERT_NOK_WITH_MSG(GlobalIndexUtils::UnionResults({nullptr}), "null global index result");

    const int64_t high_row_id = int64_t{1} << 32;
    auto left = std::make_shared<BitmapGlobalIndexResult>(RoaringBitmap64::From({1, high_row_id}));
    auto right = std::make_shared<BitmapGlobalIndexResult>(RoaringBitmap64::From({2, high_row_id}));
    auto empty = std::make_shared<BitmapGlobalIndexResult>(RoaringBitmap64());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> single,
                         GlobalIndexUtils::UnionResults({left}));
    ASSERT_EQ(single, left);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> merged,
                         GlobalIndexUtils::UnionResults({left, empty, right}));
    auto bitmap_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(merged);
    ASSERT_TRUE(bitmap_result);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, bitmap_result->GetBitmap());
    ASSERT_EQ(*bitmap, RoaringBitmap64::From({1, 2, high_row_id}));
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* left_bitmap, left->GetBitmap());
    ASSERT_EQ(*left_bitmap, RoaringBitmap64::From({1, high_row_id}));
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* right_bitmap, right->GetBitmap());
    ASSERT_EQ(*right_bitmap, RoaringBitmap64::From({2, high_row_id}));
}

TEST_F(GlobalIndexUtilsTest, TestUnionResultsPreservesScoredSemantics) {
    auto left = std::make_shared<BitmapScoredGlobalIndexResult>(RoaringBitmap64::From({1}),
                                                                std::vector<float>{0.5f});
    auto right = std::make_shared<BitmapScoredGlobalIndexResult>(RoaringBitmap64::From({2}),
                                                                 std::vector<float>{1.5f});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> merged,
                         GlobalIndexUtils::UnionResults({left, right}));
    auto scored_result = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(merged);
    ASSERT_TRUE(scored_result);
    ASSERT_EQ(scored_result->GetScores(), (std::vector<float>{0.5f, 1.5f}));
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, scored_result->GetBitmap());
    ASSERT_EQ(*bitmap, RoaringBitmap64::From({1, 2}));

    ASSERT_NOK_WITH_MSG(GlobalIndexUtils::UnionResults({left, right, left}), "same row id");

    auto unscored = std::make_shared<BitmapGlobalIndexResult>(RoaringBitmap64::From({3}));
    ASSERT_OK_AND_ASSIGN(merged, GlobalIndexUtils::UnionResults({left, unscored}));
    auto bitmap_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(merged);
    ASSERT_TRUE(bitmap_result);
    ASSERT_OK_AND_ASSIGN(bitmap, bitmap_result->GetBitmap());
    ASSERT_EQ(*bitmap, RoaringBitmap64::From({1, 3}));
}

TEST_F(GlobalIndexUtilsTest, TestNullArrowArray) {
    std::vector<int64_t> row_ids = {0, 1, 2};
    ASSERT_NOK_WITH_MSG(GlobalIndexUtils::CheckRelativeRowIds(nullptr, row_ids, std::nullopt),
                        "CheckRelativeRowIds failed: null c_arrow_array");
}

TEST_F(GlobalIndexUtilsTest, TestEmptyArrayReturnsOk) {
    auto c_array = CreateInt32Array({});
    ASSERT_OK(GlobalIndexUtils::CheckRelativeRowIds(&c_array, {}, std::nullopt));
    // ArrowArray was not released by CheckRelativeRowIds (early return for length==0),
    // so we release it manually.
    if (!ArrowArrayIsReleased(&c_array)) {
        ArrowArrayRelease(&c_array);
    }
}

TEST_F(GlobalIndexUtilsTest, TestEmptyArrayReturnsInvalid) {
    auto c_array = CreateInt32Array({});
    std::vector<int64_t> row_ids = {0, 1, 2};
    ASSERT_NOK_WITH_MSG(
        GlobalIndexUtils::CheckRelativeRowIds(&c_array, row_ids, std::nullopt),
        "relative_row_ids length 3 mismatch arrow_array length 0 in CheckRelativeRowIds");
}

TEST_F(GlobalIndexUtilsTest, TestMatchingRowIdsAndNoExpectedNextRowId) {
    auto c_array = CreateInt32Array({10, 20, 30});
    std::vector<int64_t> row_ids = {0, 1, 2};
    // expected_next_row_id is nullopt, so the first-element check is skipped.
    ASSERT_OK(GlobalIndexUtils::CheckRelativeRowIds(&c_array, row_ids, std::nullopt));
    // On success, guard.Release() is called and ArrowArray is NOT released by
    // CheckRelativeRowIds, so we must release it manually.
    if (!ArrowArrayIsReleased(&c_array)) {
        ArrowArrayRelease(&c_array);
    }
}

TEST_F(GlobalIndexUtilsTest, TestMatchingRowIdsWithCorrectExpectedNextRowId) {
    auto c_array = CreateInt32Array({10, 20, 30});
    std::vector<int64_t> row_ids = {5, 6, 7};
    // expected_next_row_id matches row_ids[0]
    ASSERT_OK(GlobalIndexUtils::CheckRelativeRowIds(&c_array, row_ids, 5));
    // On success, guard.Release() is called and ArrowArray is NOT released by
    // CheckRelativeRowIds, so we must release it manually.
    if (!ArrowArrayIsReleased(&c_array)) {
        ArrowArrayRelease(&c_array);
    }
}

TEST_F(GlobalIndexUtilsTest, TestMismatchedLength) {
    auto c_array = CreateInt32Array({10, 20, 30});
    std::vector<int64_t> row_ids = {0, 1};
    ASSERT_NOK_WITH_MSG(
        GlobalIndexUtils::CheckRelativeRowIds(&c_array, row_ids, std::nullopt),
        "relative_row_ids length 2 mismatch arrow_array length 3 in CheckRelativeRowIds");
}

TEST_F(GlobalIndexUtilsTest, TestMismatchedExpectedNextRowId) {
    auto c_array = CreateInt32Array({10, 20, 30});
    std::vector<int64_t> row_ids = {5, 6, 7};
    // expected_next_row_id is 100, but row_ids[0] is 5
    ASSERT_NOK_WITH_MSG(
        GlobalIndexUtils::CheckRelativeRowIds(&c_array, row_ids, 100),
        "first relative_row_ids 5 mismatch inner expected_next_row_id 100 in CheckRelativeRowIds");
}

TEST_F(GlobalIndexUtilsTest, TestRowIdsOfAndWithin) {
    const int64_t max_row_id = std::numeric_limits<int64_t>::max();
    ASSERT_EQ(GlobalIndexUtils::RowIdsOf(Range(3, 5)), RoaringBitmap64::From({3, 4, 5}));
    ASSERT_EQ(GlobalIndexUtils::RowIdsOf(Range(max_row_id - 1, max_row_id)),
              RoaringBitmap64::From({max_row_id - 1, max_row_id}));

    RoaringBitmap64 row_ids = RoaringBitmap64::From({0, 3, 7, 10, max_row_id});
    ASSERT_EQ(GlobalIndexUtils::RowIdsWithin(row_ids, Range(3, 7)), RoaringBitmap64::From({3, 7}));
    ASSERT_EQ(GlobalIndexUtils::RowIdsWithin(row_ids, Range(8, max_row_id)),
              RoaringBitmap64::From({10, max_row_id}));
    ASSERT_EQ(GlobalIndexUtils::RowIdsWithin(row_ids, Range(4, 6)), RoaringBitmap64());
    ASSERT_EQ(row_ids, RoaringBitmap64::From({0, 3, 7, 10, max_row_id}));
}

}  // namespace paimon::test
