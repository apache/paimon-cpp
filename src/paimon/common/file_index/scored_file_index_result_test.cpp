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

#include <cstdint>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(ScoredFileIndexResultTest, TestCreate) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ScoredFileIndexResult> result,
        ScoredFileIndexResult::Create(RoaringBitmap32::From({2, 5}), {0.25f, 0.75f}));
    EXPECT_FALSE(result->IsEmpty());
    EXPECT_EQ(RoaringBitmap32::From({2, 5}), result->GetRowPositions());
    EXPECT_EQ(std::vector<float>({0.25f, 0.75f}), result->GetScores());
    ASSERT_OK_AND_ASSIGN(bool remain, result->IsRemain());
    EXPECT_TRUE(remain);
    EXPECT_EQ("row positions: {2,5}, scores: {0.25,0.75}", result->ToString());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> empty,
                         ScoredFileIndexResult::Create(RoaringBitmap32(), {}));
    EXPECT_TRUE(empty->IsEmpty());
    ASSERT_OK_AND_ASSIGN(remain, empty->IsRemain());
    EXPECT_FALSE(remain);
}

TEST(ScoredFileIndexResultTest, TestRejectMismatchedScores) {
    ASSERT_NOK_WITH_MSG(ScoredFileIndexResult::Create(RoaringBitmap32::From({1, 3}), {0.5f}),
                        "2 row positions but 1 scores");
    ASSERT_NOK_WITH_MSG(
        ScoredFileIndexResult::Create(RoaringBitmap32::From({1, 3}), {0.5f}, /*is_exact=*/true),
        "2 row positions but 1 scores");
}

TEST(ScoredFileIndexResultTest, TestExactnessDoesNotMutateScoresOrSources) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ScoredFileIndexResult> candidate,
        ScoredFileIndexResult::Create(RoaringBitmap32::From({2, 5}), {0.25f, 0.75f}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ScoredFileIndexResult> exact,
        ScoredFileIndexResult::Create(RoaringBitmap32::From({2, 5}), {0.25f, 0.75f},
                                      /*is_exact=*/true));
    ASSERT_TRUE(exact->IsExact());
    ASSERT_FALSE(candidate->IsExact());
    ASSERT_EQ(exact->GetRowPositions(), candidate->GetRowPositions());
    ASSERT_EQ(exact->GetScores(), candidate->GetScores());

    ASSERT_OK_AND_ASSIGN(auto left, exact->And(FileIndexResult::Remain()));
    ASSERT_OK_AND_ASSIGN(auto right, FileIndexResult::Remain()->And(exact));
    for (const auto& result : {left, right}) {
        ASSERT_FALSE(result->IsExact());
        auto scored = std::dynamic_pointer_cast<ScoredFileIndexResult>(result);
        ASSERT_TRUE(scored);
        ASSERT_EQ(scored->GetRowPositions(), exact->GetRowPositions());
        ASSERT_EQ(scored->GetScores(), exact->GetScores());
    }
    ASSERT_OK_AND_ASSIGN(left, exact->And(FileIndexResult::Skip()));
    ASSERT_TRUE(left->IsExact());
    ASSERT_OK_AND_ASSIGN(bool remain, left->IsRemain());
    ASSERT_FALSE(remain);
    ASSERT_OK_AND_ASSIGN(right, exact->Or(FileIndexResult::Skip()));
    ASSERT_TRUE(right->IsExact());
    ASSERT_TRUE(exact->IsExact());
    ASSERT_FALSE(candidate->IsExact());
}

TEST(ScoredFileIndexResultTest, TestRejectRowResultCombinations) {
    std::vector<std::shared_ptr<FileIndexResult>> results;
    for (bool empty : {false, true}) {
        for (bool is_exact : {false, true}) {
            RoaringBitmap32 rows = empty ? RoaringBitmap32() : RoaringBitmap32::From({2, 5});
            results.push_back(std::make_shared<BitmapIndexResult>(
                [rows]() -> Result<RoaringBitmap32> { return rows; }, is_exact));
            std::vector<float> scores =
                empty ? std::vector<float>() : std::vector<float>({0.25f, 0.75f});
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<ScoredFileIndexResult> scored,
                ScoredFileIndexResult::Create(std::move(rows), std::move(scores), is_exact));
            results.push_back(scored);
        }
    }
    for (const auto& left : results) {
        for (const auto& right : results) {
            if (std::dynamic_pointer_cast<BitmapIndexResult>(left) &&
                std::dynamic_pointer_cast<BitmapIndexResult>(right)) {
                continue;
            }
            ASSERT_NOK_WITH_MSG(left->And(right), "Cannot AND");
            ASSERT_NOK_WITH_MSG(left->Or(right), "Cannot OR");
        }
    }
}

TEST(ScoredFileIndexResultTest, TestRejectCombinationsWithoutLoadingBitmap) {
    int32_t evaluations = 0;
    auto bitmap = std::make_shared<BitmapIndexResult>(
        [&evaluations]() -> Result<RoaringBitmap32> {
            ++evaluations;
            return Status::IOError("bitmap must not be loaded");
        },
        /*is_exact=*/true);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> scored,
                         ScoredFileIndexResult::Create(RoaringBitmap32(), {}, /*is_exact=*/true));
    ASSERT_NOK_WITH_MSG(bitmap->And(scored), "Cannot AND");
    ASSERT_NOK_WITH_MSG(scored->And(bitmap), "Cannot AND");
    ASSERT_NOK_WITH_MSG(bitmap->Or(scored), "Cannot OR");
    ASSERT_NOK_WITH_MSG(scored->Or(bitmap), "Cannot OR");
    ASSERT_EQ(evaluations, 0);
    ASSERT_TRUE(bitmap->IsExact());
    ASSERT_TRUE(scored->IsExact());
}

}  // namespace paimon::test
