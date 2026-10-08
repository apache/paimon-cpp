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

#include "paimon/file_index/scored_file_index_result.h"

#include <vector>

#include "gtest/gtest.h"
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

    std::shared_ptr<FileIndexResult> file_index_result = result;
    ASSERT_OK_AND_ASSIGN(remain, file_index_result->IsRemain());
    EXPECT_TRUE(remain);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> empty,
                         ScoredFileIndexResult::Create(RoaringBitmap32(), {}));
    EXPECT_TRUE(empty->IsEmpty());
    ASSERT_OK_AND_ASSIGN(remain, empty->IsRemain());
    EXPECT_FALSE(remain);
}

TEST(ScoredFileIndexResultTest, TestRejectMismatchedScores) {
    ASSERT_NOK_WITH_MSG(ScoredFileIndexResult::Create(RoaringBitmap32::From({1, 3}), {0.5f}),
                        "2 row positions but 1 scores");
}

}  // namespace paimon::test
