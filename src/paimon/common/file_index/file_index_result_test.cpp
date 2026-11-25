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

#include "paimon/file_index/file_index_result.h"

#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(FileIndexResultTest, TestSimple) {
    auto remain = FileIndexResult::Remain();
    auto skip = FileIndexResult::Skip();

    ASSERT_TRUE(remain->And(remain).value()->IsRemain().value());
    ASSERT_FALSE(remain->And(skip).value()->IsRemain().value());
    ASSERT_FALSE(skip->And(remain).value()->IsRemain().value());
    ASSERT_FALSE(skip->And(skip).value()->IsRemain().value());

    ASSERT_TRUE(remain->Or(remain).value()->IsRemain().value());
    ASSERT_TRUE(remain->Or(skip).value()->IsRemain().value());
    ASSERT_TRUE(skip->Or(remain).value()->IsRemain().value());
    ASSERT_FALSE(skip->Or(skip).value()->IsRemain().value());

    ASSERT_EQ(remain->ToString(), "REMAIN");
    ASSERT_EQ(skip->ToString(), "SKIP");

    ASSERT_FALSE(remain->And(remain).value()->IsExact());
    ASSERT_TRUE(remain->And(skip).value()->IsExact());
    ASSERT_TRUE(skip->And(remain).value()->IsExact());
    ASSERT_TRUE(skip->And(skip).value()->IsExact());
    ASSERT_FALSE(remain->Or(remain).value()->IsExact());
    ASSERT_FALSE(remain->Or(skip).value()->IsExact());
    ASSERT_FALSE(skip->Or(remain).value()->IsExact());
    ASSERT_TRUE(skip->Or(skip).value()->IsExact());
}

TEST(FileIndexResultTest, TestRowResultsWithSentinels) {
    auto remain = FileIndexResult::Remain();
    auto skip = FileIndexResult::Skip();
    for (bool empty : {false, true}) {
        const RoaringBitmap32 rows = empty ? RoaringBitmap32() : RoaringBitmap32::From({2, 5});
        const std::vector<float> scores =
            empty ? std::vector<float>() : std::vector<float>({0.25f, 0.75f});
        for (bool is_exact : {false, true}) {
            auto bitmap = std::make_shared<BitmapIndexResult>(
                [rows]() -> Result<RoaringBitmap32> { return rows; }, is_exact);
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> scored,
                                 ScoredFileIndexResult::Create(
                                     RoaringBitmap32(rows), std::vector<float>(scores), is_exact));
            for (const auto& source :
                 std::vector<std::shared_ptr<FileIndexResult>>{bitmap, scored}) {
                ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> left, source->And(remain));
                ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> right, remain->And(source));
                for (const auto& result : {left, right}) {
                    ASSERT_FALSE(result->IsExact());
                    if (is_exact) {
                        ASSERT_NE(result, source);
                    }
                    if (source == bitmap) {
                        auto typed = std::dynamic_pointer_cast<BitmapIndexResult>(result);
                        ASSERT_TRUE(typed);
                        ASSERT_OK_AND_ASSIGN(const RoaringBitmap32* actual, typed->GetBitmap());
                        ASSERT_EQ(*actual, rows);
                    } else {
                        auto typed = std::dynamic_pointer_cast<ScoredFileIndexResult>(result);
                        ASSERT_TRUE(typed);
                        ASSERT_EQ(typed->GetRowPositions(), rows);
                        ASSERT_EQ(typed->GetScores(), scores);
                    }
                }
                ASSERT_OK_AND_ASSIGN(left, source->Or(remain));
                ASSERT_OK_AND_ASSIGN(right, remain->Or(source));
                ASSERT_EQ(left, remain);
                ASSERT_EQ(right, remain);
                ASSERT_FALSE(left->IsExact());
                ASSERT_FALSE(right->IsExact());

                ASSERT_OK_AND_ASSIGN(left, source->And(skip));
                ASSERT_OK_AND_ASSIGN(right, skip->And(source));
                ASSERT_EQ(left, skip);
                ASSERT_EQ(right, skip);
                ASSERT_TRUE(left->IsExact());
                ASSERT_TRUE(right->IsExact());

                ASSERT_OK_AND_ASSIGN(left, source->Or(skip));
                ASSERT_OK_AND_ASSIGN(right, skip->Or(source));
                ASSERT_EQ(left, source);
                ASSERT_EQ(right, source);
                ASSERT_EQ(left->IsExact(), is_exact);
                ASSERT_EQ(right->IsExact(), is_exact);
                ASSERT_EQ(source->IsExact(), is_exact);
            }
        }
    }
}
}  // namespace paimon::test
