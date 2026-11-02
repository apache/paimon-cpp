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

#include "paimon/common/file_index/empty/empty_file_index_reader.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(EmptyFileIndexReaderTest, TestSimple) {
    Literal lit0(static_cast<int32_t>(0));
    EmptyFileIndexReader reader;

    ASSERT_FALSE(reader.SupportsSearch(FileIndexReader::SearchType::FULL_TEXT_MATCH));
    ASSERT_FALSE(reader.SupportsSearch(FileIndexReader::SearchType::FULL_TEXT_SEARCH));
    ASSERT_FALSE(reader.SupportsSearch(FileIndexReader::SearchType::VECTOR_SEARCH));

    ASSERT_FALSE(reader.VisitIsNotNull().value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitEqual(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitStartsWith(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitEndsWith(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitContains(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitLike(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitLessThan(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitGreaterOrEqual(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitLessOrEqual(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitGreaterThan(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitIn({lit0}).value()->IsRemain().value());

    ASSERT_TRUE(reader.VisitIsNull().value()->IsRemain().value());
    ASSERT_TRUE(reader.VisitNotEqual(lit0).value()->IsRemain().value());
    ASSERT_TRUE(reader.VisitNotIn({lit0}).value()->IsRemain().value());
}

TEST(EmptyFileIndexReaderTest, TestVectorSearchReturnsNoMatches) {
    EmptyFileIndexReader reader;
    auto search = std::make_shared<VectorSearch>(
        "embedding", /*limit=*/1, std::vector<float>{1.0f}, nullptr, nullptr,
        VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> result,
                         reader.VisitVectorSearch(search));
    EXPECT_TRUE(result->GetRowPositions().IsEmpty());
    EXPECT_TRUE(result->GetScores().empty());
}

TEST(EmptyFileIndexReaderTest, TestFullTextMatchAndScores) {
    EmptyFileIndexReader reader({FileIndexReader::SearchType::FULL_TEXT_MATCH,
                                 FileIndexReader::SearchType::FULL_TEXT_SEARCH});
    ASSERT_TRUE(reader.SupportsSearch(FileIndexReader::SearchType::FULL_TEXT_MATCH));
    ASSERT_TRUE(reader.SupportsSearch(FileIndexReader::SearchType::FULL_TEXT_SEARCH));
    ASSERT_FALSE(reader.SupportsSearch(FileIndexReader::SearchType::VECTOR_SEARCH));
    auto match = std::make_shared<FullTextMatch>("body", "word");
    ASSERT_OK_AND_ASSIGN(auto result, reader.VisitFullTextMatch(match, /*needs_scores=*/true));
    auto scored = std::dynamic_pointer_cast<ScoredFileIndexResult>(result);
    ASSERT_TRUE(scored);
    ASSERT_TRUE(scored->IsEmpty());
    ASSERT_TRUE(scored->GetScores().empty());
    ASSERT_OK_AND_ASSIGN(result, reader.VisitFullTextMatch(match));
    auto bitmap = std::dynamic_pointer_cast<BitmapIndexResult>(result);
    ASSERT_TRUE(bitmap);
    ASSERT_OK_AND_ASSIGN(const RoaringBitmap32* positions, bitmap->GetBitmap());
    ASSERT_TRUE(positions->IsEmpty());
}
}  // namespace paimon::test
