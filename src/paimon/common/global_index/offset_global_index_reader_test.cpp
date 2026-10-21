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

#include "paimon/common/global_index/offset_global_index_reader.h"

#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/mock/mock_global_index_reader.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::test {
class OffsetGlobalIndexReaderTest : public ::testing::Test {
 public:
    void CheckResult(const std::shared_ptr<GlobalIndexResult>& result,
                     const std::vector<int64_t>& expected) const {
        ASSERT_TRUE(result);
        auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected).ToString();
    }

    static void CheckScoredResult(const std::shared_ptr<ScoredGlobalIndexResult>& result,
                                  const std::vector<int64_t>& expected_row_ids,
                                  const std::vector<float>& expected_scores) {
        ASSERT_TRUE(result);
        auto typed_result = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected_row_ids))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected_row_ids).ToString();
        ASSERT_EQ(typed_result->GetScores(), expected_scores);
    }
};

TEST_F(OffsetGlobalIndexReaderTest, TestVisitEqualWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 1, 3});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 100, /*to=*/199);

    Literal literal_5(5);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitEqual(literal_5));
    // row ids {0, 1, 3} + offset 100 -> {100, 101, 103}
    CheckResult(result, {100, 101, 103});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitIsNotNullWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 2, 4, 6});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 50, /*to=*/149);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitIsNotNull());
    // row ids {0, 2, 4, 6} + offset 50 -> {50, 52, 54, 56}
    CheckResult(result, {50, 52, 54, 56});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitIsNullWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({1, 3});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 200, /*to=*/299);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitIsNull());
    // row ids {1, 3} + offset 200 -> {201, 203}
    CheckResult(result, {201, 203});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitLessThanWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 1});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 10, /*to=*/109);

    Literal literal_5(5);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitLessThan(literal_5));
    // row ids {0, 1} + offset 10 -> {10, 11}
    CheckResult(result, {10, 11});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitGreaterOrEqualWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({3, 4, 5});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 1000, /*to=*/1099);

    Literal literal_10(10);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitGreaterOrEqual(literal_10));
    // row ids {3, 4, 5} + offset 1000 -> {1003, 1004, 1005}
    CheckResult(result, {1003, 1004, 1005});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitInWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 2});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 5, /*to=*/104);

    std::vector<Literal> literals = {Literal(1), Literal(3)};
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitIn(literals));
    // row ids {0, 2} + offset 5 -> {5, 7}
    CheckResult(result, {5, 7});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitNotInWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({1, 3, 5});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 20, /*to=*/119);

    std::vector<Literal> literals = {Literal(2)};
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitNotIn(literals));
    // row ids {1, 3, 5} + offset 20 -> {21, 23, 25}
    CheckResult(result, {21, 23, 25});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitNotEqualWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 2, 4});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 7, /*to=*/106);

    Literal literal_1(1);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitNotEqual(literal_1));
    // row ids {0, 2, 4} + offset 7 -> {7, 9, 11}
    CheckResult(result, {7, 9, 11});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitLessOrEqualWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 1, 2});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 30, /*to=*/129);

    Literal literal_3(3);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitLessOrEqual(literal_3));
    // row ids {0, 1, 2} + offset 30 -> {30, 31, 32}
    CheckResult(result, {30, 31, 32});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitGreaterThanWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({4, 5});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 15, /*to=*/114);

    Literal literal_3(3);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitGreaterThan(literal_3));
    // row ids {4, 5} + offset 15 -> {19, 20}
    CheckResult(result, {19, 20});
}

TEST_F(OffsetGlobalIndexReaderTest, TestZeroOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 1, 2});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 0, /*to=*/99);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitIsNotNull());
    // row ids {0, 1, 2} + offset 0 -> {0, 1, 2}
    CheckResult(result, {0, 1, 2});
}

TEST_F(OffsetGlobalIndexReaderTest, TestEmptyResultWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 100, /*to=*/199);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitIsNotNull());
    // empty result + offset 100 -> still empty
    CheckResult(result, {});
}

TEST_F(OffsetGlobalIndexReaderTest, TestIsThreadSafeDelegated) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 100, /*to=*/199);
    // MockGlobalIndexReader returns true for IsThreadSafe
    ASSERT_TRUE(offset_reader->IsThreadSafe());
}

TEST_F(OffsetGlobalIndexReaderTest, TestGetIndexTypeDelegated) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 100, /*to=*/199);
    ASSERT_EQ(offset_reader->GetIndexType(), "fake");
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitFullTextSearchWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetScoredResult({0, 3, 5}, {0.5f, 0.9f, 0.1f});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 10, /*to=*/109);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitFullTextSearch(nullptr));
    // row ids {0, 3, 5} + offset 10 -> {10, 13, 15}
    CheckScoredResult(result, {10, 13, 15}, {0.5f, 0.9f, 0.1f});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitFullTextSearchConvertsIncludeRowIds) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 10, /*to=*/19);
    {
        auto search =
            std::make_shared<FullTextSearch>("f0", R"({"match":{"query":"paimon"}})", /*limit=*/7,
                                             RoaringBitmap64::From({5l, 10l, 13l, 19l, 20l, 150l}));
        ASSERT_OK(offset_reader->VisitFullTextSearch(search));
        auto captured = fake_reader->CapturedFullTextSearch();
        ASSERT_TRUE(captured);
        ASSERT_EQ(captured->field_name, "f0");
        ASSERT_EQ(captured->query, R"({"match":{"query":"paimon"}})");
        ASSERT_EQ(captured->limit, 7);
        ASSERT_EQ(captured->include_row_ids, RoaringBitmap64::From({0l, 3l, 9l}));
        ASSERT_EQ(search->include_row_ids, RoaringBitmap64::From({5l, 10l, 13l, 19l, 20l, 150l}));
    }
    {
        auto search = std::make_shared<FullTextSearch>(
            "f0", R"({"match":{"query":"paimon"}})", /*limit=*/7, RoaringBitmap64::From({5l, 20l}));
        ASSERT_OK(offset_reader->VisitFullTextSearch(search));
        ASSERT_TRUE(fake_reader->CapturedFullTextSearch()->include_row_ids);
        ASSERT_TRUE(fake_reader->CapturedFullTextSearch()->include_row_ids->IsEmpty());
    }
    {
        auto search =
            std::make_shared<FullTextSearch>("f0", R"({"match":{"query":"paimon"}})", /*limit=*/7);
        ASSERT_OK(offset_reader->VisitFullTextSearch(search));
        ASSERT_EQ(fake_reader->CapturedFullTextSearch(), search);
    }
    {
        constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
        auto last_reader = std::make_shared<MockGlobalIndexReader>();
        auto last_offset_reader =
            std::make_shared<OffsetGlobalIndexReader>(last_reader, kMax - 9, /*to=*/kMax);
        auto search =
            std::make_shared<FullTextSearch>("f0", R"({"match":{"query":"paimon"}})", /*limit=*/7,
                                             RoaringBitmap64::From({kMax - 5, kMax - 2, kMax}));
        ASSERT_OK(last_offset_reader->VisitFullTextSearch(search));
        ASSERT_EQ(last_reader->CapturedFullTextSearch()->include_row_ids,
                  RoaringBitmap64::From({4l, 7l, 9l}));
    }
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitVectorSearchWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetScoredResult({0, 2, 5}, {0.9f, 0.7f, 0.3f});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 100, /*to=*/199);

    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitVectorSearch(nullptr));
    // row ids {0, 2, 5} + offset 100 -> {100, 102, 105}, scores unchanged
    CheckScoredResult(result, {100, 102, 105}, {0.9f, 0.7f, 0.3f});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitVectorSearchNotSupported) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 10, /*to=*/109);
    // Propagate the wrapped reader's unsupported-search error.
    fake_reader->SetReturnError("vector search not supported");
    ASSERT_NOK_WITH_MSG(offset_reader->VisitVectorSearch(nullptr), "vector search not supported");
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitStartsWithWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 1, 4});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 50, /*to=*/149);

    Literal prefix(FieldType::STRING, "abc", 3);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitStartsWith(prefix));
    // row ids {0, 1, 4} + offset 50 -> {50, 51, 54}
    CheckResult(result, {50, 51, 54});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitEndsWithWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({2, 3});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 25, /*to=*/124);

    Literal suffix(FieldType::STRING, "xyz", 3);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitEndsWith(suffix));
    // row ids {2, 3} + offset 25 -> {27, 28}
    CheckResult(result, {27, 28});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitContainsWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({1, 5, 6});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 300, /*to=*/399);

    Literal literal(FieldType::STRING, "foo", 3);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitContains(literal));
    // row ids {1, 5, 6} + offset 300 -> {301, 305, 306}
    CheckResult(result, {301, 305, 306});
}

TEST_F(OffsetGlobalIndexReaderTest, TestVisitLikeWithOffset) {
    auto fake_reader = std::make_shared<MockGlobalIndexReader>();
    fake_reader->SetDefaultResult({0, 3, 7});

    auto offset_reader = std::make_shared<OffsetGlobalIndexReader>(fake_reader, 40, /*to=*/139);

    Literal pattern(FieldType::STRING, "ab%", 3);
    ASSERT_OK_AND_ASSIGN(auto result, offset_reader->VisitLike(pattern));
    // row ids {0, 3, 7} + offset 40 -> {40, 43, 47}
    CheckResult(result, {40, 43, 47});
}

}  // namespace paimon::test
