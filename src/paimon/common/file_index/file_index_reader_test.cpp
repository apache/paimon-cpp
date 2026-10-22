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

#include "paimon/file_index/file_index_reader.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class MockFileIndexReader : public FileIndexReader {
    // simulate a file with all 1
    Result<std::shared_ptr<FileIndexResult>> VisitEqual(const Literal& literal) override {
        return literal.GetValue<int32_t>() == 1 ? FileIndexResult::Remain()
                                                : FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitNotEqual(const Literal& literal) override {
        return literal.GetValue<int32_t>() == 1 ? FileIndexResult::Skip()
                                                : FileIndexResult::Remain();
    }
};

TEST(FileIndexReaderTest, TestMockIndexReader) {
    MockFileIndexReader reader;
    Literal lit0(static_cast<int32_t>(0));
    Literal lit1(static_cast<int32_t>(1));

    ASSERT_FALSE(reader.VisitEqual(lit0).value()->IsRemain().value());
    ASSERT_TRUE(reader.VisitEqual(lit1).value()->IsRemain().value());
    ASSERT_TRUE(reader.VisitNotEqual(lit0).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitNotEqual(lit1).value()->IsRemain().value());

    ASSERT_TRUE(reader.VisitIn({lit0, lit1}).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitNotIn({lit0, lit1}).value()->IsRemain().value());
    ASSERT_FALSE(reader.VisitIn({lit0, lit0}).value()->IsRemain().value());
    ASSERT_TRUE(reader.VisitNotIn({lit0, lit0}).value()->IsRemain().value());
}

TEST(FileIndexReaderTest, TestDefaultIndexReader) {
    FileIndexReader reader;
    Literal lit0(static_cast<int32_t>(0));
    Literal lit1(static_cast<int32_t>(1));

    ASSERT_TRUE(reader.VisitEqual(lit0).value()->IsRemain().value());
    ASSERT_TRUE(reader.VisitNotEqual(lit0).value()->IsRemain().value());

    ASSERT_TRUE(reader.VisitIn({lit0, lit1}).value()->IsRemain().value());
    ASSERT_TRUE(reader.VisitNotIn({lit0, lit1}).value()->IsRemain().value());

    std::shared_ptr<VectorSearch> vector_search =
        std::make_shared<VectorSearch>("embedding", 1, std::vector<float>{1.0f}, nullptr, nullptr,
                                       std::nullopt, std::map<std::string, std::string>{});
    ASSERT_NOK_WITH_MSG(reader.VisitVectorSearch(vector_search), "does not support vector search");
    std::shared_ptr<FullTextSearch> full_text_search =
        std::make_shared<FullTextSearch>("body", R"({"match":{"query":"word"}})", /*limit=*/1);
    ASSERT_NOK_WITH_MSG(reader.VisitFullTextSearch(full_text_search),
                        "does not support full text search");
    auto full_text_match = std::make_shared<FullTextMatch>("body", R"({"match":{"query":"word"}})");
    ASSERT_NOK_WITH_MSG(reader.VisitFullTextMatch(full_text_match),
                        "does not support complete full-text matching");
}

TEST(FileIndexReaderTest, TestFullTextMatchVisitorDispatch) {
    class FullTextReader : public FileIndexReader {
     public:
        Result<std::shared_ptr<FileIndexResult>> VisitFullTextMatch(
            const std::shared_ptr<FullTextMatch>& full_text_match) override {
            received_match_ = full_text_match;
            return FileIndexResult::Skip();
        }

        std::shared_ptr<FullTextMatch> received_match_;
    };

    auto match = std::make_shared<FullTextMatch>("body", R"({"match":{"query":"apple"}})");
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::FullTextMatch(0, match));
    auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    ASSERT_TRUE(leaf);

    auto default_reader = std::make_shared<FileIndexReader>();
    auto result =
        PredicateUtils::VisitPredicate<std::shared_ptr<FileIndexResult>>(leaf, default_reader);
    ASSERT_NOK_WITH_MSG(result, "This file index does not support complete full-text matching");
    ASSERT_TRUE(result.status().IsNotImplemented());

    auto reader = std::make_shared<FullTextReader>();
    ASSERT_OK_AND_ASSIGN(
        auto matches,
        PredicateUtils::VisitPredicate<std::shared_ptr<FileIndexResult>>(leaf, reader));
    ASSERT_EQ(reader->received_match_, match);
    ASSERT_OK_AND_ASSIGN(bool is_remain, matches->IsRemain());
    ASSERT_FALSE(is_remain);
}
}  // namespace paimon::test
