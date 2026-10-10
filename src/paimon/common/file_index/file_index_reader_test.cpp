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
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/literal.h"
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
    std::shared_ptr<FullTextSearch> full_text_search = std::make_shared<FullTextSearch>(
        "body", std::nullopt, "word", FullTextSearch::SearchType::MATCH_ANY, std::nullopt);
    ASSERT_NOK_WITH_MSG(reader.VisitFullTextSearch(full_text_search),
                        "does not support full text search");
}
}  // namespace paimon::test
