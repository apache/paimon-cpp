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

#include "paimon/common/predicate/full_text_match_predicate.h"

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/predicate/full_text_match_function.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/defs.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<FullTextMatch> MakeMatch(
    const std::string& field_name = "title",
    const std::string& query = R"({"match":{"query":"apple"}})") {
    return std::make_shared<FullTextMatch>(field_name, query);
}

}  // namespace

TEST(FullTextMatchPredicateTest, TestBasicAndComposesWithScalarPredicates) {
    auto match = MakeMatch();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::FullTextMatch(1, match));
    auto text = std::dynamic_pointer_cast<FullTextMatchPredicate>(predicate);
    ASSERT_TRUE(text);
    ASSERT_EQ(text->GetFullTextMatch(), match);
    ASSERT_EQ(text->FieldIndex(), 1);
    ASSERT_EQ(text->FieldName(), "title");
    ASSERT_EQ(text->GetFieldType(), FieldType::STRING);
    ASSERT_EQ(text->GetFunction().GetType(), Function::Type::FULL_TEXT_MATCH);
    ASSERT_TRUE(text->Literals().empty());
    ASSERT_FALSE(std::dynamic_pointer_cast<PredicateFilter>(predicate));
    ASSERT_NE(text->ToString().find(match->query), std::string::npos);

    auto scalar = PredicateBuilder::LessThan(0, "price", FieldType::INT, Literal(10));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> conjunction,
                         PredicateBuilder::And({scalar, predicate}));
    auto compound = std::dynamic_pointer_cast<CompoundPredicate>(conjunction);
    ASSERT_TRUE(compound);
    ASSERT_EQ(compound->Children(), (std::vector<std::shared_ptr<Predicate>>{scalar, predicate}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> disjunction,
                         PredicateBuilder::Or({scalar, predicate}));
    ASSERT_EQ(disjunction->GetFunction().GetType(), Function::Type::OR);
    ASSERT_FALSE(text->Negate());
    ASSERT_NOK_WITH_MSG(PredicateBuilder::Not(predicate), "Could not construct A NOT predicate");
    ASSERT_NOK_WITH_MSG(PredicateBuilder::Not(conjunction), "Could not construct A NOT predicate");
}

TEST(FullTextMatchPredicateTest, TestRebind) {
    auto match = std::make_shared<FullTextMatch>("title", R"({"match":{"query":"apple"}})",
                                                 RoaringBitmap64::From({1, 3, 9}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::FullTextMatch(1, match));
    auto text = std::dynamic_pointer_cast<FullTextMatchPredicate>(predicate);
    ASSERT_TRUE(text);

    auto picked = text->NewLeafPredicate(0);
    ASSERT_EQ(picked->FieldIndex(), 0);
    ASSERT_EQ(picked->GetFullTextMatch(), match);
    ASSERT_EQ(text->FieldIndex(), 1);

    auto renamed = text->NewLeafPredicate("stored_title");
    ASSERT_EQ(renamed->FieldIndex(), 1);
    ASSERT_EQ(renamed->FieldName(), "stored_title");
    ASSERT_EQ(renamed->GetFullTextMatch()->field_name, "stored_title");
    ASSERT_EQ(renamed->GetFullTextMatch()->query, match->query);
    ASSERT_EQ(renamed->GetFullTextMatch()->include_row_ids, match->include_row_ids);
    ASSERT_NE(renamed->GetFullTextMatch(), match);
    ASSERT_EQ(match->field_name, "title");
    ASSERT_EQ(text->FieldName(), "title");
}

TEST(FullTextMatchPredicateTest, TestEquality) {
    auto match = MakeMatch();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::FullTextMatch(1, match));
    auto text = std::dynamic_pointer_cast<FullTextMatchPredicate>(predicate);
    ASSERT_TRUE(text);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> same,
                         PredicateBuilder::FullTextMatch(1, MakeMatch()));
    ASSERT_TRUE(*predicate == *same);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> other_query,
        PredicateBuilder::FullTextMatch(1, MakeMatch("title", R"({"match":{"query":"orange"}})")));
    ASSERT_FALSE(*predicate == *other_query);
    auto restricted = std::make_shared<FullTextMatch>("title", R"({"match":{"query":"apple"}})",
                                                      RoaringBitmap64::From({3, 9}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> candidates,
                         PredicateBuilder::FullTextMatch(1, restricted));
    ASSERT_FALSE(*predicate == *candidates);
    ASSERT_NE(candidates->ToString().find(restricted->include_row_ids->ToString()),
              std::string::npos);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> other_field,
                         PredicateBuilder::FullTextMatch(1, MakeMatch("body")));
    ASSERT_FALSE(*predicate == *other_field);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> other_index,
                         PredicateBuilder::FullTextMatch(0, MakeMatch()));
    ASSERT_FALSE(*predicate == *other_index);
}

TEST(FullTextMatchPredicateTest, TestRejectsInvalidRequests) {
    ASSERT_NOK_WITH_MSG(PredicateBuilder::FullTextMatch(0, nullptr),
                        "FullTextMatch request must not be null");
    ASSERT_NOK_WITH_MSG(PredicateBuilder::FullTextMatch(0, MakeMatch("")),
                        "FullTextMatch field name must not be empty");
    ASSERT_NOK_WITH_MSG(PredicateBuilder::FullTextMatch(0, MakeMatch("title", "")),
                        "FullTextMatch query must not be empty");
}

TEST(FullTextMatchPredicateTest, TestFunctionCannotEvaluateRowsOrStatistics) {
    const auto& function = FullTextMatchFunction::Instance();
    arrow::NullArray array(2);
    ASSERT_NOK_WITH_MSG(
        function.Test(array, {}, arrow::default_memory_pool()),
        "FullTextMatch does not support scalar Test; a full-text index is required");
    ASSERT_NOK_WITH_MSG(
        function.Test(Literal(1), {}),
        "FullTextMatch does not support scalar Test; a full-text index is required");
    ASSERT_NOK_WITH_MSG(
        function.Test(2, Literal(1), Literal(2), /*null_count=*/0, {}),
        "FullTextMatch does not support statistics Test; a full-text index is required");
    ASSERT_FALSE(function.Negate());
}

}  // namespace paimon::test
