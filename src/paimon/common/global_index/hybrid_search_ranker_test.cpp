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

#include "paimon/global_index/hybrid_search_ranker.h"

#include <limits>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/global_index/scored_global_index_result_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
using Type = HybridSearchRanker::Type;
using Rows = std::vector<std::pair<int64_t, float>>;
using Route = HybridSearchRanker::WeightedResult;

Result<Rows> Rank(Type type, const std::vector<Route>& routes, int32_t limit) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> result,
                           HybridSearchRanker::Rank(type, routes, limit));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScoredGlobalIndexResult::ScoredIterator> iter,
                           result->CreateScoredIterator());
    Rows rows;
    while (iter->HasNext()) {
        rows.push_back(iter->NextWithScore());
    }
    return rows;
}

TEST(HybridSearchRankerTest, ReciprocalRanks) {
    auto first = ScoredGlobalIndexResultUtils::FromRows(
        {{2, 10}, {1, 10}, {3, std::numeric_limits<float>::quiet_NaN()}});
    auto second = ScoredGlobalIndexResultUtils::FromRows({{1, -0.0f}, {2, 0.0f}});
    std::vector<Route> routes = {{first}, {second, 0.5f}};
    ASSERT_OK_AND_ASSIGN(Rows rrf, Rank(Type::RRF, routes, 2));
    ASSERT_EQ(rrf, Rows({{1, 1.0f / 62 + 0.5f / 62}, {2, 1.0f / 63 + 0.5f / 61}}));
    ASSERT_OK_AND_ASSIGN(Rows mrr, Rank(Type::MRR, routes, 2));
    ASSERT_EQ(mrr, Rows({{2, 1.0f / 3 + 0.5f}, {3, 1}}));
}

TEST(HybridSearchRankerTest, WeightedNormalization) {
    auto vector = ScoredGlobalIndexResultUtils::FromRows({{1, -1}, {2, 0}, {3, 1}});
    auto text = ScoredGlobalIndexResultUtils::FromRows({{1, 250}, {2, 250}});
    auto single = ScoredGlobalIndexResultUtils::FromRows({{3, -200}});
    ASSERT_OK_AND_ASSIGN(Rows rows,
                         Rank(Type::WEIGHTED_SCORE, {{vector, 4}, {text, 5}, {single, 2}}, 3));
    ASSERT_EQ(rows, Rows({{1, 5}, {2, 7}, {3, 6}}));
    ASSERT_OK_AND_ASSIGN(rows, Rank(Type::WEIGHTED_SCORE, {{vector}}, 3));
    ASSERT_EQ(rows, Rows({{1, 0}, {2, 0.5f}, {3, 1}}));
    const float max_score = std::numeric_limits<float>::max();
    auto wide = ScoredGlobalIndexResultUtils::FromRows({{1, max_score}, {2, 0}, {3, -max_score}});
    ASSERT_OK_AND_ASSIGN(rows, Rank(Type::WEIGHTED_SCORE, {{vector, 4}, {wide, 2}}, 2));
    ASSERT_EQ(rows, Rows({{2, 3}, {3, 4}}));
    auto with_nan = ScoredGlobalIndexResultUtils::FromRows(
        {{1, -1}, {2, std::numeric_limits<float>::quiet_NaN()}, {3, 1}});
    ASSERT_OK_AND_ASSIGN(rows, Rank(Type::WEIGHTED_SCORE, {{with_nan, 2}}, 2));
    ASSERT_EQ(rows, Rows({{1, 2}, {2, 2}}));
}

TEST(HybridSearchRankerTest, FinalTiesAndEmptyResults) {
    constexpr int64_t kHighRowId = (int64_t{1} << 32) + 3;
    auto a = ScoredGlobalIndexResultUtils::FromRows({{kHighRowId + 1, -3}});
    auto b = ScoredGlobalIndexResultUtils::FromRows({{3, 20}});
    auto c = ScoredGlobalIndexResultUtils::FromRows({{kHighRowId, 30}});
    auto empty = ScoredGlobalIndexResultUtils::FromRows({});
    for (Type type : {Type::RRF, Type::MRR, Type::WEIGHTED_SCORE}) {
        const float score = type == Type::RRF ? 1.0f / 61 : 1.0f;
        ASSERT_OK_AND_ASSIGN(Rows rows, Rank(type, {{a}, {empty, 9}, {b}, {c}}, 2));
        ASSERT_EQ(rows, Rows({{3, score}, {kHighRowId, score}}));
        ASSERT_OK_AND_ASSIGN(rows, Rank(type, {{a}}, 0));
        ASSERT_TRUE(rows.empty());
        ASSERT_OK_AND_ASSIGN(rows, Rank(type, {{empty}}, 1));
        ASSERT_TRUE(rows.empty());
    }
}

TEST(HybridSearchRankerTest, NamesAndWeights) {
    for (const auto& [name, type] :
         std::vector<std::pair<std::string, Type>>{{" \t", Type::RRF},
                                                   {" rRF ", Type::RRF},
                                                   {"WeIGHTed_Score", Type::WEIGHTED_SCORE},
                                                   {" MRR\n", Type::MRR}}) {
        ASSERT_OK_AND_ASSIGN(Type actual, HybridSearchRanker::Parse(name));
        ASSERT_EQ(actual, type);
    }
    ASSERT_NOK_WITH_MSG(HybridSearchRanker::Parse("sum"), "Unsupported hybrid ranker");
    auto result = ScoredGlobalIndexResultUtils::FromRows({{1, 5}});
    for (float weight : {0.0f, -1.0f, std::numeric_limits<float>::infinity(),
                         std::numeric_limits<float>::quiet_NaN()}) {
        ASSERT_NOK_WITH_MSG(Rank(Type::RRF, {{result, weight}}, 1), "finite positive weight");
    }
    ASSERT_NOK(Rank(Type::RRF, {{nullptr, 1}}, 1));
}
}  // namespace
}  // namespace paimon::test
