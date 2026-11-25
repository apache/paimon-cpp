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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "paimon/global_index/global_index_result.h"
#include "paimon/visibility.h"

namespace paimon {
/// Fuses scored results in the same global row-id space.
class PAIMON_EXPORT HybridSearchRanker {
 public:
    HybridSearchRanker() = delete;
    ~HybridSearchRanker() = delete;

    enum class Type { RRF, WEIGHTED_SCORE, MRR };

    struct WeightedResult {
        std::shared_ptr<ScoredGlobalIndexResult> result;
        float weight = 1.0f;
    };

    /// Parses rrf (the default for a blank name), weighted_score or mrr, ignoring case and
    /// surrounding whitespace.
    static Result<Type> Parse(const std::string& name);

    /// Keeps the top limit rows by fused score, breaking ties by ascending row id. RRF sums
    /// weight / (60 + rank), MRR sums weight / rank, with ranks starting at one. Weighted
    /// score sums each route's min-max normalized scores times its weight. All hits of a
    /// constant-score route receive its full weight. Result iteration is in row-id order.
    static Result<std::shared_ptr<ScoredGlobalIndexResult>> Rank(
        Type ranker, const std::vector<WeightedResult>& routes, int32_t limit);
};
}  // namespace paimon
