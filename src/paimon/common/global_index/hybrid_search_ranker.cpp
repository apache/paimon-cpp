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

#include <algorithm>
#include <cmath>
#include <iterator>
#include <limits>
#include <unordered_map>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/global_index/scored_global_index_result_utils.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon {
namespace {
constexpr float kRrfRankOffset = 60.0f;
}  // namespace

Result<HybridSearchRanker::Type> HybridSearchRanker::Parse(const std::string& name) {
    std::string normalized = StringUtils::ToLowerCase(name);
    StringUtils::Trim(&normalized);
    if (normalized.empty() || normalized == "rrf") {
        return Type::RRF;
    }
    if (normalized == "weighted_score") {
        return Type::WEIGHTED_SCORE;
    }
    if (normalized == "mrr") {
        return Type::MRR;
    }
    return Status::Invalid(fmt::format("Unsupported hybrid ranker: {}", name));
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> HybridSearchRanker::Rank(
    Type ranker, const std::vector<WeightedResult>& routes, int32_t limit) {
    using Row = std::pair<int64_t, float>;
    std::unordered_map<int64_t, float> scores;
    for (const auto& route : routes) {
        if (!route.result || !std::isfinite(route.weight) || route.weight <= 0) {
            return Status::Invalid("A route requires a scored result and a finite positive weight");
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScoredGlobalIndexResult::ScoredIterator> iter,
                               route.result->CreateScoredIterator());
        if (!iter->HasNext()) {
            continue;
        }
        if (ranker == Type::WEIGHTED_SCORE) {
            float min_score = std::numeric_limits<float>::infinity();
            float max_score = -min_score;
            while (iter->HasNext()) {
                const float score = iter->NextWithScore().second;
                // Java's Math.min and Math.max propagate NaN, giving every hit the full weight.
                if (std::isnan(score)) {
                    min_score = max_score = score;
                    break;
                }
                min_score = std::min(min_score, score);
                max_score = std::max(max_score, score);
            }
            const double range = static_cast<double>(max_score) - min_score;
            // Keep Java's float rounding when the score range fits in a float.
            const bool wide_range =
                std::isfinite(range) && range > std::numeric_limits<float>::max();
            PAIMON_ASSIGN_OR_RAISE(iter, route.result->CreateScoredIterator());
            while (iter->HasNext()) {
                const auto [id, score] = iter->NextWithScore();
                float normalized = 1.0f;
                if (wide_range) {
                    normalized =
                        static_cast<float>((static_cast<double>(score) - min_score) / range);
                } else if (range > 0) {
                    normalized = (score - min_score) / static_cast<float>(range);
                }
                scores[id] += route.weight * normalized;
            }
        } else {
            std::vector<Row> rows;
            while (iter->HasNext()) {
                rows.push_back(iter->NextWithScore());
            }
            std::sort(rows.begin(), rows.end(), ScoredGlobalIndexResultUtils::RanksBefore);
            const float offset = ranker == Type::RRF ? kRrfRankOffset : 0.0f;
            for (size_t i = 0; i < rows.size(); ++i) {
                scores[rows[i].first] += route.weight / (offset + static_cast<float>(i) + 1.0f);
            }
        }
    }

    if (limit <= 0) {
        return ScoredGlobalIndexResultUtils::FromRows({});
    }
    if (scores.size() <= static_cast<size_t>(limit)) {
        return ScoredGlobalIndexResultUtils::FromRows({scores.begin(), scores.end()});
    }
    auto remaining = std::next(scores.begin(), limit);
    std::vector<Row> rows(scores.begin(), remaining);
    const auto ranks_before = ScoredGlobalIndexResultUtils::RanksBefore;
    // The weakest kept row is at the head of the heap.
    std::make_heap(rows.begin(), rows.end(), ranks_before);
    for (; remaining != scores.end(); ++remaining) {
        const Row candidate = *remaining;
        if (ranks_before(candidate, rows.front())) {
            std::pop_heap(rows.begin(), rows.end(), ranks_before);
            rows.back() = candidate;
            std::push_heap(rows.begin(), rows.end(), ranks_before);
        }
    }
    return ScoredGlobalIndexResultUtils::FromRows(std::move(rows));
}
}  // namespace paimon
