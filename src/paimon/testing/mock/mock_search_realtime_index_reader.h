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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/realtime/index/realtime_index_reader.h"
#include "paimon/realtime/offset_range.h"

namespace paimon::test {

/// Configured search results and capabilities for real-time search tests.
struct MockSearchState {
    std::vector<std::pair<int64_t, float>> ranked;
    std::map<std::string, RoaringBitmap64> text_matches;
    bool supports_search = true;
};

/// Real-time search test reader. Applies range and candidate filters before TopK and returns
/// scores aligned with ascending offsets. Ranked results are supplied through MockSearchState.
class MockSearchReader final : public RealtimeIndexReader {
 public:
    MockSearchReader(SearchType search_type, const OffsetRange& range, RoaringBitmap64 offsets,
                     const std::shared_ptr<MockSearchState>& state)
        : search_type_(search_type), range_(range), offsets_(std::move(offsets)), state_(state) {}

    bool SupportsSearch(SearchType type) const override {
        return state_->supports_search &&
               (type == search_type_ || (search_type_ == SearchType::FULL_TEXT_SEARCH &&
                                         type == SearchType::FULL_TEXT_MATCH));
    }

    std::optional<double> EstimateSelectivity(
        const std::shared_ptr<LeafPredicate>&) const override {
        return 0.1;
    }

    Result<std::shared_ptr<ScoredRealtimeIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& search) override {
        return Search(search->limit, search->pre_filter);
    }

    Result<std::shared_ptr<ScoredRealtimeIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& search) override {
        return Search(search->limit, [search](int64_t offset) {
            return !search->include_row_ids || search->include_row_ids->Contains(offset);
        });
    }

    Result<std::shared_ptr<RealtimeIndexResult>> VisitFullTextMatch(
        const std::shared_ptr<FullTextMatch>& match, bool needs_scores) override {
        auto filter = [this, match](int64_t offset) {
            auto found = state_->text_matches.find(match->query);
            return (!match->include_row_ids || match->include_row_ids->Contains(offset)) &&
                   (found == state_->text_matches.end() || found->second.Contains(offset));
        };
        if (!needs_scores) {
            RoaringBitmap64 rows;
            for (const auto& entry : state_->ranked) {
                int64_t offset = entry.first;
                if (offset >= range_.begin && offset < range_.end && offsets_.Contains(offset) &&
                    filter(offset)) {
                    rows.Add(offset);
                }
            }
            return RealtimeIndexResult::Create(std::move(rows));
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredRealtimeIndexResult> result,
                               Search(static_cast<int32_t>(state_->ranked.size()), filter));
        return std::shared_ptr<RealtimeIndexResult>(std::move(result));
    }

 private:
    Result<std::shared_ptr<ScoredRealtimeIndexResult>> Search(
        int32_t limit, const std::function<bool(int64_t)>& filter) const {
        std::map<int64_t, float> selected;
        for (const auto& [offset, score] : state_->ranked) {
            if (selected.size() >= static_cast<size_t>(limit)) {
                break;
            }
            if (offset >= range_.begin && offset < range_.end && offsets_.Contains(offset) &&
                (!filter || filter(offset))) {
                selected.emplace(offset, score);
            }
        }
        RoaringBitmap64 rows;
        std::vector<float> scores;
        for (const auto& [offset, score] : selected) {
            rows.Add(offset);
            scores.push_back(score);
        }
        return ScoredRealtimeIndexResult::Create(std::move(rows), std::move(scores));
    }

    SearchType search_type_;
    OffsetRange range_;
    RoaringBitmap64 offsets_;
    std::shared_ptr<MockSearchState> state_;
};

}  // namespace paimon::test
