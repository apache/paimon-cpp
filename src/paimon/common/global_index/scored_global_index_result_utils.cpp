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

#include "paimon/common/global_index/scored_global_index_result_utils.h"

#include <algorithm>
#include <unordered_set>
#include <utility>

#include "paimon/common/utils/fields_comparator.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

bool ScoredGlobalIndexResultUtils::RanksBefore(const std::pair<int64_t, float>& left,
                                               const std::pair<int64_t, float>& right) {
    int32_t score_order = FieldsComparator::CompareFloatingPoint(left.second, right.second);
    if (score_order != 0) {
        return score_order > 0;
    }
    return left.first < right.first;
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> ScoredGlobalIndexResultUtils::MergeTopK(
    const std::vector<std::shared_ptr<ScoredGlobalIndexResult>>& results, int32_t limit) {
    if (limit <= 0) {
        return Status::Invalid("merging scored results requires a positive limit");
    }
    std::unordered_set<int64_t> merged_row_ids;
    std::vector<std::pair<int64_t, float>> rows;
    for (const auto& result : results) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScoredGlobalIndexResult::ScoredIterator> iter,
                               result->CreateScoredIterator());
        while (iter->HasNext()) {
            std::pair<int64_t, float> row = iter->NextWithScore();
            if (merged_row_ids.insert(row.first).second) {
                rows.push_back(row);
            }
        }
    }
    return FromRows(std::move(rows), limit);
}

std::shared_ptr<ScoredGlobalIndexResult> ScoredGlobalIndexResultUtils::FromRows(
    std::vector<std::pair<int64_t, float>> rows, size_t limit) {
    if (rows.size() > limit) {
        std::nth_element(rows.begin(), rows.begin() + limit, rows.end(), RanksBefore);
        rows.resize(limit);
    }
    std::sort(rows.begin(), rows.end());
    RoaringBitmap64 bitmap;
    std::vector<float> scores;
    scores.reserve(rows.size());
    for (const auto& [row_id, score] : rows) {
        bitmap.Add(row_id);
        scores.push_back(score);
    }
    return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap), std::move(scores));
}

}  // namespace paimon
