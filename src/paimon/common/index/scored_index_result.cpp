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

#include "paimon/index/scored_index_result.h"

#include <utility>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/macros.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

template <typename Bitmap>
Result<std::shared_ptr<ScoredIndexResult<Bitmap>>> ScoredIndexResult<Bitmap>::Create(
    Bitmap row_positions, std::vector<float> scores, bool is_exact) {
    for (auto iter = row_positions.Begin(); iter != row_positions.End(); ++iter) {
        if (*iter < 0) {
            return Status::Invalid("Index result returned a negative row position");
        }
    }
    if (static_cast<size_t>(row_positions.Cardinality()) != scores.size()) {
        return Status::Invalid(fmt::format("Vector search returned {} row positions but {} scores",
                                           row_positions.Cardinality(), scores.size()));
    }
    return std::shared_ptr<ScoredIndexResult<Bitmap>>(
        new ScoredIndexResult<Bitmap>(std::move(row_positions), std::move(scores), is_exact));
}

template <typename Bitmap>
ScoredIndexResult<Bitmap>::ScoredIndexResult(Bitmap row_positions, std::vector<float> scores,
                                             bool is_exact)
    : IndexResult<Bitmap>(is_exact),
      row_positions_(std::move(row_positions)),
      scores_(std::move(scores)) {}

template <typename Bitmap>
Result<std::shared_ptr<IndexResult<Bitmap>>> ScoredIndexResult<Bitmap>::And(
    const std::shared_ptr<IndexResult<Bitmap>>& other) {
    if (std::dynamic_pointer_cast<RemainIndexResult<Bitmap>>(other)) {
        if (!this->IsExact()) {
            return this->shared_from_this();
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredIndexResult<Bitmap>> result,
                               Create(Bitmap(row_positions_), std::vector<float>(scores_),
                                      /*is_exact=*/false));
        return result;
    } else if (std::dynamic_pointer_cast<SkipIndexResult<Bitmap>>(other)) {
        return IndexResult<Bitmap>::Skip();
    } else {
        return Status::Invalid("Cannot AND ScoredIndexResult with unsupported result type");
    }
}

template <typename Bitmap>
Result<std::shared_ptr<IndexResult<Bitmap>>> ScoredIndexResult<Bitmap>::Or(
    const std::shared_ptr<IndexResult<Bitmap>>& other) {
    if (std::dynamic_pointer_cast<RemainIndexResult<Bitmap>>(other)) {
        return IndexResult<Bitmap>::Remain();
    } else if (std::dynamic_pointer_cast<SkipIndexResult<Bitmap>>(other)) {
        return this->shared_from_this();
    } else {
        return Status::Invalid("Cannot OR ScoredIndexResult with unsupported result type");
    }
}

template <typename Bitmap>
std::string ScoredIndexResult<Bitmap>::ToString() const {
    std::vector<std::string> formatted_scores;
    formatted_scores.reserve(scores_.size());
    for (float score : scores_) {
        formatted_scores.push_back(fmt::format("{:.2f}", score));
    }
    return fmt::format("row positions: {}, scores: {{{}}}", row_positions_.ToString(),
                       fmt::join(formatted_scores, ","));
}

template class ScoredIndexResult<RoaringBitmap32>;
template class ScoredIndexResult<RoaringBitmap64>;

}  // namespace paimon
