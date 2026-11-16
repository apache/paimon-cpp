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

#include "paimon/file_index/scored_file_index_result.h"

#include <utility>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/status.h"

namespace paimon {

Result<std::shared_ptr<ScoredFileIndexResult>> ScoredFileIndexResult::Create(
    RoaringBitmap32&& row_positions, std::vector<float>&& scores, bool is_exact) {
    if (static_cast<size_t>(row_positions.Cardinality()) != scores.size()) {
        return Status::Invalid(fmt::format("Vector search returned {} row positions but {} scores",
                                           row_positions.Cardinality(), scores.size()));
    }
    return std::shared_ptr<ScoredFileIndexResult>(
        new ScoredFileIndexResult(std::move(row_positions), std::move(scores), is_exact));
}

ScoredFileIndexResult::ScoredFileIndexResult(RoaringBitmap32&& row_positions,
                                             std::vector<float>&& scores, bool is_exact)
    : FileIndexResult(is_exact),
      row_positions_(std::move(row_positions)),
      scores_(std::move(scores)) {}

Result<std::shared_ptr<FileIndexResult>> ScoredFileIndexResult::And(
    const std::shared_ptr<FileIndexResult>& other) {
    if (std::dynamic_pointer_cast<paimon::Remain>(other)) {
        if (!IsExact()) {
            return shared_from_this();
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredFileIndexResult> result,
                               Create(RoaringBitmap32(row_positions_), std::vector<float>(scores_),
                                      /*is_exact=*/false));
        return result;
    } else if (std::dynamic_pointer_cast<paimon::Skip>(other)) {
        return FileIndexResult::Skip();
    } else {
        return Status::Invalid("Cannot AND ScoredFileIndexResult with unsupported result type");
    }
}

Result<std::shared_ptr<FileIndexResult>> ScoredFileIndexResult::Or(
    const std::shared_ptr<FileIndexResult>& other) {
    if (std::dynamic_pointer_cast<paimon::Remain>(other)) {
        return FileIndexResult::Remain();
    } else if (std::dynamic_pointer_cast<paimon::Skip>(other)) {
        return shared_from_this();
    } else {
        return Status::Invalid("Cannot OR ScoredFileIndexResult with unsupported result type");
    }
}

Result<bool> ScoredFileIndexResult::IsRemain() const {
    return !IsEmpty();
}

std::string ScoredFileIndexResult::ToString() const {
    std::vector<std::string> formatted_scores;
    formatted_scores.reserve(scores_.size());
    for (float score : scores_) {
        formatted_scores.push_back(fmt::format("{:.2f}", score));
    }
    return fmt::format("row positions: {}, scores: {{{}}}", row_positions_.ToString(),
                       fmt::join(formatted_scores, ","));
}

}  // namespace paimon
