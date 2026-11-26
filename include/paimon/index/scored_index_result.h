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

#include <memory>
#include <string>
#include <vector>

#include "paimon/index/index_result.h"

namespace paimon {

/// Scores follow ascending row positions, not ranking order.
/// IsExact() describes predicate matching within the candidate scope,
/// not search recall or global TopK completeness.
template <typename Bitmap>
class PAIMON_EXPORT ScoredIndexResult : public IndexResult<Bitmap> {
 public:
    static Result<std::shared_ptr<ScoredIndexResult<Bitmap>>> Create(Bitmap rows,
                                                                     std::vector<float> scores,
                                                                     bool is_exact = false);

    bool IsEmpty() const {
        return row_positions_.IsEmpty();
    }

    Result<bool> IsRemain() const override {
        return !IsEmpty();
    }

    const Bitmap& GetRowPositions() const {
        return row_positions_;
    }

    const std::vector<float>& GetScores() const {
        return scores_;
    }

    Result<std::shared_ptr<IndexResult<Bitmap>>> And(
        const std::shared_ptr<IndexResult<Bitmap>>& other) override;
    Result<std::shared_ptr<IndexResult<Bitmap>>> Or(
        const std::shared_ptr<IndexResult<Bitmap>>& other) override;
    std::string ToString() const override;

 private:
    ScoredIndexResult(Bitmap rows, std::vector<float> scores, bool is_exact);

    Bitmap row_positions_;
    std::vector<float> scores_;
};

}  // namespace paimon
