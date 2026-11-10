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
#include <utility>
#include <vector>

#include "paimon/realtime/index/realtime_index_result.h"
#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/visibility.h"

namespace paimon {

/// Search results keyed by real-time offset. Scores follow ascending offset order, not rank.
class PAIMON_EXPORT ScoredRealtimeIndexResult : public RealtimeIndexResult {
 public:
    static Result<std::shared_ptr<ScoredRealtimeIndexResult>> Create(RoaringBitmap64 rows,
                                                                     std::vector<float> scores) {
        if (rows.Cardinality() != static_cast<int64_t>(scores.size())) {
            return Status::Invalid("Real-time search score count does not match offsets");
        }
        PAIMON_RETURN_NOT_OK(ValidateRowPositions(rows));
        return std::shared_ptr<ScoredRealtimeIndexResult>(
            new ScoredRealtimeIndexResult(std::move(rows), std::move(scores)));
    }

    const std::vector<float>& GetScores() const {
        return scores_;
    }

 private:
    ScoredRealtimeIndexResult(RoaringBitmap64 rows, std::vector<float> scores)
        : RealtimeIndexResult(std::move(rows)), scores_(std::move(scores)) {}

    std::vector<float> scores_;
};

}  // namespace paimon
