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

#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "paimon/global_index/global_index_result.h"
#include "paimon/result.h"

namespace paimon {
/// Helpers that combine `ScoredGlobalIndexResult`s.
class ScoredGlobalIndexResultUtils {
 public:
    ScoredGlobalIndexResultUtils() = delete;
    ~ScoredGlobalIndexResultUtils() = delete;

    /// Compares hits by descending score in Java float order, then ascending row id.
    static bool RanksBefore(const std::pair<int64_t, float>& left,
                            const std::pair<int64_t, float>& right);

    /// Keeps up to `limit` of the best rows, with scores aligned to ascending row ids. Row ids must
    /// be unique.
    static std::shared_ptr<ScoredGlobalIndexResult> FromRows(
        std::vector<std::pair<int64_t, float>> rows,
        size_t limit = std::numeric_limits<size_t>::max());

    /// Selects the best `limit` rows, keeping the first score for each row id.
    /// Requires non-null results and a positive limit.
    static Result<std::shared_ptr<ScoredGlobalIndexResult>> MergeTopK(
        const std::vector<std::shared_ptr<ScoredGlobalIndexResult>>& results, int32_t limit);
};

}  // namespace paimon
