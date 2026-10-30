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
#include <memory>
#include <vector>

#include "paimon/global_index/global_index_result.h"
#include "paimon/result.h"

namespace paimon {
/// Helpers that combine `ScoredGlobalIndexResult`s.
class ScoredGlobalIndexResultUtils {
 public:
    ScoredGlobalIndexResultUtils() = delete;
    ~ScoredGlobalIndexResultUtils() = delete;

    /// Merges `results` and keeps the `limit` rows that rank first: a higher score first, then a
    /// lower row id, with scores ordered as -infinity < -0.0 < +0.0 < +infinity < NaN. A row id in
    /// more than one result keeps its score from the first of those results. This is Java
    /// `ScoredGlobalIndexResult#merge` followed by `ScoredGlobalIndexResult#topK`.
    /// @param results The results to merge; none may be null.
    /// @param limit   The number of rows to keep; must be positive.
    static Result<std::shared_ptr<ScoredGlobalIndexResult>> MergeTopK(
        const std::vector<std::shared_ptr<ScoredGlobalIndexResult>>& results, int32_t limit);
};

}  // namespace paimon
