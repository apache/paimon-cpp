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

#include "paimon/core/table/source/primary_key_sorted_index_scan.h"
#include "paimon/table/source/split.h"

namespace paimon {
/// Converts an evaluated primary-key sorted-index plan into scan splits addressed by
/// physical data-file row positions.
///
/// Files whose index result is missing or untrustworthy keep a normal single-file scan,
/// files with an empty result are omitted, and files with a valid result become indexed
/// splits carrying their file-local row ranges next to the aligned deletion file. Source
/// splits that are not raw-convertible are preserved unchanged.
class PrimaryKeySortedIndexResult {
 public:
    /// The fragmentation guard of the Java implementation: a file whose index result needs
    /// more ranges falls back to a normal scan.
    static constexpr int32_t kMaxIndexedRangesPerFile = 4096;

    PrimaryKeySortedIndexResult() = delete;
    ~PrimaryKeySortedIndexResult() = delete;

    static Result<std::vector<std::shared_ptr<Split>>> ToSplits(
        const PrimaryKeySortedIndexScan::EvaluatedPlan& evaluated_plan);
};

}  // namespace paimon
