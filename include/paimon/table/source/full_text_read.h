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
#include <vector>

#include "paimon/global_index/global_index_result.h"
#include "paimon/result.h"
#include "paimon/table/source/full_text_scan.h"
#include "paimon/table/source/full_text_search_split.h"
#include "paimon/visibility.h"

namespace paimon {
/// Searches the splits of a table-level full-text search, aligned with Java
/// `org.apache.paimon.table.source.FullTextRead`. Create one with
/// `FullTextSearchBuilder::NewFullTextRead()`.
class PAIMON_EXPORT FullTextRead {
 public:
    virtual ~FullTextRead() = default;

    /// Searches the splits of `plan`; see `Read(splits)`.
    Result<std::shared_ptr<ScoredGlobalIndexResult>> Read(const FullTextScan::Plan& plan) {
        return Read(plan.Splits());
    }

    /// Returns the top `limit` hits across `splits`, in ascending global row id order. Pass the
    /// result to `ScanContextBuilder::SetGlobalIndexResult()` to read the rows with `_INDEX_SCORE`.
    ///
    /// Row filters apply before ranking. Each stored shard scores against its whole corpus.
    /// The temporary index scores against all live, non-null text in the raw ranges.
    /// Each backend resolves ties at its cutoff. The merge breaks ties by ascending row id.
    ///
    /// Planned splits can be read independently. When raw and stored splits overlap, stored
    /// shards exclude the raw ranges before selecting their top hits.
    ///
    /// On stored shards, `full` and `detail` scalar modes read filter columns wherever scalar
    /// indexes lack coverage or cannot answer the predicate.
    /// Inexact candidates rank only after verification with `global-index.filter.refine-from-data`.
    /// Workers bind row-filter fields to the planning schema when the splits carry its ID.
    /// @param splits Splits planned for the same snapshot, schema, column and filters.
    /// @return The rows found, or an error if a split is invalid, its column was replaced, or an
    ///         index or data file cannot be read.
    virtual Result<std::shared_ptr<ScoredGlobalIndexResult>> Read(
        const std::vector<std::shared_ptr<FullTextSearchSplit>>& splits) = 0;
};

}  // namespace paimon
