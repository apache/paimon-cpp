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

    /// Searches `splits` and returns the `limit` highest-scoring rows among the candidates that
    /// they return. The result holds global row ids and iterates them in ascending order; pass it
    /// to `ScanContextBuilder::SetGlobalIndexResult()` to read the rows, whose scores the read
    /// returns in the `_INDEX_SCORE` field.
    ///
    /// The splits are searched in parallel, and each one returns at most `limit` candidates, chosen
    /// by its index. Ties among the candidates are broken by ascending row id, but which of the
    /// rows tied at the `limit`-th score of a split become candidates is up to its index. A split
    /// ranks only its search row ranges, and with deletion vectors enabled only the rows that are
    /// live in the snapshot the splits were planned from; BM25 statistics still come from its whole
    /// index shard.
    /// @param splits Splits planned by a `FullTextScan` of the same column, all from one snapshot.
    /// @return The rows found, or an error if a split is invalid, its index files do not index the
    ///         column of the latest schema (for example, after the column was replaced), or an
    ///         index cannot be read.
    virtual Result<std::shared_ptr<ScoredGlobalIndexResult>> Read(
        const std::vector<std::shared_ptr<FullTextSearchSplit>>& splits) = 0;
};

}  // namespace paimon
