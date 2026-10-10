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
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/predicate/predicate.h"
#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/visibility.h"
namespace paimon {
/// A full-text search on a full-text indexed field, aligned with Java
/// `org.apache.paimon.predicate.FullTextSearch`.
struct PAIMON_EXPORT FullTextSearch {
    FullTextSearch(const std::string& _field_name, const std::string& _query, int32_t _limit,
                   std::optional<RoaringBitmap64> _include_row_ids = std::nullopt)
        : field_name(_field_name),
          query(_query),
          limit(_limit),
          include_row_ids(std::move(_include_row_ids)) {}

    /// Returns a copy of this search for an index shard covering the global row ids [`from`, `to`]
    /// (both inclusive), as Java `FullTextSearch#offsetRange` does: `include_row_ids` is clipped to
    /// the range and shifted to shard-local row ids by subtracting `from`.
    std::shared_ptr<FullTextSearch> OffsetRange(int64_t from, int64_t to) const {
        if (!include_row_ids) {
            return std::make_shared<FullTextSearch>(*this);
        }
        const RoaringBitmap64& global_row_ids = include_row_ids.value();
        RoaringBitmap64 local_row_ids;
        for (auto iter = global_row_ids.EqualOrLarger(from), end = global_row_ids.End();
             iter != end && *iter <= to; ++iter) {
            local_row_ids.Add(*iter - from);
        }
        return std::make_shared<FullTextSearch>(field_name, query, limit, std::move(local_row_ids));
    }

    /// Name of the field to search within (must be a full-text indexed field).
    std::string field_name;
    /// The JSON DSL query defined by the `paimon-full-text` engine (`core/src/query.rs` in
    /// apache/paimon-full-text). The root object holds exactly one query:
    ///
    /// - `match`: `query` (alias `terms`), optional `column`, `operator` (`Or` by default, or
    ///   `And`), `boost` (1.0), `fuzziness` (0, a number, or `"auto"` or `null` for automatic),
    ///   `max_expansions` (50) and `prefix_length` (0). The text is analyzed with the analyzer
    ///   used at indexing time.
    /// - `multi_match`: `query`, `columns`, `boosts`, `operator`, `fuzziness`, `max_expansions`
    ///   and `prefix_length`.
    /// - `match_phrase` (alias `phrase`): `query`, optional `column` and `slop` (0).
    /// - `boolean`: `must`, `should` and `must_not` lists of queries, and `queries` as
    ///   `[occur, query]` pairs.
    /// - `boost`: `positive`, `negative` and `negative_boost` (0.5).
    ///
    /// Examples: `{"match":{"query":"paimon lake","operator":"And"}}` and
    /// `{"match_phrase":{"query":"paimon lake","slop":1}}`.
    ///
    /// `field_name` selects the table column. DSL column names refer to fields inside the index:
    /// the `full-text` backend defaults to `text`, while `lucene-fts` uses `field_name`.
    ///
    /// The `full-text` index passes the query to the native engine unchanged. The `lucene-fts`
    /// index translates `match`, `multi_match`, `match_phrase` and `boolean` into Lucene queries
    /// and rejects `boost` queries and a `fuzziness` other than 0. An invalid query is reported as
    /// an error `Status`.
    std::string query;
    /// Maximum number of highest-scoring rows requested from each index shard; must be positive.
    int32_t limit;
    /// The **global row ids** to search within, aligned with Java `includeRowIds`. Only these rows
    /// are ranked, so an empty set matches no rows. If not set, all rows are searched.
    std::optional<RoaringBitmap64> include_row_ids;
};
}  // namespace paimon
