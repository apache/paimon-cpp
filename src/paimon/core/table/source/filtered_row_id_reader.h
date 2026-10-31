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
#include <set>
#include <string>

#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
/// Evaluates a row filter on the data of the given row ids and returns those that satisfy it,
/// aligned with Java `FilteredRowIdReader`. Only the filter columns and `_ROW_ID` are read, so this
/// is the exact counterpart of a scalar global index lookup: a search can hand the result to its
/// index as include row ids without touching the index corpus.
class FilteredRowIdReader {
 public:
    /// @param root_path        Root directory of the table.
    /// @param table_schema     Schema that the search resolved, which the data is read with.
    /// @param options          Options of the table, merged with those of the search.
    /// @param partition_filter Predicate over the partition fields, or null for all partitions.
    /// @param filter           The row filter; must not be null.
    /// @param snapshot_id      The snapshot to read. Once it has expired, a `scan.tag-name` in
    ///                         `options` is read instead, as a tag may outlive its snapshot, and
    ///                         must still refer to it.
    /// @param executor         Executor to read data on.
    /// @param pool             Memory pool for reading data.
    FilteredRowIdReader(const std::string& root_path,
                        const std::shared_ptr<TableSchema>& table_schema,
                        const CoreOptions& options,
                        const std::shared_ptr<PredicateFilter>& partition_filter,
                        const std::shared_ptr<Predicate>& filter, int64_t snapshot_id,
                        const std::shared_ptr<Executor>& executor,
                        const std::shared_ptr<MemoryPool>& pool);

    /// Returns whether an evaluation of `filter` that `contributing_field_ids` decided is an exact
    /// match set. The global index contract only promises candidates: an evaluation drops a
    /// conjunct no index can evaluate, and BTree answers `Contains`, `EndsWith` and `Like` with
    /// every non-null row.
    static Result<bool> IsExact(const TableSchema& table_schema,
                                const std::shared_ptr<Predicate>& filter,
                                const std::set<int32_t>& contributing_field_ids);

    /// Returns the subset of `rows` whose data satisfies the filter.
    Result<RoaringBitmap64> MatchingRowIds(const RoaringBitmap64& rows) const;

 private:
    std::string root_path_;
    std::shared_ptr<TableSchema> table_schema_;
    CoreOptions options_;
    std::shared_ptr<PredicateFilter> partition_filter_;
    std::shared_ptr<Predicate> filter_;
    int64_t snapshot_id_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
