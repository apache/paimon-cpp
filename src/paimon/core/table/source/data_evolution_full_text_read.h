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
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/index_full_text_search_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate.h"
#include "paimon/table/source/full_text_read.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
/// `FullTextRead` of a data-evolution table, aligned with Java `DataEvolutionFullTextRead`.
class DataEvolutionFullTextRead : public FullTextRead {
 public:
    /// @param root_path        Root directory of the table.
    /// @param table_schema     Schema of the table.
    /// @param options          Options of the table, merged with those of the search.
    /// @param path_factory     Path factory of the table.
    /// @param partition_filter Predicate over the partition fields used to read live rows and
    ///                         candidate rows, or null for all partitions.
    /// @param filter           Row filter beyond the partition filter, or null for all rows.
    /// @param text_field       The searched column.
    /// @param query            The JSON query.
    /// @param limit            The number of rows to return; must be positive.
    /// @param executor         Executor to search splits on, or null to create one with
    ///                         `global-index.thread-num` threads.
    /// @param pool             Memory pool for reading indexes, manifests and candidate rows.
    static Result<std::unique_ptr<DataEvolutionFullTextRead>> Create(
        const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<PredicateFilter>& partition_filter,
        const std::shared_ptr<Predicate>& filter, const DataField& text_field,
        const std::string& query, int32_t limit, const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool);

    using FullTextRead::Read;

    Result<std::shared_ptr<ScoredGlobalIndexResult>> Read(
        const std::vector<std::shared_ptr<FullTextSearchSplit>>& splits) override;

    /// Returns the row ids that `split` ranks, as Java `DataEvolutionFullTextRead#includeRowIds`
    /// does: its search row ranges, intersected with `eligible_rows` if not null, which are the
    /// rows that are live and pass the row filter. Returns nullopt if they are every row of the
    /// physical range of the split, so the index needs no filter. A search span wider than 2^32
    /// rows trims `eligible_rows` to the ranges instead of expanding them, so that a wide shard
    /// costs no more than the eligible rows.
    static std::optional<RoaringBitmap64> IncludeRowIds(const IndexFullTextSearchSplit& split,
                                                        const RoaringBitmap64* eligible_rows);

 private:
    DataEvolutionFullTextRead(const std::string& root_path,
                              const std::shared_ptr<TableSchema>& table_schema,
                              const CoreOptions& options,
                              const std::shared_ptr<IndexPathFactory>& index_path_factory,
                              const std::shared_ptr<GlobalIndexFileManager>& index_file_manager,
                              const std::shared_ptr<PredicateFilter>& partition_filter,
                              const std::shared_ptr<Predicate>& filter, const DataField& text_field,
                              const std::string& query, int32_t limit,
                              const std::shared_ptr<Executor>& executor,
                              const std::shared_ptr<MemoryPool>& pool);

    /// Returns the rows live in snapshot `snapshot_id`, or nullopt if every row counts as live.
    Result<std::optional<RoaringBitmap64>> LiveRows(int64_t snapshot_id) const;

    /// Resolves the row filter into the rows of `index_splits` that satisfy it, as Java
    /// `DataEvolutionFullTextRead#matchedRows` does in the `fast` `scalar-index.search-mode`, the
    /// only one supported so far: rows that no scalar index can evaluate are excluded. Candidates
    /// that the indexes cannot decide exactly are verified against the data with
    /// `global-index.filter.refine-from-data`, and excluded without it.
    /// @return The matched rows, all of them live, or nullopt if there is no row filter.
    Result<std::optional<RoaringBitmap64>> MatchedRows(
        const std::vector<std::shared_ptr<IndexFullTextSearchSplitImpl>>& index_splits,
        int64_t snapshot_id, const std::optional<RoaringBitmap64>& live_rows) const;

    /// Searches the index shard of `split` among `include_row_ids`, or all of its rows if not set,
    /// and returns at most `limit_` rows with global row ids.
    Result<std::shared_ptr<ScoredGlobalIndexResult>> SearchSplit(
        const IndexFullTextSearchSplitImpl& split,
        const std::optional<RoaringBitmap64>& include_row_ids) const;

    std::string root_path_;
    std::shared_ptr<TableSchema> table_schema_;
    CoreOptions options_;
    std::shared_ptr<IndexPathFactory> index_path_factory_;
    std::shared_ptr<GlobalIndexFileManager> index_file_manager_;
    std::shared_ptr<PredicateFilter> partition_filter_;
    std::shared_ptr<Predicate> filter_;
    DataField text_field_;
    std::string query_;
    int32_t limit_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
