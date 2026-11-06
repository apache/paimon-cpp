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
#include <string>
#include <vector>

#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/table/source/full_text_scan.h"
#include "paimon/utils/range.h"

namespace paimon {
/// `FullTextScan` of a data-evolution table, aligned with Java `DataEvolutionFullTextScan`: one
/// split per index shard of the searched column, planned from the global index files of one
/// snapshot.
class DataEvolutionFullTextScan : public FullTextScan {
 public:
    /// The index shard chosen to rank some rows of the searched column.
    struct IndexRangeSelection {
        /// The physical row range of the shard.
        Range file_range;
        /// The identity of the index of the shard; see `IndexIdentity()`.
        std::string identity;
        /// The files of the shard, in index manifest order.
        std::vector<std::shared_ptr<IndexFileMeta>> files;
        /// The sorted, disjoint and non-adjacent row ranges the shard ranks.
        std::vector<Range> search_ranges;
    };

    /// @param root_path        Root directory of the table.
    /// @param table_schema     Schema of the table.
    /// @param options          Options of the table, merged with those of the search.
    /// @param path_factory     Path factory of the table.
    /// @param partition_filter Predicate over the partition fields, or null for all partitions.
    /// @param text_field       The searched column.
    /// @param pool             Memory pool for reading index manifests.
    DataEvolutionFullTextScan(const std::string& root_path,
                              const std::shared_ptr<TableSchema>& table_schema,
                              const CoreOptions& options,
                              const std::shared_ptr<FileStorePathFactory>& path_factory,
                              const std::shared_ptr<PredicateFilter>& partition_filter,
                              const DataField& text_field, const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<Plan>> CreatePlan() override;

    /// Returns whether `meta` indexes the field `field_id`, as its indexed field or as one of its
    /// extra fields.
    static bool IndexesField(const GlobalIndexMeta& meta, int32_t field_id);

    /// Returns the identity of the index that `file` belongs to: its index type, indexed field id
    /// and extra field ids, as `type|index_field_id|[extra_field_ids]`. Files of one identity
    /// differ only by shard, while files of different identities are never read together.
    static std::string IndexIdentity(const IndexFileMeta& file);

    /// Chooses one index shard for every row that a shard of the field `field_id` covers, as Java
    /// `DataEvolutionFullTextScan#chooseIndexRanges` does (apache/paimon#8547). The shards are
    /// grouped by physical range and identity, the coverage is split at every shard boundary, and
    /// each piece goes to the first of the shards covering it: a shard whose indexed field is
    /// `field_id` before one that carries it as an extra field, then by identity, start and end.
    /// Adjacent pieces of a shard are merged.
    /// @param index_files Global index files; those that do not index `field_id` are ignored.
    /// @param field_id    The id of the searched field.
    /// @return The chosen shards, ordered by start, end and identity.
    static std::vector<IndexRangeSelection> ChooseIndexRanges(
        const std::vector<std::shared_ptr<IndexFileMeta>>& index_files, int32_t field_id);

    /// Returns whether the index files of `index_type` serve a full-text search. A `full-text` or
    /// `lucene-fts` index without a registered indexer fails instead: Paimon C++ is then built
    /// without its backend, and skipping it would drop its rows from the result.
    /// @param index_type  The index type of the files.
    /// @param indexer     The registered indexer of `index_type`, or null if there is none.
    /// @param table_path  The table of the files, for the error.
    /// @param snapshot_id The snapshot the files are planned from, for the error.
    static Result<bool> ServesFullTextSearch(const std::string& index_type,
                                             const GlobalIndexer* indexer,
                                             const std::string& table_path, int64_t snapshot_id);

 private:
    std::string root_path_;
    std::shared_ptr<TableSchema> table_schema_;
    CoreOptions options_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::shared_ptr<PredicateFilter> partition_filter_;
    DataField text_field_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
