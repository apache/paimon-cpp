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

#include "paimon/core/table/source/data_evolution_full_text_read.h"

#include <functional>
#include <future>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/global_index/global_index_utils.h"
#include "paimon/common/global_index/offset_global_index_reader.h"
#include "paimon/common/global_index/scored_global_index_result_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/global_index_live_row_filter.h"
#include "paimon/core/global_index/global_index_scan_impl.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/source/data_evolution_full_text_scan.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/core/utils/tag_manager.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/predicate/full_text_search.h"

namespace paimon {
namespace {
/// The widest span of search row ranges that a split expands into a bitmap to intersect with the
/// live rows: at most 65536 run containers, cheaper than copying the live rows of a large table for
/// every split. A wider span trims the live rows instead.
constexpr int64_t kMaxExpandedSearchRows = static_cast<int64_t>(1) << 32;

/// Loads snapshot `snapshot_id`, or the copy that a tag keeps of it once the snapshot has expired,
/// since a plan of `scan.tag-name` may come from a tag that outlives its snapshot.
Result<Snapshot> LoadPlannedSnapshot(const CoreOptions& options, const std::string& root_path,
                                     int64_t snapshot_id) {
    std::string branch = BranchManager::NormalizeBranch(options.GetBranch());
    SnapshotManager snapshot_manager(options.GetFileSystem(), root_path, branch);
    Result<Snapshot> snapshot = snapshot_manager.LoadSnapshot(snapshot_id);
    if (snapshot.ok()) {
        return snapshot;
    }
    // The snapshot may expire between planning and loading, or even while it is loaded, so a
    // failed load falls back to the tags unless the snapshot is still there.
    PAIMON_ASSIGN_OR_RAISE(bool exists, snapshot_manager.SnapshotExists(snapshot_id));
    if (exists) {
        return snapshot.status();
    }
    TagManager tag_manager(options.GetFileSystem(), root_path, branch);
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> tag_names, tag_manager.ListTagNames());
    for (const std::string& tag_name : tag_names) {
        // A tag deleted after it was listed, even while it is read, is skipped, as it no longer
        // keeps the snapshot; any other failure to read a tag is an error.
        Result<std::optional<Tag>> tag = tag_manager.Get(tag_name);
        if (!tag.ok()) {
            PAIMON_ASSIGN_OR_RAISE(bool tag_exists,
                                   options.GetFileSystem()->Exists(tag_manager.TagPath(tag_name)));
            if (tag_exists) {
                return tag.status();
            }
            continue;
        }
        if (tag.value() && tag.value()->Id() == snapshot_id) {
            return tag.value()->TrimToSnapshot();
        }
    }
    return Status::NotExist(fmt::format(
        "snapshot {} that the full-text search splits were planned from no longer exists",
        snapshot_id));
}

/// Describes `split` for the errors of its search.
std::string Describe(const IndexFullTextSearchSplitImpl& split) {
    std::vector<std::string> file_names;
    file_names.reserve(split.FullTextIndexFiles().size());
    for (const auto& file : split.FullTextIndexFiles()) {
        file_names.push_back(file->FileName());
    }
    return fmt::format(
        "while searching the split of column {} in snapshot {} over rows [{}, {}] with {} index "
        "files [{}]",
        split.ColumnName(), split.SnapshotId(), split.RowRangeStart(), split.RowRangeEnd(),
        split.FullTextIndexFiles().front()->IndexType(), fmt::join(file_names, ", "));
}
}  // namespace

DataEvolutionFullTextRead::DataEvolutionFullTextRead(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const CoreOptions& options, const std::shared_ptr<GlobalIndexFileManager>& index_file_manager,
    const std::shared_ptr<PredicateFilter>& partition_filter, const DataField& text_field,
    const std::string& query, int32_t limit, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool)
    : root_path_(root_path),
      table_schema_(table_schema),
      options_(options),
      index_file_manager_(index_file_manager),
      partition_filter_(partition_filter),
      text_field_(text_field),
      query_(query),
      limit_(limit),
      executor_(executor),
      pool_(pool) {}

Result<std::unique_ptr<DataEvolutionFullTextRead>> DataEvolutionFullTextRead::Create(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<PredicateFilter>& partition_filter, const DataField& text_field,
    const std::string& query, int32_t limit, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool) {
    if (limit <= 0) {
        return Status::Invalid("Limit must be positive, set via SetLimit()");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Executor> final_executor,
                           GlobalIndexScanImpl::GetOrCreateExecutor(executor, options));
    auto index_file_manager = std::make_shared<GlobalIndexFileManager>(
        options.GetFileSystem(), path_factory->CreateGlobalIndexFileFactory(),
        /*checkpoint_path_factory=*/nullptr);
    return std::unique_ptr<DataEvolutionFullTextRead>(new DataEvolutionFullTextRead(
        root_path, table_schema, options, index_file_manager, partition_filter, text_field, query,
        limit, final_executor, pool));
}

std::optional<RoaringBitmap64> DataEvolutionFullTextRead::IncludeRowIds(
    const IndexFullTextSearchSplit& split, const RoaringBitmap64* live_rows) {
    const std::vector<Range>& search_ranges = split.SearchRowRanges();
    if (search_ranges.empty() || (live_rows && live_rows->IsEmpty())) {
        return RoaringBitmap64();
    }
    Range physical_range(split.RowRangeStart(), split.RowRangeEnd());
    if (!live_rows && search_ranges.size() == 1 && search_ranges[0] == physical_range) {
        return std::nullopt;
    }
    Range searched_span(search_ranges.front().from, search_ranges.back().to);
    RoaringBitmap64 include_row_ids;
    if (live_rows && searched_span.Count() > kMaxExpandedSearchRows) {
        include_row_ids = GlobalIndexUtils::RowIdsWithin(*live_rows, searched_span);
        for (size_t i = 1; i < search_ranges.size(); ++i) {
            include_row_ids.RemoveRange(search_ranges[i - 1].to + 1, search_ranges[i].from);
        }
    } else {
        for (const auto& range : search_ranges) {
            include_row_ids |= GlobalIndexUtils::RowIdsOf(range);
        }
        if (live_rows) {
            include_row_ids &= *live_rows;
        }
    }
    if (live_rows && include_row_ids.Cardinality() == physical_range.Count()) {
        return std::nullopt;
    }
    return include_row_ids;
}

Result<std::optional<RoaringBitmap64>> DataEvolutionFullTextRead::LiveRows(
    int64_t snapshot_id) const {
    // Without deletion vectors or row tracking every row is live, so the planned snapshot, which
    // may be gone, is not loaded.
    if (!options_.DeletionVectorsEnabled() || !options_.RowTrackingEnabled()) {
        return std::optional<RoaringBitmap64>();
    }
    PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot,
                           LoadPlannedSnapshot(options_, root_path_, snapshot_id));
    return GlobalIndexLiveRowFilter::LiveRows(root_path_, table_schema_, snapshot,
                                              partition_filter_, options_, executor_, pool_);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> DataEvolutionFullTextRead::SearchSplit(
    const IndexFullTextSearchSplitImpl& split,
    const std::optional<RoaringBitmap64>& include_row_ids) const {
    // The splits of a column may come from different indexes, so each split is read with the
    // indexer and fields of its own files.
    const std::shared_ptr<IndexFileMeta>& first_file = split.FullTextIndexFiles().front();
    const std::string& index_type = first_file->IndexType();
    const GlobalIndexMeta& first_meta = first_file->GetGlobalIndexMeta().value();
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                           GlobalIndexerFactory::Get(index_type, options_.ToMap()));
    if (!indexer) {
        return Status::NotImplemented(
            fmt::format("no global indexer is registered for index type {}", index_type));
    }
    std::vector<DataField> indexed_fields;
    if (first_meta.extra_field_ids) {
        PAIMON_ASSIGN_OR_RAISE(DataField index_field,
                               table_schema_->GetField(first_meta.index_field_id));
        indexed_fields.push_back(std::move(index_field));
        for (int32_t extra_field_id : first_meta.extra_field_ids.value()) {
            PAIMON_ASSIGN_OR_RAISE(DataField extra_field, table_schema_->GetField(extra_field_id));
            indexed_fields.push_back(std::move(extra_field));
        }
    } else {
        indexed_fields.push_back(text_field_);
    }
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(indexed_fields);
    ::ArrowSchema c_arrow_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
    ScopeGuard guard([&c_arrow_schema]() { ArrowSchemaRelease(&c_arrow_schema); });

    std::vector<GlobalIndexIOMeta> index_io_metas;
    index_io_metas.reserve(split.FullTextIndexFiles().size());
    for (const auto& file : split.FullTextIndexFiles()) {
        index_io_metas.emplace_back(index_file_manager_->ToPath(file), file->FileSize(),
                                    file->RowCount(), file->GetGlobalIndexMeta()->index_meta);
    }
    Range physical_range(split.RowRangeStart(), split.RowRangeEnd());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<GlobalIndexReader> index_reader,
        indexer->CreateReader(&c_arrow_schema, index_file_manager_, index_io_metas,
                              physical_range.Count(), /*row_ranges=*/std::nullopt, pool_));
    OffsetGlobalIndexReader reader(std::move(index_reader), physical_range.from, physical_range.to);
    // Each split returns its own top `limit_` rows. The splits rank disjoint rows, so the top
    // `limit_` rows of their union are the global top `limit_` rows, except for which of the rows
    // tied at the `limit_`-th score of a split its index returns (apache/paimon#9955).
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> result,
                           reader.VisitFullTextSearch(std::make_shared<FullTextSearch>(
                               text_field_.Name(), query_, limit_, include_row_ids)));
    if (!result) {
        return Status::NotImplemented(
            fmt::format("global index type {} does not answer full-text searches", index_type));
    }
    return result;
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> DataEvolutionFullTextRead::Read(
    const std::vector<std::shared_ptr<FullTextSearchSplit>>& splits) {
    std::vector<std::shared_ptr<IndexFullTextSearchSplitImpl>> index_splits;
    index_splits.reserve(splits.size());
    std::optional<int64_t> snapshot_id;
    for (const auto& split : splits) {
        auto index_split = std::dynamic_pointer_cast<IndexFullTextSearchSplitImpl>(split);
        if (!index_split) {
            return Status::Invalid("invalid full-text search split, cannot cast to an index split");
        }
        if (index_split->ColumnName() != text_field_.Name()) {
            return Status::Invalid(fmt::format(
                "a split of column {} cannot be read by a full-text search of column {}",
                index_split->ColumnName(), text_field_.Name()));
        }
        // A column of the same name may have replaced the planned one, so match field ids too.
        const GlobalIndexMeta& meta =
            index_split->FullTextIndexFiles().front()->GetGlobalIndexMeta().value();
        if (!DataEvolutionFullTextScan::IndexesField(meta, text_field_.Id())) {
            return Status::Invalid(fmt::format(
                "a split of column {} does not index its field id {}; the column may have been "
                "replaced after the split was planned",
                text_field_.Name(), text_field_.Id()));
        }
        if (snapshot_id && snapshot_id.value() != index_split->SnapshotId()) {
            return Status::Invalid(fmt::format(
                "full-text search splits planned from snapshots {} and {} cannot be read together",
                snapshot_id.value(), index_split->SnapshotId()));
        }
        snapshot_id = index_split->SnapshotId();
        index_splits.push_back(std::move(index_split));
    }
    if (index_splits.empty()) {
        return std::shared_ptr<ScoredGlobalIndexResult>(
            std::make_shared<BitmapScoredGlobalIndexResult>(RoaringBitmap64(),
                                                            std::vector<float>()));
    }

    // Live rows are read on this thread before the searches fan out, so a read of manifests on
    // `executor_` never waits for it from one of its own threads. They are pinned to the snapshot
    // the splits were planned from (apache/paimon#9031).
    PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap64> live_rows, LiveRows(snapshot_id.value()));
    using SearchResult = Result<std::shared_ptr<ScoredGlobalIndexResult>>;
    std::vector<std::function<SearchResult()>> searches;
    searches.reserve(index_splits.size());
    for (const auto& index_split : index_splits) {
        auto include_row_ids = std::make_shared<const std::optional<RoaringBitmap64>>(
            IncludeRowIds(*index_split, live_rows ? &live_rows.value() : nullptr));
        // No row this split ranks is live.
        if (include_row_ids->has_value() && include_row_ids->value().IsEmpty()) {
            continue;
        }
        searches.emplace_back([this, index_split, include_row_ids]() -> SearchResult {
            SearchResult result = SearchSplit(*index_split, *include_row_ids);
            if (!result.ok()) {
                return result.status().WithMessage(
                    fmt::format("{}; {}", result.status().message(), Describe(*index_split)));
            }
            return result;
        });
    }
    std::vector<SearchResult> results;
    if (searches.size() <= 1) {
        for (const auto& search : searches) {
            results.push_back(search());
        }
    } else {
        std::vector<std::future<SearchResult>> futures;
        futures.reserve(searches.size());
        for (const auto& search : searches) {
            futures.push_back(Via(executor_.get(), search));
        }
        results = CollectAll(futures);
    }
    std::vector<std::shared_ptr<ScoredGlobalIndexResult>> scored_results;
    scored_results.reserve(results.size());
    for (auto& result : results) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> scored_result,
                               std::move(result));
        scored_results.push_back(std::move(scored_result));
    }
    return ScoredGlobalIndexResultUtils::MergeTopK(scored_results, limit_);
}

}  // namespace paimon
