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

#include <algorithm>
#include <functional>
#include <future>
#include <map>
#include <optional>
#include <set>
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
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/global_index/data_evolution_global_index_coverage.h"
#include "paimon/core/global_index/global_index_evaluator_impl.h"
#include "paimon/core/global_index/global_index_live_row_filter.h"
#include "paimon/core/global_index/global_index_scan_impl.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/source/data_evolution_full_text_scan.h"
#include "paimon/core/table/source/filtered_row_id_reader.h"
#include "paimon/core/table/source/raw_full_text_read_impl.h"
#include "paimon/core/table/source/raw_full_text_search_split_impl.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/logging.h"
#include "paimon/predicate/full_text_search.h"

namespace paimon {
namespace {
Logger* FullTextReadLogger() {
    static std::unique_ptr<Logger> logger = Logger::GetLogger("DataEvolutionFullTextRead");
    return logger.get();
}

/// The widest span of search row ranges that a split expands into a bitmap to intersect with the
/// live rows: at most 65536 run containers, cheaper than copying the live rows of a large table for
/// every split. A wider span trims the live rows instead.
constexpr int64_t kMaxExpandedSearchRows = static_cast<int64_t>(1) << 32;
constexpr size_t kMaxReportedRanges = 3;

Result<RoaringBitmap64> ToBitmap(const std::shared_ptr<GlobalIndexResult>& result) {
    if (auto bitmap_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result)) {
        PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* bitmap, bitmap_result->GetBitmap());
        return *bitmap;
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iterator,
                           result->CreateIterator());
    RoaringBitmap64 bitmap;
    while (iterator->HasNext()) {
        bitmap.Add(iterator->Next());
    }
    return bitmap;
}

/// Describes `split` for the errors of its search.
std::string Describe(const IndexFullTextSearchSplitImpl& split) {
    std::vector<std::string> file_names;
    file_names.reserve(split.FullTextIndexFiles().size());
    for (const auto& file : split.FullTextIndexFiles()) {
        file_names.push_back(file->FileName());
    }
    const auto& ranges = split.SearchRowRanges();
    std::vector<Range> sample(ranges.begin(),
                              ranges.begin() + std::min(ranges.size(), kMaxReportedRanges));
    return fmt::format(
        "while searching the split of column {} in snapshot {} over rows [{}, {}] with {} index "
        "files [{}], schema {}, {} search ranges, first {}",
        split.ColumnName(), split.SnapshotId(), split.RowRangeStart(), split.RowRangeEnd(),
        split.FullTextIndexFiles().front()->IndexType(), fmt::join(file_names, ", "),
        split.SchemaId() ? fmt::format("{}", *split.SchemaId()) : "unknown", ranges.size(),
        StringUtils::VectorToString(sample));
}

Result<std::vector<Range>> ScalarIndexCoverage(
    const std::vector<std::shared_ptr<IndexFileMeta>>& files, const std::set<int32_t>& field_ids,
    const CoreOptions& options) {
    // A worker may have fewer index backends than the process that planned its splits.
    std::map<std::string, bool> readable_types;
    std::vector<std::shared_ptr<IndexFileMeta>> readable_files;
    for (const auto& file : files) {
        const auto& meta = file->GetGlobalIndexMeta().value();
        if (meta.extra_field_ids && !meta.extra_field_ids->empty()) {
            continue;
        }
        auto [type, inserted] = readable_types.try_emplace(file->IndexType(), false);
        if (inserted) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                                   GlobalIndexerFactory::Get(type->first, options.ToMap()));
            type->second = indexer && !indexer->SupportsFullTextSearch();
        }
        if (type->second) {
            readable_files.push_back(file);
        }
    }
    return DataEvolutionGlobalIndexCoverage::IndexedRanges(readable_files, field_ids);
}

// Preserve raw precedence before top-k for overlapping legacy splits supplied to the same read.
Status ExcludeRawRanges(const RowRangeIndex& raw_rows,
                        std::vector<std::shared_ptr<IndexFullTextSearchSplitImpl>>* index_splits) {
    for (auto& split : *index_splits) {
        if (!raw_rows.Intersects(split->RowRangeStart(), split->RowRangeEnd())) {
            continue;
        }
        std::vector<Range> remaining =
            DataEvolutionGlobalIndexCoverage::ExcludeRanges(split->SearchRowRanges(), raw_rows);
        if (remaining.empty()) {
            split.reset();
        } else if (remaining != split->SearchRowRanges()) {
            PAIMON_ASSIGN_OR_RAISE(
                split, IndexFullTextSearchSplitImpl::Create(
                           split->SnapshotId(), split->ColumnName(), split->RowRangeStart(),
                           split->RowRangeEnd(), split->FullTextIndexFiles(), std::move(remaining),
                           split->ScalarIndexFiles(), split->SchemaId()));
        }
    }
    index_splits->erase(std::remove(index_splits->begin(), index_splits->end(), nullptr),
                        index_splits->end());
    return Status::OK();
}
}  // namespace

DataEvolutionFullTextRead::DataEvolutionFullTextRead(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const CoreOptions& options, const std::shared_ptr<IndexPathFactory>& index_path_factory,
    const std::shared_ptr<GlobalIndexFileManager>& index_file_manager,
    const std::shared_ptr<PredicateFilter>& partition_filter,
    const std::shared_ptr<Predicate>& filter, const DataField& text_field, const std::string& query,
    int32_t limit, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool)
    : root_path_(root_path),
      table_schema_(table_schema),
      options_(options),
      index_path_factory_(index_path_factory),
      index_file_manager_(index_file_manager),
      partition_filter_(partition_filter),
      filter_(filter),
      text_field_(text_field),
      query_(query),
      limit_(limit),
      executor_(executor),
      pool_(pool) {}

Result<std::unique_ptr<DataEvolutionFullTextRead>> DataEvolutionFullTextRead::Create(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<PredicateFilter>& partition_filter,
    const std::shared_ptr<Predicate>& filter, const DataField& text_field, const std::string& query,
    int32_t limit, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool) {
    if (limit <= 0) {
        return Status::Invalid("Limit must be positive, set via SetLimit()");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Executor> final_executor,
                           GlobalIndexScanImpl::GetOrCreateExecutor(executor, options));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexPathFactory> index_path_factory,
                           path_factory->CreateGlobalIndexFileFactory());
    auto index_file_manager = std::make_shared<GlobalIndexFileManager>(
        options.GetFileSystem(), index_path_factory, /*checkpoint_path_factory=*/nullptr);
    return std::unique_ptr<DataEvolutionFullTextRead>(new DataEvolutionFullTextRead(
        root_path, table_schema, options, index_path_factory, index_file_manager, partition_filter,
        filter, text_field, query, limit, final_executor, pool));
}

std::optional<RoaringBitmap64> DataEvolutionFullTextRead::IncludeRowIds(
    const IndexFullTextSearchSplit& split, const RoaringBitmap64* eligible_rows) {
    const std::vector<Range>& search_ranges = split.SearchRowRanges();
    if (search_ranges.empty() || (eligible_rows && eligible_rows->IsEmpty())) {
        return RoaringBitmap64();
    }
    Range physical_range(split.RowRangeStart(), split.RowRangeEnd());
    if (!eligible_rows && search_ranges.size() == 1 && search_ranges[0] == physical_range) {
        return std::nullopt;
    }
    Range searched_span(search_ranges.front().from, search_ranges.back().to);
    RoaringBitmap64 include_row_ids;
    if (eligible_rows && searched_span.Count() > kMaxExpandedSearchRows) {
        include_row_ids = GlobalIndexUtils::RowIdsWithin(*eligible_rows, searched_span);
        for (size_t i = 1; i < search_ranges.size(); ++i) {
            include_row_ids.RemoveRange(search_ranges[i - 1].to + 1, search_ranges[i].from);
        }
    } else {
        for (const auto& range : search_ranges) {
            include_row_ids |= GlobalIndexUtils::RowIdsOf(range);
        }
        if (eligible_rows) {
            include_row_ids &= *eligible_rows;
        }
    }
    if (eligible_rows && include_row_ids.Cardinality() == physical_range.Count()) {
        return std::nullopt;
    }
    return include_row_ids;
}

Result<std::optional<RoaringBitmap64>> DataEvolutionFullTextRead::LiveRows(
    const std::vector<std::shared_ptr<IndexFullTextSearchSplitImpl>>& index_splits,
    int64_t snapshot_id) const {
    if (index_splits.empty() || !options_.DeletionVectorsEnabled() ||
        !options_.RowTrackingEnabled()) {
        return std::optional<RoaringBitmap64>();
    }
    PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot,
                           FilteredRowIdReader::LoadSnapshot(options_, root_path_, snapshot_id));
    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex search_rows,
                           RowRangeIndex::Create(CollectSearchRanges(index_splits)));
    return GlobalIndexLiveRowFilter::LiveRows(root_path_, table_schema_, snapshot,
                                              partition_filter_, std::move(search_rows), options_,
                                              executor_, pool_);
}

Result<std::optional<RoaringBitmap64>> DataEvolutionFullTextRead::MatchedRows(
    const std::vector<std::shared_ptr<IndexFullTextSearchSplitImpl>>& index_splits,
    int64_t snapshot_id, const std::optional<RoaringBitmap64>& live_rows,
    const std::shared_ptr<TableSchema>& read_schema) const {
    if (!filter_ || index_splits.empty() || (live_rows && live_rows->IsEmpty())) {
        return std::optional<RoaringBitmap64>();
    }
    // Keyed by file name: a scalar index file may intersect more than one split.
    std::map<std::string, std::shared_ptr<IndexFileMeta>> files_by_name;
    for (const auto& split : index_splits) {
        for (const auto& file : split->ScalarIndexFiles()) {
            files_by_name.emplace(file->FileName(), file);
        }
    }
    std::vector<std::shared_ptr<IndexFileMeta>> scalar_index_files;
    scalar_index_files.reserve(files_by_name.size());
    for (const auto& [file_name, file] : files_by_name) {
        scalar_index_files.push_back(file);
    }

    const GlobalIndexSearchMode search_mode = options_.GetScalarIndexSearchMode();
    std::vector<Range> search_ranges;
    std::vector<Range> unindexed_ranges;
    bool use_scalar_indexes = !scalar_index_files.empty();
    if (search_mode != GlobalIndexSearchMode::FAST) {
        search_ranges = Range::SortAndMergeOverlap(CollectSearchRanges(index_splits), true);
        PAIMON_ASSIGN_OR_RAISE(std::set<int32_t> filter_field_ids,
                               GlobalIndexEvaluatorImpl::CollectFieldIds(*read_schema, filter_));
        PAIMON_ASSIGN_OR_RAISE(std::vector<Range> indexed_ranges,
                               ScalarIndexCoverage(scalar_index_files, filter_field_ids, options_));
        const std::vector<Range> covered = Range::And(search_ranges, indexed_ranges);
        use_scalar_indexes =
            std::any_of(covered.begin(), covered.end(), [&live_rows](const Range& range) {
                if (!live_rows) {
                    return true;
                }
                auto first = live_rows->EqualOrLarger(range.from);
                return first != live_rows->End() && *first <= range.to;
            });
        if (use_scalar_indexes && covered != search_ranges) {
            PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, FilteredRowIdReader::LoadSnapshot(
                                                          options_, root_path_, snapshot_id));
            auto data_ranges = [&]() {
                return GlobalIndexLiveRowFilter::DataRowRanges(root_path_, read_schema, snapshot,
                                                               partition_filter_, options_,
                                                               executor_, pool_);
            };
            PAIMON_ASSIGN_OR_RAISE(std::vector<Range> gaps,
                                   DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                                       search_mode, snapshot.NextRowId(), covered, data_ranges));
            unindexed_ranges = Range::And(search_ranges, gaps);
        }
    }

    std::optional<GlobalIndexEvaluatorImpl::Evaluation> evaluation;
    if (use_scalar_indexes) {
        std::unique_ptr<GlobalIndexScanImpl> index_scan =
            GlobalIndexScanImpl::CreateForScalarFilters(read_schema, options_, index_path_factory_,
                                                        scalar_index_files, executor_, pool_);
        PAIMON_ASSIGN_OR_RAISE(evaluation, index_scan->ScanWithCoverage(filter_));
    }
    FilteredRowIdReader row_id_reader(root_path_, read_schema, options_, partition_filter_, filter_,
                                      snapshot_id, executor_, pool_);
    auto read_matches = [&](const std::vector<Range>& ranges) -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(RowRangeIndex rows, RowRangeIndex::Create(ranges));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 matches, row_id_reader.MatchingRowIds(rows));
        if (live_rows) {
            matches &= *live_rows;
        }
        return matches;
    };
    if (!evaluation) {
        if (search_mode == GlobalIndexSearchMode::FAST) {
            PAIMON_LOG_WARN(
                FullTextReadLogger(),
                "Full-text search on table %s in snapshot %ld excluded indexed rows because no "
                "scalar global index can evaluate the row filter %s in fast mode. Build a scalar "
                "index on the filtered columns or set %s to full or detail.",
                root_path_.c_str(), snapshot_id, filter_->ToString().c_str(),
                Options::SCALAR_INDEX_SEARCH_MODE);
            return std::optional<RoaringBitmap64>(RoaringBitmap64());
        }
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 matches, read_matches(search_ranges));
        return std::optional<RoaringBitmap64>(std::move(matches));
    }
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 index_rows, ToBitmap(evaluation->result));
    if (live_rows) {
        index_rows &= live_rows.value();
    }
    for (const Range& range : unindexed_ranges) {
        if (range.to == RoaringBitmap64::MAX_VALUE) {
            index_rows.RemoveRange(range.from, range.to);
            index_rows -= RoaringBitmap64::From({range.to});
        } else {
            index_rows.RemoveRange(range.from, range.to + 1);
        }
    }
    PAIMON_ASSIGN_OR_RAISE(
        bool exact,
        FilteredRowIdReader::IsExact(*read_schema, filter_, evaluation->contributing_field_ids));
    if (!exact) {
        RoaringBitmap64 candidates;
        for (const auto& split : index_splits) {
            std::optional<RoaringBitmap64> split_rows = IncludeRowIds(*split, &index_rows);
            if (split_rows) {
                candidates |= split_rows.value();
            } else {
                candidates |=
                    GlobalIndexUtils::RowIdsOf(Range(split->RowRangeStart(), split->RowRangeEnd()));
            }
        }
        if (candidates.IsEmpty()) {
            index_rows = std::move(candidates);
        } else if (!options_.GlobalIndexFilterRefineFromData()) {
            PAIMON_LOG_WARN(FullTextReadLogger(),
                            "The scalar global index can only answer the row filter %s on table "
                            "%s in snapshot %ld with candidates, and %s is false, so those %ld "
                            "rows are excluded from the search; the result may hold fewer rows "
                            "than requested. Set the option to true to verify the candidates "
                            "against the data, or build an index that answers the predicate "
                            "exactly.",
                            filter_->ToString().c_str(), root_path_.c_str(), snapshot_id,
                            Options::GLOBAL_INDEX_FILTER_REFINE_FROM_DATA,
                            candidates.Cardinality());
            index_rows = RoaringBitmap64();
        } else {
            PAIMON_ASSIGN_OR_RAISE(index_rows, row_id_reader.MatchingRowIds(std::move(candidates)));
        }
    }
    if (!unindexed_ranges.empty()) {
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 unindexed_matches, read_matches(unindexed_ranges));
        index_rows |= unindexed_matches;
    }
    return std::optional<RoaringBitmap64>(std::move(index_rows));
}

std::vector<Range> DataEvolutionFullTextRead::CollectSearchRanges(
    const std::vector<std::shared_ptr<IndexFullTextSearchSplitImpl>>& index_splits) {
    std::vector<Range> ranges;
    for (const auto& split : index_splits) {
        const std::vector<Range>& search_ranges = split->SearchRowRanges();
        ranges.insert(ranges.end(), search_ranges.begin(), search_ranges.end());
    }
    return ranges;
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> DataEvolutionFullTextRead::SearchSplit(
    const IndexFullTextSearchSplitImpl& split,
    const std::optional<RoaringBitmap64>& include_row_ids, const TableSchema& read_schema) const {
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
                               read_schema.GetField(first_meta.index_field_id));
        indexed_fields.push_back(std::move(index_field));
        for (int32_t extra_field_id : first_meta.extra_field_ids.value()) {
            PAIMON_ASSIGN_OR_RAISE(DataField extra_field, read_schema.GetField(extra_field_id));
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
    std::string raw_index_type;
    std::vector<Range> raw_ranges;
    std::optional<int64_t> snapshot_id;
    std::optional<int64_t> schema_id;
    auto check_split = [&](const std::string& column_name, int64_t split_snapshot_id,
                           std::optional<int64_t> split_schema_id) -> Status {
        if (column_name != text_field_.Name()) {
            return Status::Invalid(fmt::format(
                "a split of column {} cannot be read by a full-text search of column {}",
                column_name, text_field_.Name()));
        }
        if (snapshot_id && snapshot_id.value() != split_snapshot_id) {
            return Status::Invalid(fmt::format(
                "full-text search splits planned from snapshots {} and {} cannot be read together",
                snapshot_id.value(), split_snapshot_id));
        }
        snapshot_id = split_snapshot_id;
        if (split_schema_id) {
            if (schema_id && schema_id != split_schema_id) {
                return Status::Invalid(
                    fmt::format("full-text search splits planned with schemas {} and {} cannot be "
                                "read together",
                                *schema_id, *split_schema_id));
            }
            schema_id = split_schema_id;
        }
        return Status::OK();
    };
    for (const auto& split : splits) {
        if (auto raw_split = std::dynamic_pointer_cast<RawFullTextSearchSplitImpl>(split)) {
            PAIMON_RETURN_NOT_OK(check_split(raw_split->ColumnName(), raw_split->SnapshotId(),
                                             raw_split->SchemaId()));
            if (raw_split->FieldId() && *raw_split->FieldId() != text_field_.Id()) {
                return Status::Invalid(fmt::format(
                    "a raw split of column {} was planned for field id {}, but the column now has "
                    "field id {}",
                    text_field_.Name(), *raw_split->FieldId(), text_field_.Id()));
            }
            if (!raw_index_type.empty() && raw_index_type != raw_split->IndexType()) {
                return Status::Invalid(
                    fmt::format("cannot combine raw full-text splits with "
                                "index types {} and {}",
                                raw_index_type, raw_split->IndexType()));
            }
            raw_index_type = raw_split->IndexType();
            const auto& ranges = raw_split->RowRanges();
            raw_ranges.insert(raw_ranges.end(), ranges.begin(), ranges.end());
            continue;
        }
        auto index_split = std::dynamic_pointer_cast<IndexFullTextSearchSplitImpl>(split);
        if (!index_split) {
            return Status::Invalid(
                "invalid full-text search split, cannot cast to an index or raw split");
        }
        PAIMON_RETURN_NOT_OK(check_split(index_split->ColumnName(), index_split->SnapshotId(),
                                         index_split->SchemaId()));
        // A column of the same name may have replaced the planned one, so match field ids too.
        const GlobalIndexMeta& meta =
            index_split->FullTextIndexFiles().front()->GetGlobalIndexMeta().value();
        if (!DataEvolutionFullTextScan::IndexesField(meta, text_field_.Id())) {
            return Status::Invalid(fmt::format(
                "a split of column {} does not index its field id {}; the column may have been "
                "replaced after the split was planned",
                text_field_.Name(), text_field_.Id()));
        }
        index_splits.push_back(std::move(index_split));
    }
    if (!snapshot_id) {
        return std::shared_ptr<ScoredGlobalIndexResult>(
            std::make_shared<BitmapScoredGlobalIndexResult>(RoaringBitmap64(),
                                                            std::vector<float>()));
    }

    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex raw_rows, RowRangeIndex::Create(raw_ranges));
    if (!raw_ranges.empty()) {
        PAIMON_RETURN_NOT_OK(ExcludeRawRanges(raw_rows, &index_splits));
    }
    // Predicate names and multi-field index layouts refer to the coordinator's schema.
    std::shared_ptr<TableSchema> read_schema = table_schema_;
    if (schema_id && *schema_id != table_schema_->Id()) {
        SchemaManager schemas(options_.GetFileSystem(), root_path_,
                              BranchManager::NormalizeBranch(options_.GetBranch()));
        PAIMON_ASSIGN_OR_RAISE(read_schema, schemas.ReadSchema(*schema_id));
    }
    if (filter_) {
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
            *DataField::ConvertDataFieldsToArrowSchema(read_schema->Fields()), filter_,
            /*validate_field_idx=*/false));
    }
    // Keep data reads on the caller: they can submit work to the same executor as the shards.
    PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap64> live_rows,
                           LiveRows(index_splits, snapshot_id.value()));
    PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap64> matched_rows,
                           MatchedRows(index_splits, snapshot_id.value(), live_rows, read_schema));
    const std::optional<RoaringBitmap64>& eligible_rows = matched_rows ? matched_rows : live_rows;
    using SearchResult = Result<std::shared_ptr<ScoredGlobalIndexResult>>;
    std::vector<std::function<SearchResult()>> searches;
    searches.reserve(index_splits.size());
    for (const auto& index_split : index_splits) {
        auto include_row_ids = std::make_shared<const std::optional<RoaringBitmap64>>(
            IncludeRowIds(*index_split, eligible_rows ? &eligible_rows.value() : nullptr));
        if (include_row_ids->has_value() && include_row_ids->value().IsEmpty()) {
            continue;
        }
        searches.emplace_back([this, index_split, include_row_ids, read_schema]() -> SearchResult {
            SearchResult result = SearchSplit(*index_split, *include_row_ids, *read_schema);
            if (!result.ok()) {
                return result.status().WithMessage(
                    fmt::format("{}; {}", result.status().message(), Describe(*index_split)));
            }
            return result;
        });
    }
    std::vector<SearchResult> results;
    std::vector<std::future<SearchResult>> futures;
    if (searches.size() <= 1) {
        for (const auto& search : searches) {
            results.push_back(search());
        }
    } else {
        futures.reserve(searches.size());
        for (const auto& search : searches) {
            futures.push_back(Via(executor_.get(), search));
        }
    }
    std::optional<SearchResult> raw_result;
    if (!raw_ranges.empty()) {
        RawFullTextReadImpl raw_read(root_path_, read_schema, options_, partition_filter_, filter_,
                                     text_field_, query_, limit_, executor_, pool_);
        raw_result = raw_read.Search(*snapshot_id, raw_index_type, raw_rows);
    }
    if (!futures.empty()) {
        results = CollectAll(futures);
    }
    std::vector<std::shared_ptr<ScoredGlobalIndexResult>> scored_results;
    scored_results.reserve(results.size() + 1);
    if (raw_result) {
        if (!raw_result->ok()) {
            const Status& status = raw_result->status();
            const auto& ranges = raw_rows.Ranges();
            std::vector<Range> sample(ranges.begin(),
                                      ranges.begin() + std::min(ranges.size(), kMaxReportedRanges));
            return status.WithMessage(fmt::format(
                "{}; while searching column {} in snapshot {} with a temporary {} index "
                "over {} unindexed row ranges, first {}, schema {}, field id {}",
                status.message(), text_field_.Name(), *snapshot_id, raw_index_type, ranges.size(),
                StringUtils::VectorToString(sample), read_schema->Id(), text_field_.Id()));
        }
        scored_results.push_back(std::move(*raw_result).value());
    }
    for (auto& result : results) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> scored_result,
                               std::move(result));
        scored_results.push_back(std::move(scored_result));
    }
    return ScoredGlobalIndexResultUtils::MergeTopK(scored_results, limit_);
}

}  // namespace paimon
