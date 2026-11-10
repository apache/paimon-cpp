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
#include "paimon/core/global_index/global_index_scan_impl.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/global_index/offset_global_index_reader.h"
#include "paimon/common/global_index/union_global_index_reader.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/global_index_evaluator_impl.h"
#include "paimon/core/global_index/global_index_live_row_filter.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"

namespace paimon {
GlobalIndexScanImpl::GlobalIndexScanImpl(const std::shared_ptr<TableSchema>& table_schema,
                                         const CoreOptions& options,
                                         const std::shared_ptr<IndexPathFactory>& path_factory,
                                         IndexMetaMap&& index_metas, LiveRowsLoader load_live_rows,
                                         const std::shared_ptr<Executor>& executor,
                                         const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      table_schema_(table_schema),
      options_(options),
      index_file_manager_(std::make_shared<GlobalIndexFileManager>(
          options.GetFileSystem(), path_factory, /*checkpoint_path_factory=*/nullptr)),
      index_metas_(std::move(index_metas)),
      load_live_rows_(std::move(load_live_rows)),
      executor_(executor) {}

Result<std::shared_ptr<Executor>> GlobalIndexScanImpl::GetOrCreateExecutor(
    const std::shared_ptr<Executor>& executor, const CoreOptions& options) {
    if (executor) {
        return executor;
    }
    std::optional<int32_t> thread_num = options.GetGlobalIndexThreadNum();
    if (thread_num) {
        if (thread_num.value() <= 0) {
            return Status::Invalid(
                fmt::format("invalid global index thread number {}", thread_num.value()));
        }
    } else {
        uint32_t cpu_count = std::thread::hardware_concurrency();
        thread_num = cpu_count > 0 ? static_cast<int32_t>(cpu_count) : 1;
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Executor> created_executor,
                           CreateDefaultExecutor(static_cast<uint32_t>(thread_num.value())));
    return std::shared_ptr<Executor>(std::move(created_executor));
}

Result<std::unique_ptr<GlobalIndexScanImpl>> GlobalIndexScanImpl::Create(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const Snapshot& snapshot, const std::shared_ptr<PredicateFilter>& partitions,
    const CoreOptions& options, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Executor> final_executor,
                           GetOrCreateExecutor(executor, options));
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> external_path_weights,
                           options.GetExternalPathWeights());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> file_store_path_factory,
        FileStorePathFactory::Create(
            root_path, arrow_schema, table_schema->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_path, options.IndexFileInDataFileDir(),
            /*external_path_weights=*/external_path_weights, pool));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexPathFactory> path_factory,
                           file_store_path_factory->CreateGlobalIndexFileFactory());

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexManifestFile> index_manifest_file,
        IndexManifestFile::Create(options.GetFileSystem(), options.GetManifestFormat(),
                                  options.GetManifestCompression(), file_store_path_factory,
                                  ResolveBucketMode(options.GetBucket(), table_schema), pool,
                                  options));
    auto index_file_handler = std::make_unique<IndexFileHandler>(
        options.GetFileSystem(), std::move(index_manifest_file),
        std::make_shared<IndexFilePathFactories>(file_store_path_factory),
        options.DeletionVectorsBitmap64(), pool);

    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> partition_fields,
                           table_schema->GetFields(table_schema->PartitionKeys()));
    auto partition_schema = DataField::ConvertDataFieldsToArrowSchema(partition_fields);
    std::function<Result<bool>(const IndexManifestEntry&)> filter =
        [&](const IndexManifestEntry& entry) -> Result<bool> {
        if (partitions) {
            PAIMON_ASSIGN_OR_RAISE(bool saved, partitions->Test(partition_schema, entry.partition));
            if (!saved) {
                return false;
            }
        }
        if (!entry.index_file->GetGlobalIndexMeta()) {
            return false;
        }
        return true;
    };
    PAIMON_ASSIGN_OR_RAISE(std::vector<IndexManifestEntry> entries,
                           index_file_handler->Scan(snapshot, filter));
    IndexMetaMap index_metas;
    for (const auto& entry : entries) {
        auto index_file_meta = entry.index_file;
        const auto& index_meta = index_file_meta->GetGlobalIndexMeta();
        assert(index_meta);
        Range range(index_meta->row_range_start, index_meta->row_range_end);
        index_metas[index_meta->index_field_id][index_file_meta->IndexType()][range].push_back(
            index_file_meta);
    }
    // Full-text and vector searches rank only the rows live in the snapshot the index is read from.
    LiveRowsLoader load_live_rows;
    if (options.DeletionVectorsEnabled()) {
        load_live_rows = [root_path, table_schema, snapshot, partitions, options, final_executor,
                          pool](const RowRangeIndex& row_range_index) {
            return GlobalIndexLiveRowFilter::LiveRows(root_path, table_schema, snapshot, partitions,
                                                      row_range_index, options, final_executor,
                                                      pool);
        };
    }
    return std::unique_ptr<GlobalIndexScanImpl>(
        new GlobalIndexScanImpl(table_schema, options, path_factory, std::move(index_metas),
                                std::move(load_live_rows), final_executor, pool));
}

std::unique_ptr<GlobalIndexScanImpl> GlobalIndexScanImpl::CreateForScalarFilters(
    const std::shared_ptr<TableSchema>& table_schema, const CoreOptions& options,
    const std::shared_ptr<IndexPathFactory>& path_factory,
    const std::vector<std::shared_ptr<IndexFileMeta>>& index_files,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    IndexMetaMap index_metas;
    for (const auto& index_file : index_files) {
        const auto& index_meta = index_file->GetGlobalIndexMeta();
        assert(index_meta);
        if (index_meta->extra_field_ids && !index_meta->extra_field_ids->empty()) {
            continue;
        }
        Range range(index_meta->row_range_start, index_meta->row_range_end);
        index_metas[index_meta->index_field_id][index_file->IndexType()][range].push_back(
            index_file);
    }
    return std::unique_ptr<GlobalIndexScanImpl>(
        new GlobalIndexScanImpl(table_schema, options, path_factory, std::move(index_metas),
                                LiveRowsLoader(), executor, pool));
}

Result<std::shared_ptr<GlobalIndexEvaluator>> GlobalIndexScanImpl::GetOrCreateIndexEvaluator() {
    if (evaluator_) {
        return evaluator_;
    }
    GlobalIndexEvaluatorImpl::IndexReadersCreator create_index_readers =
        [this](int32_t field_id) -> Result<std::vector<std::shared_ptr<GlobalIndexReader>>> {
        return CreateReaders(field_id, /*row_range_index=*/std::nullopt);
    };
    evaluator_ = std::make_shared<GlobalIndexEvaluatorImpl>(table_schema_, create_index_readers);
    return evaluator_;
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GlobalIndexScanImpl::CreateReaders(
    int32_t field_id, const std::optional<RowRangeIndex>& row_range_index) const {
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(field_id));
    return CreateReaders(field, /*index_type=*/std::nullopt, row_range_index);
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GlobalIndexScanImpl::CreateReaders(
    const std::string& field_name, const std::optional<RowRangeIndex>& row_range_index) const {
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(field_name));
    return CreateReaders(field, /*index_type=*/std::nullopt, row_range_index);
}

Result<std::shared_ptr<GlobalIndexReader>> GlobalIndexScanImpl::CreateReader(
    const std::string& field_name, const std::string& index_type,
    const std::optional<RowRangeIndex>& row_range_index) const {
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema_->GetField(field_name));
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::shared_ptr<GlobalIndexReader>> readers,
        CreateReaders(field, std::optional<std::string>(index_type), row_range_index));
    if (readers.empty()) {
        return std::shared_ptr<GlobalIndexReader>();
    }
    if (readers.size() != 1) {
        return Status::Invalid(
            fmt::format("invalid global index reader size, expected 1, actual {}", readers.size()));
    }
    return readers[0];
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GlobalIndexScanImpl::CreateReaders(
    const DataField& field, const std::optional<std::string>& index_type,
    const std::optional<RowRangeIndex>& row_range_index) const {
    auto field_iter = index_metas_.find(field.Id());
    if (field_iter == index_metas_.end()) {
        return std::vector<std::shared_ptr<GlobalIndexReader>>();
    }
    const auto& index_type_to_metas = field_iter->second;
    std::vector<std::shared_ptr<GlobalIndexReader>> readers;
    readers.reserve(index_type ? 1 : index_type_to_metas.size());
    for (const auto& [current_index_type, range_to_metas] : index_type_to_metas) {
        if (index_type && current_index_type != index_type.value()) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                               GlobalIndexerFactory::Get(current_index_type, options_.ToMap()));
        if (!indexer) {
            continue;
        }
        std::vector<std::shared_ptr<GlobalIndexReader>> union_readers;
        std::vector<Range> row_ranges;
        std::vector<std::optional<Range>> search_ranges;
        union_readers.reserve(range_to_metas.size());
        row_ranges.reserve(range_to_metas.size());
        search_ranges.reserve(range_to_metas.size());
        // Assign each full-text row to one reader before requesting per-reader top-k: a row is
        // ranked by the first range in (from, to) order that covers it, which leaves each range
        // only its rows after the largest `to` of earlier ones.
        int64_t covered_to = std::numeric_limits<int64_t>::min();
        for (const auto& [range, metas] : range_to_metas) {
            std::optional<Range> search_range;
            if (covered_to < range.to) {
                search_range = Range(std::max(range.from, covered_to + 1), range.to);
            }
            covered_to = std::max(covered_to, range.to);
            if (row_range_index && !row_range_index->Intersects(range.from, range.to)) {
                continue;
            }
            std::optional<std::vector<Range>> local_row_ranges;
            if (row_range_index) {
                std::vector<Range> intersected_ranges =
                    row_range_index->IntersectedRanges(range.from, range.to);
                local_row_ranges.emplace();
                local_row_ranges->reserve(intersected_ranges.size());
                for (const Range& intersected_range : intersected_ranges) {
                    local_row_ranges->emplace_back(intersected_range.from - range.from,
                                                   intersected_range.to - range.from);
                }
            }
            auto arrow_field = DataField::ConvertDataFieldToArrowField(field);
            auto arrow_schema = arrow::schema({arrow_field});

            ArrowSchema c_arrow_schema;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
            auto index_io_metas = ToGlobalIndexIOMetas(metas);
            ScopeGuard guard([&]() { ArrowSchemaRelease(&c_arrow_schema); });
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<GlobalIndexReader> index_reader,
                indexer->CreateReader(&c_arrow_schema, index_file_manager_, index_io_metas,
                                      range.Count(), local_row_ranges, pool_));
            union_readers.push_back(std::make_shared<OffsetGlobalIndexReader>(
                std::move(index_reader), range.from, range.to));
            row_ranges.push_back(range);
            search_ranges.push_back(search_range);
        }
        if (union_readers.empty()) {
            continue;
        }
        if (load_live_rows_) {
            // As Java `AbstractDataEvolutionVectorRead#preFilters` does, load only the live rows of
            // the indexed row ranges, so Data Evolution tables skip the deletion vectors of other
            // data files.
            PAIMON_ASSIGN_OR_RAISE(RowRangeIndex indexed_row_ranges,
                                   RowRangeIndex::Create(row_ranges));
            LiveRowGlobalIndexReader::LiveRowsSupplier live_rows_supplier =
                LiveRowGlobalIndexReader::CacheLiveRows(
                    [load_live_rows = load_live_rows_, indexed_row_ranges]() {
                        return load_live_rows(indexed_row_ranges);
                    });
            readers.push_back(LiveRowGlobalIndexReader::CreateUnion(
                std::move(union_readers), std::move(row_ranges), std::move(search_ranges),
                executor_, live_rows_supplier));
        } else {
            readers.push_back(std::make_shared<UnionGlobalIndexReader>(
                std::move(union_readers), std::move(row_ranges), std::move(search_ranges),
                executor_));
        }
    }
    return readers;
}

std::vector<GlobalIndexIOMeta> GlobalIndexScanImpl::ToGlobalIndexIOMetas(
    const std::vector<std::shared_ptr<IndexFileMeta>>& metas) const {
    std::vector<GlobalIndexIOMeta> index_io_metas;
    index_io_metas.reserve(metas.size());
    for (const auto& meta : metas) {
        index_io_metas.push_back(ToGlobalIndexIOMeta(meta));
    }
    return index_io_metas;
}

GlobalIndexIOMeta GlobalIndexScanImpl::ToGlobalIndexIOMeta(
    const std::shared_ptr<IndexFileMeta>& index_meta) const {
    assert(index_meta->GetGlobalIndexMeta());
    const auto& global_index_meta = index_meta->GetGlobalIndexMeta().value();
    return {index_file_manager_->ToPath(index_meta), index_meta->FileSize(), index_meta->RowCount(),
            global_index_meta.index_meta};
}

Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexScanImpl::Scan(
    const std::shared_ptr<Predicate>& predicate) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexEvaluator> evaluator,
                           GetOrCreateIndexEvaluator());
    return evaluator->Evaluate(predicate);
}

Result<std::optional<GlobalIndexEvaluatorImpl::Evaluation>> GlobalIndexScanImpl::ScanWithCoverage(
    const std::shared_ptr<Predicate>& predicate) const {
    GlobalIndexEvaluatorImpl evaluator(
        table_schema_,
        [this](int32_t field_id) -> Result<std::vector<std::shared_ptr<GlobalIndexReader>>> {
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<GlobalIndexReader>> readers,
                                   CreateReaders(field_id, /*row_range_index=*/std::nullopt));
            if (readers.size() <= 1) {
                return readers;
            }
            // Each reader already fans out on `executor_`, so the union stays on this thread and
            // no task of `executor_` waits for another one.
            std::vector<std::shared_ptr<GlobalIndexReader>> union_reader = {
                std::make_shared<UnionGlobalIndexReader>(std::move(readers), std::vector<Range>(),
                                                         std::vector<std::optional<Range>>(),
                                                         /*executor=*/nullptr)};
            return union_reader;
        });
    return evaluator.EvaluateWithContributingFields(predicate);
}

}  // namespace paimon
