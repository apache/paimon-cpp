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

#include "paimon/core/operation/raw_file_split_read.h"

#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/file_index/bitmap/apply_bitmap_index_batch_reader.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/reader/late_materialization_batch_reader.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/file_index_evaluator.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class DataFilePathFactory;
class Executor;
class Predicate;

struct RawFileSplitRead::LateMaterializationPlan {
    std::shared_ptr<arrow::Schema> probe_schema;
    std::shared_ptr<arrow::Schema> payload_schema;
};

struct RawFileSplitRead::LateMaterializationReadResult {
    bool applied = false;
    std::vector<std::unique_ptr<BatchReader>> readers;
    std::shared_ptr<Metrics> completed_metrics;
};

RawFileSplitRead::RawFileSplitRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                                   const std::shared_ptr<InternalReadContext>& context,
                                   const std::shared_ptr<MemoryPool>& memory_pool,
                                   const std::shared_ptr<Executor>& executor)
    : AbstractSplitRead(path_factory, context,
                        std::make_unique<SchemaManager>(context->GetCoreOptions().GetFileSystem(),
                                                        context->GetPath(),
                                                        context->GetCoreOptions().GetBranch()),
                        memory_pool, executor) {}

Result<std::unique_ptr<BatchReader>> RawFileSplitRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
    if (!data_split) {
        return Status::Invalid("cannot cast split to data_split in RawFileSplitRead");
    }
    return CreateReader(data_split->Partition(), data_split->Bucket(), data_split->DataFiles(),
                        data_split->DeletionFiles());
}

Result<std::unique_ptr<BatchReader>> RawFileSplitRead::CreateReader(
    const BinaryRow& partition, int32_t bucket,
    const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    DeletionVector::Factory dv_factory) {
    const auto& predicate = context_->GetPredicate();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           path_factory_->CreateDataFilePathFactory(partition, bucket));

    std::shared_ptr<Metrics> completed_late_materialization_metrics;
    if (options_.ReadLateMaterializationEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(LateMaterializationReadResult late_result,
                               TryCreateLateMaterializedReader(partition, data_files, predicate,
                                                               dv_factory, data_file_path_factory));
        completed_late_materialization_metrics = std::move(late_result.completed_metrics);
        if (late_result.applied) {
            std::unique_ptr<ConcatBatchReader> late_reader = std::make_unique<ConcatBatchReader>(
                std::move(late_result.readers), pool_, completed_late_materialization_metrics);
            return std::make_unique<CompleteRowKindBatchReader>(std::move(late_reader), pool_);
        }
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<FileBatchReader>> raw_file_readers,
        CreateRawFileReaders(partition, data_files, raw_read_schema_, predicate, dv_factory,
                             /*row_ranges=*/{}, data_file_path_factory,
                             /*extra_format_options=*/{}));

    auto raw_readers =
        ObjectUtils::MoveVector<std::unique_ptr<BatchReader>>(std::move(raw_file_readers));
    auto concat_batch_reader = std::make_unique<ConcatBatchReader>(
        std::move(raw_readers), pool_, completed_late_materialization_metrics);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> batch_reader,
                           ApplyPredicateFilterIfNeeded(std::move(concat_batch_reader), predicate));
    return std::make_unique<CompleteRowKindBatchReader>(std::move(batch_reader), pool_);
}

Result<std::unique_ptr<BatchReader>> RawFileSplitRead::CreateReader(
    const BinaryRow& partition, int32_t bucket,
    const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    const std::vector<std::optional<DeletionFile>>& deletion_files) {
    auto dv_factory = DeletionVector::CreateFactory(
        options_.GetFileSystem(), DeletionVector::CreateDeletionFileMap(data_files, deletion_files),
        pool_);
    return CreateReader(partition, bucket, data_files, dv_factory);
}

Result<bool> RawFileSplitRead::Match(const std::shared_ptr<Split>& split,
                                     bool force_keep_delete) const {
    auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
    if (split_impl == nullptr) {
        return Status::Invalid("unexpected error, split cast to impl failed");
    }
    if (context_->GetTableSchema()->PrimaryKeys().empty()) {
        // for append table, always return true
        return true;
    }
    bool matched = !force_keep_delete && !split_impl->IsStreaming() && split_impl->RawConvertible();
    if (matched) {
        // for legacy version, we are not sure if there are delete rows, but in order to be
        // compatible with the query acceleration of the OLAP engine, we have generated raw
        // files.
        // Here, for the sake of correctness, we still need to perform drop delete filtering.
        for (const auto& file : split_impl->DataFiles()) {
            if (file->delete_row_count == std::nullopt) {
                return false;
            }
        }
    }
    return matched;
}

Result<std::optional<RawFileSplitRead::LateMaterializationPlan>>
RawFileSplitRead::BuildLateMaterializationPlan(const std::shared_ptr<Predicate>& predicate) const {
    if (!context_->EnablePredicateFilter() || predicate == nullptr) {
        return std::optional<LateMaterializationPlan>();
    }

    std::set<std::string> predicate_field_names;
    PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(predicate, &predicate_field_names));
    if (predicate_field_names.empty()) {
        return std::optional<LateMaterializationPlan>();
    }

    std::vector<std::shared_ptr<arrow::Field>> probe_fields;
    std::set<std::string> probe_names;
    for (const auto& field : raw_read_schema_->fields()) {
        if (predicate_field_names.count(field->name()) > 0) {
            probe_fields.push_back(field);
            probe_names.insert(field->name());
        }
    }
    for (const auto& field_name : predicate_field_names) {
        if (probe_names.count(field_name) > 0) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(DataField data_field,
                               context_->GetTableSchema()->GetField(field_name));
        probe_fields.push_back(data_field.ArrowField());
        probe_names.insert(field_name);
    }

    if (probe_fields.empty()) {
        return std::optional<LateMaterializationPlan>();
    }

    std::vector<std::shared_ptr<arrow::Field>> payload_fields;
    for (const auto& field : raw_read_schema_->fields()) {
        if (probe_names.count(field->name()) == 0) {
            payload_fields.push_back(field);
        }
    }
    if (payload_fields.empty()) {
        return std::optional<LateMaterializationPlan>();
    }

    return std::optional<LateMaterializationPlan>(LateMaterializationPlan{
        arrow::schema(std::move(probe_fields)), arrow::schema(std::move(payload_fields))});
}

auto RawFileSplitRead::TryCreateLateMaterializedReader(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    const std::shared_ptr<Predicate>& predicate, DeletionVector::Factory dv_factory,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const
    -> Result<LateMaterializationReadResult> {
    PAIMON_ASSIGN_OR_RAISE(std::optional<LateMaterializationPlan> plan,
                           BuildLateMaterializationPlan(predicate));
    if (!plan) {
        return LateMaterializationReadResult();
    }

    std::map<std::string, int32_t> probe_field_name_to_idx;
    for (int32_t i = 0; i < plan->probe_schema->num_fields(); ++i) {
        probe_field_name_to_idx[plan->probe_schema->field(i)->name()] = i;
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<Predicate> probe_predicate,
        PredicateUtils::CreatePickedFieldFilter(predicate, probe_field_name_to_idx));
    std::shared_ptr<PredicateFilter> predicate_filter =
        std::dynamic_pointer_cast<PredicateFilter>(probe_predicate);
    if (!predicate_filter) {
        return LateMaterializationReadResult();
    }

    int64_t total_match_rows = 0;
    std::vector<std::unique_ptr<BatchReader>> readers;
    std::shared_ptr<Metrics> completed_metrics = std::make_shared<MetricsImpl>();
    for (const auto& file : data_files) {
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::unique_ptr<FileBatchReader>> probe_readers,
            CreateRawFileReaders(partition, {file}, plan->probe_schema, predicate, dv_factory,
                                 /*row_ranges=*/{}, data_file_path_factory,
                                 /*extra_format_options=*/{}));
        if (probe_readers.empty()) {
            continue;
        }
        if (probe_readers.size() != 1) {
            return Status::Invalid("late materialization expects one probe reader per data file");
        }

        std::vector<std::shared_ptr<arrow::Array>> probe_chunks;
        RoaringBitmap32 selected_file_rows;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                                   probe_readers[0]->NextBatchWithBitmap());
            if (BatchReader::IsEofBatch(batch_with_bitmap)) {
                break;
            }
            BatchReader::ReadBatchWithBitmap moved_batch = std::move(batch_with_bitmap);
            BatchReader::ReadBatch& batch = moved_batch.first;
            RoaringBitmap32& valid_bitmap = moved_batch.second;
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> probe_array,
                arrow::ImportArray(batch.first.get(), batch.second.get()));
            std::shared_ptr<arrow::StructArray> probe_struct =
                arrow::internal::checked_pointer_cast<arrow::StructArray>(probe_array);
            PAIMON_ASSIGN_OR_RAISE(std::vector<char> predicate_result,
                                   predicate_filter->Test(*probe_struct));
            if (static_cast<int64_t>(predicate_result.size()) != probe_struct->length()) {
                return Status::Invalid(fmt::format(
                    "late materialization predicate returned {} results for {} probe rows",
                    predicate_result.size(), probe_struct->length()));
            }

            RoaringBitmap32 selected_bitmap;
            for (auto iter = valid_bitmap.Begin(); iter != valid_bitmap.End(); ++iter) {
                uint32_t batch_row_id = *iter;
                if (batch_row_id >= predicate_result.size()) {
                    return Status::Invalid(fmt::format(
                        "late materialization bitmap row {} exceeds probe batch length {}",
                        batch_row_id, predicate_result.size()));
                }
                if (!predicate_result[batch_row_id]) {
                    continue;
                }
                PAIMON_ASSIGN_OR_RAISE(uint64_t file_row_id,
                                       probe_readers[0]->GetPreviousBatchFileRowId(batch_row_id));
                if (file_row_id > std::numeric_limits<uint32_t>::max()) {
                    return Status::Invalid(
                        fmt::format("late materialization file row id {} exceeds bitmap capacity",
                                    file_row_id));
                }
                selected_file_rows.Add(static_cast<uint32_t>(file_row_id));
                selected_bitmap.Add(batch_row_id);
            }

            if (!selected_bitmap.IsEmpty()) {
                total_match_rows += static_cast<int64_t>(selected_bitmap.Cardinality());
                if (total_match_rows >
                    static_cast<int64_t>(options_.GetReadLateMaterializationMaxMatchRows())) {
                    completed_metrics->Merge(probe_readers[0]->GetReaderMetrics());
                    probe_readers[0]->Close();
                    for (const auto& reader : readers) {
                        completed_metrics->Merge(reader->GetReaderMetrics());
                        reader->Close();
                    }
                    return LateMaterializationReadResult{/*applied=*/false, /*readers=*/{},
                                                         std::move(completed_metrics)};
                }
                PAIMON_ASSIGN_OR_RAISE(
                    arrow::ArrayVector selected_arrays,
                    ReaderUtils::GenerateFilteredArrayVector(probe_struct, selected_bitmap));
                probe_chunks.insert(probe_chunks.end(), selected_arrays.begin(),
                                    selected_arrays.end());
            }
        }
        completed_metrics->Merge(probe_readers[0]->GetReaderMetrics());
        probe_readers[0]->Close();

        if (selected_file_rows.IsEmpty()) {
            continue;
        }

        std::unique_ptr<arrow::MemoryPool> probe_arrow_pool = GetArrowPool(pool_);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> probe_data,
                                          arrow::Concatenate(probe_chunks, probe_arrow_pool.get()));
        std::shared_ptr<arrow::StructArray> probe_struct =
            arrow::internal::checked_pointer_cast<arrow::StructArray>(probe_data);
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::unique_ptr<FileBatchReader>> payload_readers,
            CreateRawFileReaders(partition, {file}, plan->payload_schema,
                                 /*predicate=*/nullptr, dv_factory,
                                 /*row_ranges=*/std::nullopt, data_file_path_factory,
                                 /*extra_format_options=*/{}, selected_file_rows));
        if (payload_readers.empty()) {
            return Status::Invalid("late materialization payload reader was filtered out");
        }
        if (payload_readers.size() != 1) {
            return Status::Invalid("late materialization expects one payload reader per data file");
        }
        std::unique_ptr<BatchReader> payload_reader = std::move(payload_readers[0]);
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<LateMaterializationBatchReader> late_reader,
            LateMaterializationBatchReader::Create(
                raw_read_schema_, plan->probe_schema, std::move(probe_struct), plan->payload_schema,
                std::move(payload_reader), options_.GetReadBatchSize(), pool_,
                std::move(probe_arrow_pool)));
        readers.push_back(std::move(late_reader));
    }

    return LateMaterializationReadResult{/*applied=*/true, std::move(readers),
                                         std::move(completed_metrics)};
}

Result<std::unique_ptr<FileBatchReader>> RawFileSplitRead::ApplyIndexAndDvReaderIfNeeded(
    std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& read_schema, const std::shared_ptr<Predicate>& predicate,
    DeletionVector::Factory dv_factory, const std::optional<std::vector<Range>>& ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const std::optional<RoaringBitmap32>& file_selection) const {
    std::shared_ptr<FileIndexResult> file_index_result;
    if (options_.FileIndexReadEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(
            file_index_result,
            FileIndexEvaluator::Evaluate(data_schema, predicate, data_file_path_factory, file,
                                         options_.GetFileSystem(), pool_));
        PAIMON_ASSIGN_OR_RAISE(bool is_remain, file_index_result->IsRemain());
        if (!is_remain) {
            return std::unique_ptr<FileBatchReader>();
        }
    }
    // prepare selection bitmap for index
    const RoaringBitmap32* selection = nullptr;
    if (auto* bitmap_file_index = dynamic_cast<BitmapIndexResult*>(file_index_result.get())) {
        PAIMON_ASSIGN_OR_RAISE(selection, bitmap_file_index->GetBitmap());
    }

    // prepare deletion bitmap for deletion vector
    std::shared_ptr<DeletionVector> deletion_vector;
    if (dv_factory) {
        PAIMON_ASSIGN_OR_RAISE(deletion_vector, dv_factory(file->file_name));
    }
    const RoaringBitmap32* deletion = nullptr;
    if (auto* bitmap_dv = dynamic_cast<BitmapDeletionVector*>(deletion_vector.get())) {
        deletion = bitmap_dv->GetBitmap();
    }

    // merge deletion and bitmap index selection
    std::optional<RoaringBitmap32> actual_selection;
    if (selection && deletion) {
        actual_selection = RoaringBitmap32::AndNot(*selection, *deletion);
    } else if (selection) {
        actual_selection = *selection;
    } else if (deletion) {
        actual_selection = *deletion;
        PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, file_reader->GetNumberOfRows());
        actual_selection.value().Flip(0, num_rows);
    }

    // `ranges` uses global row ids. ToFileSelection intersects it with this file's global row id
    // span and returns a file-local bitmap, which can be merged with index and deletion bitmaps.
    PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap32> range_selection,
                           file->ToFileSelection(ranges));
    if (range_selection) {
        if (actual_selection) {
            actual_selection =
                RoaringBitmap32::And(actual_selection.value(), range_selection.value());
        } else {
            actual_selection = std::move(range_selection);
        }
    }

    // Late materialization already operates on one FileBatchReader at a time, so its selection
    // can stay file-local instead of making a round trip through global row IDs.
    if (file_selection) {
        actual_selection = actual_selection ? RoaringBitmap32::And(actual_selection.value(),
                                                                   file_selection.value())
                                            : file_selection;
    }

    if (actual_selection && actual_selection.value().IsEmpty()) {
        return std::unique_ptr<FileBatchReader>();
    }

    ::ArrowSchema c_read_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, &c_read_schema));
    PAIMON_RETURN_NOT_OK(file_reader->SetReadSchema(&c_read_schema, predicate, actual_selection));

    std::unique_ptr<FileBatchReader> reader;
    if (!file_reader->SupportPreciseBitmapSelection() && actual_selection) {
        // Some formats (for example blob) return an accurate batch result, where
        // ApplyBitmapIndexBatchReader is not necessary
        reader = std::make_unique<ApplyBitmapIndexBatchReader>(std::move(file_reader),
                                                               std::move(actual_selection).value());
    } else {
        reader = std::move(file_reader);
    }

    if (deletion_vector && !deletion && !deletion_vector->IsEmpty()) {
        // TODO(xinyu.lxy): if deletion vector is bitmap64, use ApplyBitmapIndexBatchReader to
        // filter result
        return Status::NotImplemented("Only support BitmapDeletionVector");
    }
    return std::move(reader);
}

}  // namespace paimon
