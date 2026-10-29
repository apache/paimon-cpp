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

#include "paimon/core/operation/data_evolution_split_read.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/data/blob_view_struct.h"
#include "paimon/common/file_index/bitmap/apply_bitmap_index_batch_reader.h"
#include "paimon/common/reader/blob_fallback_batch_reader.h"
#include "paimon/common/reader/blob_view_resolving_batch_reader.h"
#include "paimon/common/reader/complete_index_score_batch_reader.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/common/utils/vector_store_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/apply_deletion_vector_batch_reader.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/file_index_evaluator.h"
#include "paimon/core/io/file_index_options.h"
#include "paimon/core/table/format/lazy_concat_batch_reader.h"
#include "paimon/core/utils/blob_view_lookup.h"
#include "paimon/core/utils/data_evolution_utils.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/predicate/predicate_utils.h"

namespace paimon {
namespace {
/// A read-only view over one file's window of a row range group's deletion vector. The reader
/// probes with positions local to its file; IsDeleted(p) forwards them as IsDeleted(p + offset),
/// the anchor-relative positions the group deletion vector is indexed by.
///
/// The shift rides on a full DeletionVector because that is what DeletionVector::Factory hands
/// to ApplyIndexAndDvReaderIfNeeded; narrowing it to Paimon Java's read-only DeletionVectorJudger
/// would reach every caller of the factory. The mutating and serializing halves reject instead.
class PositionShiftedDeletionVector : public DeletionVector {
 public:
    PositionShiftedDeletionVector(const std::shared_ptr<DeletionVector>& inner, int64_t offset,
                                  int64_t length)
        : inner_(inner), offset_(offset), length_(length) {}

    Result<bool> IsDeleted(int64_t position) const override {
        if (position < 0 || position >= length_) {
            return Status::Invalid(
                fmt::format("PositionShiftedDeletionVector position {} out of window [0, {})",
                            position, length_));
        }
        return inner_->IsDeleted(position + offset_);
    }

    /// Conservative: false only means the group deletion vector holds deletions somewhere, not
    /// necessarily inside this window. Over-reporting just makes the wrapping filter a no-op.
    bool IsEmpty() const override {
        return inner_->IsEmpty();
    }

    /// Deleted positions inside this window. Off the read path, which only calls IsDeleted and
    /// IsEmpty; the row counting that does consult a vector's cardinality builds its factory
    /// from the split's deletion files, never from this view.
    Result<int64_t> GetCardinality() const override {
        int64_t count = 0;
        PAIMON_RETURN_NOT_OK(ForEachDeletedPosition([&count](int64_t) { ++count; }));
        return count;
    }

    Status ForEachDeletedPosition(const std::function<void(int64_t)>& consumer) const override {
        return inner_->ForEachDeletedPosition([&](int64_t position) {
            if (position >= offset_ && position - offset_ < length_) {
                consumer(position - offset_);
            }
        });
    }

    Status Delete(int64_t) override {
        return Status::Invalid("PositionShiftedDeletionVector is read-only");
    }

    Result<bool> CheckedDelete(int64_t) override {
        return Status::Invalid("PositionShiftedDeletionVector is read-only");
    }

    Status Merge(const std::shared_ptr<DeletionVector>&) override {
        return Status::Invalid("PositionShiftedDeletionVector is read-only");
    }

    Result<int32_t> SerializeTo(const std::shared_ptr<MemoryPool>&, DataOutputStream*) override {
        return Status::Invalid("PositionShiftedDeletionVector does not support serialization");
    }

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeToBytes(const std::shared_ptr<MemoryPool>&) override {
        return Status::Invalid("PositionShiftedDeletionVector does not support serialization");
    }

 private:
    std::shared_ptr<DeletionVector> inner_;
    const int64_t offset_;
    const int64_t length_;
};
}  // namespace

Result<int64_t> DataEvolutionSplitRead::BlobBunch::RowCount() const {
    const auto merged = Range::SortAndMergeOverlap(ranges_, /*adjacent=*/true);
    if (expected_row_range_) {
        for (const auto& range : merged) {
            if (range.from < expected_row_range_->from || range.to > expected_row_range_->to) {
                return Status::Invalid("Blob file range should be within normal file range.");
            }
        }
        return expected_row_range_->Count();
    }
    if (!has_row_ids_selection_ && merged.size() != 1) {
        return Status::Invalid("Blob file bunch should always contain a contiguous row range.");
    }
    int64_t row_count = 0;
    for (const auto& range : merged) {
        row_count += range.Count();
    }
    return row_count;
}

Result<Range> DataEvolutionSplitRead::BlobBunch::LogicalRange() const {
    if (expected_row_range_) {
        return *expected_row_range_;
    }
    const auto merged = Range::SortAndMergeOverlap(ranges_, /*adjacent=*/true);
    if (merged.empty()) {
        return Status::Invalid("Blob file bunch should not be empty.");
    }
    return Range(merged.front().from, merged.back().to);
}

Status DataEvolutionSplitRead::BlobBunch::Add(const std::shared_ptr<DataFileMeta>& file) {
    if (!BlobUtils::IsBlobFile(file->file_name)) {
        return Status::Invalid("Only blob file can be added to a blob bunch.");
    }
    if (!files_.empty()) {
        // Blob files for the same field may span schema ids.
        if (file->write_cols != files_[0]->write_cols) {
            return Status::Invalid("All files in a blob bunch should have the same write columns.");
        }
    }
    PAIMON_ASSIGN_OR_RAISE(Range range, file->NonNullRowIdRange());
    files_.push_back(file);
    ranges_.push_back(range);
    return Status::OK();
}

DataEvolutionSplitRead::DataEvolutionSplitRead(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor)
    : AbstractSplitRead(path_factory, context,
                        std::make_unique<SchemaManager>(context->GetCoreOptions().GetFileSystem(),
                                                        context->GetPath(),
                                                        context->GetCoreOptions().GetBranch()),
                        memory_pool, executor) {}

bool DataEvolutionSplitRead::HasIndexScoreField(const std::shared_ptr<arrow::Schema>& read_schema) {
    return read_schema->GetFieldIndex(SpecialFields::IndexScore().Name()) != -1;
}

std::vector<std::string> DataEvolutionSplitRead::HasBlobViewField(
    const CoreOptions& options, const std::shared_ptr<arrow::Schema>& read_schema) {
    std::vector<std::string> read_blob_view_fields;
    std::vector<std::string> blob_view_fields = options.GetBlobViewFields();
    for (const auto& blob : blob_view_fields) {
        if (read_schema->GetFieldByName(blob)) {
            read_blob_view_fields.push_back(blob);
        }
    }
    return read_blob_view_fields;
}

Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    if (auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(indexed_split->Validate());
        const auto& data_split = indexed_split->GetDataSplit();
        auto read_schema = raw_read_schema_;
        bool complete_scores = HasIndexScoreField(read_schema) && !indexed_split->Scores().empty();
        bool remove_row_id = false;
        std::unordered_map<int64_t, float> scores_by_row_id;
        if (complete_scores) {
            scores_by_row_id.reserve(indexed_split->Scores().size());
            size_t score_idx = 0;
            for (const auto& range : indexed_split->RowRanges()) {
                for (int64_t row_id = range.from; row_id <= range.to; ++row_id) {
                    scores_by_row_id.emplace(row_id, indexed_split->Scores()[score_idx++]);
                }
            }
            remove_row_id = read_schema->GetFieldIndex(SpecialFields::RowId().Name()) < 0;
            if (remove_row_id) {
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    read_schema, read_schema->AddField(read_schema->num_fields(),
                                                       DataField::ConvertDataFieldToArrowField(
                                                           SpecialFields::RowId())));
            }
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<BatchReader> batch_reader,
            InnerCreateReader(data_split, indexed_split->RowRanges(), read_schema));
        if (complete_scores) {
            batch_reader = std::make_unique<CompleteIndexScoreBatchReader>(
                std::move(batch_reader), std::move(scores_by_row_id), remove_row_id, arrow_pool_);
        }
        return WrapWithBlobViewResolverIfNeeded(data_split, std::move(batch_reader),
                                                indexed_split->RowRanges());
    } else if (auto data_split = std::dynamic_pointer_cast<DataSplit>(split)) {
        if (HasIndexScoreField(raw_read_schema_)) {
            return Status::Invalid(
                "Invalid read schema, read _INDEX_SCORE while split cannot cast to IndexedSplit");
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<BatchReader> inner_reader,
            InnerCreateReader(data_split, /*row_ranges=*/std::nullopt, raw_read_schema_));
        return WrapWithBlobViewResolverIfNeeded(data_split, std::move(inner_reader),
                                                /*row_ranges=*/std::nullopt);
    }
    return Status::Invalid("Invalid Split, cannot cast to IndexedSplit or DataSplit");
}

Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::WrapWithBlobViewResolverIfNeeded(
    const std::shared_ptr<DataSplit>& data_split, std::unique_ptr<BatchReader>&& inner_reader,
    const std::optional<std::vector<Range>>& row_ranges) const {
    if (!options_.BlobViewResolveEnabled()) {
        // preserve serialized BlobViewStruct bytes, e.g. for forwarding blob view values to
        // another blob-view table
        return std::move(inner_reader);
    }
    std::vector<std::string> read_blob_view_fields = HasBlobViewField(options_, raw_read_schema_);
    if (read_blob_view_fields.empty()) {
        return std::move(inner_reader);
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> pre_reader,
                           CreateBlobViewReader(data_split, read_blob_view_fields, row_ranges));
    PAIMON_ASSIGN_OR_RAISE(std::unordered_set<BlobViewStruct> blob_view_structs,
                           ExtractBlobViewStructs(pre_reader.get()));
    // use global thread number
    uint32_t cpu_count = std::thread::hardware_concurrency();
    uint32_t thread_num = cpu_count > 0 ? cpu_count : 1;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Executor> executor, CreateDefaultExecutor(thread_num));
    PAIMON_ASSIGN_OR_RAISE(
        BlobViewResolver resolver,
        BlobViewLookup::CreateResolver(
            blob_view_structs, context_->CreateDependencyCatalogContext(), pool_, executor));
    return std::make_unique<BlobViewResolvingBatchReader>(std::move(inner_reader),
                                                          std::move(read_blob_view_fields),
                                                          std::move(resolver), arrow_pool_);
}

Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::CreateBlobViewReader(
    const std::shared_ptr<DataSplit>& data_split,
    const std::vector<std::string>& read_blob_view_fields,
    const std::optional<std::vector<Range>>& row_ranges) const {
    auto split_impl = dynamic_cast<DataSplitImpl*>(data_split.get());
    if (split_impl == nullptr) {
        return Status::Invalid("unexpected error, split cast to impl failed");
    }
    assert(raw_read_schema_->num_fields() > 0);

    arrow::FieldVector blob_view_arrow_fields;
    blob_view_arrow_fields.reserve(read_blob_view_fields.size());
    for (const auto& field_name : read_blob_view_fields) {
        auto field = raw_read_schema_->GetFieldByName(field_name);
        if (field == nullptr) {
            return Status::Invalid(
                fmt::format("Blob view field {} not found in read schema.", field_name));
        }
        blob_view_arrow_fields.push_back(std::move(field));
    }
    auto blob_view_schema = arrow::schema(std::move(blob_view_arrow_fields));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<DataFilePathFactory> data_file_path_factory,
        path_factory_->CreateDataFilePathFactory(split_impl->Partition(), split_impl->Bucket()));

    DeletionVector::Factory split_dv_factory = CreateSplitDvFactory(*split_impl);
    auto metas = split_impl->DataFiles();
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_by_row_id,
                           MergeRangesAndSort(std::move(metas)));

    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    for (const std::vector<std::shared_ptr<DataFileMeta>>& group : split_by_row_id) {
        // Dedicated files contain payloads, not the blob-view columns.
        std::vector<std::shared_ptr<DataFileMeta>> data_files;
        data_files.reserve(group.size());
        for (const auto& file : group) {
            if (DataEvolutionUtils::IsNormalFile(file->file_name)) {
                data_files.push_back(file);
            }
        }
        if (data_files.empty()) {
            continue;
        }
        // the anchor is looked up over the whole group, as the main read does, so both find the
        // same deletion vector; only the shifted views are built for the files read here
        PAIMON_ASSIGN_OR_RAISE(std::optional<GroupDeletionVector> group_dv,
                               ReadGroupDeletionVector(group, split_dv_factory));
        PAIMON_ASSIGN_OR_RAISE(DeletionVector::Factory group_dv_factory,
                               CreateGroupDvFactory(data_files, group_dv, row_ranges));
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::unique_ptr<FileBatchReader>> raw_file_readers,
            CreateRawFileReaders(split_impl->Partition(), data_files, blob_view_schema,
                                 /*predicate=*/nullptr, group_dv_factory, row_ranges,
                                 data_file_path_factory,
                                 /*extra_format_options=*/{}));
        for (std::unique_ptr<FileBatchReader>& raw_file_reader : raw_file_readers) {
            batch_readers.push_back(std::move(raw_file_reader));
        }
    }
    return std::make_unique<ConcatBatchReader>(std::move(batch_readers), arrow_pool_);
}

Result<std::unordered_set<BlobViewStruct>> DataEvolutionSplitRead::ExtractBlobViewStructs(
    BatchReader* reader) {
    if (reader == nullptr) {
        return Status::Invalid("invalid reader in ExtractBlobViewStructs, reader is nullptr");
    }
    std::unordered_set<BlobViewStruct> blob_view_structs;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (!arrow_array || arrow_array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid(
                "invalid array in ExtractBlobViewStructs, batch array is not a StructArray.");
        }
        auto struct_array = checked_pointer_cast<arrow::StructArray>(arrow_array);

        for (int32_t field_idx = 0; field_idx < struct_array->num_fields(); ++field_idx) {
            auto field_array = struct_array->field(field_idx);
            if (!field_array || field_array->type_id() != arrow::Type::LARGE_BINARY) {
                return Status::Invalid(
                    "invalid array in ExtractBlobViewStructs, blob view column is not a "
                    "LargeBinaryArray.");
            }
            auto binary_array = checked_pointer_cast<arrow::LargeBinaryArray>(field_array);
            for (int64_t row = 0; row < binary_array->length(); ++row) {
                if (binary_array->IsNull(row)) {
                    continue;
                }
                std::string_view bytes = binary_array->GetView(row);
                PAIMON_ASSIGN_OR_RAISE(
                    bool is_view, BlobViewStruct::IsBlobViewStruct(bytes.data(), bytes.size()));
                if (!is_view) {
                    return Status::Invalid(
                        "blob-view-field requires blob field value to be a serialized "
                        "BlobViewStruct.");
                }
                PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlobViewStruct> view_struct,
                                       BlobViewStruct::Deserialize(bytes.data(), bytes.size()));
                blob_view_structs.insert(*view_struct);
            }
        }
    }
    return blob_view_structs;
}

Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::InnerCreateReader(
    const std::shared_ptr<DataSplit>& data_split,
    const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<arrow::Schema>& read_schema) const {
    auto split_impl = dynamic_cast<DataSplitImpl*>(data_split.get());
    if (split_impl == nullptr) {
        return Status::Invalid("unexpected error, split cast to impl failed");
    }
    assert(read_schema->num_fields() > 0);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<DataFilePathFactory> data_file_path_factory,
        path_factory_->CreateDataFilePathFactory(split_impl->Partition(), split_impl->Bucket()));
    auto metas = split_impl->DataFiles();
    DeletionVector::Factory split_dv_factory = CreateSplitDvFactory(*split_impl);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> push_down_predicate,
                           CreatePushDownPredicate(context_->GetPredicate(), read_schema));

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_by_row_id,
                           MergeRangesAndSort(std::move(metas)));

    std::vector<std::unique_ptr<BatchReader>> sub_readers;
    for (const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files : split_by_row_id) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap32> group_selection,
                               SelectByFileIndex(push_down_predicate, need_merge_files, read_schema,
                                                 row_ranges, data_file_path_factory));
        if (group_selection && group_selection->IsEmpty()) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::optional<GroupDeletionVector> group_dv,
                               ReadGroupDeletionVector(need_merge_files, split_dv_factory));
        PAIMON_ASSIGN_OR_RAISE(DeletionVector::Factory group_dv_factory,
                               CreateGroupDvFactory(need_merge_files, group_dv, row_ranges));
        if (group_dv && group_selection) {
            PAIMON_ASSIGN_OR_RAISE(
                *group_selection,
                ExcludeDeletedPositions(std::move(*group_selection), group_dv->deletion_vector));
            if (group_selection->IsEmpty()) {
                continue;
            }
        }
        DataEvolutionSplitRead group_read(path_factory_, context_, pool_, executor_);
        group_read.file_indexes_evaluated_ = true;
        group_read.group_selection_ = std::move(group_selection);
        if (need_merge_files.size() == 1) {
            // No need to merge fields, just create a single file reader
            PAIMON_ASSIGN_OR_RAISE(
                std::vector<std::unique_ptr<FileBatchReader>> raw_file_readers,
                group_read.CreateRawFileReaders(split_impl->Partition(), need_merge_files,
                                                read_schema, push_down_predicate, group_dv_factory,
                                                row_ranges, data_file_path_factory,
                                                /*extra_format_options=*/{}));
            if (raw_file_readers.empty()) {
                continue;
            }
            if (raw_file_readers.size() != 1) {
                return Status::Invalid("Single-file data evolution group created multiple readers");
            }
            sub_readers.push_back(std::move(raw_file_readers[0]));
        } else {
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<BatchReader> evolution_reader,
                group_read.CreateUnionReader(split_impl->Partition(), need_merge_files, row_ranges,
                                             data_file_path_factory, group_dv_factory, group_dv,
                                             read_schema));
            sub_readers.push_back(std::move(evolution_reader));
        }
    }
    auto concat_batch_reader =
        std::make_unique<ConcatBatchReader>(std::move(sub_readers), arrow_pool_);
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BatchReader> batch_reader,
        ApplyPredicateFilterIfNeeded(std::move(concat_batch_reader), context_->GetPredicate()));
    return std::make_unique<CompleteRowKindBatchReader>(std::move(batch_reader), arrow_pool_);
}

Result<std::shared_ptr<Predicate>> DataEvolutionSplitRead::CreatePushDownPredicate(
    const std::shared_ptr<Predicate>& predicate,
    const std::shared_ptr<arrow::Schema>& read_schema) {
    std::map<std::string, int32_t> picked_field_name_to_idx;
    for (int32_t i = 0; i < read_schema->num_fields(); ++i) {
        const std::string& field_name = read_schema->field(i)->name();
        if (!SpecialFields::IsSystemField(field_name)) {
            picked_field_name_to_idx.emplace(field_name, i);
        }
    }
    return PredicateUtils::CreatePickedFieldFilter(predicate, picked_field_name_to_idx);
}

Result<std::optional<RoaringBitmap32>> DataEvolutionSplitRead::SelectByFileIndex(
    const std::shared_ptr<Predicate>& predicate,
    const std::vector<std::shared_ptr<DataFileMeta>>& files,
    const std::shared_ptr<arrow::Schema>& read_schema,
    const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    if (row_ranges) {
        std::vector<Range> file_ranges;
        for (const auto& file : files) {
            PAIMON_ASSIGN_OR_RAISE(Range range, file->NonNullRowIdRange());
            file_ranges.push_back(range);
        }
        if (Range::And(Range::SortAndMergeOverlap(*row_ranges, /*adjacent=*/true),
                       Range::SortAndMergeOverlap(file_ranges, /*adjacent=*/true))
                .empty()) {
            return std::optional<RoaringBitmap32>(RoaringBitmap32());
        }
    }
    if (!options_.FileIndexReadEnabled() || !predicate) {
        return std::optional<RoaringBitmap32>();
    }

    // Like Java's canPushDownGroupSelection, only share exact bitmap selections across
    // normal files with the same row-id range. Other groups can still be rejected by indexes.
    bool can_push_down_selection = true;
    if (files.size() > 1) {
        PAIMON_ASSIGN_OR_RAISE(Range group_range, files[0]->NonNullRowIdRange());
        for (const auto& file : files) {
            if (!DataEvolutionUtils::IsNormalFile(file->file_name)) {
                can_push_down_selection = false;
                break;
            }
            PAIMON_ASSIGN_OR_RAISE(Range file_range, file->NonNullRowIdRange());
            if (!(file_range == group_range)) {
                can_push_down_selection = false;
                break;
            }
        }
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FieldMappingBuilder> field_mapping_builder,
        FieldMappingBuilder::Create(read_schema, context_->GetPartitionKeys(), predicate));
    std::optional<RoaringBitmap32> selection;
    std::set<int32_t> claimed_field_ids;
    for (const auto& file : files) {
        // Blob and vector-store files may cover only part of the row range, so their indexes
        // cannot prove that the complete merged group misses the predicate.
        if (files.size() > 1 && !DataEvolutionUtils::IsNormalFile(file->file_name)) {
            continue;
        }

        std::shared_ptr<TableSchema> data_schema = context_->GetTableSchema();
        if (file->schema_id != data_schema->Id()) {
            PAIMON_ASSIGN_OR_RAISE(data_schema, schema_manager_->ReadSchema(file->schema_id));
        }
        std::vector<DataField> written_fields;
        if (file->write_cols) {
            std::vector<std::string> data_write_cols;
            data_write_cols.reserve(file->write_cols->size());
            for (const auto& write_col : file->write_cols.value()) {
                if (!SpecialFields::IsSystemField(write_col)) {
                    data_write_cols.push_back(write_col);
                }
            }
            PAIMON_ASSIGN_OR_RAISE(written_fields, data_schema->GetFields(data_write_cols));
        } else {
            written_fields = data_schema->Fields();
        }

        std::set<std::string> overwritten_field_names;
        for (const auto& field : written_fields) {
            if (!claimed_field_ids.insert(field.Id()).second) {
                overwritten_field_names.insert(field.Name());
            }
        }

        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldMapping> field_mapping,
                               field_mapping_builder->CreateFieldMapping(written_fields));
        std::shared_ptr<Predicate> data_predicate =
            field_mapping->non_partition_info.non_partition_filter;
        if (!overwritten_field_names.empty()) {
            PAIMON_ASSIGN_OR_RAISE(data_predicate, PredicateUtils::ExcludePredicateWithFields(
                                                       data_predicate, overwritten_field_names));
        }
        if (!data_predicate) {
            continue;
        }

        auto written_schema = DataField::ConvertDataFieldsToArrowSchema(written_fields);
        auto field_name_mapping = FieldMappingBuilder::CreateFieldNameMapping(
            context_->GetTableSchema()->Fields(), written_fields);
        std::map<std::string, std::string> file_index_options;
        PAIMON_ASSIGN_OR_RAISE(file_index_options, FileIndexOptions::RewriteFieldNames(
                                                       options_.ToMap(), field_name_mapping));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> index_result,
                               FileIndexEvaluator::Evaluate(written_schema, file_index_options,
                                                            data_predicate, data_file_path_factory,
                                                            file, options_.GetFileSystem(), pool_));
        PAIMON_ASSIGN_OR_RAISE(bool is_remain, index_result->IsRemain());
        if (!is_remain) {
            return std::optional<RoaringBitmap32>(RoaringBitmap32());
        }
        if (!can_push_down_selection) {
            continue;
        }
        if (auto* bitmap_index = dynamic_cast<BitmapIndexResult*>(index_result.get())) {
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* bitmap, bitmap_index->GetBitmap());
            if (selection) {
                *selection &= *bitmap;
            } else {
                selection = *bitmap;
            }
        }
    }
    if (selection && row_ranges) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap32> range_selection,
                               files[0]->ToFileSelection(row_ranges));
        if (range_selection) {
            *selection &= *range_selection;
        }
    }
    return selection;
}

Result<std::unique_ptr<FileBatchReader>> DataEvolutionSplitRead::ApplyIndexAndDvReaderIfNeeded(
    std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& read_schema, const std::shared_ptr<Predicate>& predicate,
    DeletionVector::Factory dv_factory, const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const std::map<std::string, std::string>& file_index_options,
    std::vector<float>* index_scores) const {
    std::shared_ptr<FileIndexResult> file_index_result;
    if (options_.FileIndexReadEnabled() && !file_indexes_evaluated_) {
        PAIMON_ASSIGN_OR_RAISE(file_index_result,
                               FileIndexEvaluator::Evaluate(data_schema, file_index_options,
                                                            predicate, data_file_path_factory, file,
                                                            options_.GetFileSystem(), pool_));
        PAIMON_ASSIGN_OR_RAISE(bool is_remain, file_index_result->IsRemain());
        if (!is_remain) {
            return std::unique_ptr<FileBatchReader>();
        }
    }
    const RoaringBitmap32* index_selection = group_selection_ ? &*group_selection_ : nullptr;
    if (auto* bitmap_index = dynamic_cast<BitmapIndexResult*>(file_index_result.get())) {
        PAIMON_ASSIGN_OR_RAISE(index_selection, bitmap_index->GetBitmap());
    }

    // the factory is per row range group and already returns a view taking file-local positions.
    // Unlike RawFileSplitRead the vector is not folded into the format reader's selection: it is
    // no BitmapDeletionVector, and the blob fallback path's gap segments have no format reader.
    std::shared_ptr<DeletionVector> deletion_vector;
    if (dv_factory) {
        PAIMON_ASSIGN_OR_RAISE(deletion_vector, dv_factory(file->file_name));
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap32> selection_row_ids,
                           file->ToFileSelection(row_ranges));
    if (index_selection) {
        if (selection_row_ids) {
            selection_row_ids.value() &= *index_selection;
        } else {
            selection_row_ids = *index_selection;
        }
    }
    if (selection_row_ids && selection_row_ids->IsEmpty()) {
        return std::unique_ptr<FileBatchReader>();
    }
    ::ArrowSchema c_read_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, &c_read_schema));
    PAIMON_RETURN_NOT_OK(file_reader->SetReadSchema(&c_read_schema, predicate, selection_row_ids));

    std::unique_ptr<FileBatchReader> reader;
    if (!file_reader->SupportPreciseBitmapSelection() && selection_row_ids) {
        // Some formats (for example blob) return an accurate batch result, where
        // ApplyBitmapIndexBatchReader is not necessary
        reader = std::make_unique<ApplyBitmapIndexBatchReader>(
            std::move(file_reader), std::move(selection_row_ids).value());
    } else {
        reader = std::move(file_reader);
    }

    if (deletion_vector && !deletion_vector->IsEmpty()) {
        reader =
            std::make_unique<ApplyDeletionVectorBatchReader>(std::move(reader), deletion_vector);
    }
    return std::move(reader);
}

Result<std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>>>
DataEvolutionSplitRead::SplitFieldBunches(
    const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files,
    const std::function<Result<int32_t>(const std::shared_ptr<DataFileMeta>&)>&
        blob_field_to_field_id,
    bool has_row_ranges_selection) {
    std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> fields_files;
    std::map<int32_t, std::shared_ptr<DataEvolutionSplitRead::BlobBunch>> blob_bunch_map;
    std::optional<Range> expected_row_range;
    for (const auto& file : need_merge_files) {
        if (BlobUtils::IsBlobFile(file->file_name)) {
            PAIMON_ASSIGN_OR_RAISE(int32_t field_id, blob_field_to_field_id(file));
            auto iter = blob_bunch_map.find(field_id);
            if (iter == blob_bunch_map.end()) {
                auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
                    expected_row_range, has_row_ranges_selection);
                PAIMON_RETURN_NOT_OK(blob_bunch->Add(file));
                blob_bunch_map.insert(std::make_pair(field_id, blob_bunch));
            } else {
                assert(iter->second);
                PAIMON_RETURN_NOT_OK(iter->second->Add(file));
            }
        } else {
            // Normal file, just add it to the current merge split
            fields_files.push_back(std::make_shared<DataEvolutionSplitRead::DataBunch>(file));
            if (DataEvolutionUtils::IsNormalFile(file->file_name)) {
                PAIMON_ASSIGN_OR_RAISE(expected_row_range, file->NonNullRowIdRange());
            }
        }
    }
    for (const auto& [field_id, blob_bunch] : blob_bunch_map) {
        fields_files.push_back(blob_bunch);
    }
    return fields_files;
}

Result<std::vector<DataEvolutionSplitRead::VectorReadRange>>
DataEvolutionSplitRead::PlanVectorReadRanges(
    const std::vector<std::shared_ptr<DataFileMeta>>& files,
    const std::vector<int32_t>& read_field_ids,
    const std::function<Result<std::vector<int32_t>>(const std::shared_ptr<DataFileMeta>&)>&
        file_to_field_ids) {
    struct Candidate {
        std::shared_ptr<DataFileMeta> file;
        std::vector<int32_t> field_ids;
    };
    std::vector<Candidate> candidates;
    std::vector<std::shared_ptr<DataFileMeta>> other_files;
    std::map<int64_t, std::vector<size_t>> boundaries;
    std::unordered_set<int32_t> projected_ids(read_field_ids.begin(), read_field_ids.end());
    std::optional<Range> logical_range;
    int64_t first_row_id = std::numeric_limits<int64_t>::max();
    int64_t end_row_id = std::numeric_limits<int64_t>::min();
    bool vector_only = std::all_of(files.begin(), files.end(), [](const auto& file) {
        return VectorStoreUtils::IsVectorStoreFile(file->file_name);
    });
    for (const auto& file : files) {
        PAIMON_ASSIGN_OR_RAISE(int64_t first, file->NonNullFirstRowId());
        if (first < 0 || file->row_count <= 0 ||
            first > std::numeric_limits<int64_t>::max() - file->row_count) {
            return Status::Invalid("Invalid row id range in VECTOR merge group");
        }
        int64_t end = first + file->row_count;
        first_row_id = std::min(first_row_id, first);
        end_row_id = std::max(end_row_id, end);
        if (!VectorStoreUtils::IsVectorStoreFile(file->file_name)) {
            other_files.push_back(file);
            if (DataEvolutionUtils::IsNormalFile(file->file_name)) {
                logical_range = Range(first, end - 1);
            }
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> field_ids, file_to_field_ids(file));
        field_ids.erase(std::remove_if(field_ids.begin(), field_ids.end(),
                                       [&](int32_t id) { return projected_ids.count(id) == 0; }),
                        field_ids.end());
        if (!field_ids.empty() || vector_only) {
            size_t index = candidates.size();
            candidates.push_back(Candidate{file, std::move(field_ids)});
            boundaries[first].push_back(index);
            boundaries[end].push_back(index);
        }
    }
    if (files.empty()) {
        return std::vector<VectorReadRange>();
    }
    if (!logical_range) {
        logical_range = Range(first_row_id, end_row_id - 1);
    }
    boundaries[logical_range->from];
    boundaries[logical_range->to + 1];

    auto newest_first = [&](size_t left, size_t right) {
        const auto& lhs = candidates[left].file;
        const auto& rhs = candidates[right].file;
        if (lhs->max_sequence_number != rhs->max_sequence_number) {
            return lhs->max_sequence_number > rhs->max_sequence_number;
        }
        if (lhs->file_name != rhs->file_name) {
            return lhs->file_name < rhs->file_name;
        }
        return left < right;
    };
    std::set<size_t, decltype(newest_first)> active(newest_first);
    std::vector<VectorReadRange> result;
    int64_t start = boundaries.begin()->first;
    for (const auto& [end, events] : boundaries) {
        int64_t from = std::max(start, logical_range->from);
        int64_t to = std::min(end - 1, logical_range->to);
        if (from <= to) {
            std::unordered_set<int32_t> assigned;
            std::vector<std::shared_ptr<DataFileMeta>> providers = other_files;
            for (size_t index : active) {
                const auto& candidate = candidates[index];
                bool provides_field = false;
                for (int32_t id : candidate.field_ids) {
                    provides_field |= assigned.insert(id).second;
                }
                // An explicit NULL in a newer file overrides the older value as well.
                if (provides_field) {
                    providers.push_back(candidate.file);
                }
            }
            if (providers.empty() && vector_only && !active.empty()) {
                providers.push_back(candidates[*active.begin()].file);
            }
            // Ordinary inline fields and dedicated fields share the same sequence precedence.
            // BLOB files remain last and retain their own per-row fallback ordering.
            std::stable_sort(
                providers.begin(), providers.end(), [](const auto& lhs, const auto& rhs) {
                    bool lhs_blob = BlobUtils::IsBlobFile(lhs->file_name);
                    bool rhs_blob = BlobUtils::IsBlobFile(rhs->file_name);
                    if (lhs_blob != rhs_blob) {
                        return !lhs_blob;
                    }
                    return !lhs_blob && lhs->max_sequence_number > rhs->max_sequence_number;
                });
            if (!result.empty() && result.back().range.to + 1 == from &&
                result.back().files == providers) {
                result.back().range.to = to;
            } else {
                result.push_back(VectorReadRange{Range(from, to), std::move(providers)});
            }
        }
        for (size_t index : events) {
            if (active.erase(index) == 0) {
                active.insert(index);
            }
        }
        start = end;
    }
    return result;
}

Result<std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>>>
DataEvolutionSplitRead::TryPlanSequentialVectorRead(
    const std::vector<std::shared_ptr<DataFileMeta>>& files,
    const std::function<Result<std::vector<int32_t>>(const std::shared_ptr<DataFileMeta>&)>&
        file_to_field_ids) {
    std::vector<std::shared_ptr<FieldBunch>> no_match;
    std::shared_ptr<DataFileMeta> main_file;
    std::vector<std::shared_ptr<DataFileMeta>> vector_files;
    for (const auto& file : files) {
        if (VectorStoreUtils::IsVectorStoreFile(file->file_name)) {
            vector_files.push_back(file);
        } else if (DataEvolutionUtils::IsNormalFile(file->file_name) && !main_file) {
            main_file = file;
        } else {
            return no_match;
        }
    }
    if (!main_file || vector_files.empty()) {
        return no_match;
    }
    for (const auto& file : files) {
        PAIMON_ASSIGN_OR_RAISE(int64_t first, file->NonNullFirstRowId());
        if (first < 0 || file->row_count <= 0 ||
            first > std::numeric_limits<int64_t>::max() - file->row_count) {
            return Status::Invalid("Invalid row id range in VECTOR merge group");
        }
    }
    std::sort(vector_files.begin(), vector_files.end(), [](const auto& left, const auto& right) {
        return left->first_row_id.value() < right->first_row_id.value();
    });
    const auto& first_vector = vector_files[0];
    if (!first_vector->write_cols || first_vector->write_cols->size() != 1) {
        return no_match;
    }
    PAIMON_ASSIGN_OR_RAISE(std::string format, first_vector->FileFormat());
    int64_t next_row_id = main_file->first_row_id.value();
    for (const auto& file : vector_files) {
        PAIMON_ASSIGN_OR_RAISE(std::string current_format, file->FileFormat());
        if (file->schema_id != first_vector->schema_id || current_format != format ||
            file->write_cols != first_vector->write_cols || file->first_row_id != next_row_id) {
            return no_match;
        }
        next_row_id += file->row_count;
    }
    if (next_row_id != main_file->first_row_id.value() + main_file->row_count) {
        return no_match;
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> vector_ids, file_to_field_ids(first_vector));
    PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> main_ids, file_to_field_ids(main_file));
    // An inline VECTOR in a historical main file can have different precedence in each range.
    if (vector_ids.size() != 1 ||
        std::find(main_ids.begin(), main_ids.end(), vector_ids[0]) != main_ids.end()) {
        return no_match;
    }
    return std::vector<std::shared_ptr<FieldBunch>>{
        std::make_shared<DataBunch>(main_file),
        std::make_shared<VectorBunch>(std::move(vector_files), main_file->row_count)};
}

Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::CreateUnionReader(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files,
    const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const DeletionVector::Factory& group_dv_factory,
    const std::optional<GroupDeletionVector>& group_dv,
    const std::shared_ptr<arrow::Schema>& read_schema) const {
    if (std::none_of(need_merge_files.begin(), need_merge_files.end(), [](const auto& file) {
            return VectorStoreUtils::IsVectorStoreFile(file->file_name);
        })) {
        return CreateFieldUnionReader(partition, need_merge_files, row_ranges,
                                      data_file_path_factory, group_dv_factory, group_dv,
                                      read_schema);
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> read_fields,
                           DataField::ConvertArrowSchemaToDataFields(read_schema));
    std::vector<int32_t> read_field_ids;
    for (const auto& field : read_fields) {
        if (!SpecialFields::IsSystemField(field.Name())) {
            read_field_ids.push_back(field.Id());
        }
    }
    auto file_to_field_ids =
        [&](const std::shared_ptr<DataFileMeta>& file) -> Result<std::vector<int32_t>> {
        if (VectorStoreUtils::IsVectorStoreFile(file->file_name) && !file->write_cols) {
            return Status::Invalid("VECTOR files must declare their write columns");
        }
        auto data_schema = context_->GetTableSchema();
        if (file->schema_id != data_schema->Id()) {
            PAIMON_ASSIGN_OR_RAISE(data_schema, schema_manager_->ReadSchema(file->schema_id));
        }
        if (!file->write_cols) {
            return DataField::GetAllFieldIds(data_schema->Fields());
        }
        std::vector<int32_t> ids;
        for (const auto& name : file->write_cols.value()) {
            if (SpecialFields::IsSystemField(name)) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField field, data_schema->GetField(name));
            ids.push_back(field.Id());
        }
        return ids;
    };
    std::optional<std::vector<Range>> normalized_ranges;
    if (row_ranges) {
        normalized_ranges = Range::SortAndMergeOverlap(row_ranges.value(), /*adjacent=*/true);
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<FieldBunch>> sequential_bunches,
                           TryPlanSequentialVectorRead(need_merge_files, file_to_field_ids));
    auto split_read =
        std::make_shared<DataEvolutionSplitRead>(path_factory_, context_, pool_, executor_);
    split_read->file_indexes_evaluated_ = file_indexes_evaluated_;
    split_read->group_selection_ = group_selection_;
    if (!sequential_bunches.empty()) {
        std::vector<LazyConcatBatchReader::Source> sources;
        sources.push_back(LazyConcatBatchReader::Source{
            sequential_bunches[0]->Files()[0]->file_name,
            [split_read, partition, sequential_bunches = std::move(sequential_bunches),
             normalized_ranges, data_file_path_factory, group_dv_factory, group_dv,
             read_schema]() -> Result<std::unique_ptr<BatchReader>> {
                return split_read->CreateFieldUnionReader(partition, sequential_bunches,
                                                          normalized_ranges, data_file_path_factory,
                                                          group_dv_factory, group_dv, read_schema);
            }});
        return std::make_unique<LazyConcatBatchReader>(std::move(sources), arrow_pool_);
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<VectorReadRange> ranges,
        PlanVectorReadRanges(need_merge_files, read_field_ids, file_to_field_ids));
    std::vector<LazyConcatBatchReader::Source> sources;
    for (const auto& range : ranges) {
        std::vector<Range> selected = {range.range};
        if (normalized_ranges) {
            selected = Range::And(selected, normalized_ranges.value());
        }
        if (selected.empty() || range.files.empty()) {
            continue;
        }
        // Keep original file metadata and the group's DV; selection uses absolute row ids.
        // Open one range at a time, so independently rolled files do not multiply open readers.
        sources.push_back(LazyConcatBatchReader::Source{
            range.range.ToString(),
            [split_read, partition, files = range.files, selected = std::move(selected),
             data_file_path_factory, group_dv_factory, group_dv,
             read_schema]() -> Result<std::unique_ptr<BatchReader>> {
                return split_read->CreateFieldUnionReader(partition, files, selected,
                                                          data_file_path_factory, group_dv_factory,
                                                          group_dv, read_schema);
            }});
    }
    return std::make_unique<LazyConcatBatchReader>(std::move(sources), arrow_pool_);
}

Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::CreateFieldUnionReader(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files,
    const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const DeletionVector::Factory& group_dv_factory,
    const std::optional<GroupDeletionVector>& group_dv,
    const std::shared_ptr<arrow::Schema>& read_schema) const {
    auto blob_field_to_field_id =
        [&](const std::shared_ptr<DataFileMeta>& file_meta) -> Result<int32_t> {
        if (!BlobUtils::IsBlobFile(file_meta->file_name)) {
            return Status::Invalid("Only blob file need to call this method.");
        }
        auto data_schema = context_->GetTableSchema();
        if (data_schema->Id() != file_meta->schema_id) {
            PAIMON_ASSIGN_OR_RAISE(data_schema, schema_manager_->ReadSchema(file_meta->schema_id));
        }
        const auto& write_cols = file_meta->write_cols;
        if (write_cols == std::nullopt || write_cols.value().size() != 1) {
            return Status::Invalid("Blob file only has one blob field");
        }
        PAIMON_ASSIGN_OR_RAISE(DataField data_field, data_schema->GetField(write_cols.value()[0]));
        return data_field.Id();
    };

    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> fields_files,
        SplitFieldBunches(need_merge_files, blob_field_to_field_id,
                          /*has_row_ranges_selection=*/row_ranges.has_value()));

    return CreateFieldUnionReader(partition, fields_files, row_ranges, data_file_path_factory,
                                  group_dv_factory, group_dv, read_schema);
}

Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::CreateFieldUnionReader(
    const BinaryRow& partition, const std::vector<std::shared_ptr<FieldBunch>>& fields_files,
    const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const DeletionVector::Factory& group_dv_factory,
    const std::optional<GroupDeletionVector>& group_dv,
    const std::shared_ptr<arrow::Schema>& read_schema) const {
    assert(!fields_files.empty());
    auto bunch_first_row_id = [](const std::shared_ptr<FieldBunch>& bunch) -> Result<int64_t> {
        if (auto blob_bunch = std::dynamic_pointer_cast<BlobBunch>(bunch)) {
            PAIMON_ASSIGN_OR_RAISE(Range range, blob_bunch->LogicalRange());
            return range.from;
        }
        return bunch->Files()[0]->NonNullFirstRowId();
    };
    PAIMON_ASSIGN_OR_RAISE(int64_t row_count, fields_files[0]->RowCount());
    PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, bunch_first_row_id(fields_files[0]));
    if (!row_ranges) {
        for (const auto& bunch : fields_files) {
            PAIMON_ASSIGN_OR_RAISE(int64_t bunch_row_count, bunch->RowCount());
            if (bunch_row_count != row_count) {
                return Status::Invalid(
                    "All files in a field merge split should have the same row count.");
            }
            PAIMON_ASSIGN_OR_RAISE(int64_t current_bunch_first_row_id, bunch_first_row_id(bunch));
            if (current_bunch_first_row_id != first_row_id) {
                return Status::Invalid(
                    "All files in a field merge split should have the same first row id and could "
                    "not be null.");
            }
        }
    }

    // Init all we need to create a compound reader
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> all_read_fields,
                           DataField::ConvertArrowSchemaToDataFields(read_schema));
    std::vector<std::unique_ptr<BatchReader>> file_batch_readers(fields_files.size());
    std::vector<int32_t> read_field_ids = DataField::GetAllFieldIds(all_read_fields);
    // which row the read field index belongs to
    std::vector<int32_t> reader_offsets(all_read_fields.size(), -1);
    // which field index in the reading row
    std::vector<int32_t> field_offsets(all_read_fields.size(), -1);
    const bool has_normal_file =
        std::any_of(fields_files.begin(), fields_files.end(), [](const auto& bunch) {
            return DataEvolutionUtils::IsNormalFile(bunch->Files()[0]->file_name);
        });

    for (int32_t file_idx = 0; file_idx < static_cast<int32_t>(fields_files.size()); file_idx++) {
        const auto& bunch = fields_files[file_idx];
        const auto& first_file = bunch->Files()[0];
        int64_t schema_id = first_file->schema_id;
        std::shared_ptr<TableSchema> data_schema = context_->GetTableSchema();
        if (schema_id != data_schema->Id()) {
            PAIMON_ASSIGN_OR_RAISE(data_schema, schema_manager_->ReadSchema(schema_id));
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<DataField> data_fields,
            ProjectFieldsForRowTrackingAndDataEvolution(data_schema, first_file->write_cols));
        std::vector<int32_t> data_field_ids = DataField::GetAllFieldIds(data_fields);
        std::vector<DataField> read_fields_in_file;
        for (int32_t read_field_idx = 0;
             read_field_idx < static_cast<int32_t>(read_field_ids.size()); read_field_idx++) {
            // -1 indicates that the read fields are not matched
            if (reader_offsets[read_field_idx] != -1) {
                continue;
            }
            // Java takes sequence numbers from normal files before dedicated column files.
            if (has_normal_file && !DataEvolutionUtils::IsNormalFile(first_file->file_name) &&
                read_field_ids[read_field_idx] == SpecialFields::SequenceNumber().Id()) {
                continue;
            }
            for (int32_t data_field_id : data_field_ids) {
                // Check if the read field index matches the file field
                // index
                if (data_field_id == read_field_ids[read_field_idx]) {
                    // "file_idx" is the reader index, and "read_fields_in_file.size()"
                    // is the offset the that row
                    reader_offsets[read_field_idx] = file_idx;
                    field_offsets[read_field_idx] = read_fields_in_file.size();
                    read_fields_in_file.push_back(all_read_fields[read_field_idx]);
                    break;
                }
            }
        }

        if (!read_fields_in_file.empty()) {
            // create new FieldMappingReader for read partial fields
            auto file_read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields_in_file);
            auto blob_bunch = std::dynamic_pointer_cast<BlobBunch>(bunch);
            if (std::dynamic_pointer_cast<VectorBunch>(bunch)) {
                std::vector<LazyConcatBatchReader::Source> sources;
                auto split_read = std::make_shared<DataEvolutionSplitRead>(path_factory_, context_,
                                                                           pool_, executor_);
                split_read->file_indexes_evaluated_ = file_indexes_evaluated_;
                split_read->group_selection_ = group_selection_;
                for (const auto& file : bunch->Files()) {
                    std::optional<std::vector<Range>> selected = row_ranges;
                    if (row_ranges) {
                        PAIMON_ASSIGN_OR_RAISE(int64_t first, file->NonNullFirstRowId());
                        selected = Range::And({Range(first, first + file->row_count - 1)},
                                              row_ranges.value());
                        if (selected->empty()) {
                            continue;
                        }
                    }
                    sources.push_back(LazyConcatBatchReader::Source{
                        file->file_name,
                        [split_read, partition, file, file_read_schema, selected,
                         data_file_path_factory,
                         group_dv_factory]() -> Result<std::unique_ptr<BatchReader>> {
                            PAIMON_ASSIGN_OR_RAISE(
                                std::vector<std::unique_ptr<FileBatchReader>> readers,
                                split_read->CreateRawFileReaders(
                                    partition, {file}, file_read_schema, /*predicate=*/nullptr,
                                    group_dv_factory, selected, data_file_path_factory,
                                    /*extra_format_options=*/{}));
                            return std::make_unique<ConcatBatchReader>(
                                ObjectUtils::MoveVector<std::unique_ptr<BatchReader>>(
                                    std::move(readers)),
                                split_read->arrow_pool_);
                        }});
                }
                file_batch_readers[file_idx] =
                    std::make_unique<LazyConcatBatchReader>(std::move(sources), arrow_pool_);
            } else if (blob_bunch) {
                PAIMON_ASSIGN_OR_RAISE(Range logical_range, blob_bunch->LogicalRange());
                PAIMON_ASSIGN_OR_RAISE(
                    file_batch_readers[file_idx],
                    CreateBlobFallbackReader(partition, blob_bunch->Files(), logical_range,
                                             file_read_schema, row_ranges, data_file_path_factory,
                                             group_dv_factory, group_dv));
            } else {
                PAIMON_ASSIGN_OR_RAISE(
                    std::vector<std::unique_ptr<FileBatchReader>> file_readers,
                    CreateRawFileReaders(partition, bunch->Files(), file_read_schema,
                                         /*predicate=*/nullptr, group_dv_factory, row_ranges,
                                         data_file_path_factory,
                                         /*extra_format_options=*/{}));
                if (file_readers.size() == 1) {
                    file_batch_readers[file_idx] = std::move(file_readers[0]);
                } else {
                    auto raw_readers = ObjectUtils::MoveVector<std::unique_ptr<BatchReader>>(
                        std::move(file_readers));
                    file_batch_readers[file_idx] =
                        std::make_unique<ConcatBatchReader>(std::move(raw_readers), arrow_pool_);
                }
            }
        }
    }
    if (std::none_of(file_batch_readers.begin(), file_batch_readers.end(),
                     [](const auto& reader) { return reader != nullptr; })) {
        // A projected VECTOR can be absent in this range. Let schema evolution fill NULLs,
        // using an original file as the row-count provider even when no field is projected.
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::unique_ptr<FileBatchReader>> file_readers,
            CreateRawFileReaders(partition, fields_files[0]->Files(), read_schema,
                                 /*predicate=*/nullptr, group_dv_factory, row_ranges,
                                 data_file_path_factory, /*extra_format_options=*/{}));
        auto readers =
            ObjectUtils::MoveVector<std::unique_ptr<BatchReader>>(std::move(file_readers));
        return std::make_unique<ConcatBatchReader>(std::move(readers), arrow_pool_);
    }
    // TODO(xinyu.lxy): check nullable when reader_offsets[read_field_idx] = -1
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BatchReader> reader,
        DataEvolutionFileReader::Create(std::move(file_batch_readers), read_schema,
                                        options_.GetReadBatchSize(), reader_offsets, field_offsets,
                                        arrow_pool_));
    return reader;
}

namespace {
/// Selected row ids in [from, to] as sorted disjoint ranges: the whole range without a
/// selection, otherwise its intersection with the (possibly overlapping) selected ranges.
std::vector<Range> SelectedRangesInRange(int64_t from, int64_t to,
                                         const std::optional<std::vector<Range>>& row_ranges) {
    if (!row_ranges) {
        return {Range(from, to)};
    }
    std::vector<Range> selected;
    Range gap_range(from, to);
    for (const auto& range : Range::SortAndMergeOverlap(row_ranges.value(), /*adjacent=*/true)) {
        std::optional<Range> intersection = Range::Intersection(gap_range, range);
        if (intersection) {
            selected.push_back(*intersection);
        }
    }
    return selected;
}
}  // namespace

Result<std::unique_ptr<BatchReader>> DataEvolutionSplitRead::CreateBlobFallbackReader(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& files,
    const Range& logical_range, const std::shared_ptr<arrow::Schema>& file_read_schema,
    const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const DeletionVector::Factory& group_dv_factory,
    const std::optional<GroupDeletionVector>& group_dv) const {
    if (files.empty()) {
        return Status::Invalid("Blob bunch should not be empty.");
    }
    int64_t union_first_row_id = logical_range.from;
    int64_t union_last_row_id = logical_range.to;
    using FileWithFirstRowId = std::pair<int64_t, std::shared_ptr<DataFileMeta>>;
    std::map<int64_t, std::vector<FileWithFirstRowId>, std::greater<>> sequence_groups;
    for (const auto& file : files) {
        PAIMON_ASSIGN_OR_RAISE(Range file_range, file->NonNullRowIdRange());
        if (!row_ranges &&
            (file_range.from < logical_range.from || file_range.to > logical_range.to)) {
            return Status::Invalid("Blob file range should be within logical range.");
        }
        if (row_ranges && !Range::HasIntersection(file_range, logical_range)) {
            return Status::Invalid("Blob file range should intersect logical range.");
        }
        sequence_groups[file->max_sequence_number].emplace_back(file_range.from, file);
    }
    // the blob format reader must emit placeholder sentinels instead of failing on them
    const std::map<std::string, std::string> blob_format_options = {
        {BlobDefs::kEmitPlaceholderSentinelKey, "true"}};
    std::vector<std::vector<BlobFallbackBatchReader::Segment>> groups;
    groups.reserve(sequence_groups.size());
    for (auto& [max_sequence_number, group_files] : sequence_groups) {
        std::stable_sort(group_files.begin(), group_files.end(),
                         [](const FileWithFirstRowId& f1, const FileWithFirstRowId& f2) {
                             return f1.first < f2.first;
                         });
        int64_t previous_file_end = std::numeric_limits<int64_t>::min();
        for (const auto& [first_row_id, file] : group_files) {
            if (first_row_id < previous_file_end) {
                return Status::Invalid(fmt::format(
                    "Blob files with the same max sequence number should not have overlapping "
                    "row id ranges: file {} (max sequence number {}) starts at row id {} before "
                    "the previous file's end {}",
                    file->file_name, max_sequence_number, first_row_id, previous_file_end));
            }
            previous_file_end = first_row_id + file->row_count;
        }
        std::vector<BlobFallbackBatchReader::Segment> segments;
        // Pad rows this layer does not cover, so that every layer spans the logical range.
        int64_t next_row_id = union_first_row_id;
        for (const auto& [first_row_id, file] : group_files) {
            if (first_row_id > next_row_id && next_row_id <= union_last_row_id) {
                std::vector<Range> gap_selected_ranges = SelectedRangesInRange(
                    next_row_id, std::min(first_row_id - 1, union_last_row_id), row_ranges);
                if (group_dv && !gap_selected_ranges.empty()) {
                    // gap segments have no file reader to wrap, so the deleted row ids are
                    // dropped from their ranges to stay aligned with the other layers
                    PAIMON_ASSIGN_OR_RAISE(
                        gap_selected_ranges,
                        ExcludeDeletedRowIds(gap_selected_ranges, group_dv.value()));
                }
                if (!gap_selected_ranges.empty()) {
                    segments.push_back(
                        BlobFallbackBatchReader::Segment{nullptr, std::move(gap_selected_ranges)});
                }
            }
            std::optional<std::vector<Range>> file_ranges =
                SelectedRangesInRange(union_first_row_id, union_last_row_id, row_ranges);
            if (!SelectedRangesInRange(first_row_id, first_row_id + file->row_count - 1,
                                       file_ranges)
                     .empty()) {
                PAIMON_ASSIGN_OR_RAISE(std::vector<RawFileReaderWithMeta> file_readers,
                                       CreateRawFileReadersWithMeta(
                                           partition, {file}, file_read_schema,
                                           /*predicate=*/nullptr, group_dv_factory, file_ranges,
                                           data_file_path_factory, blob_format_options));
                if (file_readers.size() != 1) {
                    return Status::Invalid("Unexpected: blob fallback file reader was skipped.");
                }
                segments.push_back(
                    BlobFallbackBatchReader::Segment{std::move(file_readers[0].reader),
                                                     {},
                                                     std::move(file_readers[0].blob_context)});
            }
            next_row_id = std::max(next_row_id, first_row_id + file->row_count);
        }
        if (next_row_id <= union_last_row_id) {
            std::vector<Range> gap_selected_ranges =
                SelectedRangesInRange(next_row_id, union_last_row_id, row_ranges);
            if (group_dv && !gap_selected_ranges.empty()) {
                PAIMON_ASSIGN_OR_RAISE(gap_selected_ranges,
                                       ExcludeDeletedRowIds(gap_selected_ranges, group_dv.value()));
            }
            if (!gap_selected_ranges.empty()) {
                segments.push_back(
                    BlobFallbackBatchReader::Segment{nullptr, std::move(gap_selected_ranges)});
            }
        }
        if (segments.empty()) {
            return std::make_unique<ConcatBatchReader>(std::vector<std::unique_ptr<BatchReader>>(),
                                                       arrow_pool_);
        }
        groups.push_back(std::move(segments));
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BlobFallbackBatchReader> fallback_reader,
        BlobFallbackBatchReader::Create(std::move(groups), file_read_schema,
                                        options_.GetReadBatchSize(), arrow_pool_));
    return std::move(fallback_reader);
}

Result<bool> DataEvolutionSplitRead::Match(const std::shared_ptr<Split>& split,
                                           bool force_keep_delete) const {
    return true;
}

Result<std::vector<std::vector<std::shared_ptr<DataFileMeta>>>>
DataEvolutionSplitRead::MergeRangesAndSort(std::vector<std::shared_ptr<DataFileMeta>>&& files) {
    // group by row id range
    RangeHelper<std::shared_ptr<DataFileMeta>> range_helper(
        [](const std::shared_ptr<DataFileMeta>& meta) -> Result<int64_t> {
            return meta->NonNullFirstRowId();
        },
        [](const std::shared_ptr<DataFileMeta>& meta) -> Result<int64_t> {
            PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, meta->NonNullFirstRowId());
            return first_row_id + meta->row_count - 1;
        });

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> result,
                           range_helper.MergeOverlappingRanges(std::move(files)));
    // Keep ordinary files separate from independently rolled dedicated files.
    for (auto& group : result) {
        // split to data files and blob files
        std::vector<std::shared_ptr<DataFileMeta>> data_files;
        data_files.reserve(group.size());
        std::vector<std::shared_ptr<DataFileMeta>> blob_files;
        blob_files.reserve(group.size());
        std::vector<std::shared_ptr<DataFileMeta>> vector_files;
        vector_files.reserve(group.size());
        for (auto& meta : group) {
            if (BlobUtils::IsBlobFile(meta->file_name)) {
                blob_files.push_back(std::move(meta));
            } else if (VectorStoreUtils::IsVectorStoreFile(meta->file_name)) {
                vector_files.push_back(std::move(meta));
            } else {
                data_files.push_back(std::move(meta));
            }
        }

        // data files sort by reversed max sequence number
        std::stable_sort(
            data_files.begin(), data_files.end(),
            [](const std::shared_ptr<DataFileMeta>& m1, const std::shared_ptr<DataFileMeta>& m2) {
                return m1->max_sequence_number > m2->max_sequence_number;
            });

        PAIMON_ASSIGN_OR_RAISE(bool all_range_same, range_helper.AreAllRangesSame(data_files));
        if (!all_range_same) {
            return Status::Invalid("Data files should be all row id ranges same.");
        }

        // blob files sort by first row id then by reversed max sequence number
        std::stable_sort(
            blob_files.begin(), blob_files.end(),
            [](const std::shared_ptr<DataFileMeta>& m1, const std::shared_ptr<DataFileMeta>& m2) {
                if (m1->first_row_id.value() != m2->first_row_id.value()) {
                    return m1->first_row_id.value() < m2->first_row_id.value();
                }
                return m1->max_sequence_number > m2->max_sequence_number;
            });
        std::stable_sort(vector_files.begin(), vector_files.end(),
                         [](const auto& lhs, const auto& rhs) {
                             if (lhs->first_row_id != rhs->first_row_id) {
                                 return lhs->first_row_id < rhs->first_row_id;
                             }
                             return lhs->max_sequence_number > rhs->max_sequence_number;
                         });
        group.clear();
        group.insert(group.end(), std::make_move_iterator(data_files.begin()),
                     std::make_move_iterator(data_files.end()));
        group.insert(group.end(), std::make_move_iterator(vector_files.begin()),
                     std::make_move_iterator(vector_files.end()));
        group.insert(group.end(), std::make_move_iterator(blob_files.begin()),
                     std::make_move_iterator(blob_files.end()));
    }
    return result;
}

DeletionVector::Factory DataEvolutionSplitRead::CreateSplitDvFactory(
    const DataSplitImpl& split_impl) const {
    const std::vector<std::optional<DeletionFile>>& deletion_files = split_impl.DeletionFiles();
    bool has_deletion_files =
        std::any_of(deletion_files.begin(), deletion_files.end(),
                    [](const std::optional<DeletionFile>& file) { return file != std::nullopt; });
    if (!has_deletion_files) {
        return DeletionVector::Factory();
    }
    return DeletionVector::CreateFactory(
        options_.GetFileSystem(),
        DeletionVector::CreateDeletionFileMap(split_impl.DataFiles(), deletion_files), pool_);
}

Result<std::optional<DataEvolutionSplitRead::GroupDeletionVector>>
DataEvolutionSplitRead::ReadGroupDeletionVector(
    const std::vector<std::shared_ptr<DataFileMeta>>& group,
    const DeletionVector::Factory& split_dv_factory) {
    if (!split_dv_factory) {
        return std::optional<GroupDeletionVector>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> anchor,
                           DataEvolutionUtils::RetrieveAnchorFile(group));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DeletionVector> deletion_vector,
                           split_dv_factory(anchor->file_name));
    if (!deletion_vector || deletion_vector->IsEmpty()) {
        return std::optional<GroupDeletionVector>();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t anchor_first_row_id, anchor->NonNullFirstRowId());
    return std::make_optional(GroupDeletionVector{
        deletion_vector, Range(anchor_first_row_id, anchor_first_row_id + anchor->row_count - 1)});
}

Result<DeletionVector::Factory> DataEvolutionSplitRead::CreateGroupDvFactory(
    const std::vector<std::shared_ptr<DataFileMeta>>& group,
    const std::optional<GroupDeletionVector>& group_dv,
    const std::optional<std::vector<Range>>& row_ranges) {
    if (!group_dv) {
        return DeletionVector::Factory();
    }
    auto shifted_dvs =
        std::make_shared<std::unordered_map<std::string, std::shared_ptr<DeletionVector>>>();
    for (const auto& file : group) {
        PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file->NonNullFirstRowId());
        int64_t last_row_id = first_row_id + file->row_count - 1;
        const std::vector<Range> selected_ranges =
            row_ranges ? Range::And({Range(first_row_id, last_row_id)},
                                    Range::SortAndMergeOverlap(*row_ranges, /*adjacent=*/true))
                       : std::vector<Range>{Range(first_row_id, last_row_id)};
        for (const auto& range : selected_ranges) {
            if (range.from < group_dv->anchor_range.from || range.to > group_dv->anchor_range.to) {
                return Status::Invalid(fmt::format(
                    "Deletion vector range [{}, {}] should contain selected row id range [{}, {}] "
                    "of file {}.",
                    group_dv->anchor_range.from, group_dv->anchor_range.to, range.from, range.to,
                    file->file_name));
            }
        }
        int64_t offset = first_row_id - group_dv->anchor_range.from;
        bool covers_whole_anchor_range = offset == 0 && last_row_id == group_dv->anchor_range.to;
        (*shifted_dvs)[file->file_name] =
            covers_whole_anchor_range ? group_dv->deletion_vector
                                      : std::make_shared<PositionShiftedDeletionVector>(
                                            group_dv->deletion_vector, offset, file->row_count);
    }
    return DeletionVector::Factory(
        [shifted_dvs](const std::string& file_name) -> Result<std::shared_ptr<DeletionVector>> {
            auto iter = shifted_dvs->find(file_name);
            if (iter == shifted_dvs->end()) {
                return std::shared_ptr<DeletionVector>();
            }
            return iter->second;
        });
}

Result<RoaringBitmap32> DataEvolutionSplitRead::ExcludeDeletedPositions(
    RoaringBitmap32 selection, const std::shared_ptr<DeletionVector>& deletion_vector) {
    if (selection.IsEmpty() || deletion_vector->IsEmpty()) {
        return selection;
    }
    if (auto* bitmap_dv = dynamic_cast<BitmapDeletionVector*>(deletion_vector.get())) {
        selection -= *bitmap_dv->GetBitmap();
        return selection;
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t cardinality, deletion_vector->GetCardinality());
    if (cardinality < selection.Cardinality()) {
        RoaringBitmap32 deleted;
        PAIMON_RETURN_NOT_OK(deletion_vector->ForEachDeletedPosition([&](int64_t position) {
            if (position >= 0 && position <= std::numeric_limits<uint32_t>::max()) {
                deleted.Add(static_cast<int32_t>(position));
            }
        }));
        selection -= deleted;
        return selection;
    }
    RoaringBitmap32 live;
    const auto end = selection.End();
    for (auto iter = selection.Begin(); iter != end; ++iter) {
        PAIMON_ASSIGN_OR_RAISE(bool is_deleted,
                               deletion_vector->IsDeleted(static_cast<uint32_t>(*iter)));
        if (!is_deleted) {
            live.Add(*iter);
        }
    }
    return live;
}

Result<std::vector<Range>> DataEvolutionSplitRead::ExcludeDeletedRowIds(
    const std::vector<Range>& ranges, const GroupDeletionVector& group_dv) {
    if (ranges.empty()) {
        return std::vector<Range>();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t cardinality, group_dv.deletion_vector->GetCardinality());
    int64_t selected_count = 0;
    for (const auto& range : ranges) {
        if (range.Count() > cardinality - selected_count) {
            // Sparse deletions in a large selection: visit deleted positions instead of every
            // selected row. Coalesce runs before taking the complement to keep memory bounded
            // for contiguous deletions.
            std::vector<Range> deleted;
            PAIMON_RETURN_NOT_OK(
                group_dv.deletion_vector->ForEachDeletedPosition([&](int64_t position) {
                    if (position < 0 ||
                        position > group_dv.anchor_range.to - group_dv.anchor_range.from) {
                        return;
                    }
                    int64_t row_id = group_dv.anchor_range.from + position;
                    // The caller supplies sorted, disjoint selected gap ranges. Do not expand
                    // deletions elsewhere in the anchor into temporary ranges.
                    auto selected = std::lower_bound(
                        ranges.begin(), ranges.end(), row_id,
                        [](const Range& range, int64_t value) { return range.to < value; });
                    if (selected == ranges.end() || selected->from > row_id) {
                        return;
                    }
                    if (!deleted.empty() && deleted.back().to < row_id &&
                        row_id - deleted.back().to == 1) {
                        deleted.back().to = row_id;
                    } else {
                        deleted.emplace_back(row_id, row_id);
                    }
                }));
            if (deleted.empty()) {
                return ranges;
            }
            return Range::And(ranges,
                              Range(ranges.front().from, ranges.back().to).Exclude(deleted));
        }
        selected_count += range.Count();
    }
    std::vector<Range> remaining;
    for (const auto& range : ranges) {
        std::optional<int64_t> run_start;
        for (int64_t row_id = range.from; row_id <= range.to; row_id++) {
            PAIMON_ASSIGN_OR_RAISE(bool is_deleted, group_dv.deletion_vector->IsDeleted(
                                                        row_id - group_dv.anchor_range.from));
            if (is_deleted) {
                if (run_start) {
                    remaining.emplace_back(run_start.value(), row_id - 1);
                    run_start = std::nullopt;
                }
            } else if (!run_start) {
                run_start = row_id;
            }
        }
        if (run_start) {
            remaining.emplace_back(run_start.value(), range.to);
        }
    }
    return remaining;
}
}  // namespace paimon
