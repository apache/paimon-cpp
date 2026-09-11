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

#include "paimon/common/reader/late_materializing_file_batch_reader.h"

#include <cassert>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/concatenate.h"
#include "arrow/array/util.h"
#include "arrow/c/bridge.h"
#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/status.h"

namespace paimon {

Result<std::unique_ptr<LateMaterializingFileBatchReader>> LateMaterializingFileBatchReader::Create(
    std::unique_ptr<FileBatchReader> inner, const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
    ProbeValidation validation) {
    if (arrow_pool == nullptr) {
        return Status::Invalid("arrow pool could not be nullptr.");
    }
    if (inner == nullptr) {
        return Status::Invalid("inner could not be nullptr.");
    }
    auto* prefetch_inner = dynamic_cast<PrefetchFileBatchReader*>(inner.get());
    auto reader =
        std::unique_ptr<LateMaterializingFileBatchReader>(new LateMaterializingFileBatchReader(
            std::move(inner), prefetch_inner, arrow_pool, std::move(validation)));
    return reader;
}

Result<FileBatchReader::ReadBatch> LateMaterializingFileBatchReader::NextBatch() {
    if (state_ == kInit) {
        // SetReadSchema has not been called: read with the file schema, matching the
        // FileBatchReader contract for schema-less reads.
        if (validation_.validate) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ArrowSchema> schema, inner_->GetFileSchema());
            PAIMON_RETURN_NOT_OK(SetReadSchema(schema.get(), nullptr, std::nullopt));
        } else {
            state_ = kNoLatMat;
        }
    }
    if (state_ == kProbing) {
        PAIMON_RETURN_NOT_OK(ReadAndFilterProbeData());
        if (!row_ids_fit_) {
            // File-level selection is limited to uint32_t. Reread full rows and filter each
            // batch locally, preserving uint64_t physical row IDs without truncation.
            matched_bitmap_ = RoaringBitmap32();
            PAIMON_ASSIGN_OR_RAISE(full_filter_, BindFilter(full_schema_));
            PAIMON_RETURN_NOT_OK(SetInnerReadSchema(full_schema_, nullptr, std::nullopt));
            state_ = kFiltering;
        } else if (matched_bitmap_.IsEmpty()) {
            state_ = kEOF;
        } else {
            // payload pass reads only the matched rows (matched_bitmap_ is non-empty here).
            PAIMON_RETURN_NOT_OK(
                SetInnerReadSchema(payload_schema_, /*predicate=*/nullptr, matched_bitmap_));
            state_ = kRunning;
        }
    }

    if (state_ == kNoLatMat) {
        return inner_->NextBatch();
    } else if (state_ == kRunning) {
        return ReadPayloadBatch();
    } else if (state_ == kFiltering) {
        return ReadFilteredBatch();
    } else if (state_ == kEOF) {
        return MakeEofBatch();
    }
    return Status::Invalid("invalid state when calling NextBatch: " + std::to_string(state_));
}

Result<RoaringBitmap32> LateMaterializingFileBatchReader::FilterProbeBatch(
    const std::shared_ptr<arrow::Array>& array,
    const std::shared_ptr<PredicateFilter>& bound_filter) {
    // TODO(zhouhonfeng.zhf): use arrow::compute::Filter instead of PredicateFilter
    std::vector<char> results;
    if (bound_filter) {
        PAIMON_ASSIGN_OR_RAISE(results, bound_filter->Test(*array, arrow_pool_.get()));
    } else {
        results.assign(array->length(), 1);
    }
    if (results.size() != static_cast<size_t>(array->length())) {
        return Status::Invalid(
            fmt::format("predicate result size {} does not match probe batch length {}",
                        results.size(), array->length()));
    }
    // batch-local offsets of the rows passing both the predicate and the selection
    RoaringBitmap32 batch_matched;
    for (int64_t i = 0; i < array->length(); ++i) {
        if (!results[static_cast<size_t>(i)]) {
            continue;
        }
        // map batch offset to file row id
        PAIMON_ASSIGN_OR_RAISE(uint64_t file_row,
                               inner_->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
        const bool fits = file_row <= std::numeric_limits<uint32_t>::max();
        if (selection_ && (!fits || !selection_->Contains(static_cast<uint32_t>(file_row)))) {
            continue;
        }
        batch_matched.Add(static_cast<uint32_t>(i));
        if (state_ == kProbing) {
            if (fits) {
                matched_bitmap_.Add(static_cast<uint32_t>(file_row));
            } else {
                row_ids_fit_ = false;
            }
        }
    }
    return batch_matched;
}

Status LateMaterializingFileBatchReader::ReadAndFilterProbeData() {
    matched_bitmap_ = RoaringBitmap32();
    probe_cursor_ = 0;
    arrow::ArrayVector probe_arrays;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(FileBatchReader::ReadBatch batch, inner_->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (validation_.validate) {
            PAIMON_RETURN_NOT_OK(validation_.validate(array));
        }
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 batch_matched,
                               FilterProbeBatch(array, probe_filter_));
        if (!row_ids_fit_) {
            probe_arrays.clear();
            continue;
        }
        // Compact each probe batch down to its matched rows so probe_data_ aligns row-for-row
        // (ascending file order) with matched_bitmap_ and the later payload output.
        if (!batch_matched.IsEmpty()) {
            PAIMON_ASSIGN_OR_RAISE(arrow::ArrayVector matched_slices,
                                   ReaderUtils::GenerateFilteredArrayVector(array, batch_matched));
            probe_arrays.insert(probe_arrays.end(), std::make_move_iterator(matched_slices.begin()),
                                std::make_move_iterator(matched_slices.end()));
        }
    }

    if (!row_ids_fit_) {
        return Status::OK();
    }
    std::shared_ptr<arrow::Array> probe_array;
    if (probe_arrays.empty()) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            probe_array, arrow::MakeEmptyArray(arrow::struct_(probe_schema_->fields())));
    } else {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(probe_array,
                                          arrow::Concatenate(probe_arrays, arrow_pool_.get()));
    }
    probe_data_ = arrow::internal::checked_pointer_cast<arrow::StructArray>(probe_array);
    return Status::OK();
}

Result<FileBatchReader::ReadBatch> LateMaterializingFileBatchReader::ReadPayloadBatch() {
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(FileBatchReader::ReadBatchWithBitmap batch_with_bitmap,
                               inner_->NextBatchWithBitmap());
        if (BatchReader::IsEofBatch(batch_with_bitmap)) {
            state_ = kEOF;
            if (probe_cursor_ != probe_data_->length()) {
                return Status::Invalid(
                    fmt::format("probe cursor {} does not match probe data length {}",
                                probe_cursor_, probe_data_->length()));
            }
            return MakeEofBatch();
        }
        auto& [batch, bitmap] = batch_with_bitmap;
        if (bitmap.IsEmpty()) {
            ReaderUtils::ReleaseReadBatch(std::move(batch));
            return Status::Invalid("inner read bitmap is empty.");
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> payload_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));

        // Generate the valid bitmap and row_mapping_
        RoaringBitmap32 valid;
        row_mapping_.clear();
        for (auto it = bitmap.Begin(); it != bitmap.End(); ++it) {
            auto offset = static_cast<uint64_t>(*it);
            PAIMON_ASSIGN_OR_RAISE(uint64_t file_row, inner_->GetPreviousBatchFileRowId(offset));
            if (file_row > std::numeric_limits<uint32_t>::max() ||
                !matched_bitmap_.Contains(static_cast<uint32_t>(file_row))) {
                continue;
            }
            valid.Add(static_cast<uint32_t>(offset));
            row_mapping_.push_back(file_row);
        }
        if (valid.IsEmpty()) {
            ReaderUtils::ReleaseReadBatch(std::move(batch));
            continue;
        }

        // Compact the payload superset down to the matched rows (ascending file row order).
        PAIMON_ASSIGN_OR_RAISE(arrow::ArrayVector payload_slices,
                               ReaderUtils::GenerateFilteredArrayVector(payload_array, valid));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> payload_compacted,
                                          arrow::Concatenate(payload_slices, arrow_pool_.get()));

        auto card = static_cast<int64_t>(valid.Cardinality());
        if (probe_cursor_ + card > probe_data_->length()) {
            return Status::Invalid(
                fmt::format("probe cache underflow: cursor {} + {} exceeds probe rows {}",
                            probe_cursor_, card, probe_data_->length()));
        }
        std::shared_ptr<arrow::Array> probe_selected = probe_data_->Slice(probe_cursor_, card);
        PAIMON_ASSIGN_OR_RAISE(
            probe_selected, ArrowUtils::NormalizeArrayOffsets(probe_selected, arrow_pool_.get()));
        probe_cursor_ += card;

        PAIMON_ASSIGN_OR_RAISE(FileBatchReader::ReadBatch assembled,
                               AssembleFullBatch(payload_compacted, probe_selected));
        return assembled;
    }
}

Result<FileBatchReader::ReadBatch> LateMaterializingFileBatchReader::ReadFilteredBatch() {
    row_mapping_.clear();
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(FileBatchReader::ReadBatch batch, inner_->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            return MakeEofBatch();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ImportArray(batch.first.get(), batch.second.get()));
        if (validation_.validate) {
            PAIMON_RETURN_NOT_OK(validation_.validate(array));
        }
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 valid, FilterProbeBatch(array, full_filter_));
        if (valid.IsEmpty()) {
            continue;
        }
        for (auto it = valid.Begin(); it != valid.End(); ++it) {
            PAIMON_ASSIGN_OR_RAISE(uint64_t file_row,
                                   inner_->GetPreviousBatchFileRowId(static_cast<uint32_t>(*it)));
            row_mapping_.push_back(file_row);
        }
        PAIMON_ASSIGN_OR_RAISE(arrow::ArrayVector slices,
                               ReaderUtils::GenerateFilteredArrayVector(array, valid));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> compacted,
                                          arrow::Concatenate(slices, arrow_pool_.get()));
        return AssembleFullBatch(compacted, compacted);
    }
}

Result<std::shared_ptr<PredicateFilter>> LateMaterializingFileBatchReader::BindFilter(
    const std::shared_ptr<arrow::Schema>& schema) const {
    if (!predicate_) {
        return std::shared_ptr<PredicateFilter>();
    }
    PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
        *schema, predicate_, /*validate_field_idx=*/false));
    std::map<std::string, int32_t> name_to_idx;
    for (int32_t i = 0; i < schema->num_fields(); ++i) {
        name_to_idx.emplace(schema->field(i)->name(), i);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> bound_predicate,
                           PredicateUtils::CreatePickedFieldFilter(predicate_, name_to_idx));
    auto filter = std::dynamic_pointer_cast<PredicateFilter>(bound_predicate);
    if (!filter) {
        return Status::Invalid("failed to bind predicate to read schema");
    }
    return filter;
}

Result<FileBatchReader::ReadBatch> LateMaterializingFileBatchReader::AssembleFullBatch(
    const std::shared_ptr<arrow::Array>& payload_array,
    const std::shared_ptr<arrow::Array>& probe_array) {
    auto payload_struct = arrow::internal::checked_pointer_cast<arrow::StructArray>(payload_array);
    auto probe_struct = arrow::internal::checked_pointer_cast<arrow::StructArray>(probe_array);
    arrow::ArrayVector children;
    children.reserve(full_schema_->num_fields());
    for (const auto& field : full_schema_->fields()) {
        std::shared_ptr<arrow::Array> col = payload_struct->GetFieldByName(field->name());
        if (!col) {
            col = probe_struct->GetFieldByName(field->name());
        }
        if (!col) {
            return Status::Invalid(
                fmt::format("field {} missing in both payload and probe columns", field->name()));
        }
        PAIMON_ASSIGN_OR_RAISE(col, ArrowUtils::NormalizeArrayOffsets(col, arrow_pool_.get()));
        children.push_back(std::move(col));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> full_struct,
                                      arrow::StructArray::Make(children, full_schema_->fields()));
    std::unique_ptr<::ArrowArray> c_array = std::make_unique<::ArrowArray>();
    std::unique_ptr<::ArrowSchema> c_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*full_struct, c_array.get(), c_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), arrow_pool_));
    return std::make_pair(std::move(c_array), std::move(c_schema));
}

Status LateMaterializingFileBatchReader::SetInnerReadSchema(
    const std::shared_ptr<arrow::Schema>& read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection) {
    ::ArrowSchema c_read_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, &c_read_schema));
    PAIMON_RETURN_NOT_OK(inner_->SetReadSchema(&c_read_schema, predicate, selection));
    return Status::OK();
}

Status LateMaterializingFileBatchReader::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    Reset();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(full_schema_, arrow::ImportSchema(read_schema));
    predicate_ = predicate;
    selection_ = selection_bitmap;
    for (const auto& name : validation_.field_names) {
        if (!full_schema_->GetFieldByName(name)) {
            return Status::Invalid("validation field missing or ambiguous in read schema: ", name);
        }
    }
    if (predicate_ != nullptr) {
        std::set<std::string> probe_names;
        PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(predicate_, &probe_names));
        probe_names.insert(validation_.field_names.begin(), validation_.field_names.end());
        arrow::FieldVector probe_fields;
        arrow::FieldVector payload_fields;
        for (const auto& field : full_schema_->fields()) {
            if (probe_names.count(field->name()) > 0) {
                probe_fields.push_back(field);
            } else {
                payload_fields.push_back(field);
            }
        }
        // probing only pays off when the predicate fields are a strict subset of the read schema
        if (!probe_fields.empty() && !payload_fields.empty()) {
            probe_schema_ = arrow::schema(probe_fields, full_schema_->metadata());
            payload_schema_ = arrow::schema(payload_fields, full_schema_->metadata());
            PAIMON_ASSIGN_OR_RAISE(probe_filter_, BindFilter(probe_schema_));
        }
    }

    if (predicate_ == nullptr || probe_schema_ == nullptr) {
        if (validation_.validate) {
            PAIMON_ASSIGN_OR_RAISE(full_filter_, BindFilter(full_schema_));
            PAIMON_RETURN_NOT_OK(SetInnerReadSchema(full_schema_, nullptr, std::nullopt));
            state_ = kFiltering;
        } else {
            PAIMON_RETURN_NOT_OK(SetInnerReadSchema(full_schema_, predicate_, selection_));
            state_ = kNoLatMat;
        }
    } else {
        PAIMON_RETURN_NOT_OK(SetInnerReadSchema(probe_schema_,
                                                validation_.validate ? nullptr : predicate_,
                                                validation_.validate ? std::nullopt : selection_));
        state_ = kProbing;
    }
    return Status::OK();
}

Result<uint64_t> LateMaterializingFileBatchReader::GetPreviousBatchFileRowId(
    uint64_t batch_row_id) const {
    if (state_ == kNoLatMat) {
        return inner_->GetPreviousBatchFileRowId(batch_row_id);
    }
    // In kRunning the emitted batch is compacted/reassembled, so row ids come from row_mapping_
    // instead of the inner reader.
    if (batch_row_id >= row_mapping_.size()) {
        return Status::Invalid(
            fmt::format("batch_row_id {} is out of range, last batch row count is {}", batch_row_id,
                        row_mapping_.size()));
    }
    return row_mapping_[batch_row_id];
}

Status LateMaterializingFileBatchReader::SeekToRow(uint64_t row_number) {
    PAIMON_ASSIGN_OR_RAISE(PrefetchFileBatchReader * prefetch_reader,
                           GetPrefetchReaderOrRaise("SeekToRow"));
    PAIMON_RETURN_NOT_OK(prefetch_reader->SeekToRow(row_number));
    if (state_ == kRunning || state_ == kEOF) {
        if (matched_bitmap_.IsEmpty()) {
            state_ = kEOF;
            return Status::OK();
        }
        int64_t cursor = 0;
        for (auto it = matched_bitmap_.Begin(); it != matched_bitmap_.End(); ++it) {
            if (static_cast<uint64_t>(*it) >= row_number) {
                break;
            }
            ++cursor;
        }
        probe_cursor_ = cursor;
        // a seek after EOF re-activates payload reading
        state_ = kRunning;
    }
    return Status::OK();
}

Status LateMaterializingFileBatchReader::SetReadRanges(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) {
    if (prefetch_inner_ == nullptr) {
        // Only the format reader can act on this hint, and the PrefetchFileBatchReader contract
        // lets an implementation that cannot honor it ignore the hint.
        return Status::OK();
    }
    return prefetch_inner_->SetReadRanges(read_ranges);
}

void LateMaterializingFileBatchReader::Reset() {
    state_ = kInit;
    matched_bitmap_ = RoaringBitmap32();
    probe_data_.reset();
    probe_cursor_ = 0;
    row_mapping_.clear();
    probe_schema_.reset();
    payload_schema_.reset();
    full_schema_.reset();
    probe_filter_.reset();
    full_filter_.reset();
    row_ids_fit_ = true;
    predicate_.reset();
    selection_.reset();
    probe_cursor_ = 0;
    row_mapping_.clear();
}

}  // namespace paimon
