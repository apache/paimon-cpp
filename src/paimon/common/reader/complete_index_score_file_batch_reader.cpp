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

#include "paimon/common/reader/complete_index_score_file_batch_reader.h"

#include <cstddef>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/status.h"

namespace paimon {
CompleteIndexScoreFileBatchReader::CompleteIndexScoreFileBatchReader(
    std::unique_ptr<FileBatchReader>&& reader, const std::vector<float>& scores,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : CompleteIndexScoreFileBatchReader(std::move(reader),
                                        std::map<std::string, std::vector<float>>{}, arrow_pool) {
    if (!scores.empty()) {
        scores_.emplace(SpecialFields::IndexScore().Name(), scores);
    }
}

CompleteIndexScoreFileBatchReader::CompleteIndexScoreFileBatchReader(
    std::unique_ptr<FileBatchReader>&& reader,
    const std::map<std::string, std::vector<float>>& scores,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : arrow_pool_(arrow_pool), reader_(std::move(reader)), scores_(scores) {}

Result<BatchReader::ReadBatch> CompleteIndexScoreFileBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           NextBatchWithBitmap());
    return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool_);
}

Result<BatchReader::ReadBatchWithBitmap> CompleteIndexScoreFileBatchReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           reader_->NextBatchWithBitmap());
    if (BatchReader::IsEofBatch(batch_with_bitmap)) {
        for (const auto& [field_name, scores] : scores_) {
            if (score_cursor_ != scores.size()) {
                return Status::Invalid(
                    fmt::format("Index score count {} for field '{}' does not match rows read {}",
                                scores.size(), field_name, score_cursor_));
            }
        }
        return batch_with_bitmap;
    }
    if (scores_.empty()) {
        return batch_with_bitmap;
    }

    auto& [batch, bitmap] = batch_with_bitmap;
    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    if (!arrow_array || arrow_array->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            "cannot cast array to StructArray in CompleteIndexScoreFileBatchReader");
    }
    auto struct_array = checked_pointer_cast<arrow::StructArray>(arrow_array);
    auto struct_type = struct_array->struct_type();
    arrow::ArrayVector array_vec = struct_array->fields();
    for (const auto& [field_name, scores] : scores_) {
        PAIMON_ASSIGN_OR_RAISE(int32_t field_index, GetScoreFieldIndex(struct_type, field_name));
        PAIMON_ASSIGN_OR_RAISE(
            array_vec[field_index],
            PrepareScoreArray(field_name, scores, struct_array->length(), bitmap));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> array_with_score,
                                      arrow::StructArray::Make(array_vec, struct_type->fields()));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*array_with_score, c_array.get(), c_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), arrow_pool_));
    score_cursor_ += bitmap.Cardinality();
    return batch_with_bitmap;
}

Result<int32_t> CompleteIndexScoreFileBatchReader::GetScoreFieldIndex(
    const arrow::StructType* struct_type, const std::string& field_name) {
    auto iter = index_score_field_indices_.find(field_name);
    if (iter != index_score_field_indices_.end()) {
        return iter->second;
    }
    const auto field_indices = struct_type->GetAllFieldIndices(field_name);
    if (field_indices.empty()) {
        return Status::Invalid(fmt::format("Missing index score field '{}'", field_name));
    }
    if (field_indices.size() != 1) {
        return Status::Invalid(fmt::format("Duplicate index score field '{}'", field_name));
    }
    int32_t field_index = field_indices[0];
    if (struct_type->field(field_index)->type()->id() != arrow::Type::FLOAT) {
        return Status::Invalid(
            fmt::format("Index score field '{}' must have float32 type", field_name));
    }
    index_score_field_indices_.emplace(field_name, field_index);
    return field_index;
}

Result<std::shared_ptr<arrow::Array>> CompleteIndexScoreFileBatchReader::PrepareScoreArray(
    const std::string& field_name, const std::vector<float>& scores, int64_t row_count,
    const RoaringBitmap32& bitmap) const {
    size_t score_count = bitmap.Cardinality();
    if (score_cursor_ > scores.size() || score_count > scores.size() - score_cursor_) {
        return Status::Invalid(
            fmt::format("Index score count {} for field '{}' is smaller than rows read {}",
                        scores.size(), field_name, score_cursor_ + score_count));
    }
    arrow::FloatBuilder builder(arrow_pool_.get());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(row_count));
    bool all_not_null = (row_count == bitmap.Cardinality());
    size_t cursor = score_cursor_;
    for (int64_t i = 0; i < row_count; i++) {
        if (all_not_null || bitmap.Contains(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(scores[cursor++]));
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
        }
    }
    std::shared_ptr<arrow::Array> score_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&score_array));
    return score_array;
}

Result<std::unique_ptr<::ArrowSchema>> CompleteIndexScoreFileBatchReader::GetFileSchema() const {
    return reader_->GetFileSchema();
}

Status CompleteIndexScoreFileBatchReader::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    score_cursor_ = 0;
    index_score_field_indices_.clear();
    return reader_->SetReadSchema(read_schema, predicate, selection_bitmap);
}

Result<uint64_t> CompleteIndexScoreFileBatchReader::GetPreviousBatchFileRowId(
    uint64_t batch_row_id) const {
    return reader_->GetPreviousBatchFileRowId(batch_row_id);
}

Result<uint64_t> CompleteIndexScoreFileBatchReader::GetNumberOfRows() const {
    return reader_->GetNumberOfRows();
}

bool CompleteIndexScoreFileBatchReader::SupportPreciseBitmapSelection() const {
    return reader_->SupportPreciseBitmapSelection();
}

void CompleteIndexScoreFileBatchReader::Warmup() {
    reader_->Warmup();
}

void CompleteIndexScoreFileBatchReader::Close() {
    reader_->Close();
}

std::shared_ptr<Metrics> CompleteIndexScoreFileBatchReader::GetReaderMetrics() const {
    return reader_->GetReaderMetrics();
}
}  // namespace paimon
