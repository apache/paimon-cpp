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
#include "paimon/common/reader/index_score_batch_utils.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/table/special_fields.h"
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

    const size_t score_count = batch_with_bitmap.second.Cardinality();
    for (const auto& [field_name, scores] : scores_) {
        if (score_cursor_ > scores.size() || score_count > scores.size() - score_cursor_) {
            return Status::Invalid(
                fmt::format("Index score count {} for field '{}' is smaller than rows read {}",
                            scores.size(), field_name, score_cursor_ + score_count));
        }
    }
    auto& [c_array, c_schema] = batch_with_bitmap.first;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    if (!imported || imported->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            "cannot cast array to StructArray in CompleteIndexScoreFileBatchReader");
    }
    auto data = checked_pointer_cast<arrow::StructArray>(imported);
    PAIMON_ASSIGN_OR_RAISE(
        BatchReader::ReadBatchWithBitmap completed,
        IndexScoreBatchUtils::CompleteScores(
            std::move(batch_with_bitmap), data, arrow::schema(data->struct_type()->fields()),
            scores_,
            [this](const std::string&, const std::vector<float>& scores, int64_t,
                   size_t selected) -> Result<float> { return scores[score_cursor_ + selected]; },
            arrow_pool_));
    score_cursor_ += score_count;
    return completed;
}

Result<std::unique_ptr<::ArrowSchema>> CompleteIndexScoreFileBatchReader::GetFileSchema() const {
    return reader_->GetFileSchema();
}

Status CompleteIndexScoreFileBatchReader::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    score_cursor_ = 0;
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
