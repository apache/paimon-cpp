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

#include <utility>

#include "arrow/c/abi.h"

namespace paimon {
CompleteIndexScoreFileBatchReader::CompleteIndexScoreFileBatchReader(
    std::unique_ptr<FileBatchReader>&& reader, const std::vector<float>& scores,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : file_reader_(reader.get()), score_reader_(std::move(reader), scores, arrow_pool) {}

Result<BatchReader::ReadBatch> CompleteIndexScoreFileBatchReader::NextBatch() {
    return score_reader_.NextBatch();
}

Result<BatchReader::ReadBatchWithBitmap> CompleteIndexScoreFileBatchReader::NextBatchWithBitmap() {
    return score_reader_.NextBatchWithBitmap();
}

Result<std::unique_ptr<::ArrowSchema>> CompleteIndexScoreFileBatchReader::GetFileSchema() const {
    return file_reader_->GetFileSchema();
}

Status CompleteIndexScoreFileBatchReader::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    score_reader_.ResetScoreState();
    return file_reader_->SetReadSchema(read_schema, predicate, selection_bitmap);
}

Result<uint64_t> CompleteIndexScoreFileBatchReader::GetPreviousBatchFileRowId(
    uint64_t batch_row_id) const {
    return file_reader_->GetPreviousBatchFileRowId(batch_row_id);
}

Result<uint64_t> CompleteIndexScoreFileBatchReader::GetNumberOfRows() const {
    return file_reader_->GetNumberOfRows();
}

bool CompleteIndexScoreFileBatchReader::SupportPreciseBitmapSelection() const {
    return file_reader_->SupportPreciseBitmapSelection();
}

void CompleteIndexScoreFileBatchReader::Warmup() {
    file_reader_->Warmup();
}

void CompleteIndexScoreFileBatchReader::Close() {
    score_reader_.Close();
}

std::shared_ptr<Metrics> CompleteIndexScoreFileBatchReader::GetReaderMetrics() const {
    return score_reader_.GetReaderMetrics();
}
}  // namespace paimon
