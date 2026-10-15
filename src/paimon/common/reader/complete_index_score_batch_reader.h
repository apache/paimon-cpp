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
#include <unordered_map>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {
class MemoryPool;
class Metrics;
/// A batch reader that enriches the output Arrow array with index score information.
/// It assumes the input data already contains the `_INDEX_SCORE` column,
/// and ensures this score is properly updated in the returned batches.
///
/// @pre The read schema must include the `_INDEX_SCORE` and `_ROW_ID` fields.
/// The schema must remain unchanged across batches, and selected row ids must be non-null.
class CompleteIndexScoreBatchReader : public BatchReader {
 public:
    /// Align global index scores with surviving row ids after filtering.
    /// Remove `_ROW_ID` from the output only when it was added internally for score lookup.
    CompleteIndexScoreBatchReader(std::unique_ptr<BatchReader>&& reader,
                                  std::unordered_map<int64_t, float>&& scores_by_row_id,
                                  bool remove_row_id,
                                  const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Result<ReadBatch> NextBatch() override;

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    void Close() override {
        reader_->Close();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

 private:
    Status InitFieldIndices(const arrow::StructType* struct_type);

 private:
    int32_t index_score_field_idx_ = -1;
    int32_t row_id_field_idx_ = -1;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<BatchReader> reader_;
    std::unordered_map<int64_t, float> scores_by_row_id_;
    bool remove_row_id_ = false;
};
}  // namespace paimon
