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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {
/// Adds index scores to a single-file reader while retaining its file-specific operations.
class CompleteIndexScoreFileBatchReader : public FileBatchReader {
 public:
    CompleteIndexScoreFileBatchReader(std::unique_ptr<FileBatchReader>&& reader,
                                      const std::vector<float>& scores,
                                      const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    /// Scores are keyed by output field name and aligned with the selected rows in file order.
    CompleteIndexScoreFileBatchReader(std::unique_ptr<FileBatchReader>&& reader,
                                      const std::map<std::string, std::vector<float>>& scores,
                                      const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Result<ReadBatch> NextBatch() override;
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;
    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;
    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;
    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;
    Result<uint64_t> GetNumberOfRows() const override;
    bool SupportPreciseBitmapSelection() const override;
    void Warmup() override;
    void Close() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;

 private:
    Result<int32_t> GetScoreFieldIndex(const arrow::StructType* struct_type,
                                       const std::string& field_name);

    Result<std::shared_ptr<arrow::Array>> PrepareScoreArray(const std::string& field_name,
                                                            const std::vector<float>& scores,
                                                            int64_t row_count,
                                                            const RoaringBitmap32& bitmap) const;

    size_t score_cursor_ = 0;
    std::map<std::string, int32_t> index_score_field_indices_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<FileBatchReader> reader_;
    std::map<std::string, std::vector<float>> scores_;
};
}  // namespace paimon
