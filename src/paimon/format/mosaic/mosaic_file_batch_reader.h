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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <limits>
#include <memory>

#include "paimon/format/mosaic/mosaic_ffi.h"
#include "paimon/format/mosaic/mosaic_stream.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
}  // namespace arrow
namespace paimon {
class InputStream;
class MemoryPool;
class Metrics;
}  // namespace paimon

namespace paimon::mosaic {

class MosaicFileBatchReader : public FileBatchReader {
 public:
    static Result<std::unique_ptr<MosaicFileBatchReader>> Create(
        const std::shared_ptr<InputStream>& input, int32_t batch_size,
        const std::shared_ptr<MemoryPool>& pool);

    ~MosaicFileBatchReader() override;

    Result<ReadBatch> NextBatch() override;
    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;
    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;
    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;
    Result<uint64_t> GetNumberOfRows() const override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;
    bool SupportPreciseBitmapSelection() const override {
        return false;
    }

 private:
    MosaicFileBatchReader(const std::shared_ptr<InputStream>& input, int32_t batch_size,
                          std::unique_ptr<MosaicInputContext> input_context,
                          MosaicReaderHandle* reader,
                          const std::shared_ptr<arrow::Schema>& file_schema,
                          uint32_t num_row_groups, uint64_t total_rows,
                          const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Result<std::shared_ptr<arrow::Array>> ReadNextRowGroup();
    void CloseInternal();

    std::shared_ptr<InputStream> input_;
    int32_t batch_size_;
    std::unique_ptr<MosaicInputContext> input_context_;
    MosaicReaderHandle* reader_;
    std::shared_ptr<arrow::Schema> file_schema_;
    uint32_t num_row_groups_;
    uint64_t total_rows_;
    uint32_t next_row_group_ = 0;
    uint64_t next_row_group_first_row_ = 0;
    uint64_t current_row_group_first_row_ = 0;
    std::shared_ptr<arrow::Array> current_batch_;
    int64_t current_batch_offset_ = 0;
    uint64_t previous_first_row_ = std::numeric_limits<uint64_t>::max();
    uint64_t previous_batch_row_count_ = 0;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> metrics_;
    bool closed_ = false;
};

}  // namespace paimon::mosaic
