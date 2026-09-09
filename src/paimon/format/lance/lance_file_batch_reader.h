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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/format/lance/lance_ffi.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {
class InputStream;
class MemoryPool;
class Metrics;
}  // namespace paimon

namespace paimon::lance {

class LanceFileBatchReader : public FileBatchReader {
 public:
    static Result<std::unique_ptr<LanceFileBatchReader>> Create(
        const std::shared_ptr<InputStream>& input, int32_t batch_size, uint32_t batch_readahead,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    ~LanceFileBatchReader() override;

    Result<ReadBatch> NextBatch() override;
    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;
    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;
    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;
    Result<uint64_t> GetNumberOfRows() const override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;
    bool SupportPreciseBitmapSelection() const override {
        return true;
    }

 private:
    LanceFileBatchReader(const std::shared_ptr<InputStream>& input, int32_t batch_size,
                         uint32_t batch_readahead, PaimonLanceReader* reader,
                         const std::shared_ptr<arrow::Schema>& file_schema, uint64_t total_rows,
                         const std::shared_ptr<MemoryPool>& pool,
                         const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Status OpenStream();
    Result<ReadBatch> AlignBatch(ReadBatch batch) const;
    void CloseInternal();

    std::shared_ptr<InputStream> input_;
    int32_t batch_size_;
    uint32_t batch_readahead_;
    PaimonLanceReader* reader_;
    PaimonLanceBatchReader* batch_reader_ = nullptr;
    std::shared_ptr<arrow::Schema> file_schema_;
    std::shared_ptr<arrow::Schema> read_schema_;
    uint64_t total_rows_;
    std::vector<std::string> projection_names_;
    std::vector<uint64_t> selection_row_ids_;
    std::vector<uint64_t> selection_starts_;
    std::vector<uint64_t> selection_ends_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> metrics_;
    uint64_t next_row_offset_ = 0;
    uint64_t previous_row_offset_ = std::numeric_limits<uint64_t>::max();
    uint64_t previous_batch_row_count_ = 0;
    bool has_selection_ = false;
    bool closed_ = false;
};

}  // namespace paimon::lance
