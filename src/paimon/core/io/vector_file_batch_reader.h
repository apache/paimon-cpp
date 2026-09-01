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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "arrow/c/abi.h"
#include "paimon/reader/file_batch_reader.h"

namespace arrow {
class DataType;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace paimon {
class MemoryPool;

/// Reconciles logical VECTOR values with the variable-length LIST representation exposed to file
/// format plugins.
class VectorFileBatchReader : public FileBatchReader {
 public:
    VectorFileBatchReader(std::unique_ptr<FileBatchReader>&& reader,
                          const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    static bool ContainsVector(const std::shared_ptr<arrow::Schema>& schema);

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
        return reader_->GetFileSchema();
    }

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Result<ReadBatch> NextBatch() override;

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override {
        return reader_->GetPreviousBatchFileRowId(batch_row_id);
    }

    Result<uint64_t> GetNumberOfRows() const override {
        return reader_->GetNumberOfRows();
    }

    bool SupportPreciseBitmapSelection() const override {
        return reader_->SupportPreciseBitmapSelection();
    }

 private:
    Result<ReadBatch> ConvertBatch(ReadBatch&& batch) const;

    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<arrow::DataType> read_type_;
    std::unique_ptr<FileBatchReader> reader_;
};

}  // namespace paimon
