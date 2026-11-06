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

#include <memory>
#include <optional>

#include "arrow/c/abi.h"
#include "paimon/common/reader/file_read_ahead.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {

/// A synchronous file reader with independent byte caching and raw warmup.
class WarmupFileBatchReader : public FileBatchReader {
 public:
    WarmupFileBatchReader(std::shared_ptr<PrefetchFileBatchReader> reader,
                          std::shared_ptr<FileReadAhead> read_ahead, WarmupLevel level);
    ~WarmupFileBatchReader() override;

    Result<ReadBatch> NextBatch() override;
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;
    Status SetReadSchema(::ArrowSchema* schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& bitmap) override;
    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;
    Result<uint64_t> GetNumberOfRows() const override;
    bool SupportPreciseBitmapSelection() const override;
    void Warmup() override;
    void Close() override;

    /// For a containing reader which configures the underlying format reader itself.
    void Prepare();
    void ResetReadState();

 private:
    template <typename Batch>
    Result<Batch> ReadOneBatch();

    std::shared_ptr<PrefetchFileBatchReader> reader_;
    std::shared_ptr<FileReadAhead> read_ahead_;
    const WarmupLevel level_;
    Status read_status_;
    bool prepared_ = false;
    bool closed_ = false;
    bool batch_returned_ = false;
};

}  // namespace paimon
