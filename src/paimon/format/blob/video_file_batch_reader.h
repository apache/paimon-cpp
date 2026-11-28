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

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/reader/file_batch_reader.h"

namespace arrow {
class Field;
class MemoryPool;
}  // namespace arrow

namespace paimon {
class InputStream;
class MemoryPool;
struct BlobReadContext;
}  // namespace paimon

namespace paimon::blob {

/// Reads the row index of a Paimon v1 .video file and emits lazy frame descriptors.
class VideoFileBatchReader : public FileBatchReader {
 public:
    static Result<std::unique_ptr<VideoFileBatchReader>> Create(
        const std::shared_ptr<InputStream>& input, int32_t batch_size,
        const std::shared_ptr<MemoryPool>& pool, bool emit_placeholder_sentinel,
        const std::shared_ptr<BlobReadContext>& blob_context);

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;
    Status SetReadSchema(::ArrowSchema* schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection) override;
    Result<ReadBatch> NextBatch() override;
    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;
    Result<uint64_t> GetNumberOfRows() const override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;
    bool SupportPreciseBitmapSelection() const override {
        return true;
    }

 private:
    struct VideoRange {
        int64_t offset;
        int64_t length;
        int64_t moov_offset;
        int64_t moov_length;
    };

    struct FrameRun {
        uint64_t end;
        int64_t reference;
        int64_t first_frame;
    };

    VideoFileBatchReader(std::string uri, int32_t batch_size, std::shared_ptr<MemoryPool> pool,
                         bool emit_placeholder_sentinel,
                         const std::shared_ptr<BlobReadContext>& blob_context);

    Status ReadSixIndexLayout(InputStream* input, const std::array<int32_t, 6>& index_lengths,
                              int64_t index_start);

    std::string uri_;
    int32_t batch_size_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<arrow::Field> field_;
    std::shared_ptr<Metrics> metrics_;
    bool emit_placeholder_sentinel_;
    std::shared_ptr<BlobReadContext> blob_context_;
    std::vector<VideoRange> videos_;
    std::vector<FrameRun> runs_;
    std::optional<RoaringBitmap32> selection_;
    std::vector<int32_t> previous_selected_rows_;
    uint64_t row_count_ = 0;
    uint64_t read_row_count_ = 0;
    uint64_t current_ = 0;
    uint64_t previous_batch_size_ = 0;
    bool closed_ = false;
};

}  // namespace paimon::blob
