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
#include <memory>
#include <vector>

#include "arrow/api.h"
#include "paimon/metrics.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {
class FileBatchReader;
class MemoryPool;

/// This reader is to concatenate a list of BatchReaders and read them sequentially. The input list
/// is already sorted by key and sequence number, and the key intervals do not overlap each other.
class ConcatBatchReader : public BatchReader {
 public:
    ConcatBatchReader(std::vector<std::unique_ptr<BatchReader>>&& readers,
                      const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Result<ReadBatch> NextBatch() override;
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;
    void Close() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;

 private:
    /// How many child readers ahead of the one being consumed get their first read started. Each
    /// warm reader holds its own prefetched data, so one ahead is a conservative trade-off between
    /// overlapped I/O and extra memory.
    static constexpr size_t kWarmupLookahead = 1;

    /// Warms up \p count readers starting at \p idx, stopping at the end of the list. Warmup() is
    /// idempotent, so this is safe to call on every batch.
    void WarmupRange(size_t idx, size_t count);
    void CloseAndReleaseReader(size_t reader_index);

    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> finished_reader_metrics_;
    std::vector<std::unique_ptr<BatchReader>> readers_;
    // readers_[i] as a FileBatchReader, or nullptr when it is not one or has been released.
    // Warmup() lives on FileBatchReader, so the cast is done once instead of on every batch.
    std::vector<FileBatchReader*> file_readers_;
    size_t current_;
};
}  // namespace paimon
