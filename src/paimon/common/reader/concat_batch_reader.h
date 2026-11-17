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

#include <atomic>
#include <cstddef>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/executor/reader_build_executor.h"
#include "paimon/metrics.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class FileBatchReader;
class MemoryPool;

/// This reader is to concatenate a list of BatchReaders and read them sequentially. The input list
/// is already sorted by key and sequence number, and the key intervals do not overlap each other.
class ConcatBatchReader : public BatchReader {
 public:
    /// Must own the state needed to build the indexed reader beyond its caller's lifetime.
    /// Called at most once per index, possibly concurrently. A null reader means a skipped file.
    using ReaderFactory = std::function<Result<std::unique_ptr<BatchReader>>(size_t)>;

    static constexpr size_t kPreparationWindow = kReaderBuildMaxParallelNum;

    /// Builds the initial window concurrently and refills it as readers reach EOF. Later
    /// construction failures are terminal read errors when the failed reader is reached.
    static Result<std::unique_ptr<ConcatBatchReader>> CreateLazy(
        size_t reader_count, ReaderFactory factory,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
        const std::shared_ptr<Executor>& build_executor = GetReaderBuildExecutor());

    ConcatBatchReader(std::vector<std::unique_ptr<BatchReader>>&& readers,
                      const std::shared_ptr<arrow::MemoryPool>& arrow_pool);
    ~ConcatBatchReader() override;

    Result<ReadBatch> NextBatch() override;
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;
    void Close() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;

 private:
    /// How many child readers ahead of the one being consumed get their first read started. Each
    /// warm reader holds its own prefetched data, so one ahead is a conservative trade-off between
    /// overlapped I/O and extra memory.
    static constexpr size_t kWarmupLookahead = 1;

    struct ReaderState {
        std::unique_ptr<BatchReader> reader;
        FileBatchReader* file_reader = nullptr;
        std::future<Result<std::unique_ptr<BatchReader>>> future;
        Status failure;
    };

    struct Preparation {
        explicit Preparation(ReaderFactory factory) : factory(std::move(factory)) {}
        ReaderFactory factory;
        std::atomic<bool> cancelled = false;
    };

    void FillWindow();
    Status ResolveReader(ReaderState* state, bool wait);
    void WarmupRange();
    void CloseAndReleaseReader(ReaderState* state);

    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> finished_reader_metrics_;
    std::deque<ReaderState> readers_;
    std::shared_ptr<Preparation> preparation_;
    std::shared_ptr<Executor> build_executor_;
    size_t reader_count_ = 0;
    size_t next_reader_ = 0;
    Status failure_;
};
}  // namespace paimon
