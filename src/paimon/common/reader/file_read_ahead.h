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
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/utils/read_ahead_cache.h"
#include "paimon/reader/prefetch_file_batch_reader.h"
#include "paimon/visibility.h"

namespace arrow {
class MemoryPool;
}  // namespace arrow

namespace paimon {

class Executor;
class ReaderBuilder;
struct PrefetchIoMetricsState;

/// Internal per-file cache context, shared by streams, format readers and LM wrappers.
/// It owns no readers, so builders can pass it through CacheInputStream without ownership cycles.
/// Callers must stop their reader tasks before resetting or releasing a generation.
class PAIMON_EXPORT FileReadAhead {
 public:
    struct OpenedReaders {
        std::vector<std::shared_ptr<PrefetchFileBatchReader>> readers;
        std::shared_ptr<FileReadAhead> read_ahead;
    };

    static std::shared_ptr<FileReadAhead> Create(const std::shared_ptr<ReadAheadCache>& cache,
                                                 const std::string& path);

    static Result<OpenedReaders> Open(const std::string& path, int64_t file_size,
                                      const ReaderBuilder* builder,
                                      const std::shared_ptr<FileSystem>& fs, uint32_t reader_count,
                                      const std::shared_ptr<Executor>& executor, bool cache_enabled,
                                      const CacheConfig& cache_config, bool enable_io_metrics,
                                      const std::shared_ptr<MemoryPool>& pool);

    static Result<std::unique_ptr<FileBatchReader>> OpenSynchronous(
        const std::string& path, int64_t file_size, const ReaderBuilder* builder,
        const std::shared_ptr<FileSystem>& fs, bool cache_enabled, const CacheConfig& cache_config,
        bool enable_io_metrics, WarmupLevel level, const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Status Warmup(PrefetchFileBatchReader* reader);
    /// Prepare ranges as a format advances or LM switches projections. Inactive contexts and
    /// disabled caches do nothing, including skipping the reader's range query.
    Status PrepareRanges(PrefetchFileBatchReader* reader);
    void SetSynchronous(bool synchronous);
    Status GetStatus() const;
    uint64_t GetErrorCount() const;
    void EndGeneration();
    void Reset();
    void ReleaseBuffers();
    std::shared_ptr<Metrics> GetMetrics() const;

 private:
    FileReadAhead(const std::shared_ptr<ReadAheadCache>& cache, const std::string& path)
        : cache_(cache), path_(path) {}

    using ByteRanges = std::vector<std::pair<uint64_t, uint64_t>>;
    Status PrepareRangesLocked(Result<ByteRanges>&& ranges, bool initial);

    std::shared_ptr<ReadAheadCache> cache_;
    std::shared_ptr<PrefetchIoMetricsState> io_metrics_;
    std::string path_;
    mutable std::mutex mutex_;
    bool warmed_ = false;
    Status status_;
    uint64_t error_count_ = 0;
};

}  // namespace paimon
