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

namespace paimon {

class Executor;
class ReaderBuilder;
struct PrefetchIoMetricsState;

/// Per-file byte caching and I/O instrumentation, shared by synchronous and parallel readers.
/// Callers must stop their reader tasks before resetting or releasing a generation.
class FileReadAhead {
 public:
    struct OpenedReaders {
        std::vector<std::shared_ptr<PrefetchFileBatchReader>> readers;
        std::shared_ptr<FileReadAhead> read_ahead;
    };

    static Result<OpenedReaders> Open(const std::string& path, int64_t file_size,
                                      const ReaderBuilder* builder,
                                      const std::shared_ptr<FileSystem>& fs, uint32_t reader_count,
                                      const std::shared_ptr<Executor>& executor, bool cache_enabled,
                                      const CacheConfig& cache_config, bool enable_io_metrics,
                                      const std::shared_ptr<MemoryPool>& pool);

    Status Warmup(PrefetchFileBatchReader* reader);
    Status GetStatus() const;
    uint64_t GetErrorCount() const;
    void EndGeneration();
    void Reset();
    void ReleaseBuffers();
    std::shared_ptr<Metrics> GetMetrics() const;

 private:
    FileReadAhead(const std::shared_ptr<ReadAheadCache>& cache,
                  const std::shared_ptr<PrefetchIoMetricsState>& io_metrics)
        : cache_(cache), io_metrics_(io_metrics) {}

    void RegisterLateRanges(std::vector<std::pair<uint64_t, uint64_t>>&& ranges);

    std::shared_ptr<ReadAheadCache> cache_;
    std::shared_ptr<PrefetchIoMetricsState> io_metrics_;
    mutable std::mutex mutex_;
    bool warmed_ = false;
    Status status_;
    uint64_t error_count_ = 0;
};

}  // namespace paimon
