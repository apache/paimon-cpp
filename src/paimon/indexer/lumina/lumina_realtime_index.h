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
#include <map>
#include <memory>
#include <string>

#include "lumina/api/LuminaStreamer.h"
#include "lumina/extensions/SearchWithFilterExtension.h"
#include "paimon/indexer/lumina/lumina_index_options.h"
#include "paimon/realtime/index/realtime_index.h"
#include "paimon/realtime/index/realtime_index_reader.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class MemoryPool;
}  // namespace paimon

namespace paimon::lumina {

class LuminaRealtimeIndexReader;
class LuminaMemoryPool;

/// Append-only vector index with one writer and concurrent readers.
/// Insert IDs and SearchResult.topk[].id are realtime offsets.
class LuminaRealtimeIndex final : public RealtimeIndex,
                                  public std::enable_shared_from_this<LuminaRealtimeIndex> {
 public:
    /// Options use native Lumina keys such as index.dimension.
    static Result<std::shared_ptr<LuminaRealtimeIndex>> Create(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::map<std::string, std::string>& lumina_options,
        const std::shared_ptr<MemoryPool>& pool);

    ~LuminaRealtimeIndex() override;

    /// Consumes [_REALTIME_OFFSET, vector]. Validates the whole batch before inserting.
    /// The caller guarantees unique, increasing offsets and publishes query ranges only after
    /// successful writes. An insertion failure disables subsequent writes.
    Status AddBatch(::ArrowArray* batch) override;

    Result<std::shared_ptr<RealtimeIndexReader>> CreateReader(
        const OffsetRange& offset_range, const std::shared_ptr<MemoryPool>& pool) const override;

    /// Bytes currently allocated by this streamer's Lumina memory resource.
    uint64_t GetMemoryUsage() const override;

 private:
    friend class LuminaRealtimeIndexReader;
    LuminaRealtimeIndex(std::shared_ptr<arrow::Schema> schema, LuminaIndexInfo index_info,
                        std::unique_ptr<::lumina::extensions::StreamWithFilterExtension> filter,
                        std::unique_ptr<::lumina::api::LuminaStreamer> streamer,
                        std::unique_ptr<LuminaMemoryPool> pool);

    std::shared_ptr<arrow::Schema> schema_;
    LuminaIndexInfo index_info_;
    bool write_failed_ = false;
    // Destroy the streamer before its attached extension and memory resource.
    std::unique_ptr<LuminaMemoryPool> pool_;
    std::unique_ptr<::lumina::extensions::StreamWithFilterExtension> filter_;
    std::unique_ptr<::lumina::api::LuminaStreamer> streamer_;
};

/// Shares the streamer while restricting every search to [begin, end).
/// Later appends cannot make offsets outside this range visible. Approximate search results
/// within the range may vary as the graph grows; this reader does not snapshot the graph.
class LuminaRealtimeIndexReader final : public RealtimeIndexReader {
 public:
    LuminaRealtimeIndexReader(std::shared_ptr<const LuminaRealtimeIndex> index,
                              const OffsetRange& offset_range,
                              const std::shared_ptr<MemoryPool>& pool);

    bool SupportsSearch(SearchType search_type) const override;

    /// pre_filter receives realtime offsets and runs before TopK selection. Must be thread-safe.
    /// Vector predicates (tags) are not supported.
    Result<std::shared_ptr<ScoredRealtimeIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override;

 private:
    std::shared_ptr<const LuminaRealtimeIndex> index_;
    OffsetRange offset_range_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon::lumina
