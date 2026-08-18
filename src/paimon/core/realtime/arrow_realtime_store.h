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
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "paimon/realtime/realtime_store.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {
class MemoryPool;

/// Internal Arrow-backed implementation of the default `RealtimeStore`.
class ArrowRealtimeStore : public RealtimeStore {
 public:
    ArrowRealtimeStore(const std::shared_ptr<arrow::Schema>& write_schema,
                       StatisticsMode statistics_mode,
                       const std::shared_ptr<MemoryPool>& memory_pool,
                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Status Write(RealtimeWriteBatch&& write_batch) override;

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() override;

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) override;

    Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() override;

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view, int64_t offset_begin,
        const RealtimeQueryContext& context) override;

    Status AdvanceCommittedOffset(int64_t committed_end_offset) override;

    uint64_t GetMemoryUsage() const override;

 private:
    struct BatchStatistics {
        std::shared_ptr<arrow::StructArray> min_values;
        std::shared_ptr<arrow::StructArray> max_values;
        std::shared_ptr<arrow::Array> null_counts;
    };

    struct StoredBatch {
        std::shared_ptr<arrow::StructArray> data;
        std::vector<RecordBatch::RowKind> row_kinds;
        OffsetRange offset_range;
        std::optional<BatchStatistics> statistics;
        uint64_t memory_usage;
    };

    class Segment;
    class ReadView;
    class CommitBatchReader;
    class QueryBatchReader;

    Result<std::optional<BatchStatistics>> CollectStatistics(
        const std::shared_ptr<arrow::StructArray>& data) const;

    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    StatisticsMode statistics_mode_;
    mutable std::mutex mutex_;
    std::vector<StoredBatch> building_batches_;
    std::vector<std::shared_ptr<Segment>> sealed_segments_;
    std::optional<OffsetRange> building_range_;
    uint64_t building_memory_usage_ = 0;
};

}  // namespace paimon
