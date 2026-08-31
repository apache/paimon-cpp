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
#include <mutex>
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/utils/batch_writer.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/realtime/realtime_store.h"

namespace arrow {
class MemoryPool;
class Schema;
}  // namespace arrow

namespace paimon {

class MemoryPool;
class MergeTreeWriter;
class FieldsComparator;
class RealtimeContextImpl;
struct RealtimeStoreState;

class RealtimePrimaryKeyWriter final : public BatchWriter {
 public:
    static Result<std::shared_ptr<RealtimePrimaryKeyWriter>> Create(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        const std::shared_ptr<arrow::Schema>& write_schema,
        const std::shared_ptr<arrow::Schema>& transport_schema,
        const std::vector<std::string>& trimmed_primary_keys,
        const std::shared_ptr<FieldsComparator>& key_comparator, const CoreOptions& options,
        const std::shared_ptr<RealtimeContextImpl>& realtime_context,
        const RealtimeStoreState& store_state, int64_t restored_max_sequence_number,
        const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
        const std::shared_ptr<MemoryPool>& memory_pool);

    Status Write(std::unique_ptr<RecordBatch>&& batch) override;
    Result<CommitIncrement> PrepareCommit(bool wait_compaction) override;
    Status Compact(bool full_compaction) override;
    uint64_t GetMemoryUsage() const override;
    Status FlushMemory() override;
    Result<bool> CompactNotCompleted() override;
    Status Sync() override;
    Status Close() override;
    std::shared_ptr<Metrics> GetMetrics() const override;

 private:
    RealtimePrimaryKeyWriter(const std::shared_ptr<RealtimeStore>& realtime_store,
                             const std::shared_ptr<MergeTreeWriter>& merge_tree_writer,
                             const std::shared_ptr<RealtimeContextImpl>& realtime_context,
                             const RealtimePartitionBucket& partition_bucket,
                             const std::shared_ptr<arrow::Schema>& write_schema,
                             const std::shared_ptr<arrow::Schema>& transport_schema,
                             const std::shared_ptr<arrow::Schema>& key_schema,
                             const std::vector<std::string>& trimmed_primary_keys,
                             const std::shared_ptr<FieldsComparator>& key_comparator,
                             const CoreOptions& options, int64_t next_offset,
                             int64_t last_sequence_number,
                             const std::shared_ptr<MemoryPool>& memory_pool);

    Status FlushSegment(const std::shared_ptr<RealtimeSegmentHandle>& segment,
                        const OffsetRange& sealed_offsets);

    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<RealtimeStore> realtime_store_;
    std::shared_ptr<MergeTreeWriter> merge_tree_writer_;
    std::shared_ptr<RealtimeContextImpl> realtime_context_;
    RealtimePartitionBucket partition_bucket_;
    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<arrow::Schema> transport_schema_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::vector<std::string> trimmed_primary_keys_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    CoreOptions options_;
    int64_t next_offset_;
    int64_t last_sequence_number_;
    std::mutex realtime_store_mutex_;
    std::mutex prepare_mutex_;
};

}  // namespace paimon
