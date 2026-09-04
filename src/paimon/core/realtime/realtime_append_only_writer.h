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
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/utils/batch_writer.h"
#include "paimon/realtime/realtime_store.h"

namespace arrow {
class MemoryPool;
class Schema;
}  // namespace arrow

namespace paimon {

class AppendOnlyWriter;
class CoreOptions;
class MemoryPool;
class RealtimeContext;

class RealtimeAppendOnlyWriter : public BatchWriter {
 public:
    static Result<std::shared_ptr<RealtimeAppendOnlyWriter>> Create(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::shared_ptr<AppendOnlyWriter>& file_writer,
        const std::shared_ptr<RealtimeSchemaLayout>& schema_layout, const CoreOptions& options,
        const std::shared_ptr<MemoryPool>& memory_pool);

    Status Write(std::unique_ptr<RecordBatch>&& batch) override;

    Status Seal() override;

    Result<CommitIncrement> PrepareCommit(bool wait_compaction) override;

    Status Compact(bool full_compaction) override;

    uint64_t GetMemoryUsage() const override;

    Status FlushMemory() override;

    Result<bool> CompactNotCompleted() override;

    Status Sync() override;

    Status Close() override;

    std::shared_ptr<Metrics> GetMetrics() const override;

 private:
    RealtimeAppendOnlyWriter(const std::shared_ptr<RealtimeStore>& realtime_store,
                             const std::shared_ptr<AppendOnlyWriter>& file_writer,
                             const std::shared_ptr<RealtimeSchemaLayout>& schema_layout,
                             int64_t next_offset, const std::shared_ptr<MemoryPool>& memory_pool);

    Status FlushSegment(const std::shared_ptr<RealtimeSegmentHandle>& segment);
    Status SealCurrentSegment();

    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<RealtimeStore> realtime_store_;
    std::shared_ptr<AppendOnlyWriter> file_writer_;
    std::shared_ptr<RealtimeSchemaLayout> schema_layout_;
    std::vector<std::shared_ptr<RealtimeSegmentHandle>> sealed_segments_;
    int64_t next_offset_;
    std::mutex realtime_store_mutex_;
    std::mutex prepare_mutex_;
};

}  // namespace paimon
