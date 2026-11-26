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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/core/realtime/realtime_writer.h"

namespace arrow {
class Field;
class MemoryPool;
}  // namespace arrow

namespace paimon {

class AppendOnlyWriter;
class ArrowDeduplicateRealtimeStore;
class CoreOptions;
class DataFilePathFactory;
class MemoryPool;
class RealtimeContext;
class RealtimeSchemaLayout;
class RealtimeSegmentHandle;
class SchemaManager;
struct DataFileMeta;

/// Writer for an append table whose user-defined deduplicate key uses last-write-wins semantics.
class RealtimeDeduplicateWriter final : public RealtimeWriter {
 public:
    static Result<std::shared_ptr<RealtimeDeduplicateWriter>> Create(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::shared_ptr<AppendOnlyWriter>& file_writer,
        const std::shared_ptr<RealtimeSchemaLayout>& schema_layout, int64_t schema_id,
        const std::vector<std::string>& deduplicate_key_fields,
        const std::vector<std::shared_ptr<DataFileMeta>>& restored_data_files,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        const CoreOptions& options, const std::string& temp_directory,
        const std::shared_ptr<SchemaManager>& schema_manager,
        const std::shared_ptr<MemoryPool>& memory_pool);

    Status Write(std::unique_ptr<RecordBatch>&& batch) override;
    Status Compact(bool full_compaction) override;
    uint64_t GetMemoryUsage() const override;
    Status FlushMemory() override;
    Result<bool> CompactNotCompleted() override;
    Status Sync() override;
    Status Close() override;
    std::shared_ptr<Metrics> GetMetrics() const override;

 private:
    RealtimeDeduplicateWriter(std::shared_ptr<ArrowDeduplicateRealtimeStore> realtime_store,
                              std::shared_ptr<AppendOnlyWriter> file_writer,
                              std::shared_ptr<RealtimeSchemaLayout> schema_layout,
                              std::shared_ptr<arrow::Field> deduplicate_key_field,
                              int64_t next_offset, std::shared_ptr<MemoryPool> memory_pool);

    ArrowDeduplicateRealtimeStore* DeduplicateStore() const;
    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealStore(
        bool allow_spill) override;
    Result<CommitIncrement> PrepareCommitImpl(
        const std::vector<std::shared_ptr<RealtimeSegmentHandle>>& segments,
        bool wait_compaction) override;
    Status FlushSegment(const std::shared_ptr<RealtimeSegmentHandle>& segment);

    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<AppendOnlyWriter> file_writer_;
    std::shared_ptr<RealtimeSchemaLayout> schema_layout_;
    std::shared_ptr<arrow::Field> deduplicate_key_field_;
};

}  // namespace paimon
