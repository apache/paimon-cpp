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

#include "paimon/core/realtime/realtime_append_only_writer.h"

#include <cstdint>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/append/append_only_writer.h"
#include "paimon/core/core_options.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_offset_utils.h"
#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/macros.h"
#include "paimon/realtime/realtime_context.h"

namespace paimon {

Result<std::shared_ptr<RealtimeAppendOnlyWriter>> RealtimeAppendOnlyWriter::Create(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    const std::shared_ptr<RealtimeContext>& realtime_context,
    const std::shared_ptr<AppendOnlyWriter>& file_writer,
    const std::shared_ptr<RealtimeSchemaLayout>& schema_layout, const CoreOptions& options,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!realtime_context) {
        return Status::Invalid("real-time context is null");
    }
    if (!schema_layout) {
        return Status::Invalid("append real-time schema layout is null");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                           RealtimeContextImpl::Cast(realtime_context));
    auto write_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportSchema(*schema_layout->StoreWriteSchema(), write_schema.get()));
    RealtimeStoreCreateRequest request{std::move(write_schema), options.ToMap(), memory_pool,
                                       RealtimeStoreMode::APPEND_ONLY,
                                       options.GetRealtimeStoreStatisticsMode()};
    PAIMON_ASSIGN_OR_RAISE(RealtimeStoreState store_state,
                           realtime_context_impl->GetOrCreateRealtimeStore(
                               std::move(request), RealtimePartitionBucket(partition, bucket)));
    return std::shared_ptr<RealtimeAppendOnlyWriter>(new RealtimeAppendOnlyWriter(
        store_state.store, file_writer, schema_layout, store_state.initial_offset, memory_pool));
}

RealtimeAppendOnlyWriter::RealtimeAppendOnlyWriter(
    const std::shared_ptr<RealtimeStore>& realtime_store,
    const std::shared_ptr<AppendOnlyWriter>& file_writer,
    const std::shared_ptr<RealtimeSchemaLayout>& schema_layout, int64_t next_offset,
    const std::shared_ptr<MemoryPool>& memory_pool)
    : arrow_pool_(GetArrowPool(memory_pool)),
      realtime_store_(realtime_store),
      file_writer_(file_writer),
      schema_layout_(schema_layout),
      next_offset_(next_offset) {}

Status RealtimeAppendOnlyWriter::Write(std::unique_ptr<RecordBatch>&& batch) {
    if (!batch || !batch->GetData()) {
        return Status::Invalid("append real-time write batch is null");
    }
    for (RecordBatch::RowKind row_kind : batch->GetRowKind()) {
        if (row_kind != RecordBatch::RowKind::INSERT) {
            PAIMON_ASSIGN_OR_RAISE(const RowKind* kind,
                                   RowKind::FromByteValue(static_cast<int8_t>(row_kind)));
            return Status::Invalid("Append only writer can not accept record batch with RowKind ",
                                   kind->Name());
        }
    }

    int64_t row_count = batch->GetData()->length;
    if (row_count == 0) {
        return Status::OK();
    }
    std::lock_guard<std::mutex> lock(realtime_store_mutex_);
    PAIMON_ASSIGN_OR_RAISE(RealtimeOffsetUtils::ValidatedBatch validated,
                           RealtimeOffsetUtils::ValidateBatch(
                               batch.get(), schema_layout_->InputSchema(), next_offset_));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*validated.data, batch->GetData()));
    PAIMON_RETURN_NOT_OK(
        realtime_store_->Write(RealtimeWriteBatch{std::move(batch), validated.offset_range}));
    next_offset_ = validated.offset_range.end;
    has_building_data_ = true;
    return Status::OK();
}

Status RealtimeAppendOnlyWriter::SealCurrentSegment() {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                           realtime_store_->SealForCommit());
    if (segment) {
        if (!segment.value()) {
            return Status::Invalid("append real-time store sealed a null segment");
        }
        sealed_segments_.push_back(std::move(segment.value()));
        has_building_data_ = false;
    }
    return Status::OK();
}

Status RealtimeAppendOnlyWriter::Seal() {
    std::lock_guard<std::mutex> lock(realtime_store_mutex_);
    return SealCurrentSegment();
}

Result<CommitIncrement> RealtimeAppendOnlyWriter::PrepareCommit(bool wait_compaction) {
    std::lock_guard<std::mutex> lock(prepare_mutex_);
    std::vector<std::shared_ptr<RealtimeSegmentHandle>> segments;
    {
        std::lock_guard<std::mutex> realtime_store_lock(realtime_store_mutex_);
        PAIMON_RETURN_NOT_OK(SealCurrentSegment());
        segments.swap(sealed_segments_);
    }
    for (const std::shared_ptr<RealtimeSegmentHandle>& segment : segments) {
        PAIMON_RETURN_NOT_OK(FlushSegment(segment));
    }
    PAIMON_ASSIGN_OR_RAISE(CommitIncrement increment, file_writer_->PrepareCommit(wait_compaction));
    if (!segments.empty()) {
        increment.SetRealtimeOffsetRange(OffsetRange(segments.front()->GetOffsetRange().begin,
                                                     segments.back()->GetOffsetRange().end));
    }
    return increment;
}

Status RealtimeAppendOnlyWriter::FlushSegment(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> readers,
                           realtime_store_->CreateCommitReaders(segment));
    ConcatBatchReader reader(std::move(readers), arrow_pool_);
    ScopeGuard reader_guard([&reader]() { reader.Close(); });
    const int64_t expected_rows = segment->GetRowCount();
    int64_t emitted_rows = 0;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader.NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (!imported || imported->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("real-time store commit reader returned a non-StructArray");
        }
        std::shared_ptr<arrow::StructArray> struct_array =
            checked_pointer_cast<arrow::StructArray>(imported);
        PAIMON_ASSIGN_OR_RAISE(struct_array,
                               ArrowUtils::RemoveFieldFromStructArray(
                                   struct_array, SpecialFields::RealtimeOffset().Name()));
        if (!struct_array->type()->Equals(
                arrow::struct_(schema_layout_->CommitSchema()->fields()))) {
            return Status::Invalid(
                "real-time store commit reader schema does not match table write schema");
        }

        int64_t row_count = struct_array->length();
        if (row_count > expected_rows - emitted_rows) {
            return Status::Invalid(
                "real-time store commit readers returned more rows than the sealed offset range");
        }
        emitted_rows += row_count;

        auto output = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, output.get()));
        RecordBatchBuilder builder(output.get());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> record_batch, builder.Finish());
        PAIMON_RETURN_NOT_OK(file_writer_->Write(std::move(record_batch)));
    }
    if (emitted_rows != expected_rows) {
        return Status::Invalid(
            "real-time store commit readers returned fewer rows than the sealed offset range");
    }
    return Status::OK();
}

Status RealtimeAppendOnlyWriter::Compact(bool) {
    return Status::Invalid("real-time append write does not support explicit compaction");
}

uint64_t RealtimeAppendOnlyWriter::GetMemoryUsage() const {
    // The first implementation does not spill sealed or building segments through
    // WriterMemoryManager.
    return 0;
}

Status RealtimeAppendOnlyWriter::FlushMemory() {
    return Status::OK();
}

Result<bool> RealtimeAppendOnlyWriter::CompactNotCompleted() {
    return file_writer_->CompactNotCompleted();
}

Status RealtimeAppendOnlyWriter::Sync() {
    return file_writer_->Sync();
}

Status RealtimeAppendOnlyWriter::Close() {
    // The shared real-time context owns the real-time store for scans and later writers.
    return file_writer_->Close();
}

bool RealtimeAppendOnlyWriter::HasUnpreparedRealtimeData() const {
    std::lock_guard<std::mutex> lock(realtime_store_mutex_);
    return has_building_data_ || !sealed_segments_.empty();
}

std::shared_ptr<Metrics> RealtimeAppendOnlyWriter::GetMetrics() const {
    return file_writer_->GetMetrics();
}

}  // namespace paimon
