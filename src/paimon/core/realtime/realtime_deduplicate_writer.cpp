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

#include "paimon/core/realtime/realtime_deduplicate_writer.h"

#include <optional>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/append/append_only_writer.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/realtime/arrow_deduplicate_realtime_store.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_deduplicate_state.h"
#include "paimon/core/realtime/realtime_key_offset_index.h"
#include "paimon/core/realtime/realtime_offset_file_index_lookup.h"
#include "paimon/core/realtime/realtime_offset_utils.h"
#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/realtime/realtime_utils.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/macros.h"
#include "paimon/realtime/realtime_context.h"

namespace paimon {
namespace {

template <typename T>
void AppendAll(const std::vector<std::shared_ptr<T>>& source,
               std::vector<std::shared_ptr<T>>* target) {
    target->insert(target->end(), source.begin(), source.end());
}

}  // namespace

Result<std::shared_ptr<RealtimeDeduplicateWriter>> RealtimeDeduplicateWriter::Create(
    const std::map<std::string, std::string>& partition, int32_t bucket,
    const std::shared_ptr<RealtimeContext>& realtime_context,
    const std::shared_ptr<AppendOnlyWriter>& file_writer,
    const std::shared_ptr<RealtimeSchemaLayout>& schema_layout, int64_t schema_id,
    const std::vector<std::string>& deduplicate_key_fields,
    const std::vector<std::shared_ptr<DataFileMeta>>& restored_data_files,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory, const CoreOptions& options,
    const std::string& temp_directory, const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!realtime_context || !file_writer || !schema_layout || !data_file_path_factory ||
        !memory_pool) {
        return Status::Invalid("deduplicate real-time writer is missing a dependency");
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t key_position,
                           RealtimeUtils::GetDeduplicateKeyPosition(
                               schema_layout->StoreWriteSchema(), deduplicate_key_fields));
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(schema_layout->StoreWriteSchema()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> context,
                           RealtimeContextImpl::Cast(realtime_context));
    auto write_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportSchema(*schema_layout->StoreWriteSchema(), write_schema.get()));
    RealtimeStoreCreateRequest request{std::move(write_schema), options.ToMap(), memory_pool,
                                       RealtimeStoreMode::DEDUPLICATE,
                                       options.GetRealtimeStoreStatisticsMode()};
    request.deduplicate_key_fields = deduplicate_key_fields;
    if (options.RealtimeSpillEnabled()) {
        request.temp_directory = temp_directory;
        request.file_system = options.GetFileSystem();
    }
    PAIMON_ASSIGN_OR_RAISE(RealtimeStoreState store_state,
                           context->GetOrCreateRealtimeStore(
                               std::move(request), RealtimePartitionBucket(partition, bucket)));
    std::shared_ptr<ArrowDeduplicateRealtimeStore> store =
        std::dynamic_pointer_cast<ArrowDeduplicateRealtimeStore>(store_state.store);
    if (!store || !store_state.deduplicate_state) {
        return Status::Invalid("DEDUPLICATE mode requires the default Arrow store and state");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<RealtimeOffsetFileIndexLookup> created_lookup,
        RealtimeOffsetFileIndexLookup::Create(
            schema_layout->CommitSchema(), schema_id,
            schema_layout->StoreWriteSchema()->field(key_position), restored_data_files,
            data_file_path_factory, options.GetFileSystem(), options, schema_manager, memory_pool));
    PAIMON_RETURN_NOT_OK(
        store_state.deduplicate_state->AttachFileIndexLookup(created_lookup, store));
    return std::shared_ptr<RealtimeDeduplicateWriter>(new RealtimeDeduplicateWriter(
        store, file_writer, schema_layout, schema_layout->StoreWriteSchema()->field(key_position),
        store_state.initial_offset, memory_pool));
}

RealtimeDeduplicateWriter::RealtimeDeduplicateWriter(
    std::shared_ptr<ArrowDeduplicateRealtimeStore> realtime_store,
    std::shared_ptr<AppendOnlyWriter> file_writer,
    std::shared_ptr<RealtimeSchemaLayout> schema_layout,
    std::shared_ptr<arrow::Field> deduplicate_key_field, int64_t next_offset,
    std::shared_ptr<MemoryPool> memory_pool)
    : arrow_pool_(GetArrowPool(memory_pool)),
      realtime_store_(std::move(realtime_store)),
      file_writer_(std::move(file_writer)),
      schema_layout_(std::move(schema_layout)),
      deduplicate_key_field_(std::move(deduplicate_key_field)),
      next_offset_(next_offset) {}

Status RealtimeDeduplicateWriter::Write(std::unique_ptr<RecordBatch>&& batch) {
    if (!batch || !batch->GetData()) {
        return Status::Invalid("deduplicate real-time write batch is null");
    }
    if (batch->GetData()->length == 0) {
        return Status::OK();
    }
    std::vector<RecordBatch::RowKind> row_kinds = batch->GetRowKind();
    if (row_kinds.empty()) {
        row_kinds.assign(batch->GetData()->length, RecordBatch::RowKind::INSERT);
    }
    if (row_kinds.size() != static_cast<size_t>(batch->GetData()->length)) {
        return Status::Invalid("deduplicate row-kind count does not match batch row count");
    }

    std::lock_guard<std::mutex> lock(realtime_store_mutex_);
    PAIMON_ASSIGN_OR_RAISE(RealtimeOffsetUtils::ValidatedBatch validated,
                           RealtimeOffsetUtils::ValidateBatch(
                               batch.get(), schema_layout_->InputSchema(), next_offset_));
    const int32_t key_position =
        schema_layout_->StoreWriteSchema()->GetFieldIndex(deduplicate_key_field_->name());
    if (key_position < 0) {
        return Status::Invalid("deduplicate key field is missing from write data");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> keys,
        arrow::StructArray::Make({validated.data->field(key_position)}, {deduplicate_key_field_}));

    // Pin the complete lookup under the short store lock, then perform sidecar lookup without
    // holding it. Release the building-index snapshot before mutating the index, so ordinary
    // writes do not copy the entire map. The writer mutex keeps writes ordered while Advance may
    // proceed.
    RoaringBitmap64 previous_offsets;
    {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const KeyOffsetLookup> lookup,
                               realtime_store_->AcquireKeyOffsetLookup());
        PAIMON_ASSIGN_OR_RAISE(previous_offsets, lookup->LookupOffsets(keys));
    }
    PAIMON_RETURN_NOT_OK(realtime_store_->WriteAndLookup(validated.data, row_kinds,
                                                         validated.offset_range, previous_offsets));
    next_offset_ = validated.offset_range.end;
    has_building_data_ = true;
    return Status::OK();
}

Status RealtimeDeduplicateWriter::SealCurrentSegment(bool spill) {
    PAIMON_ASSIGN_OR_RAISE(
        std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
        spill ? realtime_store_->SealForCommit() : realtime_store_->FreezeBuildingSegment());
    std::lock_guard<std::mutex> lock(realtime_store_mutex_);
    if (!segment) {
        return Status::OK();
    }
    if (!segment.value()) {
        return Status::Invalid("deduplicate real-time store sealed a null segment");
    }
    has_building_data_ = next_offset_ > segment.value()->GetOffsetRange().end;
    sealed_segments_.push_back(std::move(segment.value()));
    return Status::OK();
}

Status RealtimeDeduplicateWriter::Seal() {
    std::lock_guard<std::mutex> lock(prepare_mutex_);
    return SealCurrentSegment(/*spill=*/true);
}

Result<CommitIncrement> RealtimeDeduplicateWriter::PrepareCommit(bool wait_compaction) {
    std::lock_guard<std::mutex> prepare_lock(prepare_mutex_);
    PAIMON_RETURN_NOT_OK(SealCurrentSegment(/*spill=*/false));
    std::vector<std::shared_ptr<RealtimeSegmentHandle>> segments;
    {
        std::lock_guard<std::mutex> store_lock(realtime_store_mutex_);
        segments.swap(sealed_segments_);
    }
    if (segments.empty()) {
        return CommitIncrement(DataIncrement({}, {}, {}, {}, {}),
                               CompactIncrement({}, {}, {}, {}, {}), nullptr);
    }
    auto deleted_offsets = std::make_shared<RoaringBitmap64>();
    std::vector<std::shared_ptr<DataFileMeta>> new_files;
    std::vector<std::shared_ptr<IndexFileMeta>> new_index_files;
    std::vector<std::shared_ptr<IndexFileMeta>> deleted_index_files;
    for (const std::shared_ptr<RealtimeSegmentHandle>& segment : segments) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const RoaringBitmap64> offset_deletions,
                               ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(segment));
        *deleted_offsets |= *offset_deletions;

        if (segment->GetRowCount() == 0) {
            continue;
        }
        PAIMON_RETURN_NOT_OK(FlushSegment(segment));
        PAIMON_ASSIGN_OR_RAISE(CommitIncrement segment_increment,
                               file_writer_->PrepareCommit(wait_compaction));
        const DataIncrement& increment = segment_increment.GetNewFilesIncrement();
        if (increment.NewFiles().empty() || !increment.DeletedFiles().empty() ||
            !increment.ChangelogFiles().empty() ||
            !segment_increment.GetCompactIncrement().IsEmpty() ||
            segment_increment.GetCompactDeletionFile()) {
            return Status::Invalid("deduplicate segment must produce append data files only");
        }
        int64_t remaining_rows = segment->GetRowCount();
        for (const std::shared_ptr<DataFileMeta>& data_file : increment.NewFiles()) {
            if (!data_file || data_file->row_count <= 0 || data_file->row_count > remaining_rows) {
                return Status::Invalid("deduplicate segment and data-file row counts differ");
            }
            remaining_rows -= data_file->row_count;
            new_files.push_back(data_file);
        }
        if (remaining_rows != 0) {
            return Status::Invalid("deduplicate segment and data-file row counts differ");
        }
        AppendAll(increment.NewIndexFiles(), &new_index_files);
        AppendAll(increment.DeletedIndexFiles(), &deleted_index_files);
    }

    DataIncrement data_increment(std::move(new_files), {}, {}, std::move(new_index_files),
                                 std::move(deleted_index_files));
    CompactIncrement compact_increment({}, {}, {}, {}, {});
    CommitIncrement result(data_increment, compact_increment, nullptr);
    result.SetRealtimeDeletedOffsets(std::move(deleted_offsets));
    result.SetRealtimeOffsetRange(OffsetRange(segments.front()->GetOffsetRange().begin,
                                              segments.back()->GetOffsetRange().end));
    return result;
}

Status RealtimeDeduplicateWriter::FlushSegment(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> readers,
                           realtime_store_->CreateCommitReaders(segment));
    ConcatBatchReader reader(std::move(readers), arrow_pool_);
    ScopeGuard reader_guard([&reader]() { reader.Close(); });
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
            return Status::Invalid("deduplicate commit reader returned a non-StructArray");
        }
        std::shared_ptr<arrow::StructArray> data =
            checked_pointer_cast<arrow::StructArray>(imported);
        emitted_rows += data->length();

        auto output = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*data, output.get()));
        RecordBatchBuilder builder(output.get());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> record_batch, builder.Finish());
        PAIMON_RETURN_NOT_OK(file_writer_->Write(std::move(record_batch)));
    }
    if (emitted_rows != segment->GetRowCount()) {
        return Status::Invalid("deduplicate commit reader row count does not match segment");
    }
    return Status::OK();
}

Status RealtimeDeduplicateWriter::Compact(bool) {
    return Status::Invalid("deduplicate real-time write does not support explicit compaction");
}

uint64_t RealtimeDeduplicateWriter::GetMemoryUsage() const {
    return realtime_store_->GetMemoryUsage();
}

Status RealtimeDeduplicateWriter::FlushMemory() {
    return Status::OK();
}

Result<bool> RealtimeDeduplicateWriter::CompactNotCompleted() {
    return file_writer_->CompactNotCompleted();
}

Status RealtimeDeduplicateWriter::Sync() {
    return file_writer_->Sync();
}

Status RealtimeDeduplicateWriter::Close() {
    return file_writer_->Close();
}

bool RealtimeDeduplicateWriter::HasUnpreparedRealtimeData() const {
    std::lock_guard<std::mutex> lock(realtime_store_mutex_);
    return has_building_data_ || !sealed_segments_.empty();
}

std::shared_ptr<Metrics> RealtimeDeduplicateWriter::GetMetrics() const {
    return file_writer_->GetMetrics();
}

}  // namespace paimon
