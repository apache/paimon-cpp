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

#include "paimon/core/table/source/append_only_table_read.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/reader/predicate_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/operation/data_evolution_split_read.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_reader.h"
#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/realtime/realtime_store_read_pipeline.h"
#include "paimon/core/table/source/append_count_reader.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/realtime/realtime_store.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
class DataSplit;
class Executor;
class FileStorePathFactory;
class MemoryPool;

namespace {

class RemoveRealtimeOffsetBatchReader final : public BatchReader {
 public:
    explicit RemoveRealtimeOffsetBatchReader(std::unique_ptr<BatchReader>&& reader)
        : reader_(std::move(reader)) {}

    Result<ReadBatch> NextBatch() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, reader_->NextBatch());
        PAIMON_RETURN_NOT_OK(RemoveOffset(&batch));
        return batch;
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, reader_->NextBatchWithBitmap());
        PAIMON_RETURN_NOT_OK(RemoveOffset(&batch.first));
        return batch;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

 private:
    Status RemoveOffset(ReadBatch* batch) const {
        if (IsEofBatch(*batch)) {
            return Status::OK();
        }
        auto& [c_array, c_schema] = *batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (!array || array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("deduplicate disk reader must return a StructArray");
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::StructArray> output,
            ArrowUtils::RemoveFieldFromStructArray(checked_pointer_cast<arrow::StructArray>(array),
                                                   SpecialFields::RealtimeOffset().Name()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*output, c_array.get(), c_schema.get()));
        return Status::OK();
    }

    std::unique_ptr<BatchReader> reader_;
};

}  // namespace

AppendOnlyTableRead::AppendOnlyTableRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                                         const std::shared_ptr<InternalReadContext>& context,
                                         const std::shared_ptr<MemoryPool>& memory_pool,
                                         const std::shared_ptr<Executor>& executor)
    : context_(context) {
    const auto& core_options = context->GetCoreOptions();
    if (core_options.DataEvolutionEnabled()) {
        // add data evolution first
        split_reads_.push_back(
            std::make_unique<DataEvolutionSplitRead>(path_factory, context, memory_pool, executor));
    } else {
        split_reads_.push_back(
            std::make_unique<RawFileSplitRead>(path_factory, context, memory_pool, executor));
    }
}

Result<std::unique_ptr<BatchReader>> AppendOnlyTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    std::shared_ptr<RealtimeSplit> realtime_split = std::dynamic_pointer_cast<RealtimeSplit>(split);
    if (!realtime_split) {
        return CreateDiskReader(split);
    }
    return CreateRealtimeReader(realtime_split, /*release_ticket=*/true);
}

Result<std::unique_ptr<BatchReader>> AppendOnlyTableRead::CreateReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.reserve(splits.size());
    std::vector<std::shared_ptr<RealtimeSplit>> realtime_splits;
    ScopeGuard cleanup_guard([&]() {
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            if (reader) {
                reader->Close();
            }
        }
    });
    for (const std::shared_ptr<Split>& split : splits) {
        std::shared_ptr<RealtimeSplit> realtime_split =
            std::dynamic_pointer_cast<RealtimeSplit>(split);
        if (realtime_split) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                                   CreateRealtimeReader(realtime_split,
                                                        /*release_ticket=*/false));
            readers.push_back(std::move(reader));
            realtime_splits.push_back(std::move(realtime_split));
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, CreateDiskReader(split));
            readers.push_back(std::move(reader));
        }
    }

    if (!realtime_splits.empty()) {
        const std::shared_ptr<RealtimeContext> realtime_context = context_->GetRealtimeContext();
        if (!realtime_context) {
            return Status::Invalid("reading a real-time split requires a real-time context");
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                               RealtimeContextImpl::Cast(realtime_context));
        for (const std::shared_ptr<RealtimeSplit>& realtime_split : realtime_splits) {
            PAIMON_RETURN_NOT_OK(
                realtime_context_impl->ReleaseReadView(realtime_split->OpaqueTicket()));
        }
    }
    return std::make_unique<ConcatBatchReader>(std::move(readers), context_->GetArrowMemoryPool());
}

Result<std::unique_ptr<BatchReader>> AppendOnlyTableRead::CreateRealtimeReader(
    const std::shared_ptr<RealtimeSplit>& realtime_split, bool release_ticket) {
    if (realtime_split->Version() != RealtimeSplit::kCurrentVersion) {
        return Status::Invalid("unsupported real-time split version");
    }
    const std::shared_ptr<RealtimeContext> realtime_context = context_->GetRealtimeContext();
    if (!realtime_context) {
        return Status::Invalid("reading a real-time split requires a real-time context");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                           RealtimeContextImpl::Cast(realtime_context));
    PAIMON_ASSIGN_OR_RAISE(RealtimePartitionBucketView memory,
                           realtime_context_impl->ResolveReadView(realtime_split->OpaqueTicket()));
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.reserve(realtime_split->DiskSplits().size() + 1);
    ScopeGuard readers_guard([&readers]() {
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            if (reader) {
                reader->Close();
            }
        }
    });
    const RealtimePartitionBucket expected_partition_bucket(realtime_split->Partition(),
                                                            realtime_split->Bucket());
    if (memory.partition_bucket != expected_partition_bucket) {
        return Status::Invalid("real-time read-view ticket belongs to another partition-bucket");
    }
    const std::optional<OffsetRange> memory_range = memory.read_view->GetOffsetRange();
    if (!memory_range || memory_range->end != realtime_split->MemoryEndOffset()) {
        return Status::Invalid("real-time read-view ticket does not match the split offset range");
    }

    for (const std::shared_ptr<Split>& disk_split : realtime_split->DiskSplits()) {
        std::unique_ptr<BatchReader> disk_reader;
        if (memory.mode == RealtimeStoreMode::DEDUPLICATE) {
            PAIMON_ASSIGN_OR_RAISE(
                disk_reader, CreateDeduplicateDiskReader(disk_split, memory.offset_deletions));
        } else {
            PAIMON_ASSIGN_OR_RAISE(disk_reader, CreateDiskReader(disk_split));
        }
        readers.push_back(std::move(disk_reader));
    }

    std::shared_ptr<arrow::Schema> table_schema =
        DataField::ConvertDataFieldsToArrowSchema(context_->GetTableSchema()->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RealtimeSchemaLayout> schema_layout,
                           RealtimeSchemaLayout::Create(memory.mode, table_schema));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RealtimeStoreReadPipeline> pipeline,
                           RealtimeStoreReadPipeline::Create(
                               context_->GetReadSchema(), *schema_layout, context_->GetMemoryPool(),
                               context_->GetArrowMemoryPool()));
    const std::shared_ptr<arrow::Schema>& store_read_schema = pipeline->StoreReadSchema();
    auto c_read_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*store_read_schema, c_read_schema.get()));
    ScopeGuard schema_guard([schema = c_read_schema.get()]() { ArrowSchemaRelease(schema); });
    std::map<std::string, int32_t> realtime_field_name_to_index;
    for (int32_t i = 0; i < store_read_schema->num_fields(); ++i) {
        realtime_field_name_to_index.emplace(store_read_schema->field(i)->name(), i);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> realtime_predicate,
                           PredicateUtils::CreatePickedFieldFilter(context_->GetPredicate(),
                                                                   realtime_field_name_to_index));
    RealtimeQueryContext query_context{c_read_schema.get(), std::move(realtime_predicate),
                                       context_->GetCoreOptions().GetReadBatchSize()};
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> memory_readers,
                           memory.store->CreateQueryReaders(memory.read_view, query_context));
    const size_t first_memory_reader = readers.size();
    readers.reserve(readers.size() + memory_readers.size());
    for (std::unique_ptr<BatchReader>& memory_reader : memory_readers) {
        readers.push_back(std::move(memory_reader));
    }

    for (size_t i = first_memory_reader; i < readers.size(); ++i) {
        std::unique_ptr<BatchReader>& memory_reader = readers[i];
        if (!memory_reader) {
            return Status::Invalid("append-only real-time store returned a null query reader");
        }
        PAIMON_ASSIGN_OR_RAISE(memory_reader,
                               pipeline->Wrap(std::move(memory_reader),
                                              OffsetRange(realtime_split->CommittedEndOffset(),
                                                          realtime_split->MemoryEndOffset())));
        memory_reader = std::make_unique<CompleteRowKindBatchReader>(
            std::move(memory_reader), context_->GetArrowMemoryPool());
        if (context_->EnablePredicateFilter() && context_->GetPredicate()) {
            PAIMON_ASSIGN_OR_RAISE(
                memory_reader,
                PredicateBatchReader::Create(std::move(memory_reader), context_->GetPredicate(),
                                             context_->GetArrowMemoryPool()));
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RealtimeReader> realtime_reader,
                               RealtimeReader::Create(memory.read_view, std::move(memory_reader)));
        memory_reader = std::move(realtime_reader);
    }
    if (release_ticket) {
        PAIMON_RETURN_NOT_OK(
            realtime_context_impl->ReleaseReadView(realtime_split->OpaqueTicket()));
    }
    std::unique_ptr<BatchReader> result =
        std::make_unique<ConcatBatchReader>(std::move(readers), context_->GetArrowMemoryPool());
    readers_guard.Release();
    return result;
}

Result<std::unique_ptr<BatchReader>> AppendOnlyTableRead::CreateDiskReader(
    const std::shared_ptr<Split>& split) {
    for (const auto& read : split_reads_) {
        PAIMON_ASSIGN_OR_RAISE(bool matched, read->Match(split, /*force_keep_delete=*/false));
        if (matched) {
            return read->CreateReader(split);
        }
    }
    return Status::Invalid("create reader failed, not read match with split.");
}

Result<std::unique_ptr<BatchReader>> AppendOnlyTableRead::CreateDeduplicateDiskReader(
    const std::shared_ptr<Split>& split,
    const std::shared_ptr<const RoaringBitmap64>& offset_deletions) {
    if (!offset_deletions) {
        return Status::Invalid("deduplicate real-time split does not pin an offset DV");
    }
    auto raw_read = dynamic_cast<RawFileSplitRead*>(split_reads_.front().get());
    if (!raw_read) {
        return Status::NotImplemented(
            "deduplicate real-time offset-DV overlay only supports raw append reads");
    }

    std::shared_ptr<DataSplitImpl> data_split;
    std::optional<std::vector<Range>> row_ranges;
    if (auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(indexed_split->Validate());
        if (!indexed_split->Scores().empty()) {
            return Status::NotImplemented(
                "deduplicate real-time reads do not support scored indexed splits yet");
        }
        data_split = std::dynamic_pointer_cast<DataSplitImpl>(indexed_split->GetDataSplit());
        row_ranges = indexed_split->RowRanges();
    } else {
        data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
    }
    if (!data_split) {
        return Status::Invalid("deduplicate real-time disk split is not a data split");
    }

    if (offset_deletions->IsEmpty()) {
        return raw_read->CreateReader(data_split->Partition(), data_split->Bucket(),
                                      data_split->DataFiles(), data_split->DeletionFiles(),
                                      row_ranges);
    }

    std::shared_ptr<arrow::Schema> read_schema = context_->GetReadSchema();
    const std::string& offset_name = SpecialFields::RealtimeOffset().Name();
    const bool expose_offset = read_schema->GetFieldIndex(offset_name) >= 0;
    if (!expose_offset) {
        arrow::FieldVector fields = read_schema->fields();
        PAIMON_ASSIGN_OR_RAISE(DataField offset_field,
                               context_->GetTableSchema()->GetField(offset_name));
        fields.push_back(DataField::ConvertDataFieldToArrowField(offset_field));
        read_schema = arrow::schema(std::move(fields), read_schema->metadata());
    }

    // TODO(xinyu.lxy): Build a file-specific visibility predicate for each data file. Use the
    // `_REALTIME_OFFSET` min/max statistics in DataFileMeta to remove deleted offsets that cannot
    // intersect that file, instead of pushing the complete bucket offset DV to every file.
    std::vector<Literal> deleted_offsets;
    deleted_offsets.reserve(offset_deletions->Cardinality());
    for (RoaringBitmap64::Iterator offset = offset_deletions->Begin();
         offset != offset_deletions->End(); ++offset) {
        deleted_offsets.emplace_back(static_cast<int64_t>(*offset));
    }
    const int32_t offset_position = read_schema->GetFieldIndex(offset_name);
    std::shared_ptr<Predicate> visibility_predicate =
        PredicateBuilder::NotIn(offset_position, offset_name, FieldType::BIGINT, deleted_offsets);
    std::shared_ptr<Predicate> predicate = visibility_predicate;
    if (context_->GetPredicate()) {
        PAIMON_ASSIGN_OR_RAISE(predicate, PredicateBuilder::And({context_->GetPredicate(),
                                                                 std::move(visibility_predicate)}));
    }

    DeletionVector::Factory committed_factory = DeletionVector::CreateFactory(
        context_->GetCoreOptions().GetFileSystem(),
        DeletionVector::CreateDeletionFileMap(data_split->DataFiles(), data_split->DeletionFiles()),
        context_->GetMemoryPool());
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BatchReader> reader,
        raw_read->CreateReader(data_split->Partition(), data_split->Bucket(),
                               data_split->DataFiles(), std::move(committed_factory), row_ranges,
                               read_schema, predicate));
    if (!expose_offset) {
        reader = std::make_unique<RemoveRealtimeOffsetBatchReader>(std::move(reader));
    }
    return reader;
}

Result<std::unique_ptr<CountReader>> AppendOnlyTableRead::CreateCountReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    for (const std::shared_ptr<Split>& split : splits) {
        if (std::dynamic_pointer_cast<RealtimeSplit>(split)) {
            return Status::NotImplemented(
                "CreateCountReader does not support process-local real-time splits");
        }
    }
    if (context_->GetPredicate() != nullptr) {
        return Status::NotImplemented(
            "CreateCountReader with predicate pushdown is not supported yet");
    }

    return std::make_unique<AppendCountReader>(splits, context_->GetCoreOptions().GetFileSystem(),
                                               context_->GetMemoryPool());
}

}  // namespace paimon
