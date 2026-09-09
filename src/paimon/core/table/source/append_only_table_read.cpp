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

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/reader/predicate_batch_reader.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/data_evolution_split_read.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_offset_utils.h"
#include "paimon/core/realtime/realtime_reader.h"
#include "paimon/core/realtime/realtime_store_read_pipeline.h"
#include "paimon/core/table/source/append_count_reader.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/realtime/realtime_store.h"
#include "paimon/status.h"

namespace paimon {
class DataSplit;
class Executor;
class FileStorePathFactory;
class MemoryPool;

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
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> disk_reader,
                               CreateDiskReader(disk_split));
        readers.push_back(std::move(disk_reader));
    }

    std::shared_ptr<arrow::Schema> table_schema =
        DataField::ConvertDataFieldsToArrowSchema(context_->GetTableSchema()->Fields());
    std::shared_ptr<arrow::Schema> realtime_write_schema =
        RealtimeOffsetUtils::CreateInputSchema(table_schema);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RealtimeStoreReadPipeline> pipeline,
                           RealtimeStoreReadPipeline::Create(
                               context_->GetReadSchema(), realtime_write_schema,
                               context_->GetMemoryPool(), context_->GetArrowMemoryPool()));
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
    RealtimeQueryContext query_context{c_read_schema.get(), std::move(realtime_predicate)};
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
