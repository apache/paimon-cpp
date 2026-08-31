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

#include "paimon/core/table/source/key_value_table_read.h"

#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/merged_key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/core/operation/merge_file_split_read.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_primary_key_reader.h"
#include "paimon/core/realtime/realtime_reader.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/pk_count_reader.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/core/utils/primary_key_table_utils.h"
#include "paimon/status.h"

namespace paimon {
class DataSplit;
class Executor;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;
struct ColumnarBatchContext;

namespace {

Result<std::shared_ptr<arrow::Schema>> CreateRealtimePrimaryKeyQueryTransportSchema(
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema) {
    arrow::FieldVector transport_value_fields;
    transport_value_fields.reserve(key_schema->num_fields() + value_schema->num_fields());
    std::unordered_set<int32_t> field_ids;
    for (const std::shared_ptr<arrow::Field>& field : key_schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t field_id, NestedProjectionUtils::GetPaimonFieldId(field));
        if (field_ids.insert(field_id).second) {
            transport_value_fields.push_back(field);
        }
    }
    for (const std::shared_ptr<arrow::Field>& field : value_schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t field_id, NestedProjectionUtils::GetPaimonFieldId(field));
        if (field_ids.insert(field_id).second) {
            transport_value_fields.push_back(field);
        }
    }
    return RealtimePrimaryKeyLayout::CreateSchema(transport_value_fields);
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>> CreateMemoryReaders(
    const std::shared_ptr<RealtimeSplit>& split, const RealtimePartitionBucketView& memory,
    const std::shared_ptr<arrow::Schema>& transport_schema,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    auto c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*transport_schema, c_schema.get()));
    ScopeGuard schema_guard([schema = c_schema.get()]() { ArrowSchemaRelease(schema); });
    RealtimeQueryContext query_context{c_schema.get(), /*predicate=*/nullptr};
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> batch_readers,
                           memory.store->CreateQueryReaders(memory.read_view, query_context));
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<KeyValueRecordReader>> realtime_primary_key_readers,
        RealtimePrimaryKeyReaderFactory::Create(
            std::move(batch_readers),
            OffsetRange(split->CommittedEndOffset(), split->MemoryEndOffset()), key_schema,
            value_schema, memory_pool));
    std::vector<std::unique_ptr<KeyValueRecordReader>> result;
    result.reserve(realtime_primary_key_readers.size());
    for (std::unique_ptr<KeyValueRecordReader>& realtime_primary_key_reader :
         realtime_primary_key_readers) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<MergeFunction> merge,
                               PrimaryKeyTableUtils::CreateMergeFunction(
                                   value_schema, context->GetTableSchema()->PrimaryKeys(),
                                   context->GetCoreOptions(), memory_pool));
        result.push_back(std::make_unique<MergedKeyValueRecordReader>(
            std::move(realtime_primary_key_reader), key_comparator,
            std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge))));
    }
    return result;
}

}  // namespace

KeyValueTableRead::KeyValueTableRead(
    std::vector<std::unique_ptr<SplitRead>>&& split_reads,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<arrow::Schema>& realtime_primary_key_transport_schema,
    const std::shared_ptr<Executor>& executor)
    : split_reads_(std::move(split_reads)),
      path_factory_(path_factory),
      context_(context),
      realtime_primary_key_transport_schema_(realtime_primary_key_transport_schema),
      executor_(executor) {}

Result<std::unique_ptr<TableRead>> KeyValueTableRead::Create(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor) {
    auto raw_file_split_read =
        std::make_unique<RawFileSplitRead>(path_factory, context, memory_pool, executor);
    std::vector<std::unique_ptr<SplitRead>> split_reads;
    split_reads.emplace_back(std::move(raw_file_split_read));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<MergeFileSplitRead> merge_file_split_read,
        MergeFileSplitRead::Create(path_factory, context, memory_pool, executor));
    std::shared_ptr<arrow::Schema> realtime_primary_key_transport_schema;
    if (context->GetRealtimeContext()) {
        PAIMON_ASSIGN_OR_RAISE(
            realtime_primary_key_transport_schema,
            CreateRealtimePrimaryKeyQueryTransportSchema(merge_file_split_read->GetKeySchema(),
                                                         merge_file_split_read->GetValueSchema()));
    }
    split_reads.emplace_back(std::move(merge_file_split_read));

    return std::unique_ptr<TableRead>(
        new KeyValueTableRead(std::move(split_reads), path_factory, context,
                              realtime_primary_key_transport_schema, executor));
}

void KeyValueTableRead::ForceKeepDelete(bool force_keep_delete) {
    force_keep_delete_ = force_keep_delete;
    for (const auto& read : split_reads_) {
        auto* merge_read = dynamic_cast<MergeFileSplitRead*>(read.get());
        if (merge_read != nullptr) {
            merge_read->ForceKeepDelete(force_keep_delete);
        }
    }
}

Result<std::unique_ptr<BatchReader>> KeyValueTableRead::CreateReader(
    const std::shared_ptr<Split>& split) {
    std::shared_ptr<RealtimeSplit> realtime_split = std::dynamic_pointer_cast<RealtimeSplit>(split);
    if (realtime_split) {
        return CreateRealtimeReader(realtime_split, /*release_ticket=*/true);
    }

    std::shared_ptr<Split> dispatch_split = split;
    if (auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split)) {
        PAIMON_RETURN_NOT_OK(indexed_split->Validate());
        if (!indexed_split->Scores().empty()) {
            // TODO(wangyong9999): Propagate indexed scores through the primary-key
            // physical-position read path.
            return Status::NotImplemented(
                "Primary-key reads do not support scored indexed splits yet.");
        }
        // Primary-key indexed splits carry physical positions and are routed independently
        // of the inner split's raw-convertible marker, matching Java's dedicated provider.
        const std::shared_ptr<DataSplit>& inner_split = indexed_split->GetDataSplit();
        if (!force_keep_delete_) {
            bool has_raw_reader = false;
            for (const auto& read : split_reads_) {
                if (dynamic_cast<RawFileSplitRead*>(read.get()) != nullptr) {
                    has_raw_reader = true;
                    PAIMON_ASSIGN_OR_RAISE(bool matched,
                                           read->Match(indexed_split, /*force_keep_delete=*/false));
                    if (matched) {
                        return read->CreateReader(indexed_split);
                    }
                    // A manually supplied or deserialized indexed split can still reference
                    // legacy files. Preserve merge semantics when raw-read safety is uncertain.
                    dispatch_split = inner_split;
                    break;
                }
            }
            if (!has_raw_reader) {
                return Status::Invalid(
                    "create reader failed, primary-key indexed split has no raw reader.");
            }
        } else {
            // Keeping delete rows is incompatible with physical-position pruning. Reading the
            // inner split through the normal merge path preserves correctness.
            dispatch_split = inner_split;
        }
    }
    auto data_split = std::dynamic_pointer_cast<DataSplit>(dispatch_split);
    if (!data_split) {
        return Status::Invalid("split cannot be casted to DataSplit");
    }
    for (const auto& read : split_reads_) {
        PAIMON_ASSIGN_OR_RAISE(bool matched, read->Match(data_split, force_keep_delete_));
        if (matched) {
            return read->CreateReader(data_split);
        }
    }
    return Status::Invalid("create reader failed, not read match with data split.");
}

Result<std::unique_ptr<BatchReader>> KeyValueTableRead::CreateReader(
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
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, CreateReader(split));
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

Result<std::unique_ptr<BatchReader>> KeyValueTableRead::CreateRealtimeReader(
    const std::shared_ptr<RealtimeSplit>& realtime_split, bool release_ticket) {
    if (realtime_split->Version() != RealtimeSplit::kCurrentVersion) {
        return Status::Invalid("unsupported real-time split version");
    }
    if (realtime_split->MemoryEndOffset() < realtime_split->CommittedEndOffset()) {
        return Status::Invalid("real-time split memory end offset precedes committed end offset");
    }
    const std::shared_ptr<RealtimeContext> realtime_context = context_->GetRealtimeContext();
    if (!realtime_context) {
        return Status::Invalid("reading a real-time split requires a real-time context");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                           RealtimeContextImpl::Cast(realtime_context));
    PAIMON_ASSIGN_OR_RAISE(RealtimePartitionBucketView memory,
                           realtime_context_impl->ResolveReadView(realtime_split->OpaqueTicket()));
    const RealtimePartitionBucket expected_partition_bucket(realtime_split->Partition(),
                                                            realtime_split->Bucket());
    if (memory.partition_bucket != expected_partition_bucket) {
        return Status::Invalid("real-time read-view ticket belongs to another partition-bucket");
    }
    const std::optional<OffsetRange> memory_range = memory.read_view->GetOffsetRange();
    if (!memory_range || memory_range->end != realtime_split->MemoryEndOffset()) {
        return Status::Invalid("real-time read-view ticket does not match the split offset range");
    }
    for (const std::unique_ptr<SplitRead>& read : split_reads_) {
        auto* merge_read = dynamic_cast<MergeFileSplitRead*>(read.get());
        if (merge_read) {
            PAIMON_ASSIGN_OR_RAISE(
                std::vector<std::unique_ptr<KeyValueRecordReader>> memory_readers,
                CreateMemoryReaders(realtime_split, memory, realtime_primary_key_transport_schema_,
                                    merge_read->GetKeySchema(), merge_read->GetValueSchema(),
                                    merge_read->GetKeyComparator(), context_,
                                    context_->GetMemoryPool()));
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                                   merge_read->CreateRealtimeReader(realtime_split->DiskSplits(),
                                                                    std::move(memory_readers)));
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RealtimeReader> realtime_reader,
                                   RealtimeReader::Create(memory.read_view, std::move(reader)));
            if (release_ticket) {
                PAIMON_RETURN_NOT_OK(
                    realtime_context_impl->ReleaseReadView(realtime_split->OpaqueTicket()));
            }
            return std::unique_ptr<BatchReader>(std::move(realtime_reader));
        }
    }
    return Status::Invalid("create reader failed, merge file split read not found");
}

Result<std::unique_ptr<CountReader>> KeyValueTableRead::CreateCountReader(
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

    if (force_keep_delete_) {
        return Status::NotImplemented("CreateCountReader with force_keep_delete is not supported");
    }

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<PKCountReader> pk_count_reader,
                           PKCountReader::Create(splits, path_factory_, context_,
                                                 context_->GetMemoryPool(), executor_));

    return pk_count_reader;
}

}  // namespace paimon
