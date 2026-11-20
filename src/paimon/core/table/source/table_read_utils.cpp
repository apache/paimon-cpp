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

#include "paimon/core/table/source/table_read_utils.h"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/table/format/lazy_concat_batch_reader.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/table/source/table_read.h"

namespace paimon {

namespace {
Status ValidateDiskSplit(std::shared_ptr<Split> split, bool allow_indexed) {
    if (const auto* indexed = dynamic_cast<const IndexedSplitImpl*>(split.get())) {
        PAIMON_RETURN_NOT_OK(indexed->Validate());
        if (!allow_indexed) {
            return Status::Invalid("merge input disk split is not a data split");
        }
        split = indexed->GetDataSplit();
    }
    const auto* data_split = dynamic_cast<const DataSplitImpl*>(split.get());
    if (!data_split) {
        return Status::Invalid("expected a data or indexed split");
    }
    const auto& files = data_split->DataFiles();
    const auto& deletion_files = data_split->DeletionFiles();
    if (!deletion_files.empty() && deletion_files.size() != files.size()) {
        return Status::Invalid("deletion files must be empty or match data files");
    }
    if (std::find(files.begin(), files.end(), nullptr) != files.end()) {
        return Status::Invalid("data file is null");
    }
    return Status::OK();
}
}  // namespace

Result<RealtimePartitionBucketView> TableReadUtils::ResolveRealtimeReadView(
    const RealtimeSplit& split, const InternalReadContext& context) {
    if (split.Version() != RealtimeSplit::kCurrentVersion) {
        return Status::Invalid("unsupported real-time split version");
    }
    if (split.CommittedEndOffset() < 0) {
        return Status::Invalid("real-time split committed end offset must not be negative");
    }
    if (split.MemoryEndOffset() < split.CommittedEndOffset()) {
        return Status::Invalid("real-time split memory end offset precedes committed end offset");
    }
    for (const auto& disk_split : split.DiskSplits()) {
        if (!disk_split) {
            return Status::Invalid("real-time disk split is null");
        }
        PAIMON_RETURN_NOT_OK(ValidateDiskSplit(disk_split, context.GetPrimaryKeys().empty()));
    }
    std::shared_ptr<RealtimeContext> realtime_context = context.GetRealtimeContext();
    if (!realtime_context) {
        return Status::Invalid("reading a real-time split requires a real-time context");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> context_impl,
                           RealtimeContextImpl::Cast(realtime_context));
    PAIMON_ASSIGN_OR_RAISE(RealtimePartitionBucketView memory,
                           context_impl->ResolveReadView(split.OpaqueTicket()));
    if (memory.partition_bucket != RealtimePartitionBucket(split.Partition(), split.Bucket())) {
        return Status::Invalid("real-time read-view ticket belongs to another partition-bucket");
    }
    const std::optional<OffsetRange> range = memory.read_view->GetOffsetRange();
    if (!range || range->end != split.MemoryEndOffset()) {
        return Status::Invalid("real-time read-view ticket does not match the split offset range");
    }
    PAIMON_RETURN_NOT_OK(memory.ValidateTableSchema(
        DataField::ConvertDataFieldsToArrowSchema(context.GetTableSchema()->Fields())));
    return memory;
}

Result<std::unique_ptr<BatchReader>> TableReadUtils::CreateLazyReader(
    const std::vector<std::shared_ptr<Split>>& splits, const DiskReaderFactory& open_disk,
    const RealtimeReaderFactory& open_realtime, const InternalReadContext& context) {
    std::vector<LazyConcatBatchReader::Source> sources;
    sources.reserve(splits.size());
    std::vector<std::string> tickets;
    for (size_t i = 0; i < splits.size(); ++i) {
        const std::shared_ptr<Split>& split = splits[i];
        if (!split) {
            return Status::Invalid("split is null");
        }
        LazyConcatBatchReader::ReaderFactory open;
        if (auto realtime_split = std::dynamic_pointer_cast<RealtimeSplit>(split)) {
            PAIMON_ASSIGN_OR_RAISE(RealtimePartitionBucketView view,
                                   ResolveRealtimeReadView(*realtime_split, context));
            tickets.push_back(realtime_split->OpaqueTicket());
            open = [open_realtime, realtime_split = std::move(realtime_split),
                    view = std::move(view)]() { return open_realtime(realtime_split, view); };
        } else {
            PAIMON_RETURN_NOT_OK(ValidateDiskSplit(split, /*allow_indexed=*/true));
            open = [open_disk, split]() { return open_disk(split); };
        }
        sources.push_back({fmt::format("split {}", i), std::move(open)});
    }
    auto reader =
        std::make_unique<LazyConcatBatchReader>(std::move(sources), context.GetArrowMemoryPool());
    if (!tickets.empty()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> context_impl,
                               RealtimeContextImpl::Cast(context.GetRealtimeContext()));
        for (const std::string& ticket : tickets) {
            PAIMON_RETURN_NOT_OK(context_impl->ReleaseReadView(ticket));
        }
    }
    return reader;
}

}  // namespace paimon
