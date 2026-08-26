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

#include "paimon/core/table/source/append_count_reader.h"

#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/status.h"

namespace paimon {

Result<int64_t> AppendCountReader::CountRows() {
    int64_t total = 0;
    for (const auto& split : splits_) {
        PAIMON_ASSIGN_OR_RAISE(int64_t split_count, CountSingleSplit(split));
        total += split_count;
    }
    return total;
}

Result<int64_t> AppendCountReader::CountSingleSplit(const std::shared_ptr<Split>& split) const {
    std::shared_ptr<RealtimeSplit> realtime_split = std::dynamic_pointer_cast<RealtimeSplit>(split);
    if (realtime_split) {
        int64_t total = realtime_split->MemoryEndOffset() - realtime_split->CommittedEndOffset();
        for (const std::shared_ptr<Split>& disk_split : realtime_split->DiskSplits()) {
            PAIMON_ASSIGN_OR_RAISE(int64_t disk_count, CountSingleSplit(disk_split));
            total += disk_count;
        }
        return total;
    }

    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
    if (!data_split) {
        return Status::Invalid("split cannot be cast to DataSplitImpl");
    }

    if (data_split->DataFiles().empty()) {
        return 0;
    }

    return MetadataCount(data_split);
}

Result<int64_t> AppendCountReader::MetadataCount(
    const std::shared_ptr<DataSplitImpl>& split) const {
    if (split->RawConvertible()) {
        if (!file_system_ || !pool_) {
            return Status::Invalid(
                "file_system or memory_pool is null for DV-based append count fallback");
        }

        DeletionVector::Factory dv_factory = DeletionVector::CreateFactory(
            file_system_,
            DeletionVector::CreateDeletionFileMap(split->DataFiles(), split->DeletionFiles()),
            pool_);

        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> merged_count,
                               split->MergedRowCount(dv_factory));
        if (merged_count.has_value()) {
            return merged_count.value();
        }
    } else {
        // Non-raw-convertible splits are typically produced by data evolution when multiple files
        // overlap on row-id ranges. Count them through data-evolution metadata instead of using a
        // deletion-vector factory for raw file counts.
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> merged_count, split->MergedRowCount());
        if (merged_count.has_value()) {
            return merged_count.value();
        }
    }

    return Status::Invalid("not support split in append count fallback");
}

}  // namespace paimon
