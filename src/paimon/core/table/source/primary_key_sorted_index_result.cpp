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

#include "paimon/core/table/source/primary_key_sorted_index_result.h"

#include <limits>
#include <map>
#include <optional>
#include <set>
#include <utility>

#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"

namespace paimon {
namespace {
Result<std::shared_ptr<DataSplitImpl>> ToSingleFileSplit(
    const PrimaryKeySortedIndexScan::FilePlan& file) {
    const std::shared_ptr<DataSplitImpl>& source = file.SourceSplit();
    std::vector<std::shared_ptr<DataFileMeta>> data_files{file.DataFile()};
    DataSplitImpl::Builder builder(source->Partition(), source->Bucket(), source->BucketPath(),
                                   std::move(data_files));
    builder.WithSnapshot(source->SnapshotId())
        .WithTotalBuckets(source->TotalBuckets())
        .IsStreaming(false)
        .RawConvertible(source->RawConvertible());
    if (!source->DeletionFiles().empty()) {
        builder.WithDataDeletionFiles({source->DeletionFiles()[file.FileIndex()]});
    }
    return builder.Build();
}

/// Converts sorted file-local positions to merged ranges. Returns `std::nullopt` when a
/// position is invalid or the result is over-fragmented, in which case the file must fall
/// back to a normal scan.
Result<std::optional<std::vector<Range>>> ToRanges(const GlobalIndexResult& result,
                                                   int64_t row_count) {
    std::vector<Range> ranges;
    int64_t from = -1;
    int64_t to = -1;
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iterator,
                           result.CreateIterator());
    while (iterator->HasNext()) {
        int64_t position = iterator->Next();
        if (position < 0 || position >= row_count ||
            position >= std::numeric_limits<int32_t>::max()) {
            return std::optional<std::vector<Range>>();
        }
        if (from < 0) {
            from = position;
        } else if (position != to + 1) {
            if (ranges.size() >=
                static_cast<size_t>(PrimaryKeySortedIndexResult::kMaxIndexedRangesPerFile)) {
                return std::optional<std::vector<Range>>();
            }
            ranges.emplace_back(from, to);
            from = position;
        }
        to = position;
    }
    if (ranges.size() >=
        static_cast<size_t>(PrimaryKeySortedIndexResult::kMaxIndexedRangesPerFile)) {
        return std::optional<std::vector<Range>>();
    }
    ranges.emplace_back(from, to);
    return std::optional<std::vector<Range>>(std::move(ranges));
}
}  // namespace

Result<std::vector<std::shared_ptr<Split>>> PrimaryKeySortedIndexResult::ToSplits(
    const PrimaryKeySortedIndexScan::EvaluatedPlan& evaluated_plan) {
    std::map<const DataSplitImpl*, bool> preserve_raw_splits;
    for (const PrimaryKeySortedIndexScan::EvaluatedFile& evaluated_file : evaluated_plan.Files()) {
        const std::shared_ptr<DataSplitImpl>& source_split = evaluated_file.File().SourceSplit();
        if (!source_split->RawConvertible()) {
            continue;
        }
        auto iter = preserve_raw_splits.emplace(source_split.get(), true).first;
        if (evaluated_file.IndexResult() != nullptr) {
            iter->second = false;
        }
    }

    std::vector<std::shared_ptr<Split>> splits;
    std::set<const DataSplitImpl*> preserved_splits;
    for (const PrimaryKeySortedIndexScan::EvaluatedFile& evaluated_file : evaluated_plan.Files()) {
        const PrimaryKeySortedIndexScan::FilePlan& file = evaluated_file.File();
        const std::shared_ptr<DataSplitImpl>& source_split = file.SourceSplit();
        if (!source_split->RawConvertible() || preserve_raw_splits[source_split.get()]) {
            // Preserve the planner's bin packing when the split cannot be read file by file
            // or no file in the split has a usable index result.
            if (preserved_splits.insert(source_split.get()).second) {
                splits.push_back(source_split);
            }
            continue;
        }

        const std::shared_ptr<GlobalIndexResult>& result = evaluated_file.IndexResult();
        if (result == nullptr) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> fallback_split,
                                   ToSingleFileSplit(file));
            splits.push_back(std::move(fallback_split));
            continue;
        }

        PAIMON_ASSIGN_OR_RAISE(bool is_empty, result->IsEmpty());
        if (is_empty) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::vector<Range>> ranges,
                               ToRanges(*result, file.DataFile()->row_count));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplitImpl> single_file_split,
                               ToSingleFileSplit(file));
        if (ranges == std::nullopt) {
            // The index returned an invalid or over-fragmented row position set; fall back
            // to a normal scan for this file.
            splits.push_back(std::move(single_file_split));
        } else {
            splits.push_back(std::make_shared<IndexedSplitImpl>(
                std::move(single_file_split), std::move(ranges).value(), std::vector<float>()));
        }
    }
    return splits;
}

}  // namespace paimon
