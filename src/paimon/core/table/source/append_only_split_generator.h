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

#pragma once

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "paimon/common/utils/bin_packing.h"
#include "paimon/core/append/bucketed_append_compact_manager.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/source/split_generator.h"

namespace paimon {
/// Append only implementation of `SplitGenerator`.
class AppendOnlySplitGenerator : public SplitGenerator {
 public:
    AppendOnlySplitGenerator(int64_t target_split_size, int64_t open_file_cost,
                             const BucketMode& bucket_mode)
        : target_split_size_(target_split_size),
          open_file_cost_(open_file_cost),
          bucket_mode_(bucket_mode) {}

    Result<std::vector<SplitGroup>> SplitForBatch(
        std::vector<std::shared_ptr<DataFileMeta>>&& input) const override {
        return SplitForBatchWithCancellation(std::move(input), nullptr);
    }

    Result<std::vector<SplitGroup>> SplitForBatchWithCancellation(
        std::vector<std::shared_ptr<DataFileMeta>>&& input,
        const ScanCancellationToken* token) const override {
        PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
        std::vector<std::shared_ptr<DataFileMeta>> files = std::move(input);
        std::stable_sort(files.begin(), files.end(),
                         BucketedAppendCompactManager::FileComparator(bucket_mode_ ==
                                                                      BucketMode::BUCKET_UNAWARE));
        PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
        auto weight_func = [open_file_cost = open_file_cost_](
                               const std::shared_ptr<DataFileMeta>& meta) -> int64_t {
            return std::max(meta->file_size, open_file_cost);
        };
        std::function<Status()> checkpoint;
        if (token) {
            checkpoint = [token]() { return token->CheckCancelled(); };
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> packed,
                               BinPacking::PackForOrdered<std::shared_ptr<DataFileMeta>>(
                                   std::move(files), weight_func, target_split_size_, checkpoint));
        std::vector<SplitGroup> ret;
        ret.reserve(packed.size());
        for (auto& pack : packed) {
            PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
            ret.push_back(SplitGroup::RawConvertibleGroup(std::move(pack)));
        }
        PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
        return ret;
    }

    Result<std::vector<SplitGroup>> SplitForStreaming(
        std::vector<std::shared_ptr<DataFileMeta>>&& files) const override {
        // When the bucket mode is unaware, we split the files as batch, because unaware-bucket
        // table only contains one bucket (bucket 0).
        if (bucket_mode_ == BucketMode::BUCKET_UNAWARE) {
            return SplitForBatch(std::move(files));
        } else {
            return std::vector<SplitGroup>({SplitGroup::RawConvertibleGroup(std::move(files))});
        }
    }

 private:
    int64_t target_split_size_;
    int64_t open_file_cost_;
    BucketMode bucket_mode_;
};
}  // namespace paimon
