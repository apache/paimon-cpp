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

#include <memory>

#include "paimon/core/mergetree/compact/changelog_merge_tree_rewriter.h"

namespace paimon {

/// A `MergeTreeCompactRewriter` which produces changelog files for each full compaction.
class FullChangelogMergeTreeCompactRewriter : public ChangelogMergeTreeRewriter {
 public:
    static Result<std::unique_ptr<FullChangelogMergeTreeCompactRewriter>> Create(
        int32_t max_level, int32_t bucket, const BinaryRow& partition,
        const std::shared_ptr<TableSchema>& table_schema, DeletionVector::Factory dv_factory,
        const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
        const CoreOptions& options,
        const std::shared_ptr<CancellationController>& cancellation_controller,
        const std::shared_ptr<MemoryPool>& pool);

    Result<CompactResult> Rewrite(int32_t output_level, bool drop_delete,
                                  const std::vector<std::vector<SortedRun>>& sections) override;

 private:
    FullChangelogMergeTreeCompactRewriter(
        int32_t max_level, const BinaryRow& partition, int32_t bucket, int64_t schema_id,
        const std::vector<std::string>& trimmed_primary_keys, const CoreOptions& options,
        const std::shared_ptr<arrow::Schema>& data_schema,
        const std::shared_ptr<arrow::Schema>& write_schema, DeletionVector::Factory dv_factory,
        const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
        std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
        MergeFunctionWrapperFactory merge_function_wrapper_factory,
        ChangelogMergeFunctionWrapperFactory changelog_merge_function_wrapper_factory,
        const std::shared_ptr<CancellationController>& cancellation_controller,
        const std::shared_ptr<MemoryPool>& pool);

    bool RewriteChangelog(int32_t output_level, bool drop_delete,
                          const std::vector<std::vector<SortedRun>>& sections) const override {
        return output_level == max_level_;
    }

    UpgradeStrategy GenerateUpgradeStrategy(
        int32_t output_level, const std::shared_ptr<DataFileMeta>& file) const override {
        return output_level == max_level_ ? UpgradeStrategy::ChangelogNoRewrite()
                                          : UpgradeStrategy::NoChangelogNoRewrite();
    }
};

}  // namespace paimon
