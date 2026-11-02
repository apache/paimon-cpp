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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/operation/commit/commit_changes_provider.h"
#include "paimon/core/operation/commit/realtime_deduplicate_dv_materializer.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class CommitScanner;
class Executor;
class FileStorePathFactory;
class MemoryPool;
class RawFileSplitRead;
class TableSchema;

/// Net changes of any number of rewrites included in a single compaction commit.
struct RealtimeCompactionPlan {
    static Result<RealtimeCompactionPlan> Create(const std::vector<ManifestEntry>& entries);

    std::vector<ManifestEntry> delta_files;
    // Outputs consumed by later rewrites in the same commit.
    std::vector<ManifestEntry> intermediate_files;
};

/// Rebases realtime deduplicate compaction DVs without rewriting the data files again.
class RealtimeDeduplicateCompactionChangesProvider final : public CommitChangesProvider {
 public:
    RealtimeDeduplicateCompactionChangesProvider(
        RealtimeCompactionPlan plan, std::vector<ManifestEntry> changelog_files,
        std::vector<IndexManifestEntry> index_entries, std::shared_ptr<TableSchema> table_schema,
        std::shared_ptr<FileStorePathFactory> path_factory, std::shared_ptr<CommitScanner> scanner,
        const CoreOptions& options, std::shared_ptr<Executor> executor,
        std::shared_ptr<MemoryPool> pool);

    Result<std::shared_ptr<CommitChanges>> Provide(
        const std::optional<Snapshot>& latest_snapshot) const override;

 private:
    Result<std::vector<std::string>> CollectIntermediateFilesToDelete(
        const RealtimeDeduplicateDvMaterializer::SnapshotBuckets& snapshot_buckets) const;

    Status MaterializeBucketDeletions(
        const BinaryRow& partition, int32_t bucket, const std::vector<ManifestEntry>& files,
        const RealtimeDeduplicateDvMaterializer::BucketSnapshot& snapshot,
        const std::shared_ptr<arrow::Schema>& offset_schema, RawFileSplitRead* read,
        CommitChanges* changes) const;

    RealtimeCompactionPlan plan_;
    std::vector<ManifestEntry> changelog_files_;
    std::vector<IndexManifestEntry> index_entries_;
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    CoreOptions options_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<MemoryPool> pool_;
    RealtimeDeduplicateDvMaterializer materializer_;
};

}  // namespace paimon
