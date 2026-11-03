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

#include <map>
#include <memory>
#include <optional>
#include <utility>

#include "paimon/core/operation/commit/manifest_entry_changes.h"
#include "paimon/core/operation/commit/realtime_deduplicate_dv_materializer.h"
#include "paimon/realtime/realtime_context.h"

namespace paimon {

/// Materializes logical realtime deletions against each commit attempt's latest files.
class RealtimeDeduplicateCommitChangesProvider final : public CommitChangesProvider {
 public:
    RealtimeDeduplicateCommitChangesProvider(
        ManifestEntryChanges changes,
        std::map<RealtimePartitionBucket, RoaringBitmap64> deleted_offsets,
        std::shared_ptr<TableSchema> table_schema,
        std::shared_ptr<FileStorePathFactory> path_factory, std::shared_ptr<CommitScanner> scanner,
        const CoreOptions& options, std::shared_ptr<MemoryPool> pool);

    Result<std::shared_ptr<CommitChanges>> Provide(
        const std::optional<Snapshot>& latest_snapshot) const override;

 private:
    ManifestEntryChanges changes_;
    std::map<RealtimePartitionBucket, RoaringBitmap64> deleted_offsets_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    RealtimeDeduplicateDvMaterializer materializer_;
};

}  // namespace paimon
