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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/operation/commit/commit_changes_provider.h"
#include "paimon/core/realtime/realtime_offset_file_index_lookup.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

class CommitScanner;
class DeletionVector;
class DeletionVectorsIndexFile;
class FileStorePathFactory;
class MemoryPool;
class SchemaManager;
class TableSchema;

/// Shared, attempt-local DV processing for realtime deduplicate writes and compaction.
class RealtimeDeduplicateDvMaterializer {
 public:
    struct BucketSnapshot {
        std::vector<ManifestEntry> files;
        std::vector<std::shared_ptr<IndexFileMeta>> dv_index_files;
    };

    // Outer key: partition values encoded as a BinaryRow. Inner key: bucket ID.
    using SnapshotBuckets = std::unordered_map<BinaryRow, std::map<int32_t, BucketSnapshot>>;

    // Compaction F1/F2 -> F3 with F5 retained: lookup_files={F3}, final_file_names={F3, F5},
    // and deleted_offsets contains offsets recovered from rows deleted by F1/F2's latest DVs.
    struct BucketUpdate {
        std::vector<std::shared_ptr<DataFileMeta>> lookup_files;
        // Unlike lookup_files, this includes files not participating in compaction.
        std::unordered_set<std::string> final_file_names;
        RoaringBitmap64 deleted_offsets;
    };

    RealtimeDeduplicateDvMaterializer(std::shared_ptr<TableSchema> table_schema,
                                      std::shared_ptr<FileStorePathFactory> path_factory,
                                      std::shared_ptr<CommitScanner> scanner,
                                      const CoreOptions& options, std::shared_ptr<MemoryPool> pool);

    Result<SnapshotBuckets> LoadSnapshot(const std::optional<Snapshot>& snapshot,
                                         const std::vector<BinaryRow>& partitions) const;

    Result<std::map<std::string, std::shared_ptr<DeletionVector>>> ReadDeletionVectors(
        const BinaryRow& partition, int32_t bucket, const BucketSnapshot& snapshot) const;

    Result<RealtimeOffsetFileIndexLookup::FilePositions> LookupFilePositions(
        const BinaryRow& partition, int32_t bucket, const BucketUpdate& update) const;

    Status MaterializeBucket(const BinaryRow& partition, int32_t bucket,
                             const BucketSnapshot& snapshot,
                             std::map<std::string, std::shared_ptr<DeletionVector>> baseline,
                             const BucketUpdate& update,
                             const RealtimeOffsetFileIndexLookup::FilePositions& positions,
                             CommitChanges* changes) const;

    /// Only for files whose provider has not yet returned successfully.
    /// Deletes the files listed in changes.temporary_files without modifying changes itself.
    void CleanUpUnpublishedFiles(const CommitChanges& changes) const;

 private:
    Result<std::shared_ptr<DeletionVectorsIndexFile>> CreateDvFile(const BinaryRow& partition,
                                                                   int32_t bucket) const;

    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::shared_ptr<CommitScanner> scanner_;
    CoreOptions options_;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
