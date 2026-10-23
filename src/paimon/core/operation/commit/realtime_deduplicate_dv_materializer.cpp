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

#include "paimon/core/operation/commit/realtime_deduplicate_dv_materializer.h"

#include <utility>

#include "paimon/common/types/data_field.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/operation/commit/commit_scanner.h"
#include "paimon/core/realtime/realtime_offset_file_index_lookup.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/fs/file_system.h"

namespace paimon {

RealtimeDeduplicateDvMaterializer::RealtimeDeduplicateDvMaterializer(
    std::shared_ptr<TableSchema> table_schema, std::shared_ptr<FileStorePathFactory> path_factory,
    std::shared_ptr<CommitScanner> scanner, const CoreOptions& options,
    std::shared_ptr<MemoryPool> pool)
    : table_schema_(std::move(table_schema)),
      path_factory_(std::move(path_factory)),
      scanner_(std::move(scanner)),
      options_(options),
      pool_(std::move(pool)) {}

Result<RealtimeDeduplicateDvMaterializer::SnapshotBuckets>
RealtimeDeduplicateDvMaterializer::LoadSnapshot(const std::optional<Snapshot>& snapshot,
                                                const std::vector<BinaryRow>& partitions) const {
    SnapshotBuckets buckets;
    if (!snapshot || partitions.empty()) {
        return buckets;
    }
    std::vector<std::map<std::string, std::string>> filters;
    const std::unordered_set<BinaryRow> unique_partitions(partitions.begin(), partitions.end());
    for (const BinaryRow& partition : unique_partitions) {
        using PartitionValues = std::vector<std::pair<std::string, std::string>>;
        PAIMON_ASSIGN_OR_RAISE(PartitionValues values,
                               path_factory_->GeneratePartitionVector(partition));
        filters.emplace_back(values.begin(), values.end());
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestEntry> files,
                           scanner_->ReadAllEntriesFromPartitions(*snapshot, filters));
    PAIMON_ASSIGN_OR_RAISE(std::vector<IndexManifestEntry> indexes,
                           scanner_->ReadAllIndexEntriesFromPartitions(*snapshot, filters));
    for (const ManifestEntry& entry : files) {
        buckets[entry.Partition()][entry.Bucket()].files.push_back(entry);
    }
    for (const IndexManifestEntry& entry : indexes) {
        if (entry.index_file->IndexType() == DeletionVectorsIndexFile::DELETION_VECTORS_INDEX) {
            buckets[entry.partition][entry.bucket].dv_index_files.push_back(entry.index_file);
        }
    }
    return buckets;
}

Result<std::shared_ptr<DeletionVectorsIndexFile>> RealtimeDeduplicateDvMaterializer::CreateDvFile(
    const BinaryRow& partition, int32_t bucket) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexPathFactory> paths,
                           path_factory_->CreateIndexFileFactory(partition, bucket));
    return std::make_shared<DeletionVectorsIndexFile>(options_.GetFileSystem(), paths,
                                                      /*bitmap64=*/false, pool_);
}

Result<RealtimeDeduplicateDvMaterializer::DeletionVectors>
RealtimeDeduplicateDvMaterializer::ReadDeletionVectors(const BinaryRow& partition, int32_t bucket,
                                                       const BucketSnapshot& snapshot) const {
    if (snapshot.dv_index_files.empty()) {
        return DeletionVectors();
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DeletionVectorsIndexFile> file,
                           CreateDvFile(partition, bucket));
    return file->ReadAllDeletionVectors(snapshot.dv_index_files);
}

Result<RealtimeDeduplicateDvMaterializer::FilePositions>
RealtimeDeduplicateDvMaterializer::LookupFilePositions(const BinaryRow& partition, int32_t bucket,
                                                       const BucketUpdate& update) const {
    FilePositions positions;
    if (!update.deleted_offsets.IsEmpty() && !update.lookup_files.empty()) {
        const auto key_fields = options_.GetRealtimeDeduplicateKeyFields();
        if (key_fields.empty()) {
            return Status::Invalid("realtime DV materialization requires deduplicate key fields");
        }
        const auto schema = DataField::ConvertDataFieldsToArrowSchema(table_schema_->Fields());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> paths,
                               path_factory_->CreateDataFilePathFactory(partition, bucket));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<RealtimeOffsetFileIndexLookup> lookup,
            RealtimeOffsetFileIndexLookup::Create(
                schema, table_schema_->Id(), schema->GetFieldByName(key_fields.front()),
                update.lookup_files, paths, options_.GetFileSystem(), pool_, options_));
        PAIMON_ASSIGN_OR_RAISE(positions, lookup->LookupFilePositions(update.deleted_offsets));
    }

    return positions;
}

Status RealtimeDeduplicateDvMaterializer::MaterializeBucket(
    const BinaryRow& partition, int32_t bucket, const BucketSnapshot& snapshot,
    DeletionVectors baseline, const BucketUpdate& update, const FilePositions& positions,
    CommitChanges* changes) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DeletionVectorsIndexFile> file,
                           CreateDvFile(partition, bucket));
    // baseline is freshly read for this Provide call. Never mutate a cached or writer-owned DV.
    BucketedDvMaintainer maintainer(file, baseline);
    for (auto iter = baseline.begin(); iter != baseline.end();) {
        if (update.final_file_names.count(iter->first) == 0) {
            maintainer.RemoveDeletionVectorOf(iter->first);
            iter = baseline.erase(iter);
        } else {
            ++iter;
        }
    }
    bool has_dvs = !baseline.empty();
    for (const auto& [name, bitmap] : positions) {
        if (update.final_file_names.count(name) == 0) {
            return Status::Invalid("realtime DV lookup returned a file absent from the final set");
        }
        for (auto iter = bitmap.Begin(); iter != bitmap.End(); ++iter) {
            PAIMON_RETURN_NOT_OK(maintainer.NotifyNewDeletion(name, *iter));
            has_dvs = true;
        }
    }
    if (!has_dvs) {
        // Replacing a fixed bucket's DV with an empty map must remove the old index reference.
        for (const auto& index : snapshot.dv_index_files) {
            changes->index_entries.emplace_back(FileKind::Delete(), partition, bucket, index);
        }
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<IndexFileMeta>> index,
                           maintainer.WriteDeletionVectorsIndex());
    if (index) {
        changes->temporary_files.push_back(file->Path(*index));
        changes->index_entries.emplace_back(FileKind::Add(), partition, bucket, *index);
    }
    return Status::OK();
}

void RealtimeDeduplicateDvMaterializer::CleanUpUnpublishedFiles(
    const CommitChanges& changes) const {
    for (const std::string& path : changes.temporary_files) {
        [[maybe_unused]] Status status =
            options_.GetFileSystem()->Delete(path, /*recursive=*/false);
    }
}

}  // namespace paimon
