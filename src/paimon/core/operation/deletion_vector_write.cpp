/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/deletion_vector_write.h"

#include <algorithm>
#include <map>
#include <utility>

#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/deletionvectors/bucketed_dv_maintainer.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/operation/append_dv_context.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/operation/write_restore.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/fs/file_system.h"
#include "paimon/scan_context.h"

namespace paimon {
namespace {

using BucketKey = std::pair<std::map<std::string, std::string>, int32_t>;

struct BucketDeletions {
    BinaryRow partition;
    int32_t bucket = 0;
    std::vector<std::shared_ptr<IndexFileMeta>> indexes;
    std::map<std::string, RoaringBitmap32> positions;
};

Status ValidateDeletion(const FileRowSelection& deletion, const DataFileMeta& file) {
    const auto& reference = deletion.file;
    // Split delete_row_count comes from DV cardinality.
    if (reference.file_size != file.file_size || reference.row_count != file.row_count ||
        reference.min_sequence_number != file.min_sequence_number ||
        reference.max_sequence_number != file.max_sequence_number ||
        reference.schema_id != file.schema_id || reference.level != file.level ||
        !(reference.creation_time == file.creation_time)) {
        return Status::Invalid("Deletion file metadata does not match the base snapshot");
    }
    if (file.row_count > RoaringBitmap32::MAX_VALUE) {
        return Status::NotImplemented("DeletionVectorWrite requires bitmap32-addressable files");
    }
    for (auto iter = deletion.positions.Begin(); iter != deletion.positions.End(); ++iter) {
        if (*iter < 0 || *iter >= file.row_count) {
            return Status::Invalid("Deletion position is outside the data file");
        }
    }
    return Status::OK();
}

Result<std::vector<BucketDeletions>> CollectDeletions(
    const AppendDvContext& context, const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const Snapshot& snapshot, const std::vector<FileRowSelection>& deletions,
    const std::shared_ptr<IndexFileHandler>& index_handler,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    FileStore store(context.path_factory->RootPath(), context.options);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ManifestList> manifest_list,
                           store.NewManifestList(context.path_factory, pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(context.arrow_schema, context.schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ManifestFile> manifest_file,
                           store.NewManifestFile(context.path_factory, partition_schema, pool));
    std::map<BucketKey, std::vector<const FileRowSelection*>> grouped;
    for (const auto& deletion : deletions) {
        grouped[{deletion.partition, deletion.bucket}].push_back(&deletion);
    }
    std::vector<BucketDeletions> buckets;
    for (const auto& [key, requests] : grouped) {
        BucketDeletions bucket;
        PAIMON_ASSIGN_OR_RAISE(bucket.partition, context.path_factory->ToBinaryRow(key.first));
        bucket.bucket = key.second;
        std::vector<std::map<std::string, std::string>> partitions;
        if (!key.first.empty()) {
            partitions.push_back(key.first);
        }
        auto filter = std::make_shared<ScanFilter>(nullptr, partitions, bucket.bucket);
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<AppendOnlyFileStoreScan> scan,
            AppendOnlyFileStoreScan::Create(snapshot_manager, context.schema_manager, manifest_list,
                                            manifest_file, context.schema, context.arrow_schema,
                                            filter, context.options, executor, pool));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan,
                               scan->WithSnapshot(snapshot)->CreatePlan());
        std::vector<std::shared_ptr<DataFileMeta>> files;
        PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] std::optional<int32_t> total_buckets,
                               WriteRestore::ExtractDataFiles(plan->Files(), &files));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFilePathFactory> paths,
            context.path_factory->CreateDataFilePathFactory(bucket.partition, bucket.bucket));
        std::map<std::string, std::shared_ptr<DataFileMeta>> live_files;
        for (const auto& file : files) {
            if (!live_files.emplace(paths->ToPath(file), file).second) {
                return Status::Invalid("Base snapshot contains duplicate physical file paths");
            }
        }
        for (const auto* request : requests) {
            auto file = live_files.find(request->file.file_path);
            if (file == live_files.end()) {
                return Status::Invalid(
                    "Deletion target is not a live data file in the base snapshot");
            }
            PAIMON_RETURN_NOT_OK(ValidateDeletion(*request, *file->second));
            if (!request->positions.IsEmpty()) {
                bucket.positions[file->second->file_name] |= request->positions;
            }
        }
        if (!bucket.positions.empty()) {
            PAIMON_ASSIGN_OR_RAISE(
                bucket.indexes,
                index_handler->Scan(snapshot, DeletionVectorsIndexFile::DELETION_VECTORS_INDEX,
                                    bucket.partition, bucket.bucket));
            buckets.push_back(std::move(bucket));
        }
    }
    return buckets;
}

Result<std::vector<std::shared_ptr<IndexFileMeta>>> SelectIndexes(const BucketDeletions& bucket,
                                                                  bool fixed_bucket) {
    std::vector<std::shared_ptr<IndexFileMeta>> selected;
    for (const auto& index : bucket.indexes) {
        if (!index->DvRanges()) {
            return Status::Invalid("Historical DV index has no file ranges");
        }
        if (fixed_bucket || std::any_of(index->DvRanges()->begin(), index->DvRanges()->end(),
                                        [&](const auto& entry) {
                                            return bucket.positions.count(entry.first) != 0;
                                        })) {
            selected.push_back(index);
        }
    }
    return selected;
}

Result<std::shared_ptr<CommitMessage>> WriteBucket(
    const BucketDeletions& bucket, bool fixed_bucket,
    const std::shared_ptr<IndexFileHandler>& index_handler, std::vector<std::string>* generated) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<IndexFileMeta>> indexes,
                           SelectIndexes(bucket, fixed_bucket));
    std::map<std::string, std::shared_ptr<DeletionVector>> vectors;
    PAIMON_ASSIGN_OR_RAISE(
        vectors, index_handler->ReadAllDeletionVectors(bucket.partition, bucket.bucket, indexes));
    for (const auto& [name, dv] : vectors) {
        if (dynamic_cast<BitmapDeletionVector*>(dv.get()) == nullptr) {
            return Status::NotImplemented(
                "DeletionVectorWrite does not support bitmap64 deletion vectors");
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BucketedDvMaintainer> maintainer,
                           BucketedDvMaintainer::Factory(index_handler)
                               .Create(bucket.partition, bucket.bucket, vectors));
    for (const auto& [name, positions] : bucket.positions) {
        PAIMON_RETURN_NOT_OK(maintainer->NotifyNewDeletion(name, positions));
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<IndexFileMeta>> new_index,
                           maintainer->WriteDeletionVectorsIndex());
    if (!new_index) {
        return std::shared_ptr<CommitMessage>();
    }
    generated->push_back(maintainer->DvIndexFile()->Path(*new_index));
    if (fixed_bucket) {
        indexes.clear();
    }
    return std::make_shared<CommitMessageImpl>(
        bucket.partition, bucket.bucket, std::nullopt,
        DataIncrement({}, {}, {}, {*new_index}, std::move(indexes)), CompactIncrement({}, {}, {}));
}

}  // namespace

Result<std::vector<std::shared_ptr<CommitMessage>>> DeletionVectorWrite::PrepareCommit(
    const std::string& table_path, const std::string& branch,
    const std::map<std::string, std::string>& options,
    const std::optional<int64_t>& base_snapshot_id, const std::vector<FileRowSelection>& deletions,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor) {
    if (!executor) {
        return Status::Invalid("DeletionVectorWrite requires an executor");
    }
    if (base_snapshot_id && *base_snapshot_id < Snapshot::FIRST_SNAPSHOT_ID) {
        return Status::Invalid("Invalid base snapshot id");
    }
    PAIMON_ASSIGN_OR_RAISE(AppendDvContext context,
                           AppendDvContext::Create(table_path, branch, options, file_system, pool));
    if (!context.options.DeletionVectorsEnabled()) {
        return Status::Invalid("DeletionVectorWrite requires deletion-vectors.enabled=true");
    }
    FileStore store(table_path, context.options);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SnapshotManager> snapshot_manager,
                           store.NewSnapshotManager(nullptr, std::nullopt));
    std::optional<Snapshot> snapshot;
    if (base_snapshot_id) {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager->LoadSnapshot(*base_snapshot_id));
    } else {
        PAIMON_ASSIGN_OR_RAISE(snapshot, snapshot_manager->LatestSnapshot());
    }
    if (!snapshot) {
        return Status::Invalid("DeletionVectorWrite requires an existing snapshot");
    }
    std::vector<std::shared_ptr<CommitMessage>> messages;
    if (deletions.empty()) {
        return messages;
    }
    const bool fixed_bucket = context.options.GetBucket() != -1;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexFileHandler> index_handler,
                           store.NewIndexFileHandler(
                               fixed_bucket ? BucketMode::HASH_FIXED : BucketMode::BUCKET_UNAWARE,
                               context.path_factory, pool));
    PAIMON_ASSIGN_OR_RAISE(std::vector<BucketDeletions> buckets,
                           CollectDeletions(context, snapshot_manager, *snapshot, deletions,
                                            index_handler, executor, pool));
    std::vector<std::string> generated;
    ScopeGuard cleanup([&]() {
        for (const auto& path : generated) {
            (void)file_system->Delete(path, false);
        }
    });
    for (const auto& bucket : buckets) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CommitMessage> message,
                               WriteBucket(bucket, fixed_bucket, index_handler, &generated));
        if (message) {
            messages.push_back(std::move(message));
        }
    }
    cleanup.Release();
    return messages;
}

}  // namespace paimon
