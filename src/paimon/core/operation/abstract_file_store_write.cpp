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

#include "paimon/core/operation/abstract_file_store_write.h"

#include <algorithm>
#include <cassert>
#include <map>
#include <optional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/file_system_write_restore.h"
#include "paimon/core/operation/metrics/compaction_metrics.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/batch_writer.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/macros.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Executor;
class MemoryPool;

AbstractFileStoreWrite::AbstractFileStoreWrite(
    const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager, const std::string& commit_user,
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema,
    const std::shared_ptr<BucketedDvMaintainer::Factory>& dv_maintainer_factory,
    const std::shared_ptr<BucketedPrimaryKeyIndexMaintainer::Factory>&
        primary_key_index_maintainer_factory,
    const std::shared_ptr<IOManager>& io_manager, const CoreOptions& options,
    bool ignore_previous_files, bool is_streaming_mode, bool ignore_num_bucket_check,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      executor_(executor),
      file_store_path_factory_(file_store_path_factory),
      snapshot_manager_(snapshot_manager),
      schema_manager_(schema_manager),
      commit_user_(commit_user),
      root_path_(root_path),
      schema_(schema),
      write_schema_(write_schema),
      table_schema_(table_schema),
      partition_schema_(partition_schema),
      dv_maintainer_factory_(dv_maintainer_factory),
      primary_key_index_maintainer_factory_(primary_key_index_maintainer_factory),
      io_manager_(io_manager),
      options_(options),
      compact_executor_(CreateDefaultExecutor()),
      compaction_metrics_(std::make_shared<CompactionMetrics>()),
      ignore_previous_files_(ignore_previous_files),
      is_streaming_mode_(is_streaming_mode),
      ignore_num_bucket_check_(ignore_num_bucket_check),
      metrics_(std::make_shared<MetricsImpl>()),
      logger_(Logger::GetLogger("AbstractFileStoreWrite")) {
    writer_memory_manager_ =
        std::make_unique<WriterMemoryManager>(static_cast<uint64_t>(options.GetWriteBufferSize()));
    cache_manager_ = std::make_shared<CacheManager>(options.GetLookupCacheMaxMemory(),
                                                    options.GetLookupCacheHighPrioPoolRatio());
}

Status AbstractFileStoreWrite::Write(std::unique_ptr<RecordBatch>&& batch) {
    if (PAIMON_UNLIKELY(batch == nullptr)) {
        return Status::Invalid("batch is null pointer");
    }
    // in FileStoreWrite::Create() we have checked the table kind and bucket mode, here we only
    // check the bucket id in batch
    if (options_.GetBucket() == -1) {
        assert(table_schema_->PrimaryKeys().empty());
        if (!batch->HasSpecifiedBucket()) {
            batch->SetBucket(BucketModeDefine::UNAWARE_BUCKET);
        } else if (batch->GetBucket() != BucketModeDefine::UNAWARE_BUCKET) {
            return Status::Invalid(
                fmt::format("batch bucket is {} while options bucket is -1", batch->GetBucket()));
        }
    } else if (options_.GetBucket() == BucketModeDefine::POSTPONE_BUCKET) {
        assert(!table_schema_->PrimaryKeys().empty());
        if (!batch->HasSpecifiedBucket()) {
            batch->SetBucket(BucketModeDefine::POSTPONE_BUCKET);
        } else if (batch->GetBucket() != BucketModeDefine::POSTPONE_BUCKET) {
            return Status::Invalid(
                fmt::format("batch bucket is {} while options bucket is -2", batch->GetBucket()));
        }
    } else {
        assert(options_.GetBucket() > 0);
        if (!(batch->GetBucket() >= 0 && batch->GetBucket() < options_.GetBucket())) {
            return Status::Invalid(
                fmt::format("fixed bucketed mode must specify a bucket which in [0, {}) in "
                            "RecordBatch",
                            options_.GetBucket()));
        }
    }
    // check nullability
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> data,
        arrow::ImportArray(batch->GetData(), arrow::struct_(write_schema_->fields())));
    PAIMON_RETURN_NOT_OK(ArrowUtils::CheckNullabilityMatch(write_schema_, data));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*data, batch->GetData()));

    PAIMON_ASSIGN_OR_RAISE(BinaryRow partition,
                           file_store_path_factory_->ToBinaryRow(batch->GetPartition()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BatchWriter> writer,
                           GetWriter(partition, batch->GetBucket()));
    assert(writer);
    PAIMON_RETURN_NOT_OK(writer->Write(std::move(batch)));
    return writer_memory_manager_->OnWriteCompleted(writer.get());
}

Status AbstractFileStoreWrite::Compact(const std::map<std::string, std::string>& partition,
                                       int32_t bucket, bool full_compaction) {
    PAIMON_ASSIGN_OR_RAISE(BinaryRow part, file_store_path_factory_->ToBinaryRow(partition));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BatchWriter> writer, GetWriter(part, bucket));
    assert(writer);
    return writer->Compact(full_compaction);
}

Result<std::vector<std::shared_ptr<CommitMessage>>> AbstractFileStoreWrite::PrepareCommit(
    bool wait_compaction, int64_t commit_identifier) {
    if (IsRealtimeWrite()) {
        return Status::Invalid(
            "real-time writer must use PrepareCommitWithProgress to preserve offset ranges");
    }
    if (batch_committed_) {
        return Status::Invalid("batch write mode only support one-time committing.");
    }
    if (is_streaming_mode_ == false) {
        // batch write prepare commit will ignore these params
        batch_committed_ = true;
        wait_compaction = true;
        commit_identifier = std::numeric_limits<int64_t>::max();
    }
    int64_t latest_committed_identifier = std::numeric_limits<int64_t>::min();
    for (const auto& kv : writers_) {
        const auto& buckets = kv.second;
        for (const auto& kv : buckets) {
            const auto& writer_container = kv.second;
            latest_committed_identifier = std::max(
                latest_committed_identifier, writer_container.last_modified_commit_identifier);
        }
    }
    if (latest_committed_identifier == std::numeric_limits<int64_t>::min()) {
        // Optimization for the first commit.
        //
        // If this is the first commit, no writer has previous modified commit, so the value of
        // `latestCommittedIdentifier` does not matter.
        //
        // Without this optimization, we may need to scan through all snapshots only to find
        // that there is no previous snapshot by this user, which is very inefficient.
    } else {
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager_->LatestSnapshotOfUser(commit_user_));
        if (latest_snapshot == std::nullopt) {
            latest_committed_identifier = std::numeric_limits<int64_t>::min();
        } else {
            latest_committed_identifier = latest_snapshot.value().CommitIdentifier();
        }
    }

    std::vector<std::shared_ptr<CommitMessage>> result;
    auto metrics = compaction_metrics_->GetMetrics();
    for (auto partition_iter = writers_.begin(); partition_iter != writers_.end();) {
        auto& partition = partition_iter->first;
        auto& buckets = partition_iter->second;
        for (auto bucket_iter = buckets.begin(); bucket_iter != buckets.end();) {
            int32_t bucket = bucket_iter->first;
            WriterContainer<BatchWriter>& writer_container = bucket_iter->second;
            PAIMON_ASSIGN_OR_RAISE(CommitIncrement increment,
                                   writer_container.writer->PrepareCommit(wait_compaction));
            writer_memory_manager_->RefreshWriterMemory(writer_container.writer.get());
            auto compact_deletion_file = increment.GetCompactDeletionFile();
            auto& compact_increment = increment.GetCompactIncrement();
            if (compact_deletion_file) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::optional<std::shared_ptr<IndexFileMeta>> dv_index_file_meta,
                    compact_deletion_file->GetOrCompute());
                if (dv_index_file_meta) {
                    compact_increment.AddNewIndexFiles({dv_index_file_meta.value()});
                }
            }
            if (writer_container.primary_key_index_maintainer) {
                Status index_status =
                    writer_container.primary_key_index_maintainer->PrepareCommit(&increment);
                if (!index_status.ok()) {
                    if (compact_deletion_file) {
                        const auto& new_index_files =
                            increment.GetCompactIncrement().NewIndexFiles();
                        for (const std::shared_ptr<IndexFileMeta>& index_file : new_index_files) {
                            if (index_file != nullptr &&
                                index_file->IndexType() ==
                                    DeletionVectorsIndexFile::DELETION_VECTORS_INDEX) {
                                PAIMON_ASSIGN_OR_RAISE(
                                    std::string index_path,
                                    dv_maintainer_factory_->GetIndexFileHandler()->FilePath(
                                        partition, bucket, index_file));
                                [[maybe_unused]] Status cleanup_status =
                                    options_.GetFileSystem()->Delete(index_path);
                            }
                        }
                    }
                    return index_status;
                }
            }

            auto committable = std::make_shared<CommitMessageImpl>(
                partition, bucket, writer_container.total_buckets, increment.GetNewFilesIncrement(),
                compact_increment);
            result.push_back(committable);
            if (!committable->IsEmpty()) {
                writer_container.last_modified_commit_identifier = commit_identifier;
                metrics->Merge(writer_container.writer->GetMetrics());
                ++bucket_iter;
                continue;
            }
            // Condition 1: There is no more record waiting to be committed. Note that the
            // condition is < (instead of <=), because each commit identifier may have
            // multiple snapshots. We must make sure all snapshots of this identifier are
            // committed.
            // Condition 2: No compaction is in progress. That is, no more changelog will be
            // produced.
            //
            // Condition 3: The writer has no postponed compaction like gentle lookup
            // compaction.
            if (writer_container.last_modified_commit_identifier < latest_committed_identifier) {
                PAIMON_ASSIGN_OR_RAISE(bool has_pending_compaction,
                                       writer_container.writer->CompactNotCompleted());
                if (!has_pending_compaction) {
                    // Clear writer if no update, and if its latest modification has committed.
                    //
                    // We need a mechanism to clear writers, otherwise there will be more and
                    // more such as yesterday's partition that no longer needs to be written.
                    PAIMON_LOG_DEBUG(logger_,
                                     "Closing writer for partition %s, bucket %d. "
                                     "Writer's last modified identifier is %ld, "
                                     "while latest committed identifier is %ld, "
                                     "current commit identifier is %ld.",
                                     partition.ToString().c_str(), bucket,
                                     writer_container.last_modified_commit_identifier,
                                     latest_committed_identifier, commit_identifier);
                    writer_memory_manager_->UnregisterWriter(writer_container.writer.get());
                    PAIMON_RETURN_NOT_OK(writer_container.writer->Close());
                    bucket_iter = buckets.erase(bucket_iter);
                    continue;
                }
            }
            metrics->Merge(writer_container.writer->GetMetrics());
            ++bucket_iter;
        }

        if (buckets.empty()) {
            partition_iter = writers_.erase(partition_iter);
        } else {
            ++partition_iter;
        }
    }

    metrics_->Overwrite(metrics);
    return result;
}

Result<std::vector<RealtimeCommitProgress>> AbstractFileStoreWrite::PrepareCommitWithProgress(
    int64_t) {
    if (!IsRealtimeWrite()) {
        return Status::Invalid("PrepareCommitWithProgress is only supported by a real-time writer");
    }
    if (is_streaming_mode_ == false) {
        return Status::Invalid("PrepareCommitWithProgress requires streaming mode");
    }
    // Real-time prepare snapshots writers under a short lock so writes can continue while sealed
    // segments are flushed. The normal path iterates and may erase writers in place, which would
    // either race with concurrent writer creation or hold writers_mutex_ for the whole prepare.
    return PrepareRealtimeCommit();
}

Result<std::vector<RealtimeCommitProgress>> AbstractFileStoreWrite::PrepareRealtimeCommit() {
    struct WriterSnapshot {
        BinaryRow partition;
        int32_t bucket;
        int32_t total_buckets;
        std::shared_ptr<BatchWriter> writer;
    };
    std::vector<WriterSnapshot> writer_snapshots;
    {
        std::lock_guard<std::mutex> lock(writers_mutex_);
        for (const auto& [partition, buckets] : writers_) {
            for (const auto& [bucket, container] : buckets) {
                writer_snapshots.push_back(
                    WriterSnapshot{partition, bucket, container.total_buckets, container.writer});
            }
        }
    }

    std::vector<RealtimeCommitProgress> result;
    for (const WriterSnapshot& snapshot : writer_snapshots) {
        PAIMON_ASSIGN_OR_RAISE(CommitIncrement increment,
                               snapshot.writer->PrepareCommit(/*wait_compaction=*/false));
        writer_memory_manager_->RefreshWriterMemory(snapshot.writer.get());
        auto committable = std::make_shared<CommitMessageImpl>(
            snapshot.partition, snapshot.bucket, snapshot.total_buckets,
            increment.GetNewFilesIncrement(), increment.GetCompactIncrement());
        if (!increment.GetRealtimeOffsetRange()) {
            if (!committable->IsEmpty()) {
                return Status::Invalid("real-time commit message does not have an offset range");
            }
            continue;
        }
        if (committable->IsEmpty()) {
            return Status::Invalid("sealed real-time segment produced an empty commit message");
        }
        std::vector<std::pair<std::string, std::string>> partition_values;
        PAIMON_ASSIGN_OR_RAISE(partition_values, file_store_path_factory_->GeneratePartitionVector(
                                                     snapshot.partition));
        std::map<std::string, std::string> partition(partition_values.begin(),
                                                     partition_values.end());
        result.push_back(RealtimeCommitProgress{
            committable, RealtimePartitionBucket(std::move(partition), snapshot.bucket),
            increment.GetRealtimeOffsetRange().value()});
    }
    {
        std::lock_guard<std::mutex> lock(realtime_metrics_mutex_);
        std::shared_ptr<Metrics> metrics = compaction_metrics_->GetMetrics();
        for (const WriterSnapshot& snapshot : writer_snapshots) {
            metrics->Merge(snapshot.writer->GetMetrics());
        }
        metrics_->Overwrite(metrics);
    }
    return result;
}

Status AbstractFileStoreWrite::Close() {
    std::lock_guard<std::mutex> lock(writers_mutex_);
    for (auto& [_, bucket_writers] : writers_) {
        for (auto& [_, writer_container] : bucket_writers) {
            writer_memory_manager_->UnregisterWriter(writer_container.writer.get());
            PAIMON_RETURN_NOT_OK(writer_container.writer->Close());
        }
    }
    writers_.clear();
    compact_executor_->ShutdownNow();
    return Status::OK();
}

std::shared_ptr<Metrics> AbstractFileStoreWrite::GetMetrics() const {
    return metrics_;
}

int32_t AbstractFileStoreWrite::GetDefaultBucketNum() const {
    return options_.GetBucket();
}

Result<std::shared_ptr<RestoreFiles>> AbstractFileStoreWrite::ScanExistingFileMetas(
    const BinaryRow& partition, int32_t bucket) const {
    std::vector<std::pair<std::string, std::string>> part_values;
    PAIMON_ASSIGN_OR_RAISE(part_values,
                           file_store_path_factory_->GeneratePartitionVector(partition));
    std::map<std::string, std::string> part_values_map;
    for (const auto& [key, value] : part_values) {
        part_values_map[key] = value;
    }
    std::vector<std::map<std::string, std::string>> partition_filters;
    if (!part_values_map.empty()) {
        partition_filters.push_back(part_values_map);
    }
    auto scan_filter = std::make_shared<ScanFilter>(
        /*predicate=*/nullptr, partition_filters, std::optional<int32_t>(bucket));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan, CreateFileStoreScan(scan_filter));
    std::shared_ptr<IndexFileHandler> index_file_handler;
    if (dv_maintainer_factory_) {
        index_file_handler = dv_maintainer_factory_->GetIndexFileHandler();
    } else if (primary_key_index_maintainer_factory_) {
        index_file_handler = primary_key_index_maintainer_factory_->GetIndexFileHandler();
    }
    // Paimon Java currently drops value stats during writer restore. This is a known bug: a
    // restored file can become a compact-after ADD via metadata-only level upgrade and lose its
    // stats (https://github.com/apache/paimon/issues/7026). C++ intentionally does not align with
    // that behavior; stats are dropped later only when the final entry kind is DELETE.
    FileSystemWriteRestore restore(snapshot_manager_, std::move(scan), index_file_handler);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<RestoreFiles> restore_files,
        restore.GetRestoreFiles(partition, bucket, dv_maintainer_factory_ != nullptr,
                                primary_key_index_maintainer_factory_ != nullptr));

    std::optional<int32_t> restored_total_buckets = restore_files->TotalBuckets();
    int32_t total_buckets = GetDefaultBucketNum();
    if (restored_total_buckets) {
        total_buckets = restored_total_buckets.value();
    }

    if (!ignore_num_bucket_check_ && total_buckets != options_.GetBucket()) {
        return Status::Invalid(fmt::format(
            "Try to write table with a new bucket num {}, but the previous "
            "bucket num is {}. Please switch to batch mode, and perform INSERT OVERWRITE to "
            "rescale current data layout first.",
            options_.GetBucket(), total_buckets));
    }
    return restore_files;
}

Result<std::shared_ptr<BatchWriter>> AbstractFileStoreWrite::GetWriter(const BinaryRow& partition,
                                                                       int32_t bucket) {
    std::lock_guard<std::mutex> lock(writers_mutex_);
    auto partition_iter = writers_.find(partition);
    if (partition_iter != writers_.end()) {
        auto& buckets = partition_iter->second;
        auto bucket_iter = buckets.find(bucket);
        if (PAIMON_LIKELY(bucket_iter != buckets.end())) {
            return bucket_iter->second.writer;
        }
    }

    std::shared_ptr<RestoreFiles> restored = RestoreFiles::Empty();
    if (!ignore_previous_files_) {
        PAIMON_ASSIGN_OR_RAISE(restored, ScanExistingFileMetas(partition, bucket));
    }

    auto restore_data_files = restored->DataFiles();
    int64_t max_sequence_number = DataFileMeta::GetMaxSequenceNumber(restore_data_files);
    std::shared_ptr<BucketedDvMaintainer> dv_maintainer;
    if (dv_maintainer_factory_) {
        PAIMON_ASSIGN_OR_RAISE(
            dv_maintainer,
            dv_maintainer_factory_->Create(partition, bucket, restored->DeleteVectorsIndex()));
    }

    std::shared_ptr<BucketedPrimaryKeyIndexMaintainer> primary_key_index_maintainer;
    if (primary_key_index_maintainer_factory_) {
        PAIMON_ASSIGN_OR_RAISE(
            primary_key_index_maintainer,
            primary_key_index_maintainer_factory_->CreateMaintainer(
                partition, bucket, restore_data_files, restored->PrimaryKeyIndexPayloads()));
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<BatchWriter> writer,
        CreateWriter(partition, bucket, restore_data_files, max_sequence_number, dv_maintainer));
    int32_t total_buckets = restored->TotalBuckets().value_or(GetDefaultBucketNum());

    if (partition_iter == writers_.end()) {
        writers_.emplace(
            partition, std::unordered_map<int32_t, WriterContainer<BatchWriter>>(
                           {{bucket, WriterContainer<BatchWriter>(writer, total_buckets,
                                                                  primary_key_index_maintainer)}}));
    } else {
        partition_iter->second.emplace(
            bucket,
            WriterContainer<BatchWriter>(writer, total_buckets, primary_key_index_maintainer));
    }
    writer_memory_manager_->RegisterWriter(writer.get());

    return writer;
}

}  // namespace paimon
