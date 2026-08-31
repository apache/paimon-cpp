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

#include "paimon/core/operation/key_value_file_store_write.h"

#include <optional>
#include <vector>

#include "arrow/c/bridge.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/compact/noop_compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/mergetree/levels.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/key_value_file_store_scan.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_primary_key_reader.h"
#include "paimon/core/realtime/realtime_primary_key_writer.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/primary_key_table_utils.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/realtime/realtime_context.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class DataFilePathFactory;
class Executor;
class MemoryPool;
struct KeyValue;
template <typename T>
class MergeFunctionWrapper;

KeyValueFileStoreWrite::KeyValueFileStoreWrite(
    const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager, const std::string& commit_user,
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::Schema>& partition_schema,
    const std::shared_ptr<BucketedDvMaintainer::Factory>& dv_maintainer_factory,
    const std::shared_ptr<BucketedPrimaryKeyIndexMaintainer::Factory>&
        primary_key_index_maintainer_factory,
    const std::shared_ptr<IOManager>& io_manager,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
    const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper,
    const CoreOptions& options, bool ignore_previous_files, bool is_streaming_mode,
    bool ignore_num_bucket_check, bool enable_multi_thread_spill,
    const std::shared_ptr<RealtimeContext>& realtime_context,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool)
    : AbstractFileStoreWrite(
          file_store_path_factory, snapshot_manager, schema_manager, commit_user, root_path,
          table_schema, schema, /*write_schema=*/schema, partition_schema, dv_maintainer_factory,
          primary_key_index_maintainer_factory, io_manager, options, ignore_previous_files,
          is_streaming_mode, ignore_num_bucket_check, executor, pool),
      enable_multi_thread_spill_(enable_multi_thread_spill),
      realtime_context_(realtime_context),
      key_comparator_(key_comparator),
      user_defined_seq_comparator_(user_defined_seq_comparator),
      merge_function_wrapper_(merge_function_wrapper),
      compact_manager_factory_(std::make_unique<MergeTreeCompactManagerFactory>(
          options_, key_comparator_, user_defined_seq_comparator_, compaction_metrics_,
          table_schema_, schema_, schema_manager_, io_manager_, cache_manager_,
          file_store_path_factory_, root_path_, ignore_previous_files, pool_)),
      logger_(Logger::GetLogger("KeyValueFileStoreWrite")) {
    if (realtime_context_) {
        writer_memory_manager_ = std::make_unique<NoopWriterMemoryManager>();
    }
}

Result<std::unique_ptr<FileStoreScan>> KeyValueFileStoreWrite::CreateFileStoreScan(
    const std::shared_ptr<ScanFilter>& scan_filter) const {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                             options_.GetManifestCompression(), file_store_path_factory_,
                             options_.GetCache(), pool_));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                             options_.GetManifestCompression(), file_store_path_factory_,
                             options_.GetManifestTargetFileSize(), pool_, options_,
                             partition_schema_));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan,
                           KeyValueFileStoreScan::Create(
                               snapshot_manager_, schema_manager_, manifest_list, manifest_file,
                               table_schema_, schema_, scan_filter, options_, executor_, pool_));
    return scan;
}

Result<std::shared_ptr<BatchWriter>> KeyValueFileStoreWrite::CreateWriter(
    const BinaryRow& partition, int32_t bucket,
    const std::vector<std::shared_ptr<DataFileMeta>>& restore_data_files,
    int64_t restore_max_seq_number, const std::shared_ptr<BucketedDvMaintainer>& dv_maintainer) {
    PAIMON_LOG_DEBUG(logger_, "Creating key value writer for partition %s, bucket %d",
                     partition.ToString().c_str(), bucket);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           file_store_path_factory_->CreateDataFilePathFactory(partition, bucket));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> trimmed_primary_keys,
                           table_schema_->TrimmedPrimaryKeys());
    std::map<std::string, std::string> partition_map;
    std::shared_ptr<CompactManager> compact_manager;
    std::shared_ptr<RealtimeContextImpl> realtime_context_impl;
    std::optional<RealtimeStoreState> realtime_store_state;
    std::shared_ptr<arrow::Schema> transport_schema;
    if (realtime_context_) {
        std::vector<std::pair<std::string, std::string>> partition_values;
        PAIMON_ASSIGN_OR_RAISE(partition_values,
                               file_store_path_factory_->GeneratePartitionVector(partition));
        partition_map =
            std::map<std::string, std::string>(partition_values.begin(), partition_values.end());
        PAIMON_ASSIGN_OR_RAISE(realtime_context_impl, RealtimeContextImpl::Cast(realtime_context_));
        transport_schema = RealtimePrimaryKeyLayout::CreateSchema(schema_->fields());
        auto c_write_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportSchema(*transport_schema, c_write_schema.get()));
        PAIMON_ASSIGN_OR_RAISE(
            RealtimeStoreState store_state,
            realtime_context_impl->GetOrCreateRealtimeStore(
                RealtimeStoreCreateRequest{std::move(c_write_schema), options_.ToMap(), pool_,
                                           RealtimeStoreMode::PRIMARY_KEY},
                RealtimePartitionBucket(partition_map, bucket)));
        realtime_store_state = std::move(store_state);
        compact_manager = std::make_shared<NoopCompactManager>();
    } else {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<Levels> levels,
            Levels::Create(key_comparator_, restore_data_files, options_.GetNumLevels()));
        auto compact_strategy = compact_manager_factory_->CreateCompactStrategy();
        PAIMON_ASSIGN_OR_RAISE(compact_manager, compact_manager_factory_->CreateCompactManager(
                                                    partition, bucket, compact_strategy,
                                                    compact_executor_, levels, dv_maintainer));
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<MergeTreeWriter> writer,
        MergeTreeWriter::Create(
            restore_max_seq_number, trimmed_primary_keys, data_file_path_factory, key_comparator_,
            user_defined_seq_comparator_, merge_function_wrapper_, table_schema_->Id(), schema_,
            options_, compact_manager, realtime_context_ ? nullptr : io_manager_,
            enable_multi_thread_spill_, pool_));
    if (!realtime_context_) {
        return std::shared_ptr<BatchWriter>(std::move(writer));
    }
    return RealtimePrimaryKeyWriter::Create(partition_map, bucket, schema_, transport_schema,
                                            trimmed_primary_keys, key_comparator_, options_,
                                            realtime_context_impl, realtime_store_state.value(),
                                            restore_max_seq_number, writer, pool_);
}

Status KeyValueFileStoreWrite::RefreshCommittedSnapshot(int64_t snapshot_id) {
    if (!realtime_context_) {
        return Status::Invalid("refresh committed snapshot requires a real-time writer");
    }
    PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, snapshot_manager_->LoadSnapshot(snapshot_id));
    PAIMON_ASSIGN_OR_RAISE(
        RealtimeOffsetMap committed_offsets,
        RealtimeCommitProperties::ReadOffsets(std::optional<Snapshot>(std::move(snapshot)),
                                              options_.GetFileSystem()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                           RealtimeContextImpl::Cast(realtime_context_));
    return realtime_context_impl->AdvanceCommittedProgress(snapshot_id, committed_offsets);
}

Status KeyValueFileStoreWrite::Close() {
    PAIMON_RETURN_NOT_OK(AbstractFileStoreWrite::Close());
    compact_manager_factory_->Close();
    return Status::OK();
}

}  // namespace paimon
