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

#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/core/operation/write_restore.h"
#include "paimon/core/utils/snapshot_manager.h"

namespace paimon {

/// `WriteRestore` to restore files directly from file system.
class FileSystemWriteRestore : public WriteRestore {
 public:
    /// Create a write restore backed by a file store scan.
    ///
    /// @param snapshot_manager Snapshot manager used to locate restore state.
    /// @param scan Scan used to load existing files.
    /// @param index_file_handler Handler used to restore index files.
    FileSystemWriteRestore(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                           std::unique_ptr<FileStoreScan>&& scan,
                           const std::shared_ptr<IndexFileHandler>& index_file_handler)
        : snapshot_manager_(snapshot_manager),
          scan_(std::move(scan)),
          index_file_handler_(index_file_handler) {}

    Result<int64_t> LatestCommittedIdentifier(const std::string& user) const override {
        // TODO(yonghao.fyh): in java paimon is LatestSnapshotOfUserFromFileSystem
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager_->LatestSnapshotOfUser(user));
        if (latest_snapshot) {
            return latest_snapshot.value().CommitIdentifier();
        }
        return std::numeric_limits<int64_t>::min();
    }

    Result<std::shared_ptr<RestoreFiles>> GetRestoreFiles(
        const BinaryRow& partition, int32_t bucket, bool scan_deletion_vectors_index,
        bool scan_source_index_payloads) const override {
        // TODO(yonghao.fyh): java paimon doesn't use snapshot_manager.LatestSnapshot() here,
        // because they don't want to flood the catalog with high concurrency
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                               snapshot_manager_->LatestSnapshot());
        if (snapshot == std::nullopt) {
            return RestoreFiles::Empty();
        }

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStoreScan::RawPlan> plan,
                               scan_->WithSnapshot(snapshot.value())->CreatePlan());
        std::vector<ManifestEntry> entries = plan->Files();
        std::vector<std::shared_ptr<DataFileMeta>> restore_data_files;
        PAIMON_ASSIGN_OR_RAISE(std::optional<int32_t> total_buckets,
                               WriteRestore::ExtractDataFiles(entries, &restore_data_files));

        std::vector<std::shared_ptr<IndexFileMeta>> deletion_vectors_index;
        if (scan_deletion_vectors_index) {
            PAIMON_ASSIGN_OR_RAISE(
                deletion_vectors_index,
                index_file_handler_->Scan(
                    snapshot.value(), std::string(DeletionVectorsIndexFile::DELETION_VECTORS_INDEX),
                    partition, bucket));
        }

        std::vector<std::shared_ptr<IndexFileMeta>> primary_key_index_payloads;
        if (scan_source_index_payloads) {
            if (index_file_handler_ == nullptr) {
                return Status::Invalid("Primary-key index restore requires an index file handler.");
            }
            PAIMON_ASSIGN_OR_RAISE(
                primary_key_index_payloads,
                index_file_handler_->ScanSourceIndexes(snapshot.value(), partition, bucket));
        }

        return std::make_shared<RestoreFiles>(snapshot, total_buckets, restore_data_files,
                                              /*dynamic_bucket_index=*/nullptr,
                                              deletion_vectors_index, primary_key_index_payloads);
    }

 private:
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::unique_ptr<FileStoreScan> scan_;
    std::shared_ptr<IndexFileHandler> index_file_handler_;
};

}  // namespace paimon
