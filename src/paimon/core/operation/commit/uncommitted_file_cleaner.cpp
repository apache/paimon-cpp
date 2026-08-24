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

#include "paimon/core/operation/commit/uncommitted_file_cleaner.h"

#include <set>
#include <string>

#include "paimon/commit_message.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"

namespace paimon {

Status UncommittedFileCleaner::Delete(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<FileSystem>& fs,
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages, Logger* logger) {
    // A message this cleaner cannot handle stops that message, never the ones after it: giving
    // up on the whole list would strand every file the remaining messages describe, which is
    // the leak this class exists to prevent. The first such failure is reported once the list
    // has been walked.
    Status first_error = Status::OK();
    for (const auto& message : commit_messages) {
        auto* msg = dynamic_cast<CommitMessageImpl*>(message.get());
        if (msg == nullptr) {
            if (first_error.ok()) {
                first_error = Status::Invalid("fail to cast commit message to impl");
            }
            continue;
        }
        Result<std::shared_ptr<DataFilePathFactory>> data_file_path_factory_result =
            path_factory->CreateDataFilePathFactory(msg->Partition(), msg->Bucket());
        Result<std::unique_ptr<IndexPathFactory>> index_file_path_factory_result =
            path_factory->CreateIndexFileFactory(msg->Partition(), msg->Bucket());
        if (!data_file_path_factory_result.ok() || !index_file_path_factory_result.ok()) {
            const Status& status = data_file_path_factory_result.ok()
                                       ? index_file_path_factory_result.status()
                                       : data_file_path_factory_result.status();
            if (first_error.ok()) {
                first_error = status;
            }
            PAIMON_LOG_WARN(logger,
                            "Cannot resolve the paths of an uncommitted message in bucket %d: %s. "
                            "Its files are left behind.",
                            msg->Bucket(), status.ToString().c_str());
            continue;
        }
        std::shared_ptr<DataFilePathFactory> data_file_path_factory =
            std::move(data_file_path_factory_result).value();
        std::unique_ptr<IndexPathFactory> index_file_path_factory =
            std::move(index_file_path_factory_result).value();

        const DataIncrement& new_files_increment = msg->GetNewFilesIncrement();
        const CompactIncrement& compact_increment = msg->GetCompactIncrement();

        std::vector<std::shared_ptr<DataFileMeta>> data_files_to_delete;
        auto append_data_files =
            [&data_files_to_delete](const std::vector<std::shared_ptr<DataFileMeta>>& files) {
                data_files_to_delete.insert(data_files_to_delete.end(), files.begin(), files.end());
            };
        append_data_files(new_files_increment.NewFiles());
        append_data_files(new_files_increment.ChangelogFiles());
        append_data_files(compact_increment.CompactAfter());
        append_data_files(compact_increment.ChangelogFiles());
        for (const auto& file : data_files_to_delete) {
            // A rolled back data file takes its companion files with it (e.g. a managed blob
            // reference sidecar), the same as everywhere else a data file is removed.
            for (const std::string& path : data_file_path_factory->CollectFiles(file)) {
                // Best-effort cleanup: delete failures are ignored.
                [[maybe_unused]] Status status = fs->Delete(path, /*recursive=*/false);
            }
        }
        // Only the packs this message's writer created, never the ones its files merely
        // reference, for the reason the class documents. Nothing else collects a pack — orphan
        // file cleaning skips `.managed.blob`, because a pack may be shared by several data
        // files — so a failure here leaks it for good and is worth naming.
        std::set<std::string> managed_blob_packs(msg->OwnedManagedBlobPacks().begin(),
                                                 msg->OwnedManagedBlobPacks().end());
        for (const std::string& pack_path : managed_blob_packs) {
            Status status = fs->Delete(pack_path, /*recursive=*/false);
            if (!status.ok() && !status.IsNotExist()) {
                PAIMON_LOG_WARN(logger,
                                "Failed to delete the managed blob pack %s of an uncommitted "
                                "message: %s. Nothing collects it later.",
                                pack_path.c_str(), status.ToString().c_str());
            }
        }

        std::vector<std::shared_ptr<IndexFileMeta>> index_files_to_delete;
        auto append_index_files = [&index_files_to_delete](
                                      const std::vector<std::shared_ptr<IndexFileMeta>>& files) {
            index_files_to_delete.insert(index_files_to_delete.end(), files.begin(), files.end());
        };
        append_index_files(new_files_increment.NewIndexFiles());
        append_index_files(compact_increment.NewIndexFiles());
        for (const auto& file : index_files_to_delete) {
            [[maybe_unused]] Status status =
                fs->Delete(index_file_path_factory->ToPath(file), /*recursive=*/false);
        }
    }
    return first_error;
}

}  // namespace paimon
