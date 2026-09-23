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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/index/index_checkpoint_path_factory.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/io/global_index_checkpoint_file_manager.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/global_index/io/global_index_file_writer.h"

namespace paimon {
/// Helper class for managing global index files.
/// Checkpoint storage is optional and is never accessed by construction or ordinary index I/O.
class GlobalIndexFileManager : public GlobalIndexFileReader,
                               public GlobalIndexFileWriter,
                               public GlobalIndexCheckpointFileManager {
 public:
    GlobalIndexFileManager(
        const std::shared_ptr<FileSystem>& fs,
        const std::shared_ptr<IndexPathFactory>& path_factory,
        std::unique_ptr<IndexCheckpointPathFactory> checkpoint_path_factory = nullptr)
        : fs_(fs),
          path_factory_(path_factory),
          checkpoint_path_factory_(std::move(checkpoint_path_factory)) {}

    Result<std::unique_ptr<InputStream>> GetInputStream(
        const std::string& file_path) const override {
        return fs_->Open(file_path);
    }

    Result<std::string> NewFileName(const std::string& prefix) const override {
        std::string uuid;
        if (PAIMON_UNLIKELY(!UUID::Generate(&uuid))) {
            return Status::Invalid("fail to generate uuid for global index file manager");
        }
        return prefix + "-" + "global-index-" + uuid + ".index";
    }

    std::string ToPath(const std::string& file_name) const override {
        return path_factory_->ToPath(file_name);
    }

    std::string ToPath(const std::shared_ptr<IndexFileMeta>& file) const {
        return path_factory_->ToPath(file);
    }

    Result<std::unique_ptr<OutputStream>> NewOutputStream(
        const std::string& file_name) const override {
        return fs_->Create(ToPath(file_name), /*overwrite=*/false);
    }

    Result<int64_t> GetFileSize(const std::string& file_name) const override {
        PAIMON_ASSIGN_OR_RAISE(FileStatus file_status, fs_->GetFileStatus(ToPath(file_name)));
        return file_status.GetLen();
    }

    bool IsExternalPath() const {
        return path_factory_->IsExternalPath();
    }

    bool SupportsCheckpoint() const override {
        return checkpoint_path_factory_ != nullptr;
    }

    Result<std::unique_ptr<OutputStream>> CreateCheckpointOutputStream() const override {
        if (!SupportsCheckpoint()) {
            return Status::Invalid("global index checkpoint storage is not configured");
        }
        PAIMON_ASSIGN_OR_RAISE(std::string file_name, NewCheckpointFileName());
        PAIMON_RETURN_NOT_OK(fs_->Mkdirs(checkpoint_path_factory_->GetDirectoryPath()));
        return fs_->Create(checkpoint_path_factory_->ToPath(file_name), /*overwrite=*/false);
    }

    Result<std::unique_ptr<InputStream>> OpenCheckpointInputStream() const override {
        if (!SupportsCheckpoint()) {
            return Status::Invalid("global index checkpoint storage is not configured");
        }
        PAIMON_ASSIGN_OR_RAISE(std::optional<CheckpointFile> checkpoint_file,
                               LatestCheckpointFile());
        if (!checkpoint_file) {
            return Status::NotExist("global index checkpoint file does not exist");
        }
        return fs_->Open(checkpoint_file->path);
    }

    Result<bool> CheckpointExists() const override {
        if (!SupportsCheckpoint()) {
            return Status::Invalid("global index checkpoint storage is not configured");
        }
        PAIMON_ASSIGN_OR_RAISE(std::optional<CheckpointFile> checkpoint_file,
                               LatestCheckpointFile());
        return checkpoint_file.has_value();
    }

    Status DeleteCheckpoint() const override {
        if (!SupportsCheckpoint()) {
            return Status::Invalid("global index checkpoint storage is not configured");
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<CheckpointFile> checkpoint_files, ListCheckpointFiles());
        for (const CheckpointFile& checkpoint_file : checkpoint_files) {
            PAIMON_RETURN_NOT_OK(fs_->Delete(checkpoint_file.path, /*recursive=*/false));
        }
        return Status::OK();
    }

 private:
    struct CheckpointFile {
        int64_t id;
        std::string path;
    };

    Result<std::string> NewCheckpointFileName() const {
        if (!checkpoint_file_id_initialized_) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<CheckpointFile> checkpoint_file,
                                   LatestCheckpointFile());
            checkpoint_path_factory_->InitializeFileId(checkpoint_file ? checkpoint_file->id : -1);
            checkpoint_file_id_initialized_ = true;
        }
        PAIMON_ASSIGN_OR_RAISE(std::string path, checkpoint_path_factory_->NewPath());
        return PathUtil::GetName(path);
    }

    Result<std::vector<CheckpointFile>> ListCheckpointFiles() const {
        std::vector<BasicFileStatus> file_statuses;
        PAIMON_RETURN_NOT_OK(
            fs_->ListDir(checkpoint_path_factory_->GetDirectoryPath(), &file_statuses));
        std::vector<CheckpointFile> checkpoint_files;
        for (const BasicFileStatus& file_status : file_statuses) {
            if (file_status.IsDir()) {
                continue;
            }
            std::string file_name = PathUtil::GetName(file_status.GetPath());
            std::optional<int64_t> id = checkpoint_path_factory_->GetCheckpointId(file_name);
            if (!id) {
                continue;
            }
            checkpoint_files.push_back(
                CheckpointFile{id.value(), checkpoint_path_factory_->ToPath(file_name)});
        }
        return checkpoint_files;
    }

    Result<std::optional<CheckpointFile>> LatestCheckpointFile() const {
        PAIMON_ASSIGN_OR_RAISE(std::vector<CheckpointFile> checkpoint_files, ListCheckpointFiles());
        if (checkpoint_files.empty()) {
            return std::optional<CheckpointFile>();
        }
        CheckpointFile latest =
            *std::max_element(checkpoint_files.begin(), checkpoint_files.end(),
                              [](const CheckpointFile& left, const CheckpointFile& right) {
                                  return left.id < right.id;
                              });
        return std::optional<CheckpointFile>(std::move(latest));
    }

    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<IndexPathFactory> path_factory_;
    std::unique_ptr<IndexCheckpointPathFactory> checkpoint_path_factory_;
    // Historical ids are loaded only on the first successful file name allocation scan.
    mutable bool checkpoint_file_id_initialized_ = false;
};
}  // namespace paimon
