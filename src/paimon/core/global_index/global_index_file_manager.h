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
#include <string>

#include "fmt/format.h"
#include "paimon/common/global_index/cache_namespace_provider.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/global_index/io/global_index_file_writer.h"

namespace paimon {
/// Helper class for managing global index files.
class GlobalIndexFileManager : public GlobalIndexFileReader,
                               public GlobalIndexFileWriter,
                               public CacheNamespaceProvider {
 public:
    GlobalIndexFileManager(const std::shared_ptr<FileSystem>& fs,
                           const std::shared_ptr<IndexPathFactory>& path_factory)
        : fs_(fs), path_factory_(path_factory) {}

    std::string CacheNamespace() const override {
        return fmt::format("filesystem:{}", fmt::ptr(fs_.get()));
    }

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

 private:
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<IndexPathFactory> path_factory_;
};
}  // namespace paimon
