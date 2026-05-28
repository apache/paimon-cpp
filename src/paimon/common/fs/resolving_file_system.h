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

#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// An implementation of `FileSystem` that supports multiple file system schemes. It
/// dynamically selects the appropriate `FileSystem` based on the URI scheme of the given path.
class ResolvingFileSystem : public FileSystem {
 public:
    ResolvingFileSystem(const std::map<std::string, std::string>& scheme_to_fs_identifier,
                        const std::string& default_fs_identifier,
                        const std::map<std::string, std::string>& options);
    ~ResolvingFileSystem() override = default;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override;
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override;

    Status Mkdirs(const std::string& path) const override;
    Status Rename(const std::string& src, const std::string& dst) const override;
    Status Delete(const std::string& path, bool recursive = true) const override;
    Result<std::unique_ptr<FileStatus>> GetFileStatus(const std::string& path) const override;
    Status ListDir(const std::string& directory,
                   std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) const override;
    Status ListFileStatus(
        const std::string& path,
        std::vector<std::unique_ptr<FileStatus>>* file_status_list) const override;
    Result<bool> Exists(const std::string& path) const override;

 private:
    Result<std::shared_ptr<FileSystem>> GetRealFileSystem(const std::string& uri) const;

 private:
    // e.g.: {{"file", "local"}, {"oss", "jindo"}}
    std::map<std::string, std::string> scheme_to_fs_identifier_;
    std::string default_fs_identifier_;
    std::map<std::string, std::string> options_;
    // {scheme, authority} -> FileSystem
    mutable std::map<std::pair<std::string, std::string>, std::shared_ptr<FileSystem>> fs_cache_;
    // Read-write lock for fs_cache_ thread safety
    mutable std::shared_mutex fs_cache_mutex_;
};

}  // namespace paimon
