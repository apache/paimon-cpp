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

#include "paimon/common/fs/resolving_file_system.h"

#include <mutex>
#include <shared_mutex>
#include <utility>

#include "paimon/common/utils/path_util.h"
#include "paimon/fs/file_system_factory.h"

namespace paimon {

ResolvingFileSystem::ResolvingFileSystem(
    const std::map<std::string, std::string>& scheme_to_fs_identifier,
    const std::string& default_fs_identifier, const std::map<std::string, std::string>& options)
    : scheme_to_fs_identifier_(scheme_to_fs_identifier),
      default_fs_identifier_(default_fs_identifier),
      options_(options) {
    // local file's scheme may be 'file' or empty
    auto identifier_iter = scheme_to_fs_identifier_.find("file");
    if (identifier_iter != scheme_to_fs_identifier_.end()) {
        scheme_to_fs_identifier_[""] = identifier_iter->second;
    }
}

Result<std::shared_ptr<FileSystem>> ResolvingFileSystem::GetRealFileSystem(
    const std::string& uri) const {
    PAIMON_ASSIGN_OR_RAISE(Path path, PathUtil::ToPath(uri));

    // Try to get file system from cache with shared lock (read lock)
    {
        std::shared_lock<std::shared_mutex> read_lock(fs_cache_mutex_);
        auto fs_iter = fs_cache_.find({path.scheme, path.authority});
        if (fs_iter != fs_cache_.end()) {
            return fs_iter->second;
        }
    }

    // Cache miss, create file system and set it to cache with exclusive lock (write lock)
    std::unique_lock<std::shared_mutex> write_lock(fs_cache_mutex_);

    // Double-check pattern: check again after acquiring write lock
    auto fs_iter = fs_cache_.find({path.scheme, path.authority});
    if (fs_iter != fs_cache_.end()) {
        return fs_iter->second;
    }

    // Create file system
    std::string identifier = default_fs_identifier_;
    auto identifier_iter = scheme_to_fs_identifier_.find(path.scheme);
    if (identifier_iter != scheme_to_fs_identifier_.end()) {
        identifier = identifier_iter->second;
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs,
                           FileSystemFactory::Get(identifier, uri, options_));
    fs_cache_.emplace(std::make_pair(path.scheme, path.authority), fs);
    return fs;
}

Result<std::unique_ptr<InputStream>> ResolvingFileSystem::Open(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, GetRealFileSystem(path));
    return fs->Open(path);
}

Result<std::unique_ptr<OutputStream>> ResolvingFileSystem::Create(const std::string& path,
                                                                  bool overwrite) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, GetRealFileSystem(path));
    return fs->Create(path, overwrite);
}

Status ResolvingFileSystem::Mkdirs(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, GetRealFileSystem(path));
    return fs->Mkdirs(path);
}

Status ResolvingFileSystem::Rename(const std::string& src, const std::string& dst) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, GetRealFileSystem(src));
    return fs->Rename(src, dst);
}

Status ResolvingFileSystem::Delete(const std::string& path, bool recursive) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, GetRealFileSystem(path));
    return fs->Delete(path, recursive);
}

Result<std::unique_ptr<FileStatus>> ResolvingFileSystem::GetFileStatus(
    const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, GetRealFileSystem(path));
    return fs->GetFileStatus(path);
}

Status ResolvingFileSystem::ListDir(
    const std::string& directory,
    std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, GetRealFileSystem(directory));
    return fs->ListDir(directory, file_status_list);
}

Status ResolvingFileSystem::ListFileStatus(
    const std::string& path, std::vector<std::unique_ptr<FileStatus>>* file_status_list) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, GetRealFileSystem(path));
    return fs->ListFileStatus(path, file_status_list);
}

Result<bool> ResolvingFileSystem::Exists(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, GetRealFileSystem(path));
    return fs->Exists(path);
}

}  // namespace paimon
