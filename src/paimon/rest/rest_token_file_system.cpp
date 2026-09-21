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

#include "paimon/rest/rest_token_file_system.h"

#include <string>
#include <utility>

#include "paimon/core/core_options.h"

namespace paimon {

std::shared_ptr<RestTokenFileSystemCache> RestTokenFileSystem::CreateFileSystemCache() {
    RestTokenFileSystemCache::Options cache_options;
    cache_options.max_weight = kMaxCachedFileSystems;
    cache_options.expire_after_access_ms = kFileSystemCacheExpireAfterAccessMillis;
    return std::make_shared<RestTokenFileSystemCache>(std::move(cache_options));
}

RestTokenFileSystem::RestTokenFileSystem(
    std::shared_ptr<RestCredentialProvider> provider,
    const std::map<std::string, std::string>& catalog_options,
    std::shared_ptr<RestTokenFileSystemCache> fs_cache,
    const std::map<std::string, std::string>& fs_scheme_to_identifier_map)
    : catalog_options_(catalog_options),
      fs_scheme_to_identifier_map_(fs_scheme_to_identifier_map),
      fs_cache_(std::move(fs_cache)),
      provider_(std::move(provider)) {}

std::map<std::string, std::string> RestTokenFileSystem::MergeTokenOptions(
    const std::map<std::string, std::string>& catalog_options, const RestToken& token) {
    std::map<std::string, std::string> fs_options = catalog_options;
    for (const auto& [key, value] : token.token) {
        fs_options[key] = value;
    }
    return fs_options;
}

Result<std::shared_ptr<FileSystem>> RestTokenFileSystem::BuildFileSystem(
    const RestToken& token) const {
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions core_options,
        CoreOptions::FromMap(MergeTokenOptions(catalog_options_, token),
                             /*specified_file_system=*/nullptr, fs_scheme_to_identifier_map_));
    std::shared_ptr<FileSystem> fs = core_options.GetFileSystem();
    if (fs == nullptr) {
        return Status::Invalid("failed to build the file system from the data token credentials");
    }
    return fs;
}

Result<std::shared_ptr<FileSystem>> RestTokenFileSystem::Delegate() const {
    PAIMON_ASSIGN_OR_RAISE(RestToken token, ValidToken());
    return fs_cache_->Get(
        token, [this](const RestToken& cached_token) { return BuildFileSystem(cached_token); });
}

Result<RestToken> RestTokenFileSystem::ValidToken() const {
    return provider_->ValidToken();
}

Result<std::unique_ptr<InputStream>> RestTokenFileSystem::Open(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, Delegate());
    return fs->Open(path);
}

Result<std::unique_ptr<InputStream>> RestTokenFileSystem::Open(
    const FileStatus& file_status) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, Delegate());
    return fs->Open(file_status);
}

Result<std::unique_ptr<OutputStream>> RestTokenFileSystem::Create(const std::string& path,
                                                                  bool overwrite) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, Delegate());
    return fs->Create(path, overwrite);
}

Status RestTokenFileSystem::Mkdirs(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, Delegate());
    return fs->Mkdirs(path);
}

Status RestTokenFileSystem::Rename(const std::string& src, const std::string& dst) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, Delegate());
    return fs->Rename(src, dst);
}

Status RestTokenFileSystem::Delete(const std::string& path, bool recursive) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, Delegate());
    return fs->Delete(path, recursive);
}

Result<FileStatus> RestTokenFileSystem::GetFileStatus(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, Delegate());
    return fs->GetFileStatus(path);
}

Status RestTokenFileSystem::ListDir(const std::string& directory,
                                    std::vector<BasicFileStatus>* file_status_list) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, Delegate());
    return fs->ListDir(directory, file_status_list);
}

Status RestTokenFileSystem::ListFileStatus(const std::string& path,
                                           std::vector<FileStatus>* file_status_list) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, Delegate());
    return fs->ListFileStatus(path, file_status_list);
}

Result<bool> RestTokenFileSystem::Exists(const std::string& path) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileSystem> fs, Delegate());
    return fs->Exists(path);
}

}  // namespace paimon
