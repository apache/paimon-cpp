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

#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"

namespace paimon {
namespace {

// Length of the "fs.<scheme>." prefix of a file system option key, or 0 when `key` is not
// scheme-qualified. Only a scheme-qualified key such as "fs.oss.accessKeyId" can have a
// bucket-scoped variant such as "fs.oss.bucket.<bucket>.accessKeyId".
size_t FileSystemOptionPrefixLength(const std::string& key) {
    if (!StringUtils::StartsWith(key, "fs.")) {
        return 0;
    }
    size_t scheme_end = key.find('.', 3);
    return scheme_end == std::string::npos ? 0 : scheme_end + 1;
}

}  // namespace

std::shared_ptr<RestTokenFileSystemCache> RestTokenFileSystem::CreateFileSystemCache() {
    RestTokenFileSystemCache::Options cache_options;
    cache_options.max_weight = kMaxCachedFileSystems;
    cache_options.expire_after_access_ms = kFileSystemCacheExpireAfterAccessMillis;
    return std::make_shared<RestTokenFileSystemCache>(std::move(cache_options));
}

RestTokenFileSystem::RestTokenFileSystem(const std::shared_ptr<RestApi>& api,
                                         const std::map<std::string, std::string>& catalog_options,
                                         const Identifier& identifier,
                                         std::shared_ptr<RestTokenFileSystemCache> fs_cache,
                                         Clock clock)
    : catalog_options_(catalog_options),
      identifier_(identifier),
      fs_cache_(fs_cache != nullptr ? std::move(fs_cache) : CreateFileSystemCache()),
      provider_(std::make_shared<RestCredentialProvider>(api, catalog_options, identifier,
                                                         std::move(clock))) {}

std::map<std::string, std::string> RestTokenFileSystem::MergeTokenOptions(
    const std::map<std::string, std::string>& catalog_options, const RestToken& token) {
    std::map<std::string, std::string> fs_options = catalog_options;
    for (const auto& [key, value] : token.token) {
        // A file system resolves a bucket-scoped option such as
        // "fs.oss.bucket.<bucket>.accessKeyId" ahead of the flat "fs.oss.accessKeyId" the
        // token carries. Keeping a catalog's bucket-scoped credential would sign an access
        // with that stale key pair together with the token's security token, an invalid
        // combination, so drop the bucket-scoped variant of every option the token sets: the
        // issued credentials then win whichever bucket the table's data lives in.
        size_t prefix_length = FileSystemOptionPrefixLength(key);
        if (prefix_length != 0) {
            std::string bucket_prefix = key.substr(0, prefix_length) + "bucket.";
            std::string bucket_suffix = "." + key.substr(prefix_length);
            for (auto it = fs_options.begin(); it != fs_options.end();) {
                if (it->first.size() > bucket_prefix.size() + bucket_suffix.size() &&
                    StringUtils::StartsWith(it->first, bucket_prefix) &&
                    StringUtils::EndsWith(it->first, bucket_suffix)) {
                    it = fs_options.erase(it);
                } else {
                    ++it;
                }
            }
        }
        fs_options[key] = value;
    }
    return fs_options;
}

Result<std::shared_ptr<FileSystem>> RestTokenFileSystem::BuildFileSystem(
    const RestToken& token) const {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(MergeTokenOptions(catalog_options_, token),
                                                /*specified_file_system=*/nullptr));
    std::shared_ptr<FileSystem> fs = core_options.GetFileSystem();
    if (fs == nullptr) {
        return Status::Invalid("failed to build the file system of the data token of ",
                               identifier_.ToString());
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
