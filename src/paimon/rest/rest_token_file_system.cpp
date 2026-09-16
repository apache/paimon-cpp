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

#include <functional>
#include <mutex>
#include <utility>

#include "paimon/catalog_options.h"
#include "paimon/core/core_options.h"

namespace paimon {

namespace {
/// Declared here rather than taken from the OSS file system, which is an optional build
/// component this module must not depend on.
constexpr const char kOssEndpointOption[] = "fs.oss.endpoint";
}  // namespace

size_t RestToken::Hash::operator()(const RestToken& rest_token) const {
    size_t result = std::hash<int64_t>()(rest_token.expires_at_millis);
    for (const auto& [key, value] : rest_token.token) {
        result = result * 31 + std::hash<std::string>()(key);
        result = result * 31 + std::hash<std::string>()(value);
    }
    return result;
}

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
    : api_(api),
      catalog_options_(catalog_options),
      identifier_(identifier),
      fs_cache_(fs_cache != nullptr ? std::move(fs_cache) : CreateFileSystemCache()),
      clock_(std::move(clock)),
      logger_(Logger::GetLogger("RestTokenFileSystem")) {}

bool RestTokenFileSystem::ShouldRefresh() const {
    if (!token_) {
        return true;
    }
    int64_t now_millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(clock_().time_since_epoch()).count();
    return token_->expires_at_millis - now_millis < RestApi::kTokenExpirationSafeTimeMillis;
}

std::map<std::string, std::string> RestTokenFileSystem::MergeTokenOptions(
    const std::map<std::string, std::string>& token) const {
    std::map<std::string, std::string> merged = token;
    // The DLF OSS endpoint overrides the standard one, since the credentials are issued
    // for the DLF endpoint rather than for the endpoint the catalog was configured with.
    auto dlf_oss_endpoint = catalog_options_.find(CatalogOptions::DLF_OSS_ENDPOINT);
    if (dlf_oss_endpoint != catalog_options_.end() && !dlf_oss_endpoint->second.empty()) {
        merged[kOssEndpointOption] = dlf_oss_endpoint->second;
    }
    return merged;
}

Status RestTokenFileSystem::RefreshToken() const {
    PAIMON_LOG_INFO(logger_, "begin refresh data token for identifier [%s]",
                    identifier_.ToString().c_str());
    PAIMON_ASSIGN_OR_RAISE(GetTableTokenResponse response, api_->LoadTableToken(identifier_));
    PAIMON_LOG_INFO(logger_, "end refresh data token for identifier [%s] expiresAtMillis [%ld]",
                    identifier_.ToString().c_str(),
                    static_cast<int64_t>(response.GetExpiresAtMillis()));

    token_ = RestToken{MergeTokenOptions(response.GetToken()), response.GetExpiresAtMillis()};
    return Status::OK();
}

Result<std::shared_ptr<FileSystem>> RestTokenFileSystem::BuildFileSystem(
    const RestToken& token) const {
    std::map<std::string, std::string> fs_options = catalog_options_;
    for (const auto& [key, value] : token.token) {
        fs_options[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(fs_options, /*specified_file_system=*/nullptr));
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
    {
        std::shared_lock<std::shared_mutex> read_lock(mutex_);
        if (!ShouldRefresh()) {
            return token_.value();
        }
    }

    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    // Double-check, another thread may have refreshed while this one waited for the lock.
    if (!ShouldRefresh()) {
        return token_.value();
    }
    PAIMON_RETURN_NOT_OK(RefreshToken());
    return token_.value();
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
