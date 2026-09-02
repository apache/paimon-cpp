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

RestTokenFileSystem::RestTokenFileSystem(const std::shared_ptr<RestApi>& api,
                                         const std::map<std::string, std::string>& catalog_options,
                                         const Identifier& identifier, Clock clock)
    : api_(api),
      catalog_options_(catalog_options),
      identifier_(identifier),
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

Status RestTokenFileSystem::Refresh() const {
    PAIMON_LOG_INFO(logger_, "begin refresh data token for identifier [%s]",
                    identifier_.ToString().c_str());
    PAIMON_ASSIGN_OR_RAISE(GetTableTokenResponse response, api_->LoadTableToken(identifier_));
    PAIMON_LOG_INFO(logger_, "end refresh data token for identifier [%s] expiresAtMillis [%ld]",
                    identifier_.ToString().c_str(),
                    static_cast<int64_t>(response.GetExpiresAtMillis()));

    RestToken token{MergeTokenOptions(response.GetToken()), response.GetExpiresAtMillis()};
    // The credentials are the only file system options that change, so the file system is
    // rebuilt from the catalog options with the credentials merged over them.
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

    retained_fs_.push_back(fs);
    while (retained_fs_.size() > kMaxRetainedFileSystems) {
        retained_fs_.pop_front();
    }
    token_ = std::move(token);
    fs_ = std::move(fs);
    return Status::OK();
}

Result<std::shared_ptr<FileSystem>> RestTokenFileSystem::Delegate() const {
    {
        std::shared_lock<std::shared_mutex> read_lock(mutex_);
        if (!ShouldRefresh()) {
            return fs_;
        }
    }

    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    // Double-check, another thread may have refreshed while this one waited for the lock.
    if (!ShouldRefresh()) {
        return fs_;
    }
    PAIMON_RETURN_NOT_OK(Refresh());
    return fs_;
}

Result<RestToken> RestTokenFileSystem::ValidToken() const {
    {
        std::shared_lock<std::shared_mutex> read_lock(mutex_);
        if (!ShouldRefresh()) {
            return token_.value();
        }
    }

    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    if (!ShouldRefresh()) {
        return token_.value();
    }
    PAIMON_RETURN_NOT_OK(Refresh());
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
