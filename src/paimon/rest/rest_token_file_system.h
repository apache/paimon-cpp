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

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "paimon/catalog/identifier.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"
#include "paimon/rest/rest_api.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// The temporary credentials of one table, as issued by the REST catalog.
struct RestToken {
    /// File system options, e.g. "fs.oss.accessKeyId".
    std::map<std::string, std::string> token;
    int64_t expires_at_millis = 0;

    /// Orders tokens so that one can key the file system cache.
    bool operator<(const RestToken& other) const {
        if (expires_at_millis != other.expires_at_millis) {
            return expires_at_millis < other.expires_at_millis;
        }
        return token < other.token;
    }
};

/// A `FileSystem` that accesses table data with the temporary credentials issued by the
/// REST catalog for one table, reloading them before they expire. Every operation is
/// delegated to the file system built from the credentials merged over the catalog
/// options, so the schemes configured for the catalog keep working.
class RestTokenFileSystem : public FileSystem {
 public:
    using Clock = std::function<std::chrono::system_clock::time_point()>;

    /// The file systems of the most recent credentials are retained so that a stream
    /// opened just before a rotation is not left with a destroyed file system.
    static constexpr size_t kMaxRetainedFileSystems = 4;

    /// @param api Client of the catalog that issues the credentials. Shared because this
    ///            file system commonly outlives the catalog it was obtained from.
    /// @param catalog_options Options the credentials are merged over.
    /// @param identifier The table the credentials are requested for.
    /// @param clock Source of the current time, overridable for tests.
    RestTokenFileSystem(const std::shared_ptr<RestApi>& api,
                        const std::map<std::string, std::string>& catalog_options,
                        const Identifier& identifier, Clock clock = std::chrono::system_clock::now);

    ~RestTokenFileSystem() override = default;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override;
    Result<std::unique_ptr<InputStream>> Open(const FileStatus& file_status) const override;
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override;

    Status Mkdirs(const std::string& path) const override;
    Status Rename(const std::string& src, const std::string& dst) const override;
    Status Delete(const std::string& path, bool recursive = true) const override;
    Result<FileStatus> GetFileStatus(const std::string& path) const override;
    Status ListDir(const std::string& directory,
                   std::vector<BasicFileStatus>* file_status_list) const override;
    Status ListFileStatus(const std::string& path,
                          std::vector<FileStatus>* file_status_list) const override;
    Result<bool> Exists(const std::string& path) const override;

    /// Returns credentials that are not about to expire, reloading them when needed. Lets
    /// a caller that brings its own file system use the credentials of this table.
    Result<RestToken> ValidToken() const;

 private:
    /// Returns the file system of the current credentials, reloading them when they
    /// expire in less than `RestApi::kTokenExpirationSafeTimeMillis`.
    Result<std::shared_ptr<FileSystem>> Delegate() const;

    /// Reloads the credentials and builds their file system. Called with the write lock
    /// of `mutex_` held.
    Status Refresh() const;

    /// Whether `token_` is absent or expires within the safe time.
    bool ShouldRefresh() const;

    /// `catalog_options_` with `token` merged over it.
    std::map<std::string, std::string> MergeTokenOptions(
        const std::map<std::string, std::string>& token) const;

    std::shared_ptr<RestApi> api_;
    std::map<std::string, std::string> catalog_options_;
    Identifier identifier_;
    Clock clock_;
    std::shared_ptr<Logger> logger_;

    mutable std::shared_mutex mutex_;
    mutable std::optional<RestToken> token_;
    mutable std::shared_ptr<FileSystem> fs_;
    /// Retains the file systems of the recent credentials, oldest first.
    mutable std::deque<std::shared_ptr<FileSystem>> retained_fs_;
};

}  // namespace paimon
