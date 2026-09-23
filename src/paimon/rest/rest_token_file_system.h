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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/utils/generic_lru_cache.h"
#include "paimon/fs/file_system.h"
#include "paimon/rest/rest_credential_provider.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// File systems keyed by the credentials they were built from, so that the tables the
/// server issues the same credentials for share one file system. Sharing this cache
/// between the `RestTokenFileSystem` instances of many tables keeps a rotation of one
/// table's credentials from rebuilding the file systems of the others.
using RestTokenFileSystemCache =
    GenericLruCache<RestToken, std::shared_ptr<FileSystem>, RestToken::Hash>;

/// A `FileSystem` that accesses table data with the temporary credentials issued by the
/// REST catalog for one table, reloading them before they expire. Every operation is
/// delegated to the file system built from the credentials merged over the catalog
/// options, so the schemes configured for the catalog keep working.
class RestTokenFileSystem : public FileSystem {
 public:
    /// Bounds of the file system cache, matching the Java client: the file system of
    /// credentials that were not used for this long is dropped, which also keeps a stream
    /// opened just before a rotation from losing the file system it came from.
    static constexpr int64_t kFileSystemCacheExpireAfterAccessMillis = 10 * 3600 * 1000;
    static constexpr int64_t kMaxCachedFileSystems = 1000;

    /// Creates a cache the file systems of many tables can share.
    static std::shared_ptr<RestTokenFileSystemCache> CreateFileSystemCache();

    /// @param provider Source of this table's credentials, built and owned by the catalog
    ///                 and injected so this file system neither reaches the server nor knows
    ///                 how the credentials are obtained. Shared because this file system
    ///                 commonly outlives the catalog it was obtained from.
    /// @param catalog_options Options the credentials are merged over to build a delegate.
    /// @param fs_cache Cache of the delegates, shared with the file systems of the other
    ///                 tables of the same catalog so that tables issued equal credentials
    ///                 reuse one delegate. The catalog owns it and hands it out.
    /// @param fs_scheme_to_identifier_map Maps a URI scheme to the registered file system
    ///                 identifier that serves it, so the delegate built from the credentials
    ///                 routes each scheme the same way the catalog-level file system does.
    RestTokenFileSystem(std::shared_ptr<RestCredentialProvider> provider,
                        const std::map<std::string, std::string>& catalog_options,
                        std::shared_ptr<RestTokenFileSystemCache> fs_cache,
                        const std::map<std::string, std::string>& fs_scheme_to_identifier_map = {});

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
    /// a caller that brings its own file system use the credentials of this table, so it
    /// builds no file system of its own and needs nothing but the catalog options the
    /// credentials are requested with.
    Result<RestToken> ValidToken() const;

 private:
    /// Returns the file system of the current credentials, reloading them when they
    /// expire in less than `RestApi::kTokenExpirationSafeTimeMillis`.
    Result<std::shared_ptr<FileSystem>> Delegate() const;

    /// Builds the file system of `token`: the credentials are the only file system options
    /// that change, so it is built from the catalog options with them merged over.
    Result<std::shared_ptr<FileSystem>> BuildFileSystem(const RestToken& token) const;

    std::map<std::string, std::string> catalog_options_;
    /// Maps a URI scheme to the registered file system identifier that serves it, applied when
    /// the delegate is built from the merged options so a data token access routes each scheme
    /// the same way the catalog-level file system does.
    std::map<std::string, std::string> fs_scheme_to_identifier_map_;
    std::shared_ptr<RestTokenFileSystemCache> fs_cache_;

    /// The credentials this file system delegates with, reloaded before they expire.
    std::shared_ptr<RestCredentialProvider> provider_;
};

}  // namespace paimon
