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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/fs/credential_provider.h"
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

    /// Credentials are interchangeable only when they grant the same access until the same
    /// point in time, which is what makes them a file system cache key.
    bool operator==(const RestToken& other) const {
        return expires_at_millis == other.expires_at_millis && token == other.token;
    }

    struct Hash {
        size_t operator()(const RestToken& rest_token) const;
    };
};

/// The default `CredentialProvider`: it loads the temporary credentials the REST catalog issues
/// for the data of one table and reloads them before they expire, so every access draws
/// credentials that are still valid without the caller reaching the server or tracking the
/// expiry itself.
///
/// The credentials are cached, and a refresh happens only once they are absent or expire
/// within `RestApi::kTokenExpirationSafeTimeMillis`, so repeated accesses within the valid
/// window ask the server nothing.
class RestCredentialProvider : public CredentialProvider {
 public:
    using Clock = std::function<std::chrono::system_clock::time_point()>;

    /// @param api Client of the catalog that issues the credentials. Shared because a
    ///            provider commonly outlives the catalog it was obtained from.
    /// @param catalog_options Options the credentials are merged over.
    /// @param identifier The table the credentials are requested for.
    /// @param clock Source of the current time, overridable for tests.
    RestCredentialProvider(const std::shared_ptr<RestApi>& api,
                           const std::map<std::string, std::string>& catalog_options,
                           const Identifier& identifier,
                           Clock clock = std::chrono::system_clock::now);

    ~RestCredentialProvider() override = default;

    /// Returns credentials that are not about to expire, reloading them when needed. The
    /// `RestToken` also carries the expiration, which is what a file system cache keys the
    /// delegates it builds from these credentials by.
    Result<RestToken> ValidToken() const;

    /// The credentials to sign an access with, reloaded before they expire. This is the
    /// `CredentialProvider` side of the same credentials `ValidToken()` serves, so the file
    /// systems built from them and the callers handed these credentials draw from one
    /// source.
    Result<std::map<std::string, std::string>> GetCredentials() const override;

 private:
    /// Reloads the credentials from the server. Called with the write lock of `mutex_`
    /// held.
    Status RefreshToken() const;

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

    /// Guards `token_`.
    mutable std::shared_mutex mutex_;
    mutable std::optional<RestToken> token_;
};

}  // namespace paimon
