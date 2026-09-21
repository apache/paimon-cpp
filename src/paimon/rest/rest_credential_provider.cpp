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

#include "paimon/rest/rest_credential_provider.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <mutex>
#include <set>
#include <utility>

#include "paimon/catalog_options.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon {

namespace {
/// Declared here rather than taken from the OSS file system, which is an optional build
/// component this module must not depend on.
constexpr const char kOssEndpointOption[] = "fs.oss.endpoint";
constexpr const char kOssOptionPrefix[] = "fs.oss.";
constexpr const char kOssBucketPrefix[] = "fs.oss.bucket.";
// The two OSS names for the STS token: the backend reads securityToken first and only
// consults sessionToken when it is empty, so a token under either name has to clear the
// catalog value under both.
constexpr const char kOssSecurityTokenSuffix[] = "securityToken";
constexpr const char kOssSessionTokenSuffix[] = "sessionToken";

/// The suffix of a flat OSS option, i.e. what follows "fs.oss." for a key like
/// "fs.oss.accessKeyId"; empty for a bucket-scoped ("fs.oss.bucket.<bucket>.<suffix>") or a
/// non-OSS key.
std::string FlatOssOptionSuffix(const std::string& key) {
    if (!StringUtils::StartsWith(key, kOssOptionPrefix) ||
        StringUtils::StartsWith(key, kOssBucketPrefix)) {
        return "";
    }
    return key.substr(std::string(kOssOptionPrefix).size());
}
}  // namespace

size_t RestToken::Hash::operator()(const RestToken& rest_token) const {
    size_t result = std::hash<int64_t>()(rest_token.expires_at_millis);
    for (const auto& [key, value] : rest_token.token) {
        result = result * 31 + std::hash<std::string>()(key);
        result = result * 31 + std::hash<std::string>()(value);
    }
    return result;
}

RestCredentialProvider::RestCredentialProvider(const std::shared_ptr<RestApi>& api,
                                               const Identifier& identifier, Clock clock)
    : api_(api),
      identifier_(identifier),
      clock_(std::move(clock)),
      logger_(Logger::GetLogger("RestCredentialProvider")) {}

bool RestCredentialProvider::ShouldRefresh() const {
    if (!token_) {
        return true;
    }
    int64_t now_millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(clock_().time_since_epoch()).count();
    return token_->expires_at_millis - now_millis < RestApi::kTokenExpirationSafeTimeMillis;
}

std::map<std::string, std::string> RestCredentialProvider::MergeOptionsWithCredentials(
    const std::map<std::string, std::string>& base_options,
    const std::map<std::string, std::string>& credentials) const {
    std::map<std::string, std::string> merged = base_options;
    for (const auto& [key, value] : credentials) {
        merged[key] = value;
    }
    // The OSS backend resolves a bucket-scoped option ("fs.oss.bucket.<b>.<suffix>") ahead of the
    // flat one and reads "fs.oss.securityToken" ahead of its "fs.oss.sessionToken" alias, so a
    // stale catalog value can shadow a credential the token just refreshed. Drop the stale
    // variants of every credential the token supplies -- including both security-token aliases
    // when it supplies either -- so the issued credential is the one that wins.
    std::set<std::string> refreshed_suffixes;
    bool refreshed_security_token = false;
    for (const auto& [key, value] : credentials) {
        std::string suffix = FlatOssOptionSuffix(key);
        if (suffix.empty()) {
            continue;
        }
        refreshed_suffixes.insert(suffix);
        refreshed_security_token = refreshed_security_token || suffix == kOssSecurityTokenSuffix ||
                                   suffix == kOssSessionTokenSuffix;
    }
    if (refreshed_security_token) {
        refreshed_suffixes.insert(kOssSecurityTokenSuffix);
        refreshed_suffixes.insert(kOssSessionTokenSuffix);
    }
    for (auto it = merged.begin(); it != merged.end();) {
        const std::string& key = it->first;
        bool stale_bucket_scoped = StringUtils::StartsWith(key, kOssBucketPrefix) &&
                                   std::any_of(refreshed_suffixes.begin(), refreshed_suffixes.end(),
                                               [&](const std::string& suffix) {
                                                   return StringUtils::EndsWith(key, "." + suffix);
                                               });
        bool stale_security_token_alias =
            refreshed_security_token && credentials.find(key) == credentials.end() &&
            (key == std::string(kOssOptionPrefix) + kOssSecurityTokenSuffix ||
             key == std::string(kOssOptionPrefix) + kOssSessionTokenSuffix);
        if (stale_bucket_scoped || stale_security_token_alias) {
            it = merged.erase(it);
        } else {
            ++it;
        }
    }
    // The DLF OSS endpoint overrides the standard one, since the credentials are issued
    // for the DLF endpoint rather than for the endpoint the catalog was configured with.
    auto dlf_oss_endpoint = base_options.find(CatalogOptions::DLF_OSS_ENDPOINT);
    if (dlf_oss_endpoint != base_options.end() && !dlf_oss_endpoint->second.empty()) {
        merged[kOssEndpointOption] = dlf_oss_endpoint->second;
    }
    return merged;
}

Status RestCredentialProvider::RefreshToken() const {
    PAIMON_LOG_INFO(logger_, "begin refresh data token for identifier [%s]",
                    identifier_.ToString().c_str());
    PAIMON_ASSIGN_OR_RAISE(GetTableTokenResponse response, api_->LoadTableToken(identifier_));
    PAIMON_LOG_INFO(logger_, "end refresh data token for identifier [%s] expiresAtMillis [%ld]",
                    identifier_.ToString().c_str(),
                    static_cast<int64_t>(response.GetExpiresAtMillis()));

    token_ = RestToken{response.GetToken(), response.GetExpiresAtMillis()};
    return Status::OK();
}

Result<RestToken> RestCredentialProvider::ValidToken() const {
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

Result<std::map<std::string, std::string>> RestCredentialProvider::GetCredentials() const {
    PAIMON_ASSIGN_OR_RAISE(RestToken token, ValidToken());
    return token.token;
}

}  // namespace paimon
