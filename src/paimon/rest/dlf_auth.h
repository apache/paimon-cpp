/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/utils/http_client.h"
#include "paimon/rest/rest_auth.h"

namespace paimon {

/// Access key credentials used to sign DLF REST requests.
class DlfToken {
 public:
    DlfToken(const std::string& access_key_id, const std::string& access_key_secret,
             const std::optional<std::string>& security_token,
             const std::optional<int64_t>& expiration_at_millis);

    static Result<DlfToken> FromJson(const std::string& json);

    const std::string& GetAccessKeyId() const {
        return access_key_id_;
    }

    const std::string& GetAccessKeySecret() const {
        return access_key_secret_;
    }

    const std::optional<std::string>& GetSecurityToken() const {
        return security_token_;
    }

    const std::optional<int64_t>& GetExpirationAtMillis() const {
        return expiration_at_millis_;
    }

    bool ShouldRefresh(std::chrono::system_clock::time_point now) const;

 private:
    std::string access_key_id_;
    std::string access_key_secret_;
    std::optional<std::string> security_token_;
    std::optional<int64_t> expiration_at_millis_;
};

/// Loads refreshable DLF credentials.
class DlfTokenLoader {
 public:
    virtual ~DlfTokenLoader() = default;

    virtual Result<DlfToken> LoadToken() = 0;
    virtual std::string Description() const = 0;
};

/// Loads a DLF STS token from a JSON file.
class DlfLocalFileTokenLoader : public DlfTokenLoader {
 public:
    DlfLocalFileTokenLoader(const std::string& token_file_path, int32_t max_attempts,
                            std::chrono::milliseconds retry_delay);

    Result<DlfToken> LoadToken() override;
    std::string Description() const override;

 private:
    std::string token_file_path_;
    int32_t max_attempts_;
    std::chrono::milliseconds retry_delay_;
};

/// Loads a DLF STS token from the Alibaba Cloud ECS metadata service.
class DlfEcsTokenLoader : public DlfTokenLoader {
 public:
    DlfEcsTokenLoader(const std::string& metadata_url, const std::optional<std::string>& role_name,
                      std::unique_ptr<HttpClient> http_client);

    static std::unique_ptr<DlfEcsTokenLoader> Create(const std::string& metadata_url,
                                                     const std::optional<std::string>& role_name);

    Result<DlfToken> LoadToken() override;
    std::string Description() const override;

 private:
    Result<std::string> Get(const std::string& url) const;

    std::string metadata_url_;
    std::optional<std::string> role_name_;
    std::unique_ptr<HttpClient> http_client_;
};

/// Signs a DLF REST request using one of the endpoint-specific algorithms.
class DlfRequestSigner {
 public:
    using Headers = std::map<std::string, std::string>;

    virtual ~DlfRequestSigner() = default;

    virtual Result<Headers> SignHeaders(const std::string& body,
                                        std::chrono::system_clock::time_point now,
                                        const std::optional<std::string>& security_token,
                                        const std::string& host) const = 0;

    virtual Result<std::string> Authorization(const RestAuthParameter& parameter,
                                              const DlfToken& token, const std::string& host,
                                              const Headers& sign_headers) const = 0;
};

/// DLF4-HMAC-SHA256 signer used by the default DLF VPC endpoint.
class DlfDefaultSigner : public DlfRequestSigner {
 public:
    static constexpr const char* kIdentifier = "default";

    explicit DlfDefaultSigner(const std::string& region);

    Result<Headers> SignHeaders(const std::string& body, std::chrono::system_clock::time_point now,
                                const std::optional<std::string>& security_token,
                                const std::string& host) const override;

    Result<std::string> Authorization(const RestAuthParameter& parameter, const DlfToken& token,
                                      const std::string& host,
                                      const Headers& sign_headers) const override;

 private:
    std::string region_;
};

/// ROA HMAC-SHA1 signer used by DlfNext OpenAPI endpoints.
class DlfOpenApiSigner : public DlfRequestSigner {
 public:
    static constexpr const char* kIdentifier = "openapi";

    Result<Headers> SignHeaders(const std::string& body, std::chrono::system_clock::time_point now,
                                const std::optional<std::string>& security_token,
                                const std::string& host) const override;

    Result<std::string> Authorization(const RestAuthParameter& parameter, const DlfToken& token,
                                      const std::string& host,
                                      const Headers& sign_headers) const override;
};

/// Generates DLF authentication headers and refreshes expiring credentials.
class DlfAuthProvider : public AuthProvider {
 public:
    using Clock = std::function<std::chrono::system_clock::time_point()>;

    static Result<std::unique_ptr<DlfAuthProvider>> Create(
        const std::map<std::string, std::string>& options);

    static Result<std::unique_ptr<DlfAuthProvider>> FromAccessKey(
        const DlfToken& token, const std::string& uri, const std::string& region,
        const std::string& signing_algorithm, Clock clock);

    static Result<std::unique_ptr<DlfAuthProvider>> FromTokenLoader(
        std::unique_ptr<DlfTokenLoader> token_loader, const std::string& uri,
        const std::string& region, const std::string& signing_algorithm, Clock clock);

    Result<std::map<std::string, std::string>> MergeAuthHeader(
        const std::map<std::string, std::string>& base_header,
        const RestAuthParameter& parameter) const override;

    bool AllowsRedirects() const override {
        return false;
    }

    static Result<std::string> ParseRegionFromUri(const std::string& uri);
    static std::string ParseSigningAlgorithmFromUri(const std::string& uri);
    static Result<std::string> ExtractHost(const std::string& uri);

 private:
    DlfAuthProvider(std::unique_ptr<DlfTokenLoader> token_loader,
                    const std::optional<DlfToken>& token, const std::string& host,
                    std::unique_ptr<DlfRequestSigner> signer, Clock clock);

    Result<DlfToken> GetFreshToken(std::chrono::system_clock::time_point now) const;

    std::unique_ptr<DlfTokenLoader> token_loader_;
    mutable std::optional<DlfToken> token_;
    std::string host_;
    std::unique_ptr<DlfRequestSigner> signer_;
    Clock clock_;
    mutable std::mutex token_mutex_;
};

}  // namespace paimon
