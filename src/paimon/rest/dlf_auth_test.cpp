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

#include "paimon/rest/dlf_auth.h"

#include <atomic>
#include <chrono>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/catalog_options.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

using StringMap = std::map<std::string, std::string>;

std::chrono::system_clock::time_point FixedTime() {
    return std::chrono::system_clock::from_time_t(1744775086);
}

class SequenceTokenLoader : public DlfTokenLoader {
 public:
    explicit SequenceTokenLoader(
        std::vector<DlfToken> tokens,
        std::chrono::milliseconds load_delay = std::chrono::milliseconds(0))
        : tokens_(std::move(tokens)), load_delay_(load_delay) {}

    Result<DlfToken> LoadToken() override {
        std::this_thread::sleep_for(load_delay_);
        int32_t index = load_count_.fetch_add(1);
        if (index >= static_cast<int32_t>(tokens_.size())) {
            return Status::Invalid("test token loader exhausted");
        }
        return tokens_[index];
    }

    std::string Description() const override {
        return "test sequence";
    }

    int32_t GetLoadCount() const {
        return load_count_.load();
    }

 private:
    std::vector<DlfToken> tokens_;
    std::chrono::milliseconds load_delay_;
    std::atomic<int32_t> load_count_{0};
};

class MockEcsHttpClient : public HttpClient {
 public:
    explicit MockEcsHttpClient(const std::string& metadata_url, bool direct_token = false)
        : metadata_url_(metadata_url), direct_token_(direct_token) {}

    Result<HttpResponse> Execute(const HttpRequest& request,
                                 const HttpBodyConsumer& consumer) const override {
        last_request_timeout_ms_.store(request.request_timeout_ms);
        HttpResponse response;
        response.status_code = 200;
        std::string body;
        if (request.url == metadata_url_) {
            if (direct_token_) {
                token_requests_.fetch_add(1);
                body = R"({"AccessKeyId":"ecs-ak","AccessKeySecret":"ecs-sk",)"
                       R"("SecurityToken":"ecs-sts","Expiration":"2027-04-16T05:44:46Z"})";
            } else {
                role_requests_.fetch_add(1);
                body = " test-role\n";
            }
        } else if (request.url == metadata_url_ + "test-role") {
            token_requests_.fetch_add(1);
            body = R"({"AccessKeyId":"ecs-ak","AccessKeySecret":"ecs-sk",)"
                   R"("SecurityToken":"ecs-sts","Expiration":"2027-04-16T05:44:46Z"})";
        } else {
            response.status_code = 404;
        }
        if (!body.empty()) {
            PAIMON_RETURN_NOT_OK(consumer(body.data(), static_cast<int64_t>(body.size())));
            response.body_size = static_cast<int64_t>(body.size());
        }
        return response;
    }

    int32_t GetRoleRequestCount() const {
        return role_requests_.load();
    }

    int32_t GetTokenRequestCount() const {
        return token_requests_.load();
    }

    int64_t GetLastRequestTimeoutMillis() const {
        return last_request_timeout_ms_.load();
    }

 private:
    std::string metadata_url_;
    bool direct_token_;
    mutable std::atomic<int32_t> role_requests_{0};
    mutable std::atomic<int32_t> token_requests_{0};
    mutable std::atomic<int64_t> last_request_timeout_ms_{-1};
};

class FailingEcsHttpClient : public HttpClient {
 public:
    Result<HttpResponse> Execute(const HttpRequest&, const HttpBodyConsumer&) const override {
        return Status::IOError("connection refused");
    }
};

}  // namespace

TEST(DlfDefaultSignerTest, SignsJavaCompatibleRequest) {
    DlfDefaultSigner signer("cn-beijing");
    const std::string body = R"({"name":"t1"})";
    DlfToken token("YourAccessKeyId", "YourAccessKeySecret", "securityToken");
    RestAuthParameter parameter =
        RestAuthParameter::Create("POST", "/v1/wh/databases/db/tables",
                                  {{"warehouse", "my instance"}, {"branch", "main"}}, body);

    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders(body, FixedTime(), token.GetSecurityToken(), "unused"));
    ASSERT_EQ("20250416T034446Z", headers.at("x-dlf-date"));
    ASSERT_EQ("Od9T1x3c2+JusJPFMpXe9Q==", headers.at("Content-MD5"));
    ASSERT_EQ("application/json", headers.at("Content-Type"));
    ASSERT_EQ("UNSIGNED-PAYLOAD", headers.at("x-dlf-content-sha256"));
    ASSERT_EQ("v1", headers.at("x-dlf-version"));
    ASSERT_EQ("securityToken", headers.at("x-dlf-security-token"));

    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, "unused", headers));
    ASSERT_EQ(
        "DLF4-HMAC-SHA256 Credential=YourAccessKeyId/20250416/cn-beijing/"
        "DlfNext/aliyun_v4_request,Signature="
        "22594f8bbb8bb0ec296ced6003b7ffdf7022a8ca3815da5b53090daa11a06558",
        authorization);
}

TEST(DlfDefaultSignerTest, MatchesJavaGoldenAuthorization) {
    // Mirrors Java DLFAuthSignatureTest#testGetAuthorization.
    DlfDefaultSigner signer("cn-hangzhou");
    const std::string body = R"({"name":"database","options":{"a":"b"}})";
    DlfToken token("access-key-id", "access-key-secret", "securityToken");
    RestAuthParameter parameter = RestAuthParameter::Create("POST", "/v1/paimon/databases",
                                                            {{"k1", "v1"}, {"k2", "v2"}}, body);
    const std::chrono::system_clock::time_point signing_time =
        std::chrono::system_clock::from_time_t(1701605532);

    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders(body, signing_time, token.GetSecurityToken(), "host"));
    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, "host", headers));
    ASSERT_EQ(
        "DLF4-HMAC-SHA256 Credential=access-key-id/20231203/cn-hangzhou/"
        "DlfNext/aliyun_v4_request,Signature="
        "c72caf1d40b55b1905d891ee3e3de48a2f8bebefa7e39e4f277acc93c269c5e3",
        authorization);
}

TEST(DlfDefaultSignerTest, OmitsBodyAndSecurityTokenHeadersWhenAbsent) {
    DlfDefaultSigner signer("cn-hangzhou");
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders("", FixedTime(), std::nullopt, "unused"));
    ASSERT_EQ(3, headers.size());
    ASSERT_EQ(0, headers.count("Content-MD5"));
    ASSERT_EQ(0, headers.count("Content-Type"));
    ASSERT_EQ(0, headers.count("x-dlf-security-token"));
}

TEST(DlfOpenApiSignerTest, SignsJavaCompatibleRequest) {
    DlfOpenApiSigner signer;
    const std::string body = R"({"CategoryName":"test","CategoryType":"UNSTRUCTURED"})";
    const std::string host = "dlfnext.cn-beijing.aliyuncs.com";
    DlfToken token("YourAccessKeyId", "YourAccessKeySecret", "securityToken");
    RestAuthParameter parameter =
        RestAuthParameter::Create("POST", "/llm-p2e4XXXXXXXXsvtn/datacenter/category", {}, body);

    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders(body, FixedTime(), token.GetSecurityToken(), host));
    headers["x-acs-signature-nonce"] = "ef34aae7-7bd2-413d-a541-680cd2c48538";
    ASSERT_EQ("Wed, 16 Apr 2025 03:44:46 GMT", headers.at("Date"));
    ASSERT_EQ("q2qaEcR4P47+Z7CUzHRTBw==", headers.at("Content-MD5"));
    ASSERT_EQ("application/json", headers.at("Accept"));
    ASSERT_EQ("application/json", headers.at("Content-Type"));
    ASSERT_EQ(host, headers.at("Host"));
    ASSERT_EQ("HMAC-SHA1", headers.at("x-acs-signature-method"));
    ASSERT_EQ("1.0", headers.at("x-acs-signature-version"));
    ASSERT_EQ("2026-01-18", headers.at("x-acs-version"));
    ASSERT_EQ("securityToken", headers.at("x-acs-security-token"));

    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, host, headers));
    ASSERT_EQ("acs YourAccessKeyId:wX4CDPSCtfgYkxdK9tJIO3ez5VI=", authorization);
}

TEST(DlfOpenApiSignerTest, DecodesPathAndQueryValuesBeforeSigning) {
    DlfOpenApiSigner signer;
    DlfToken token("ak", "sk");
    RestAuthParameter parameter = RestAuthParameter::Create(
        "GET", "/v1/%24snapshots", {{"z", ""}, {"name", "hello world"}}, "");
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders("", FixedTime(), std::nullopt, "host"));
    headers["x-acs-signature-nonce"] = "fixed-nonce";

    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, "host", headers));
    ASSERT_EQ("acs ak:vD+M7291KKoOTvt2gQuD6jPDlw8=", authorization);
    ASSERT_EQ(0, headers.count("Content-MD5"));
    ASSERT_EQ(0, headers.count("Content-Type"));
}

TEST(DlfTokenTest, ParsesExpirationAndRefreshBoundary) {
    ASSERT_OK_AND_ASSIGN(
        DlfToken token,
        DlfToken::FromJson(R"({"AccessKeyId":"ak","AccessKeySecret":"sk","SecurityToken":"sts",)"
                           R"("Expiration":"2025-04-16T05:44:46Z","Ignored":"value"})"));
    ASSERT_EQ("ak", token.GetAccessKeyId());
    ASSERT_EQ("sk", token.GetAccessKeySecret());
    ASSERT_EQ(std::optional<std::string>("sts"), token.GetSecurityToken());
    ASSERT_EQ(std::optional<int64_t>(1744782286000), token.GetExpirationAtMillis());
    ASSERT_FALSE(token.ShouldRefresh(FixedTime() + std::chrono::hours(1)));
    ASSERT_TRUE(
        token.ShouldRefresh(FixedTime() + std::chrono::hours(1) + std::chrono::milliseconds(1)));

    DlfToken permanent("ak", "sk");
    ASSERT_FALSE(permanent.ShouldRefresh(FixedTime() + std::chrono::hours(100000)));
}

TEST(DlfTokenTest, ParseFailureDoesNotLeakCredentials) {
    const std::string secret = "STSSECRET_AKID_9999";
    Status status =
        DlfToken::FromJson(R"({"AccessKeyId":"ak","AccessKeySecret":")" + secret).status();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(std::string::npos, status.ToString().find(secret));
}

TEST(DlfLocalFileTokenLoaderTest, LoadsTokenAndRedactsMalformedContent) {
    std::unique_ptr<UniqueTestDirectory> test_dir = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, test_dir);
    const std::string path = test_dir->Str() + "/dlf-token.json";
    {
        std::ofstream file(path);
        file << R"({"AccessKeyId":"file-ak","AccessKeySecret":"file-sk",)"
                R"("SecurityToken":"file-sts","Expiration":"2027-04-16T05:44:46Z"})";
    }
    DlfLocalFileTokenLoader loader(path, 1, std::chrono::milliseconds(0));
    ASSERT_OK_AND_ASSIGN(DlfToken token, loader.LoadToken());
    ASSERT_EQ("file-ak", token.GetAccessKeyId());
    ASSERT_EQ(std::optional<std::string>("file-sts"), token.GetSecurityToken());

    std::map<std::string, std::string> options = {
        {CatalogOptions::URI, "https://cn-hangzhou-vpc.dlf.aliyuncs.com"},
        {CatalogOptions::TOKEN_PROVIDER, "dlf"},
        {CatalogOptions::DLF_TOKEN_PATH, path},
        {CatalogOptions::DLF_ACCESS_KEY_ID, "ignored-ak"},
        {CatalogOptions::DLF_ACCESS_KEY_SECRET, "ignored-sk"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AuthProvider> provider, AuthProvider::Create(options));
    RestAuthParameter parameter = RestAuthParameter::Create("GET", "/v1/config", {}, "");
    ASSERT_OK_AND_ASSIGN(StringMap headers, provider->MergeAuthHeader({}, parameter));
    ASSERT_NE(std::string::npos, headers.at("Authorization").find("Credential=file-ak/"));

    options[CatalogOptions::DLF_TOKEN_LOADER] = "local_file";
    ASSERT_OK_AND_ASSIGN(provider, AuthProvider::Create(options));
    ASSERT_OK(provider->MergeAuthHeader({}, parameter));

    const std::string secret = "FILE_SECRET_9999";
    {
        std::ofstream file(path);
        file << R"({"AccessKeyId":"ak","AccessKeySecret":")" << secret;
    }
    Status status = loader.LoadToken().status();
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(std::string::npos, status.ToString().find(secret));
}

TEST(DlfEcsTokenLoaderTest, DiscoversRoleOnceAndRefreshesToken) {
    const std::string metadata_url = "http://100.100.100.200/metadata/";
    auto http_client = std::make_unique<MockEcsHttpClient>(metadata_url);
    MockEcsHttpClient* http_client_ptr = http_client.get();
    DlfEcsTokenLoader loader(metadata_url, std::nullopt, std::move(http_client));

    ASSERT_OK_AND_ASSIGN(DlfToken first, loader.LoadToken());
    ASSERT_OK_AND_ASSIGN(DlfToken second, loader.LoadToken());
    ASSERT_EQ("ecs-ak", first.GetAccessKeyId());
    ASSERT_EQ("ecs-ak", second.GetAccessKeyId());
    ASSERT_EQ(1, http_client_ptr->GetRoleRequestCount());
    ASSERT_EQ(2, http_client_ptr->GetTokenRequestCount());
    ASSERT_EQ(180000, http_client_ptr->GetLastRequestTimeoutMillis());
}

TEST(DlfEcsTokenLoaderTest, ExplicitEmptyRoleUsesMetadataUrlAsTokenEndpoint) {
    const std::string metadata_url = "http://100.100.100.200/metadata/token";
    auto http_client = std::make_unique<MockEcsHttpClient>(metadata_url, /*direct_token=*/true);
    MockEcsHttpClient* http_client_ptr = http_client.get();
    DlfEcsTokenLoader loader(metadata_url, std::string(""), std::move(http_client));

    ASSERT_OK_AND_ASSIGN(DlfToken token, loader.LoadToken());
    ASSERT_EQ("ecs-ak", token.GetAccessKeyId());
    ASSERT_EQ(0, http_client_ptr->GetRoleRequestCount());
    ASSERT_EQ(1, http_client_ptr->GetTokenRequestCount());
    ASSERT_EQ(180000, http_client_ptr->GetLastRequestTimeoutMillis());
}

TEST(DlfEcsTokenLoaderTest, PreservesTransportFailureDetail) {
    DlfEcsTokenLoader loader("http://100.100.100.200/metadata/token", std::string(""),
                             std::make_unique<FailingEcsHttpClient>());
    Status status = loader.LoadToken().status();
    ASSERT_NOK_WITH_MSG(status, "failed to request DLF credentials from ECS metadata service");
    ASSERT_NOK_WITH_MSG(status, "connection refused");
}

TEST(DlfAuthProviderTest, RefreshesWithinSafeWindow) {
    std::atomic<int64_t> now_seconds{1744775086};
    std::vector<DlfToken> tokens;
    tokens.emplace_back("ak-1", "sk-1", std::nullopt, 1744782286000);
    tokens.emplace_back("ak-2", "sk-2");
    auto loader = std::make_unique<SequenceTokenLoader>(tokens);
    SequenceTokenLoader* loader_ptr = loader.get();
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DlfAuthProvider> provider,
        DlfAuthProvider::FromTokenLoader(
            std::move(loader), "https://cn-beijing-vpc.dlf.aliyuncs.com", "cn-beijing", "default",
            [&] { return std::chrono::system_clock::from_time_t(now_seconds.load()); }));
    RestAuthParameter parameter = RestAuthParameter::Create("GET", "/v1/config", {}, "");

    ASSERT_OK_AND_ASSIGN(StringMap first, provider->MergeAuthHeader({}, parameter));
    ASSERT_NE(std::string::npos, first.at("Authorization").find("Credential=ak-1/"));
    ASSERT_OK(provider->MergeAuthHeader({}, parameter));
    ASSERT_EQ(1, loader_ptr->GetLoadCount());

    now_seconds.fetch_add(3601);
    ASSERT_OK_AND_ASSIGN(StringMap refreshed, provider->MergeAuthHeader({}, parameter));
    ASSERT_NE(std::string::npos, refreshed.at("Authorization").find("Credential=ak-2/"));
    ASSERT_EQ(2, loader_ptr->GetLoadCount());
}

TEST(DlfAuthProviderTest, ConcurrentFirstUseLoadsTokenOnce) {
    auto loader = std::make_unique<SequenceTokenLoader>(
        std::vector<DlfToken>{DlfToken("concurrent-ak", "concurrent-sk")},
        std::chrono::milliseconds(10));
    SequenceTokenLoader* loader_ptr = loader.get();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<DlfAuthProvider> provider,
                         DlfAuthProvider::FromTokenLoader(std::move(loader),
                                                          "https://cn-beijing-vpc.dlf.aliyuncs.com",
                                                          "cn-beijing", "default", FixedTime));
    RestAuthParameter parameter = RestAuthParameter::Create("GET", "/v1/config", {}, "");
    std::vector<std::thread> threads;
    std::vector<Status> statuses;
    std::mutex statuses_mutex;
    for (int32_t i = 0; i < 16; ++i) {
        threads.emplace_back([&] {
            Status status = provider->MergeAuthHeader({}, parameter).status();
            std::scoped_lock lock(statuses_mutex);
            statuses.push_back(status);
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    ASSERT_EQ(16, statuses.size());
    for (const Status& status : statuses) {
        ASSERT_OK(status);
    }
    ASSERT_EQ(1, loader_ptr->GetLoadCount());
}

TEST(DlfAuthProviderTest, SelectsEndpointSignerAndCredentialSource) {
    std::map<std::string, std::string> options = {
        {CatalogOptions::URI, "https://dlfnext.cn-hangzhou.aliyuncs.com"},
        {CatalogOptions::TOKEN_PROVIDER, "dlf"},
        {CatalogOptions::DLF_ACCESS_KEY_ID, "ak"},
        {CatalogOptions::DLF_ACCESS_KEY_SECRET, "sk"},
        {CatalogOptions::DLF_SECURITY_TOKEN, "sts"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AuthProvider> provider, AuthProvider::Create(options));
    RestAuthParameter parameter = RestAuthParameter::Create("GET", "/v1/config", {}, "");
    ASSERT_OK_AND_ASSIGN(StringMap headers, provider->MergeAuthHeader({{"Authorization", "old"},
                                                                       {"x-acs-version", "old"},
                                                                       {"custom-header", "kept"}},
                                                                      parameter));
    ASSERT_NE(std::string::npos, headers.at("Authorization").find("acs ak:"));
    ASSERT_EQ("2026-01-18", headers.at("x-acs-version"));
    ASSERT_EQ("sts", headers.at("x-acs-security-token"));
    ASSERT_EQ("kept", headers.at("custom-header"));
    ASSERT_FALSE(provider->AllowsRedirects());

    BearTokenAuthProvider bear_provider("token");
    ASSERT_TRUE(bear_provider.AllowsRedirects());

    ASSERT_EQ("openapi", DlfAuthProvider::ParseSigningAlgorithmFromUri(options.at("uri")));
    ASSERT_EQ("default", DlfAuthProvider::ParseSigningAlgorithmFromUri(
                             "https://cn-hangzhou-vpc.dlf.aliyuncs.com"));
    ASSERT_OK_AND_ASSIGN(std::string region,
                         DlfAuthProvider::ParseRegionFromUri(options.at("uri")));
    ASSERT_EQ("cn-hangzhou", region);
    ASSERT_OK_AND_ASSIGN(std::string host,
                         DlfAuthProvider::ExtractHost("https://example.com:8443/prefix"));
    ASSERT_EQ("example.com:8443", host);
}

TEST(DlfAuthProviderTest, NormalizesSigningHostLikeTransport) {
    std::map<std::string, std::string> options = {
        {CatalogOptions::URI, " https://dlfnext.cn-hangzhou.aliyuncs.com "},
        {CatalogOptions::TOKEN_PROVIDER, "dlf"},
        {CatalogOptions::DLF_ACCESS_KEY_ID, "ak"},
        {CatalogOptions::DLF_ACCESS_KEY_SECRET, "sk"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AuthProvider> provider, AuthProvider::Create(options));
    RestAuthParameter parameter = RestAuthParameter::Create("GET", "/v1/config", {}, "");
    ASSERT_OK_AND_ASSIGN(StringMap headers, provider->MergeAuthHeader({}, parameter));
    ASSERT_EQ("dlfnext.cn-hangzhou.aliyuncs.com", headers.at("Host"));
}

TEST(DlfAuthProviderTest, RejectsIncompleteOrUnknownConfiguration) {
    const std::map<std::string, std::string> base = {
        {CatalogOptions::URI, "https://dlfnext.cn-hangzhou.aliyuncs.com"},
        {CatalogOptions::TOKEN_PROVIDER, "dlf"}};
    ASSERT_NOK_WITH_MSG(AuthProvider::Create(base).status(), "token path or access key");

    std::map<std::string, std::string> options = base;
    options[CatalogOptions::DLF_TOKEN_LOADER] = "ecs";
    options[CatalogOptions::DLF_TOKEN_ECS_METADATA_URL] = "http://metadata/";
    options[CatalogOptions::DLF_TOKEN_ECS_ROLE_NAME] = "role";
    ASSERT_OK(AuthProvider::Create(options));

    options = base;
    options[CatalogOptions::DLF_TOKEN_LOADER] = "unknown";
    ASSERT_NOK_WITH_MSG(AuthProvider::Create(options).status(), "unsupported DLF token loader");

    options = base;
    options[CatalogOptions::DLF_ACCESS_KEY_ID] = "ak";
    options[CatalogOptions::DLF_ACCESS_KEY_SECRET] = "sk";
    options[CatalogOptions::DLF_SIGNING_ALGORITHM] = "unknown";
    ASSERT_NOK_WITH_MSG(AuthProvider::Create(options).status(),
                        "unsupported DLF signing algorithm");

    options[CatalogOptions::DLF_SIGNING_ALGORITHM] = "default";
    options[CatalogOptions::URI] = "http://127.0.0.1:8080";
    ASSERT_NOK_WITH_MSG(AuthProvider::Create(options).status(), "DLF region");
}

}  // namespace paimon::test
