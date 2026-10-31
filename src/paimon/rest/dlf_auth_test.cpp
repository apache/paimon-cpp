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
#include <cstdint>
#include <ctime>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/catalog_options.h"
#include "paimon/rest/resource_paths.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::test {

namespace {

using StringMap = std::map<std::string, std::string>;

std::chrono::system_clock::time_point FixedTime() {
    return std::chrono::system_clock::from_time_t(1744775086);
}

constexpr const char kEmptyBodySha256[] =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

void AssertAction(const std::optional<std::string>& expected, const std::string& method,
                  const std::string& path) {
    ASSERT_EQ(expected, DlfOpenApiActions::Resolve(method, path)) << method << " " << path;
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
    DlfToken token("YourAccessKeyId", "YourAccessKeySecret", "securityToken", std::nullopt);
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
                         signer.Authorization(parameter, token, "unused", &headers));
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
    DlfToken token("access-key-id", "access-key-secret", "securityToken", std::nullopt);
    RestAuthParameter parameter = RestAuthParameter::Create("POST", "/v1/paimon/databases",
                                                            {{"k1", "v1"}, {"k2", "v2"}}, body);
    const std::chrono::system_clock::time_point signing_time =
        std::chrono::system_clock::from_time_t(1701605532);

    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders(body, signing_time, token.GetSecurityToken(), "host"));
    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, "host", &headers));
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
    DlfToken token("YourAccessKeyId", "YourAccessKeySecret", "securityToken", std::nullopt);
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
                         signer.Authorization(parameter, token, host, &headers));
    ASSERT_EQ("acs YourAccessKeyId:wX4CDPSCtfgYkxdK9tJIO3ez5VI=", authorization);
}

TEST(DlfOpenApiSignerTest, DecodesPathAndQueryValuesBeforeSigning) {
    DlfOpenApiSigner signer;
    DlfToken token("ak", "sk", std::nullopt, std::nullopt);
    RestAuthParameter parameter = RestAuthParameter::Create(
        "GET", "/v1/%24snapshots", {{"z", ""}, {"name", "hello world"}}, "");
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders("", FixedTime(), std::nullopt, "host"));
    headers["x-acs-signature-nonce"] = "fixed-nonce";

    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, "host", &headers));
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

    DlfToken permanent("ak", "sk", std::nullopt, std::nullopt);
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
    tokens.emplace_back("ak-2", "sk-2", std::nullopt, std::nullopt);
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
        std::vector<DlfToken>{
            DlfToken("concurrent-ak", "concurrent-sk", std::nullopt, std::nullopt)},
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

TEST(DlfDefaultSignerTest, SignsSpecialCharQueryLikeJava) {
    // Golden produced by the Java DLFDefaultSigner for identical inputs; it locks the
    // form-encoding of query values (space->'+', '+'->%2B, '='->%3D, '&'->%26, '%'->%25)
    // that the default signer signs over.
    DlfDefaultSigner signer("cn-hangzhou");
    const std::string body = R"({"name":"database"})";
    DlfToken token("access-key-id", "access-key-secret", "securityToken", std::nullopt);
    RestAuthParameter parameter = RestAuthParameter::Create(
        "POST", "/v1/paimon/databases",
        {{"k1", "a b"}, {"k2", "x+y"}, {"k3", "p=q&r"}, {"k4", "100%"}}, body);
    const std::chrono::system_clock::time_point signing_time =
        std::chrono::system_clock::from_time_t(1701605532);  // 2023-12-03T12:12:12Z

    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders(body, signing_time, token.GetSecurityToken(), "host"));
    ASSERT_EQ("20231203T121212Z", headers.at("x-dlf-date"));
    ASSERT_EQ("F2vHiexkcYvt4XOY5o4tmQ==", headers.at("Content-MD5"));
    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, "host", &headers));
    ASSERT_EQ(
        "DLF4-HMAC-SHA256 Credential=access-key-id/20231203/cn-hangzhou/"
        "DlfNext/aliyun_v4_request,Signature="
        "3054a64554697227800ff06197e50b2c3de958f6b5584e3b531957207e685b4b",
        authorization);
}

TEST(DlfOpenApiSignerTest, OmitsBodyAndSecurityTokenHeadersWhenAbsent) {
    DlfOpenApiSigner signer;
    ASSERT_OK_AND_ASSIGN(
        DlfRequestSigner::Headers headers,
        signer.SignHeaders("", FixedTime(), std::nullopt, "dlfnext.cn-beijing.aliyuncs.com"));
    ASSERT_EQ("Wed, 16 Apr 2025 03:44:46 GMT", headers.at("Date"));
    ASSERT_EQ("application/json", headers.at("Accept"));
    ASSERT_EQ("dlfnext.cn-beijing.aliyuncs.com", headers.at("Host"));
    ASSERT_EQ("HMAC-SHA1", headers.at("x-acs-signature-method"));
    ASSERT_EQ("1.0", headers.at("x-acs-signature-version"));
    ASSERT_EQ("2026-01-18", headers.at("x-acs-version"));
    ASSERT_FALSE(headers.at("x-acs-signature-nonce").empty());
    ASSERT_EQ(0, headers.count("Content-MD5"));
    ASSERT_EQ(0, headers.count("Content-Type"));
    ASSERT_EQ(0, headers.count("x-acs-security-token"));
    ASSERT_EQ(7, headers.size());
}

TEST(DlfOpenApiSignerTest, FormatsGmtDateAcrossWeekdayAndMonthBounds) {
    // Guards the fixed weekday/month tables used for the RFC 1123 GMT Date header.
    const std::vector<std::pair<time_t, std::string>> cases = {
        {1736060889, "Sun, 05 Jan 2025 07:08:09 GMT"},  // single-digit day, weekday index 0
        {1709251199, "Thu, 29 Feb 2024 23:59:59 GMT"},  // leap day
        {1672444800, "Sat, 31 Dec 2022 00:00:00 GMT"},  // month index 11, weekday index 6
        {1635768000, "Mon, 01 Nov 2021 12:00:00 GMT"},  // month index 10, weekday index 1
    };
    DlfOpenApiSigner signer;
    for (const auto& [epoch, expected] : cases) {
        ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                             signer.SignHeaders("", std::chrono::system_clock::from_time_t(epoch),
                                                std::nullopt, "dlfnext.cn-hangzhou.aliyuncs.com"));
        ASSERT_EQ(expected, headers.at("Date"));
    }
}

TEST(DlfOpenApiSignerTest, GeneratesUniqueUuidShapedNonce) {
    DlfOpenApiSigner signer;
    static const std::regex kUuidPattern(
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    static const std::regex kTimestampPattern("[0-9]{10,}");
    std::set<std::string> nonces;
    for (int32_t i = 0; i < 500; ++i) {
        ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                             signer.SignHeaders("", FixedTime(), std::nullopt, "host"));
        const std::string nonce = headers.at("x-acs-signature-nonce");
        ASSERT_TRUE(std::regex_search(nonce, kUuidPattern));
        ASSERT_TRUE(std::regex_search(nonce, kTimestampPattern));
        nonces.insert(nonce);
    }
    ASSERT_EQ(500, nonces.size());
}

TEST(DlfOpenApiSignerTest, ConcurrentNonceGenerationStaysUnique) {
    DlfOpenApiSigner signer;
    std::set<std::string> nonces;
    std::vector<Status> statuses;
    std::mutex mutex;
    std::vector<std::thread> threads;
    for (int32_t t = 0; t < 8; ++t) {
        threads.emplace_back([&] {
            for (int32_t i = 0; i < 50; ++i) {
                Result<DlfRequestSigner::Headers> headers =
                    signer.SignHeaders("", FixedTime(), std::nullopt, "host");
                std::scoped_lock lock(mutex);
                statuses.push_back(headers.ok() ? Status::OK() : headers.status());
                if (headers.ok()) {
                    nonces.insert(headers.value().at("x-acs-signature-nonce"));
                }
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    ASSERT_EQ(400, statuses.size());
    for (const Status& status : statuses) {
        ASSERT_OK(status);
    }
    ASSERT_EQ(400, nonces.size());
}

TEST(DlfSignerTest, ExposesJavaCompatibleIdentifiers) {
    ASSERT_EQ(std::string("default"), std::string(DlfDefaultSigner::kIdentifier));
    ASSERT_EQ(std::string("openapi"), std::string(DlfOpenApiSigner::kIdentifier));
    ASSERT_EQ(std::string("openapi-v4"), std::string(DlfOpenApiV4Signer::kIdentifier));
}

TEST(DlfAuthProviderTest, ParsesSigningAlgorithmFromUriBoundaries) {
    ASSERT_EQ("openapi",
              DlfAuthProvider::ParseSigningAlgorithmFromUri("dlfnext.cn-hangzhou.aliyuncs.com"));
    ASSERT_EQ("openapi", DlfAuthProvider::ParseSigningAlgorithmFromUri(
                             "dlfnext-vpc.cn-hangzhou.aliyuncs.com"));
    ASSERT_EQ("openapi", DlfAuthProvider::ParseSigningAlgorithmFromUri(
                             "https://dlfnext.cn-hangzhou.aliyuncs.com"));
    ASSERT_EQ("default",
              DlfAuthProvider::ParseSigningAlgorithmFromUri("cn-hangzhou-vpc.dlf.aliyuncs.com"));
    ASSERT_EQ("default", DlfAuthProvider::ParseSigningAlgorithmFromUri(
                             "cn-hangzhou-intranet.dlf.aliyuncs.com"));
    ASSERT_EQ("default", DlfAuthProvider::ParseSigningAlgorithmFromUri(
                             "https://cn-hangzhou-vpc.dlf.aliyuncs.com"));
    ASSERT_EQ("default", DlfAuthProvider::ParseSigningAlgorithmFromUri("unknown.example.com"));
    ASSERT_EQ("default", DlfAuthProvider::ParseSigningAlgorithmFromUri("127.0.0.1"));
    ASSERT_EQ("default", DlfAuthProvider::ParseSigningAlgorithmFromUri("http://127.0.0.1:8080"));
    ASSERT_EQ("default", DlfAuthProvider::ParseSigningAlgorithmFromUri(""));
}

TEST(DlfAuthProviderTest, ParsesRegionFromUriVariants) {
    const std::vector<std::string> hangzhou_uris = {
        "https://cn-hangzhou-vpc.dlf.aliyuncs.com", "https://cn-hangzhou-intranet.dlf.aliyuncs.com",
        "https://cn-hangzhou.dlf.aliyuncs.com", "https://pre-cn-hangzhou-vpc.dlf.aliyuncs.com"};
    for (const std::string& uri : hangzhou_uris) {
        ASSERT_OK_AND_ASSIGN(std::string region, DlfAuthProvider::ParseRegionFromUri(uri));
        ASSERT_EQ("cn-hangzhou", region);
    }
    const std::vector<std::string> us_east_uris = {"https://us-east-1-vpc.dlf.aliyuncs.com",
                                                   "https://us-east-1-intranet.dlf.aliyuncs.com"};
    for (const std::string& uri : us_east_uris) {
        ASSERT_OK_AND_ASSIGN(std::string region, DlfAuthProvider::ParseRegionFromUri(uri));
        ASSERT_EQ("us-east-1", region);
    }
    ASSERT_NOK_WITH_MSG(DlfAuthProvider::ParseRegionFromUri("http://127.0.0.1:8080").status(),
                        "could not determine DLF region");
}

TEST(DlfOpenApiV4SignerTest, SignsHeadersWithBody) {
    DlfOpenApiV4Signer signer("cn-beijing");
    const std::string body = R"({"CategoryName":"test","CategoryType":"UNSTRUCTURED"})";
    const std::string host = "dlfnext.cn-beijing.aliyuncs.com";
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders(body, FixedTime(), std::nullopt, host));
    ASSERT_EQ("2025-04-16T03:44:46Z", headers.at("x-acs-date"));
    ASSERT_EQ(host, headers.at("Host"));
    ASSERT_EQ("d8171130c3edc0f7d4e2827c5dab801a603c9a376b214a9ab6d5d12665c8c5df",
              headers.at("x-acs-content-sha256"));
    ASSERT_EQ("application/json", headers.at("Content-Type"));
    ASSERT_FALSE(headers.at("x-acs-signature-nonce").empty());
    ASSERT_EQ("2026-01-18", headers.at("x-acs-version"));
    ASSERT_EQ(6, headers.size());

    ASSERT_NOK_WITH_MSG(signer.SignHeaders(body, FixedTime(), std::nullopt, "").status(),
                        "host is empty");
}

TEST(DlfOpenApiV4SignerTest, OmitsBodyAndSecurityTokenHeadersWhenAbsent) {
    DlfOpenApiV4Signer signer("cn-beijing");
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders("", FixedTime(), std::nullopt, "host"));
    ASSERT_EQ(kEmptyBodySha256, headers.at("x-acs-content-sha256"));
    ASSERT_EQ(0, headers.count("Content-Type"));
    ASSERT_EQ(0, headers.count("x-acs-security-token"));
    ASSERT_EQ(5, headers.size());
}

TEST(DlfOpenApiV4SignerTest, SignsUtcDateInNonUtcTimezone) {
    TimezoneGuard timezone_guard("Asia/Shanghai");
    DlfOpenApiV4Signer signer("cn-beijing");
    DlfToken token("TestAKId", "TestAKSecret", std::nullopt, std::nullopt);
    RestAuthParameter parameter = RestAuthParameter::Create("GET", "/test/path", {}, "");
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders("", std::chrono::system_clock::from_time_t(1744747200),
                                            std::nullopt, "host"));
    ASSERT_EQ("2025-04-15T20:00:00Z", headers.at("x-acs-date"));

    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, "host", &headers));
    ASSERT_EQ(0, authorization.find("ACS4-HMAC-SHA256 Credential=TestAKId/20250415/cn-beijing/"));
}

TEST(DlfOpenApiV4SignerTest, MatchesJavaGoldenSignatureForEncodedQueryValues) {
    DlfOpenApiV4Signer signer("cn-hangzhou");
    DlfToken token("TestAKId", "TestAKSecret", std::nullopt, std::nullopt);
    const std::string host = "dlfnext.cn-hangzhou.aliyuncs.com";
    RestAuthParameter parameter =
        RestAuthParameter::Create("GET", "/v1/prefix/functions",
                                  {{"pageToken", "a b/c"}, {"functionNamePattern", "func%"}}, "");
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders("", FixedTime(), std::nullopt, host));
    headers["x-acs-signature-nonce"] = "fixed-nonce-for-test";

    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, host, &headers));
    ASSERT_EQ(
        "ACS4-HMAC-SHA256 Credential=TestAKId/20250416/cn-hangzhou/DlfNext/aliyun_v4_request,"
        "SignedHeaders=host;x-acs-action;x-acs-content-sha256;x-acs-date;"
        "x-acs-signature-nonce;x-acs-version,"
        "Signature=2d5fdb5201901a5cdceee97318366666a88504eeae0b1ce534dbf86889cc00fb",
        authorization);
    ASSERT_EQ("ListFunctionsGlobally", headers.at("x-acs-action"));
}

TEST(DlfOpenApiV4SignerTest, MatchesJavaGoldenSignatureWithAction) {
    DlfOpenApiV4Signer signer("cn-hangzhou");
    DlfToken token("TestAKId", "TestAKSecret", std::nullopt, std::nullopt);
    const std::string host = "dlfnext.cn-hangzhou.aliyuncs.com";
    const std::string body = R"({"identifier":{"database":"db","object":"t"}})";
    RestAuthParameter parameter =
        RestAuthParameter::Create("POST", "/v1/clg-paimon-1/databases/db/tables", {}, body);
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders(body, FixedTime(), std::nullopt, host));
    headers["x-acs-signature-nonce"] = "fixed-nonce-for-test";

    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, host, &headers));
    ASSERT_EQ("CreateTable", headers.at("x-acs-action"));
    ASSERT_EQ(
        "ACS4-HMAC-SHA256 Credential=TestAKId/20250416/cn-hangzhou/DlfNext/aliyun_v4_request,"
        "SignedHeaders=content-type;host;x-acs-action;x-acs-content-sha256;"
        "x-acs-date;x-acs-signature-nonce;x-acs-version,"
        "Signature=420206b5263536e6bc271a7a32582b4a820e23c8c58ae692b708c69f9d19e51d",
        authorization);
}

TEST(DlfOpenApiV4SignerTest, SignatureDependsOnRegion) {
    DlfToken token("TestAKId", "TestAKSecret", std::nullopt, std::nullopt);
    RestAuthParameter parameter = RestAuthParameter::Create("GET", "/test/path", {}, "");
    DlfOpenApiV4Signer beijing("cn-beijing");
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         beijing.SignHeaders("", FixedTime(), std::nullopt, "host"));
    headers["x-acs-signature-nonce"] = "fixed-nonce-for-test";
    ASSERT_OK_AND_ASSIGN(std::string beijing_authorization,
                         beijing.Authorization(parameter, token, "host", &headers));

    DlfOpenApiV4Signer hangzhou("cn-hangzhou");
    ASSERT_OK_AND_ASSIGN(std::string hangzhou_authorization,
                         hangzhou.Authorization(parameter, token, "host", &headers));
    size_t beijing_signature_start = beijing_authorization.find(",Signature=");
    size_t hangzhou_signature_start = hangzhou_authorization.find(",Signature=");
    ASSERT_NE(std::string::npos, beijing_signature_start);
    ASSERT_NE(std::string::npos, hangzhou_signature_start);
    ASSERT_NE(beijing_authorization.substr(beijing_signature_start),
              hangzhou_authorization.substr(hangzhou_signature_start));
    ASSERT_NE(std::string::npos, beijing_authorization.find("/cn-beijing/DlfNext/"));
    ASSERT_NE(std::string::npos, hangzhou_authorization.find("/cn-hangzhou/DlfNext/"));
}

TEST(DlfOpenApiV4SignerTest, SignsHeaderNamesInLowerCaseAndValuesTrimmed) {
    DlfOpenApiV4Signer signer("cn-beijing");
    DlfToken token("TestAKId", "TestAKSecret", std::nullopt, std::nullopt);
    RestAuthParameter parameter = RestAuthParameter::Create("GET", "/test/path", {}, "");
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders("", FixedTime(), std::nullopt, "host"));
    headers["x-acs-signature-nonce"] = "fixed-nonce-for-test";
    DlfRequestSigner::Headers renamed_headers = headers;
    renamed_headers.erase("x-acs-signature-nonce");
    renamed_headers["X-ACS-SIGNATURE-NONCE"] = " fixed-nonce-for-test ";

    ASSERT_OK_AND_ASSIGN(std::string expected,
                         signer.Authorization(parameter, token, "host", &headers));
    ASSERT_OK_AND_ASSIGN(std::string actual,
                         signer.Authorization(parameter, token, "host", &renamed_headers));
    ASSERT_EQ(expected, actual);
}

TEST(DlfOpenApiV4SignerTest, SignsUnregisteredPathWithoutAction) {
    DlfOpenApiV4Signer signer("cn-beijing");
    DlfToken token("TestAKId", "TestAKSecret", std::nullopt, std::nullopt);
    RestAuthParameter parameter =
        RestAuthParameter::Create("GET", "/v1/clg-paimon-1/databases/db/tables/t/unknown", {}, "");
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders("", FixedTime(), std::nullopt, "host"));

    ASSERT_OK_AND_ASSIGN(std::string authorization,
                         signer.Authorization(parameter, token, "host", &headers));
    ASSERT_EQ(0, headers.count("x-acs-action"));
    ASSERT_EQ(std::string::npos, authorization.find("x-acs-action"));
}

TEST(DlfOpenApiV4SignerTest, RequiresSigningDate) {
    DlfOpenApiV4Signer signer("cn-beijing");
    DlfToken token("TestAKId", "TestAKSecret", std::nullopt, std::nullopt);
    RestAuthParameter parameter = RestAuthParameter::Create("GET", "/test/path", {}, "");
    ASSERT_OK_AND_ASSIGN(DlfRequestSigner::Headers headers,
                         signer.SignHeaders("", FixedTime(), std::nullopt, "host"));
    headers["x-acs-date"] = "2025";
    ASSERT_NOK_WITH_MSG(signer.Authorization(parameter, token, "host", &headers).status(),
                        "signing date is invalid");
    headers.erase("x-acs-date");
    ASSERT_NOK_WITH_MSG(signer.Authorization(parameter, token, "host", &headers).status(),
                        "x-acs-date");
}

TEST(DlfAuthProviderTest, SignsWithOpenApiV4OnlyWhenConfigured) {
    std::map<std::string, std::string> options = {
        {CatalogOptions::URI, "https://dlfnext.cn-hangzhou.aliyuncs.com"},
        {CatalogOptions::TOKEN_PROVIDER, "dlf"},
        {CatalogOptions::DLF_ACCESS_KEY_ID, "ak"},
        {CatalogOptions::DLF_ACCESS_KEY_SECRET, "sk"},
        {CatalogOptions::DLF_SECURITY_TOKEN, "sts"}};
    ASSERT_EQ("openapi",
              DlfAuthProvider::ParseSigningAlgorithmFromUri(options.at(CatalogOptions::URI)));
    const std::string body = R"({"name":"db"})";
    RestAuthParameter parameter =
        RestAuthParameter::Create("POST", "/v1/clg-paimon-1/databases", {}, body);
    const StringMap base_headers = {{"Content-Type", "application/json"}};
    for (const char* algorithm : {DlfDefaultSigner::kIdentifier, DlfOpenApiSigner::kIdentifier}) {
        options[CatalogOptions::DLF_SIGNING_ALGORITHM] = algorithm;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<AuthProvider> provider, AuthProvider::Create(options));
        ASSERT_OK_AND_ASSIGN(StringMap headers, provider->MergeAuthHeader(base_headers, parameter));
        ASSERT_EQ(0, headers.count("x-acs-action")) << algorithm;
    }

    options[CatalogOptions::DLF_SIGNING_ALGORITHM] = DlfOpenApiV4Signer::kIdentifier;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AuthProvider> provider, AuthProvider::Create(options));
    ASSERT_OK_AND_ASSIGN(StringMap headers, provider->MergeAuthHeader(base_headers, parameter));
    const std::string& authorization = headers.at("Authorization");
    ASSERT_EQ(0, authorization.find("ACS4-HMAC-SHA256 Credential=ak/"));
    ASSERT_NE(std::string::npos,
              authorization.find("/cn-hangzhou/DlfNext/aliyun_v4_request,SignedHeaders="
                                 "content-type;host;x-acs-action;x-acs-content-sha256;x-acs-date;"
                                 "x-acs-security-token;x-acs-signature-nonce;x-acs-version,"));
    ASSERT_EQ("CreateDatabase", headers.at("x-acs-action"));
    ASSERT_EQ("sts", headers.at("x-acs-security-token"));
    ASSERT_EQ("application/json", headers.at("Content-Type"));
    ASSERT_EQ(0, headers.count("content-type"));
    ASSERT_EQ("dlfnext.cn-hangzhou.aliyuncs.com", headers.at("Host"));
    ASSERT_EQ(0, headers.count("host"));
}

TEST(DlfOpenApiActionsTest, EveryOperationResolvesToItself) {
    static const std::regex kVariable("\\{[^}]+\\}");
    std::set<std::string> actions;
    for (const DlfOpenApiActions::Operation& operation : DlfOpenApiActions::Operations()) {
        AssertAction(operation.action, operation.method,
                     std::regex_replace(operation.path_template, kVariable, "x1"));
        actions.insert(operation.action);
    }
    ASSERT_EQ(DlfOpenApiActions::Operations().size(), actions.size());
    ASSERT_EQ(74, actions.size());
}

TEST(DlfOpenApiActionsTest, ClientPathsResolveToRegisteredActions) {
    ResourcePaths paths("clg-paimon-1");
    AssertAction("GetConfig", "GET", ResourcePaths::Config());
    AssertAction("ListDatabases", "GET", paths.Databases());
    AssertAction("CreateDatabase", "POST", paths.Databases());
    AssertAction("GetDatabase", "GET", paths.Database("db"));
    AssertAction("DropDatabase", "DELETE", paths.Database("db"));
    AssertAction("ListTables", "GET", paths.Tables("db"));
    AssertAction("CreateTable", "POST", paths.Tables("db"));
    AssertAction("GetTable", "GET", paths.Table("db", "t"));
    AssertAction("DropTable", "DELETE", paths.Table("db", "t"));
    AssertAction("RenameTable", "POST", paths.RenameTable());
    AssertAction("ListSnapshots", "GET", paths.Snapshots("db", "t"));
    AssertAction("CommitTable", "POST", paths.CommitTable("db", "t"));
    AssertAction("GetTableSnapshot", "GET", paths.TableSnapshot("db", "t"));
    AssertAction("GetTableToken", "GET", paths.TableToken("db", "t"));
}

TEST(DlfOpenApiActionsTest, MatchesWholeSegmentsAndMethodIgnoringCase) {
    ResourcePaths paths("clg-paimon-1");
    AssertAction("GetConfig", "get", ResourcePaths::Config());
    AssertAction("GetDatabase", "GET", paths.Database("tables"));
    AssertAction("GetTable", "GET", paths.Table("db", "token"));
    AssertAction("GetTableToken", "GET", paths.TableToken("config", "t$snapshots"));
    AssertAction("GetTable", "GET", paths.Table("a/b", "c d"));
    AssertAction("ListDatabases", "GET", ResourcePaths("rename").Databases());
    AssertAction("DropBranch", "DELETE", "/v1/clg-paimon-1/databases/db/tables/t/branches/forward");
    AssertAction("GetTag", "GET", "/v1/clg-paimon-1/databases/db/tables/t/tags/tags");
}

TEST(DlfOpenApiActionsTest, UnregisteredRequestsHaveNoAction) {
    ResourcePaths paths("clg-paimon-1");
    AssertAction(std::nullopt, "GET", paths.RenameTable());
    AssertAction(std::nullopt, "DELETE", "/v1/clg-paimon-1/tables/id/tbl-1");
    AssertAction(std::nullopt, "GET", paths.CommitTable("db", "t"));
    AssertAction(std::nullopt, "PUT", paths.Table("db", "t"));
    AssertAction(std::nullopt, "GET", ResourcePaths::Config() + "/");
    AssertAction(std::nullopt, "GET", "/v1//databases");
    AssertAction(std::nullopt, "GET", ResourcePaths("").Databases());
    AssertAction(std::nullopt, "", ResourcePaths::Config());
    AssertAction(std::nullopt, "GET", "");
}

}  // namespace paimon::test
