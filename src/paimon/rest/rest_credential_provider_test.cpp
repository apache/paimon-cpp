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

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/catalog_options.h"
#include "paimon/fs/credential_provider.h"
#include "paimon/rest/mock_rest_server.h"
#include "paimon/rest/rest_api.h"
#include "paimon/rest/rest_messages.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

constexpr const char kToken[] = "test-token";
constexpr const char kOssEndpointOption[] = "fs.oss.endpoint";

using Credentials = std::map<std::string, std::string>;

// The credentials the mock server hands out, plus the number of times it was asked for
// them.
struct MockTokenState {
    Credentials token = {{"fs.oss.accessKeyId", "ak-1"}};
    int64_t expires_at_millis = 0;
    // when set, the token endpoint fails with this http code
    std::optional<int32_t> force_error_code;
    // guards the fields above: the handler runs on the server's accept thread while tests
    // seed and inspect the state
    std::mutex mutex;
    std::atomic<int32_t> request_count{0};
};

MockRestServer::Response HandleTokenRequest(MockTokenState* state,
                                            const MockRestServer::Request& request) {
    MockRestServer::Response response;
    if (request.path != "/v1/databases/db1/tables/t1/token") {
        ErrorResponse error("", "", "unknown path " + request.path, 404);
        response.code = 404;
        response.body = error.ToJsonString().value();
        return response;
    }
    state->request_count++;

    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->force_error_code) {
        ErrorResponse error(ErrorResponse::kResourceTypeTable, "t1", "no permission",
                            state->force_error_code.value());
        response.code = state->force_error_code.value();
        response.body = error.ToJsonString().value();
        return response;
    }
    GetTableTokenResponse token(state->token, state->expires_at_millis);
    response.body = token.ToJsonString().value();
    return response;
}

}  // namespace

class RestCredentialProviderTest : public ::testing::Test {
 protected:
    void SetUp() override {
        state_ = std::make_shared<MockTokenState>();
        // credentials that are valid well beyond the safe time, so nothing refreshes
        // unless a test moves the clock
        state_->expires_at_millis = kExpiresAtMillis;
        now_millis_ = kNowMillis;
        ASSERT_OK_AND_ASSIGN(
            server_, MockRestServer::Start([state = state_](const MockRestServer::Request& req) {
                return HandleTokenRequest(state.get(), req);
            }));

        catalog_options_ = {
            {CatalogOptions::URI, server_->GetBaseUri()},
            {CatalogOptions::TOKEN_PROVIDER, "bear"},
            {CatalogOptions::TOKEN, kToken},
        };
    }

    void TearDown() override {
        if (server_) {
            server_->Stop();
        }
    }

    std::shared_ptr<RestCredentialProvider> CreateProvider() {
        Result<std::unique_ptr<RestApi>> api =
            RestApi::Create(catalog_options_, "", /*config_required=*/false);
        if (!api.ok()) {
            return nullptr;
        }
        std::shared_ptr<RestApi> shared_api(std::move(api).value());
        return std::make_shared<RestCredentialProvider>(
            shared_api, Identifier("db1", "t1"), [this] {
                return std::chrono::system_clock::time_point(
                    std::chrono::milliseconds(now_millis_.load()));
            });
    }

    // Epoch millis the injected clock starts at; an arbitrary point far enough from 0
    // that subtracting the safe time stays positive.
    static constexpr int64_t kNowMillis = 1700000000000;
    // Expiration the mock server reports, far beyond the safe time of `kNowMillis`.
    static constexpr int64_t kExpiresAtMillis =
        kNowMillis + 10 * RestApi::kTokenExpirationSafeTimeMillis;

    std::shared_ptr<MockTokenState> state_;
    std::unique_ptr<MockRestServer> server_;
    std::map<std::string, std::string> catalog_options_;
    std::atomic<int64_t> now_millis_{kNowMillis};
};

TEST_F(RestCredentialProviderTest, LoadsCredentialsOnceAndReusesThemWithinTheSafeTime) {
    std::shared_ptr<RestCredentialProvider> provider = CreateProvider();
    ASSERT_NE(nullptr, provider);

    // the provider is seen through the interface a caller that brings its own file system
    // holds, so the credentials are pulled from it rather than from a delegated file system
    std::shared_ptr<CredentialProvider> credential_provider = provider;
    ASSERT_OK_AND_ASSIGN(Credentials credentials, credential_provider->GetCredentials());
    ASSERT_EQ("ak-1", credentials.at("fs.oss.accessKeyId"));
    ASSERT_EQ(1, state_->request_count.load());

    // one millisecond before the safe time the credentials are still served as they are
    now_millis_ = kExpiresAtMillis - RestApi::kTokenExpirationSafeTimeMillis - 1;
    ASSERT_OK_AND_ASSIGN(Credentials reused, credential_provider->GetCredentials());
    ASSERT_EQ("ak-1", reused.at("fs.oss.accessKeyId"));
    ASSERT_EQ(1, state_->request_count.load());
}

TEST_F(RestCredentialProviderTest, RefreshesWithinTheSafeTime) {
    std::shared_ptr<RestCredentialProvider> provider = CreateProvider();
    ASSERT_NE(nullptr, provider);

    ASSERT_OK_AND_ASSIGN(RestToken first, provider->ValidToken());
    ASSERT_EQ("ak-1", first.token.at("fs.oss.accessKeyId"));
    ASSERT_EQ(1, state_->request_count.load());

    int64_t next_expiration = kExpiresAtMillis + RestApi::kTokenExpirationSafeTimeMillis;
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{"fs.oss.accessKeyId", "ak-2"}};
        state_->expires_at_millis = next_expiration;
    }
    now_millis_ = kExpiresAtMillis - 1;
    ASSERT_OK_AND_ASSIGN(RestToken second, provider->ValidToken());
    ASSERT_EQ("ak-2", second.token.at("fs.oss.accessKeyId"));
    ASSERT_EQ(next_expiration, second.expires_at_millis);
    ASSERT_EQ(2, state_->request_count.load());

    // the refreshed credentials are reused
    ASSERT_OK_AND_ASSIGN(Credentials credentials, provider->GetCredentials());
    ASSERT_EQ("ak-2", credentials.at("fs.oss.accessKeyId"));
    ASSERT_EQ(2, state_->request_count.load());
}

TEST_F(RestCredentialProviderTest, ExpiredTokenReloadsOnEveryCall) {
    // an expiration the server did not report makes the credentials expire immediately
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->expires_at_millis = 0;
    }
    std::shared_ptr<RestCredentialProvider> provider = CreateProvider();
    ASSERT_NE(nullptr, provider);

    ASSERT_OK(provider->ValidToken().status());
    ASSERT_OK(provider->ValidToken().status());
    ASSERT_EQ(2, state_->request_count.load());
}

TEST_F(RestCredentialProviderTest, DlfEndpointOverridesTheServerEndpointOnMerge) {
    catalog_options_[CatalogOptions::DLF_OSS_ENDPOINT] = "dlf-endpoint";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{"fs.oss.accessKeyId", "ak-1"}, {kOssEndpointOption, "server-endpoint"}};
    }
    std::shared_ptr<RestCredentialProvider> provider = CreateProvider();
    ASSERT_NE(nullptr, provider);

    // the token is the cache key and stays exactly as issued, carrying no catalog secrets and
    // not the endpoint correction, which is a merge concern
    ASSERT_OK_AND_ASSIGN(RestToken token, provider->ValidToken());
    ASSERT_EQ("ak-1", token.token.at("fs.oss.accessKeyId"));
    ASSERT_EQ("server-endpoint", token.token.at(kOssEndpointOption));
    ASSERT_EQ(kExpiresAtMillis, token.expires_at_millis);
    ASSERT_EQ(0u, token.token.count(CatalogOptions::TOKEN));
    ASSERT_EQ(2u, token.token.size());

    // merging shapes the credentials into the file system options: the endpoint the credentials
    // were issued for wins over the one the server reported
    Credentials merged = provider->MergeOptionsWithCredentials(catalog_options_, token.token);
    ASSERT_EQ("ak-1", merged.at("fs.oss.accessKeyId"));
    ASSERT_EQ("dlf-endpoint", merged.at(kOssEndpointOption));
}

TEST_F(RestCredentialProviderTest, EmptyDlfOssEndpointIsNotApplied) {
    catalog_options_[CatalogOptions::DLF_OSS_ENDPOINT] = "";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{kOssEndpointOption, "server-endpoint"}};
    }
    std::shared_ptr<RestCredentialProvider> provider = CreateProvider();
    ASSERT_NE(nullptr, provider);

    // an unset dlf endpoint leaves the endpoint the credentials carry alone through the merge
    ASSERT_OK_AND_ASSIGN(RestToken token, provider->ValidToken());
    Credentials merged = provider->MergeOptionsWithCredentials(catalog_options_, token.token);
    ASSERT_EQ("server-endpoint", merged.at(kOssEndpointOption));
}

TEST_F(RestCredentialProviderTest, IssuedCredentialsClearStaleBucketScopedCatalogVariants) {
    // The OSS backend resolves a bucket-scoped option ahead of the flat one, so a stale
    // bucket-scoped catalog value would shadow the flat credential the token just refreshed.
    std::shared_ptr<RestCredentialProvider> provider = CreateProvider();
    ASSERT_NE(nullptr, provider);

    Credentials base = {
        {"fs.oss.accessKeyId", "catalog-ak"},
        {"fs.oss.bucket.b.accessKeyId", "catalog-bucket-ak"},
        {"unrelated", "kept"},
    };
    Credentials credentials = {{"fs.oss.accessKeyId", "token-ak"}};

    Credentials merged = provider->MergeOptionsWithCredentials(base, credentials);
    ASSERT_EQ("token-ak", merged.at("fs.oss.accessKeyId"));
    ASSERT_EQ(0u, merged.count("fs.oss.bucket.b.accessKeyId"));
    ASSERT_EQ("kept", merged.at("unrelated"));
}

TEST_F(RestCredentialProviderTest, SessionTokenCredentialClearsStaleSecurityTokenAliases) {
    // A token carrying a fresh session token must clear a stale catalog security token -- the
    // OSS backend reads securityToken first, so leaving it in place would pair the refreshed
    // key pair with the old STS token and fail authentication. Both the flat and the
    // bucket-scoped stale securityToken have to go.
    std::shared_ptr<RestCredentialProvider> provider = CreateProvider();
    ASSERT_NE(nullptr, provider);

    Credentials base = {
        {"fs.oss.securityToken", "stale-sts"},
        {"fs.oss.bucket.b.securityToken", "stale-bucket-sts"},
    };
    Credentials credentials = {{"fs.oss.accessKeyId", "token-ak"},
                               {"fs.oss.accessKeySecret", "token-sk"},
                               {"fs.oss.sessionToken", "fresh-sts"}};

    Credentials merged = provider->MergeOptionsWithCredentials(base, credentials);
    ASSERT_EQ("token-ak", merged.at("fs.oss.accessKeyId"));
    ASSERT_EQ("token-sk", merged.at("fs.oss.accessKeySecret"));
    ASSERT_EQ("fresh-sts", merged.at("fs.oss.sessionToken"));
    ASSERT_EQ(0u, merged.count("fs.oss.securityToken"));
    ASSERT_EQ(0u, merged.count("fs.oss.bucket.b.securityToken"));
}

TEST_F(RestCredentialProviderTest, SecurityTokenCredentialClearsStaleSessionTokenAlias) {
    // The alias goes the other way too: a fresh securityToken clears a stale sessionToken so the
    // backend cannot fall back to it.
    std::shared_ptr<RestCredentialProvider> provider = CreateProvider();
    ASSERT_NE(nullptr, provider);

    Credentials base = {{"fs.oss.sessionToken", "stale-sts"}};
    Credentials credentials = {{"fs.oss.securityToken", "fresh-sts"}};

    Credentials merged = provider->MergeOptionsWithCredentials(base, credentials);
    ASSERT_EQ("fresh-sts", merged.at("fs.oss.securityToken"));
    ASSERT_EQ(0u, merged.count("fs.oss.sessionToken"));
}

TEST_F(RestCredentialProviderTest, ForbiddenIsReportedToTheCaller) {
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->force_error_code = 403;
    }
    std::shared_ptr<RestCredentialProvider> provider = CreateProvider();
    ASSERT_NE(nullptr, provider);

    Status status = provider->GetCredentials().status();
    ASSERT_NOK(status);
    ASSERT_NOK_WITH_MSG(status, "no permission");

    // a later success is not blocked by the earlier failure
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->force_error_code.reset();
    }
    ASSERT_OK_AND_ASSIGN(Credentials credentials, provider->GetCredentials());
    ASSERT_EQ("ak-1", credentials.at("fs.oss.accessKeyId"));
}

}  // namespace paimon::test
