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

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/catalog_options.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/defs.h"
#include "paimon/rest/mock_rest_server.h"
#include "paimon/rest/rest_api.h"
#include "paimon/rest/rest_messages.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

constexpr const char kToken[] = "test-token";
constexpr const char kOssEndpointOption[] = "fs.oss.endpoint";

// The credentials the mock server hands out, plus the number of times it was asked for
// them.
struct MockTokenState {
    std::map<std::string, std::string> token = {{"fs.oss.accessKeyId", "ak-1"}};
    int64_t expires_at_millis = 0;
    // when set, the token endpoint fails with this http code
    std::optional<int32_t> force_error_code;
    // guards all fields above: the handler runs on the server's accept thread while
    // tests seed and inspect the state
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

int64_t ToMillis(std::chrono::system_clock::time_point time) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(time.time_since_epoch()).count();
}

}  // namespace

class RestTokenFileSystemTest : public ::testing::Test {
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
        temp_dir_ = UniqueTestDirectory::Create("local");
        ASSERT_NE(nullptr, temp_dir_);

        catalog_options_ = {
            {CatalogOptions::URI, server_->GetBaseUri()},
            {CatalogOptions::TOKEN_PROVIDER, "bear"},
            {CatalogOptions::TOKEN, kToken},
            {Options::FILE_SYSTEM, "local"},
        };
    }

    void TearDown() override {
        if (server_) {
            server_->Stop();
        }
    }

    std::shared_ptr<RestTokenFileSystem> CreateFileSystem() {
        Result<std::unique_ptr<RestApi>> api =
            RestApi::Create(catalog_options_, "", /*config_required=*/false);
        if (!api.ok()) {
            return nullptr;
        }
        std::shared_ptr<RestApi> shared_api(std::move(api).value());
        return std::make_shared<RestTokenFileSystem>(
            shared_api, catalog_options_, Identifier("db1", "t1"), [this] {
                return std::chrono::system_clock::time_point(
                    std::chrono::milliseconds(now_millis_.load()));
            });
    }

    // Writes `content` to a file of the temp directory with the local file system and
    // returns its path.
    std::string WriteFile(const std::string& name, const std::string& content) {
        std::string path = temp_dir_->Str() + "/" + name;
        Result<std::unique_ptr<OutputStream>> out =
            temp_dir_->GetFileSystem()->Create(path, /*overwrite=*/true);
        EXPECT_OK(out.status());
        if (!out.ok()) {
            return path;
        }
        std::unique_ptr<OutputStream> stream = std::move(out).value();
        EXPECT_OK(stream->Write(content.data(), content.size()).status());
        EXPECT_OK(stream->Close());
        return path;
    }

    // Epoch millis the injected clock starts at; an arbitrary point far enough from 0
    // that subtracting the safe time stays positive.
    static constexpr int64_t kNowMillis = 1700000000000;
    // Expiration the mock server reports, far beyond the safe time of `kNowMillis`.
    static constexpr int64_t kExpiresAtMillis =
        kNowMillis + 10 * RestApi::kTokenExpirationSafeTimeMillis;

    std::shared_ptr<MockTokenState> state_;
    std::unique_ptr<MockRestServer> server_;
    std::unique_ptr<UniqueTestDirectory> temp_dir_;
    std::map<std::string, std::string> catalog_options_;
    std::atomic<int64_t> now_millis_{kNowMillis};
};

TEST_F(RestTokenFileSystemTest, DelegatesWithTheLoadedToken) {
    std::string path = WriteFile("data", "paimon");
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem();
    ASSERT_NE(nullptr, fs);

    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));
    ASSERT_EQ("paimon", content);
    ASSERT_EQ(1, state_->request_count.load());

    // the other operations reach the same delegate
    ASSERT_OK_AND_ASSIGN(bool exists, fs->Exists(path));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(FileStatus status, fs->GetFileStatus(path));
    ASSERT_EQ(6, status.GetLen());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> in, fs->Open(status));
    ASSERT_OK(in->Close());
    std::vector<BasicFileStatus> basic_file_status_list;
    ASSERT_OK(fs->ListDir(temp_dir_->Str(), &basic_file_status_list));
    ASSERT_EQ(1u, basic_file_status_list.size());
    std::vector<FileStatus> file_status_list;
    ASSERT_OK(fs->ListFileStatus(temp_dir_->Str(), &file_status_list));
    ASSERT_EQ(1u, file_status_list.size());
    ASSERT_OK(fs->Rename(path, path + ".renamed"));
    ASSERT_OK(fs->Mkdirs(temp_dir_->Str() + "/sub"));
    ASSERT_OK(fs->Delete(temp_dir_->Str() + "/sub"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> out,
                         fs->Create(temp_dir_->Str() + "/written", /*overwrite=*/true));
    ASSERT_OK(out->Close());

    // credentials that are not about to expire are loaded once
    ASSERT_EQ(1, state_->request_count.load());
}

TEST_F(RestTokenFileSystemTest, ValidTokenCarriesOnlyTheServerCredentials) {
    catalog_options_[CatalogOptions::DLF_OSS_ENDPOINT] = "dlf-endpoint";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{"fs.oss.accessKeyId", "ak-1"}, {kOssEndpointOption, "server-endpoint"}};
    }
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem();
    ASSERT_NE(nullptr, fs);

    ASSERT_OK_AND_ASSIGN(RestToken token, fs->ValidToken());
    ASSERT_EQ("ak-1", token.token.at("fs.oss.accessKeyId"));
    // the endpoint the credentials were issued for wins over the one the server reported
    ASSERT_EQ("dlf-endpoint", token.token.at(kOssEndpointOption));
    ASSERT_EQ(kExpiresAtMillis, token.expires_at_millis);
    // the catalog options are not part of the token, so its secrets stay private
    ASSERT_EQ(0u, token.token.count(CatalogOptions::TOKEN));
    ASSERT_EQ(2u, token.token.size());
}

TEST_F(RestTokenFileSystemTest, ReloadsWithinTheSafeTime) {
    std::string path = WriteFile("data", "paimon");
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem();
    ASSERT_NE(nullptr, fs);

    ASSERT_OK_AND_ASSIGN(RestToken first, fs->ValidToken());
    ASSERT_EQ("ak-1", first.token.at("fs.oss.accessKeyId"));
    ASSERT_EQ(1, state_->request_count.load());

    // one millisecond before the safe time the credentials are still used as they are
    now_millis_ = kExpiresAtMillis - RestApi::kTokenExpirationSafeTimeMillis - 1;
    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));
    ASSERT_EQ("paimon", content);
    ASSERT_EQ(1, state_->request_count.load());

    // a stream opened with the current credentials must survive their rotation, since it
    // does not own the file system it came from
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> in, fs->Open(path));

    int64_t next_expiration = kExpiresAtMillis + RestApi::kTokenExpirationSafeTimeMillis;
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{"fs.oss.accessKeyId", "ak-2"}};
        state_->expires_at_millis = next_expiration;
    }
    now_millis_ = kExpiresAtMillis - 1;
    ASSERT_OK_AND_ASSIGN(RestToken second, fs->ValidToken());
    ASSERT_EQ("ak-2", second.token.at("fs.oss.accessKeyId"));
    ASSERT_EQ(next_expiration, second.expires_at_millis);
    ASSERT_EQ(2, state_->request_count.load());

    content.assign(6, '\0');
    ASSERT_OK_AND_ASSIGN(int64_t read_length, in->Read(content.data(), 6));
    ASSERT_EQ(6, read_length);
    ASSERT_EQ("paimon", content);
    ASSERT_OK(in->Close());

    // the refreshed credentials are reused
    ASSERT_OK(fs->ReadFile(path, &content));
    ASSERT_EQ(2, state_->request_count.load());
}

TEST_F(RestTokenFileSystemTest, ExpiredTokenReloadsOnEveryCall) {
    // an expiration the server did not report makes the credentials expire immediately
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->expires_at_millis = 0;
    }
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem();
    ASSERT_NE(nullptr, fs);

    ASSERT_OK(fs->ValidToken().status());
    ASSERT_OK(fs->ValidToken().status());
    ASSERT_EQ(2, state_->request_count.load());
}

TEST_F(RestTokenFileSystemTest, ForbiddenIsReportedToTheCaller) {
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->force_error_code = 403;
    }
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem();
    ASSERT_NE(nullptr, fs);

    Status status = fs->Exists("any-path").status();
    ASSERT_NOK(status);
    ASSERT_NOK_WITH_MSG(status, "no permission");
    ASSERT_NE(nullptr, status.detail());
    ASSERT_EQ(std::string(RestErrorDetail::kTypeId), status.detail()->type_id());
    ASSERT_EQ(403, checked_pointer_cast<RestErrorDetail>(status.detail())->GetCode());

    // a later success is not blocked by the earlier failure
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->force_error_code.reset();
    }
    std::string path = WriteFile("data", "paimon");
    ASSERT_OK_AND_ASSIGN(bool exists, fs->Exists(path));
    ASSERT_TRUE(exists);
}

TEST_F(RestTokenFileSystemTest, DefaultClockIsTheSystemClock) {
    // the default clock is only exercised here: the other tests inject their own
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->expires_at_millis = ToMillis(std::chrono::system_clock::now()) +
                                    10 * RestApi::kTokenExpirationSafeTimeMillis;
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestApi> api,
                         RestApi::Create(catalog_options_, "", /*config_required=*/false));
    RestTokenFileSystem fs(std::shared_ptr<RestApi>(std::move(api)), catalog_options_,
                           Identifier("db1", "t1"));
    ASSERT_OK(fs.ValidToken().status());
    ASSERT_OK(fs.ValidToken().status());
    ASSERT_EQ(1, state_->request_count.load());
}

TEST_F(RestTokenFileSystemTest, EmptyDlfOssEndpointIsNotApplied) {
    catalog_options_[CatalogOptions::DLF_OSS_ENDPOINT] = "";
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{kOssEndpointOption, "server-endpoint"}};
    }
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem();
    ASSERT_NE(nullptr, fs);

    // an unset dlf endpoint leaves the endpoint the server reported alone
    ASSERT_OK_AND_ASSIGN(RestToken token, fs->ValidToken());
    ASSERT_EQ("server-endpoint", token.token.at(kOssEndpointOption));
    ASSERT_EQ(1u, token.token.size());
}

TEST_F(RestTokenFileSystemTest, TokenOverridesTheCatalogFileSystemOptions) {
    // the credentials must reach the options the delegate is built from: the catalog is
    // configured with the local file system, yet an option of the token replaces it
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{Options::FILE_SYSTEM, "no-such-file-system"}};
    }
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem();
    ASSERT_NE(nullptr, fs);

    std::string path = WriteFile("data", "paimon");
    Status status = fs->Exists(path).status();
    ASSERT_NOK(status);
    ASSERT_NOK_WITH_MSG(status, "no-such-file-system");
    ASSERT_EQ(1, state_->request_count.load());
}

TEST_F(RestTokenFileSystemTest, ConcurrentFirstAccessLoadsTheTokenOnce) {
    std::string path = WriteFile("data", "paimon");
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem();
    ASSERT_NE(nullptr, fs);

    constexpr size_t kThreads = 8;
    std::atomic<int32_t> failures{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (size_t i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            Result<bool> exists = fs->Exists(path);
            if (!exists.ok() || !exists.value()) {
                failures++;
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(0, failures.load());
    // the double-checked refresh keeps the threads that waited for the lock from each
    // loading credentials of their own
    ASSERT_EQ(1, state_->request_count.load());
}

TEST(RestTokenTest, OrdersByExpirationThenCredentials) {
    RestToken early{{{"k", "v"}}, 1};
    RestToken late{{{"k", "v"}}, 2};
    ASSERT_TRUE(early < late);
    ASSERT_FALSE(late < early);

    RestToken other_credentials{{{"k", "w"}}, 1};
    ASSERT_TRUE(early < other_credentials);
    ASSERT_FALSE(other_credentials < early);
    ASSERT_FALSE(early < early);
}

}  // namespace paimon::test
