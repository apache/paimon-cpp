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
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/catalog_options.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
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
    // when set, the token endpoint answers with this body and http 200, which lets a test
    // return a malformed response without going through serialization
    std::optional<std::string> response_body;
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
    if (state->response_body) {
        response.body = state->response_body.value();
        return response;
    }
    GetTableTokenResponse token(state->token, state->expires_at_millis);
    response.body = token.ToJsonString().value();
    return response;
}

int64_t ToMillis(std::chrono::system_clock::time_point time) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(time.time_since_epoch()).count();
}

// A local file system that runs a callback right after a file was created, so a test can
// rotate the credentials in the middle of a multi step operation without depending on
// thread scheduling or on waiting for real time to pass.
class RefreshOnCreateFileSystem : public LocalFileSystem {
 public:
    explicit RefreshOnCreateFileSystem(std::function<void()> on_create)
        : on_create_(std::move(on_create)) {}

    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out,
                               LocalFileSystem::Create(path, overwrite));
        on_create_();
        return out;
    }

 private:
    std::function<void()> on_create_;
};

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

    std::shared_ptr<RestTokenFileSystem> CreateFileSystem(
        std::shared_ptr<RestTokenFileSystemCache> fs_cache = nullptr) {
        Result<std::unique_ptr<RestApi>> api =
            RestApi::Create(catalog_options_, "", /*config_required=*/false);
        if (!api.ok()) {
            return nullptr;
        }
        std::shared_ptr<RestApi> shared_api(std::move(api).value());
        return std::make_shared<RestTokenFileSystem>(
            shared_api, catalog_options_, Identifier("db1", "t1"), std::move(fs_cache), [this] {
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

TEST(RestTokenFileSystemMergeTokenOptions, IssuedCredentialsWinOverBucketScopedCatalogOnes) {
    // The catalog is configured with bucket-scoped credentials, which a file system resolves
    // ahead of the flat options, so they must not shadow the credentials the server issues.
    std::map<std::string, std::string> catalog_options = {
        {"fs.oss.bucket.b.accessKeyId", "catalog-bucket-ak"},
        {"fs.oss.bucket.b.accessKeySecret", "catalog-bucket-sk"},
        {"fs.oss.bucket.other.accessKeyId", "catalog-other-ak"},
        {"fs.oss.accessKeyId", "catalog-ak"},
        {"fs.oss.endpoint", "catalog-endpoint"},
        {"fs.oss.bucket.b.endpoint", "catalog-bucket-endpoint"},
        {"unrelated", "kept"},
    };
    RestToken token;
    token.token = {{"fs.oss.accessKeyId", "token-ak"},
                   {"fs.oss.accessKeySecret", "token-sk"},
                   {"fs.oss.securityToken", "token-sts"}};

    std::map<std::string, std::string> merged =
        RestTokenFileSystem::MergeTokenOptions(catalog_options, token);

    // The issued credentials are present and every bucket-scoped variant of them is gone, so
    // the per-bucket resolution a file system does falls back to the issued flat options
    // rather than signing with a stale catalog key pair and the issued security token.
    ASSERT_EQ("token-ak", merged.at("fs.oss.accessKeyId"));
    ASSERT_EQ("token-sk", merged.at("fs.oss.accessKeySecret"));
    ASSERT_EQ("token-sts", merged.at("fs.oss.securityToken"));
    ASSERT_EQ(0u, merged.count("fs.oss.bucket.b.accessKeyId"));
    ASSERT_EQ(0u, merged.count("fs.oss.bucket.b.accessKeySecret"));
    ASSERT_EQ(0u, merged.count("fs.oss.bucket.other.accessKeyId"));

    // Options the token does not set keep their catalog value, bucket-scoped ones included.
    ASSERT_EQ("catalog-endpoint", merged.at("fs.oss.endpoint"));
    ASSERT_EQ("catalog-bucket-endpoint", merged.at("fs.oss.bucket.b.endpoint"));
    ASSERT_EQ("kept", merged.at("unrelated"));
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

TEST_F(RestTokenFileSystemTest, ValidTokenNeedsNoDelegate) {
    // a caller that brings its own file system only needs the credentials, so an option
    // the delegate cannot be built from must not keep it from getting them
    catalog_options_["manifest.format"] = "no-such-format";
    std::string path = WriteFile("data", "paimon");
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem();
    ASSERT_NE(nullptr, fs);

    ASSERT_OK_AND_ASSIGN(RestToken token, fs->ValidToken());
    ASSERT_EQ("ak-1", token.token.at("fs.oss.accessKeyId"));
    ASSERT_EQ(kExpiresAtMillis, token.expires_at_millis);

    // a file operation does need the delegate and reports why it cannot be built
    Status status = fs->Exists(path).status();
    ASSERT_NOK(status);
    ASSERT_NOK_WITH_MSG(status, "no-such-format");
}

TEST_F(RestTokenFileSystemTest, FileSystemsOfEqualCredentialsAreShared) {
    std::string path = WriteFile("data", "paimon");
    std::shared_ptr<RestTokenFileSystemCache> cache = RestTokenFileSystem::CreateFileSystemCache();
    std::shared_ptr<RestTokenFileSystem> first = CreateFileSystem(cache);
    std::shared_ptr<RestTokenFileSystem> second = CreateFileSystem(cache);
    ASSERT_NE(nullptr, first);
    ASSERT_NE(nullptr, second);

    ASSERT_OK(first->Exists(path).status());
    ASSERT_OK(second->Exists(path).status());
    // both loaded credentials of their own, which are equal and so share one delegate
    ASSERT_EQ(2, state_->request_count.load());
    ASSERT_EQ(1u, cache->Size());

    // the delegate of the rotated credentials is added while the previous one is kept, so
    // a stream opened just before the rotation does not lose the file system it came from
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{"fs.oss.accessKeyId", "ak-2"}};
        state_->expires_at_millis = kExpiresAtMillis + RestApi::kTokenExpirationSafeTimeMillis;
    }
    now_millis_ = kExpiresAtMillis - 1;
    ASSERT_OK(first->Exists(path).status());
    ASSERT_EQ(2u, cache->Size());
}

TEST_F(RestTokenFileSystemTest, MalformedTokenResponseDoesNotAccessBackend) {
    catalog_options_["fs.oss.accessKeyId"] = "catalog-ak";
    std::string path = WriteFile("data", "original");
    const std::vector<std::string> bodies = {
        fmt::format(R"({{"expiresAtMillis":{}}})", kExpiresAtMillis),
        fmt::format(R"({{"token":null,"expiresAtMillis":{}}})", kExpiresAtMillis)};
    for (bool warm_cache : {false, true}) {
        for (const auto& body : bodies) {
            SCOPED_TRACE(body);
            SCOPED_TRACE(warm_cache);
            now_millis_ = kNowMillis;
            {
                std::lock_guard<std::mutex> lock(state_->mutex);
                state_->response_body.reset();
                state_->expires_at_millis = kExpiresAtMillis;
            }
            std::shared_ptr<RestTokenFileSystemCache> cache =
                RestTokenFileSystem::CreateFileSystemCache();
            std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem(cache);
            ASSERT_NE(nullptr, fs);
            if (warm_cache) {
                ASSERT_OK_AND_ASSIGN(bool exists, fs->Exists(path));
                ASSERT_TRUE(exists);
                // the loaded credentials have not expired yet but entered the refresh
                // window, so a refresh that fails must not fall back to them
                now_millis_ = kExpiresAtMillis - RestApi::kTokenExpirationSafeTimeMillis + 1;
            }
            {
                std::lock_guard<std::mutex> lock(state_->mutex);
                state_->response_body = body;
            }
            int32_t requests_before = state_->request_count.load();
            // the hook counts every access of the local files, so it shows that a failed
            // refresh never reaches the backend
            IOHook* hook = IOHook::GetInstance();
            ScopeGuard guard([hook]() { hook->Clear(); });
            hook->Reset(0, IOHook::Mode::SILENT);
            Status token_status = fs->ValidToken().status();
            ASSERT_TRUE(token_status.IsInvalid()) << token_status.ToString();
            Status read_status = fs->Exists(path).status();
            ASSERT_TRUE(read_status.IsInvalid()) << read_status.ToString();
            Status write_status = fs->WriteFile(path, "modified", /*overwrite=*/true);
            ASSERT_TRUE(write_status.IsInvalid()) << write_status.ToString();
            ASSERT_EQ(0, hook->IOCount());
            ASSERT_EQ(warm_cache ? 1u : 0u, cache->Size());
            ASSERT_EQ(requests_before + 3, state_->request_count.load());
            hook->Clear();

            std::string content;
            ASSERT_OK(temp_dir_->GetFileSystem()->ReadFile(path, &content));
            ASSERT_EQ("original", content);
            {
                std::lock_guard<std::mutex> lock(state_->mutex);
                state_->response_body.reset();
                state_->expires_at_millis =
                    kExpiresAtMillis + RestApi::kTokenExpirationSafeTimeMillis;
            }
            ASSERT_OK(fs->ReadFile(path, &content));
            ASSERT_EQ("original", content);
            ASSERT_EQ(requests_before + 4, state_->request_count.load());
        }
    }
}

TEST_F(RestTokenFileSystemTest, ExplicitEmptyTokenAllowsFileOperations) {
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token.clear();
    }
    std::shared_ptr<RestTokenFileSystemCache> cache = RestTokenFileSystem::CreateFileSystemCache();
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem(cache);
    ASSERT_NE(nullptr, fs);
    ASSERT_OK_AND_ASSIGN(RestToken token, fs->ValidToken());
    ASSERT_TRUE(token.token.empty());
    ASSERT_EQ(0u, cache->Size());
    std::string path = temp_dir_->Str() + "/empty-token";
    ASSERT_OK(fs->WriteFile(path, "data", /*overwrite=*/false));
    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));
    ASSERT_EQ("data", content);
    ASSERT_EQ(1u, cache->Size());
    ASSERT_EQ(1, state_->request_count.load());
}

TEST_F(RestTokenFileSystemTest, DefaultCacheEvictsAndRebuildsBackendWithoutReloadingToken) {
    std::shared_ptr<RestTokenFileSystemCache> cache = RestTokenFileSystem::CreateFileSystemCache();
    ASSERT_EQ(1000, cache->GetMaxWeight());
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem(cache);
    ASSERT_NE(nullptr, fs);
    std::string path = WriteFile("data", "paimon");
    ASSERT_OK(fs->Exists(path).status());
    ASSERT_OK_AND_ASSIGN(RestToken token, fs->ValidToken());
    std::optional<std::shared_ptr<FileSystem>> cached = cache->GetIfPresent(token);
    ASSERT_TRUE(cached.has_value());
    std::weak_ptr<FileSystem> old_backend = cached.value();
    cached.reset();

    // fill the cache with other credentials to reach its real default capacity; the file
    // system asked for the credentials stays in use, only the delegate it built is evicted
    std::shared_ptr<FileSystem> filler = temp_dir_->GetFileSystem();
    for (int32_t i = 0; i < 1000; ++i) {
        RestToken other{{{"entry", fmt::format("{}", i)}}, kExpiresAtMillis};
        ASSERT_OK(cache->Get(other, [&filler](const RestToken&) { return filler; }).status());
    }
    ASSERT_EQ(1000u, cache->Size());
    ASSERT_FALSE(cache->GetIfPresent(token).has_value());
    ASSERT_TRUE(old_backend.expired());
    ASSERT_OK_AND_ASSIGN(bool exists, fs->Exists(path));
    ASSERT_TRUE(exists);
    ASSERT_TRUE(cache->GetIfPresent(token).has_value());
    ASSERT_EQ(1000u, cache->Size());
    ASSERT_EQ(1, state_->request_count.load());
}

TEST_F(RestTokenFileSystemTest, CacheExpirationDoesNotReloadValidToken) {
    using RemovalCause = RestTokenFileSystemCache::RemovalCause;
    std::vector<RemovalCause> causes;
    std::vector<std::weak_ptr<FileSystem>> removed_backends;
    RestTokenFileSystemCache::Options options;
    // an idle time of zero expires every entry right away, which the production default of
    // ten hours cannot do within a test
    options.expire_after_access_ms = 0;
    options.removal_callback = [&](const RestToken&, const std::shared_ptr<FileSystem>& backend,
                                   RemovalCause cause) {
        causes.push_back(cause);
        removed_backends.push_back(backend);
    };
    std::shared_ptr<RestTokenFileSystemCache> cache =
        std::make_shared<RestTokenFileSystemCache>(options);
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem(cache);
    ASSERT_NE(nullptr, fs);
    std::string path = WriteFile("data", "paimon");
    for (int32_t i = 0; i < 2; ++i) {
        ASSERT_OK_AND_ASSIGN(bool exists, fs->Exists(path));
        ASSERT_TRUE(exists);
        ASSERT_EQ(0u, cache->Size());
    }
    ASSERT_EQ((std::vector<RemovalCause>{RemovalCause::EXPIRED, RemovalCause::EXPIRED}), causes);
    ASSERT_EQ(2u, removed_backends.size());
    ASSERT_TRUE(removed_backends[0].expired());
    ASSERT_TRUE(removed_backends[1].expired());
    ASSERT_EQ(1, state_->request_count.load());
}

TEST_F(RestTokenFileSystemTest, LocalStreamsSurviveRotationAndCacheEviction) {
    RestTokenFileSystemCache::Options options;
    options.max_weight = 1;
    std::shared_ptr<RestTokenFileSystemCache> cache =
        std::make_shared<RestTokenFileSystemCache>(options);
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem(cache);
    ASSERT_NE(nullptr, fs);
    std::string path = WriteFile("data", "paimon");
    std::string output_path = temp_dir_->Str() + "/output";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> in, fs->Open(path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> out,
                         fs->Create(output_path, /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(RestToken token, fs->ValidToken());
    std::optional<std::shared_ptr<FileSystem>> cached = cache->GetIfPresent(token);
    ASSERT_TRUE(cached.has_value());
    std::weak_ptr<FileSystem> old_backend = cached.value();
    cached.reset();
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{"fs.oss.accessKeyId", "ak-2"}};
        state_->expires_at_millis = kExpiresAtMillis + RestApi::kTokenExpirationSafeTimeMillis;
    }
    now_millis_ = kExpiresAtMillis - 1;
    ASSERT_OK(fs->Exists(path).status());
    ASSERT_EQ(1u, cache->Size());
    ASSERT_FALSE(cache->GetIfPresent(token).has_value());
    ASSERT_TRUE(old_backend.expired());
    ASSERT_EQ(2, state_->request_count.load());
    fs.reset();
    cache.reset();

    // the streams of the local file system own their file handle, so they outlive the file
    // system they came from; other backends do not have to make that guarantee
    std::string content(6, '\0');
    ASSERT_OK_AND_ASSIGN(int64_t read_length, in->Read(content.data(), content.size()));
    ASSERT_EQ(6, read_length);
    ASSERT_EQ("paimon", content);
    ASSERT_OK(in->Close());
    ASSERT_OK_AND_ASSIGN(int64_t written, out->Write(content.data(), content.size()));
    ASSERT_EQ(6, written);
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
    ASSERT_OK(temp_dir_->GetFileSystem()->ReadFile(output_path, &content));
    ASSERT_EQ("paimon", content);
}

TEST_F(RestTokenFileSystemTest, HighLevelFileOperationsAcrossTokenRefresh) {
    std::shared_ptr<RestTokenFileSystemCache> cache = RestTokenFileSystem::CreateFileSystemCache();
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem(cache);
    ASSERT_NE(nullptr, fs);
    ASSERT_OK_AND_ASSIGN(RestToken first_token, fs->ValidToken());
    bool created = false;
    // `AtomicStore` writes a temporary file and renames it, so the rotation happens between
    // the steps of one operation
    std::shared_ptr<FileSystem> delegate = std::make_shared<RefreshOnCreateFileSystem>([&]() {
        created = true;
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{"fs.oss.accessKeyId", "ak-2"}};
        state_->expires_at_millis = kExpiresAtMillis + RestApi::kTokenExpirationSafeTimeMillis;
        now_millis_ = kExpiresAtMillis - 1;
    });
    ASSERT_OK(cache->Get(first_token, [&delegate](const RestToken&) { return delegate; }).status());
    std::string path = temp_dir_->Str() + "/atomic";
    ASSERT_OK(fs->AtomicStore(path, "original"));
    ASSERT_TRUE(created);
    ASSERT_EQ(2, state_->request_count.load());
    ASSERT_OK_AND_ASSIGN(RestToken second_token, fs->ValidToken());
    ASSERT_EQ("ak-2", second_token.token.at("fs.oss.accessKeyId"));
    ASSERT_TRUE(cache->GetIfPresent(second_token).has_value());
    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));
    ASSERT_EQ("original", content);
    ASSERT_OK(fs->WriteFile(path, "updated", /*overwrite=*/true));
    ASSERT_NOK(fs->WriteFile(path, "forbidden", /*overwrite=*/false));
    ASSERT_NOK(fs->AtomicStore(path, "forbidden"));
    ASSERT_OK(fs->ReadFile(path, &content));
    ASSERT_EQ("updated", content);
    std::vector<FileStatus> files;
    ASSERT_OK(fs->ListFileStatus(temp_dir_->Str(), &files));
    ASSERT_EQ(1u, files.size());
    ASSERT_EQ(path, files[0].GetPath());
    ASSERT_EQ(2, state_->request_count.load());
}

TEST_F(RestTokenFileSystemTest, ConcurrentFileSystemsShareOneCachedBackend) {
    std::string path = WriteFile("data", "paimon");
    std::shared_ptr<RestTokenFileSystemCache> cache = RestTokenFileSystem::CreateFileSystemCache();
    constexpr size_t kThreads = 8;
    std::vector<std::shared_ptr<RestTokenFileSystem>> file_systems;
    file_systems.reserve(kThreads);
    for (size_t i = 0; i < kThreads; ++i) {
        std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem(cache);
        ASSERT_NE(nullptr, fs);
        file_systems.push_back(std::move(fs));
    }

    std::atomic<int32_t> failures{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (const std::shared_ptr<RestTokenFileSystem>& file_system : file_systems) {
        threads.emplace_back([&failures, &path, file_system] {
            Result<bool> exists = file_system->Exists(path);
            if (!exists.ok() || !exists.value()) {
                failures++;
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(0, failures.load());
    // every file system loads credentials of its own, but since the server issues equal ones
    // to all of them, the delegates they ask the cache for converge to a single entry
    ASSERT_EQ(static_cast<int32_t>(kThreads), state_->request_count.load());
    ASSERT_EQ(1u, cache->Size());
    ASSERT_OK_AND_ASSIGN(RestToken token, file_systems.front()->ValidToken());
    ASSERT_TRUE(cache->GetIfPresent(token).has_value());
}

TEST_F(RestTokenFileSystemTest, RefreshedTokenThatCannotBuildDelegateFails) {
    std::string path = WriteFile("data", "paimon");
    std::shared_ptr<RestTokenFileSystemCache> cache = RestTokenFileSystem::CreateFileSystemCache();
    std::shared_ptr<RestTokenFileSystem> fs = CreateFileSystem(cache);
    ASSERT_NE(nullptr, fs);
    ASSERT_OK(fs->Exists(path).status());
    ASSERT_EQ(1u, cache->Size());

    // the refreshed credentials name a file system that cannot be built
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{Options::FILE_SYSTEM, "no-such-file-system"}};
        state_->expires_at_millis = kExpiresAtMillis + RestApi::kTokenExpirationSafeTimeMillis;
    }
    now_millis_ = kExpiresAtMillis - 1;
    Status status = fs->Exists(path).status();
    ASSERT_NOK(status);
    ASSERT_NOK_WITH_MSG(status, "no-such-file-system");
    ASSERT_EQ(2, state_->request_count.load());
    // the refreshed credentials replaced the previous ones, so the delegate that worked
    // before is not served as a fallback
    std::string content;
    Status read_status = fs->ReadFile(path, &content);
    ASSERT_NOK(read_status);
    ASSERT_NOK_WITH_MSG(read_status, "no-such-file-system");
    ASSERT_EQ(2, state_->request_count.load());

    // credentials the delegate can be built from recover the file system
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->token = {{"fs.oss.accessKeyId", "ak-2"}};
        state_->expires_at_millis = kExpiresAtMillis + 2 * RestApi::kTokenExpirationSafeTimeMillis;
    }
    now_millis_ = kExpiresAtMillis + RestApi::kTokenExpirationSafeTimeMillis - 1;
    ASSERT_OK(fs->ReadFile(path, &content));
    ASSERT_EQ("paimon", content);
    ASSERT_EQ(3, state_->request_count.load());
}

TEST(RestTokenTest, EqualCredentialsShareOneFileSystemKey) {
    RestToken token{{{"k", "v"}}, 1};
    RestToken same{{{"k", "v"}}, 1};
    ASSERT_TRUE(token == same);
    ASSERT_EQ(RestToken::Hash()(token), RestToken::Hash()(same));

    // neither a later expiration nor other credentials may be served the same file system
    RestToken later{{{"k", "v"}}, 2};
    RestToken other_credentials{{{"k", "w"}}, 1};
    ASSERT_FALSE(token == later);
    ASSERT_FALSE(token == other_credentials);
}

}  // namespace paimon::test
