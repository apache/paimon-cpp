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

#include "paimon/rest/rest_commit_lock.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/catalog_options.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/rest/mock_rest_server.h"
#include "paimon/rest/rest_api.h"
#include "paimon/rest/rest_http_client.h"
#include "paimon/rest/rest_messages.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

constexpr const char kCommitUser[] = "commit-user";
constexpr const char kTableUuid[] = "table-uuid";
constexpr const char kAcquirePath[] = "/v1/databases/db/tables/t1/commit-lock";
constexpr const char kRenewPath[] = "/v1/databases/db/tables/t1/commit-lock/renew";

struct LeaseServerState {
    std::mutex mutex;
    std::vector<CommitLockRequest> acquire_requests;
    std::vector<CommitLockRequest> renew_requests;
    int32_t contended_acquires = 0;
    int64_t lease_millis = 60000;
    std::optional<std::string> granted_owner;
    std::optional<int64_t> renewed_lease_millis;
    size_t refused_renewals = 0;
    std::set<std::string> refused_renewal_users;
    std::optional<int32_t> acquire_error_code;
    std::optional<int32_t> renew_error_code;
    int64_t acquire_delay_millis = 0;
    std::optional<size_t> answered_renewals;
    std::condition_variable renewal_answered;
    std::atomic<int64_t> now_millis{1000};
};

MockRestServer::Response JsonResponse(int32_t code, const std::string& body) {
    MockRestServer::Response response;
    response.code = code;
    response.body = body;
    return response;
}

MockRestServer::Response Error(int32_t code) {
    ErrorResponse error(ErrorResponse::kResourceTypeTable, "t1", "lease request rejected", code);
    return JsonResponse(code, error.ToJsonString().value());
}

MockRestServer::Response Lease(bool acquired, const std::string& owner, int64_t lease_millis) {
    CommitLockResponse response(
        acquired, acquired ? std::optional<std::string>(owner) : std::nullopt,
        acquired ? 1700000000000 + lease_millis : 0, acquired ? lease_millis : 0, std::nullopt);
    return JsonResponse(200, response.ToJsonString().value());
}

MockRestServer::Response HandleLeaseRequest(LeaseServerState* state,
                                            const MockRestServer::Request& request) {
    std::unique_lock<std::mutex> lock(state->mutex);
    if (request.method != "POST") {
        return Error(400);
    }
    Result<CommitLockRequest> parsed = CommitLockRequest::FromJsonString(request.body);
    if (!parsed.ok()) {
        return Error(400);
    }
    if (request.path == kAcquirePath) {
        state->acquire_requests.push_back(parsed.value());
        state->now_millis += state->acquire_delay_millis;
        if (state->acquire_error_code) {
            return Error(state->acquire_error_code.value());
        }
        if (state->contended_acquires > 0) {
            --state->contended_acquires;
            return Lease(false, "", 0);
        }
        return Lease(true, state->granted_owner.value_or(parsed.value().GetCommitUser()),
                     state->lease_millis);
    }
    if (request.path == kRenewPath) {
        state->renew_requests.push_back(parsed.value());
        const size_t renewal = state->renew_requests.size();
        state->renewal_answered.wait_for(lock, std::chrono::seconds(10), [state, renewal]() {
            return !state->answered_renewals || state->answered_renewals.value() >= renewal;
        });
        if (state->renew_error_code) {
            return Error(state->renew_error_code.value());
        }
        if (renewal <= state->refused_renewals ||
            state->refused_renewal_users.count(parsed.value().GetCommitUser()) > 0) {
            return Lease(false, "", 0);
        }
        return Lease(true, state->granted_owner.value_or(parsed.value().GetCommitUser()),
                     state->renewed_lease_millis.value_or(state->lease_millis));
    }
    return Error(404);
}

}  // namespace

class RestCommitLockTest : public ::testing::Test {
 protected:
    void SetUp() override {
        state_ = std::make_shared<LeaseServerState>();
        ASSERT_OK_AND_ASSIGN(
            server_, MockRestServer::Start([state = state_](const MockRestServer::Request& req) {
                return HandleLeaseRequest(state.get(), req);
            }));
        std::map<std::string, std::string> options = {
            {CatalogOptions::URI, server_->GetBaseUri()},
            {CatalogOptions::TOKEN_PROVIDER, "bear"},
            {CatalogOptions::TOKEN, "test-token"},
        };
        RestHttpClient::Config config;
        config.retry_base_delay_ms = 1;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestApi> api,
                             RestApi::Create(options, "", /*config_required=*/false, config));
        api_ = std::move(api);
    }

    void TearDown() override {
        if (server_) {
            server_->Stop();
        }
    }

    Result<std::unique_ptr<RestCommitLock>> CreateLock(
        int64_t acquire_timeout_millis = 10000, const std::string& commit_user = kCommitUser,
        RestCommitLock::ThreadStarter start_thread = &RestCommitLock::StartDetachedThread) {
        std::shared_ptr<LeaseServerState> state = state_;
        return RestCommitLock::Create(
            api_, Identifier("db", "t1"), std::string(kTableUuid), commit_user,
            acquire_timeout_millis, [state]() { return state->now_millis.load(); },
            std::move(start_thread));
    }

    size_t AcquireRequests() {
        std::lock_guard<std::mutex> lock(state_->mutex);
        return state_->acquire_requests.size();
    }

    size_t RenewRequests(const std::optional<std::string>& commit_user = std::nullopt) {
        std::lock_guard<std::mutex> lock(state_->mutex);
        return static_cast<size_t>(
            std::count_if(state_->renew_requests.begin(), state_->renew_requests.end(),
                          [&commit_user](const CommitLockRequest& request) {
                              return !commit_user || request.GetCommitUser() == commit_user.value();
                          }));
    }

    bool AwaitRenewals(size_t count, const std::optional<std::string>& commit_user = std::nullopt) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (RenewRequests(commit_user) < count) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return true;
    }

    void AnswerRenewals(std::optional<size_t> count) {
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->answered_renewals = count;
        }
        state_->renewal_answered.notify_all();
    }

    static Status AwaitInvalid(CommitLockScope* scope) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (true) {
            Status status = scope->EnsureValid();
            if (!status.ok() || std::chrono::steady_clock::now() > deadline) {
                return status;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    std::shared_ptr<LeaseServerState> state_;
    std::unique_ptr<MockRestServer> server_;
    std::shared_ptr<RestApi> api_;
};

TEST_F(RestCommitLockTest, MissingIdentityIsRejectedBeforeAnyRequest) {
    Identifier identifier("db", "t1");
    ASSERT_NOK_WITH_MSG(RestCommitLock::Create(api_, identifier, std::nullopt, kCommitUser,
                                               /*acquire_timeout_millis=*/0),
                        "requires the table UUID");
    ASSERT_NOK_WITH_MSG(RestCommitLock::Create(api_, identifier, std::string(), kCommitUser, 0),
                        "requires the table UUID");
    ASSERT_NOK_WITH_MSG(RestCommitLock::Create(api_, identifier, std::string(kTableUuid), "", 0),
                        "requires a commit user");
    ASSERT_NOK_WITH_MSG(
        RestCommitLock::Create(api_, identifier, std::string(kTableUuid), kCommitUser, -1),
        "cannot be negative");
    ASSERT_NOK_WITH_MSG(
        RestCommitLock::Create(nullptr, identifier, std::string(kTableUuid), kCommitUser, 0),
        "requires a rest api");
    ASSERT_NOK_WITH_MSG(
        RestCommitLock::Create(api_, identifier, std::string(kTableUuid), kCommitUser, 0,
                               &RestCommitLock::SteadyClockMillis, /*start_thread=*/nullptr),
        "a thread starter");
    ASSERT_EQ(0u, AcquireRequests());
}

TEST_F(RestCommitLockTest, AcquirePollsUntilGranted) {
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->contended_acquires = 2;
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCommitLock> lock, CreateLock());
    ASSERT_EQ(0u, AcquireRequests());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitLockScope> scope, lock->Acquire());
    ASSERT_NE(nullptr, scope);
    ASSERT_OK(scope->EnsureValid());
    std::lock_guard<std::mutex> guard(state_->mutex);
    ASSERT_EQ(3u, state_->acquire_requests.size());
    for (const CommitLockRequest& request : state_->acquire_requests) {
        ASSERT_EQ(kTableUuid, request.GetTableId());
        ASSERT_EQ(kCommitUser, request.GetCommitUser());
    }
}

TEST_F(RestCommitLockTest, ContentionTimesOutWithoutRenewing) {
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->contended_acquires = 1000;
        state_->acquire_delay_millis = 50;
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCommitLock> lock,
                         CreateLock(/*acquire_timeout_millis=*/120));
    Status status = lock->Acquire().status();
    ASSERT_TRUE(status.IsIOError()) << status.ToString();
    ASSERT_NOK_WITH_MSG(status, "timed out acquiring the commit lock");
    ASSERT_EQ(3u, AcquireRequests());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCommitLock> impatient,
                         CreateLock(/*acquire_timeout_millis=*/0));
    ASSERT_NOK_WITH_MSG(impatient->Acquire(), "timed out acquiring the commit lock");
    ASSERT_EQ(4u, AcquireRequests());
    ASSERT_EQ(0u, RenewRequests());
}

TEST_F(RestCommitLockTest, FailedAcquisitionStartsNoLease) {
    auto failed_starts = std::make_shared<std::atomic<int32_t>>(1);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCommitLock> lock,
                         CreateLock(/*acquire_timeout_millis=*/10000, kCommitUser,
                                    [failed_starts](std::function<void()> task) -> Status {
                                        if (failed_starts->fetch_sub(1) > 0) {
                                            return Status::UnknownError("no thread available");
                                        }
                                        return RestCommitLock::StartDetachedThread(std::move(task));
                                    }));
    for (int32_t code : {400, 403, 404}) {
        SCOPED_TRACE(code);
        {
            std::lock_guard<std::mutex> guard(state_->mutex);
            state_->acquire_error_code = code;
        }
        Status status = lock->Acquire().status();
        ASSERT_NOK_WITH_MSG(status, "failed to acquire the commit lock of");
        ASSERT_NOK_WITH_MSG(status, "lease request rejected");
        if (code == 400) {
            ASSERT_TRUE(status.IsInvalid()) << status.ToString();
        } else if (code == 403) {
            ASSERT_TRUE(status.IsIOError()) << status.ToString();
            ASSERT_NOK_WITH_MSG(status, "forbidden");
        } else {
            ASSERT_TRUE(status.IsNotExist()) << status.ToString();
        }
    }

    for (bool other_owner : {true, false}) {
        SCOPED_TRACE(other_owner);
        {
            std::lock_guard<std::mutex> guard(state_->mutex);
            state_->acquire_error_code.reset();
            state_->granted_owner = other_owner ? "another-writer" : kCommitUser;
            state_->lease_millis = other_owner ? 60000 : 0;
        }
        Status status = lock->Acquire().status();
        ASSERT_TRUE(status.IsInvalid()) << status.ToString();
        ASSERT_NOK_WITH_MSG(status, "the server returned an invalid commit lease");
        const char* reason = other_owner ? "expected owner 'commit-user', got 'another-writer'"
                                         : "expected a positive leaseMillis, got 0";
        ASSERT_NOK_WITH_MSG(status, reason);
    }

    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->granted_owner.reset();
        state_->lease_millis = 30;
    }
    Status status = lock->Acquire().status();
    ASSERT_TRUE(status.IsUnknownError()) << status.ToString();
    ASSERT_NOK_WITH_MSG(status, "failed to start renewing the commit lease of");
    ASSERT_NOK_WITH_MSG(status, "no thread available");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(6u, AcquireRequests());
    ASSERT_EQ(0u, RenewRequests());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitLockScope> scope, lock->Acquire());
    ASSERT_TRUE(AwaitRenewals(2));
    ASSERT_OK(scope->EnsureValid());
    ASSERT_EQ(7u, AcquireRequests());
}

TEST_F(RestCommitLockTest, RenewalExtendsTheLeaseFromEachRequest) {
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->lease_millis = 30;
        state_->answered_renewals = 0;
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCommitLock> lock, CreateLock());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitLockScope> scope, lock->Acquire());
    ASSERT_TRUE(AwaitRenewals(1));
    state_->now_millis += 20;
    AnswerRenewals(1);
    ASSERT_TRUE(AwaitRenewals(2));
    state_->now_millis += 9;
    AnswerRenewals(2);
    ASSERT_TRUE(AwaitRenewals(3));
    state_->now_millis += 20;
    ASSERT_OK(scope->EnsureValid());
    state_->now_millis += 1;
    ASSERT_NOK_WITH_MSG(scope->EnsureValid(), "expired locally");
    AnswerRenewals(3);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_NOK_WITH_MSG(scope->EnsureValid(), "expired locally");
    ASSERT_EQ(3u, RenewRequests());
    std::lock_guard<std::mutex> guard(state_->mutex);
    for (const CommitLockRequest& renewal : state_->renew_requests) {
        ASSERT_EQ(kTableUuid, renewal.GetTableId());
        ASSERT_EQ(kCommitUser, renewal.GetCommitUser());
    }
}

TEST_F(RestCommitLockTest, EndingTheScopeDoesNotWaitForARenewalInFlight) {
    struct TokenServerState {
        std::mutex mutex;
        std::condition_variable changed;
        bool hold = false;
        bool released = false;
        int32_t held = 0;
    };
    auto tokens = std::make_shared<TokenServerState>();
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<MockRestServer> token_server,
        MockRestServer::Start([tokens](const MockRestServer::Request&) {
            std::unique_lock<std::mutex> lock(tokens->mutex);
            if (tokens->hold) {
                ++tokens->held;
                tokens->changed.notify_all();
                tokens->changed.wait_for(lock, std::chrono::seconds(30),
                                         [&tokens]() { return tokens->released; });
            }
            MockRestServer::Response response;
            response.body = R"({"AccessKeyId": "ak", "AccessKeySecret": "sk",)"
                            R"( "SecurityToken": "sts", "Expiration": "2020-01-01T00:00:00Z"})";
            return response;
        }));
    auto release_tokens = [tokens]() {
        {
            std::lock_guard<std::mutex> lock(tokens->mutex);
            tokens->released = true;
        }
        tokens->changed.notify_all();
    };
    ScopeGuard release_on_exit(release_tokens);

    std::map<std::string, std::string> options = {
        {CatalogOptions::URI, server_->GetBaseUri()},
        {CatalogOptions::TOKEN_PROVIDER, "dlf"},
        {CatalogOptions::DLF_REGION, "cn-hangzhou"},
        {CatalogOptions::DLF_TOKEN_LOADER, "ecs"},
        {CatalogOptions::DLF_TOKEN_ECS_METADATA_URL, token_server->GetBaseUri() + "/"},
        {CatalogOptions::DLF_TOKEN_ECS_ROLE_NAME, "role"},
    };
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestApi> dlf_api,
                         RestApi::Create(options, "", /*config_required=*/false));
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->lease_millis = 30;
    }
    std::shared_ptr<LeaseServerState> state = state_;
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RestCommitLock> lock,
        RestCommitLock::Create(std::move(dlf_api), Identifier("db", "t1"), std::string(kTableUuid),
                               kCommitUser, /*acquire_timeout_millis=*/10000,
                               [state]() { return state->now_millis.load(); }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitLockScope> scope, lock->Acquire());
    {
        std::unique_lock<std::mutex> guard(tokens->mutex);
        tokens->hold = true;
        ASSERT_TRUE(tokens->changed.wait_for(guard, std::chrono::seconds(10),
                                             [&tokens]() { return tokens->held > 0; }));
    }
    size_t renewals = RenewRequests();
    auto ended_at = std::chrono::steady_clock::now();
    scope.reset();
    std::chrono::steady_clock::duration ending = std::chrono::steady_clock::now() - ended_at;
    release_tokens();
    ASSERT_LT(ending, std::chrono::seconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(renewals, RenewRequests());
}

TEST_F(RestCommitLockTest, LateAnswerToAnEndedScopeCannotAffectTheNextScope) {
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->lease_millis = 30;
        state_->answered_renewals = 0;
        state_->refused_renewals = 1;
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCommitLock> lock, CreateLock());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitLockScope> first, lock->Acquire());
    ASSERT_TRUE(AwaitRenewals(1));
    first.reset();

    Result<std::unique_ptr<CommitLockScope>> second = Status::UnknownError("not acquired yet");
    std::thread acquiring([&lock, &second]() { second = lock->Acquire(); });
    ScopeGuard join_on_exit([&acquiring]() {
        if (acquiring.joinable()) {
            acquiring.join();
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    AnswerRenewals(std::nullopt);
    acquiring.join();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitLockScope> scope, std::move(second));
    ASSERT_TRUE(AwaitRenewals(3));
    ASSERT_OK(scope->EnsureValid());
    ASSERT_EQ(2u, AcquireRequests());
}

TEST_F(RestCommitLockTest, LeasesOfDifferentWritersDoNotInterfere) {
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->lease_millis = 30;
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCommitLock> lock_a,
                         CreateLock(/*acquire_timeout_millis=*/10000, "writer-a"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCommitLock> lock_b,
                         CreateLock(/*acquire_timeout_millis=*/10000, "writer-b"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitLockScope> scope_a, lock_a->Acquire());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitLockScope> scope_b, lock_b->Acquire());

    ASSERT_TRUE(AwaitRenewals(1, "writer-b"));
    scope_b.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    size_t renewals_b = RenewRequests("writer-b");
    ASSERT_TRUE(AwaitRenewals(RenewRequests("writer-a") + 2, "writer-a"));
    ASSERT_EQ(renewals_b, RenewRequests("writer-b"));
    ASSERT_OK(scope_a->EnsureValid());

    ASSERT_OK_AND_ASSIGN(scope_b, lock_b->Acquire());
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->refused_renewal_users.insert("writer-b");
    }
    ASSERT_NOK_WITH_MSG(AwaitInvalid(scope_b.get()),
                        "held by commit user writer-b is no longer usable");
    ASSERT_TRUE(AwaitRenewals(RenewRequests("writer-a") + 2, "writer-a"));
    ASSERT_OK(scope_a->EnsureValid());
}

TEST_F(RestCommitLockTest, FailedRenewalInvalidatesTheLease) {
    enum class Failure { kLost, kError, kOtherOwner, kOtherDuration };
    for (Failure failure :
         {Failure::kLost, Failure::kError, Failure::kOtherOwner, Failure::kOtherDuration}) {
        SCOPED_TRACE(static_cast<int32_t>(failure));
        {
            std::lock_guard<std::mutex> guard(state_->mutex);
            state_->lease_millis = 30;
            state_->refused_renewal_users.clear();
            state_->renew_error_code.reset();
            state_->granted_owner = kCommitUser;
            state_->renewed_lease_millis.reset();
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCommitLock> lock, CreateLock());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitLockScope> scope, lock->Acquire());
        {
            std::lock_guard<std::mutex> guard(state_->mutex);
            if (failure == Failure::kLost) {
                state_->refused_renewal_users.insert(kCommitUser);
            }
            state_->renew_error_code =
                failure == Failure::kError ? std::optional<int32_t>(500) : std::nullopt;
            state_->granted_owner =
                failure == Failure::kOtherOwner ? "another-writer" : kCommitUser;
            state_->renewed_lease_millis =
                failure == Failure::kOtherDuration ? std::optional<int64_t>(60) : std::nullopt;
        }
        Status status = AwaitInvalid(scope.get());
        ASSERT_NOK_WITH_MSG(status, "no longer usable");
        switch (failure) {
            case Failure::kLost:
                ASSERT_TRUE(status.IsInvalid()) << status.ToString();
                ASSERT_NOK_WITH_MSG(status, "the commit lease has expired");
                break;
            case Failure::kError:
                ASSERT_TRUE(status.IsIOError()) << status.ToString();
                ASSERT_NOK_WITH_MSG(status, "server error");
                break;
            case Failure::kOtherOwner:
                ASSERT_TRUE(status.IsInvalid()) << status.ToString();
                ASSERT_NOK_WITH_MSG(status, "expected owner 'commit-user', got 'another-writer'");
                break;
            case Failure::kOtherDuration:
                ASSERT_TRUE(status.IsInvalid()) << status.ToString();
                ASSERT_NOK_WITH_MSG(status,
                                    "duration changed during renewal: expected leaseMillis 30, "
                                    "got 60");
                break;
        }
        size_t renewals = RenewRequests();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ASSERT_EQ(renewals, RenewRequests());
        ASSERT_NOK(scope->EnsureValid());
    }
}

TEST_F(RestCommitLockTest, LocalExpiryInvalidatesTheLease) {
    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->lease_millis = 1000;
        state_->acquire_delay_millis = 1000;
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestCommitLock> lock, CreateLock());
    ASSERT_NOK_WITH_MSG(lock->Acquire(), "expired locally");

    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->lease_millis = 60000;
        state_->acquire_delay_millis = 0;
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitLockScope> scope, lock->Acquire());
    state_->now_millis += 59999;
    ASSERT_OK(scope->EnsureValid());
    state_->now_millis += 1;
    Status status = scope->EnsureValid();
    ASSERT_TRUE(status.IsInvalid()) << status.ToString();
    ASSERT_NOK_WITH_MSG(status, "expired locally");
    state_->now_millis -= 30000;
    ASSERT_NOK_WITH_MSG(scope->EnsureValid(), "expired locally");
    ASSERT_EQ(0u, RenewRequests());

    {
        std::lock_guard<std::mutex> guard(state_->mutex);
        state_->lease_millis = 30;
        state_->answered_renewals = 0;
    }
    ASSERT_OK_AND_ASSIGN(scope, lock->Acquire());
    ASSERT_TRUE(AwaitRenewals(1));
    state_->now_millis += 30;
    AnswerRenewals(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(1u, RenewRequests());
    ASSERT_NOK_WITH_MSG(scope->EnsureValid(), "expired locally");
}

}  // namespace paimon::test
