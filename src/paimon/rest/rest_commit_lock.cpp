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
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <system_error>
#include <thread>
#include <utility>

#include "fmt/format.h"
#include "paimon/rest/rest_api.h"
#include "paimon/rest/rest_http_client.h"
#include "paimon/rest/rest_messages.h"

namespace paimon {

namespace {

// Longest pause between two acquisition requests while another writer holds the lease.
constexpr int64_t kMaxPollIntervalMillis = 100;

Status ValidateGrant(const CommitLockResponse& response, const std::string& commit_user) {
    const std::optional<std::string>& owner = response.GetCommitUser();
    if (owner != commit_user) {
        return Status::Invalid(fmt::format(
            "the server returned an invalid commit lease: expected owner '{}', got {}", commit_user,
            owner ? fmt::format("'{}'", owner.value()) : std::string("null")));
    }
    if (response.GetLeaseMillis() <= 0) {
        return Status::Invalid(fmt::format(
            "the server returned an invalid commit lease: expected a positive leaseMillis, got {}",
            response.GetLeaseMillis()));
    }
    return Status::OK();
}

// A granted lease, shared by its scope and the thread renewing it, which may outlive the scope.
class LeaseState {
 public:
    LeaseState(std::shared_ptr<RestApi> api, Identifier identifier, std::string table_uuid,
               std::string commit_user, RestCommitLock::Clock clock, int64_t lease_millis,
               int64_t requested_at_millis, std::shared_ptr<Logger> logger)
        : api_(std::move(api)),
          identifier_(std::move(identifier)),
          table_uuid_(std::move(table_uuid)),
          commit_user_(std::move(commit_user)),
          clock_(std::move(clock)),
          lease_millis_(lease_millis),
          logger_(std::move(logger)),
          renewed_at_millis_(requested_at_millis) {}

    Status EnsureValid() {
        std::lock_guard<std::mutex> lock(mutex_);
        return CheckValidLocked();
    }

    void Close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        closed_cv_.notify_all();
        renewal_cancellation_.Cancel();
    }

    void RenewLoop() {
        const std::chrono::milliseconds interval(std::max<int64_t>(1, lease_millis_ / 3));
        std::unique_lock<std::mutex> lock(mutex_);
        while (true) {
            const bool closed = closed_cv_.wait_for(lock, interval, [this]() { return closed_; });
            if (closed) {
                return;
            }
            if (!CheckValidLocked().ok()) {
                return;
            }
            const int64_t requested_at = clock_();
            lock.unlock();
            Status renewed = Renew();
            lock.lock();
            // A late answer cannot revive a scope that ended or expired while it was in flight.
            if (closed_ || !failure_.ok()) {
                return;
            }
            if (!renewed.ok()) {
                failure_ = renewed;
                PAIMON_LOG_WARN(logger_, "Stopped renewing the commit lease of %s: %s",
                                identifier_.ToString().c_str(), renewed.ToString().c_str());
                return;
            }
            renewed_at_millis_ = requested_at;
        }
    }

 private:
    Status CheckValidLocked() {
        // Measured from when the last grant was requested, so the deadline also covers the
        // response time, and enforced without relying on the renewal thread having run.
        if (failure_.ok() && clock_() - renewed_at_millis_ >= lease_millis_) {
            failure_ = Status::Invalid("the commit lease has expired locally");
        }
        if (!failure_.ok()) {
            return failure_.WithMessage(
                fmt::format("the commit lease of {} held by commit user {} is no longer usable: {}",
                            identifier_.ToString(), commit_user_, failure_.message()));
        }
        return Status::OK();
    }

    Status Renew() const {
        PAIMON_ASSIGN_OR_RAISE(
            CommitLockResponse response,
            api_->RenewCommitLock(identifier_, table_uuid_, commit_user_, &renewal_cancellation_));
        if (!response.IsAcquired()) {
            return Status::Invalid("the commit lease has expired");
        }
        PAIMON_RETURN_NOT_OK(ValidateGrant(response, commit_user_));
        if (response.GetLeaseMillis() != lease_millis_) {
            return Status::Invalid(fmt::format(
                "the commit lease duration changed during renewal: expected leaseMillis {}, got {}",
                lease_millis_, response.GetLeaseMillis()));
        }
        return Status::OK();
    }

    const std::shared_ptr<RestApi> api_;
    const Identifier identifier_;
    const std::string table_uuid_;
    const std::string commit_user_;
    const RestCommitLock::Clock clock_;
    const int64_t lease_millis_;
    const std::shared_ptr<Logger> logger_;
    RestHttpClient::Cancellation renewal_cancellation_;

    // Guards the fields below, shared with the renewal thread.
    std::mutex mutex_;
    std::condition_variable closed_cv_;
    bool closed_ = false;
    Status failure_;
    int64_t renewed_at_millis_;
};

// The scope of one granted lease, renewed in the background until the scope ends or the lease
// fails.
class RestCommitLease : public CommitLockScope {
 public:
    explicit RestCommitLease(std::shared_ptr<LeaseState> state) : state_(std::move(state)) {}

    ~RestCommitLease() override {
        state_->Close();
    }

    Status StartRenewal(const RestCommitLock::ThreadStarter& start_thread) {
        // Detached rather than joined when the scope ends: a renewal can block where no
        // cancellation reaches, as while refreshing credentials, and must not hold up the commit.
        return start_thread([state = state_]() { state->RenewLoop(); });
    }

    Status EnsureValid() override {
        return state_->EnsureValid();
    }

 private:
    std::shared_ptr<LeaseState> state_;
};

}  // namespace

int64_t RestCommitLock::SteadyClockMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

Status RestCommitLock::StartDetachedThread(std::function<void()> task) {
    try {
        std::thread(std::move(task)).detach();
    } catch (const std::system_error& e) {
        return Status::UnknownError(std::string("failed to start a thread: ") + e.what());
    }
    return Status::OK();
}

RestCommitLock::RestCommitLock(const std::shared_ptr<RestApi>& api, const Identifier& identifier,
                               const std::string& table_uuid, const std::string& commit_user,
                               int64_t acquire_timeout_millis, Clock clock,
                               ThreadStarter start_thread)
    : api_(api),
      identifier_(identifier),
      table_uuid_(table_uuid),
      commit_user_(commit_user),
      acquire_timeout_millis_(acquire_timeout_millis),
      clock_(std::move(clock)),
      start_thread_(std::move(start_thread)),
      logger_(Logger::GetLogger("RestCommitLock")) {}

Result<std::unique_ptr<RestCommitLock>> RestCommitLock::Create(
    const std::shared_ptr<RestApi>& api, const Identifier& identifier,
    const std::optional<std::string>& table_uuid, const std::string& commit_user,
    int64_t acquire_timeout_millis, Clock clock, ThreadStarter start_thread) {
    if (api == nullptr || !clock || !start_thread) {
        return Status::Invalid("a commit lease requires a rest api, a clock and a thread starter");
    }
    if (!table_uuid || table_uuid.value().empty()) {
        return Status::Invalid(
            "a commit lease requires the table UUID; pass Table::CatalogUuid() to WithTableId()");
    }
    if (commit_user.empty()) {
        return Status::Invalid("a commit lease requires a commit user");
    }
    if (acquire_timeout_millis < 0) {
        return Status::Invalid("the commit lock acquisition timeout cannot be negative");
    }
    return std::unique_ptr<RestCommitLock>(
        new RestCommitLock(api, identifier, table_uuid.value(), commit_user, acquire_timeout_millis,
                           std::move(clock), std::move(start_thread)));
}

Result<std::unique_ptr<CommitLockScope>> RestCommitLock::Acquire() {
    const int64_t started_at = clock_();
    bool waiting = false;
    while (true) {
        const int64_t requested_at = clock_();
        Result<CommitLockResponse> response =
            api_->AcquireCommitLock(identifier_, table_uuid_, commit_user_);
        if (!response.ok()) {
            // Any error, including the rejection from a server without the lease protocol, fails
            // the acquisition instead of letting the commit proceed unprotected.
            const Status& status = response.status();
            return status.WithMessage(fmt::format("failed to acquire the commit lock of {}: {}",
                                                  identifier_.ToString(), status.message()));
        }
        const CommitLockResponse& grant = response.value();
        if (grant.IsAcquired()) {
            PAIMON_RETURN_NOT_OK(ValidateGrant(grant, commit_user_));
            auto lease = std::make_unique<RestCommitLease>(
                std::make_shared<LeaseState>(api_, identifier_, table_uuid_, commit_user_, clock_,
                                             grant.GetLeaseMillis(), requested_at, logger_));
            // A grant that took longer than its lease to arrive protects nothing.
            PAIMON_RETURN_NOT_OK(lease->EnsureValid());
            Status started = lease->StartRenewal(start_thread_);
            if (!started.ok()) {
                return started.WithMessage(
                    fmt::format("failed to start renewing the commit lease of {}: {}",
                                identifier_.ToString(), started.message()));
            }
            PAIMON_LOG_DEBUG(logger_, "Acquired the commit lease of %s for %ld ms",
                             identifier_.ToString().c_str(), grant.GetLeaseMillis());
            return std::unique_ptr<CommitLockScope>(std::move(lease));
        }
        const int64_t waited = clock_() - started_at;
        if (waited >= acquire_timeout_millis_) {
            return Status::IOError(fmt::format(
                "timed out acquiring the commit lock of {} after {} ms, another writer holds it",
                identifier_.ToString(), waited));
        }
        if (!waiting) {
            PAIMON_LOG_INFO(logger_, "Another writer holds the commit lease of %s, waiting for it",
                            identifier_.ToString().c_str());
            waiting = true;
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(std::min(kMaxPollIntervalMillis, acquire_timeout_millis_)));
    }
}

}  // namespace paimon
