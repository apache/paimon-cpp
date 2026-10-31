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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/core/catalog/commit_lock.h"
#include "paimon/logging.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class RestApi;

/// A renewable REST commit lease bound to a table or branch and to one writer, mirroring Java
/// `RESTCatalogLock`.
///
/// `Acquire()` asks the server for a lease and polls while another writer holds one, until the
/// acquisition timeout elapses. The timeout is checked after each response, so it does not
/// interrupt a request in flight. The scope it returns renews the lease in the background every
/// third of the lease duration, on a thread of its own; when that thread cannot be started,
/// `Acquire()` fails and may be called again. The scope tracks the deadline on a monotonic clock
/// from when each grant was requested, so `EnsureValid()` fails once a renewal is refused or
/// fails, or the deadline passes even if no renewal ran.
///
/// There is no unlock request. The server ends a lease when a snapshot is published under it;
/// ending a scope only stops renewing, so a lease that published nothing stays held until it
/// expires. Ending a scope does not wait for a renewal in flight: its request is cancelled, and
/// its thread exits on its own once the call returns.
class RestCommitLock : public CommitLock {
 public:
    /// Monotonic time in milliseconds.
    using Clock = std::function<int64_t()>;

    /// Runs a task on a new detached thread, failing instead of throwing when none can be started.
    using ThreadStarter = std::function<Status(std::function<void()> task)>;

    /// Milliseconds of `std::chrono::steady_clock`.
    static int64_t SteadyClockMillis();

    /// Runs `task` on a detached `std::thread`.
    static Status StartDetachedThread(std::function<void()> task);

    /// Creates the lock without sending any request.
    ///
    /// @param api Client of the REST server holding the leases.
    /// @param identifier Table or branch to lock, as the server addresses it.
    /// @param table_uuid Catalog UUID of the table or branch, so a recreated table cannot inherit
    /// a lease; required.
    /// @param commit_user Exact commit user of the snapshots published under the lease; required.
    /// @param acquire_timeout_millis How long `Acquire()` waits while another writer holds the
    /// lease.
    /// @param clock Monotonic clock, replaceable for tests.
    /// @param start_thread Starts the thread renewing each lease, replaceable for tests.
    static Result<std::unique_ptr<RestCommitLock>> Create(
        const std::shared_ptr<RestApi>& api, const Identifier& identifier,
        const std::optional<std::string>& table_uuid, const std::string& commit_user,
        int64_t acquire_timeout_millis, Clock clock = &RestCommitLock::SteadyClockMillis,
        ThreadStarter start_thread = &RestCommitLock::StartDetachedThread);

    Result<std::unique_ptr<CommitLockScope>> Acquire() override;

 private:
    RestCommitLock(const std::shared_ptr<RestApi>& api, const Identifier& identifier,
                   const std::string& table_uuid, const std::string& commit_user,
                   int64_t acquire_timeout_millis, Clock clock, ThreadStarter start_thread);

    std::shared_ptr<RestApi> api_;
    Identifier identifier_;
    std::string table_uuid_;
    std::string commit_user_;
    int64_t acquire_timeout_millis_;
    Clock clock_;
    ThreadStarter start_thread_;
    std::shared_ptr<Logger> logger_;
};

}  // namespace paimon
