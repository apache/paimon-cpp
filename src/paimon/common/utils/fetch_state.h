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

#include <condition_variable>
#include <functional>
#include <mutex>
#include <utility>
#include <vector>

#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {

/// The completion state of one asynchronous fetch, shared by the fetch itself
/// and by every reader waiting for it.
///
/// This replaces a promise/shared_future pair when the waiters must not block a
/// thread: `OnComplete()` attaches a continuation instead of waiting for the
/// fetch, so a caller whose own interface is asynchronous returns right away.
///
/// The waiters are invoked OUTSIDE the internal lock, so a waiter may read the
/// cache owning this state again without deadlocking. That also means a waiter
/// can still be running after `Wait()` has returned - `Complete()` resolves the
/// state and only then runs the waiters - so whatever a waiter touches must be
/// kept alive by what it captures, never by the owner of this state.
///
/// A state that has already been resolved runs the continuation inline, which
/// keeps a stream whose `ReadAsync` completes inline - the local filesystem -
/// behaving exactly as a synchronous read would.
class PAIMON_EXPORT FetchState {
 public:
    FetchState() = default;

    // The mutex and the condition variable are not movable, and a state is
    // always held through a shared_ptr by the fetch and its waiters anyway.
    FetchState(const FetchState&) = delete;
    FetchState& operator=(const FetchState&) = delete;
    FetchState(FetchState&&) = delete;
    FetchState& operator=(FetchState&&) = delete;

    /// Resolve the fetch with the outcome of its IO and run the continuations
    /// attached so far. Only the first call has an effect: a fetch is dispatched
    /// once, and a stream resolving it twice must not run the waiters twice.
    /// @param status Outcome of the fetch, handed to every waiter.
    void Complete(Status status) {
        std::vector<std::function<void(Status)>> waiters;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (done_) {
                return;
            }
            status_ = std::move(status);
            done_ = true;
            waiters.swap(waiters_);
            cv_.notify_all();
        }
        // Outside the lock: a waiter may read the cache owning this state again.
        for (auto& waiter : waiters) {
            waiter(status_);
        }
    }

    /// Block until the fetch is resolved and return its outcome. For the callers
    /// that need the bytes right away, and for the owners that must not let a
    /// fetch outlive the buffer it writes into.
    Status Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return done_; });
        return status_;
    }

    /// Run `callback` with the outcome of the fetch: inline when it has already
    /// been resolved, from the thread resolving it otherwise. The callback runs
    /// exactly once, and never while the internal lock is held.
    void OnComplete(std::function<void(Status)> callback) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!done_) {
                waiters_.push_back(std::move(callback));
                return;
            }
        }
        callback(status_);
    }

 private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    // The outcome of the fetch, written once by Complete() before done_ is set
    // and only read afterwards, so the readers outside the lock are safe.
    Status status_;
    bool done_ = false;
    std::vector<std::function<void(Status)>> waiters_;
};

}  // namespace paimon
