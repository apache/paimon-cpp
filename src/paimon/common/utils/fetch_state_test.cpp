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

#include "paimon/common/utils/fetch_state.h"

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// A continuation attached to an unresolved fetch runs when the fetch is resolved,
// with the outcome the fetch was resolved with.
TEST(TestFetchState, TestOnCompleteRunsWhenTheFetchIsResolved) {
    auto state = std::make_shared<FetchState>();
    std::vector<Status> observed;
    state->OnComplete([&observed](Status status) { observed.push_back(std::move(status)); });

    ASSERT_TRUE(observed.empty());
    state->Complete(Status::IOError("fetch failed"));
    ASSERT_EQ(observed.size(), 1u);
    ASSERT_TRUE(observed[0].IsIOError());
    ASSERT_EQ("IOError: fetch failed", observed[0].ToString());
}

// A fetch that has already been resolved runs the continuation inline, which is
// what keeps a stream whose ReadAsync completes inline - the local filesystem -
// behaving exactly as a synchronous read would.
TEST(TestFetchState, TestOnCompleteRunsInlineOnceResolved) {
    auto state = std::make_shared<FetchState>();
    state->Complete(Status::OK());

    bool called = false;
    state->OnComplete([&called](Status status) {
        ASSERT_TRUE(status.ok());
        called = true;
    });
    ASSERT_TRUE(called);
}

// Every reader of one fetch is a waiter of it, and a stream resolving the fetch
// twice must not run them twice.
TEST(TestFetchState, TestEveryWaiterRunsOnce) {
    auto state = std::make_shared<FetchState>();
    int32_t first = 0;
    int32_t second = 0;
    state->OnComplete([&first](Status status) {
        ASSERT_TRUE(status.ok());
        first++;
    });
    state->OnComplete([&second](Status status) {
        ASSERT_TRUE(status.ok());
        second++;
    });

    state->Complete(Status::OK());
    // Only the first resolution has an effect.
    state->Complete(Status::IOError("late"));
    ASSERT_EQ(first, 1);
    ASSERT_EQ(second, 1);
    // A waiter attaching after the second resolution still sees the first one.
    Status observed = Status::IOError("unset");
    state->OnComplete([&observed](Status status) { observed = std::move(status); });
    ASSERT_OK(observed);
}

// The blocking side of the same fetch returns the outcome the waiters got.
TEST(TestFetchState, TestWaitReturnsTheOutcomeOfTheFetch) {
    auto state = std::make_shared<FetchState>();
    std::thread completer([state]() { state->Complete(Status::IOError("fetch failed")); });
    const Status waited = state->Wait();
    completer.join();

    ASSERT_TRUE(waited.IsIOError());
    ASSERT_EQ("IOError: fetch failed", waited.ToString());
    // A resolved state is waited for without blocking.
    ASSERT_TRUE(waited.Equals(state->Wait()));
}

// The waiters run outside the internal lock, so a waiter may touch the state it
// waits for again instead of deadlocking against itself.
TEST(TestFetchState, TestWaiterMayReadTheStateAgain) {
    auto state = std::make_shared<FetchState>();
    bool reattached = false;
    state->OnComplete([state, &reattached](Status status) {
        ASSERT_TRUE(status.ok());
        // Blocking on the state that is currently resolving: the resolution is
        // already published, so this returns instead of waiting for itself.
        ASSERT_OK(state->Wait());
        state->OnComplete([&reattached](Status inner) {
            ASSERT_TRUE(inner.ok());
            reattached = true;
        });
    });

    state->Complete(Status::OK());
    ASSERT_TRUE(reattached);
}

}  // namespace paimon::test
