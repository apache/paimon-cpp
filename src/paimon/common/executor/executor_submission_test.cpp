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

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <new>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/executor.h"

namespace {
thread_local bool armed = false;
thread_local bool fired = false;
thread_local int32_t allocations = 0;
thread_local int32_t fail_at = 0;

class AllocationFault {
 public:
    explicit AllocationFault(int32_t ordinal) {
        allocations = 0;
        fail_at = ordinal;
        fired = false;
        armed = true;
    }
    ~AllocationFault() {
        armed = false;
    }
};
}  // namespace

extern "C" void* __real__Znwm(std::size_t size);
extern "C" void* __wrap__Znwm(std::size_t size) {
    if (armed && ++allocations == fail_at) {
        armed = false;
        fired = true;
        throw std::bad_alloc();
    }
    return __real__Znwm(size);
}

namespace paimon::test {
namespace {
struct SubmissionResult {
    int32_t allocations;
    bool fired;
    bool caught;
    bool recovered;
    bool released;
    int32_t runs;
};

SubmissionResult Submit(uint32_t workers, int32_t ordinal, bool recover, bool shutdown) {
    std::atomic<int32_t> runs = 0;
    std::promise<void> marker;
    auto ready = marker.get_future();
    auto executor = CreateDefaultExecutor(workers).value();
    auto owner = std::make_shared<int32_t>(0);
    std::weak_ptr<int32_t> retained = owner;
    std::function<void()> task = [&, owner]() { ++runs; };
    owner.reset();
    std::function<void()> recovery = [&]() { marker.set_value(); };
    bool caught = false;
    {
        AllocationFault fault(ordinal);
        try {
            executor->Add(std::move(task));
        } catch (const std::bad_alloc&) {
            caught = true;
        }
    }
    SubmissionResult result = {allocations, fired, caught, false, retained.expired(), 0};
    if (recover) {
        executor->Add(std::move(recovery));
        result.recovered = ready.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    }
    if (shutdown) {
        executor->ShutdownNow();
    }
    // Graceful destruction also drains callbacks on the multi-worker oracle.
    executor.reset();
    result.runs = runs.load();
    return result;
}
}  // namespace

TEST(ExecutorSubmissionTest, StartupAllocationFailuresDoNotAcceptCallback) {
    for (uint32_t workers : {1u, 4u}) {
        const auto observation = Submit(workers, 0, true, false);
        ASSERT_GT(observation.allocations, 0);
        ASSERT_FALSE(observation.fired);
        ASSERT_FALSE(observation.caught);
        ASSERT_TRUE(observation.recovered);
        ASSERT_EQ(observation.runs, 1);
        for (int32_t ordinal = 1; ordinal <= observation.allocations; ++ordinal) {
            SCOPED_TRACE(::testing::Message() << "workers=" << workers << ", ordinal=" << ordinal);
            const auto result = Submit(workers, ordinal, true, false);
            EXPECT_TRUE(result.fired);
            EXPECT_TRUE(result.caught);
            EXPECT_EQ(result.allocations, ordinal);
            EXPECT_TRUE(result.recovered);
            EXPECT_TRUE(result.released);
            EXPECT_EQ(result.runs, 0);
            for (bool shutdown : {false, true}) {
                const auto cleanup = Submit(workers, ordinal, false, shutdown);
                EXPECT_TRUE(cleanup.fired);
                EXPECT_TRUE(cleanup.caught);
                EXPECT_TRUE(cleanup.released);
                EXPECT_EQ(cleanup.runs, 0);
            }
        }
    }
}

TEST(ExecutorSubmissionTest, QueueAllocationFailurePreservesAcceptedPrefix) {
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::promise<void> marker;
    auto ready = marker.get_future();
    std::atomic<int32_t> runs = 0;
    std::array<std::function<void()>, 128> tasks;
    for (auto& task : tasks) {
        task = [&]() { ++runs; };
    }
    std::function<void()> recovery = [&]() { marker.set_value(); };
    auto executor = CreateDefaultExecutor(1).value();
    ScopeGuard release_on_error([&]() { release.set_value(); });
    executor->Add([gate]() { gate.wait(); });
    int32_t accepted = 0;
    bool caught = false;
    bool hit = false;
    for (auto& task : tasks) {
        {
            AllocationFault fault(1);
            try {
                executor->Add(std::move(task));
            } catch (const std::bad_alloc&) {
                caught = true;
            }
        }
        if (fired) {
            hit = true;
            break;
        }
        ++accepted;
    }
    release.set_value();
    release_on_error.Release();
    executor->Add(std::move(recovery));
    const bool recovered = ready.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    executor.reset();
    EXPECT_TRUE(hit);
    EXPECT_TRUE(caught);
    EXPECT_TRUE(recovered);
    EXPECT_GT(accepted, 0);
    EXPECT_EQ(runs.load(), accepted);
}
}  // namespace paimon::test
