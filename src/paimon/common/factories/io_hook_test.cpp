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

#include "paimon/common/factories/io_hook.h"

#include <atomic>
#include <stdexcept>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(IOHookTest, TestReturnErrorMode) {
    auto hook = IOHook::GetInstance();
    hook->Reset(0, IOHook::Mode::RETURN_ERROR);
    ASSERT_NOK(hook->Try("path"));
    ASSERT_NOK(hook->Try("path"));
    ASSERT_EQ(2, hook->IOCount());
    hook->Clear();
}

TEST(IOHookTest, TestSilentMode) {
    auto hook = IOHook::GetInstance();
    hook->Reset(0, IOHook::Mode::SILENT);
    ASSERT_OK(hook->Try("path"));
    ASSERT_OK(hook->Try("path"));
    ASSERT_EQ(2, hook->IOCount());
    hook->Clear();
}

TEST(IOHookTest, TestSingleton) {
    auto hook = IOHook::GetInstance();
    auto hook2 = IOHook::GetInstance();
    ASSERT_EQ(hook, hook2);
    hook->Clear();
}

TEST(IOHookTest, TestThrowExceptionMode) {
    auto hook = IOHook::GetInstance();
    hook->Reset(0, IOHook::Mode::THROW_EXCEPTION);
    auto Try = [hook]() {
        auto s = hook->Try("path");
        (void)s;
    };
    EXPECT_THROW(Try(), std::runtime_error);
    EXPECT_THROW(Try(), std::runtime_error);
    ASSERT_EQ(2, hook->IOCount());
    hook->Clear();
}

// Regression test for the data race on IOHook's mode: Reset()/Clear() run on one
// thread while other threads call Try() concurrently. Under a ThreadSanitizer build
// this deterministically reports the unsynchronized mode access; functionally it must
// never crash and every Try() must return OK.
TEST(IOHookTest, TestConcurrentResetAndTry) {
    auto hook = IOHook::GetInstance();

    constexpr int32_t kResetIterations = 200000;
    constexpr int32_t kTryIterations = 50000;
    constexpr int32_t kNumWorkers = 4;

    std::atomic<bool> observed_error{false};

    std::thread reset_thread([hook]() {
        for (int32_t i = 0; i < kResetIterations; i++) {
            hook->Reset(INT64_MAX, IOHook::Mode::RETURN_ERROR);
            hook->Clear();
        }
    });

    std::vector<std::thread> workers;
    workers.reserve(kNumWorkers);
    for (int32_t t = 0; t < kNumWorkers; t++) {
        workers.emplace_back([hook, &observed_error]() {
            for (int32_t i = 0; i < kTryIterations; i++) {
                Status status = hook->Try("concurrent_path");
                // Reset() arms an unreachable position, while Clear() uses SILENT mode,
                // so both complete states return OK. An IOError exposes a torn state.
                if (!status.ok()) {
                    observed_error.store(true, std::memory_order_relaxed);
                }
            }
        });
    }

    reset_thread.join();
    for (auto& worker : workers) {
        worker.join();
    }

    ASSERT_FALSE(observed_error.load(std::memory_order_relaxed));
    // Leave the process-wide singleton in its default SILENT state for later tests.
    hook->Clear();
}

}  // namespace paimon::test
