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

#include "paimon/factories/singleton.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"

namespace paimon::test {

namespace {

constexpr int32_t kNumThreads = 32;

// Runs `worker(i)` on kNumThreads threads that are all blocked on a shared start
// flag and released at (nearly) the same time, so that they race on the first
// Singleton::GetInstance() publication. Joins all threads before returning.
template <typename Worker>
void RunStorm(const Worker& worker) {
    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (int32_t i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([&start, &worker, i]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            worker(i);
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }
}

}  // namespace

// Regression gate for the Singleton double-checked-locking publication race.
// It only exercises the first construction if nothing has touched
// Singleton<IOHook> before, so this must stay the first GetInstance() call in
// this binary (singleton_test.cpp is the first source of common_factories_test
// and this is its first test). A FactoryCreator storm cannot serve as the
// gate: the REGISTER_PAIMON_FACTORY constructors in paimon_shared already
// initialize Singleton<FactoryCreator> before main().
TEST(SingletonTest, TestConcurrentIOHookGetInstance) {
    std::array<IOHook*, kNumThreads> hooks{};
    std::array<bool, kNumThreads> try_oks{};
    RunStorm([&hooks, &try_oks](int32_t i) {
        hooks[i] = Singleton<IOHook>::GetInstance();
        // The default (and cleared) IOHook state is SILENT, so Try() must succeed.
        try_oks[i] = hooks[i]->Try("singleton_storm_path").ok();
    });

    IOHook* expected = hooks[0];
    ASSERT_NE(expected, nullptr);
    for (int32_t i = 0; i < kNumThreads; ++i) {
        ASSERT_EQ(expected, hooks[i]);
        ASSERT_TRUE(try_oks[i]);
    }
    ASSERT_GE(expected->IOCount(), kNumThreads);
    // Leave the process-wide singleton in its default SILENT state for later tests.
    expected->Clear();
}

}  // namespace paimon::test
