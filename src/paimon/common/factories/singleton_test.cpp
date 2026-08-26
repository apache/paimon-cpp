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

// Local to this translation unit, so nothing else in the test binary can have
// instantiated Singleton<FirstPublicationTarget> before this test runs: the storm
// below is guaranteed to race on the *first* publication regardless of link order,
// --gtest_shuffle, or --gtest_filter. GetInstance() is defined in the header, so a
// translation-unit-local type can instantiate it.
class FirstPublicationTarget {
 public:
    FirstPublicationTarget() {
        for (size_t i = 0; i < payload_.size(); ++i) {
            payload_[i] = kMagic ^ (i * 0x9E3779B97F4A7C15ULL);
        }
    }

    // The publication race let a reader observe the instance pointer before the
    // constructor's stores were visible; this checks every word the ctor wrote.
    bool IsFullyConstructed() const {
        for (size_t i = 0; i < payload_.size(); ++i) {
            if (payload_[i] != (kMagic ^ (i * 0x9E3779B97F4A7C15ULL))) {
                return false;
            }
        }
        return true;
    }

 private:
    static constexpr uint64_t kMagic = 0xA5A5F00D12345678ULL;
    std::array<uint64_t, 64> payload_{};
};

}  // namespace

// Regression gate for the Singleton double-checked-locking publication race: 32
// threads race the first GetInstance() of a type local to this file, so the gate
// cannot silently degrade into exercising only the already-published fast path.
TEST(SingletonTest, TestConcurrentFirstPublication) {
    std::array<FirstPublicationTarget*, kNumThreads> instances{};
    std::array<bool, kNumThreads> fully_constructed{};
    RunStorm([&instances, &fully_constructed](int32_t i) {
        instances[i] = Singleton<FirstPublicationTarget>::GetInstance();
        fully_constructed[i] = instances[i]->IsFullyConstructed();
    });

    FirstPublicationTarget* expected = instances[0];
    ASSERT_NE(expected, nullptr);
    for (int32_t i = 0; i < kNumThreads; ++i) {
        ASSERT_EQ(expected, instances[i]);
        ASSERT_TRUE(fully_constructed[i]);
    }
}

}  // namespace paimon::test
