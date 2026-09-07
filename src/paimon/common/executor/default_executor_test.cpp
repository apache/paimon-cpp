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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#ifdef __linux__
#include <dirent.h>
#endif

#include "gtest/gtest.h"
#include "paimon/common/executor/future.h"
#include "paimon/executor.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

#ifdef __linux__
// Number of threads of the current process according to /proc.
int32_t CountProcessThreads() {
    DIR* dir = opendir("/proc/self/task");
    if (dir == nullptr) {
        return -1;
    }
    int32_t count = 0;
    while (struct dirent* entry = readdir(dir)) {
        if (entry->d_name[0] != '.') {
            ++count;
        }
    }
    closedir(dir);
    return count;
}
#endif

TEST(DefaultExecutorTest, TestWorkersStartOnFirstTask) {
#ifdef __linux__
    int32_t threads_before = CountProcessThreads();
    ASSERT_GT(threads_before, 0);
#endif
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Executor> executor, CreateDefaultExecutor(4));
    ASSERT_EQ(4u, executor->GetThreadNum());
#ifdef __linux__
    // Constructing the executor does not spawn any worker thread.
    ASSERT_EQ(threads_before, CountProcessThreads());
#endif

    std::atomic<int64_t> sum = {0};
    std::vector<std::future<void>> futures;
    for (int32_t index = 0; index < 8; ++index) {
        futures.push_back(Via(executor.get(), [&sum]() { sum++; }));
    }
    Wait(futures);
    ASSERT_EQ(8, sum.load());
#ifdef __linux__
    ASSERT_EQ(threads_before + 4, CountProcessThreads());
#endif
    executor.reset();
#ifdef __linux__
    // A joined worker may trail in /proc for a moment, so poll briefly.
    int32_t threads_after = CountProcessThreads();
    for (int32_t i = 0; i < 100 && threads_after != threads_before; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        threads_after = CountProcessThreads();
    }
    ASSERT_EQ(threads_before, threads_after);
#endif
}

TEST(DefaultExecutorTest, TestShutdownWithoutTasks) {
    // Shutting down or destroying an executor that never ran a task must not
    // block or touch workers that were never started.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Executor> executor, CreateDefaultExecutor(2));
    executor->ShutdownNow();
    std::atomic<bool> ran = {false};
    executor->Add([&ran]() { ran = true; });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_FALSE(ran.load());
    executor.reset();
    std::unique_ptr<Executor> idle_executor = CreateDefaultExecutor();
    idle_executor.reset();
}

TEST(DefaultExecutorTest, TestViaVoidFunc) {
    auto executor = GetGlobalDefaultExecutor();
    std::atomic<int64_t> sum = {0};
    std::vector<std::future<void>> futures;
    for (int32_t index = 0; index < 10; ++index) {
        futures.push_back(Via(executor.get(), [&sum]() { sum++; }));
    }
    Wait(futures);
    ASSERT_EQ(10, sum.load());
}

TEST(DefaultExecutorTest, TestVia) {
    auto executor = GetGlobalDefaultExecutor();
    std::atomic<int64_t> sum = {0};
    std::vector<std::future<int>> futures;
    for (int32_t index = 0; index < 10; ++index) {
        futures.push_back(Via(executor.get(), [index, &sum]() -> int32_t {
            sum++;
            return index * 2;
        }));
    }
    auto results = CollectAll(futures);
    ASSERT_EQ(10, results.size());
    std::vector<int> expected = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18};
    ASSERT_EQ(expected, results);
    ASSERT_EQ(10, sum.load());
}

TEST(DefaultExecutorTest, TestViaWithResult) {
    auto executor = GetGlobalDefaultExecutor();
    std::vector<std::future<Result<std::vector<int32_t>>>> futures;
    std::vector<int32_t> inputs = {-2, -1, 1, 2};
    for (const auto& input : inputs) {
        futures.push_back(Via(executor.get(), [input]() -> Result<std::vector<int32_t>> {
            if (input > 0) {
                std::vector<int32_t> output = {-2, -1, 1, 2};
                return output;
            }
            return Status::Invalid("negative");
        }));
    }
    auto results = CollectAll(futures);
    ASSERT_EQ(4, results.size());
}

TEST(DefaultExecutorTest, TestViaWithException) {
    auto executor = GetGlobalDefaultExecutor();
    auto future = Via(executor.get(), []() { throw std::runtime_error("test"); });
    ASSERT_THROW(future.get(), std::runtime_error);
}

TEST(DefaultExecutorTest, TestShutdownNowDropsPendingTasks) {
    ASSERT_OK_AND_ASSIGN(auto executor, CreateDefaultExecutor(/*thread_count=*/1));
    std::atomic<bool> first_started = false;
    std::atomic<int32_t> executed_count = 0;
    std::promise<void> release_first_task;
    auto release_future = release_first_task.get_future();
    executor->Add([&]() {
        first_started.store(true);
        release_future.wait();
        ++executed_count;
    });

    for (int32_t index = 0; index < 20; ++index) {
        executor->Add([&]() { ++executed_count; });
    }

    for (int32_t retry = 0; retry < 100 && !first_started.load(); ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_TRUE(first_started.load());
    std::thread shutdown_thread([&]() { executor->ShutdownNow(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    release_first_task.set_value();
    shutdown_thread.join();

    ASSERT_EQ(executed_count.load(), 1);
}

TEST(DefaultExecutorTest, TestAddTaskAfterShutdownNowIgnored) {
    ASSERT_OK_AND_ASSIGN(auto executor, CreateDefaultExecutor(/*thread_count=*/1));
    std::atomic<int32_t> executed_count = 0;

    executor->ShutdownNow();
    executor->Add([&]() { ++executed_count; });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    ASSERT_EQ(executed_count.load(), 0);
}

TEST(DefaultExecutorTest, TestConcurrentShutdownNow) {
    constexpr int32_t kShutdownThreadCount = 2;
    constexpr int32_t kAttempts = 50;
    for (int32_t attempt = 0; attempt < kAttempts; ++attempt) {
        ASSERT_OK_AND_ASSIGN(auto executor, CreateDefaultExecutor(/*thread_count=*/4));
        std::atomic<int32_t> ready_shutdown_count = 0;
        std::promise<void> start_signal;
        std::shared_future<void> start_future = start_signal.get_future().share();
        std::vector<std::thread> shutdown_threads;
        shutdown_threads.reserve(kShutdownThreadCount);

        for (int32_t thread_index = 0; thread_index < kShutdownThreadCount; ++thread_index) {
            shutdown_threads.emplace_back([&]() {
                ++ready_shutdown_count;
                start_future.wait();
                executor->ShutdownNow();
            });
        }
        for (int32_t retry = 0; retry < 100 && ready_shutdown_count.load() < kShutdownThreadCount;
             ++retry) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        const int32_t ready_count_before_start = ready_shutdown_count.load();
        start_signal.set_value();
        for (std::thread& shutdown_thread : shutdown_threads) {
            shutdown_thread.join();
        }
        ASSERT_EQ(kShutdownThreadCount, ready_count_before_start);
    }
}

TEST(DefaultExecutorTest, TestDestroyFromWorkerThread) {
    std::unique_ptr<Executor> created = CreateDefaultExecutor();
    std::shared_ptr<Executor> executor(std::move(created));
    std::shared_ptr<Executor> task_executor = executor;
    auto release = std::make_shared<std::promise<void>>();
    std::shared_future<void> release_future = release->get_future().share();
    auto destroyed = std::make_shared<std::promise<void>>();
    std::future<void> future = destroyed->get_future();

    executor->Add([executor = std::move(task_executor), release_future, destroyed]() mutable {
        release_future.wait();
        executor.reset();
        destroyed->set_value();
    });

    executor.reset();
    release->set_value();
    ASSERT_EQ(std::future_status::ready, future.wait_for(std::chrono::seconds(5)));
}

TEST(DefaultExecutorTest, TestAddTaskFromMultipleThreads) {
    ASSERT_OK_AND_ASSIGN(auto executor, CreateDefaultExecutor(/*thread_count=*/4));

    constexpr int32_t kSubmitterCount = 8;
    constexpr int32_t kTaskCountPerSubmitter = 64;
    constexpr int32_t kTotalTaskCount = kSubmitterCount * kTaskCountPerSubmitter;

    std::vector<std::atomic<int32_t>> executed_slots(kTotalTaskCount);
    for (auto& executed_slot : executed_slots) {
        executed_slot.store(0);
    }
    std::vector<std::promise<void>> task_promises(kTotalTaskCount);
    std::vector<std::future<void>> task_futures;
    task_futures.reserve(kTotalTaskCount);
    for (auto& task_promise : task_promises) {
        task_futures.push_back(task_promise.get_future());
    }
    std::atomic<int32_t> ready_submitter_count = 0;
    std::atomic<int32_t> executed_count = 0;
    std::promise<void> start_signal;
    std::shared_future<void> start_future = start_signal.get_future().share();
    std::vector<std::thread> submitters;
    submitters.reserve(kSubmitterCount);

    for (int32_t submitter_index = 0; submitter_index < kSubmitterCount; ++submitter_index) {
        submitters.emplace_back([&, submitter_index]() {
            ++ready_submitter_count;
            start_future.wait();
            for (int32_t task_index = 0; task_index < kTaskCountPerSubmitter; ++task_index) {
                const int32_t slot_index = submitter_index * kTaskCountPerSubmitter + task_index;
                executor->Add([&, slot_index]() {
                    ++executed_slots[slot_index];
                    ++executed_count;
                    task_promises[slot_index].set_value();
                });
            }
        });
    }

    for (int32_t retry = 0; retry < 100 && ready_submitter_count.load() < kSubmitterCount;
         ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    const int32_t ready_submitter_count_before_start = ready_submitter_count.load();
    start_signal.set_value();
    for (auto& submitter : submitters) {
        submitter.join();
    }
    ASSERT_EQ(kSubmitterCount, ready_submitter_count_before_start);
    Wait(task_futures);

    ASSERT_EQ(kTotalTaskCount, executed_count.load());
    for (const auto& executed_slot : executed_slots) {
        ASSERT_EQ(1, executed_slot.load());
    }
}

TEST(DefaultExecutorTest, TestCreateWithZeroThreadCount) {
    ASSERT_NOK_WITH_MSG(CreateDefaultExecutor(/*thread_count=*/0),
                        "default executor thread count should be greater than 0");
}

}  // namespace paimon::test
