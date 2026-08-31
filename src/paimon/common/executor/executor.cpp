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

#include "paimon/executor.h"

#include <cassert>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

namespace paimon {

class DefaultExecutor : public Executor {
 public:
    explicit DefaultExecutor(uint32_t thread_count);
    ~DefaultExecutor() override;

    void Add(std::function<void()> func) override;
    void ShutdownNow() override;
    uint32_t GetThreadNum() const override;

 private:
    struct State {
        std::queue<std::function<void()>> tasks;
        std::mutex mutex;
        std::condition_variable condition;
        bool stop = false;
    };

    static void WorkerThread(std::shared_ptr<State> state);
    void ShutdownInternal(bool wait_for_pending_tasks);

 private:
    uint32_t thread_count_;
    std::vector<std::thread> workers_;
    std::shared_ptr<State> state_ = std::make_shared<State>();
};

DefaultExecutor::DefaultExecutor(uint32_t thread_count) : thread_count_(thread_count) {
    assert(thread_count > 0);
    for (uint32_t i = 0; i < thread_count_; ++i) {
        workers_.emplace_back(&DefaultExecutor::WorkerThread, state_);
    }
}

uint32_t DefaultExecutor::GetThreadNum() const {
    return thread_count_;
}

void DefaultExecutor::ShutdownInternal(bool wait_for_pending_tasks) {
    {
        std::unique_lock<std::mutex> lock(state_->mutex);
        if (state_->stop) {
            return;
        }
        state_->stop = true;
        if (!wait_for_pending_tasks) {
            // Discard all pending tasks immediately.
            std::queue<std::function<void()>> empty;
            state_->tasks.swap(empty);
        }
        state_->condition.notify_all();
    }
    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            if (worker.get_id() == std::this_thread::get_id()) {
                worker.detach();
            } else {
                worker.join();
            }
        }
    }
}

DefaultExecutor::~DefaultExecutor() {
    // Graceful shutdown: wait for all pending tasks to complete.
    ShutdownInternal(/*wait_for_pending_tasks=*/true);
}

void DefaultExecutor::ShutdownNow() {
    // Immediate shutdown: discard all pending tasks.
    ShutdownInternal(/*wait_for_pending_tasks=*/false);
}

void DefaultExecutor::Add(std::function<void()> func) {
    if (!func) {
        return;
    }
    {
        std::unique_lock<std::mutex> lock(state_->mutex);
        if (state_->stop) {
            return;
        }
        state_->tasks.emplace(std::move(func));
    }
    state_->condition.notify_one();
}

void DefaultExecutor::WorkerThread(std::shared_ptr<State> state) {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            state->condition.wait(lock, [&state] { return state->stop || !state->tasks.empty(); });
            if (state->stop && state->tasks.empty()) {
                state->condition.notify_all();
                return;
            }
            if (!state->tasks.empty()) {
                task = std::move(state->tasks.front());
                state->tasks.pop();
            }
        }
        if (task) {
            task();
        }
    }
}

PAIMON_EXPORT std::shared_ptr<Executor> GetGlobalDefaultExecutor() {
    static uint32_t all_cores = std::thread::hardware_concurrency();
    static std::shared_ptr<Executor> internal =
        std::make_shared<DefaultExecutor>(/*thread_count=*/all_cores > 0 ? all_cores : 1);
    return internal;
}

PAIMON_EXPORT std::unique_ptr<Executor> CreateDefaultExecutor() {
    return std::make_unique<DefaultExecutor>(DEFAULT_EXECUTOR_THREAD_COUNT);
}

PAIMON_EXPORT Result<std::unique_ptr<Executor>> CreateDefaultExecutor(uint32_t thread_count) {
    if (thread_count == 0) {
        return Status::Invalid("default executor thread count should be greater than 0");
    }
    return std::make_unique<DefaultExecutor>(thread_count);
}

}  // namespace paimon
