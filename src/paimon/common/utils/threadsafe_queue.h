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

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

namespace paimon {

template <typename T>
class ThreadsafeQueue {
 public:
    void push(const T& value) {
        std::unique_lock<std::mutex> lock(mtx_);
        queue_.push(value);
    }

    void push(T&& value) {
        std::unique_lock<std::mutex> lock(mtx_);
        queue_.push(std::move(value));
    }

    std::optional<T> try_pop() {
        std::unique_lock<std::mutex> lock(mtx_);
        if (queue_.empty()) {
            return std::nullopt;
        }
        T front = std::move(queue_.front());
        queue_.pop();
        return front;
    }

    const T* try_front() const {
        std::unique_lock<std::mutex> lock(mtx_);
        if (queue_.empty()) {
            return nullptr;
        }
        return &queue_.front();
    }

    size_t size() const {
        std::unique_lock<std::mutex> lock(mtx_);
        return queue_.size();
    }

    bool empty() const {
        std::unique_lock<std::mutex> lock(mtx_);
        return queue_.empty();
    }

 private:
    mutable std::queue<T> queue_;
    mutable std::mutex mtx_;
};

}  // namespace paimon
