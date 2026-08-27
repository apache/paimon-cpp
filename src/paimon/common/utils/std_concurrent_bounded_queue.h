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
#include <cstddef>
#include <limits>
#include <mutex>
#include <queue>
#include <utility>

namespace paimon {

template <typename T>
class ConcurrentBoundedQueue {
 public:
    using size_type = std::ptrdiff_t;
    using value_type = T;
    using reference = T&;
    using const_reference = const T&;

    ConcurrentBoundedQueue() = default;
    ~ConcurrentBoundedQueue() = default;

    ConcurrentBoundedQueue(const ConcurrentBoundedQueue&) = delete;
    ConcurrentBoundedQueue& operator=(const ConcurrentBoundedQueue&) = delete;
    ConcurrentBoundedQueue(ConcurrentBoundedQueue&&) = delete;
    ConcurrentBoundedQueue& operator=(ConcurrentBoundedQueue&&) = delete;

    void set_capacity(size_type capacity) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            capacity_ =
                capacity < 0 ? std::numeric_limits<size_t>::max() : static_cast<size_t>(capacity);
        }
        capacity_available_.notify_all();
    }

    void push(const T& value) {
        T copied_value = value;
        push(std::move(copied_value));
    }

    void push(T&& value) {
        std::unique_lock<std::mutex> lock(mutex_);
        capacity_available_.wait(lock, [this]() { return queue_.size() < capacity_; });
        queue_.push(std::move(value));
    }

    bool try_pop(T& value) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (queue_.empty()) {
                return false;
            }
            value = std::move(queue_.front());
            queue_.pop();
        }
        capacity_available_.notify_one();
        return true;
    }

    bool empty() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.empty();
    }

 private:
    std::queue<T> queue_;
    size_t capacity_ = std::numeric_limits<size_t>::max();
    mutable std::mutex mutex_;
    std::condition_variable capacity_available_;
};

}  // namespace paimon
