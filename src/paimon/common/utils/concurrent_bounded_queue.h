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

#include <cstddef>
#include <memory>
#include <utility>

#include "paimon/common/utils/concurrent_backend_factory.h"
#ifdef PAIMON_USE_TBB
#include "tbb/concurrent_queue.h"
#else
#include <condition_variable>
#include <limits>
#include <mutex>
#include <queue>
#endif

namespace paimon {

template <typename T>
class ConcurrentBoundedQueueBackend {
 public:
    virtual ~ConcurrentBoundedQueueBackend() = default;

    virtual void SetCapacity(size_t capacity) = 0;
    virtual void Push(T&& value) = 0;
    virtual bool TryPop(T& value) = 0;
    virtual bool Empty() const = 0;
};

#ifndef PAIMON_USE_TBB
namespace detail {

template <typename T>
class StdConcurrentBoundedQueueBackend : public ConcurrentBoundedQueueBackend<T> {
 public:
    void SetCapacity(size_t capacity) override {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            capacity_ = capacity;
        }
        capacity_available_.notify_all();
    }

    void Push(T&& value) override {
        std::unique_lock<std::mutex> lock(mutex_);
        capacity_available_.wait(lock, [this]() { return queue_.size() < capacity_; });
        queue_.push(std::move(value));
    }

    bool TryPop(T& value) override {
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

    bool Empty() const override {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.empty();
    }

 private:
    std::queue<T> queue_;
    size_t capacity_ = std::numeric_limits<size_t>::max();
    mutable std::mutex mutex_;
    std::condition_variable capacity_available_;
};

}  // namespace detail
#endif

template <typename T>
class ConcurrentBoundedQueue {
 public:
#ifdef PAIMON_USE_TBB
    ConcurrentBoundedQueue() = default;
#else
    ConcurrentBoundedQueue()
        : backend_(ConcurrentBackendFactory<ConcurrentBoundedQueueBackend<T> >::Create()) {
        if (backend_ == nullptr) {
            backend_ = std::make_unique<detail::StdConcurrentBoundedQueueBackend<T> >();
        }
    }
#endif
    ~ConcurrentBoundedQueue() = default;

    ConcurrentBoundedQueue(const ConcurrentBoundedQueue&) = delete;
    ConcurrentBoundedQueue& operator=(const ConcurrentBoundedQueue&) = delete;
    ConcurrentBoundedQueue(ConcurrentBoundedQueue&&) = delete;
    ConcurrentBoundedQueue& operator=(ConcurrentBoundedQueue&&) = delete;

    void SetCapacity(size_t capacity) {
#ifdef PAIMON_USE_TBB
        queue_.set_capacity(static_cast<std::ptrdiff_t>(capacity));
#else
        backend_->SetCapacity(capacity);
#endif
    }

    void Push(const T& value) {
#ifdef PAIMON_USE_TBB
        queue_.push(value);
#else
        T copied_value = value;
        backend_->Push(std::move(copied_value));
#endif
    }

    void Push(T&& value) {
#ifdef PAIMON_USE_TBB
        queue_.push(std::move(value));
#else
        backend_->Push(std::move(value));
#endif
    }

    bool TryPop(T& value) {
#ifdef PAIMON_USE_TBB
        return queue_.try_pop(value);
#else
        return backend_->TryPop(value);
#endif
    }

    bool Empty() const {
#ifdef PAIMON_USE_TBB
        return queue_.empty();
#else
        return backend_->Empty();
#endif
    }

 private:
#ifdef PAIMON_USE_TBB
    tbb::concurrent_bounded_queue<T> queue_;
#else
    std::unique_ptr<ConcurrentBoundedQueueBackend<T> > backend_;
#endif
};

}  // namespace paimon
