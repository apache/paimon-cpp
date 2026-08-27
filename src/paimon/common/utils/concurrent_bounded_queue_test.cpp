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

#include "paimon/common/utils/concurrent_bounded_queue.h"

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <queue>
#include <utility>

#include "gtest/gtest.h"

namespace paimon::test {
namespace {

struct PluginQueueValue {
    int32_t value = 0;
};

class PluginQueueBackend : public ConcurrentBoundedQueueBackend<PluginQueueValue> {
 public:
    void SetCapacity(size_t capacity) override {
        capacity_ = capacity;
    }

    void Push(PluginQueueValue&& value) override {
        queue_.push(std::move(value));
    }

    bool TryPop(PluginQueueValue& value) override {
        if (queue_.empty()) {
            return false;
        }
        value = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    bool Empty() const override {
        return queue_.empty();
    }

    size_t Capacity() const {
        return capacity_;
    }

 private:
    std::queue<PluginQueueValue> queue_;
    size_t capacity_ = 0;
};

int32_t plugin_queue_backend_create_count = 0;
const bool plugin_queue_backend_registered =
    ConcurrentBackendFactory<ConcurrentBoundedQueueBackend<PluginQueueValue>>::Register([]() {
        ++plugin_queue_backend_create_count;
        return std::make_unique<PluginQueueBackend>();
    });

}  // namespace

TEST(ConcurrentBoundedQueueTest, TestPushAndTryPop) {
    ConcurrentBoundedQueue<int32_t> queue;
    queue.SetCapacity(2);
    ASSERT_TRUE(queue.Empty());

    queue.Push(1);
    queue.Push(2);
    ASSERT_FALSE(queue.Empty());

    int32_t value = 0;
    ASSERT_TRUE(queue.TryPop(value));
    ASSERT_EQ(value, 1);
    ASSERT_TRUE(queue.TryPop(value));
    ASSERT_EQ(value, 2);
    ASSERT_FALSE(queue.TryPop(value));
    ASSERT_TRUE(queue.Empty());
}

TEST(ConcurrentBoundedQueueTest, TestPushWaitsForCapacity) {
    ConcurrentBoundedQueue<int32_t> queue;
    queue.SetCapacity(1);
    queue.Push(1);

    std::future<void> push_future = std::async(std::launch::async, [&queue]() { queue.Push(2); });
    std::future_status initial_status = push_future.wait_for(std::chrono::milliseconds(50));

    int32_t value = 0;
    ASSERT_TRUE(queue.TryPop(value));
    ASSERT_EQ(value, 1);
    ASSERT_EQ(initial_status, std::future_status::timeout);
    ASSERT_EQ(push_future.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    ASSERT_TRUE(queue.TryPop(value));
    ASSERT_EQ(value, 2);
}

TEST(ConcurrentBoundedQueueTest, TestStaticallyRegisteredBackendSelection) {
    ASSERT_TRUE(plugin_queue_backend_registered);
    ConcurrentBoundedQueue<PluginQueueValue> queue;
    queue.SetCapacity(1);
    queue.Push(PluginQueueValue{42});

    PluginQueueValue value;
    ASSERT_TRUE(queue.TryPop(value));
    ASSERT_EQ(value.value, 42);
#ifdef PAIMON_USE_TBB
    ASSERT_EQ(plugin_queue_backend_create_count, 0);
#else
    ASSERT_EQ(plugin_queue_backend_create_count, 1);
#endif
}

}  // namespace paimon::test
