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

#include "gtest/gtest.h"

namespace paimon::test {

TEST(ConcurrentBoundedQueueTest, TestPushAndTryPop) {
    ConcurrentBoundedQueue<int32_t> queue;
    queue.set_capacity(2);
    ASSERT_TRUE(queue.empty());

    queue.push(1);
    queue.push(2);
    ASSERT_FALSE(queue.empty());

    int32_t value = 0;
    ASSERT_TRUE(queue.try_pop(value));
    ASSERT_EQ(value, 1);
    ASSERT_TRUE(queue.try_pop(value));
    ASSERT_EQ(value, 2);
    ASSERT_FALSE(queue.try_pop(value));
    ASSERT_TRUE(queue.empty());
}

TEST(ConcurrentBoundedQueueTest, TestPushWaitsForCapacity) {
    ConcurrentBoundedQueue<int32_t> queue;
    queue.set_capacity(1);
    queue.push(1);

    std::future<void> push_future = std::async(std::launch::async, [&queue]() { queue.push(2); });
    std::future_status initial_status = push_future.wait_for(std::chrono::milliseconds(50));

    int32_t value = 0;
    ASSERT_TRUE(queue.try_pop(value));
    ASSERT_EQ(value, 1);
    ASSERT_EQ(initial_status, std::future_status::timeout);
    ASSERT_EQ(push_future.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    ASSERT_TRUE(queue.try_pop(value));
    ASSERT_EQ(value, 2);
}

}  // namespace paimon::test
