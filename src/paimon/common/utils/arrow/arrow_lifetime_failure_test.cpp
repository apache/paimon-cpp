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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/arrow/abi.h"
#include "paimon/common/utils/arrow/mem_utils.h"

namespace {
thread_local bool fail_next_allocation = false;
}  // namespace

extern "C" void* __real__Znwm(std::size_t size);
extern "C" void* __wrap__Znwm(std::size_t size) {
    if (std::exchange(fail_next_allocation, false)) {
        throw std::bad_alloc();
    }
    return __real__Znwm(size);
}

namespace paimon::test {

TEST(ArrowLifetimeFailureTest, ReleasesBothRootsWhenHoldingLifetimeRunsOutOfMemory) {
    ArrowArray array{};
    ArrowSchema schema{};
    int32_t array_releases = 0;
    int32_t schema_releases = 0;
    array.private_data = &array_releases;
    array.release = [](ArrowArray* root) {
        ++*static_cast<int32_t*>(root->private_data);
        root->release = nullptr;
    };
    schema.private_data = &schema_releases;
    schema.release = [](ArrowSchema* root) {
        ++*static_cast<int32_t*>(root->private_data);
        root->release = nullptr;
    };
    std::shared_ptr<void> owner = std::make_shared<int32_t>(1);

    Status status;
    bool threw = false;
    fail_next_allocation = true;
    try {
        status = AddArrowArrayLifetime(&array, &schema, owner);
    } catch (const std::bad_alloc&) {
        threw = true;
    }
    const bool fired = !std::exchange(fail_next_allocation, false);

    EXPECT_TRUE(fired);
    EXPECT_FALSE(threw);
    EXPECT_TRUE(status.IsOutOfMemory());
    EXPECT_EQ(array_releases, 1);
    EXPECT_EQ(schema_releases, 1);
    EXPECT_EQ(array.release, nullptr);
    EXPECT_EQ(schema.release, nullptr);
}

}  // namespace paimon::test
