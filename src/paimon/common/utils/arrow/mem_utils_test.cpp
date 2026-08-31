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

#include "paimon/common/utils/arrow/mem_utils.h"

#include <cstdint>
#include <memory>
#include <new>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/status.h"
#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

// A MemoryPool whose allocations always fail, either by returning nullptr or by
// throwing std::bad_alloc, so the adaptor's out-of-memory paths can be exercised.
class FailingMemoryPool : public MemoryPool {
 public:
    enum class Mode { kReturnNull, kThrowBadAlloc };

    explicit FailingMemoryPool(Mode mode) : mode_(mode) {}

    void* Malloc(uint64_t /*size*/, uint64_t /*alignment*/ = 0) override {
        if (mode_ == Mode::kThrowBadAlloc) {
            throw std::bad_alloc();
        }
        return nullptr;
    }

    void* Realloc(void* /*p*/, size_t /*old_size*/, size_t /*new_size*/,
                  uint64_t /*alignment*/ = 0) override {
        if (mode_ == Mode::kThrowBadAlloc) {
            throw std::bad_alloc();
        }
        return nullptr;
    }

    void Free(void* /*p*/, uint64_t /*size*/) override {}
    uint64_t CurrentUsage() const override {
        return 0;
    }
    uint64_t MaxMemoryUsage() const override {
        return 0;
    }

 private:
    Mode mode_;
};

class TrackingMemoryPool : public MemoryPool {
 public:
    TrackingMemoryPool(bool* destroyed, int32_t* free_count)
        : destroyed_(destroyed), free_count_(free_count), delegate_(GetDefaultPool()) {}

    ~TrackingMemoryPool() override {
        *destroyed_ = true;
    }

    void* Malloc(uint64_t size, uint64_t alignment = 0) override {
        return delegate_->Malloc(size, alignment);
    }

    void* Realloc(void* p, size_t old_size, size_t new_size, uint64_t alignment = 0) override {
        return delegate_->Realloc(p, old_size, new_size, alignment);
    }

    void Free(void* p, uint64_t size) override {
        ++*free_count_;
        delegate_->Free(p, size);
    }

    void Free(void* p, uint64_t size, uint64_t alignment) override {
        ++*free_count_;
        delegate_->Free(p, size, alignment);
    }

    uint64_t CurrentUsage() const override {
        return delegate_->CurrentUsage();
    }

    uint64_t MaxMemoryUsage() const override {
        return delegate_->MaxMemoryUsage();
    }

 private:
    bool* destroyed_;
    int32_t* free_count_;
    std::shared_ptr<MemoryPool> delegate_;
};

struct TrackingArrowArrayPrivateData {
    std::shared_ptr<void> lifetime;
    std::vector<int32_t>* release_order;
};

void ReleaseTrackingArrowArray(ArrowArray* array) {
    std::unique_ptr<TrackingArrowArrayPrivateData> data(
        static_cast<TrackingArrowArrayPrivateData*>(array->private_data));
    data->release_order->push_back(0);
    array->release = nullptr;
}

}  // namespace

TEST(MemUtilsTest, TestSimple) {
    const int64_t alignment = 64;
    auto pool = GetArrowPool(GetDefaultPool());
    ASSERT_EQ("Paimon Pool", pool->backend_name());
    ASSERT_EQ(0, pool->total_bytes_allocated());
    ASSERT_EQ(0, pool->num_allocations());

    uint8_t* ptr1 = nullptr;
    ASSERT_TRUE(pool->Allocate(10, alignment, &ptr1).ok());
    ASSERT_TRUE(ptr1);
    ASSERT_EQ(10, pool->total_bytes_allocated());
    ASSERT_EQ(10, pool->bytes_allocated());
    ASSERT_EQ(10, pool->max_memory());
    ASSERT_EQ(1, pool->num_allocations());

    // test malloc and free
    uint8_t* ptr2 = nullptr;
    ASSERT_TRUE(pool->Allocate(20, alignment, &ptr2).ok());
    ASSERT_TRUE(ptr2);
    ASSERT_EQ(30, pool->bytes_allocated());
    ASSERT_EQ(30, pool->max_memory());
    pool->Free(ptr2, 20, alignment);
    ASSERT_EQ(10, pool->bytes_allocated());
    ASSERT_EQ(30, pool->max_memory());
    ASSERT_EQ(2, pool->num_allocations());

    // test realloc with nullptr
    uint8_t* ptr3 = nullptr;
    ASSERT_TRUE(pool->Reallocate(/*old_size=*/0, /*new_size=*/40, alignment, &ptr3).ok());
    ASSERT_TRUE(ptr3);
    ASSERT_EQ(50, pool->bytes_allocated());
    ASSERT_EQ(50, pool->max_memory());
    ASSERT_EQ(3, pool->num_allocations());

    uint8_t* ptr3_old = ptr3;
    // test realloc with same size
    ASSERT_TRUE(pool->Reallocate(/*old_size=*/40, /*new_size=*/40, alignment, &ptr3).ok());
    ASSERT_EQ(ptr3_old, ptr3);
    ASSERT_EQ(50, pool->bytes_allocated());
    ASSERT_EQ(50, pool->max_memory());
    ASSERT_EQ(3, pool->num_allocations());

    pool->Free(ptr1, 10, alignment);
    pool->Free(ptr3, 40, alignment);
    ASSERT_EQ(0, pool->bytes_allocated());
    ASSERT_EQ(70, pool->total_bytes_allocated());
    ASSERT_EQ(3, pool->num_allocations());
    ASSERT_EQ(50, pool->max_memory());
}

TEST(MemUtilsTest, TestAllocateOutOfMemory) {
    uint8_t* ptr = nullptr;

    // Underlying pool returns nullptr for a positive size.
    auto null_pool =
        GetArrowPool(std::make_shared<FailingMemoryPool>(FailingMemoryPool::Mode::kReturnNull));
    ASSERT_TRUE(null_pool->Allocate(16, 64, &ptr).IsOutOfMemory());

    // Underlying pool throws std::bad_alloc.
    auto throw_pool =
        GetArrowPool(std::make_shared<FailingMemoryPool>(FailingMemoryPool::Mode::kThrowBadAlloc));
    ASSERT_TRUE(throw_pool->Allocate(16, 64, &ptr).IsOutOfMemory());
}

TEST(MemUtilsTest, TestReallocateOutOfMemory) {
    uint8_t* ptr = nullptr;

    // Underlying pool returns nullptr for a positive new size.
    auto null_pool =
        GetArrowPool(std::make_shared<FailingMemoryPool>(FailingMemoryPool::Mode::kReturnNull));
    ASSERT_TRUE(null_pool->Reallocate(/*old_size=*/0, /*new_size=*/16, 64, &ptr).IsOutOfMemory());

    // Underlying pool throws std::bad_alloc.
    auto throw_pool =
        GetArrowPool(std::make_shared<FailingMemoryPool>(FailingMemoryPool::Mode::kThrowBadAlloc));
    ASSERT_TRUE(throw_pool->Reallocate(/*old_size=*/0, /*new_size=*/16, 64, &ptr).IsOutOfMemory());
}

TEST(MemUtilsTest, TestAddArrowArrayLifetimeComposesReleaseChain) {
    std::vector<int32_t> release_order;
    std::shared_ptr<void> original_lifetime(new int32_t(1), [&release_order](void* ptr) {
        delete static_cast<int32_t*>(ptr);
        release_order.push_back(1);
    });
    std::shared_ptr<void> inner_lifetime(new int32_t(2), [&release_order](void* ptr) {
        delete static_cast<int32_t*>(ptr);
        release_order.push_back(2);
    });
    std::shared_ptr<void> outer_lifetime(new int32_t(3), [&release_order](void* ptr) {
        delete static_cast<int32_t*>(ptr);
        release_order.push_back(3);
    });
    ArrowArray array{};
    array.release = ReleaseTrackingArrowArray;
    array.private_data = new TrackingArrowArrayPrivateData{original_lifetime, &release_order};

    ASSERT_OK(AddArrowArrayLifetime(&array, inner_lifetime));
    ASSERT_OK(AddArrowArrayLifetime(&array, outer_lifetime));
    original_lifetime.reset();
    inner_lifetime.reset();
    outer_lifetime.reset();

    ArrowArrayRelease(&array);
    ASSERT_EQ(nullptr, array.release);
    ASSERT_EQ(std::vector<int32_t>({0, 1, 2, 3}), release_order);
}

TEST(MemUtilsTest, TestAddArrowArrayLifetimeKeepsPoolAliveUntilOriginalRelease) {
    bool pool_destroyed = false;
    int32_t free_count = 0;
    std::shared_ptr<MemoryPool> paimon_pool =
        std::make_shared<TrackingMemoryPool>(&pool_destroyed, &free_count);
    std::weak_ptr<MemoryPool> weak_paimon_pool = paimon_pool;
    std::shared_ptr<arrow::MemoryPool> arrow_pool = GetSharedArrowPool(paimon_pool);
    ArrowArray c_array{};
    {
        arrow::Int32Builder builder(arrow_pool.get());
        ASSERT_TRUE(builder.Append(42).ok());
        arrow::Result<std::shared_ptr<arrow::Array>> array_result = builder.Finish();
        ASSERT_TRUE(array_result.ok()) << array_result.status().ToString();
        std::shared_ptr<arrow::Array> array = std::move(array_result).ValueOrDie();
        ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    }
    ASSERT_OK(AddArrowArrayLifetime(&c_array, arrow_pool));

    arrow_pool.reset();
    paimon_pool.reset();
    ASSERT_FALSE(weak_paimon_pool.expired());
    ASSERT_FALSE(pool_destroyed);

    ArrowArrayRelease(&c_array);
    ASSERT_EQ(nullptr, c_array.release);
    ASSERT_GT(free_count, 0);
    ASSERT_TRUE(weak_paimon_pool.expired());
    ASSERT_TRUE(pool_destroyed);
}

TEST(MemUtilsTest, TestAddArrowArrayLifetimeDeduplicatesSameOwner) {
    std::vector<int32_t> release_order;
    std::shared_ptr<void> lifetime = std::make_shared<int32_t>(1);
    ArrowArray array{};
    array.release = ReleaseTrackingArrowArray;
    array.private_data = new TrackingArrowArrayPrivateData{nullptr, &release_order};

    ASSERT_OK(AddArrowArrayLifetime(&array, lifetime));
    const int64_t use_count = lifetime.use_count();
    ASSERT_OK(AddArrowArrayLifetime(&array, lifetime));
    ASSERT_EQ(use_count, lifetime.use_count());

    ArrowArrayRelease(&array);
}

TEST(MemUtilsTest, TestAddArrowArrayLifetimeRejectsInvalidInput) {
    ArrowArray array{};
    ASSERT_NOK(AddArrowArrayLifetime(nullptr, std::make_shared<int32_t>(1)));
    ASSERT_NOK(AddArrowArrayLifetime(&array, std::make_shared<int32_t>(1)));

    std::vector<int32_t> release_order;
    array.release = ReleaseTrackingArrowArray;
    array.private_data = new TrackingArrowArrayPrivateData{nullptr, &release_order};
    ASSERT_NOK(AddArrowArrayLifetime(&array, nullptr));
    ASSERT_EQ(nullptr, array.release);
    ASSERT_EQ(std::vector<int32_t>({0}), release_order);
}

}  // namespace paimon::test
