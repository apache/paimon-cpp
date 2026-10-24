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

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#if defined(__linux__) && defined(__GLIBCXX__) && _GLIBCXX_USE_CXX11_ABI && _GLIBCXX_RELEASE >= 9
#include <memory_resource>
#endif
#include <new>
#include <utility>

#include "paimon/memory/planning_budget.h"

namespace paimon {
class PlanningBudgetAccess {
 public:
    static void Reserve(const std::shared_ptr<PlanningBudget>& budget, uint64_t bytes) {
        if (budget->failure_.load() != 0) {
            throw std::bad_alloc();
        }
        uint64_t used = budget->admitted_.load();
        do {
            if (bytes > budget->limit_ - used) {
                uint8_t expected = 0;
                budget->failure_.compare_exchange_strong(expected, 1);
                throw std::bad_alloc();
            }
        } while (!budget->admitted_.compare_exchange_weak(used, used + bytes));
    }

    static void* Allocate(const std::shared_ptr<PlanningBudget>& budget, size_t bytes,
                          size_t alignment) {
        Reserve(budget, bytes);
        try {
            void* result = budget->upstream_->Malloc(bytes, std::max(alignment, alignof(void*)));
            if (bytes != 0 && result == nullptr) {
                throw std::bad_alloc();
            }
            return result;
        } catch (const std::bad_alloc&) {
            uint8_t expected = 0;
            budget->failure_.compare_exchange_strong(expected, 2);
            throw;
        }
    }

    static void Free(const std::shared_ptr<PlanningBudget>& budget, void* ptr, size_t bytes,
                     size_t alignment) {
        budget->upstream_->Free(ptr, bytes, std::max(alignment, alignof(void*)));
    }

    static const std::shared_ptr<MemoryPool>& Upstream(
        const std::shared_ptr<PlanningBudget>& budget) {
        return budget->upstream_;
    }

    static void MarkAllocationFailure(const std::shared_ptr<PlanningBudget>& budget) {
        uint8_t expected = 0;
        budget->failure_.compare_exchange_strong(expected, 2);
    }

    static Status Failure(const std::shared_ptr<PlanningBudget>& budget) {
        if (budget->failure_.load() == 1) {
            return Status::CapacityError("Native planning byte budget exhausted");
        }
        return Status::OutOfMemory("Native planning allocation failed");
    }
};

#if defined(__linux__) && defined(__GLIBCXX__) && _GLIBCXX_USE_CXX11_ABI && _GLIBCXX_RELEASE >= 9
class PlanningResource : public std::pmr::memory_resource {
 public:
    explicit PlanningResource(std::shared_ptr<PlanningBudget> budget)
        : budget_(std::move(budget)) {}
    const std::shared_ptr<PlanningBudget>& Budget() const {
        return budget_;
    }

 private:
    void* do_allocate(size_t bytes, size_t alignment) override {
        return PlanningBudgetAccess::Allocate(budget_, bytes, alignment);
    }
    void do_deallocate(void* ptr, size_t bytes, size_t alignment) override {
        PlanningBudgetAccess::Free(budget_, ptr, bytes, alignment);
    }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }
    std::shared_ptr<PlanningBudget> budget_;
};

#endif

template <typename T>
class PlanningAllocator {
 public:
    using value_type = T;
    explicit PlanningAllocator(std::shared_ptr<PlanningBudget> budget)
        : budget(std::move(budget)) {}
    template <typename U>
    PlanningAllocator(const PlanningAllocator<U>& other) : budget(other.budget) {}
    T* allocate(size_t count) {
        if (count > std::numeric_limits<size_t>::max() / sizeof(T)) {
            throw std::bad_alloc();
        }
        return static_cast<T*>(
            PlanningBudgetAccess::Allocate(budget, count * sizeof(T), alignof(T)));
    }
    void deallocate(T* ptr, size_t count) {
        PlanningBudgetAccess::Free(budget, ptr, count * sizeof(T), alignof(T));
    }
    template <typename U>
    bool operator==(const PlanningAllocator<U>& other) const {
        return budget == other.budget;
    }
    template <typename U>
    bool operator!=(const PlanningAllocator<U>& other) const {
        return !(*this == other);
    }
    std::shared_ptr<PlanningBudget> budget;
};
}  // namespace paimon
