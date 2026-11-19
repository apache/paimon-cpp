/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <memory_resource>

#include "paimon/memory/memory_pool.h"
namespace paimon::lumina {
class LuminaMemoryPool : public std::pmr::memory_resource {
 public:
    explicit LuminaMemoryPool(bool track_memory, const std::shared_ptr<MemoryPool>& pool)
        : pool_(pool), track_memory_(track_memory) {}

    ~LuminaMemoryPool() override = default;

    LuminaMemoryPool(const LuminaMemoryPool&) = delete;
    LuminaMemoryPool& operator=(const LuminaMemoryPool&) = delete;

    std::shared_ptr<MemoryPool> GetPaimonPool() const {
        return pool_;
    }

    /// Bytes currently allocated through this resource, excluding other users of the same pool.
    /// Returns zero when memory tracking is disabled.
    uint64_t GetMemoryUsage() const {
        return track_memory_ ? memory_usage_.load(std::memory_order_relaxed) : 0;
    }

 private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        static const size_t min_alignment = alignof(std::max_align_t);
        if (alignment < min_alignment) {
            alignment = min_alignment;
        }
        void* ptr = pool_->Malloc(bytes, alignment);
        if (track_memory_) {
            memory_usage_.fetch_add(bytes, std::memory_order_relaxed);
        }
        return ptr;
    }

    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
        pool_->Free(p, bytes, alignment);
        if (track_memory_) {
            memory_usage_.fetch_sub(bytes, std::memory_order_relaxed);
        }
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    const bool track_memory_;
    std::atomic<uint64_t> memory_usage_{0};
};
}  // namespace paimon::lumina
