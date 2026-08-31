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
#include <string>

#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "fmt/format.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {
namespace {

struct ArrowArrayPrivateData {
    void (*release)(ArrowArray*);
    void* private_data;
    std::shared_ptr<void> lifetime;
};

void ReleaseArrowArray(ArrowArray* array) {
    std::unique_ptr<ArrowArrayPrivateData> data(
        static_cast<ArrowArrayPrivateData*>(array->private_data));
    array->release = data->release;
    array->private_data = data->private_data;
    array->release(array);
}

bool HasSameOwner(const std::shared_ptr<void>& lhs, const std::shared_ptr<void>& rhs) {
    return !lhs.owner_before(rhs) && !rhs.owner_before(lhs);
}

class ArrowMemPoolAdaptor : public arrow::MemoryPool {
 public:
    explicit ArrowMemPoolAdaptor(const std::shared_ptr<paimon::MemoryPool>& pool)
        : pool_(*pool), life_holder_(pool) {}

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        uint8_t* new_out = nullptr;
        try {
            new_out = reinterpret_cast<uint8_t*>(pool_.Malloc(size, alignment));
        } catch (const std::bad_alloc&) {
            return arrow::Status::OutOfMemory(fmt::format("failed to allocate {} bytes", size));
        }
        if (size > 0 && new_out == nullptr) {
            return arrow::Status::OutOfMemory(fmt::format("failed to allocate {} bytes", size));
        }
        *out = new_out;
        stats_.DidAllocateBytes(size);
        return arrow::Status::OK();
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                             uint8_t** ptr) override {
        uint8_t* new_ptr = nullptr;
        try {
            new_ptr =
                reinterpret_cast<uint8_t*>(pool_.Realloc(*ptr, old_size, new_size, alignment));
        } catch (const std::bad_alloc&) {
            return arrow::Status::OutOfMemory(
                fmt::format("failed to reallocate memory from {} to {} bytes", old_size, new_size));
        }
        if (new_size > 0 && new_ptr == nullptr) {
            return arrow::Status::OutOfMemory(
                fmt::format("failed to reallocate memory from {} to {} bytes", old_size, new_size));
        }
        *ptr = new_ptr;
        stats_.DidReallocateBytes(old_size, new_size);
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
        pool_.Free(buffer, size, alignment);
        stats_.DidFreeBytes(size);
    }

    int64_t bytes_allocated() const override {
        return stats_.bytes_allocated();
    }

    int64_t max_memory() const override {
        return stats_.max_memory();
    }

    std::string backend_name() const override {
        return "Paimon Pool";
    }

    /// The number of bytes that were allocated.
    int64_t total_bytes_allocated() const override {
        return stats_.total_bytes_allocated();
    }

    /// The number of allocations or reallocations that were requested.
    int64_t num_allocations() const override {
        return stats_.num_allocations();
    }

 private:
    paimon::MemoryPool& pool_;
    std::shared_ptr<paimon::MemoryPool> life_holder_;
    arrow::internal::MemoryPoolStats stats_;
};

}  // namespace

std::unique_ptr<arrow::MemoryPool> GetArrowPool(const std::shared_ptr<MemoryPool>& pool) {
    return std::make_unique<ArrowMemPoolAdaptor>(pool);
}

std::shared_ptr<arrow::MemoryPool> GetSharedArrowPool(const std::shared_ptr<MemoryPool>& pool) {
    return std::make_shared<ArrowMemPoolAdaptor>(pool);
}

Status AddArrowArrayLifetime(ArrowArray* array, const std::shared_ptr<void>& lifetime) {
    if (array == nullptr || array->release == nullptr) {
        return Status::Invalid("cannot add lifetime to a released ArrowArray");
    }
    if (lifetime.use_count() == 0) {
        ArrowArrayRelease(array);
        return Status::Invalid("cannot add an empty lifetime to an ArrowArray");
    }
    if (array->release == ReleaseArrowArray) {
        const auto* data = static_cast<const ArrowArrayPrivateData*>(array->private_data);
        if (HasSameOwner(data->lifetime, lifetime)) {
            return Status::OK();
        }
    }
    try {
        std::unique_ptr<ArrowArrayPrivateData> data = std::make_unique<ArrowArrayPrivateData>(
            ArrowArrayPrivateData{array->release, array->private_data, lifetime});
        array->release = ReleaseArrowArray;
        array->private_data = data.release();
        return Status::OK();
    } catch (const std::bad_alloc&) {
        ArrowArrayRelease(array);
        return Status::OutOfMemory("failed to add lifetime to an ArrowArray");
    }
}

}  // namespace paimon
