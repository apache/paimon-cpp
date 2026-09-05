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

#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

/// Allocate a shared buffer that keeps the pool it was allocated from alive.
///
/// A `Bytes` holds a raw pointer to its pool and frees its allocation through
/// it, so it must not outlive the pool. Holding both in one owner is enough as
/// long as that owner drops the buffer, but not once the buffer is handed to an
/// asynchronous read: the callback of the read owns a reference to the buffer,
/// and a stream that destroys the callback after it has resolved the read - the
/// object store streams do - lets an IO thread drop the last reference to the
/// buffer after the owner and the pool are already gone. Binding the pool to
/// the buffer makes the buffer keep its own allocator alive, wherever its last
/// reference is dropped.
///
/// @param size Number of bytes to allocate.
/// @param pool Memory pool to allocate from, which the returned buffer keeps
/// alive.
inline std::shared_ptr<Bytes> AllocateBytesKeepingPoolAlive(
    size_t size, const std::shared_ptr<MemoryPool>& pool) {
    // The pool is declared before the buffer, so the holder destroys the buffer
    // first and the pool it was allocated from second.
    struct BytesWithMemoryPool {
        std::shared_ptr<MemoryPool> pool;
        std::shared_ptr<Bytes> bytes;
    };
    auto holder = std::make_shared<BytesWithMemoryPool>(
        BytesWithMemoryPool{pool, std::make_shared<Bytes>(size, pool.get())});
    Bytes* bytes = holder->bytes.get();
    return std::shared_ptr<Bytes>(std::move(holder), bytes);
}

}  // namespace paimon
