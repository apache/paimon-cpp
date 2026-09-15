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

#include <atomic>
#include <cstdint>

namespace paimon {

/// How many requests of one kind were made and how many bytes they covered,
/// which is the shape every counter of the read path has. Grouping the two
/// keeps a counter and its byte counter from drifting apart.
///
/// The atomics are relaxed throughout: the counters are only reported, never
/// used to order anything, and the read path increments them on every request.
struct AtomicCounterPair {
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> bytes{0};

    /// Record one request covering `size` bytes.
    void Add(uint64_t size) {
        count.fetch_add(1, std::memory_order_relaxed);
        bytes.fetch_add(size, std::memory_order_relaxed);
    }

    void Reset() {
        count.store(0, std::memory_order_relaxed);
        bytes.store(0, std::memory_order_relaxed);
    }

    uint64_t Count() const {
        return count.load(std::memory_order_relaxed);
    }

    uint64_t Bytes() const {
        return bytes.load(std::memory_order_relaxed);
    }
};

}  // namespace paimon
