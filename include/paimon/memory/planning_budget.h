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
#include <memory>

#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class PlanningBudgetAccess;

/// Query-owned cumulative allocation admission for bounded metadata planning.
/// Frees do not replenish admission: this deliberately also bounds temporary peaks.
/// The limit covers requested storage, not allocator overhead or process RSS.
class PAIMON_EXPORT PlanningBudget {
 public:
    static Result<std::shared_ptr<PlanningBudget>> Create(
        uint64_t byte_limit, const std::shared_ptr<MemoryPool>& upstream = GetDefaultPool());

    uint64_t ByteLimit() const {
        return limit_;
    }
    uint64_t BytesAdmitted() const {
        return admitted_.load();
    }

 private:
    friend class PlanningBudgetAccess;
    PlanningBudget(uint64_t limit, std::shared_ptr<MemoryPool> upstream);
    const uint64_t limit_;
    std::shared_ptr<MemoryPool> upstream_;
    std::atomic<uint64_t> admitted_{0};
    // The first allocation failure is terminal across scans sharing this budget.
    std::atomic<uint8_t> failure_{0};
};
}  // namespace paimon
