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

#include "paimon/memory/planning_budget.h"

#include <utility>

namespace paimon {
PlanningBudget::PlanningBudget(uint64_t limit, std::shared_ptr<MemoryPool> upstream)
    : limit_(limit), upstream_(std::move(upstream)) {}

Result<std::shared_ptr<PlanningBudget>> PlanningBudget::Create(
    uint64_t byte_limit, const std::shared_ptr<MemoryPool>& upstream) {
    if (!upstream) {
        return Status::Invalid("Planning budget requires an upstream allocator");
    }
    return std::shared_ptr<PlanningBudget>(new PlanningBudget(byte_limit, upstream));
}
}  // namespace paimon
