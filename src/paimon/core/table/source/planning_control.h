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
#include <cstddef>

#include "paimon/status.h"

namespace paimon {

/// One cumulative object allowance shared by tasks in a single scan.
class ManifestRowLimit {
 public:
    explicit ManifestRowLimit(size_t rows) : remaining_(rows) {}

    Status Admit(size_t rows) {
        auto remaining = remaining_.load(std::memory_order_relaxed);
        while (rows <= remaining) {
            if (remaining_.compare_exchange_weak(remaining, remaining - rows,
                                                 std::memory_order_relaxed)) {
                return Status::OK();
            }
        }
        return Status::CapacityError("manifest object row limit exceeded");
    }

 private:
    std::atomic<size_t> remaining_;
};

/// Per-plan object allowances. Counters do not govern decoder or buffer allocations.
struct PlanningControl {
    PlanningControl(size_t list_rows, size_t entry_rows, size_t splits)
        : manifest_list_rows(list_rows), manifest_entry_rows(entry_rows), split_count(splits) {}

    ManifestRowLimit manifest_list_rows;
    ManifestRowLimit manifest_entry_rows;
    const size_t split_count;
};

}  // namespace paimon
