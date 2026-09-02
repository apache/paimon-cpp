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

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "paimon/table/source/plan.h"

namespace paimon {

/// An implementation of `Plan`.
class PlanImpl : public Plan {
 public:
    PlanImpl(const std::optional<int64_t>& snapshot_id,
             const std::vector<std::shared_ptr<Split>>& splits)
        : PlanImpl(snapshot_id, splits, nullptr) {}

    PlanImpl(const std::optional<int64_t>& snapshot_id,
             const std::vector<std::shared_ptr<Split>>& splits,
             const std::shared_ptr<const SnapshotReadView>& snapshot_read_view)
        : snapshot_id_(snapshot_id), splits_(splits), snapshot_read_view_(snapshot_read_view) {}

    std::optional<int64_t> SnapshotId() const override {
        return snapshot_id_;
    }

    const std::vector<std::shared_ptr<Split>>& Splits() const override {
        return splits_;
    }

    std::shared_ptr<const SnapshotReadView> GetSnapshotReadView() const override {
        return snapshot_read_view_;
    }

    static const std::shared_ptr<Plan> EmptyPlan();

    static const std::shared_ptr<Plan> EmptyPlan(
        const std::shared_ptr<const SnapshotReadView>& snapshot_read_view);

 private:
    std::optional<int64_t> snapshot_id_;
    std::vector<std::shared_ptr<Split>> splits_;
    std::shared_ptr<const SnapshotReadView> snapshot_read_view_;
};
}  // namespace paimon
