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

#include <memory>
#include <optional>

#include "paimon/core/table/source/snapshot/starting_scanner.h"

namespace paimon {
/// `StartingScanner` for the `StartupMode::LatestFull()` startup mode.
class FullStartingScanner : public StartingScanner {
 public:
    explicit FullStartingScanner(const std::shared_ptr<SnapshotManager>& snapshot_manager)
        : StartingScanner(snapshot_manager) {}

    Result<std::shared_ptr<ScanResult>> Scan(
        const std::shared_ptr<SnapshotReader>& snapshot_reader) override {
        if (starting_snapshot_ == std::nullopt) {
            // Resolve the latest snapshot together with its body: a version-managed catalog
            // can hold a snapshot it never published to the table path, so reading only its
            // id and then reloading the body from the path would fail with NotExist. Whether
            // the body came from the catalog or from the path, it is the one to plan from.
            PAIMON_ASSIGN_OR_RAISE(SnapshotManager::LatestSnapshotResult latest,
                                   snapshot_manager_->LatestSnapshotWithSource());
            if (latest.snapshot == std::nullopt) {
                return std::make_shared<StartingScanner::NoSnapshot>();
            }
            starting_snapshot_ = latest.snapshot;
            starting_snapshot_id_ = latest.snapshot->Id();
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<Plan> plan,
            snapshot_reader->WithMode(ScanMode::ALL)->WithSnapshot(*starting_snapshot_)->Read());
        return std::make_shared<StartingScanner::CurrentSnapshot>(plan);
    }

 private:
    /// The body of the snapshot this scan starts from, resolved once so that a catalog-held
    /// snapshot is planned from the body the catalog returned instead of being re-read from a
    /// path it was never published to.
    std::optional<Snapshot> starting_snapshot_;
};
}  // namespace paimon
