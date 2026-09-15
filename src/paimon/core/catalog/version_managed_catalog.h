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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/snapshot.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// Snapshot publication interface for catalogs returning true from `SupportsVersionManagement()`.
///
/// Kept internal because `Snapshot` and `PartitionStatistics` are not installed API types.
class VersionManagedCatalog {
 public:
    virtual ~VersionManagedCatalog() = default;

    /// Loads the current snapshot used as the base for commits.
    ///
    /// @param identifier Identifier (database and table name) of the table to read.
    /// @return The current snapshot, or null when the table has none yet.
    virtual Result<std::optional<Snapshot>> LoadSnapshot(const Identifier& identifier) const = 0;

    /// Atomically commits a snapshot if its table and base UUIDs match the catalog state.
    ///
    /// @param identifier Identifier (database and table name) of the table to commit to.
    /// @param table_uuid Catalog table UUID used to detect table recreation; null if unavailable.
    /// @param base_snapshot_uuid Base snapshot UUID; null for an absent or legacy snapshot.
    /// @param snapshot The snapshot to commit.
    /// @param statistics Partition statistics for this change.
    /// @return True on success, false on a conflict requiring rebase, or an error on failure.
    ///         Errors leave publication uncertain, so manifests must be preserved for recovery.
    virtual Result<bool> CommitSnapshot(const Identifier& identifier,
                                        const std::optional<std::string>& table_uuid,
                                        const std::optional<std::string>& base_snapshot_uuid,
                                        const Snapshot& snapshot,
                                        const std::vector<PartitionStatistics>& statistics) = 0;
};

/// Returns version management only when the catalog both advertises and implements it.
inline VersionManagedCatalog* AsVersionManaged(const std::shared_ptr<Catalog>& catalog) {
    if (catalog == nullptr || !catalog->SupportsVersionManagement()) {
        return nullptr;
    }
    return dynamic_cast<VersionManagedCatalog*>(catalog.get());
}

/// Prevents file-system fallback when a catalog advertises version management without implementing
/// it.
inline Status CheckVersionManagementImplemented(const std::shared_ptr<Catalog>& catalog) {
    if (catalog != nullptr && catalog->SupportsVersionManagement() &&
        dynamic_cast<VersionManagedCatalog*>(catalog.get()) == nullptr) {
        return Status::Invalid(
            "this catalog reports that it manages the versions of its tables but does not "
            "implement VersionManagedCatalog, so there is nothing to commit the snapshot to");
    }
    return Status::OK();
}

}  // namespace paimon
