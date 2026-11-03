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

#include "paimon/catalog/identifier.h"
#include "paimon/core/catalog/commit_lock.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/snapshot.h"

namespace paimon {

class Catalog;
class CoreOptions;
class FileSystem;
class SnapshotManager;

/// Interface to commit snapshot atomically.
class SnapshotCommit {
 public:
    virtual ~SnapshotCommit() = default;

    /// Acquires the lock scope of one publication attempt. The caller holds it from reading the
    /// base snapshot until `Commit()` returns, and checks it right before committing.
    ///
    /// @return The scope, or null when publication takes no lock.
    virtual Result<std::unique_ptr<CommitLockScope>> AcquireLock() {
        return std::unique_ptr<CommitLockScope>();
    }
    /// Selects catalog-backed or file-system snapshot publication.
    /// @param catalog Optional catalog.
    /// @param catalog_identifier Branch-qualified identifier required by version-managed catalogs.
    /// @param options Effective table options.
    /// @param table_id Table UUID captured before preparing changes, or nullopt.
    /// @param commit_user Commit user for commit locking.
    /// @param use_rest_catalog_commit Build the request without sending it through the catalog.
    /// @param file_system File system used for snapshot rename.
    /// @param snapshot_manager Manager providing the snapshot paths.
    static Result<std::shared_ptr<SnapshotCommit>> Create(
        const std::shared_ptr<Catalog>& catalog,
        const std::optional<Identifier>& catalog_identifier, const CoreOptions& options,
        const std::optional<std::string>& table_id, const std::string& commit_user,
        bool use_rest_catalog_commit, const std::shared_ptr<FileSystem>& file_system,
        const std::shared_ptr<SnapshotManager>& snapshot_manager);

    /// @param base_snapshot_uuid Base snapshot UUID; null for an absent or legacy snapshot.
    /// @param snapshot Snapshot to be committed.
    /// @param branch Branch to publish the snapshot on; an empty name is the main branch.
    /// @param statistics Partition statistics for this change.
    virtual Result<bool> Commit(const std::optional<std::string>& base_snapshot_uuid,
                                const Snapshot& snapshot, const std::string& branch,
                                const std::vector<PartitionStatistics>& statistics) = 0;

    virtual Result<std::string> GetLastCommitTableRequest() = 0;

    /// Returns whether a successful call only prepares a request for the caller to send.
    virtual bool IsRequestOnly() const {
        return false;
    }

    /// Returns a log description of the commit target; empty for file-system commits.
    virtual std::string DescribeTarget() const {
        return "";
    }
};

}  // namespace paimon
