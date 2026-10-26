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
    /// The commit a table publishes its snapshots through, the C++ reading of the Java
    /// `CatalogEnvironment#snapshotCommit`: a catalog that version-manages the table commits
    /// through it, a caller that only wants the request built gets one with no catalog attached,
    /// and everything else publishes by renaming the snapshot file into place.
    ///
    /// @param catalog The catalog the commit was handed, or null for one that has none.
    /// @param catalog_identifier The identifier the catalog knows the table by, on the branch the
    ///        commit commits to. A version-managing catalog needs one and is refused without it.
    /// @param table_id The catalog's UUID for the table, captured before the changes were
    ///        prepared; null when there is none.
    /// @param use_rest_catalog_commit Whether the caller asks for the request rather than for the
    ///        catalog to send it.
    /// @param file_system The file system the snapshot file is renamed through.
    /// @param snapshot_manager The manager that names the snapshot file renamed.
    static Result<std::shared_ptr<SnapshotCommit>> Create(
        const std::shared_ptr<Catalog>& catalog,
        const std::optional<Identifier>& catalog_identifier,
        const std::optional<std::string>& table_id, bool use_rest_catalog_commit,
        const std::shared_ptr<FileSystem>& file_system,
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
