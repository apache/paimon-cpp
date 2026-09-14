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
#include <utility>
#include <vector>

#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/core/catalog/commit_table_request.h"
#include "paimon/core/catalog/snapshot_commit.h"
#include "paimon/core/catalog/version_managed_catalog.h"

namespace paimon {

/// Commits snapshots through a catalog or prepares requests for an external caller.
///
/// @note Without a catalog, the caller retrieves the request with `GetLastCommitTableRequest()`
/// and sends it.
class CatalogSnapshotCommit : public SnapshotCommit {
 public:
    /// @param catalog Catalog implementing version management.
    /// @param identifier Identifier of the table to commit to.
    /// @param table_id Catalog UUID captured before preparing changes; null if unavailable.
    CatalogSnapshotCommit(const std::shared_ptr<Catalog>& catalog, Identifier identifier,
                          const std::optional<std::string>& table_id)
        : catalog_(catalog),
          version_managed_catalog_(AsVersionManaged(catalog)),
          identifier_(std::move(identifier)),
          table_id_(table_id) {}

    /// @param table_id Catalog UUID to include in the request; null if unavailable.
    explicit CatalogSnapshotCommit(const std::optional<std::string>& table_id = std::nullopt)
        : identifier_("", ""), table_id_(table_id) {}

    Result<bool> Commit(const std::optional<std::string>& base_snapshot_uuid,
                        const Snapshot& snapshot,
                        const std::vector<PartitionStatistics>& statistics) override {
        commit_table_request_ =
            CommitTableRequest(table_id_, base_snapshot_uuid, snapshot, statistics);
        if (catalog_ == nullptr) {
            return true;
        }
        if (version_managed_catalog_ == nullptr) {
            return Status::Invalid(
                "this catalog does not manage the versions of its tables, so there is nothing to "
                "commit the snapshot to");
        }
        return version_managed_catalog_->CommitSnapshot(identifier_, table_id_, base_snapshot_uuid,
                                                        snapshot, statistics);
    }

    std::string DescribeTarget() const override {
        return catalog_ == nullptr ? "commit table request built, not sent"
                                   : "through catalog, " + identifier_.ToString();
    }

    bool IsRequestOnly() const override {
        return catalog_ == nullptr;
    }

    /// Returns the request from the latest attempt, including refusals and errors.
    Result<std::string> GetLastCommitTableRequest() override {
        if (commit_table_request_) {
            return commit_table_request_.value().ToJsonString();
        } else {
            return Status::Invalid("Should call Commit first before GetLastCommitTableRequest.");
        }
    }

 private:
    std::shared_ptr<Catalog> catalog_;
    // Owned by catalog_.
    VersionManagedCatalog* version_managed_catalog_ = nullptr;
    Identifier identifier_;
    std::optional<std::string> table_id_;
    std::optional<CommitTableRequest> commit_table_request_;
};

}  // namespace paimon
