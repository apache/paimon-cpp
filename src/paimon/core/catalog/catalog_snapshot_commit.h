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

#include <string>
#include <vector>

#include "paimon/core/catalog/commit_table_request.h"
#include "paimon/core/catalog/snapshot_commit.h"

namespace paimon {

/// A `SnapshotCommit` using `Catalog` to commit.
class CatalogSnapshotCommit : public SnapshotCommit {
 public:
    Result<bool> Commit(const Snapshot& snapshot,
                        const std::vector<PartitionStatistics>& statistics) override {
        commit_table_request_ = CommitTableRequest(snapshot, statistics);
        return true;
    }

    Result<std::string> GetLastCommitTableRequest() override {
        if (commit_table_request_) {
            return commit_table_request_.value().ToJsonString();
        } else {
            return Status::Invalid("Should call Commit first before GetLastCommitTableRequest.");
        }
    }

 private:
    std::optional<CommitTableRequest> commit_table_request_;
};

}  // namespace paimon
