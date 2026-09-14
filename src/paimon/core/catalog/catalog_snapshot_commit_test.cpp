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

#include "paimon/core/catalog/catalog_snapshot_commit.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/core/catalog/commit_table_request.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_catalog.h"
#include "paimon/testing/utils/snapshot_test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(CatalogSnapshotCommitTest, TestCommitThroughCatalog) {
    auto catalog = std::make_shared<MockVersionManagedCatalog>(true);
    CatalogSnapshotCommit commit(catalog, Identifier("db", "tbl"), "table-uuid");
    ASSERT_FALSE(commit.IsRequestOnly());
    ASSERT_NOK_WITH_MSG(commit.GetLastCommitTableRequest(),
                        "Should call Commit first before GetLastCommitTableRequest.");

    Snapshot snapshot = BuildTestSnapshot(2, "snapshot-uuid-2");
    std::vector<PartitionStatistics> statistics = {
        PartitionStatistics({{"f1", "20"}}, 1, 541, 1, 1724090888743, -1),
        PartitionStatistics({{"f1", "10"}}, 4, 1118, 2, 1724090888727, 2)};
    ASSERT_OK_AND_ASSIGN(bool success, commit.Commit("base-snapshot-uuid", snapshot, statistics));
    ASSERT_TRUE(success);

    ASSERT_EQ(catalog->CommitCalls().size(), 1u);
    const MockVersionManagedCatalog::CommitCall& call = catalog->CommitCalls().front();
    ASSERT_EQ(call.identifier, Identifier("db", "tbl"));
    ASSERT_EQ(call.table_uuid, std::optional<std::string>("table-uuid"));
    ASSERT_EQ(call.base_snapshot_uuid, std::optional<std::string>("base-snapshot-uuid"));
    ASSERT_EQ(call.snapshot, snapshot);
    ASSERT_EQ(call.statistics, statistics);
    ASSERT_NE(commit.DescribeTarget().find("through catalog"), std::string::npos);

    ASSERT_OK_AND_ASSIGN(std::string request_str, commit.GetLastCommitTableRequest());
    ASSERT_OK_AND_ASSIGN(CommitTableRequest request,
                         CommitTableRequest::FromJsonString(request_str));
    ASSERT_EQ(request,
              CommitTableRequest("table-uuid", "base-snapshot-uuid", snapshot, statistics));

    Snapshot next_snapshot = BuildTestSnapshot(3, "snapshot-uuid-3");
    ASSERT_OK_AND_ASSIGN(success, commit.Commit(snapshot.Uuid(), next_snapshot, {}));
    ASSERT_TRUE(success);
    ASSERT_EQ(catalog->CommitCalls().size(), 2u);
    const MockVersionManagedCatalog::CommitCall& next_call = catalog->CommitCalls().back();
    ASSERT_EQ(next_call.identifier, Identifier("db", "tbl"));
    ASSERT_EQ(next_call.table_uuid, std::optional<std::string>("table-uuid"));
    ASSERT_EQ(next_call.base_snapshot_uuid, snapshot.Uuid());
    ASSERT_EQ(next_call.snapshot, next_snapshot);
    ASSERT_TRUE(next_call.statistics.empty());
    ASSERT_OK_AND_ASSIGN(std::string next_request_str, commit.GetLastCommitTableRequest());
    ASSERT_OK_AND_ASSIGN(CommitTableRequest next_request,
                         CommitTableRequest::FromJsonString(next_request_str));
    ASSERT_EQ(next_request, CommitTableRequest("table-uuid", snapshot.Uuid(), next_snapshot, {}));
}

TEST(CatalogSnapshotCommitTest, TestLostRaceIsNotAnError) {
    auto catalog = std::make_shared<MockVersionManagedCatalog>(false);
    CatalogSnapshotCommit commit(catalog, Identifier("db", "tbl"), std::nullopt);
    ASSERT_OK_AND_ASSIGN(
        bool success, commit.Commit("base-snapshot-uuid", BuildTestSnapshot(2, std::nullopt), {}));
    ASSERT_FALSE(success);
    ASSERT_EQ(catalog->CommitCalls().size(), 1u);
    ASSERT_EQ(catalog->CommitCalls().front().table_uuid, std::nullopt);
}

TEST(CatalogSnapshotCommitTest, TestCatalogFailurePropagates) {
    auto catalog =
        std::make_shared<MockVersionManagedCatalog>(Status::IOError("catalog unreachable"));
    CatalogSnapshotCommit commit(catalog, Identifier("db", "tbl"), "table-uuid");
    ASSERT_NOK_WITH_MSG(commit.Commit(std::nullopt, BuildTestSnapshot(1, std::nullopt), {}),
                        "catalog unreachable");

    ASSERT_OK_AND_ASSIGN(std::string request_str, commit.GetLastCommitTableRequest());
    ASSERT_OK_AND_ASSIGN(CommitTableRequest request,
                         CommitTableRequest::FromJsonString(request_str));
    ASSERT_EQ(request.GetTableId(), std::optional<std::string>("table-uuid"));
    ASSERT_EQ(request.GetSnapshot().Id(), 1);
}

TEST(CatalogSnapshotCommitTest, TestCatalogWhichDoesNotManageVersionsIsRefused) {
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetSupportsVersionManagement(false);
    CatalogSnapshotCommit commit(catalog, Identifier("db", "tbl"), "table-uuid");
    ASSERT_NOK_WITH_MSG(commit.Commit(std::nullopt, BuildTestSnapshot(1, std::nullopt), {}),
                        "does not manage the versions of its tables");
    ASSERT_TRUE(catalog->CommitCalls().empty());
}

TEST(CatalogSnapshotCommitTest, TestBuildRequestWithoutCatalog) {
    CatalogSnapshotCommit commit("table-uuid");
    ASSERT_TRUE(commit.IsRequestOnly());
    ASSERT_NOK_WITH_MSG(commit.GetLastCommitTableRequest(),
                        "Should call Commit first before GetLastCommitTableRequest.");
    ASSERT_EQ(commit.DescribeTarget(), "commit table request built, not sent");

    Snapshot snapshot = BuildTestSnapshot(2, std::nullopt);
    ASSERT_OK_AND_ASSIGN(bool success, commit.Commit("base-snapshot-uuid", snapshot, {}));
    ASSERT_TRUE(success);

    ASSERT_OK_AND_ASSIGN(std::string request_str, commit.GetLastCommitTableRequest());
    ASSERT_OK_AND_ASSIGN(CommitTableRequest request,
                         CommitTableRequest::FromJsonString(request_str));
    ASSERT_EQ(request.GetTableId(), std::optional<std::string>("table-uuid"));
    ASSERT_EQ(request.GetBaseSnapshotUuid(), std::optional<std::string>("base-snapshot-uuid"));
    ASSERT_EQ(request.GetSnapshot().Id(), 2);

    CatalogSnapshotCommit bare_commit;
    ASSERT_OK_AND_ASSIGN(bool bare_success,
                         bare_commit.Commit(std::nullopt, BuildTestSnapshot(1, std::nullopt), {}));
    ASSERT_TRUE(bare_success);
    ASSERT_OK_AND_ASSIGN(std::string bare_request_str, bare_commit.GetLastCommitTableRequest());
    ASSERT_OK_AND_ASSIGN(CommitTableRequest bare_request,
                         CommitTableRequest::FromJsonString(bare_request_str));
    ASSERT_EQ(bare_request.GetTableId(), std::nullopt);
    ASSERT_EQ(bare_request.GetBaseSnapshotUuid(), std::nullopt);
}

}  // namespace paimon::test
