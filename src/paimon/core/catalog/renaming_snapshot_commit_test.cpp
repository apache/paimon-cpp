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

#include "paimon/core/catalog/renaming_snapshot_commit.h"

#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/snapshot_test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(RenamingSnapshotCommitTest, TestSimple) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto snapshot_manager = std::make_shared<SnapshotManager>(fs, dir->Str());

    auto commit = std::make_shared<RenamingSnapshotCommit>(fs, snapshot_manager);
    ASSERT_NOK_WITH_MSG(commit->GetLastCommitTableRequest(),
                        "renaming snapshot commit do not support get last commit table request");
    Snapshot snapshot = BuildTestSnapshot(1);
    ASSERT_OK_AND_ASSIGN(bool success, commit->Commit(std::nullopt, snapshot,
                                                      BranchManager::DEFAULT_MAIN_BRANCH, {}));
    ASSERT_TRUE(success);
    ASSERT_OK_AND_ASSIGN(bool exist,
                         fs->Exists(PathUtil::JoinPath(dir->Str(), "snapshot/snapshot-1")));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(bool exist1,
                         fs->Exists(PathUtil::JoinPath(dir->Str(), "snapshot/LATEST")));
    ASSERT_TRUE(exist1);
    // duplicate commit for snapshot-1
    ASSERT_OK_AND_ASSIGN(bool success1, commit->Commit("snapshot-uuid-0", snapshot,
                                                       BranchManager::DEFAULT_MAIN_BRANCH, {}));
    ASSERT_FALSE(success1);
}

TEST(RenamingSnapshotCommitTest, TestCommitToBranch) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto commit = std::make_shared<RenamingSnapshotCommit>(
        fs, std::make_shared<SnapshotManager>(fs, dir->Str(), "dev"));
    Snapshot snapshot = BuildTestSnapshot(1);

    ASSERT_OK_AND_ASSIGN(bool success, commit->Commit(std::nullopt, snapshot, "dev", {}));
    ASSERT_TRUE(success);
    const std::string branch_dir = PathUtil::JoinPath(dir->Str(), "branch/branch-dev/snapshot");
    ASSERT_OK_AND_ASSIGN(bool exist, fs->Exists(PathUtil::JoinPath(branch_dir, "snapshot-1")));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(bool hint_exist, fs->Exists(PathUtil::JoinPath(branch_dir, "LATEST")));
    ASSERT_TRUE(hint_exist);
    ASSERT_OK_AND_ASSIGN(bool main_snapshot_dir_exists,
                         fs->Exists(PathUtil::JoinPath(dir->Str(), "snapshot")));
    ASSERT_FALSE(main_snapshot_dir_exists);
    ASSERT_OK_AND_ASSIGN(bool duplicate, commit->Commit(std::nullopt, snapshot, "dev", {}));
    ASSERT_FALSE(duplicate);
}

TEST(RenamingSnapshotCommitTest, TestBranchHasToMatchSnapshotManager) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto main_commit = std::make_shared<RenamingSnapshotCommit>(
        fs, std::make_shared<SnapshotManager>(fs, dir->Str()));
    auto branch_commit = std::make_shared<RenamingSnapshotCommit>(
        fs, std::make_shared<SnapshotManager>(fs, dir->Str(), "dev"));
    Snapshot snapshot = BuildTestSnapshot(1);

    ASSERT_NOK_WITH_MSG(main_commit->Commit(std::nullopt, snapshot, "dev", {}),
                        "renaming snapshot commit built for branch 'main' cannot commit snapshot "
                        "#1 to branch 'dev'");
    ASSERT_NOK_WITH_MSG(
        branch_commit->Commit(std::nullopt, snapshot, BranchManager::DEFAULT_MAIN_BRANCH, {}),
        "renaming snapshot commit built for branch 'dev' cannot commit snapshot #1 to branch "
        "'main'");
    ASSERT_OK_AND_ASSIGN(bool branch_dir_exists,
                         fs->Exists(PathUtil::JoinPath(dir->Str(), "branch")));
    ASSERT_FALSE(branch_dir_exists);
    ASSERT_OK_AND_ASSIGN(bool main_snapshot_dir_exists,
                         fs->Exists(PathUtil::JoinPath(dir->Str(), "snapshot")));
    ASSERT_FALSE(main_snapshot_dir_exists);

    ASSERT_OK_AND_ASSIGN(bool main_success, main_commit->Commit(std::nullopt, snapshot, "", {}));
    ASSERT_TRUE(main_success);
}

}  // namespace paimon::test
