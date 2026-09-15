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

#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/partition/partition_statistics.h"
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
    ASSERT_OK_AND_ASSIGN(bool success, commit->Commit(std::nullopt, snapshot, {}));
    ASSERT_TRUE(success);
    ASSERT_OK_AND_ASSIGN(bool exist,
                         fs->Exists(PathUtil::JoinPath(dir->Str(), "snapshot/snapshot-1")));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(bool exist1,
                         fs->Exists(PathUtil::JoinPath(dir->Str(), "snapshot/LATEST")));
    ASSERT_TRUE(exist1);
    // duplicate commit for snapshot-1
    ASSERT_OK_AND_ASSIGN(bool success1, commit->Commit("snapshot-uuid-0", snapshot, {}));
    ASSERT_FALSE(success1);
}

}  // namespace paimon::test
