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

#include "paimon/core/utils/branch_manager.h"

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(BranchManagerTest, TestIsMainBranch) {
    ASSERT_TRUE(BranchManager::IsMainBranch("main"));
    ASSERT_TRUE(BranchManager::IsMainBranch(BranchManager::DEFAULT_MAIN_BRANCH));

    ASSERT_FALSE(BranchManager::IsMainBranch("m a i n"));
    ASSERT_FALSE(BranchManager::IsMainBranch(""));
}

TEST(BranchManagerTest, TestNormalizeBranch) {
    ASSERT_EQ(BranchManager::NormalizeBranch(""), BranchManager::DEFAULT_MAIN_BRANCH);
    ASSERT_EQ(BranchManager::NormalizeBranch("   "), BranchManager::DEFAULT_MAIN_BRANCH);

    ASSERT_EQ(BranchManager::NormalizeBranch("data"), "data");
    ASSERT_EQ(BranchManager::NormalizeBranch("d a t a"), "d a t a");
}

TEST(BranchManagerTest, TestBranchPath) {
    ASSERT_EQ(BranchManager::BranchPath("/root", BranchManager::DEFAULT_MAIN_BRANCH), "/root");
    ASSERT_EQ(BranchManager::BranchPath("/root", "data"), "/root/branch/branch-data");
}

TEST(BranchManagerTest, TestCheckValidBranch) {
    ASSERT_OK(BranchManager::CheckValidBranch(BranchManager::DEFAULT_MAIN_BRANCH));
    ASSERT_OK(BranchManager::CheckValidBranch("data"));
    ASSERT_OK(BranchManager::CheckValidBranch("d a t a"));
    // A branch `NormalizeBranch` maps to `main` names no directory of its own.
    ASSERT_OK(BranchManager::CheckValidBranch(""));
    ASSERT_OK(BranchManager::CheckValidBranch("   "));

    // A branch that would leave the table root is rejected.
    ASSERT_NOK_WITH_MSG(BranchManager::CheckValidBranch(".."), "branch name cannot be '.' or '..'");
    ASSERT_NOK_WITH_MSG(BranchManager::CheckValidBranch("rt/../../../../../outside"),
                        "branch name cannot contain path separators");
    ASSERT_NOK_WITH_MSG(BranchManager::CheckValidBranch("line\nfeed"),
                        "branch name cannot contain control characters");
}
}  // namespace paimon::test
