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

#include <map>
#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/defs.h"
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
    // A branch `NormalizeBranch` maps to `main` resolves to the table root, so that a raw option
    // value cannot select a directory the main branch never writes to.
    ASSERT_EQ(BranchManager::BranchPath("/root", ""), "/root");
    ASSERT_EQ(BranchManager::BranchPath("/root", "   "), "/root");
}

TEST(BranchManagerTest, TestResolveBranch) {
    const std::map<std::string, std::string> no_options;
    ASSERT_OK_AND_ASSIGN(std::string unnamed,
                         BranchManager::ResolveBranch(/*identifier=*/std::nullopt, no_options,
                                                      /*explicit_branch=*/std::nullopt, "commit"));
    ASSERT_EQ(BranchManager::DEFAULT_MAIN_BRANCH, unnamed);

    ASSERT_OK_AND_ASSIGN(std::string from_identifier,
                         BranchManager::ResolveBranch(Identifier("db", "tbl$branch_data"),
                                                      no_options, std::nullopt, "commit"));
    ASSERT_EQ("data", from_identifier);
    ASSERT_OK_AND_ASSIGN(std::string from_option,
                         BranchManager::ResolveBranch(std::nullopt, {{Options::BRANCH, "data"}},
                                                      std::nullopt, "commit"));
    ASSERT_EQ("data", from_option);
    ASSERT_OK_AND_ASSIGN(std::string from_given,
                         BranchManager::ResolveBranch(std::nullopt, no_options, "data", "write"));
    ASSERT_EQ("data", from_given);

    ASSERT_OK_AND_ASSIGN(std::string empty,
                         BranchManager::ResolveBranch(std::nullopt, no_options, "", "write"));
    ASSERT_EQ(BranchManager::DEFAULT_MAIN_BRANCH, empty);
    ASSERT_OK_AND_ASSIGN(std::string agreed, BranchManager::ResolveBranch(
                                                 Identifier("db", "tbl$branch_data"),
                                                 {{Options::BRANCH, "data"}}, "data", "write"));
    ASSERT_EQ("data", agreed);

    ASSERT_NOK_WITH_MSG(
        BranchManager::ResolveBranch(Identifier("db", "tbl$branch_data"), {{Options::BRANCH, "rt"}},
                                     std::nullopt, "commit"),
        "a commit is aimed at one branch, but both 'data' and 'rt' were named");
    ASSERT_NOK_WITH_MSG(
        BranchManager::ResolveBranch(std::nullopt, {{Options::BRANCH, "main"}}, "rt", "write"),
        "a write is aimed at one branch, but both 'main' and 'rt' were named");

    ASSERT_NOK_WITH_MSG(BranchManager::ResolveBranch(Identifier("db", "tbl$a$b$c"), no_options,
                                                     std::nullopt, "commit"),
                        "Invalid table name");

    ASSERT_NOK_WITH_MSG(
        BranchManager::ResolveBranch(Identifier("db", "tbl$branch_data"),
                                     {{Options::BRANCH, "rt/../../outside"}}, std::nullopt, "read"),
        "branch name cannot contain path separators");
    ASSERT_NOK_WITH_MSG(
        BranchManager::ResolveBranch(std::nullopt, no_options, "line\nfeed", "write"),
        "branch name cannot contain control characters");
}

TEST(BranchManagerTest, TestCheckCatalogAddressableBranch) {
    ASSERT_OK(BranchManager::CheckCatalogAddressableBranch(BranchManager::DEFAULT_MAIN_BRANCH));
    ASSERT_OK(BranchManager::CheckCatalogAddressableBranch(""));
    ASSERT_OK(BranchManager::CheckCatalogAddressableBranch("   "));
    ASSERT_OK(BranchManager::CheckCatalogAddressableBranch("data"));
    ASSERT_OK(BranchManager::CheckCatalogAddressableBranch("mainline"));

    ASSERT_NOK_WITH_MSG(BranchManager::CheckCatalogAddressableBranch("MAIN"),
                        "a catalog names branch 'MAIN' as it names the main branch");
    ASSERT_NOK_WITH_MSG(BranchManager::CheckCatalogAddressableBranch("Main"),
                        "a catalog names branch 'Main' as it names the main branch");

    ASSERT_NOK_WITH_MSG(BranchManager::CheckCatalogAddressableBranch("dev$options"),
                        "a branch a catalog addresses cannot contain '$'");
    ASSERT_NOK_WITH_MSG(BranchManager::CheckCatalogAddressableBranch("a$b$c"),
                        "a branch a catalog addresses cannot contain '$'");
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
