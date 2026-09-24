/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/commit_context.h"

#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_catalog.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(CommitContextTest, TestLegacyConstructor) {
    auto pool = GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto fs = std::make_shared<MockFileSystem>();
    const std::map<std::string, std::string> options = {{"key", "value"}};
    CommitContext context("table_root_path", "commit_user_1", false, true, true, pool, executor, fs,
                          options, nullptr);
    ASSERT_EQ(context.GetRootPath(), "table_root_path");
    ASSERT_EQ(context.GetCommitUser(), "commit_user_1");
    ASSERT_FALSE(context.IgnoreEmptyCommit());
    ASSERT_TRUE(context.UseRESTCatalogCommit());
    ASSERT_TRUE(context.AppendCommitCheckConflict());
    ASSERT_EQ(context.GetMemoryPool(), pool);
    ASSERT_EQ(context.GetExecutor(), executor);
    ASSERT_EQ(context.GetSpecificFileSystem(), fs);
    ASSERT_EQ(context.GetOptions(), options);
    ASSERT_EQ(context.GetCatalog(), nullptr);
    ASSERT_FALSE(context.GetIdentifier());
    ASSERT_FALSE(context.GetTableId());
}

TEST(CommitContextTest, TestDefaultValue) {
    CommitContextBuilder builder("table_root_path", "commit_user_1");

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto expected_root_path, PathUtil::NormalizePath("table_root_path"));

    ASSERT_EQ(ctx->GetRootPath(), expected_root_path);
    ASSERT_EQ(ctx->GetCommitUser(), "commit_user_1");
    ASSERT_TRUE(ctx->IgnoreEmptyCommit());
    ASSERT_FALSE(ctx->UseRESTCatalogCommit());
    ASSERT_EQ(ctx->GetCatalog(), nullptr);
    ASSERT_FALSE(ctx->GetIdentifier().has_value());
    ASSERT_EQ(ctx->GetTableId(), std::nullopt);
    ASSERT_FALSE(ctx->AppendCommitCheckConflict());
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
    ASSERT_TRUE(ctx->GetOptions().empty());
}

TEST(CommitContextTest, TestSetContent) {
    CommitContextBuilder builder("table_root_path", "commit_user_1");

    auto memory_pool = GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto fs = std::make_shared<MockFileSystem>();

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.IgnoreEmptyCommit(false)
                                       .UseRESTCatalogCommit(true)
                                       .WithTableId("table-uuid")
                                       .AppendCommitCheckConflict(true)
                                       .WithMemoryPool(memory_pool)
                                       .WithExecutor(executor)
                                       .WithFileSystem(fs)
                                       .AddOption("key", "value")
                                       .Finish());

    ASSERT_OK_AND_ASSIGN(auto expected_root_path, PathUtil::NormalizePath("table_root_path"));
    ASSERT_EQ(ctx->GetRootPath(), expected_root_path);
    ASSERT_EQ(ctx->GetCommitUser(), "commit_user_1");
    ASSERT_FALSE(ctx->IgnoreEmptyCommit());
    ASSERT_TRUE(ctx->UseRESTCatalogCommit());
    ASSERT_EQ(ctx->GetTableId(), std::optional<std::string>("table-uuid"));
    ASSERT_TRUE(ctx->AppendCommitCheckConflict());
    ASSERT_EQ(ctx->GetMemoryPool(), memory_pool);
    ASSERT_EQ(ctx->GetExecutor(), executor);
    ASSERT_EQ(ctx->GetSpecificFileSystem(), fs);

    std::map<std::string, std::string> expected_options = {{"key", "value"}};
    ASSERT_EQ(ctx->GetOptions(), expected_options);
}

TEST(CommitContextTest, TestWithCatalog) {
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    CommitContextBuilder builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(
        auto ctx,
        builder.WithCatalog(catalog, Identifier("db1", "t1")).WithTableId("table-uuid").Finish());
    ASSERT_EQ(ctx->GetCatalog(), catalog);
    ASSERT_TRUE(ctx->GetIdentifier().has_value());
    ASSERT_EQ(ctx->GetIdentifier().value(), Identifier("db1", "t1"));
    ASSERT_EQ(ctx->GetTableId(), std::optional<std::string>("table-uuid"));

    CommitContextBuilder null_catalog_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(null_catalog_builder.WithCatalog(nullptr, Identifier("db1", "t1")).Finish(),
                        "cannot commit through a null catalog");

    CommitContextBuilder both_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(both_builder.WithCatalog(catalog, Identifier("db1", "t1"))
                            .UseRESTCatalogCommit(true)
                            .Finish(),
                        "either goes to the catalog");

    ASSERT_OK_AND_ASSIGN(auto next_ctx, builder.Finish());
    ASSERT_EQ(next_ctx->GetCatalog(), nullptr);
    ASSERT_FALSE(next_ctx->GetIdentifier().has_value());
    ASSERT_EQ(next_ctx->GetTableId(), std::nullopt);
}

TEST(CommitContextTest, TestCatalogAddressesBranchByIdentifier) {
    auto catalog = std::make_shared<MockVersionManagedCatalog>();

    CommitContextBuilder branch_builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(
        auto branch_ctx,
        branch_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_dev")).Finish());
    ASSERT_EQ(branch_ctx->GetIdentifier().value(), Identifier("db1", "t1$branch_dev"));
    CommitContextBuilder agreeing_builder("table_root_path", "commit_user_1");
    ASSERT_OK(agreeing_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_dev"))
                  .AddOption(Options::BRANCH, "dev")
                  .Finish());
    CommitContextBuilder constructed_builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(
        auto constructed_ctx,
        constructed_builder.WithCatalog(catalog, Identifier("db1", "t1", "dev")).Finish());
    ASSERT_EQ(constructed_ctx->GetIdentifier().value(), Identifier("db1", "t1$branch_dev"));
    CommitContextBuilder main_builder("table_root_path", "commit_user_1");
    ASSERT_OK(main_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_main"))
                  .AddOption(Options::BRANCH, "main")
                  .Finish());

    CommitContextBuilder mixed_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(mixed_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_dev"))
                            .AddOption(Options::BRANCH, "main")
                            .Finish(),
                        "but both 'dev' and 'main' were named");
    CommitContextBuilder mixed_option_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(
        mixed_option_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_main"))
            .AddOption(Options::BRANCH, "dev")
            .Finish(),
        "but both 'main' and 'dev' were named");

    CommitContextBuilder option_only_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(option_only_builder.WithCatalog(catalog, Identifier("db1", "t1"))
                            .AddOption(Options::BRANCH, "dev")
                            .Finish(),
                        "name branch 'dev' there as 't1$branch_dev'");

    CommitContextBuilder upper_main_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(
        upper_main_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_MAIN")).Finish(),
        "a catalog names branch 'MAIN' as it names the main branch");

    CommitContextBuilder splitter_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(splitter_builder.WithCatalog(catalog, Identifier("db1", "t1"))
                            .AddOption(Options::BRANCH, "dev$options")
                            .Finish(),
                        "a branch a catalog addresses cannot contain '$'");

    CommitContextBuilder system_table_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(
        system_table_builder.WithCatalog(catalog, Identifier("db1", "t1", "dev$options")).Finish(),
        "Cannot 'commit' for system table");
}

TEST(CommitContextTest, TestBranch) {
    CommitContextBuilder file_system_builder("table_root_path", "commit_user_1");
    ASSERT_OK(file_system_builder.AddOption(Options::BRANCH, "dev").Finish());

    CommitContextBuilder upper_main_builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(auto upper_main_ctx,
                         upper_main_builder.AddOption(Options::BRANCH, "MAIN").Finish());
    ASSERT_EQ(upper_main_ctx->GetOptions().at(Options::BRANCH), "MAIN");
    CommitContextBuilder splitter_builder("table_root_path", "commit_user_1");
    ASSERT_OK(splitter_builder.AddOption(Options::BRANCH, "dev$options").Finish());

    CommitContextBuilder rest_builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(
        auto rest_ctx,
        rest_builder.UseRESTCatalogCommit(true).AddOption(Options::BRANCH, "dev").Finish());
    ASSERT_TRUE(rest_ctx->UseRESTCatalogCommit());
    ASSERT_EQ(rest_ctx->GetOptions().at(Options::BRANCH), "dev");
    CommitContextBuilder rest_upper_main_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(rest_upper_main_builder.UseRESTCatalogCommit(true)
                            .AddOption(Options::BRANCH, "MAIN")
                            .Finish(),
                        "a catalog names branch 'MAIN' as it names the main branch");

    CommitContextBuilder escaping_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(escaping_builder.AddOption(Options::BRANCH, "dev/../../outside").Finish(),
                        "branch name cannot contain path separators");
}

TEST(CommitContextTest, TestSetOptionsOverridesAddedOptions) {
    CommitContextBuilder builder("table_root_path", "commit_user_1");
    builder.AddOption("old", "value");
    builder.SetOptions({{"key1", "value1"}, {"key2", "value2"}});

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    std::map<std::string, std::string> expected_options = {{"key1", "value1"}, {"key2", "value2"}};
    ASSERT_EQ(ctx->GetOptions(), expected_options);
}

}  // namespace paimon::test
