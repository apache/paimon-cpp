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
    CommitContext context("table_root_path", "commit_user_1", false, true, true, pool, executor,
                          fs);
    ASSERT_EQ(context.GetRootPath(), "table_root_path");
    ASSERT_EQ(context.GetCommitUser(), "commit_user_1");
    ASSERT_FALSE(context.IgnoreEmptyCommit());
    ASSERT_TRUE(context.UseRESTCatalogCommit());
    ASSERT_TRUE(context.AppendCommitCheckConflict());
    ASSERT_EQ(context.GetMemoryPool(), pool);
    ASSERT_EQ(context.GetExecutor(), executor);
    ASSERT_EQ(context.GetSpecificFileSystem(), fs);
    ASSERT_EQ(context.GetCatalog(), nullptr);
    ASSERT_FALSE(context.GetIdentifier());
    ASSERT_FALSE(context.GetTableId());
    ASSERT_TRUE(context.GetFileSystemSchemeToIdentifierMap().empty());
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
    ASSERT_TRUE(ctx->GetFileSystemSchemeToIdentifierMap().empty());
    ASSERT_FALSE(ctx->GetSpecificTableSchema());
    ASSERT_FALSE(ctx->GetTableId());
}

TEST(CommitContextTest, TestFileSystemAndSchemeMapConflict) {
    CommitContextBuilder builder("table_root_path", "commit_user_1");
    auto fs = std::make_shared<MockFileSystem>();
    builder.WithFileSystem(fs);
    builder.WithFileSystemSchemeToIdentifierMap({{"file", "local"}});
    ASSERT_NOK_WITH_MSG(
        builder.Finish(),
        "WithFileSystem() and WithFileSystemSchemeToIdentifierMap() cannot be used together");
}

TEST(CommitContextTest, TestSchemeMapWithoutFileSystem) {
    CommitContextBuilder builder("table_root_path", "commit_user_1");
    builder.WithFileSystemSchemeToIdentifierMap({{"file", "local"}});
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    std::map<std::string, std::string> expected_fs_map = {{"file", "local"}};
    ASSERT_EQ(expected_fs_map, ctx->GetFileSystemSchemeToIdentifierMap());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
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
                                       .SetTableSchema("table-schema-json")
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
    ASSERT_TRUE(ctx->GetSpecificTableSchema().has_value());
    ASSERT_EQ("table-schema-json", ctx->GetSpecificTableSchema().value());

    std::map<std::string, std::string> expected_options = {{"key", "value"}};
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

}  // namespace paimon::test
