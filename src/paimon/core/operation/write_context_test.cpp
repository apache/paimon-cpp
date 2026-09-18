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

#include "paimon/write_context.h"

#include <cstdint>

#include "gtest/gtest.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_catalog.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(WriteContextTest, TestDefaultValue) {
    WriteContextBuilder builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_EQ(ctx->GetRootPath(), "table_root_path");
    ASSERT_EQ(ctx->GetCommitUser(), "commit_user_1");
    ASSERT_FALSE(ctx->IsStreamingMode());
    ASSERT_FALSE(ctx->IgnoreNumBucketCheck());
    ASSERT_FALSE(ctx->IgnorePreviousFiles());
    ASSERT_FALSE(ctx->EnableMultiThreadSpill());
    ASSERT_EQ(ctx->GetWriteId(), std::nullopt);
    ASSERT_EQ(ctx->GetBranch(), "main");
    ASSERT_TRUE(ctx->GetWriteSchema().empty());
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetTempDirectory().empty());
    ASSERT_TRUE(ctx->GetOptions().empty());
    ASSERT_TRUE(ctx->GetFileSystemSchemeToIdentifierMap().empty());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
    ASSERT_FALSE(ctx->GetCatalog());
    ASSERT_FALSE(ctx->GetIdentifier());
}

TEST(WriteContextTest, TestWithCatalog) {
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    WriteContextBuilder builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.WithCatalog(catalog, Identifier("db1", "t1")).Finish());
    ASSERT_EQ(ctx->GetCatalog(), catalog);
    ASSERT_EQ(ctx->GetIdentifier(), std::optional<Identifier>(Identifier("db1", "t1")));
    ASSERT_OK_AND_ASSIGN(auto next_ctx, builder.Finish());
    ASSERT_FALSE(next_ctx->GetCatalog());
    ASSERT_FALSE(next_ctx->GetIdentifier());

    ASSERT_NOK_WITH_MSG(builder.WithCatalog(nullptr, Identifier("db1", "t1")).Finish(),
                        "cannot write through a null catalog");
}

TEST(WriteContextTest, TestCatalogAddressesBranchByIdentifier) {
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    WriteContextBuilder builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.WithCatalog(catalog, Identifier("db1", "t1$branch_dev"))
                                       .WithBranch("dev")
                                       .AddOption(Options::BRANCH, "dev")
                                       .Finish());
    ASSERT_EQ(ctx->GetBranch(), "dev");

    WriteContextBuilder identifier_builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(
        auto identifier_ctx,
        identifier_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_dev")).Finish());
    ASSERT_EQ(identifier_ctx->GetBranch(), "dev");

    for (int32_t branch_source = 0; branch_source < 2; ++branch_source) {
        SCOPED_TRACE(branch_source);
        WriteContextBuilder option_only_builder("table_root_path", "commit_user_1");
        option_only_builder.WithCatalog(catalog, Identifier("db1", "t1"));
        if (branch_source == 0) {
            option_only_builder.WithBranch("dev");
        } else {
            option_only_builder.AddOption(Options::BRANCH, "dev");
        }
        ASSERT_NOK_WITH_MSG(option_only_builder.Finish(),
                            "name branch 'dev' there as 't1$branch_dev'");
    }

    WriteContextBuilder mixed_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(mixed_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_dev"))
                            .WithBranch("release")
                            .Finish(),
                        "but both 'dev' and 'release' were named");

    WriteContextBuilder main_builder("table_root_path", "commit_user_1");
    ASSERT_OK(main_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_main"))
                  .WithBranch("")
                  .AddOption(Options::BRANCH, "main")
                  .Finish());

    WriteContextBuilder upper_main_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(
        upper_main_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_MAIN")).Finish(),
        "a catalog names branch 'MAIN' as it names the main branch");

    WriteContextBuilder constructed_builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(
        auto constructed_ctx,
        constructed_builder.WithCatalog(catalog, Identifier("db1", "t1", "dev")).Finish());
    ASSERT_EQ(constructed_ctx->GetBranch(), "dev");
    WriteContextBuilder system_table_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(
        system_table_builder.WithCatalog(catalog, Identifier("db1", "t1", "dev$options")).Finish(),
        "Cannot 'write' for system table");
}

TEST(WriteContextTest, TestBranch) {
    WriteContextBuilder option_builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(auto option_ctx, option_builder.AddOption(Options::BRANCH, "rt").Finish());
    ASSERT_EQ("rt", option_ctx->GetBranch());

    WriteContextBuilder upper_main_builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(auto upper_main_ctx, upper_main_builder.WithBranch("MAIN").Finish());
    ASSERT_EQ("MAIN", upper_main_ctx->GetBranch());

    // An empty branch selects the main branch and stays accepted.
    WriteContextBuilder main_builder("table_root_path", "commit_user_1");
    main_builder.WithBranch("");
    ASSERT_OK_AND_ASSIGN(auto ctx, main_builder.Finish());
    ASSERT_EQ(BranchManager::DEFAULT_MAIN_BRANCH, ctx->GetBranch());
    WriteContextBuilder empty_builder("table_root_path", "commit_user_1");
    ASSERT_NOK_WITH_MSG(empty_builder.WithBranch("").AddOption(Options::BRANCH, "dev").Finish(),
                        "but both 'dev' and 'main' were named");

    // The branch names a directory under the root path, so a value that is not a single path
    // component is rejected when the context is built.
    WriteContextBuilder escaping_builder("table_root_path", "commit_user_1");
    escaping_builder.WithBranch("rt/../../../../../outside");
    ASSERT_NOK_WITH_MSG(escaping_builder.Finish(), "branch name cannot contain path separators");
}

TEST(WriteContextTest, TestSetContent) {
    WriteContextBuilder builder("table_root_path", "commit_user_1");

    auto memory_pool = GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    auto file_system = std::make_shared<MockFileSystem>();
    std::vector<std::string> write_schema = {"f0", "f1"};
    std::map<std::string, std::string> fs_scheme_to_identifier_map = {{"file", "local"},
                                                                      {"oss", "jindo"}};

    ASSERT_OK_AND_ASSIGN(auto ctx,
                         builder.WithStreamingMode(true)
                             .WithIgnoreNumBucketCheck(true)
                             .WithIgnorePreviousFiles(true)
                             .WithMemoryPool(memory_pool)
                             .WithExecutor(executor)
                             .WithTempDirectory("/tmp/with-all")
                             .WithWriteId(123)
                             .WithBranch("test_branch")
                             .WithWriteSchema(write_schema)
                             .WithFileSystemSchemeToIdentifierMap(fs_scheme_to_identifier_map)
                             .WithFileSystem(file_system)
                             .AddOption("key", "value")
                             .Finish());

    ASSERT_TRUE(ctx->IsStreamingMode());
    ASSERT_TRUE(ctx->IgnoreNumBucketCheck());
    ASSERT_TRUE(ctx->IgnorePreviousFiles());
    ASSERT_FALSE(ctx->EnableMultiThreadSpill());
    ASSERT_EQ(ctx->GetMemoryPool(), memory_pool);
    ASSERT_EQ(ctx->GetExecutor(), executor);
    ASSERT_EQ(ctx->GetTempDirectory(), "/tmp/with-all");
    ASSERT_EQ(ctx->GetWriteId(), 123);
    ASSERT_EQ(ctx->GetBranch(), "test_branch");
    ASSERT_EQ(ctx->GetWriteSchema(), write_schema);
    ASSERT_EQ(ctx->GetFileSystemSchemeToIdentifierMap(), fs_scheme_to_identifier_map);
    ASSERT_EQ(ctx->GetSpecificFileSystem(), file_system);
    std::map<std::string, std::string> expected_options = {{"key", "value"}};
    ASSERT_EQ(expected_options, ctx->GetOptions());
}

TEST(WriteContextTest, TestSetOptionsOverridesAddedOptions) {
    WriteContextBuilder builder("table_root_path", "commit_user_1");
    builder.AddOption("old", "value");
    builder.SetOptions({{"key1", "value1"}, {"key2", "value2"}});

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    std::map<std::string, std::string> expected_options = {{"key1", "value1"}, {"key2", "value2"}};
    ASSERT_EQ(expected_options, ctx->GetOptions());
}

TEST(WriteContextTest, TestSetWriteBufferSpillThreadNumber) {
    WriteContextBuilder builder("table_root_path", "commit_user_1");
    builder.SetWriteBufferSpillThreadNumber(2);

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    ASSERT_TRUE(ctx->EnableMultiThreadSpill());
}

}  // namespace paimon::test
