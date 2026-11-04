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
    ASSERT_TRUE(ctx->GetWriteSchema().empty());
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetTempDirectory().empty());
    ASSERT_TRUE(ctx->GetFileSystemSchemeToIdentifierMap().empty());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
    ASSERT_FALSE(ctx->GetSpecificTableSchema());
    ASSERT_FALSE(ctx->GetCatalog());
    ASSERT_FALSE(ctx->GetIdentifier());
    ASSERT_FALSE(ctx->GetTableId());
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

TEST(WriteContextTest, TestWithCatalogDefersTableFileSystem) {
    auto table_fs = std::make_shared<MockFileSystem>();
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(table_fs);

    WriteContextBuilder builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.WithCatalog(catalog, Identifier("db1", "t1")).Finish());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
    ASSERT_TRUE(catalog->TableFileSystemRequests().empty());

    WriteContextBuilder map_builder("table_root_path", "commit_user_1");
    ASSERT_OK_AND_ASSIGN(auto map_ctx, map_builder.WithCatalog(catalog, Identifier("db1", "t1"))
                                           .WithFileSystemSchemeToIdentifierMap({{"file", "local"}})
                                           .Finish());
    ASSERT_FALSE(map_ctx->GetSpecificFileSystem());
    ASSERT_TRUE(catalog->TableFileSystemRequests().empty());
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
                             .WithWriteSchema(write_schema)
                             .WithFileSystemSchemeToIdentifierMap(fs_scheme_to_identifier_map)
                             .WithFileSystem(file_system)
                             .SetTableSchema("table-schema-json")
                             .Finish());

    ASSERT_TRUE(ctx->IsStreamingMode());
    ASSERT_TRUE(ctx->IgnoreNumBucketCheck());
    ASSERT_TRUE(ctx->IgnorePreviousFiles());
    ASSERT_FALSE(ctx->EnableMultiThreadSpill());
    ASSERT_EQ(ctx->GetMemoryPool(), memory_pool);
    ASSERT_EQ(ctx->GetExecutor(), executor);
    ASSERT_EQ(ctx->GetTempDirectory(), "/tmp/with-all");
    ASSERT_EQ(ctx->GetWriteId(), 123);
    ASSERT_EQ(ctx->GetWriteSchema(), write_schema);
    ASSERT_EQ(ctx->GetFileSystemSchemeToIdentifierMap(), fs_scheme_to_identifier_map);
    ASSERT_EQ(ctx->GetSpecificFileSystem(), file_system);
    ASSERT_TRUE(ctx->GetSpecificTableSchema().has_value());
    ASSERT_EQ("table-schema-json", ctx->GetSpecificTableSchema().value());
    std::map<std::string, std::string> expected_options = {{"key", "value"}};
}

TEST(WriteContextTest, TestSetWriteBufferSpillThreadNumber) {
    WriteContextBuilder builder("table_root_path", "commit_user_1");
    builder.SetWriteBufferSpillThreadNumber(2);

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    ASSERT_TRUE(ctx->EnableMultiThreadSpill());
}

}  // namespace paimon::test
