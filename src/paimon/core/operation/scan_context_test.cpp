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

#include "paimon/scan_context.h"

#include "gtest/gtest.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_catalog.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(ScanContextTest, TestWithCatalogResolvesTableFileSystem) {
    // WithCatalog is a shorthand for WithFileSystem(catalog->GetTableFileSystem(identifier)); it
    // resolves the per-table file system when Finish() builds the context, which also keeps the
    // catalog and identifier for the operators to resolve the table through.
    auto table_fs = std::make_shared<MockFileSystem>();
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(table_fs);

    ScanContextBuilder builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.WithCatalog(catalog, Identifier("db1", "t1")).Finish());
    ASSERT_EQ(ctx->GetSpecificFileSystem(), table_fs);
    ASSERT_EQ(ctx->GetCatalog(), catalog);
    ASSERT_EQ(ctx->GetIdentifier(), std::optional<Identifier>(Identifier("db1", "t1")));
    ASSERT_EQ(catalog->TableFileSystemRequests().size(), 1U);
    ASSERT_EQ(catalog->TableFileSystemRequests().front(), Identifier("db1", "t1"));

    // Finish() resets the builder, so the next context scans without the catalog.
    ASSERT_OK_AND_ASSIGN(auto next_ctx, builder.Finish());
    ASSERT_FALSE(next_ctx->GetSpecificFileSystem());
    ASSERT_FALSE(next_ctx->GetCatalog());
    ASSERT_FALSE(next_ctx->GetIdentifier());
    ASSERT_EQ(catalog->TableFileSystemRequests().size(), 1U);
}

TEST(ScanContextTest, TestWithFileSystemOverridesCatalog) {
    // A file system the caller gave is the one that is used, and the catalog is not asked for one.
    auto table_fs = std::make_shared<MockFileSystem>();
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(table_fs);
    auto given_fs = std::make_shared<MockFileSystem>();

    ScanContextBuilder builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(
        auto ctx,
        builder.WithCatalog(catalog, Identifier("db1", "t1")).WithFileSystem(given_fs).Finish());
    ASSERT_EQ(ctx->GetSpecificFileSystem(), given_fs);
    ASSERT_TRUE(catalog->TableFileSystemRequests().empty());
}

TEST(ScanContextTest, TestWithCatalogResolvesTheBranch) {
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(std::make_shared<MockFileSystem>());

    for (const std::string branch : {"MAIN", "Main"}) {
        const std::string message =
            "a catalog names branch '" + branch + "' as it names the main branch";
        ASSERT_NOK_WITH_MSG(ScanContextBuilder("table_root_path")
                                .WithCatalog(catalog, Identifier("db1", "t1"))
                                .AddOption(Options::BRANCH, branch)
                                .Finish(),
                            message);
        ASSERT_NOK_WITH_MSG(ScanContextBuilder("table_root_path")
                                .WithCatalog(catalog, Identifier("db1", "t1$branch_" + branch))
                                .Finish(),
                            message);
    }
    ASSERT_NOK_WITH_MSG(ScanContextBuilder("table_root_path")
                            .WithCatalog(catalog, Identifier("db1", "t1"))
                            .AddOption(Options::BRANCH, "dev$options")
                            .Finish(),
                        "a branch a catalog addresses cannot contain '$'");
    ASSERT_NOK_WITH_MSG(ScanContextBuilder("table_root_path")
                            .WithCatalog(catalog, Identifier("db1", "t1", "dev"))
                            .AddOption(Options::BRANCH, "test")
                            .Finish(),
                        "a scan is aimed at one branch, but both 'dev' and 'test' were named");
    ASSERT_TRUE(catalog->TableFileSystemRequests().empty());
    ASSERT_EQ(0u, catalog->LoadTableSchemaCalls());
    ASSERT_OK_AND_ASSIGN(
        auto path_ctx,
        ScanContextBuilder("table_root_path").AddOption(Options::BRANCH, "MAIN").Finish());
    ASSERT_EQ("MAIN", path_ctx->GetOptions().at(Options::BRANCH));

    ASSERT_OK_AND_ASSIGN(auto ctx, ScanContextBuilder("table_root_path")
                                       .WithCatalog(catalog, Identifier("db1", "t1", "dev"))
                                       .Finish());
    ASSERT_EQ("dev", ctx->GetOptions().at(Options::BRANCH));
    ASSERT_OK_AND_ASSIGN(auto option_ctx, ScanContextBuilder("table_root_path")
                                              .WithCatalog(catalog, Identifier("db1", "t1"))
                                              .AddOption(Options::BRANCH, "dev")
                                              .Finish());
    ASSERT_EQ("dev", option_ctx->GetOptions().at(Options::BRANCH));
    ASSERT_EQ(Identifier("db1", "t1$branch_dev"), option_ctx->GetIdentifier().value());
    ASSERT_OK_AND_ASSIGN(auto system_ctx, ScanContextBuilder("table_root_path")
                                              .WithCatalog(catalog, Identifier("db1", "t1$files"))
                                              .AddOption(Options::BRANCH, "dev")
                                              .Finish());
    ASSERT_EQ(Identifier("db1", "t1$branch_dev$files"), system_ctx->GetIdentifier().value());
    ASSERT_OK_AND_ASSIGN(auto sys_ctx, ScanContextBuilder("table_root_path")
                                           .WithCatalog(catalog, Identifier("sys", "tables"))
                                           .AddOption(Options::BRANCH, "dev")
                                           .Finish());
    ASSERT_EQ(Identifier("sys", "tables"), sys_ctx->GetIdentifier().value());
    ASSERT_OK_AND_ASSIGN(auto main_ctx, ScanContextBuilder("table_root_path")
                                            .WithCatalog(catalog, Identifier("db1", "t1"))
                                            .Finish());
    ASSERT_EQ(0u, main_ctx->GetOptions().count(Options::BRANCH));
    ASSERT_EQ(Identifier("db1", "t1"), main_ctx->GetIdentifier().value());
    ASSERT_EQ(std::vector<Identifier>({Identifier("db1", "t1$branch_dev"),
                                       Identifier("db1", "t1$branch_dev"),
                                       Identifier("db1", "t1$branch_dev$files"),
                                       Identifier("sys", "tables"), Identifier("db1", "t1")}),
              catalog->TableFileSystemRequests());
}

TEST(ScanContextTest, TestWithCatalogNullRejected) {
    ScanContextBuilder builder("table_root_path");
    ASSERT_NOK_WITH_MSG(builder.WithCatalog(nullptr, Identifier("db1", "t1")).Finish(),
                        "cannot scan through a null catalog");
}

TEST(ScanContextTest, TestDefaultValue) {
    ScanContextBuilder builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_EQ(ctx->GetPath(), "table_root_path");
    ASSERT_FALSE(ctx->IsStreamingMode());
    ASSERT_FALSE(ctx->GetLimit());
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetScanFilters());
    ASSERT_FALSE(ctx->GetScanFilters()->GetBucketFilter());
    ASSERT_FALSE(ctx->GetScanFilters()->GetPredicate());
    ASSERT_TRUE(ctx->GetScanFilters()->GetPartitionFilters().empty());
    ASSERT_TRUE(ctx->GetOptions().empty());
    ASSERT_FALSE(ctx->GetGlobalIndexResult());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
    ASSERT_FALSE(ctx->GetReadFieldNames());
}

TEST(ScanContextTest, TestSetContent) {
    ScanContextBuilder builder("table_root_path");
    std::shared_ptr<MemoryPool> memory_pool = GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();

    builder.SetBucketFilter(10);
    std::vector<std::map<std::string, std::string>> partition_filters = {{{"f1", "20"}}};
    builder.SetPartitionFilter(partition_filters);
    auto predicate =
        PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"f2", FieldType::INT);
    builder.SetPredicate(predicate);
    std::vector<Range> row_ranges = {Range(1, 2), Range(4, 5)};
    auto global_index_result = BitmapGlobalIndexResult::FromRanges(row_ranges);
    builder.SetGlobalIndexResult(global_index_result);
    builder.SetLimit(1000);
    builder.AddOption("key", "value");
    builder.WithStreamingMode(true);
    builder.WithMemoryPool(memory_pool);
    builder.WithExecutor(executor);
    auto fs = std::make_shared<MockFileSystem>();
    builder.WithFileSystem(fs);
    builder.SetTableSchema("table-schema-json");
    auto manifest_cache = std::make_shared<LruCache>(1024);
    builder.WithCache(manifest_cache);
    builder.SetReadFieldNames({"f1", "f2"});
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_EQ(ctx->GetPath(), "table_root_path");
    ASSERT_TRUE(ctx->IsStreamingMode());
    ASSERT_EQ(1000, ctx->GetLimit());
    ASSERT_TRUE(ctx->GetScanFilters());
    ASSERT_EQ(10, ctx->GetScanFilters()->GetBucketFilter());
    ASSERT_EQ(*predicate, *(ctx->GetScanFilters()->GetPredicate()));
    ASSERT_EQ(partition_filters, ctx->GetScanFilters()->GetPartitionFilters());
    ASSERT_EQ("{1,2,4,5}", ctx->GetGlobalIndexResult()->ToString());
    ASSERT_EQ(memory_pool, ctx->GetMemoryPool());
    ASSERT_EQ(executor, ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetSpecificTableSchema().has_value());
    ASSERT_EQ("table-schema-json", ctx->GetSpecificTableSchema().value());
    std::map<std::string, std::string> expected_options = {{"key", "value"}};
    ASSERT_EQ(expected_options, ctx->GetOptions());
    ASSERT_EQ(fs, ctx->GetSpecificFileSystem());
    ASSERT_TRUE(ctx->GetCache());
    ASSERT_EQ(std::optional<std::vector<std::string>>(std::vector<std::string>{"f1", "f2"}),
              ctx->GetReadFieldNames());
}

TEST(ScanContextTest, TestSetOptionsOverridesAddedOptions) {
    ScanContextBuilder builder("table_root_path");
    builder.AddOption("old", "value");
    builder.SetOptions({{"key1", "value1"}, {"key2", "value2"}});

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    std::map<std::string, std::string> expected_options = {{"key1", "value1"}, {"key2", "value2"}};
    ASSERT_EQ(expected_options, ctx->GetOptions());
}

TEST(ScanContextTest, TestDefaultExecutorIsCreatedPerContext) {
    // A builder without WithExecutor() gives every context a default executor
    // of its own; nothing is shared across contexts.
    ScanContextBuilder first_builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto first_ctx, first_builder.Finish());
    ScanContextBuilder second_builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto second_ctx, second_builder.Finish());
    ASSERT_TRUE(first_ctx->GetExecutor());
    ASSERT_TRUE(second_ctx->GetExecutor());
    ASSERT_NE(first_ctx->GetExecutor(), second_ctx->GetExecutor());
    // Neither falls back to the process wide singleton.
    ASSERT_NE(GetGlobalDefaultExecutor(), first_ctx->GetExecutor());
    ASSERT_NE(GetGlobalDefaultExecutor(), second_ctx->GetExecutor());

    // Finish() resets the builder; an explicit executor set before does not
    // leak into the next context built from the same builder.
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    first_builder.WithExecutor(executor);
    ASSERT_OK_AND_ASSIGN(auto explicit_ctx, first_builder.Finish());
    ASSERT_EQ(executor, explicit_ctx->GetExecutor());
    ASSERT_OK_AND_ASSIGN(auto reset_ctx, first_builder.Finish());
    ASSERT_TRUE(reset_ctx->GetExecutor());
    ASSERT_NE(executor, reset_ctx->GetExecutor());
}

}  // namespace paimon::test
