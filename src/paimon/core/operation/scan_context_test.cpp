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
#include "paimon/core/schema/table_schema.h"
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
namespace {

/// A catalog serving one table schema and a file system of its own, so a test can tell the
/// per-table answers apart from the catalog-wide ones.
std::shared_ptr<MockVersionManagedCatalog> CatalogServing(
    const std::shared_ptr<FileSystem>& table_file_system, std::string* schema_json) {
    auto logical_schema = arrow::schema(
        {arrow::field("id", arrow::int64(), false), arrow::field("value", arrow::utf8())});
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema,
                         TableSchema::Create(0, logical_schema, /*partition_keys=*/{},
                                             /*primary_keys=*/{}, {}));
    EXPECT_OK_AND_ASSIGN(*schema_json, schema->GetJsonSchema());
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableSchema(schema);
    catalog->SetTableFileSystem(table_file_system);
    return catalog;
}

}  // namespace

TEST(ScanContextTest, TestWithCatalog) {
    auto table_fs = std::make_shared<MockFileSystem>();
    std::string schema_json;
    std::shared_ptr<MockVersionManagedCatalog> catalog = CatalogServing(table_fs, &schema_json);

    ScanContextBuilder builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.WithCatalog(catalog, Identifier("db1", "t1")).Finish());
    ASSERT_EQ(ctx->GetCatalog(), catalog);
    ASSERT_EQ(ctx->GetIdentifier(), std::optional<Identifier>(Identifier("db1", "t1")));
    // The file system a catalog issuing per-table credentials hands out for this table, and the
    // schema the metastore keeps, are both resolved when the context is built.
    ASSERT_EQ(ctx->GetSpecificFileSystem(), table_fs);
    ASSERT_EQ(ctx->GetSpecificTableSchema(), std::optional<std::string>(schema_json));
    ASSERT_EQ(catalog->TableFileSystemRequests().size(), 1U);
    ASSERT_EQ(catalog->TableFileSystemRequests().front(), Identifier("db1", "t1"));
    ASSERT_EQ(catalog->LoadTableSchemaCalls(), 1U);

    // Finish() resets the builder, so the next context scans without the catalog.
    ASSERT_OK_AND_ASSIGN(auto next_ctx, builder.Finish());
    ASSERT_FALSE(next_ctx->GetCatalog());
    ASSERT_FALSE(next_ctx->GetIdentifier());
    ASSERT_FALSE(next_ctx->GetSpecificFileSystem());
    ASSERT_FALSE(next_ctx->GetSpecificTableSchema());

    ASSERT_NOK_WITH_MSG(builder.WithCatalog(nullptr, Identifier("db1", "t1")).Finish(),
                        "cannot scan through a null catalog");
}

TEST(ScanContextTest, TestCatalogAnswersAreOverridable) {
    auto table_fs = std::make_shared<MockFileSystem>();
    std::string schema_json;
    std::shared_ptr<MockVersionManagedCatalog> catalog = CatalogServing(table_fs, &schema_json);

    // A file system the caller gave is the one that is used, and the catalog is not asked for one.
    auto given_fs = std::make_shared<MockFileSystem>();
    ScanContextBuilder fs_builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(
        auto fs_ctx,
        fs_builder.WithCatalog(catalog, Identifier("db1", "t1")).WithFileSystem(given_fs).Finish());
    ASSERT_EQ(fs_ctx->GetSpecificFileSystem(), given_fs);
    ASSERT_TRUE(catalog->TableFileSystemRequests().empty());

    // A schema the caller gave spares the catalog request entirely. A fresh catalog keeps the
    // count clean of the request the file-system-override builder above already made.
    std::string schema_catalog_json;
    std::shared_ptr<MockVersionManagedCatalog> schema_catalog =
        CatalogServing(table_fs, &schema_catalog_json);
    ScanContextBuilder schema_builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto schema_ctx,
                         schema_builder.WithCatalog(schema_catalog, Identifier("db1", "t1"))
                             .SetTableSchema("table-schema-json")
                             .Finish());
    ASSERT_EQ(schema_ctx->GetSpecificTableSchema(),
              std::optional<std::string>("table-schema-json"));
    ASSERT_EQ(schema_catalog->LoadTableSchemaCalls(), 0U);
}

TEST(ScanContextTest, TestCatalogScansTheMainBranchOnly) {
    auto table_fs = std::make_shared<MockFileSystem>();
    std::string schema_json;
    std::shared_ptr<MockVersionManagedCatalog> catalog = CatalogServing(table_fs, &schema_json);
    for (int32_t branch_source = 0; branch_source < 2; ++branch_source) {
        SCOPED_TRACE(branch_source);
        ScanContextBuilder builder("table_root_path");
        builder.WithCatalog(catalog, Identifier("db1", branch_source == 0 ? "t1$branch_dev" : "t1"))
            .AddOption(Options::BRANCH, branch_source == 1 ? "dev" : "main");
        ASSERT_NOK_WITH_MSG(builder.Finish(), "it cannot be aimed at branch 'dev'");
    }

    ScanContextBuilder main_builder("table_root_path");
    ASSERT_OK(main_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_main"))
                  .AddOption(Options::BRANCH, "main")
                  .Finish());
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
    ASSERT_FALSE(ctx->GetCatalog());
    ASSERT_FALSE(ctx->GetIdentifier());
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
