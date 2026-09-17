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

#include "paimon/read_context.h"

#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
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

TEST(ReadContextTest, TestWithCatalog) {
    auto table_fs = std::make_shared<MockFileSystem>();
    std::string schema_json;
    std::shared_ptr<MockVersionManagedCatalog> catalog = CatalogServing(table_fs, &schema_json);

    ReadContextBuilder builder("table_root_path");
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

    // Finish() resets the builder, so the next context reads without the catalog.
    ASSERT_OK_AND_ASSIGN(auto next_ctx, builder.Finish());
    ASSERT_FALSE(next_ctx->GetCatalog());
    ASSERT_FALSE(next_ctx->GetIdentifier());
    ASSERT_FALSE(next_ctx->GetSpecificFileSystem());
    ASSERT_FALSE(next_ctx->GetSpecificTableSchema());

    ASSERT_NOK_WITH_MSG(builder.WithCatalog(nullptr, Identifier("db1", "t1")).Finish(),
                        "cannot read through a null catalog");
}

TEST(ReadContextTest, TestCatalogAnswersAreOverridable) {
    auto table_fs = std::make_shared<MockFileSystem>();
    std::string schema_json;
    std::shared_ptr<MockVersionManagedCatalog> catalog = CatalogServing(table_fs, &schema_json);
    auto given_fs = std::make_shared<MockFileSystem>();

    // A file system the caller gave is the one that is used, and the catalog is not asked for one.
    ReadContextBuilder fs_builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(
        auto fs_ctx,
        fs_builder.WithCatalog(catalog, Identifier("db1", "t1")).WithFileSystem(given_fs).Finish());
    ASSERT_EQ(fs_ctx->GetSpecificFileSystem(), given_fs);
    ASSERT_TRUE(catalog->TableFileSystemRequests().empty());

    // So is a scheme map, which names file systems the same way for the paths it covers.
    ReadContextBuilder map_builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto map_ctx, map_builder.WithCatalog(catalog, Identifier("db1", "t1"))
                                           .WithFileSystemSchemeToIdentifierMap({{"file", "local"}})
                                           .Finish());
    ASSERT_FALSE(map_ctx->GetSpecificFileSystem());
    ASSERT_TRUE(catalog->TableFileSystemRequests().empty());

    // A schema the caller gave spares the catalog request entirely. A fresh catalog keeps the
    // count clean of the requests the schema-less builders above already made.
    std::string schema_catalog_json;
    std::shared_ptr<MockVersionManagedCatalog> schema_catalog =
        CatalogServing(table_fs, &schema_catalog_json);
    ReadContextBuilder schema_builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto schema_ctx,
                         schema_builder.WithCatalog(schema_catalog, Identifier("db1", "t1"))
                             .SetTableSchema("table-schema-json")
                             .Finish());
    ASSERT_EQ(schema_ctx->GetSpecificTableSchema(),
              std::optional<std::string>("table-schema-json"));
    ASSERT_EQ(schema_catalog->LoadTableSchemaCalls(), 0U);
}

TEST(ReadContextTest, TestCatalogReadsTheMainBranchOnly) {
    auto table_fs = std::make_shared<MockFileSystem>();
    std::string schema_json;
    std::shared_ptr<MockVersionManagedCatalog> catalog = CatalogServing(table_fs, &schema_json);
    for (int32_t branch_source = 0; branch_source < 3; ++branch_source) {
        SCOPED_TRACE(branch_source);
        ReadContextBuilder builder("table_root_path");
        builder.WithCatalog(catalog, Identifier("db1", branch_source == 0 ? "t1$branch_dev" : "t1"))
            .WithBranch(branch_source == 1 ? "dev" : "main")
            .AddOption(Options::BRANCH, branch_source == 2 ? "dev" : "main");
        ASSERT_NOK_WITH_MSG(builder.Finish(), "it cannot be aimed at branch 'dev'");
    }

    ReadContextBuilder main_builder("table_root_path");
    ASSERT_OK(main_builder.WithCatalog(catalog, Identifier("db1", "t1$branch_main"))
                  .WithBranch("")
                  .AddOption(Options::BRANCH, "main")
                  .Finish());
}

TEST(ReadContextTest, TestDefaultValue) {
    ReadContextBuilder builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_EQ(ctx->GetPath(), "table_root_path");
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetReadFieldNames().empty());
    ASSERT_TRUE(ctx->GetReadFieldIds().empty());
    ASSERT_TRUE(ctx->GetOptions().empty());
    ASSERT_FALSE(ctx->GetPredicate());
    ASSERT_FALSE(ctx->EnablePredicateFilter());
    ASSERT_FALSE(ctx->EnablePrefetch());
    ASSERT_TRUE(ctx->ReadAheadCacheEnabled());
    ASSERT_EQ(WarmupLevel::RAW, ctx->GetWarmupLevel());
    ASSERT_EQ(600, ctx->GetPrefetchBatchCount());
    ASSERT_EQ(3, ctx->GetPrefetchMaxParallelNum());
    ASSERT_FALSE(ctx->EnableMultiThreadRowToBatch());
    ASSERT_EQ(1, ctx->GetRowToBatchThreadNumber());
    ASSERT_EQ("main", ctx->GetBranch());
    ASSERT_TRUE(ctx->GetFileSystemSchemeToIdentifierMap().empty());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
    ASSERT_FALSE(ctx->GetCatalog());
    ASSERT_FALSE(ctx->GetIdentifier());
}

TEST(ReadContextTest, TestSetContent) {
    ReadContextBuilder builder("table_root_path");
    std::shared_ptr<MemoryPool> memory_pool = GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    CacheConfig cache_config;
    cache_config.SetRangeSizeLimit(512);
    cache_config.SetHoleSizeLimit(128);
    cache_config.SetPreBufferLimit(2048);

    builder.AddOption("key", "value");
    builder.SetReadFieldNames({"f1", "f2"});
    builder.SetReadFieldIds({0, 1});
    auto predicate =
        PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f1", FieldType::INT);
    builder.SetPredicate(predicate);
    builder.EnablePredicateFilter(true);
    builder.EnablePrefetch(true);
    builder.SetReadAheadCacheEnabled(false);
    builder.SetWarmupLevel(WarmupLevel::DECODED);
    builder.SetPrefetchBatchCount(1200);
    builder.SetPrefetchMaxParallelNum(6);
    builder.EnableMultiThreadRowToBatch(true);
    builder.SetRowToBatchThreadNumber(9);
    builder.WithMemoryPool(memory_pool);
    builder.WithExecutor(executor);
    builder.SetTableSchema("table-schema-json");
    builder.WithBranch("rt");
    builder.WithCacheConfig(cache_config);
    auto fs = std::make_shared<MockFileSystem>();
    builder.WithFileSystem(fs);
    auto manifest_cache = std::make_shared<LruCache>(1024);
    builder.WithCache(manifest_cache);
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    // test result
    ASSERT_EQ(ctx->GetPath(), "table_root_path");
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_EQ(ctx->GetReadFieldNames(), std::vector<std::string>({"f1", "f2"}));
    ASSERT_EQ(ctx->GetReadFieldIds(), std::vector<int32_t>({0, 1}));
    ASSERT_EQ(*predicate, *(ctx->GetPredicate()));
    ASSERT_TRUE(ctx->EnablePredicateFilter());
    ASSERT_TRUE(ctx->EnablePrefetch());
    ASSERT_FALSE(ctx->ReadAheadCacheEnabled());
    ASSERT_EQ(WarmupLevel::DECODED, ctx->GetWarmupLevel());
    ASSERT_EQ(1200, ctx->GetPrefetchBatchCount());
    ASSERT_EQ(6, ctx->GetPrefetchMaxParallelNum());
    ASSERT_TRUE(ctx->EnableMultiThreadRowToBatch());
    ASSERT_EQ(9, ctx->GetRowToBatchThreadNumber());
    ASSERT_EQ(memory_pool, ctx->GetMemoryPool());
    ASSERT_EQ(executor, ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetSpecificTableSchema().has_value());
    ASSERT_EQ("table-schema-json", ctx->GetSpecificTableSchema().value());
    ASSERT_EQ("rt", ctx->GetBranch());
    ASSERT_EQ(512U, ctx->GetCacheConfig().GetRangeSizeLimit());
    ASSERT_EQ(128U, ctx->GetCacheConfig().GetHoleSizeLimit());
    ASSERT_EQ(2048U, ctx->GetCacheConfig().GetPreBufferLimit());
    ASSERT_TRUE(ctx->GetFileSystemSchemeToIdentifierMap().empty());
    std::map<std::string, std::string> expected_options = {{"key", "value"}};
    ASSERT_EQ(expected_options, ctx->GetOptions());
    ASSERT_EQ(ctx->GetSpecificFileSystem(), fs);
    ASSERT_TRUE(ctx->GetCache());
}

TEST(ReadContextTest, TestSetWarmupLevel) {
    for (WarmupLevel level : {WarmupLevel::NONE, WarmupLevel::RAW, WarmupLevel::DECODED}) {
        ReadContextBuilder builder("table_root_path");
        // The setter hands back the builder so it chains like every other setter on it.
        ASSERT_EQ(&builder, &builder.SetWarmupLevel(level));
        ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
        ASSERT_EQ(level, ctx->GetWarmupLevel());
    }

    // Finish() resets the builder, so reusing one must not carry the previous warmup level over.
    ReadContextBuilder builder("table_root_path");
    builder.SetWarmupLevel(WarmupLevel::NONE);
    ASSERT_OK_AND_ASSIGN(auto first_ctx, builder.Finish());
    ASSERT_EQ(WarmupLevel::NONE, first_ctx->GetWarmupLevel());
    ASSERT_OK_AND_ASSIGN(auto second_ctx, builder.Finish());
    ASSERT_EQ(WarmupLevel::RAW, second_ctx->GetWarmupLevel());
}

TEST(ReadContextTest, TestSetOptionsOverridesAddedOptions) {
    ReadContextBuilder builder("table_root_path");
    builder.AddOption("old", "value");
    builder.SetOptions({{"key1", "value1"}, {"key2", "value2"}});

    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    std::map<std::string, std::string> expected_options = {{"key1", "value1"}, {"key2", "value2"}};
    ASSERT_EQ(expected_options, ctx->GetOptions());
}

TEST(ReadContextTest, TestBranch) {
    ReadContextBuilder option_builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto option_ctx, option_builder.AddOption(Options::BRANCH, "rt").Finish());
    ASSERT_EQ("rt", option_ctx->GetBranch());

    ReadContextBuilder agreeing_builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(
        auto agreeing_ctx,
        agreeing_builder.WithBranch("rt").AddOption(Options::BRANCH, "rt").Finish());
    ASSERT_EQ("rt", agreeing_ctx->GetBranch());

    ReadContextBuilder mixed_builder("table_root_path");
    ASSERT_NOK_WITH_MSG(mixed_builder.WithBranch("rt").AddOption(Options::BRANCH, "dev").Finish(),
                        "but both 'dev' and 'rt' were named");

    // An empty branch selects the main branch and stays accepted.
    ReadContextBuilder main_builder("table_root_path");
    main_builder.WithBranch("");
    ASSERT_OK_AND_ASSIGN(auto ctx, main_builder.Finish());
    ASSERT_EQ(BranchManager::DEFAULT_MAIN_BRANCH, ctx->GetBranch());
    ReadContextBuilder empty_builder("table_root_path");
    ASSERT_NOK_WITH_MSG(empty_builder.WithBranch("").AddOption(Options::BRANCH, "dev").Finish(),
                        "but both 'dev' and 'main' were named");

    // The branch names a directory under the table path, so a value that is not a single path
    // component is rejected when the context is built.
    ReadContextBuilder escaping_builder("table_root_path");
    escaping_builder.WithBranch("rt/../../../../../outside");
    ASSERT_NOK_WITH_MSG(escaping_builder.Finish(), "branch name cannot contain path separators");
}

TEST(ReadContextTest, TestFileSystemAndSchemeMapConflict) {
    ReadContextBuilder builder("table_root_path");
    auto fs = std::make_shared<MockFileSystem>();
    builder.WithFileSystem(fs);
    builder.WithFileSystemSchemeToIdentifierMap({{"file", "local"}});
    ASSERT_NOK_WITH_MSG(
        builder.Finish(),
        "WithFileSystem() and WithFileSystemSchemeToIdentifierMap() cannot be used together");
}

TEST(ReadContextTest, TestSchemeMapWithoutFileSystem) {
    ReadContextBuilder builder("table_root_path");
    builder.WithFileSystemSchemeToIdentifierMap({{"file", "local"}});
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    std::map<std::string, std::string> expected_fs_map = {{"file", "local"}};
    ASSERT_EQ(expected_fs_map, ctx->GetFileSystemSchemeToIdentifierMap());
    ASSERT_FALSE(ctx->GetSpecificFileSystem());
}

TEST(ReadContextTest, TestPrefetchMaxParallelNumZero) {
    ReadContextBuilder builder("table_root_path");
    builder.EnablePrefetch(true);
    builder.SetPrefetchMaxParallelNum(0);
    ASSERT_NOK_WITH_MSG(builder.Finish(), "prefetch max parallel num should be greater than 0");
}

TEST(ReadContextTest, TestSetReadSchemaAndHasReadSchema) {
    auto projected_schema = arrow::schema({arrow::field("f0", arrow::utf8())});
    auto c_schema = std::make_unique<ArrowSchema>();
    auto* c_schema_raw = c_schema.get();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    {
        ReadContextBuilder builder("table_root_path");
        builder.SetReadSchema(std::move(c_schema));
        ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
        ASSERT_TRUE(ctx->HasReadSchema());
        ASSERT_EQ(ctx->GetReadSchema(), c_schema_raw);
    }

    ASSERT_EQ(c_schema, nullptr);
}

TEST(ReadContextTest, TestSetInvalidReadSchemaIgnored) {
    auto invalid_schema = std::make_unique<ArrowSchema>();

    ReadContextBuilder builder("table_root_path");
    builder.SetReadSchema(std::move(invalid_schema));
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    ASSERT_FALSE(ctx->HasReadSchema());
    ASSERT_EQ(ctx->GetReadSchema(), nullptr);
}

}  // namespace paimon::test
