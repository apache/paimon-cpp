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
#include "paimon/catalog_options.h"
#include "paimon/common/io/cache/lru_cache.h"
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
TEST(ReadContextTest, TestWithCatalogResolvesTableFileSystem) {
    // WithCatalog is a shorthand for WithFileSystem(catalog->GetTableFileSystem(identifier)); it
    // resolves the per-table file system when Finish() builds the context and sets nothing else.
    auto table_fs = std::make_shared<MockFileSystem>();
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(table_fs);

    ReadContextBuilder builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.WithCatalog(catalog, Identifier("db1", "t1")).Finish());
    ASSERT_EQ(ctx->GetSpecificFileSystem(), table_fs);
    ASSERT_EQ(catalog->TableFileSystemRequests().size(), 1U);
    ASSERT_EQ(catalog->TableFileSystemRequests().front(), Identifier("db1", "t1"));

    // Finish() resets the builder, so the next context reads without the catalog.
    ASSERT_OK_AND_ASSIGN(auto next_ctx, builder.Finish());
    ASSERT_FALSE(next_ctx->GetSpecificFileSystem());
    ASSERT_EQ(catalog->TableFileSystemRequests().size(), 1U);
}

TEST(ReadContextTest, TestWithFileSystemOverridesCatalog) {
    // A file system the caller gave is the one that is used, and the catalog is not asked for one.
    auto table_fs = std::make_shared<MockFileSystem>();
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(table_fs);
    auto given_fs = std::make_shared<MockFileSystem>();

    ReadContextBuilder builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(
        auto ctx,
        builder.WithCatalog(catalog, Identifier("db1", "t1")).WithFileSystem(given_fs).Finish());
    ASSERT_EQ(ctx->GetSpecificFileSystem(), given_fs);
    ASSERT_TRUE(catalog->TableFileSystemRequests().empty());
}

TEST(ReadContextTest, TestWithCatalogNullRejected) {
    ReadContextBuilder builder("table_root_path");
    ASSERT_NOK_WITH_MSG(builder.WithCatalog(nullptr, Identifier("db1", "t1")).Finish(),
                        "cannot read through a null catalog");
}

TEST(ReadContextTest, TestReadViaHeaderReportsTheStartingTable) {
    // A read through a REST catalog carries the table it started from, so that the metastore can
    // authorize a dependency table - the blob table behind a BlobView - through that table's owner.
    // The value is the identifier's flat JSON in the form-urlencoded flavor the REST server
    // expects, byte for byte what the Java client sends.
    const std::string expected = "%7B%22database%22%3A%22db1%22%2C%22object%22%3A%22t1%22%7D";
    for (const std::string metastore : {"rest"}) {
        auto catalog = std::make_shared<MockVersionManagedCatalog>();
        catalog->SetTableFileSystem(std::make_shared<MockFileSystem>());

        ReadContextBuilder builder("table_root_path");
        builder.AddOption(CatalogOptions::METASTORE, metastore);
        ASSERT_OK_AND_ASSIGN(auto ctx,
                             builder.WithCatalog(catalog, Identifier("db1", "t1")).Finish());

        const std::map<std::string, std::string>& options = ctx->GetOptions();
        ASSERT_EQ(options.count("header.X-Paimon-Read-Via"), 1u) << metastore;
        ASSERT_EQ(options.at("header.X-Paimon-Read-Via"), expected) << metastore;
        // The metastore is the only other option, so the header is added rather than anything
        // rewritten around it.
        ASSERT_EQ(options.size(), 2u) << metastore;
    }
}

TEST(ReadContextTest, TestReadViaHeaderKeepsTheOutermostView) {
    // A view read on behalf of another view arrives with the header already set, and the outermost
    // view is the one the metastore hears about: the option in hand wins, as it does in the Java
    // `CatalogEnvironment.dependencyReadContext()`.
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(std::make_shared<MockFileSystem>());

    ReadContextBuilder builder("table_root_path");
    builder.AddOption(CatalogOptions::METASTORE, "rest");
    builder.AddOption("header.X-Paimon-Read-Via", "outer-view");
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.WithCatalog(catalog, Identifier("db1", "t1")).Finish());
    ASSERT_EQ(ctx->GetOptions().at("header.X-Paimon-Read-Via"), "outer-view");
}

TEST(ReadContextTest, TestReadViaHeaderNeedsARestCatalogAndATable) {
    // Only a REST metastore turns a `header.` option into a request header, and only a read naming
    // a table has one to report: the two guards the Java counterpart checks before copying the
    // options over. A catalog that is not one is told nothing.
    for (const std::string metastore : {"filesystem", "hive", "alake"}) {
        auto catalog = std::make_shared<MockVersionManagedCatalog>();
        catalog->SetTableFileSystem(std::make_shared<MockFileSystem>());

        ReadContextBuilder builder("table_root_path");
        builder.AddOption(CatalogOptions::METASTORE, metastore);
        ASSERT_OK_AND_ASSIGN(auto ctx,
                             builder.WithCatalog(catalog, Identifier("db1", "t1")).Finish());
        ASSERT_EQ(ctx->GetOptions().count("header.X-Paimon-Read-Via"), 0u) << metastore;
    }

    // Nor is one added when the options leave the metastore out, which reads as a filesystem
    // catalog, or when the read goes through no catalog and so names no table - however the
    // metastore is spelled, since `Catalog::Create` matches it in lower case.
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(std::make_shared<MockFileSystem>());
    ReadContextBuilder no_metastore_builder("table_root_path");
    ASSERT_OK_AND_ASSIGN(
        auto no_metastore_ctx,
        no_metastore_builder.WithCatalog(catalog, Identifier("db1", "t1")).Finish());
    ASSERT_EQ(no_metastore_ctx->GetOptions().count("header.X-Paimon-Read-Via"), 0u);

    ReadContextBuilder no_catalog_builder("table_root_path");
    no_catalog_builder.AddOption(CatalogOptions::METASTORE, "REST");
    ASSERT_OK_AND_ASSIGN(auto no_catalog_ctx, no_catalog_builder.Finish());
    ASSERT_EQ(no_catalog_ctx->GetOptions().count("header.X-Paimon-Read-Via"), 0u);
}

TEST(ReadContextTest, TestReadViaHeaderReportsTheBranchOfTheStartingTable) {
    // What is reported is the object name, the branch suffix included, because that is what the
    // Java `getObjectName()` its serializer reads returns: a read of a branch says so instead of
    // naming the data table. The metastore is matched in upper case here as well.
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(std::make_shared<MockFileSystem>());

    ReadContextBuilder builder("table_root_path");
    builder.AddOption(CatalogOptions::METASTORE, "REST");
    ASSERT_OK_AND_ASSIGN(auto ctx,
                         builder.WithCatalog(catalog, Identifier("db1", "t1", "dev")).Finish());
    ASSERT_EQ(ctx->GetOptions().at("header.X-Paimon-Read-Via"),
              "%7B%22database%22%3A%22db1%22%2C%22object%22%3A%22t1%24branch_dev%22%7D");
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
