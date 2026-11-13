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
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/table_load_context.h"
#include "paimon/catalog_options.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_catalog.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(ReadContextTest, TestDefaultValue) {
    ReadContextBuilder builder;
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetReadFieldNames().empty());
    ASSERT_TRUE(ctx->GetReadFieldIds().empty());
    ASSERT_FALSE(ctx->GetPredicate());
    ASSERT_FALSE(ctx->HasVectorOrFullTextSearch());
    ASSERT_FALSE(ctx->EnablePreciseFileIndexEvaluate());
    ASSERT_FALSE(ctx->EnablePredicateFilter());
    ASSERT_FALSE(ctx->EnablePrefetch());
    ASSERT_TRUE(ctx->ReadAheadCacheEnabled());
    ASSERT_EQ(WarmupLevel::RAW, ctx->GetWarmupLevel());
    ASSERT_EQ(600, ctx->GetPrefetchBatchCount());
    ASSERT_EQ(3, ctx->GetPrefetchMaxParallelNum());
    ASSERT_FALSE(ctx->EnableMultiThreadRowToBatch());
    ASSERT_EQ(1, ctx->GetRowToBatchThreadNumber());
}

TEST(ReadContextTest, TestSetContent) {
    ReadContextBuilder builder;
    std::shared_ptr<MemoryPool> memory_pool = GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    CacheConfig cache_config;
    cache_config.SetRangeSizeLimit(512);
    cache_config.SetHoleSizeLimit(128);
    cache_config.SetPreBufferLimit(2048);

    builder.SetReadFieldNames({"f1", "f2"});
    builder.SetReadFieldIds({0, 1});
    auto predicate =
        PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f1", FieldType::INT);
    builder.SetPredicate(predicate);
    builder.EnablePreciseFileIndexEvaluate(true);
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

    builder.WithCacheConfig(cache_config);
    auto fs = std::make_shared<MockFileSystem>();

    auto manifest_cache = std::make_shared<LruCache>(1024);
    builder.WithCache(manifest_cache);
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    // test result
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_EQ(ctx->GetReadFieldNames(), std::vector<std::string>({"f1", "f2"}));
    ASSERT_EQ(ctx->GetReadFieldIds(), std::vector<int32_t>({0, 1}));
    ASSERT_EQ(*predicate, *(ctx->GetPredicate()));
    ASSERT_TRUE(ctx->EnablePreciseFileIndexEvaluate());
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
    ASSERT_EQ(512U, ctx->GetCacheConfig().GetRangeSizeLimit());
    ASSERT_EQ(128U, ctx->GetCacheConfig().GetHoleSizeLimit());
    ASSERT_EQ(2048U, ctx->GetCacheConfig().GetPreBufferLimit());
    ASSERT_TRUE(ctx->GetCache());
}

TEST(ReadContextTest, TestSetWarmupLevel) {
    for (WarmupLevel level : {WarmupLevel::NONE, WarmupLevel::RAW, WarmupLevel::DECODED}) {
        ReadContextBuilder builder;
        // The setter hands back the builder so it chains like every other setter on it.
        ASSERT_EQ(&builder, &builder.SetWarmupLevel(level));
        ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
        ASSERT_EQ(level, ctx->GetWarmupLevel());
    }

    // Finish() resets the builder, so reusing one must not carry the previous warmup level over.
    ReadContextBuilder builder;
    builder.SetWarmupLevel(WarmupLevel::NONE);
    ASSERT_OK_AND_ASSIGN(auto first_ctx, builder.Finish());
    ASSERT_EQ(WarmupLevel::NONE, first_ctx->GetWarmupLevel());
    ASSERT_OK_AND_ASSIGN(auto second_ctx, builder.Finish());
    ASSERT_EQ(WarmupLevel::RAW, second_ctx->GetWarmupLevel());
}

TEST(ReadContextTest, TestSearchRequiresPreciseFileIndexEvaluate) {
    auto vector =
        std::make_shared<VectorSearch>("embedding", /*limit=*/3, std::vector<float>{1.0f}, nullptr,
                                       nullptr, std::nullopt, std::map<std::string, std::string>{});
    auto text = std::make_shared<FullTextSearch>("body", R"({"match":{"query":"apple"}})", 3);
    auto match = std::make_shared<FullTextMatch>("body", text->query);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> full_text,
                         PredicateBuilder::FullTextMatch(0, match));
    auto scalar = PredicateBuilder::IsNotNull(1, "price", FieldType::INT);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> disjunction,
                         PredicateBuilder::Or({scalar, full_text}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({scalar, disjunction}));
    for (int32_t search_type = 0; search_type < 3; ++search_type) {
        for (bool enable_before_search : {false, true}) {
            ReadContextBuilder builder;
            builder.EnablePreciseFileIndexEvaluate(enable_before_search);
            if (search_type == 0) {
                builder.SetVectorSearch(vector);
            } else if (search_type == 1) {
                builder.SetFullTextSearch(text);
            } else {
                builder.SetPredicate(predicate);
            }
            if (!enable_before_search) {
                builder.EnablePreciseFileIndexEvaluate(true);
            }
            ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
            ASSERT_TRUE(context->EnablePreciseFileIndexEvaluate());
        }
    }
}

TEST(ReadContextTest, TestPrefetchMaxParallelNumZero) {
    ReadContextBuilder builder;
    builder.EnablePrefetch(true);
    builder.SetPrefetchMaxParallelNum(0);
}

TEST(ReadContextTest, TestSetReadSchemaAndHasReadSchema) {
    auto projected_schema = arrow::schema({arrow::field("f0", arrow::utf8())});
    auto c_schema = std::make_unique<ArrowSchema>();
    auto* c_schema_raw = c_schema.get();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    {
        ReadContextBuilder builder;
        builder.SetReadSchema(std::move(c_schema));
        ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
        ASSERT_TRUE(ctx->HasReadSchema());
        ASSERT_EQ(ctx->GetReadSchema(), c_schema_raw);
    }

    ASSERT_EQ(c_schema, nullptr);
}

TEST(ReadContextTest, TestSetInvalidReadSchemaIgnored) {
    auto invalid_schema = std::make_unique<ArrowSchema>();

    ReadContextBuilder builder;
    builder.SetReadSchema(std::move(invalid_schema));
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());

    ASSERT_FALSE(ctx->HasReadSchema());
    ASSERT_EQ(ctx->GetReadSchema(), nullptr);
}

TEST(ReadContextTest, TestFileIndexSearchConfigurationAndReset) {
    std::shared_ptr<VectorSearch> vector_search = std::make_shared<VectorSearch>(
        "embedding", /*limit=*/3, std::vector<float>{1.0f, 2.0f}, nullptr, nullptr,
        VectorSearch::DistanceType::COSINE, std::map<std::string, std::string>{});
    ReadContextBuilder builder;
    ASSERT_EQ(&builder, &builder.SetVectorSearch(vector_search));
    builder.EnablePreciseFileIndexEvaluate(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> vector_context, builder.Finish());
    EXPECT_EQ(vector_search, vector_context->GetVectorSearch());
    EXPECT_FALSE(vector_context->GetFullTextSearch());
    EXPECT_TRUE(vector_context->HasVectorOrFullTextSearch());

    std::shared_ptr<FullTextSearch> full_text_search =
        std::make_shared<FullTextSearch>("body", R"({"match":{"query":"paimon"}})", /*limit=*/5);
    ASSERT_EQ(&builder, &builder.SetFullTextSearch(full_text_search));
    builder.EnablePreciseFileIndexEvaluate(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> full_text_context, builder.Finish());
    EXPECT_FALSE(full_text_context->GetVectorSearch());
    EXPECT_EQ(full_text_search, full_text_context->GetFullTextSearch());
    EXPECT_TRUE(full_text_context->HasVectorOrFullTextSearch());

    // Finish resets search configuration just like every other per-read option.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> reset_context, builder.Finish());
    EXPECT_FALSE(reset_context->HasVectorOrFullTextSearch());
    EXPECT_FALSE(reset_context->EnablePreciseFileIndexEvaluate());
}

TEST(ReadContextTest, TestRejectConflictingFileIndexSearch) {
    std::shared_ptr<VectorSearch> vector_search =
        std::make_shared<VectorSearch>("embedding", /*limit=*/3, std::vector<float>{1.0f}, nullptr,
                                       nullptr, std::nullopt, std::map<std::string, std::string>{});
    std::shared_ptr<FullTextSearch> full_text_search =
        std::make_shared<FullTextSearch>("body", R"({"match":{"query":"paimon"}})", /*limit=*/5);
    ReadContextBuilder conflicting;
    conflicting.EnablePreciseFileIndexEvaluate(true)
        .SetVectorSearch(vector_search)
        .SetFullTextSearch(full_text_search);
    ASSERT_NOK_WITH_MSG(conflicting.Finish(), "cannot be configured together");
}

TEST(ReadContextTest, TestFullTextPredicateAndRootTopKConfiguration) {
    auto match = std::make_shared<FullTextMatch>("body", R"({"match":{"query":"apple"}})");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> text,
                         PredicateBuilder::FullTextMatch(0, match));
    auto scalar = PredicateBuilder::LessThan(1, "price", FieldType::INT, Literal(10));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> filter, PredicateBuilder::Or({text, scalar}));
    auto vector =
        std::make_shared<VectorSearch>("embedding", 3, std::vector<float>{1.0f}, nullptr, nullptr,
                                       std::nullopt, std::map<std::string, std::string>{});
    ReadContextBuilder builder;
    builder.EnablePreciseFileIndexEvaluate(true)
        .SetReadFieldNames({"body", "price"})
        .SetPredicate(filter)
        .SetVectorSearch(vector);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
    ASSERT_EQ(context->GetPredicate(), filter);
    ASSERT_EQ(context->GetVectorSearch(), vector);

    auto ranked_text = std::make_shared<FullTextSearch>("title", match->query, 2);
    builder.EnablePreciseFileIndexEvaluate(true).SetPredicate(filter).SetFullTextSearch(
        ranked_text);
    ASSERT_OK_AND_ASSIGN(context, builder.Finish());
    ASSERT_EQ(context->GetPredicate(), filter);
    ASSERT_EQ(context->GetFullTextSearch(), ranked_text);
}

TEST(ReadContextTest, TestRootTopKValidation) {
    auto vector =
        std::make_shared<VectorSearch>("embedding", 0, std::vector<float>{1.0f}, nullptr, nullptr,
                                       std::nullopt, std::map<std::string, std::string>{});
    ReadContextBuilder vector_builder;
    vector_builder.EnablePreciseFileIndexEvaluate(true).SetVectorSearch(vector);
    ASSERT_NOK_WITH_MSG(vector_builder.Finish(), "VectorSearch TopK limit must be positive");
    auto text = std::make_shared<FullTextSearch>("body", R"({"match":{"query":"apple"}})", -1);
    ReadContextBuilder text_builder;
    text_builder.EnablePreciseFileIndexEvaluate(true).SetFullTextSearch(text);
    ASSERT_NOK_WITH_MSG(text_builder.Finish(), "FullTextSearch TopK limit must be positive");
}

}  // namespace paimon::test
