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
#include "paimon/catalog/table_load_context.h"
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

TEST(ScanContextTest, TestDefaultValue) {
    ScanContextBuilder builder;
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_FALSE(ctx->IsStreamingMode());
    ASSERT_FALSE(ctx->GetLimit());
    ASSERT_TRUE(ctx->GetMemoryPool());
    ASSERT_TRUE(ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetScanFilters());
    ASSERT_FALSE(ctx->GetScanFilters()->GetBucketFilter());
    ASSERT_FALSE(ctx->GetScanFilters()->GetPredicate());
    ASSERT_TRUE(ctx->GetScanFilters()->GetPartitionFilters().empty());
    ASSERT_FALSE(ctx->GetGlobalIndexResult());
}

TEST(ScanContextTest, TestSetContent) {
    ScanContextBuilder builder;
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

    builder.WithStreamingMode(true);
    builder.WithMemoryPool(memory_pool);
    builder.WithExecutor(executor);
    auto fs = std::make_shared<MockFileSystem>();

    auto manifest_cache = std::make_shared<LruCache>(1024);
    builder.WithCache(manifest_cache);
    builder.SetReadFieldNames({"f1", "f2"});
    ASSERT_OK_AND_ASSIGN(auto ctx, builder.Finish());
    ASSERT_TRUE(ctx->IsStreamingMode());
    ASSERT_EQ(1000, ctx->GetLimit());
    ASSERT_TRUE(ctx->GetScanFilters());
    ASSERT_EQ(10, ctx->GetScanFilters()->GetBucketFilter());
    ASSERT_EQ(*predicate, *(ctx->GetScanFilters()->GetPredicate()));
    ASSERT_EQ(partition_filters, ctx->GetScanFilters()->GetPartitionFilters());
    ASSERT_EQ("{1,2,4,5}", ctx->GetGlobalIndexResult()->ToString());
    ASSERT_EQ(memory_pool, ctx->GetMemoryPool());
    ASSERT_EQ(executor, ctx->GetExecutor());
    ASSERT_TRUE(ctx->GetCache());
    ASSERT_EQ(std::optional<std::vector<std::string>>(std::vector<std::string>{"f1", "f2"}),
              ctx->GetReadFieldNames());
}

TEST(ScanContextTest, TestDefaultExecutorIsCreatedPerContext) {
    // A builder without WithExecutor() gives every context a default executor
    // of its own; nothing is shared across contexts.
    ScanContextBuilder first_builder;
    ASSERT_OK_AND_ASSIGN(auto first_ctx, first_builder.Finish());
    ScanContextBuilder second_builder;
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
