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

#include "paimon/indexer/lumina/lumina_realtime_index.h"

#include <atomic>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/indexer/lumina/lumina_memory_pool.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/realtime/index/realtime_index_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::lumina::test {

TEST(LuminaMemoryPoolTest, TracksAllocationsSeparatelyForSharedPool) {
    std::shared_ptr<MemoryPool> pool = GetMemoryPool();
    LuminaMemoryPool first(/*track_memory=*/true, pool);
    LuminaMemoryPool second(/*track_memory=*/true, pool);
    void* first_ptr = first.allocate(64);
    ScopeGuard first_guard([&]() { first.deallocate(first_ptr, 64); });
    void* second_ptr = second.allocate(128);
    ScopeGuard second_guard([&]() { second.deallocate(second_ptr, 128); });
    ASSERT_EQ(64, first.GetMemoryUsage());
    ASSERT_EQ(128, second.GetMemoryUsage());
    ASSERT_EQ(192, pool->CurrentUsage());
    {
        LuminaMemoryPool untracked(/*track_memory=*/false, pool);
        void* ptr = untracked.allocate(32);
        ScopeGuard guard([&]() { untracked.deallocate(ptr, 32); });
        ASSERT_EQ(0, untracked.GetMemoryUsage());
        ASSERT_EQ(224, pool->CurrentUsage());
    }
    ASSERT_EQ(192, pool->CurrentUsage());
    {
        void* extra = first.allocate(32);
        ScopeGuard extra_guard([&]() { first.deallocate(extra, 32); });
        ASSERT_EQ(96, first.GetMemoryUsage());
    }
    ASSERT_EQ(64, first.GetMemoryUsage());
    ASSERT_EQ(128, second.GetMemoryUsage());
}

class LuminaRealtimeIndexTest : public ::testing::TestWithParam<std::string> {
 protected:
    void SetUp() override {
        options_ = {{"lumina.index.type", GetParam()},
                    {"lumina.index.dimension", "2"},
                    {"lumina.distance.metric", "l2"},
                    {"lumina.encoding.type", "rawf32"}};
        ASSERT_OK_AND_ASSIGN(index_, LuminaRealtimeIndex::Create(schema_, options_, pool_));
    }

    Result<std::shared_ptr<arrow::StructArray>> MakeBatch(const std::string& json) const {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> batch,
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json));
        return checked_pointer_cast<arrow::StructArray>(batch);
    }

    Status AddArray(const std::shared_ptr<arrow::Array>& array) {
        ArrowArray batch;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &batch));
        ScopeGuard guard([&batch]() { ArrowArrayRelease(&batch); });
        return index_->AddBatch(&batch);
    }

    Status Add(const std::string& json) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::StructArray> batch, MakeBatch(json));
        return AddArray(batch);
    }

    Result<std::shared_ptr<RealtimeIndexReader>> Reader(int64_t begin, int64_t end) const {
        return index_->CreateReader(OffsetRange(begin, end), query_pool_);
    }

    static Result<std::shared_ptr<RealtimeIndex>> CreateConfiguredIndex(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& pool) {
        ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &c_schema));
        ScopeGuard guard([&]() { ArrowSchemaRelease(&c_schema); });
        return RealtimeIndexFactory::Get("lumina", &c_schema, options, pool);
    }

    std::shared_ptr<VectorSearch> Search(int32_t limit) const {
        return std::make_shared<VectorSearch>(
            "embedding", limit, std::vector<float>{0.0f, 0.0f}, nullptr, nullptr,
            VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
    }

    void Check(const std::shared_ptr<ScoredRealtimeIndexResult>& result,
               const RoaringBitmap64& offsets, const std::vector<float>& scores) const {
        ASSERT_TRUE(result);
        ASSERT_EQ(offsets, result->GetRowPositions());
        ASSERT_EQ(scores.size(), result->GetScores().size());
        for (size_t i = 0; i < scores.size(); ++i) {
            ASSERT_NEAR(scores[i], result->GetScores()[i], 1e-5);
        }
    }

    std::shared_ptr<MemoryPool> pool_ = GetMemoryPool();
    std::shared_ptr<MemoryPool> query_pool_ = GetMemoryPool();
    std::shared_ptr<arrow::Schema> schema_ =
        arrow::schema({arrow::field(SpecialFields::RealtimeOffset().Name(), arrow::int64(), false),
                       arrow::field("embedding", arrow::list(arrow::float32()))});
    std::map<std::string, std::string> options_;
    std::shared_ptr<LuminaRealtimeIndex> index_;
};

TEST_P(LuminaRealtimeIndexTest, FactoryUsesOnlyTheRequestedColumnOptions) {
    const std::map<std::string, std::string> options{
        {"realtime.index.lumina.columns", "embedding,other"},
        {"realtime.index.lumina.embedding.index.type", GetParam()},
        {"realtime.index.lumina.embedding.index.dimension", "2"},
        {"realtime.index.lumina.embedding.distance.metric", "l2"},
        {"realtime.index.lumina.embedding.encoding.type", "rawf32"},
        {"realtime.index.lumina.other.index.type", GetParam()},
        {"realtime.index.lumina.other.index.dimension", "3"},
        {"realtime.index.lumina.other.distance.metric", "l2"},
        {"realtime.index.lumina.other.encoding.type", "rawf32"},
        {"lumina.index.dimension", "invalid"},
        {"file-index.lumina.embedding.index.dimension", "invalid"}};
    for (const std::string name : {"embedding", "other"}) {
        const int32_t dimension = name == "embedding" ? 2 : 3;
        auto schema = arrow::schema({schema_->field(0), schema_->field(1)->WithName(name)});
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndex> index,
                             CreateConfiguredIndex(schema, options, pool_));
        const std::string json = dimension == 2 ? "[[0,[1,0]]]" : "[[0,[1,0,0]]]";
        auto data =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json);
        ASSERT_TRUE(data.ok()) << data.status().ToString();
        ArrowArray batch;
        ASSERT_TRUE(arrow::ExportArray(*data.ValueOrDie(), &batch).ok());
        ScopeGuard guard([&]() { ArrowArrayRelease(&batch); });
        ASSERT_OK(index->AddBatch(&batch));
        ASSERT_EQ(nullptr, batch.release);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader,
                             index->CreateReader(OffsetRange(0, 1), query_pool_));
        auto search = std::make_shared<VectorSearch>(
            name, 1, std::vector<float>(dimension, 0.0f), nullptr, nullptr,
            VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                             reader->VisitVectorSearch(search));
        Check(result, RoaringBitmap64::From({0}), {1});
        search->query.resize(dimension + 1);
        ASSERT_NOK(reader->VisitVectorSearch(search));
    }
    auto missing_options = options;
    missing_options.erase("realtime.index.lumina.embedding.index.dimension");
    ASSERT_NOK(CreateConfiguredIndex(schema_, missing_options, pool_));
    ASSERT_NOK(RealtimeIndexFactory::Get("lumina", nullptr, options, pool_));
}

TEST_P(LuminaRealtimeIndexTest, ReportsVectorSearchCapabilityThroughBaseInterface) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 0));
    EXPECT_TRUE(reader->SupportsSearch(IndexReaderCapabilities::SearchType::VECTOR_SEARCH));
    EXPECT_FALSE(reader->SupportsSearch(IndexReaderCapabilities::SearchType::FULL_TEXT_SEARCH));
    EXPECT_FALSE(reader->SupportsSearch(IndexReaderCapabilities::SearchType::FULL_TEXT_MATCH));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(Search(/*limit=*/1)));
    Check(result, RoaringBitmap64(), {});
    std::shared_ptr<RealtimeIndexResult> base_result = result;
    EXPECT_TRUE(base_result->GetRowPositions().IsEmpty());
    ASSERT_NOK(reader->VisitFullTextSearch(nullptr));
    ASSERT_NOK(reader->VisitFullTextMatch(nullptr, /*needs_scores=*/true));
}

TEST_P(LuminaRealtimeIndexTest, SearchesAppendedBatchesWithSparse64BitOffsets) {
    ASSERT_OK(Add("[[2, [3, 0]], [8, [1, 0]]]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 9));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64::From({2, 8}), {9, 1});
    ASSERT_OK(Add("[[4294967300, [2, 0]]]"));
    ASSERT_OK_AND_ASSIGN(reader, Reader(0, 4294967301));
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64::From({2, 8, 4294967300}), {9, 1, 4});
}

TEST_P(LuminaRealtimeIndexTest, AppliesOffsetBoundsAndDeletionFilterBeforeTopK) {
    ASSERT_OK(Add("[[0, [0, 0]], [2, [1, 0]], [4, [2, 0]], [6, [3, 0]], [8, [4, 0]]]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(2, 8));
    auto search = Search(2);
    const RoaringBitmap64 deleted = RoaringBitmap64::From({2});
    search->pre_filter = [deleted](int64_t offset) { return !deleted.Contains(offset); };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(search));
    Check(result, RoaringBitmap64::From({4, 6}), {4, 9});
    search->pre_filter = [](int64_t) { return false; };
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(search));
    Check(result, RoaringBitmap64(), {});
}

TEST_P(LuminaRealtimeIndexTest, KeepsReaderBoundsAcrossAppends) {
    ASSERT_OK(Add("[[1, [1, 0]], [3, [2, 0]]]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(1, 4));
    ASSERT_OK(Add("[[4, [0, 0]], [7, [0.5, 0]]]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(Search(1)));
    Check(result, RoaringBitmap64::From({1}), {1});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> newer, Reader(4, 8));
    ASSERT_OK_AND_ASSIGN(result, newer->VisitVectorSearch(Search(1)));
    Check(result, RoaringBitmap64::From({4}), {0});
}

TEST_P(LuminaRealtimeIndexTest, HandlesNullVectorsAndSlicedBatches) {
    ASSERT_OK(Add("[[0, null], [1, null]]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 2));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64(), {});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> batch,
                         MakeBatch("[[2, [9, 9]], [3, null], [5, [2, 0]], [9, [1, 0]]]"));
    ASSERT_OK(AddArray(batch->Slice(1, 3)));
    ASSERT_OK_AND_ASSIGN(reader, Reader(0, 10));
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64::From({5, 9}), {4, 1});
}

TEST_P(LuminaRealtimeIndexTest, EmptyIndexAndEmptyRanges) {
    ASSERT_OK(Add("[]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 5));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64(), {});
    ASSERT_OK(Add("[[0, [0, 0]]]"));
    ASSERT_OK_AND_ASSIGN(reader, Reader(0, 0));
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64(), {});
    ASSERT_OK_AND_ASSIGN(reader, Reader(2, 3));
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64(), {});
    ASSERT_NOK(index_->CreateReader(OffsetRange(-1, 1), query_pool_));
    ASSERT_NOK(index_->CreateReader(OffsetRange(2, 1), query_pool_));
}

TEST_P(LuminaRealtimeIndexTest, RejectsNullRowsAndOffsets) {
    ASSERT_OK(Add("[[1, [1, 0]]]"));
    for (const std::string json : {"[[null, [0, 0]]]", "[null]"}) {
        ASSERT_NOK(Add(json)) << json;
    }
    ASSERT_OK(Add("[[2, null]]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 10));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64::From({1}), {1});
}

TEST_P(LuminaRealtimeIndexTest, ValidatesWholeBatchBeforeInsertingVectors) {
    for (const std::string json :
         {"[[0, [0, 0]], [1, [1]]]", "[[0, [0, 0]], [1, [1, null]]]",
          "[[0, [0, 0]], [1, [NaN, 0]]]", "[[0, [0, 0]], [1, [Infinity, 0]]]"}) {
        ASSERT_NOK(Add(json)) << json;
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 10));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64(), {});
    ASSERT_OK(Add("[[0, [1, 0]], [1, [2, 0]]]"));
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64::From({0, 1}), {1, 4});
}

TEST_P(LuminaRealtimeIndexTest, RejectsInvalidQueriesAndUnsupportedTags) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 10));
    ASSERT_NOK(reader->VisitVectorSearch(nullptr));
    auto search = Search(/*limit=*/10);
    search->field_name = "missing";
    ASSERT_NOK(reader->VisitVectorSearch(search));
    search = Search(0);
    ASSERT_NOK(reader->VisitVectorSearch(search));
    search = Search(-1);
    ASSERT_NOK(reader->VisitVectorSearch(search));
    search = Search(/*limit=*/10);
    search->query = {0};
    ASSERT_NOK(reader->VisitVectorSearch(search));
    search->query = {0, std::numeric_limits<float>::infinity()};
    ASSERT_NOK(reader->VisitVectorSearch(search));
    search = Search(/*limit=*/10);
    search->distance_type = VectorSearch::DistanceType::COSINE;
    ASSERT_NOK(reader->VisitVectorSearch(search));
    search = Search(/*limit=*/10);
    search->options = {{"lumina.index.type", "diskann"}};
    ASSERT_NOK(reader->VisitVectorSearch(search));
    search = Search(/*limit=*/10);
    search->predicate = PredicateBuilder::Equal(0, "tag", FieldType::INT, Literal(int32_t{1}));
    ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(search), "tags are not supported");
    ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap64> candidates, reader->VisitIsNull());
    ASSERT_FALSE(candidates.has_value());
}

TEST_P(LuminaRealtimeIndexTest, RejectsInvalidSchemasAndOptions) {
    ASSERT_NOK(LuminaRealtimeIndex::Create(nullptr, options_, pool_));
    ASSERT_NOK(LuminaRealtimeIndex::Create(schema_, options_, nullptr));
    for (const auto& schema :
         {arrow::schema({schema_->field(1), schema_->field(0)}), arrow::schema({schema_->field(0)}),
          arrow::schema(
              {schema_->field(0), schema_->field(1), arrow::field("tag", arrow::int32())}),
          arrow::schema({schema_->field(0), arrow::field("embedding", arrow::float32())}),
          arrow::schema(
              {schema_->field(0), arrow::field("embedding", arrow::list(arrow::float64()))}),
          arrow::schema({schema_->field(0)->WithName("other_offset"), schema_->field(1)}),
          arrow::schema(
              {schema_->field(0), schema_->field(1)->WithName(schema_->field(0)->name())}),
          arrow::schema({schema_->field(0)->WithNullable(true), schema_->field(1)})}) {
        ASSERT_NOK(LuminaRealtimeIndex::Create(schema, options_, pool_));
    }
    for (int64_t dimension : {int64_t{0}, int64_t{-1}, int64_t{1} << 33}) {
        auto options = options_;
        options["lumina.index.dimension"] = std::to_string(dimension);
        ASSERT_NOK(LuminaRealtimeIndex::Create(schema_, options, pool_));
    }
    auto options = options_;
    options["lumina.index.type"] = "diskann";
    ASSERT_NOK(LuminaRealtimeIndex::Create(schema_, options, pool_));
    options = options_;
    options["lumina.encoding.type"] = "fp16";
    ASSERT_NOK(LuminaRealtimeIndex::Create(schema_, options, pool_));
    options = options_;
    options["lumina.extension.build.tag.tag_schema"] = "[]";
    ASSERT_NOK_WITH_MSG(LuminaRealtimeIndex::Create(schema_, options, pool_),
                        "tags are not supported");
    ASSERT_NOK(index_->AddBatch(nullptr));
    ArrowArray released{};
    ASSERT_NOK(index_->AddBatch(&released));
}

TEST_P(LuminaRealtimeIndexTest, ParsesStringStreamerOptions) {
    options_["lumina." + GetParam() + ".streamer.rows_per_block"] = "64";
    options_["lumina.index.enable_experimental"] = "false";
    if (GetParam() == "flatnsw") {
        options_["lumina.flatnsw.build.neighbor_count"] = "16";
        options_["lumina.flatnsw.build.ef_construction"] = "32";
        options_["lumina.flatnsw.build.slack_pruning_factor"] = "1.25";
    }
    ASSERT_OK_AND_ASSIGN(index_, LuminaRealtimeIndex::Create(schema_, options_, pool_));
    ASSERT_OK(Add("[[0, [1, 0]]]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 1));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64::From({0}), {1});

    for (const std::string& key :
         {std::string("lumina.index.dimension"), std::string("lumina.index.enable_experimental"),
          "lumina." + GetParam() + ".streamer.capacity",
          "lumina." + GetParam() + ".streamer.rows_per_block"}) {
        auto options = options_;
        options[key] = "invalid";
        ASSERT_NOK(LuminaRealtimeIndex::Create(schema_, options, pool_)) << key;
    }
    if (GetParam() == "flatnsw") {
        options_["lumina.flatnsw.build.slack_pruning_factor"] = "invalid";
        ASSERT_NOK(LuminaRealtimeIndex::Create(schema_, options_, pool_));
    }
}

TEST_P(LuminaRealtimeIndexTest, UsesPrefixedOptionsWithoutDefaultingDistanceMetric) {
    options_["lumina.distance.metric"] = "cosine";
    options_["distance.metric"] = "l2";
    options_["index.dimension"] = "invalid";
    ASSERT_OK_AND_ASSIGN(index_, LuminaRealtimeIndex::Create(schema_, options_, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 1));
    auto search = Search(/*limit=*/10);
    ASSERT_NOK(reader->VisitVectorSearch(search));
    search->distance_type = VectorSearch::DistanceType::COSINE;
    search->query = {1, 0};
    ASSERT_OK(reader->VisitVectorSearch(search));

    options_.erase("lumina.distance.metric");
    ASSERT_NOK(LuminaRealtimeIndex::Create(schema_, options_, pool_));
}

TEST_P(LuminaRealtimeIndexTest, InsertionFailureKeepsPinnedRangeAndDisablesWrites) {
    options_["lumina." + GetParam() + ".streamer.capacity"] = "2";
    ASSERT_OK_AND_ASSIGN(index_, LuminaRealtimeIndex::Create(schema_, options_, pool_));
    ASSERT_OK(Add("[[0, [2, 0]]]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 1));
    ASSERT_NOK(Add("[[1, [0, 0]], [2, [1, 0]]]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(Search(1)));
    Check(result, RoaringBitmap64::From({0}), {4});
    ASSERT_NOK_WITH_MSG(Add("[[3, [3, 0]]]"), "cannot write after an insertion failure");
}

TEST_P(LuminaRealtimeIndexTest, SearchUsesSessionPoolAndFallsBackToReaderPool) {
    ASSERT_OK(Add("[[0, [1, 0]], [2, [2, 0]]]"));
    const uint64_t index_memory = index_->GetMemoryUsage();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 3));
    const uint64_t reader_usage = query_pool_->CurrentUsage();
    const uint64_t reader_peak = query_pool_->MaxMemoryUsage();
    std::shared_ptr<MemoryPool> session_pool = GetMemoryPool();
    auto search = Search(/*limit=*/10);
    search->pool = session_pool;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(search));
    Check(result, RoaringBitmap64::From({0, 2}), {1, 4});
    // Session allocations are backend-dependent; a search may use no pool memory.
    ASSERT_EQ(0, session_pool->CurrentUsage());
    ASSERT_EQ(reader_peak, query_pool_->MaxMemoryUsage());
    ASSERT_EQ(reader_usage, query_pool_->CurrentUsage());
    ASSERT_EQ(index_memory, index_->GetMemoryUsage());
    ASSERT_EQ(index_memory, pool_->CurrentUsage());

    search->pool.reset();
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(search));
    Check(result, RoaringBitmap64::From({0, 2}), {1, 4});
    ASSERT_EQ(reader_usage, query_pool_->CurrentUsage());
    ASSERT_EQ(index_memory, index_->GetMemoryUsage());
    ASSERT_EQ(index_memory, pool_->CurrentUsage());
    ASSERT_NOK_WITH_MSG(index_->CreateReader(OffsetRange(0, 3), nullptr),
                        "requires a query memory pool");
}

TEST_P(LuminaRealtimeIndexTest, ReaderPinsIndexAndMemoryAccountingIsPerIndex) {
    void* unrelated = pool_->Malloc(1024);
    ScopeGuard guard([&]() { pool_->Free(unrelated, 1024); });
    const uint64_t memory_before = index_->GetMemoryUsage();
    ASSERT_OK(Add("[[0, [1, 0]], [2, [2, 0]]]"));
    ASSERT_GT(index_->GetMemoryUsage(), memory_before);
    ASSERT_EQ(index_->GetMemoryUsage() + 1024, pool_->CurrentUsage());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 3));
    index_.reset();
    ASSERT_GT(pool_->CurrentUsage(), 1024);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredRealtimeIndexResult> result,
                         reader->VisitVectorSearch(Search(/*limit=*/10)));
    Check(result, RoaringBitmap64::From({0, 2}), {1, 4});
    reader.reset();
    ASSERT_EQ(1024, pool_->CurrentUsage());
}

TEST_P(LuminaRealtimeIndexTest, ConcurrentReadersWithSingleWriter) {
    ASSERT_OK(Add("[[0, [1, 0]]]"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeIndexReader> reader, Reader(0, 1));
    std::promise<void> start;
    std::shared_future<void> ready = start.get_future().share();
    std::atomic<int32_t> searches{0};
    std::atomic<bool> finished{false};
    auto read = [&]() {
        ready.wait();
        do {
            auto result = reader->VisitVectorSearch(Search(1));
            EXPECT_TRUE(result.ok());
            if (result.ok()) {
                Check(result.value(), RoaringBitmap64::From({0}), {1});
            }
            searches.fetch_add(1);
        } while (!finished.load());
    };
    std::thread first(read);
    std::thread second(read);
    start.set_value();
    // Keep the readers running for the whole sequence of append operations.
    while (searches.load() < 2) {
        std::this_thread::yield();
    }
    for (int64_t offset = 1; offset <= 64; ++offset) {
        EXPECT_OK(Add("[[" + std::to_string(offset) + ", [0, 0]]]"));
    }
    finished.store(true);
    first.join();
    second.join();
    ASSERT_GE(searches.load(), 2);
}

INSTANTIATE_TEST_SUITE_P(Backends, LuminaRealtimeIndexTest,
                         ::testing::Values("bruteforce", "flatnsw"));

}  // namespace paimon::lumina::test
