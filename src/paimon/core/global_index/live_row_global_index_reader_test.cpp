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

#include "paimon/core/global_index/live_row_global_index_reader.h"

#include <atomic>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/global_index/offset_global_index_reader.h"
#include "paimon/executor.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/testing/mock/mock_global_index_reader.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::test {
namespace {
// Applies filters to local row ids and scores each row using the query value and local row id.
class FilteringGlobalIndexReader : public MockGlobalIndexReader {
 public:
    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& search) override {
        return Search(search->pre_filter, search->query.front());
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& search) override {
        return Search(
            [search](int64_t row_id) {
                return !search->include_row_ids || search->include_row_ids->Contains(row_id);
            },
            static_cast<float>(search->limit));
    }

 private:
    Result<std::shared_ptr<ScoredGlobalIndexResult>> Search(const VectorSearch::PreFilter& filter,
                                                            float query_score) {
        RoaringBitmap64 rows;
        std::vector<float> scores;
        for (int64_t row_id = 0; row_id < 4; ++row_id) {
            if (!filter || filter(row_id)) {
                rows.Add(row_id);
                scores.push_back(query_score + static_cast<float>(row_id) + 0.1f);
            }
        }
        return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(rows), std::move(scores));
    }
};
}  // namespace

class LiveRowGlobalIndexReaderTest : public ::testing::Test {
 public:
    static LiveRowGlobalIndexReader::LiveRowsSupplier Supply(
        const std::optional<RoaringBitmap64>& live_rows, int32_t* call_count) {
        std::shared_ptr<const RoaringBitmap64> shared_live_rows;
        if (live_rows) {
            shared_live_rows = std::make_shared<const RoaringBitmap64>(live_rows.value());
        }
        return [shared_live_rows, call_count]() -> Result<std::shared_ptr<const RoaringBitmap64>> {
            (*call_count)++;
            return shared_live_rows;
        };
    }

    static RoaringBitmap64 LiveRows(int64_t from, int64_t to, const std::vector<int64_t>& deleted) {
        RoaringBitmap64 live_rows;
        live_rows.AddRange(from, to + 1);
        live_rows -= RoaringBitmap64::From(deleted);
        return live_rows;
    }

    static std::shared_ptr<FullTextSearch> Search(
        const std::optional<RoaringBitmap64>& include_row_ids = std::nullopt) {
        return std::make_shared<FullTextSearch>("f0", R"({"match":{"query":"apple"}})",
                                                /*limit=*/3, include_row_ids);
    }

    static std::shared_ptr<VectorSearch> SearchVector(
        VectorSearch::PreFilter pre_filter = nullptr) {
        return std::make_shared<VectorSearch>(
            "f1", /*limit=*/3, std::vector<float>({1.0f, 2.0f}), std::move(pre_filter),
            PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                    Literal(1)),
            VectorSearch::DistanceType::COSINE,
            std::map<std::string, std::string>({{"search.parallel", "2"}}));
    }

    static std::vector<int64_t> RowsPassingPreFilter(const VectorSearch& vector_search,
                                                     const std::vector<int64_t>& row_ids) {
        std::vector<int64_t> passed;
        for (int64_t row_id : row_ids) {
            if (vector_search.pre_filter(row_id)) {
                passed.push_back(row_id);
            }
        }
        return passed;
    }
};

TEST_F(LiveRowGlobalIndexReaderTest, TestSearchUnchangedWhenEveryRowIsLive) {
    auto mock_reader = std::make_shared<MockGlobalIndexReader>();
    mock_reader->SetScoredResult({11, 12}, {0.5f, 0.9f});
    int32_t call_count = 0;
    LiveRowGlobalIndexReader reader(mock_reader, Range(10, 14), Supply(std::nullopt, &call_count));
    std::shared_ptr<FullTextSearch> search = Search();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result,
                         reader.VisitFullTextSearch(search));
    ASSERT_EQ(mock_reader->CapturedFullTextSearch(), search);
    ASSERT_EQ(result->ToString(), "row ids: {11,12}, scores: {0.50,0.90}");
    ASSERT_EQ(call_count, 1);

    std::shared_ptr<VectorSearch> vector_search = SearchVector();
    ASSERT_OK_AND_ASSIGN(result, reader.VisitVectorSearch(vector_search));
    ASSERT_EQ(mock_reader->CapturedVectorSearch(), vector_search);
    ASSERT_EQ(result->ToString(), "row ids: {11,12}, scores: {0.50,0.90}");
    ASSERT_EQ(call_count, 2);

    LiveRowGlobalIndexReader live_shard_reader(mock_reader, Range(10, 14),
                                               Supply(LiveRows(0, 20, {9, 15}), &call_count));
    ASSERT_OK(live_shard_reader.VisitFullTextSearch(search));
    ASSERT_EQ(mock_reader->CapturedFullTextSearch(), search);
    ASSERT_OK(live_shard_reader.VisitVectorSearch(vector_search));
    ASSERT_EQ(mock_reader->CapturedVectorSearch(), vector_search);
    ASSERT_EQ(call_count, 4);
}

TEST_F(LiveRowGlobalIndexReaderTest, TestSearchOnlyLiveRowsOfShard) {
    auto mock_reader = std::make_shared<MockGlobalIndexReader>();
    mock_reader->SetScoredResult({11, 13}, {0.9f, 0.5f});
    int32_t call_count = 0;
    LiveRowGlobalIndexReader reader(mock_reader, Range(10, 14),
                                    Supply(LiveRows(0, 20, {1, 12}), &call_count));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result,
                         reader.VisitFullTextSearch(Search()));
    ASSERT_EQ(result->ToString(), "row ids: {11,13}, scores: {0.90,0.50}");
    std::shared_ptr<FullTextSearch> captured = mock_reader->CapturedFullTextSearch();
    ASSERT_TRUE(captured);
    ASSERT_EQ(captured->field_name, "f0");
    ASSERT_EQ(captured->query, R"({"match":{"query":"apple"}})");
    ASSERT_EQ(captured->limit, 3);
    ASSERT_TRUE(captured->include_row_ids);
    ASSERT_EQ(captured->include_row_ids.value(), RoaringBitmap64::From({10, 11, 13, 14}));

    std::shared_ptr<VectorSearch> vector_search = SearchVector();
    vector_search->pool = GetMemoryPool();
    ASSERT_OK_AND_ASSIGN(result, reader.VisitVectorSearch(vector_search));
    ASSERT_EQ(result->ToString(), "row ids: {11,13}, scores: {0.90,0.50}");
    std::shared_ptr<VectorSearch> captured_vector = mock_reader->CapturedVectorSearch();
    ASSERT_TRUE(captured_vector && captured_vector->pre_filter);
    ASSERT_EQ(captured_vector->field_name, vector_search->field_name);
    ASSERT_EQ(captured_vector->limit, vector_search->limit);
    ASSERT_EQ(captured_vector->query, vector_search->query);
    ASSERT_EQ(captured_vector->predicate, vector_search->predicate);
    ASSERT_TRUE(captured_vector->distance_type == vector_search->distance_type);
    ASSERT_EQ(captured_vector->options, vector_search->options);
    ASSERT_EQ(captured_vector->pool, vector_search->pool);
    std::vector<int64_t> row_ids = {1, 9, 10, 11, 12, 13, 14, 15};
    ASSERT_EQ(RowsPassingPreFilter(*captured_vector, row_ids),
              std::vector<int64_t>({10, 11, 13, 14}));

    ASSERT_OK(reader.VisitVectorSearch(SearchVector([](int64_t row_id) { return row_id != 13; })));
    captured_vector = mock_reader->CapturedVectorSearch();
    ASSERT_TRUE(captured_vector && captured_vector->pre_filter);
    ASSERT_EQ(RowsPassingPreFilter(*captured_vector, row_ids), std::vector<int64_t>({10, 11, 14}));

    constexpr int64_t kMax = RoaringBitmap64::MAX_VALUE;
    LiveRowGlobalIndexReader max_reader(mock_reader, Range(kMax - 1, kMax),
                                        Supply(RoaringBitmap64::From({kMax}), &call_count));
    ASSERT_OK(max_reader.VisitFullTextSearch(Search()));
    captured = mock_reader->CapturedFullTextSearch();
    ASSERT_TRUE(captured && captured->include_row_ids);
    ASSERT_EQ(captured->include_row_ids.value(), RoaringBitmap64::From({kMax}));
    ASSERT_OK(max_reader.VisitVectorSearch(SearchVector()));
    captured_vector = mock_reader->CapturedVectorSearch();
    ASSERT_TRUE(captured_vector && captured_vector->pre_filter);
    ASSERT_EQ(RowsPassingPreFilter(*captured_vector, {kMax - 1, kMax}),
              std::vector<int64_t>({kMax}));
}

TEST_F(LiveRowGlobalIndexReaderTest, TestIncludeRowIdsClippedToShard) {
    auto mock_reader = std::make_shared<MockGlobalIndexReader>();
    int32_t call_count = 0;
    LiveRowGlobalIndexReader reader(mock_reader, Range(10, 14),
                                    Supply(LiveRows(0, 20, {}), &call_count));
    ASSERT_OK(
        reader.VisitFullTextSearch(Search(RoaringBitmap64::From({1, 12, 20, int64_t{1} << 32}))));
    std::shared_ptr<FullTextSearch> captured = mock_reader->CapturedFullTextSearch();
    ASSERT_TRUE(captured && captured->include_row_ids);
    ASSERT_EQ(captured->include_row_ids.value(), RoaringBitmap64::From({12}));

    ASSERT_OK(reader.VisitFullTextSearch(Search(LiveRows(9, 15, {}))));
    captured = mock_reader->CapturedFullTextSearch();
    ASSERT_TRUE(captured);
    ASSERT_FALSE(captured->include_row_ids);
}

TEST_F(LiveRowGlobalIndexReaderTest, TestCreateUnionLoadsLiveRowsBeforeSearchingShards) {
    constexpr int64_t kSecondShardFrom = int64_t{1} << 32;
    auto first_shard = std::make_shared<MockGlobalIndexReader>();
    auto second_shard = std::make_shared<MockGlobalIndexReader>();
    second_shard->SetScoredResult({kSecondShardFrom}, {0.5f});
    auto live_rows = std::make_shared<const RoaringBitmap64>(
        RoaringBitmap64::From({kSecondShardFrom, kSecondShardFrom + 1}));
    std::mutex mutex;
    std::optional<std::thread::id> loading_thread;
    LiveRowGlobalIndexReader::LiveRowsSupplier supplier =
        [&]() -> Result<std::shared_ptr<const RoaringBitmap64>> {
        std::lock_guard<std::mutex> lock(mutex);
        if (!loading_thread) {
            loading_thread = std::this_thread::get_id();
        }
        return live_rows;
    };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor,
                         CreateDefaultExecutor(/*thread_count=*/1));
    std::shared_ptr<GlobalIndexReader> reader = LiveRowGlobalIndexReader::CreateUnion(
        std::vector<std::shared_ptr<GlobalIndexReader>>({first_shard, second_shard}),
        {Range(0, 1), Range(kSecondShardFrom, kSecondShardFrom + 1)},
        {Range(0, 1), Range(kSecondShardFrom, kSecondShardFrom + 1)}, executor, supplier);

    std::shared_ptr<FullTextSearch> search = Search();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result,
                         reader->VisitFullTextSearch(search));
    ASSERT_TRUE(loading_thread == std::this_thread::get_id());
    std::shared_ptr<FullTextSearch> first_search = first_shard->CapturedFullTextSearch();
    ASSERT_TRUE(first_search && first_search->include_row_ids);
    ASSERT_TRUE(first_search->include_row_ids->IsEmpty());
    ASSERT_EQ(second_shard->CapturedFullTextSearch(), search);
    ASSERT_EQ(result->ToString(), "row ids: {4294967296}, scores: {0.50}");

    loading_thread.reset();
    std::shared_ptr<VectorSearch> vector_search = SearchVector();
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(vector_search));
    ASSERT_TRUE(loading_thread == std::this_thread::get_id());
    std::shared_ptr<VectorSearch> first_vector_search = first_shard->CapturedVectorSearch();
    ASSERT_TRUE(first_vector_search && first_vector_search->pre_filter);
    ASSERT_TRUE(RowsPassingPreFilter(*first_vector_search, {0, 1}).empty());
    ASSERT_EQ(second_shard->CapturedVectorSearch(), vector_search);
    ASSERT_EQ(result->ToString(), "row ids: {4294967296}, scores: {0.50}");
}

TEST_F(LiveRowGlobalIndexReaderTest, TestCreateUnionRanksLiveRowsOfSearchRanges) {
    auto first_shard = std::make_shared<MockGlobalIndexReader>();
    first_shard->SetScoredResult({0, 1}, {0.9f, 0.2f});
    auto second_shard = std::make_shared<MockGlobalIndexReader>();
    second_shard->SetScoredResult({10, 11}, {0.8f, 0.7f});
    int32_t call_count = 0;
    std::shared_ptr<GlobalIndexReader> reader = LiveRowGlobalIndexReader::CreateUnion(
        std::vector<std::shared_ptr<GlobalIndexReader>>({first_shard, second_shard}),
        {Range(0, 9), Range(5, 14)}, {Range(0, 9), Range(10, 14)}, /*executor=*/nullptr,
        Supply(LiveRows(0, 14, {3, 12}), &call_count));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result,
                         reader->VisitFullTextSearch(Search()));
    ASSERT_EQ(result->ToString(), "row ids: {0,10,11}, scores: {0.90,0.80,0.70}");
    std::shared_ptr<FullTextSearch> first_search = first_shard->CapturedFullTextSearch();
    ASSERT_TRUE(first_search && first_search->include_row_ids);
    ASSERT_EQ(first_search->include_row_ids.value(), LiveRows(0, 9, {3}));
    std::shared_ptr<FullTextSearch> second_search = second_shard->CapturedFullTextSearch();
    ASSERT_TRUE(second_search && second_search->include_row_ids);
    ASSERT_EQ(second_search->include_row_ids.value(), RoaringBitmap64::From({10, 11, 13, 14}));
}

TEST_F(LiveRowGlobalIndexReaderTest, TestCacheLiveRows) {
    int32_t load_count = 0;
    bool fail = true;
    LiveRowGlobalIndexReader::LiveRowsSupplier supplier =
        LiveRowGlobalIndexReader::CacheLiveRows([&]() -> Result<std::optional<RoaringBitmap64>> {
            load_count++;
            if (fail) {
                return Status::IOError("cannot read manifest");
            }
            return std::optional<RoaringBitmap64>(RoaringBitmap64::From({1, 2}));
        });
    ASSERT_NOK_WITH_MSG(supplier(), "cannot read manifest");
    fail = false;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> live_rows, supplier());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> cached_live_rows, supplier());
    ASSERT_EQ(load_count, 2);
    ASSERT_TRUE(live_rows);
    ASSERT_EQ(*live_rows, RoaringBitmap64::From({1, 2}));
    ASSERT_EQ(cached_live_rows, live_rows);

    LiveRowGlobalIndexReader::LiveRowsSupplier every_row_live =
        LiveRowGlobalIndexReader::CacheLiveRows([&]() -> Result<std::optional<RoaringBitmap64>> {
            load_count++;
            return std::optional<RoaringBitmap64>();
        });
    ASSERT_OK_AND_ASSIGN(live_rows, every_row_live());
    ASSERT_OK_AND_ASSIGN(cached_live_rows, every_row_live());
    ASSERT_FALSE(live_rows);
    ASSERT_FALSE(cached_live_rows);
    ASSERT_EQ(load_count, 3);
}

TEST_F(LiveRowGlobalIndexReaderTest, TestLiveRowsErrorOnlyFailsSearchesThatRankRows) {
    auto mock_reader = std::make_shared<MockGlobalIndexReader>();
    LiveRowGlobalIndexReader::LiveRowsSupplier failing_supplier =
        []() -> Result<std::shared_ptr<const RoaringBitmap64>> {
        return Status::IOError("cannot read deletion vector");
    };
    LiveRowGlobalIndexReader reader(mock_reader, Range(0, 9), failing_supplier);
    ASSERT_NOK_WITH_MSG(reader.VisitFullTextSearch(Search()), "cannot read deletion vector");
    ASSERT_NOK_WITH_MSG(reader.VisitVectorSearch(SearchVector()), "cannot read deletion vector");
    ASSERT_EQ(mock_reader->InvocationCount(), 0);

    std::shared_ptr<GlobalIndexReader> union_reader = LiveRowGlobalIndexReader::CreateUnion(
        std::vector<std::shared_ptr<GlobalIndexReader>>({mock_reader}), {Range(0, 9)},
        {Range(5, 9)}, /*executor=*/nullptr, failing_supplier);
    ASSERT_NOK_WITH_MSG(union_reader->VisitFullTextSearch(Search()), "cannot read deletion vector");
    ASSERT_NOK_WITH_MSG(union_reader->VisitVectorSearch(SearchVector()),
                        "cannot read deletion vector");
    ASSERT_NOK_WITH_MSG(union_reader->VisitFullTextSearch(std::make_shared<FullTextSearch>(
                            "f0", R"({"match":{"query":"apple"}})", /*limit=*/0)),
                        "full-text search requires a positive limit");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result,
                         union_reader->VisitFullTextSearch(Search(RoaringBitmap64())));
    ASSERT_EQ(result->ToString(), "row ids: {}, scores: {}");
    ASSERT_OK_AND_ASSIGN(
        result, union_reader->VisitFullTextSearch(Search(RoaringBitmap64::From({2, 100}))));
    ASSERT_EQ(result->ToString(), "row ids: {}, scores: {}");
    ASSERT_EQ(mock_reader->InvocationCount(), 0);

    ASSERT_OK(reader.VisitFullTextSearch(Search(RoaringBitmap64::From({100}))));
    std::shared_ptr<FullTextSearch> captured = mock_reader->CapturedFullTextSearch();
    ASSERT_TRUE(captured && captured->include_row_ids);
    ASSERT_EQ(captured->include_row_ids.value(), RoaringBitmap64::From({100}));
}

TEST_F(LiveRowGlobalIndexReaderTest, TestConcurrentSearch) {
    for (bool parallel_shards : {false, true}) {
        SCOPED_TRACE(parallel_shards);
        std::shared_ptr<Executor> executor;
        if (parallel_shards) {
            ASSERT_OK_AND_ASSIGN(executor, CreateDefaultExecutor(/*thread_count=*/4));
        }
        std::vector<std::shared_ptr<GlobalIndexReader>> shards;
        for (int64_t offset : {100, 200}) {
            shards.push_back(std::make_shared<OffsetGlobalIndexReader>(
                std::make_shared<FilteringGlobalIndexReader>(), offset, offset + 3));
        }
        std::atomic<int32_t> load_count{0};
        auto supplier = LiveRowGlobalIndexReader::CacheLiveRows(
            [&]() -> Result<std::optional<RoaringBitmap64>> {
                ++load_count;
                // Shards cover [100, 103] and [200, 203]; DV deletes 101 and 202.
                return std::optional<RoaringBitmap64>(
                    RoaringBitmap64::From({100, 102, 103, 200, 201, 203}));
            });
        auto reader = LiveRowGlobalIndexReader::CreateUnion(
            std::move(shards), {Range(100, 103), Range(200, 203)},
            {Range(100, 103), Range(200, 203)}, executor, supplier);
        ASSERT_TRUE(reader->IsThreadSafe());

        std::promise<void> start;
        std::shared_future<void> ready = start.get_future().share();
        std::vector<std::future<void>> workers;
        for (int32_t thread_id = 0; thread_id < 4; ++thread_id) {
            workers.push_back(std::async(std::launch::async, [reader, ready, thread_id]() {
                ready.wait();
                for (int32_t iteration = 0; iteration < 16; ++iteration) {
                    const int32_t query_id = thread_id * 16 + iteration;
                    const int32_t parity = query_id % 2;
                    const float query_score = static_cast<float>(query_id + 3);
                    const RoaringBitmap64 expected_rows =
                        parity == 0 ? RoaringBitmap64::From({100, 102, 200})
                                    : RoaringBitmap64::From({103, 201, 203});
                    // Scores follow ascending global row ids; offset changes ids, not scores.
                    const std::vector<float> expected_scores =
                        parity == 0 ? std::vector<float>{query_score + 0.1f, query_score + 2.1f,
                                                         query_score + 0.1f}
                                    : std::vector<float>{query_score + 3.1f, query_score + 1.1f,
                                                         query_score + 3.1f};
                    auto vector =
                        SearchVector([parity](int64_t row_id) { return row_id % 2 == parity; });
                    vector->query = {query_score};
                    auto text = Search(parity == 0 ? RoaringBitmap64::From({100, 102, 200, 202})
                                                   : RoaringBitmap64::From({101, 103, 201, 203}));
                    text->limit = query_id + 3;
                    for (bool vector_query : {false, true}) {
                        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result,
                                             vector_query ? reader->VisitVectorSearch(vector)
                                                          : reader->VisitFullTextSearch(text));
                        auto scored =
                            std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(result);
                        ASSERT_TRUE(scored);
                        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* rows, scored->GetBitmap());
                        ASSERT_EQ(*rows, expected_rows);
                        const auto& scores = scored->GetScores();
                        ASSERT_EQ(scores.size(), expected_scores.size());
                        for (size_t i = 0; i < scores.size(); ++i) {
                            ASSERT_FLOAT_EQ(scores[i], expected_scores[i]);
                        }
                    }
                    ASSERT_TRUE(vector->pre_filter(parity == 0 ? 202 : 101));
                    ASSERT_EQ(text->include_row_ids->Cardinality(), 4);
                }
            }));
        }
        start.set_value();
        for (auto& worker : workers) {
            worker.get();
        }
        ASSERT_EQ(load_count.load(), 1);
    }
}

TEST_F(LiveRowGlobalIndexReaderTest, TestOtherVisitsDoNotReadLiveRows) {
    auto mock_reader = std::make_shared<MockGlobalIndexReader>();
    mock_reader->SetDefaultResult({1, 2});
    mock_reader->SetScoredResult({1}, {0.5f});
    int32_t call_count = 0;
    LiveRowGlobalIndexReader reader(mock_reader, Range(0, 9),
                                    Supply(LiveRows(0, 9, {1}), &call_count));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> result, reader.VisitEqual(Literal(1)));
    ASSERT_EQ(result->ToString(), "{1,2}");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> scored_result,
                         reader.VisitVectorSearch(nullptr));
    ASSERT_EQ(scored_result->ToString(), "row ids: {1}, scores: {0.50}");
    ASSERT_OK_AND_ASSIGN(scored_result, reader.VisitFullTextSearch(nullptr));
    ASSERT_EQ(call_count, 0);
    ASSERT_EQ(reader.GetIndexType(), "fake");
    ASSERT_TRUE(reader.IsThreadSafe());
    mock_reader->SetThreadSafe(false);
    ASSERT_FALSE(reader.IsThreadSafe());
}

}  // namespace paimon::test
