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

#include "paimon/core/realtime/precise_realtime_index_read.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/index/index_reader_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/realtime/arrow_deduplicate_realtime_store.h"
#include "paimon/core/realtime/arrow_realtime_store.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/append_only_table_read.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/read_context.h"
#include "paimon/record_batch.h"
#include "paimon/testing/mock/mock_search_realtime_index.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class PreciseRealtimeIndexReadTest : public testing::Test {
 public:
    void SetUp() override {
        user_schema_ = arrow::schema({arrow::field("id", arrow::int64(), false),
                                      arrow::field("category", arrow::int32()),
                                      arrow::field("text", arrow::utf8()),
                                      arrow::field("embedding", arrow::list(arrow::float32()))});
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                             TableSchema::Create(0, user_schema_, {}, {}, {}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RealtimeSchemaLayout> layout,
                             RealtimeSchemaLayout::Create(
                                 RealtimeStoreMode::APPEND_ONLY,
                                 DataField::ConvertDataFieldsToArrowSchema(schema->Fields())));
        write_schema_ = layout->StoreWriteSchema();
        state_ = std::make_shared<MockSearchState>();
        state_->ranked = {{10, 0.9f}, {20, 0.8f}, {30, 0.7f}, {40, 0.6f}};
        using SearchType = IndexReaderCapabilities::SearchType;
        const std::vector<std::pair<std::string, SearchType>> index_types{
            {"precise-mock-vector", SearchType::VECTOR_SEARCH},
            {"precise-mock-text", SearchType::FULL_TEXT_SEARCH}};
        for (const auto& [identifier, search_type] : index_types) {
            auto factory =
                std::make_unique<MockRealtimeIndexFactory>(identifier, search_type, state_);
            FactoryCreator::GetInstance()->Register(
                identifier + RealtimeIndexFactory::REALTIME_INDEX_IDENTIFIER_SUFFIX,
                factory.release());
        }
        ASSERT_OK_AND_ASSIGN(store_, CreateStore("", nullptr));
        ASSERT_OK_AND_ASSIGN(realtime_context_, RealtimeContext::Create());
    }

    void TearDown() override {
        for (const std::string identifier : {"precise-mock-vector", "precise-mock-text"}) {
            FactoryCreator::GetInstance()->TEST_Unregister(
                identifier + RealtimeIndexFactory::REALTIME_INDEX_IDENTIFIER_SUFFIX);
        }
    }

    Result<std::shared_ptr<ArrowRealtimeStore>> CreateStore(
        const std::string& temp, const std::shared_ptr<FileSystem>& fs) const {
        return ArrowRealtimeStore::Create(
            write_schema_, RealtimeStoreMode::APPEND_ONLY, StatisticsMode::NONE, temp, fs, "zstd",
            1,
            {{"realtime.index.hash.columns", "category"},
             {"realtime.index.precise-mock-vector.columns", "embedding"},
             {"realtime.index.precise-mock-text.columns", "text"}},
            pool_, arrow_pool_);
    }

    std::shared_ptr<arrow::StructArray> MakeBatch(const std::string& json) const {
        return checked_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(write_schema_->fields()), json)
                .ValueOrDie());
    }

    Status Write(const std::string& json, const OffsetRange& range) const {
        auto data = MakeBatch(json);
        ArrowArray array;
        ArrowArrayMarkReleased(&array);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*data, &array));
        ScopeGuard release([&]() { ArrowArrayRelease(&array); });
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> batch,
                               RecordBatchBuilder(&array).Finish());
        return store_->Write({std::move(batch), range});
    }

    Result<std::shared_ptr<InternalReadContext>> Context(
        const std::vector<std::string>& fields, const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<VectorSearch>& vector, const std::shared_ptr<FullTextSearch>& text,
        RealtimeStoreMode mode) const {
        std::map<std::string, std::string> options{{Options::READ_BATCH_SIZE, "1"}};
        if (mode == RealtimeStoreMode::DEDUPLICATE) {
            options["realtime.deduplicate-key-fields"] = "id";
        }
        arrow::FieldVector table_fields = user_schema_->fields();
        if (mode == RealtimeStoreMode::DEDUPLICATE) {
            table_fields.insert(table_fields.begin(),
                                arrow::field("_REALTIME_OFFSET", arrow::int64(), false));
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<TableSchema> schema,
            TableSchema::Create(0, arrow::schema(table_fields), {}, {}, options));
        ReadContextBuilder builder(directory_->Str());
        builder.SetReadFieldNames(fields)
            .SetPredicate(predicate)
            .SetVectorSearch(vector)
            .SetFullTextSearch(text)
            .EnablePreciseFileIndexEvaluate(true)
            .EnablePredicateFilter(false)
            .WithRealtimeContext(realtime_context_)
            .AddOption(Options::READ_BATCH_SIZE, "1");
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> internal,
                               InternalReadContext::Create(read_context, schema, options));
        return internal;
    }

    std::shared_ptr<VectorSearch> Vector(int32_t limit) const {
        return std::make_shared<VectorSearch>("embedding", limit, std::vector<float>{1.0f}, nullptr,
                                              nullptr, VectorSearch::DistanceType::COSINE,
                                              std::map<std::string, std::string>{});
    }

    std::shared_ptr<Predicate> Category(int32_t category) const {
        return PredicateBuilder::Equal(1, "category", FieldType::INT, Literal(category));
    }

    Result<std::shared_ptr<arrow::ChunkedArray>> Read(
        const std::shared_ptr<InternalReadContext>& context,
        const std::shared_ptr<RealtimeReadView>& view, const OffsetRange& range,
        RealtimeStoreMode mode) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<BatchReader> reader,
            PreciseRealtimeIndexRead::CreateReader(
                context, mode, query_store_ ? query_store_ : store_, view, range));
        return ReadResultCollector::CollectResult(std::move(reader));
    }

    void CheckResult(const std::shared_ptr<InternalReadContext>& context,
                     const std::shared_ptr<arrow::ChunkedArray>& result,
                     const std::string& json) const {
        // Match ReadResultCollector's nullable fields after dictionary decoding.
        arrow::FieldVector fields;
        for (const auto& field : context->GetReadSchema()->fields()) {
            fields.push_back(arrow::field(field->name(), field->type()));
        }
        auto data_result = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), json);
        ASSERT_TRUE(data_result.ok()) << data_result.status().ToString();
        auto data = checked_pointer_cast<arrow::StructArray>(data_result.ValueOrDie());
        if (data->length() == 0) {
            ASSERT_EQ(result, nullptr);
            return;
        }
        ASSERT_TRUE(result);
        auto row_kind_result = arrow::MakeArrayFromScalar(
            arrow::Int8Scalar(RowKind::Insert()->ToByteValue()), data->length(), arrow_pool_.get());
        ASSERT_TRUE(row_kind_result.ok()) << row_kind_result.status().ToString();
        arrow::ArrayVector arrays{row_kind_result.ValueOrDie()};
        arrays.insert(arrays.end(), data->fields().begin(), data->fields().end());
        fields.insert(fields.begin(),
                      arrow::field(SpecialFields::ValueKind().Name(), arrow::int8()));
        auto expected_array =
            std::make_shared<arrow::StructArray>(arrow::struct_(fields), data->length(), arrays);
        auto expected = std::make_shared<arrow::ChunkedArray>(expected_array);
        ASSERT_TRUE(result->Equals(expected))
            << "expected: " << expected->ToString() << "\nactual: " << result->ToString();
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<arrow::MemoryPool> arrow_pool_ = GetArrowPool(pool_);
    std::unique_ptr<UniqueTestDirectory> directory_ = UniqueTestDirectory::Create();
    std::shared_ptr<arrow::Schema> user_schema_;
    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<ArrowRealtimeStore> store_;
    std::shared_ptr<RealtimeStore> query_store_;
    std::shared_ptr<RealtimeContext> realtime_context_;
    std::shared_ptr<MockSearchState> state_;
};

TEST_F(PreciseRealtimeIndexReadTest, FullTextMatchScoresAreOptional) {
    using MatchResult =
        IndexReaderUtils::FullTextResult<RoaringBitmap64, ScoredRealtimeIndexResult>;
    // Scores must follow offset order even when the index ranks matches differently.
    state_->ranked = {{30, 0.9f}, {10, 0.8f}, {20, 0.7f}};
    auto reader = std::make_shared<MockSearchReader>(
        IndexReaderCapabilities::SearchType::FULL_TEXT_SEARCH, OffsetRange(10, 31),
        RoaringBitmap64::From({10, 20, 30}), state_);
    auto match =
        std::make_shared<FullTextMatch>("text", "all", RoaringBitmap64::From({10, 20, 30}));
    auto candidates = RoaringBitmap64::From({10, 30});
    ASSERT_OK_AND_ASSIGN(MatchResult bitmap,
                         IndexReaderUtils::VisitFullTextMatch(reader, match, candidates, false));
    ASSERT_EQ(bitmap.rows, candidates);
    ASSERT_EQ(bitmap.scores, nullptr);
    ASSERT_OK_AND_ASSIGN(MatchResult scored,
                         IndexReaderUtils::VisitFullTextMatch(reader, match, candidates, true));
    ASSERT_EQ(scored.rows, candidates);
    ASSERT_NE(scored.scores, nullptr);
    ASSERT_EQ(scored.scores->GetScores(), (std::vector<float>{0.8f, 0.9f}));
    ASSERT_EQ(*match->include_row_ids, RoaringBitmap64::From({10, 20, 30}));
}

TEST_F(PreciseRealtimeIndexReadTest, EmptyFullTextMatchPreservesRequestedScoreType) {
    using MatchResult =
        IndexReaderUtils::FullTextResult<RoaringBitmap64, ScoredRealtimeIndexResult>;
    auto reader = std::make_shared<MockSearchReader>(
        IndexReaderCapabilities::SearchType::FULL_TEXT_SEARCH, OffsetRange(10, 41),
        RoaringBitmap64::From({10, 20, 30, 40}), state_);
    auto match = std::make_shared<FullTextMatch>("text", "all", std::nullopt);
    ASSERT_OK_AND_ASSIGN(MatchResult bitmap, IndexReaderUtils::VisitFullTextMatch(
                                                 reader, match, RoaringBitmap64(), false));
    ASSERT_EQ(bitmap.rows.Cardinality(), 0);
    ASSERT_EQ(bitmap.scores, nullptr);
    ASSERT_OK_AND_ASSIGN(MatchResult scored, IndexReaderUtils::VisitFullTextMatch(
                                                 reader, match, RoaringBitmap64(), true));
    ASSERT_EQ(scored.rows.Cardinality(), 0);
    ASSERT_NE(scored.scores, nullptr);
    ASSERT_TRUE(scored.scores->GetScores().empty());
}

TEST_F(PreciseRealtimeIndexReadTest, FullTextMatchRejectsMissingRequestedScores) {
    class UnscoredReader final : public RealtimeIndexReader {
     public:
        Result<std::shared_ptr<RealtimeIndexResult>> VisitFullTextMatch(
            const std::shared_ptr<FullTextMatch>&, bool) override {
            return RealtimeIndexResult::Create(RoaringBitmap64());
        }
    };
    auto reader = std::make_shared<UnscoredReader>();
    auto match = std::make_shared<FullTextMatch>("text", "all", std::nullopt);
    ASSERT_NOK_WITH_MSG(
        IndexReaderUtils::VisitFullTextMatch(reader, match, RoaringBitmap64(), true),
        "Full-text index did not return requested scores");
}

TEST_F(PreciseRealtimeIndexReadTest, AppendTableRechecksScalarBeforeVectorTopK) {
    ASSERT_OK(Write(R"([[10,1,0,"a",[1]],[20,2,0,"b",[2]],
                       [30,3,1,"c",[3]],[40,4,1,"d",[4]]])",
                    OffsetRange(10, 41)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InternalReadContext> context,
                         Context({"id", "category", "_embedding_INDEX_SCORE"}, Category(1),
                                 Vector(2), nullptr, RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> realtime,
                         RealtimeContextImpl::Cast(realtime_context_));
    ASSERT_OK_AND_ASSIGN(
        std::string ticket,
        realtime->PinReadView({RealtimePartitionBucket({}, 0), store_, view,
                               RealtimeStoreMode::APPEND_ONLY, nullptr, write_schema_},
                              60000));
    auto split = std::make_shared<RealtimeSplit>(
        RealtimeSplit::kCurrentVersion, std::nullopt, std::map<std::string, std::string>{}, 0,
        std::vector<std::shared_ptr<Split>>{}, 0, 41, ticket);
    AppendOnlyTableRead table_read(nullptr, context, pool_, nullptr);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, table_read.CreateReader(split));
    ASSERT_OK_AND_ASSIGN(auto result, ReadResultCollector::CollectResult(std::move(reader)));
    CheckResult(context, result, R"([[3,1,0.7],[4,1,0.6]])");
    ASSERT_NOK(realtime->ResolveReadView(ticket));
}

TEST_F(PreciseRealtimeIndexReadTest, UnsupportedScalarVisitScansBeforeTopK) {
    ASSERT_OK(
        Write(R"([[10,1,0,"a",[1]],[20,2,0,"b",[2]],[30,3,1,"c",[3]]])", OffsetRange(10, 31)));
    auto predicate = PredicateBuilder::GreaterThan(1, "category", FieldType::INT, Literal(0));
    ASSERT_OK_AND_ASSIGN(auto context, Context({"id", "category"}, predicate, Vector(1), nullptr,
                                               RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(auto view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(auto result,
                         Read(context, view, OffsetRange(0, 31), RealtimeStoreMode::APPEND_ONLY));
    CheckResult(context, result, R"([[3,1]])");
}

TEST_F(PreciseRealtimeIndexReadTest,
       FullTextPredicateAndVectorScoresAreAlignedAfterScalarFiltering) {
    ASSERT_OK(
        Write(R"([[10,1,0,"a",[1]],[20,2,1,"b",[2]],[30,3,1,"c",[3]]])", OffsetRange(10, 31)));
    state_->text_matches["a"] = RoaringBitmap64::From({10, 30});
    ASSERT_OK_AND_ASSIGN(auto text,
                         PredicateBuilder::FullTextMatch(
                             2, std::make_shared<FullTextMatch>("text", "a", std::nullopt)));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({text, Category(1)}));
    ASSERT_OK_AND_ASSIGN(
        auto context,
        Context({"id", "category", "text", "_text_INDEX_SCORE", "_embedding_INDEX_SCORE"},
                predicate, Vector(1), nullptr, RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(auto view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(auto result,
                         Read(context, view, OffsetRange(0, 31), RealtimeStoreMode::APPEND_ONLY));
    CheckResult(context, result, R"([[3,1,"c",0.7,0.7]])");
}

TEST_F(PreciseRealtimeIndexReadTest, OrBranchesSeeTheSameCandidatesAndMissingIndexesScan) {
    ASSERT_OK(
        Write(R"([[10,1,0,"a",[1]],[20,2,0,"b",[2]],[30,3,0,"c",[3]]])", OffsetRange(10, 31)));
    state_->text_matches["a"] = RoaringBitmap64::From({10, 30});
    state_->text_matches["b"] = RoaringBitmap64::From({20, 30});
    ASSERT_OK_AND_ASSIGN(auto a,
                         PredicateBuilder::FullTextMatch(
                             1, std::make_shared<FullTextMatch>("text", "a", std::nullopt)));
    ASSERT_OK_AND_ASSIGN(auto b,
                         PredicateBuilder::FullTextMatch(
                             1, std::make_shared<FullTextMatch>("text", "b", std::nullopt)));
    auto scalar = PredicateBuilder::GreaterThan(0, "id", FieldType::BIGINT, Literal(int64_t(2)));
    ASSERT_OK_AND_ASSIGN(auto left, PredicateBuilder::And({a, scalar}));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({left, b}));
    ASSERT_OK_AND_ASSIGN(auto context, Context({"id", "text"}, predicate, nullptr, nullptr,
                                               RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(auto view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(auto result,
                         Read(context, view, OffsetRange(0, 31), RealtimeStoreMode::APPEND_ONLY));
    CheckResult(context, result, R"([[2,"b"],[3,"c"]])");
}

TEST_F(PreciseRealtimeIndexReadTest, FullTextTopKRespectsScalarAndIncludeOffsets) {
    ASSERT_OK(
        Write(R"([[10,1,0,"a",[1]],[20,2,1,"b",[2]],[30,3,1,"c",[3]]])", OffsetRange(10, 31)));
    auto text = std::make_shared<FullTextSearch>("text", "all", 1, RoaringBitmap64::From({10, 30}));
    ASSERT_OK_AND_ASSIGN(auto context, Context({"id", "category", "_text_INDEX_SCORE"}, Category(1),
                                               nullptr, text, RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(auto view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(auto result,
                         Read(context, view, OffsetRange(0, 31), RealtimeStoreMode::APPEND_ONLY));
    CheckResult(context, result, R"([[3,1,0.7]])");
    ASSERT_EQ(*text->include_row_ids, RoaringBitmap64::From({10, 30}));
}

TEST_F(PreciseRealtimeIndexReadTest, LargeSparseOffsetsAndUserFilterRespectBothPinnedBounds) {
    const int64_t base = int64_t(1) << 40;
    state_->ranked = {{base + 1, 0.9f}, {base + 100, 0.8f}, {base + 1000, 0.7f}};
    ASSERT_OK(Write(R"([[1099511627777,1,1,"a",[1]],[1099511627876,2,1,"b",[2]]])",
                    OffsetRange(base + 1, base + 101)));
    ASSERT_OK_AND_ASSIGN(auto view, store_->AcquireReadView());
    ASSERT_OK(Write(R"([[1099511628776,3,1,"c",[3]]])", OffsetRange(base + 1000, base + 1001)));
    auto search = Vector(1);
    search->pre_filter = [base](int64_t offset) { return offset != base + 1; };
    ASSERT_OK_AND_ASSIGN(auto context, Context({"id", "category"}, Category(1), search, nullptr,
                                               RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(auto result, Read(context, view, OffsetRange(base + 1, base + 101),
                                           RealtimeStoreMode::APPEND_ONLY));
    CheckResult(context, result, R"([[2,1]])");
    ASSERT_TRUE(search->pre_filter);
    ASSERT_OK_AND_ASSIGN(auto bounded, Context({"id", "category"}, Category(1), Vector(1), nullptr,
                                               RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(auto bounded_result, Read(bounded, view, OffsetRange(base + 2, base + 101),
                                                   RealtimeStoreMode::APPEND_ONLY));
    CheckResult(bounded, bounded_result, R"([[2,1]])");
}

TEST_F(PreciseRealtimeIndexReadTest, PinnedSourcesSurviveSealSpillAndReclamation) {
    ASSERT_OK_AND_ASSIGN(store_, CreateStore(directory_->Str(), directory_->GetFileSystem()));
    ASSERT_OK(Write(R"([[10,1,1,"a",[1]],[20,2,1,"b",[2]]])", OffsetRange(10, 21)));
    ASSERT_OK_AND_ASSIGN(auto building_view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(auto segment, store_->SealForCommit());
    ASSERT_TRUE(segment);
    ASSERT_OK_AND_ASSIGN(auto spilled_view, store_->AcquireReadView());
    ASSERT_OK(store_->AdvanceCommittedOffset(21));
    ASSERT_OK_AND_ASSIGN(
        auto context, Context({"id"}, nullptr, Vector(1), nullptr, RealtimeStoreMode::APPEND_ONLY));
    for (const auto& view : {building_view, spilled_view}) {
        ASSERT_OK_AND_ASSIGN(
            auto result, Read(context, view, OffsetRange(0, 21), RealtimeStoreMode::APPEND_ONLY));
        CheckResult(context, result, R"([[1]])");
    }
}

TEST_F(PreciseRealtimeIndexReadTest, DeduplicateRemovesOldOffsetsBeforeTopKAndPinsVisibility) {
    ASSERT_OK_AND_ASSIGN(auto context,
                         Context({"id", "category", "_embedding_INDEX_SCORE"}, nullptr, Vector(1),
                                 nullptr, RealtimeStoreMode::DEDUPLICATE));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RealtimeSchemaLayout> layout,
                         RealtimeSchemaLayout::Create(RealtimeStoreMode::DEDUPLICATE,
                                                      DataField::ConvertDataFieldsToArrowSchema(
                                                          context->GetTableSchema()->Fields())));
    write_schema_ = layout->StoreWriteSchema();
    ASSERT_OK_AND_ASSIGN(store_, CreateStore("", nullptr));
    ASSERT_OK_AND_ASSIGN(auto dedup, ArrowDeduplicateRealtimeStore::Create(
                                         write_schema_, {"id"}, store_, pool_, arrow_pool_));
    query_store_ = dedup;
    auto first = MakeBatch(R"([[10,1,1,"old",[1]],[20,2,1,"other",[2]]])");
    ASSERT_OK(dedup->WriteAndLookup(first,
                                    {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT},
                                    OffsetRange(10, 21), RoaringBitmap64()));
    ASSERT_OK_AND_ASSIGN(auto old_view, dedup->AcquireReadView());
    ASSERT_OK(dedup->WriteAndLookup(MakeBatch(R"([[30,1,1,"new",[3]]])"),
                                    {RecordBatch::RowKind::INSERT}, OffsetRange(30, 31),
                                    RoaringBitmap64()));
    ASSERT_OK_AND_ASSIGN(auto new_view, dedup->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(auto old_result, Read(context, old_view, OffsetRange(0, 21),
                                               RealtimeStoreMode::DEDUPLICATE));
    CheckResult(context, old_result, R"([[1,1,0.9]])");
    ASSERT_OK_AND_ASSIGN(auto new_result, Read(context, new_view, OffsetRange(0, 31),
                                               RealtimeStoreMode::DEDUPLICATE));
    CheckResult(context, new_result, R"([[2,1,0.8]])");
}

TEST_F(PreciseRealtimeIndexReadTest, MissingSearchCapabilityFailsBeforeEmptyPredicateShortCircuit) {
    ASSERT_OK(Write(R"([[10,1,1,"a",[1]]])", OffsetRange(10, 11)));
    state_->supports_search = false;
    ASSERT_OK_AND_ASSIGN(auto context, Context({"id", "category"}, Category(999), Vector(1),
                                               nullptr, RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(auto view, store_->AcquireReadView());
    ASSERT_NOK_WITH_MSG(
        PreciseRealtimeIndexRead::CreateReader(context, RealtimeStoreMode::APPEND_ONLY, store_,
                                               view, OffsetRange(0, 11)),
        "No real-time index supports requested search");
}

TEST_F(PreciseRealtimeIndexReadTest, ScalarOnlyPreciseReadDoesNotRequireSearchCapability) {
    ASSERT_OK(Write(R"([[10,1,0,"a",[1]],[20,2,1,"b",[2]]])", OffsetRange(10, 21)));
    state_->supports_search = false;
    ASSERT_OK_AND_ASSIGN(auto context, Context({"id", "category"}, Category(1), nullptr, nullptr,
                                               RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(auto view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(auto result,
                         Read(context, view, OffsetRange(0, 21), RealtimeStoreMode::APPEND_ONLY));
    CheckResult(context, result, R"([[2,1]])");
}

TEST_F(PreciseRealtimeIndexReadTest, EmptyScalarCandidatesReturnNoRows) {
    ASSERT_OK(Write(R"([[10,1,1,"a",[1]]])", OffsetRange(10, 11)));
    ASSERT_OK_AND_ASSIGN(auto context, Context({"id", "category"}, Category(999), Vector(1),
                                               nullptr, RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(auto view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(auto result,
                         Read(context, view, OffsetRange(0, 11), RealtimeStoreMode::APPEND_ONLY));
    CheckResult(context, result, "[]");
}

TEST_F(PreciseRealtimeIndexReadTest, EachPinnedSegmentExecutesItsOwnTopK) {
    // A configured search hit must not be returned unless the segment indexed that offset.
    state_->ranked.insert(state_->ranked.begin(), {15, 1.0f});
    ASSERT_OK(Write(R"([[10,1,1,"a",[1]],[20,2,1,"b",[2]]])", OffsetRange(10, 21)));
    ASSERT_OK_AND_ASSIGN(auto segment, store_->SealForCommit());
    ASSERT_TRUE(segment);
    ASSERT_OK(Write(R"([[30,3,1,"c",[3]],[40,4,1,"d",[4]]])", OffsetRange(30, 41)));
    ASSERT_OK_AND_ASSIGN(auto context, Context({"id", "_embedding_INDEX_SCORE"}, nullptr, Vector(1),
                                               nullptr, RealtimeStoreMode::APPEND_ONLY));
    ASSERT_OK_AND_ASSIGN(auto view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(auto result,
                         Read(context, view, OffsetRange(0, 41), RealtimeStoreMode::APPEND_ONLY));
    CheckResult(context, result, R"([[1,0.9],[3,0.7]])");
}

TEST_F(PreciseRealtimeIndexReadTest, RejectsMisalignedScores) {
    ASSERT_NOK_WITH_MSG(ScoredRealtimeIndexResult::Create(RoaringBitmap64::From({10, 20}), {1.0f}),
                        "score count");
}

}  // namespace
}  // namespace paimon::test
