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

#include "paimon/core/operation/precise_file_index_read.h"

#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/deletionvectors/bitmap64_deletion_vector.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_evaluator.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/data/timestamp.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_search_file_index.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class RecordingDoubleFunction final : public LeafFunction {
 public:
    RecordingDoubleFunction(const LeafFunction& function, std::vector<double>* evaluated)
        : function_(function), evaluated_(evaluated) {}

    Type GetType() const override {
        return function_.GetType();
    }

    std::string ToString() const override {
        return function_.ToString();
    }

    const LeafFunction* Negate() const override {
        return function_.Negate();
    }

    Result<std::vector<char>> Test(const arrow::Array& array, const std::vector<Literal>& literals,
                                   arrow::MemoryPool* pool) const override {
        const auto* values = dynamic_cast<const arrow::DoubleArray*>(&array);
        if (!values) {
            return Status::Invalid("Recording function expects a double field");
        }
        for (int64_t i = 0; i < values->length(); ++i) {
            evaluated_->push_back(values->Value(i));
        }
        return function_.Test(array, literals, pool);
    }

    Result<bool> Test(const Literal& value, const std::vector<Literal>& literals) const override {
        return function_.Test(value, literals);
    }

    Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                      const std::optional<int64_t>& null_count,
                      const std::vector<Literal>& literals) const override {
        return function_.Test(row_count, min_value, max_value, null_count, literals);
    }

 private:
    const LeafFunction& function_;
    std::vector<double>* evaluated_;
};

}  // namespace

class PreciseFileIndexReadTest : public ::testing::Test {
 protected:
    void SetUp() override {
        auto factory = std::make_unique<MockFullTextSearchFileIndexFactory>(statistics_);
        FactoryCreator::GetInstance()->Register(statistics_index_type_, factory.release());
    }

    void TearDown() override {
        FactoryCreator::GetInstance()->TEST_Unregister(statistics_index_type_);
    }

    static std::string SearchTablePath() {
        return GetDataDir() + "/orc/multi_partition_append_table.db/multi_partition_append_table";
    }

    std::shared_ptr<DataFileMeta> PrepareDataFile() const {
        return std::make_shared<DataFileMeta>(
            "data-01b6a930-6564-409b-b8f4-ed1307790d72-0.orc", /*file_size=*/575, /*row_count=*/3,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 12.1},
                                              {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1728497439433ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
    }

    Result<std::unique_ptr<RawFileSplitRead>> CreateSplitRead(
        const std::shared_ptr<ReadContext>& read_context,
        std::shared_ptr<TableSchema> table_schema = nullptr) const {
        if (!table_schema) {
            SchemaManager schema_manager(std::make_shared<LocalFileSystem>(),
                                         read_context->GetPath());
            PAIMON_ASSIGN_OR_RAISE(table_schema, schema_manager.ReadSchema(0));
        }
        auto options = table_schema->Options();
        for (const auto& [key, value] : read_context->GetOptions()) {
            options[key] = value;
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> context,
                               InternalReadContext::Create(read_context, table_schema, options));
        return std::make_unique<RawFileSplitRead>(nullptr, context, pool_, nullptr);
    }

    Result<std::shared_ptr<arrow::ChunkedArray>> ReadWithFileIndex(
        const std::shared_ptr<ReadContext>& read_context, const std::shared_ptr<Bytes>& index,
        const RoaringBitmap32& deleted_rows = RoaringBitmap32(),
        std::shared_ptr<TableSchema> table_schema = nullptr,
        const std::optional<std::vector<Range>>& row_ranges = std::nullopt,
        std::shared_ptr<DeletionVector> deletion_vector = nullptr,
        const std::optional<int64_t>& row_count = std::nullopt) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RawFileSplitRead> split_read,
                               CreateSplitRead(read_context, std::move(table_schema)));
        auto file = PrepareDataFile();
        file->embedded_index = index;
        if (row_count) {
            file->row_count = *row_count;
        }
        const std::string bucket_path = SearchTablePath() + "/f1=10/f2=0/bucket-0";
        auto path_factory = std::make_shared<DataFilePathFactory>();
        PAIMON_RETURN_NOT_OK(path_factory->Init(bucket_path, "orc", "data-", nullptr));
        const auto& context = split_read->context_;
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldMappingBuilder> field_mapping_builder,
                               FieldMappingBuilder::Create(context->GetReadSchema(),
                                                           context->GetPartitionKeys(), nullptr));
        const BinaryRow partition = BinaryRowGenerator::GenerateRow({10, 0}, pool_.get());
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<PreciseFileIndexRead> precise_read,
            PreciseFileIndexRead::Create(*split_read, partition, file, field_mapping_builder.get(),
                                         path_factory, {}));
        if (!deletion_vector) {
            deletion_vector = std::make_shared<BitmapDeletionVector>(deleted_rows);
        }
        DeletionVector::Factory dv_factory =
            [deletion_vector](const std::string&) -> Result<std::shared_ptr<DeletionVector>> {
            return deletion_vector;
        };
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader,
                               precise_read->CreateReader(dv_factory, row_ranges));
        if (!reader) {
            return std::shared_ptr<arrow::ChunkedArray>();
        }
        return ReadResultCollector::CollectResult(std::move(reader));
    }

    Result<std::shared_ptr<arrow::ChunkedArray>> ReadQueryPredicate(
        const std::shared_ptr<Predicate>& predicate,
        const MockSearchFileIndex::QueryMatches& matches) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<Bytes> index,
            MockSearchFileIndex::MakeQueryIndex("f0", statistics_index_type_, matches, pool_));
        ReadContextBuilder builder(SearchTablePath());
        builder.SetReadFieldNames({"f0"}).SetPredicate(predicate).EnablePreciseFileIndexEvaluate(
            true);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> context, builder.Finish());
        return ReadWithFileIndex(context, index);
    }

    static void CheckSearchResult(const std::shared_ptr<arrow::ChunkedArray>& actual,
                                  const arrow::FieldVector& fields, const std::string& json) {
        ASSERT_TRUE(actual);
        std::shared_ptr<arrow::ChunkedArray> expected;
        ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields), {json},
                                                                     &expected)
                        .ok());
        ASSERT_TRUE(actual->Equals(expected)) << actual->ToString();
    }

    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    const std::string statistics_index_type_ = "precise-read-statistics-full-text";
    std::shared_ptr<MockSearchFileIndex::Statistics> statistics_ =
        std::make_shared<MockSearchFileIndex::Statistics>();
};

TEST_F(PreciseFileIndexReadTest, TestReadRowPositionsAcrossPredicates) {
    ReadContextBuilder builder(SearchTablePath());
    builder.EnablePreciseFileIndexEvaluate(true);
    builder.SetReadFieldNames({"f0", "f1", "f2", "f3"})
        .SetPredicate(PredicateBuilder::GreaterThan(3, "f3", FieldType::DOUBLE, Literal(13.5)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RawFileSplitRead> split_read, CreateSplitRead(context));
    auto file = PrepareDataFile();
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(
        path_factory->Init(SearchTablePath() + "/f1=10/f2=0/bucket-0", "orc", "data-", nullptr));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FieldMappingBuilder> field_mapping_builder,
        FieldMappingBuilder::Create(split_read->context_->GetReadSchema(),
                                    split_read->context_->GetPartitionKeys(), nullptr));
    const BinaryRow partition = BinaryRowGenerator::GenerateRow({10, 0}, pool_.get());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PreciseFileIndexRead> read,
        PreciseFileIndexRead::Create(*split_read, partition, file, field_mapping_builder.get(),
                                     path_factory, {}));
    const RoaringBitmap32 candidates = RoaringBitmap32::From({0, 1, 2});
    auto first = PredicateBuilder::Equal(3, "f3", FieldType::DOUBLE, Literal(12.1));
    ASSERT_OK_AND_ASSIGN(RoaringBitmap32 rows, read->ReadRowPositions(first, candidates));
    ASSERT_EQ(rows, RoaringBitmap32::From({0}));
    ASSERT_OK_AND_ASSIGN(
        rows, read->ReadRowPositions(PredicateBuilder::Equal(1, "f1", FieldType::INT, Literal(10)),
                                     candidates));
    ASSERT_EQ(rows, candidates);
    ASSERT_OK_AND_ASSIGN(
        rows, read->ReadRowPositions(
                  PredicateBuilder::LessOrEqual(3, "f3", FieldType::DOUBLE, Literal(13.1)),
                  RoaringBitmap32::From({1, 2})));
    ASSERT_EQ(rows, RoaringBitmap32::From({1}));
    ASSERT_OK_AND_ASSIGN(rows, read->ReadRowPositions(first, candidates));
    ASSERT_EQ(rows, RoaringBitmap32::From({0}));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                         read->CreateReader({}, std::nullopt));
    ASSERT_TRUE(reader);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(std::move(reader)));
    CheckSearchResult(result,
                      {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                       arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())},
                      R"([["Tony",10,0,14.1]])");
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateNestedAndOrWithPartitionConstants) {
    const std::string first_query = R"({"match":{"query":"first"}})";
    const std::string second_query = R"({"match":{"query":"second"}})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> first,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", first_query)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> second,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", second_query)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> first_branch,
        PredicateBuilder::And(
            {first, PredicateBuilder::GreaterThan(3, "f3", FieldType::DOUBLE, Literal(12.5))}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> second_branch,
                         PredicateBuilder::And({second, PredicateBuilder::Equal(
                                                            2, "f2", FieldType::INT, Literal(0))}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> alternatives,
                         PredicateBuilder::Or({first_branch, second_branch}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> predicate,
        PredicateBuilder::And(
            {PredicateBuilder::Equal(1, "f1", FieldType::INT, Literal(10)), alternatives}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> index,
        MockSearchFileIndex::MakeQueryIndex(
            "f0", statistics_index_type_,
            {{first_query, {{{0, 0.2f}, {1, 0.4f}}, 0.2}}, {second_query, {{{2, 0.6f}}, 0.1}}},
            pool_));
    ReadContextBuilder builder(SearchTablePath());
    builder.EnablePreciseFileIndexEvaluate(true);
    builder.SetReadFieldNames({"f0", "f1", "f2", "f3"})
        .SetPredicate(predicate)
        .AddOption(Options::READ_BATCH_SIZE, "1");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadWithFileIndex(context, index));
    CheckSearchResult(result,
                      {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                       arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())},
                      R"([["Emily",10,0,13.1],["Tony",10,0,14.1]])");
    ASSERT_EQ(statistics_->executions.size(), 2);
    ASSERT_EQ(statistics_->executions[0].query, first_query);
    ASSERT_EQ(statistics_->executions[1].query, second_query);
    for (const auto& execution : statistics_->executions) {
        ASSERT_FALSE(execution.needs_scores);
    }
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateReordersQueries) {
    const std::string wide_query = R"({"match":{"query":"wide"}})";
    const std::string narrow_query = R"({"match":{"query":"narrow"}})";
    auto wide_match =
        std::make_shared<FullTextMatch>("f0", wide_query, RoaringBitmap64::From({1, 2}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> wide,
                         PredicateBuilder::FullTextMatch(0, wide_match));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> narrow,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", narrow_query)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({wide, narrow,
                                                PredicateBuilder::GreaterThan(
                                                    1, "f3", FieldType::DOUBLE, Literal(12.0))}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> index,
        MockSearchFileIndex::MakeQueryIndex("f0", statistics_index_type_,
                                            {{wide_query, {{{0, 0.1f}, {1, 0.2f}, {2, 0.3f}}, 0.9}},
                                             {narrow_query, {{{0, 0.4f}, {2, 0.5f}}, 0.1}}},
                                            pool_));
    ReadContextBuilder builder(SearchTablePath());
    builder.EnablePreciseFileIndexEvaluate(true);
    builder.SetReadFieldNames({"f0", "f3"}).SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadWithFileIndex(context, index));
    CheckSearchResult(result,
                      {arrow::field("f0", arrow::utf8()), arrow::field("f3", arrow::float64())},
                      R"([["Tony",14.1]])");
    ASSERT_EQ(statistics_->executions.size(), 2);
    ASSERT_EQ(statistics_->executions[0].query, narrow_query);
    ASSERT_EQ(statistics_->executions[1].query, wide_query);
    ASSERT_EQ(wide_match->include_row_ids, RoaringBitmap64::From({1, 2}));
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateSelectivityOrder) {
    const std::vector<std::string> queries = {
        R"({"match":{"query":"a"}})", R"({"match":{"query":"b"}})", R"({"match":{"query":"c"}})",
        R"({"match":{"query":"d"}})", R"({"match":{"query":"e"}})"};
    std::vector<std::shared_ptr<Predicate>> leaves;
    for (const auto& query : queries) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<Predicate> leaf,
            PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", query)));
        leaves.push_back(leaf);
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate, PredicateBuilder::And(leaves));
    struct Case {
        std::vector<std::optional<double>> estimates;
        std::vector<int32_t> order;
    };
    const std::vector<Case> cases = {
        {{std::nullopt, 0.8, 0.2, 0.2, std::nullopt}, {2, 3, 1, 0, 4}},
        {{std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt}, {0, 1, 2, 3, 4}},
        {{std::numeric_limits<double>::quiet_NaN(), -0.1, 1.1, 0.5,
          std::numeric_limits<double>::infinity()},
         {3, 0, 1, 2, 4}}};
    for (const auto& test_case : cases) {
        MockSearchFileIndex::QueryMatches matches;
        for (size_t i = 0; i < queries.size(); ++i) {
            matches.emplace(queries[i],
                            MockSearchFileIndex::QueryResult{{{0, 0.1f}, {1, 0.2f}, {2, 0.3f}},
                                                             test_case.estimates[i]});
        }
        statistics_->executions.clear();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                             ReadQueryPredicate(predicate, matches));
        CheckSearchResult(result, {arrow::field("f0", arrow::utf8())},
                          R"([["Bob"],["Emily"],["Tony"]])");
        ASSERT_EQ(statistics_->executions.size(), queries.size());
        for (size_t i = 0; i < queries.size(); ++i) {
            ASSERT_EQ(statistics_->executions[i].query, queries[test_case.order[i]]);
        }
    }
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateZeroEstimateOnlyChangesOrder) {
    const std::string a_query = R"({"match":{"query":"a"}})";
    const std::string b_query = R"({"match":{"query":"b"}})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> a,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", a_query)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> b,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", b_query)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate, PredicateBuilder::And({a, b}));
    for (bool a_first : {true, false}) {
        MockSearchFileIndex::QueryMatches matches = {
            {a_query, {{{0, 0.1f}, {1, 0.2f}}, a_first ? 0.0 : 0.9}},
            {b_query, {{{1, 0.3f}, {2, 0.4f}}, a_first ? 0.9 : 0.0}}};
        statistics_->executions.clear();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                             ReadQueryPredicate(predicate, matches));
        CheckSearchResult(result, {arrow::field("f0", arrow::utf8())}, R"([["Emily"]])");
        ASSERT_EQ(statistics_->executions.size(), 2);
        ASSERT_EQ(statistics_->executions[0].query, a_first ? a_query : b_query);
        ASSERT_EQ(statistics_->executions[1].query, a_first ? b_query : a_query);
    }
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateCompoundSelectivity) {
    const std::string a_query = R"({"match":{"query":"a"}})";
    const std::string b_query = R"({"match":{"query":"b"}})";
    const std::string c_query = R"({"match":{"query":"c"}})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> a,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", a_query)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> b,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", b_query)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> c,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", c_query)));
    struct Case {
        bool inner_and;
        std::optional<double> a_estimate;
        std::optional<double> b_estimate;
        double c_estimate;
        std::vector<std::string> order;
    };
    const std::vector<Case> cases = {
        {true, 0.2, 0.3, 0.1, {c_query, a_query, b_query}},
        {true, 0.2, 0.3, 0.4, {a_query, b_query, c_query}},
        {true, 0.2, std::nullopt, 0.3, {a_query, b_query, c_query}},
        {true, std::nullopt, 0.2, 0.3, {b_query, a_query, c_query}},
        {true, std::nullopt, std::nullopt, 0.3, {c_query, a_query, b_query}},
        {true, 0.0, std::nullopt, 0.1, {a_query, b_query, c_query}},
        {false, 0.2, 0.3, 0.6, {a_query, b_query, c_query}},
        {false, 0.6, 0.8, 1.0, {a_query, b_query, c_query}},
        {false, 0.2, std::nullopt, 0.6, {c_query, a_query, b_query}},
        {false, std::nullopt, 0.2, 0.6, {c_query, a_query, b_query}},
        {false, std::nullopt, std::nullopt, 0.6, {c_query, a_query, b_query}}};
    for (const auto& test_case : cases) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> inner, test_case.inner_and
                                                                   ? PredicateBuilder::And({a, b})
                                                                   : PredicateBuilder::Or({a, b}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                             PredicateBuilder::And({inner, c}));
        MockSearchFileIndex::QueryMatches matches = {
            {a_query, {{{0, 0.1f}, {1, 0.2f}}, test_case.a_estimate}},
            {b_query, {{{1, 0.3f}, {2, 0.4f}}, test_case.b_estimate}},
            {c_query, {{{0, 0.5f}, {1, 0.6f}, {2, 0.7f}}, test_case.c_estimate}}};
        statistics_->executions.clear();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                             ReadQueryPredicate(predicate, matches));
        CheckSearchResult(
            result, {arrow::field("f0", arrow::utf8())},
            test_case.inner_and ? R"([["Emily"]])" : R"([["Bob"],["Emily"],["Tony"]])");
        ASSERT_EQ(statistics_->executions.size(), 3);
        for (size_t i = 0; i < test_case.order.size(); ++i) {
            ASSERT_EQ(statistics_->executions[i].query, test_case.order[i]);
        }
    }
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateEmptyRowsShortCircuitExecution) {
    const std::string empty_query = R"({"match":{"query":"empty"}})";
    const std::string all_query = R"({"match":{"query":"all"}})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> empty,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", empty_query)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> all,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", all_query)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate, PredicateBuilder::And({empty, all}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> index,
                         MockSearchFileIndex::MakeQueryIndex(
                             "f0", statistics_index_type_,
                             {{empty_query, {{}, std::nullopt}},
                              {all_query, {{{0, 0.1f}, {1, 0.2f}, {2, 0.3f}}, std::nullopt}}},
                             pool_));
    ReadContextBuilder builder(SearchTablePath());
    builder.EnablePreciseFileIndexEvaluate(true);
    builder.SetReadFieldNames({"f0"}).SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    for (bool all_deleted : {false, true}) {
        statistics_->executions.clear();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result,
            ReadWithFileIndex(context, index,
                              all_deleted ? RoaringBitmap32::From({0, 1, 2}) : RoaringBitmap32()));
        ASSERT_TRUE(!result || result->length() == 0);
        ASSERT_EQ(statistics_->executions.size(), all_deleted ? 0 : 1);
        if (!all_deleted) {
            ASSERT_EQ(statistics_->executions[0].query, empty_query);
        }
    }
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateOrEvaluatesAllBranches) {
    const std::string all_query = R"({"match":{"query":"all"}})";
    const std::string one_query = R"({"match":{"query":"one"}})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> all,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", all_query)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> one,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", one_query)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate, PredicateBuilder::Or({all, one}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::ChunkedArray> result,
        ReadQueryPredicate(predicate, {{all_query, {{{0, 0.1f}, {1, 0.2f}, {2, 0.3f}}, 0.9}},
                                       {one_query, {{{1, 0.4f}}, 0.1}}}));
    CheckSearchResult(result, {arrow::field("f0", arrow::utf8())},
                      R"([["Bob"],["Emily"],["Tony"]])");
    ASSERT_EQ(statistics_->executions.size(), 2);
    ASSERT_EQ(statistics_->executions[0].query, all_query);
    ASSERT_EQ(statistics_->executions[1].query, one_query);
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateIndexedLeafSkipsUnselectedScalarBatches) {
    const std::string query = R"({"match":{"query":"selected"}})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> indexed,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", query)));
    auto scalar = std::dynamic_pointer_cast<LeafPredicateImpl>(
        PredicateBuilder::GreaterThan(1, "f3", FieldType::DOUBLE, Literal(13.0)));
    ASSERT_TRUE(scalar);
    std::vector<double> evaluated;
    RecordingDoubleFunction scalar_function(scalar->GetLeafFunction(), &evaluated);
    auto unindexed = std::make_shared<LeafPredicateImpl>(
        scalar_function, scalar->FieldIndex(), scalar->FieldName(), scalar->GetFieldType(),
        scalar->Literals());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({unindexed, indexed}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> index,
        MockSearchFileIndex::MakeQueryIndex("f0", statistics_index_type_,
                                            {{query, {{{0, 0.25f}, {2, 0.75f}}, 0.1}}}, pool_));
    ReadContextBuilder builder(SearchTablePath());
    builder.EnablePreciseFileIndexEvaluate(true);
    // Single-row batches let bitmap selection skip the unselected scalar evaluation batch.
    builder.SetReadFieldNames({"f0", "f3"})
        .SetPredicate(predicate)
        .AddOption(Options::READ_BATCH_SIZE, "1");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadWithFileIndex(context, index));
    CheckSearchResult(result,
                      {arrow::field("f0", arrow::utf8()), arrow::field("f3", arrow::float64())},
                      R"([["Tony",14.1]])");
    ASSERT_EQ(evaluated, std::vector<double>({12.1, 14.1}));
    ASSERT_EQ(statistics_->executions.size(), 1);
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateRechecksScalarIndexCandidates) {
    auto field = arrow::field("f3", arrow::float64());
    ASSERT_OK_AND_ASSIGN(auto writer, BitmapFileIndexWriter::Create(field, {}, pool_));
    // Include Bob as a false positive: the data file contains 12.1 for row 0.
    auto indexed_values = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}),
                                                                    R"([[13.1], [13.1], [14.1]])");
    ASSERT_TRUE(indexed_values.ok()) << indexed_values.status().ToString();
    ::ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*indexed_values.ValueUnsafe(), &c_array).ok());
    ASSERT_OK(writer->AddBatch(&c_array));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> scalar_payload, writer->SerializedBytes());

    const std::string query = R"({"match":{"query":"all"}})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> text_payload,
        MockSearchFileIndex::MakeQueryPayload(
            {{query, {{{0, 0.25f}, {1, 0.5f}, {2, 0.75f}}, std::nullopt}}}, pool_));
    FileIndexFormat::ColumnIndexes indexes;
    indexes["f3"]["bitmap"] = scalar_payload;
    indexes["f0"][statistics_index_type_] = text_payload;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> index,
                         MockSearchFileIndex::Serialize(indexes, pool_));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> text,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", query)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> predicate,
        PredicateBuilder::And(
            {PredicateBuilder::Equal(1, "f3", FieldType::DOUBLE, Literal(13.1)), text}));
    ReadContextBuilder builder(SearchTablePath());
    builder.EnablePreciseFileIndexEvaluate(true);
    builder.SetReadFieldNames({"f0", "f3", "_f0_INDEX_SCORE"})
        .SetPredicate(predicate)
        .AddOption(Options::READ_BATCH_SIZE, "1");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadWithFileIndex(context, index));
    CheckSearchResult(result,
                      {arrow::field("f0", arrow::utf8()), field,
                       arrow::field("_f0_INDEX_SCORE", arrow::float32())},
                      R"([["Emily",13.1,0.5]])");
    ASSERT_EQ(statistics_->executions.size(), 1);
    ASSERT_TRUE(statistics_->executions.front().needs_scores);
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicatePrechecksSearchBeforeEmptyCandidates) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> text,
        PredicateBuilder::FullTextMatch(
            0, std::make_shared<FullTextMatch>("f0", R"({"match":{"query":"q"}})")));
    for (int32_t case_id : {0, 1, 2}) {
        auto scalar =
            PredicateBuilder::Equal(1, "f1", FieldType::INT, Literal(case_id == 0 ? 20 : 10));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                             PredicateBuilder::And({scalar, text}));
        ReadContextBuilder builder(SearchTablePath());
        builder.EnablePreciseFileIndexEvaluate(true);
        builder.SetReadFieldNames({"f0", "f1"}).SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
        const auto deleted = case_id == 1 ? RoaringBitmap32::From({0, 1, 2}) : RoaringBitmap32();
        const std::optional<std::vector<Range>> ranges =
            case_id == 2 ? std::make_optional(std::vector<Range>()) : std::nullopt;
        ASSERT_NOK_WITH_MSG(ReadWithFileIndex(context, nullptr, deleted, nullptr, ranges),
                            "has no File Index for search field 'f0'");
    }
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateRenamedTextAndMissingScalarColumn) {
    SchemaManager schemas(std::make_shared<LocalFileSystem>(), SearchTablePath());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> original, schemas.ReadSchema(0));
    auto fields = original->Fields();
    fields[0] = DataField(fields[0].Id(), fields[0].ArrowField()->WithName("renamed_f0"));
    fields.emplace_back(4, arrow::field("new_value", arrow::int32()));
    auto options = original->Options();
    options[Options::READ_BATCH_SIZE] = "1";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> current,
                         TableSchema::Create(1, DataField::ConvertDataFieldsToArrowSchema(fields),
                                             original->PartitionKeys(), {}, options));
    const std::string query = R"({"match":{"query":"all"}})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> text,
        PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("renamed_f0", query)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> predicate,
        PredicateBuilder::And(
            {text, PredicateBuilder::IsNull(1, "new_value", FieldType::INT),
             PredicateBuilder::GreaterThan(2, "f3", FieldType::DOUBLE, Literal(13.5))}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> index,
                         MockSearchFileIndex::MakeQueryIndex(
                             "f0", MockSearchFileIndex::FULL_TEXT_IDENTIFIER,
                             {{query, {{{0, 0.25f}, {1, 0.5f}, {2, 0.75f}}, 0.5}}}, pool_));
    ReadContextBuilder builder(SearchTablePath());
    builder.EnablePreciseFileIndexEvaluate(true);
    builder.SetReadFieldNames({"renamed_f0", "new_value", "f3", "_renamed_f0_INDEX_SCORE"})
        .SetOptions(options)
        .SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadWithFileIndex(context, index, RoaringBitmap32(), current));
    CheckSearchResult(
        result,
        {arrow::field("renamed_f0", arrow::utf8()), arrow::field("new_value", arrow::int32()),
         arrow::field("f3", arrow::float64()),
         arrow::field("_renamed_f0_INDEX_SCORE", arrow::float32())},
        R"([["Tony",null,14.1,0.75]])");

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> nonnull,
        PredicateBuilder::And({text, PredicateBuilder::IsNotNull(1, "new_value", FieldType::INT)}));
    ReadContextBuilder nonnull_builder(SearchTablePath());
    nonnull_builder.EnablePreciseFileIndexEvaluate(true);
    nonnull_builder.SetReadFieldNames({"renamed_f0", "new_value", "f3", "_renamed_f0_INDEX_SCORE"})
        .SetOptions(options)
        .SetPredicate(nonnull);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> nonnull_context, nonnull_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> empty,
                         ReadWithFileIndex(nonnull_context, index, RoaringBitmap32(), current));
    ASSERT_TRUE(!empty || empty->length() == 0);
}

TEST_F(PreciseFileIndexReadTest, TestMixedPredicateFullTextAndVectorScoresAfterFiltering) {
    const std::string query = R"({"match":{"query":"all"}})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> text,
        PredicateBuilder::FullTextMatch(1, std::make_shared<FullTextMatch>("f0", query)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> predicate,
        PredicateBuilder::And(
            {text, PredicateBuilder::GreaterThan(2, "f3", FieldType::DOUBLE, Literal(12.5))}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> text_payload,
                         MockSearchFileIndex::MakeQueryPayload(
                             {{query, {{{0, 0.25f}, {1, 0.5f}, {2, 0.75f}}, 0.5}}}, pool_));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> vector_payload,
        MockSearchFileIndex::MakePayload({{0, 0.125f}, {1, 0.25f}, {2, 0.5f}}, pool_));
    FileIndexFormat::ColumnIndexes indexes;
    indexes["f0"][MockSearchFileIndex::FULL_TEXT_IDENTIFIER] = text_payload;
    indexes["f3"][MockSearchFileIndex::VECTOR_IDENTIFIER] = vector_payload;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> index,
                         MockSearchFileIndex::Serialize(indexes, pool_));
    auto search = std::make_shared<VectorSearch>("f3", 1, std::vector<float>{1.0f}, nullptr,
                                                 nullptr, VectorSearch::DistanceType::EUCLIDEAN,
                                                 std::map<std::string, std::string>{});
    ReadContextBuilder builder(SearchTablePath());
    builder.EnablePreciseFileIndexEvaluate(true);
    builder.SetReadFieldNames({"_f3_INDEX_SCORE", "f0", "f3", "_f0_INDEX_SCORE"})
        .SetPredicate(predicate)
        .SetVectorSearch(search)
        .AddOption(Options::READ_BATCH_SIZE, "1");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    for (int32_t case_id : {0, 1, 2}) {
        const auto deleted = case_id == 1 ? RoaringBitmap32::From({1}) : RoaringBitmap32();
        const std::optional<std::vector<Range>> ranges =
            case_id == 2 ? std::make_optional(std::vector<Range>{Range(2, 2)}) : std::nullopt;
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                             ReadWithFileIndex(context, index, deleted, nullptr, ranges));
        CheckSearchResult(
            result,
            {arrow::field("_f3_INDEX_SCORE", arrow::float32()), arrow::field("f0", arrow::utf8()),
             arrow::field("f3", arrow::float64()),
             arrow::field("_f0_INDEX_SCORE", arrow::float32())},
            case_id == 0 ? R"([[0.25,"Emily",13.1,0.5]])" : R"([[0.5,"Tony",14.1,0.75]])");
    }
}

TEST_F(PreciseFileIndexReadTest, TestFullTextRootScoresAndLimitAfterScalarDvAndRanges) {
    const std::string query = R"({"match":{"query":"ranked"}})";
    auto search = std::make_shared<FullTextSearch>("f0", query, 1, RoaringBitmap64::From({0, 2}));
    ReadContextBuilder builder(SearchTablePath());
    builder.EnablePreciseFileIndexEvaluate(true);
    builder.SetReadFieldNames({"f0", "f3", "_f0_INDEX_SCORE"})
        .SetFullTextSearch(search)
        .SetPredicate(PredicateBuilder::GreaterThan(1, "f3", FieldType::DOUBLE, Literal(13.5)))
        .AddOption(Options::READ_BATCH_SIZE, "1");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> index,
        MockSearchFileIndex::MakeQueryIndex(
            "f0", MockSearchFileIndex::FULL_TEXT_IDENTIFIER,
            {{query, {{{0, 0.25f}, {1, 0.5f}, {2, 0.75f}}, std::nullopt}}}, pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadWithFileIndex(context, index, RoaringBitmap32::From({0}), nullptr,
                                           std::vector<Range>{Range(1, 2)}));
    CheckSearchResult(result,
                      {arrow::field("f0", arrow::utf8()), arrow::field("f3", arrow::float64()),
                       arrow::field("_f0_INDEX_SCORE", arrow::float32())},
                      R"([["Tony",14.1,0.75]])");
    ASSERT_EQ(search->include_row_ids, RoaringBitmap64::From({0, 2}));
}

TEST_F(PreciseFileIndexReadTest, TestPreciseEvaluationRejectsBitmap64DeletionVectors) {
    const std::string query = R"({"match":{"query":"all"}})";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> text,
        MockSearchFileIndex::MakeQueryPayload(
            {{query, {{{0, 0.25f}, {1, 0.5f}, {2, 0.75f}}, std::nullopt}}}, pool_));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> vector,
        MockSearchFileIndex::MakePayload({{0, 0.25f}, {1, 0.5f}, {2, 0.75f}}, pool_));
    FileIndexFormat::ColumnIndexes indexes;
    indexes["f0"][MockSearchFileIndex::FULL_TEXT_IDENTIFIER] = text;
    indexes["f3"][MockSearchFileIndex::VECTOR_IDENTIFIER] = vector;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> index,
                         MockSearchFileIndex::Serialize(indexes, pool_));
    for (const std::string& query_type :
         std::vector<std::string>{"none", "scalar", "match", "full-text", "vector"}) {
        ReadContextBuilder builder(SearchTablePath());
        builder.SetReadFieldNames({"f0", "f3"}).EnablePreciseFileIndexEvaluate(true);
        if (query_type == "scalar") {
            builder.SetPredicate(
                PredicateBuilder::GreaterThan(1, "f3", FieldType::DOUBLE, Literal(13.0)));
        } else if (query_type == "match") {
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<Predicate> predicate,
                PredicateBuilder::FullTextMatch(0, std::make_shared<FullTextMatch>("f0", query)));
            builder.SetPredicate(predicate);
        } else if (query_type == "full-text") {
            builder.SetFullTextSearch(std::make_shared<FullTextSearch>("f0", query, /*limit=*/1));
        } else if (query_type == "vector") {
            builder.SetVectorSearch(std::make_shared<VectorSearch>(
                "f3", /*limit=*/1, std::vector<float>{1.0f}, nullptr, nullptr,
                VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{}));
        }
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
        for (bool empty_dv : {false, true}) {
            auto dv = std::make_shared<Bitmap64DeletionVector>();
            if (!empty_dv) {
                ASSERT_OK(dv->Delete(1));
            }
            for (bool empty_candidates : {false, true}) {
                const std::optional<std::vector<Range>> ranges =
                    empty_candidates ? std::make_optional(std::vector<Range>()) : std::nullopt;
                ASSERT_NOK_WITH_MSG(
                    ReadWithFileIndex(context, index, RoaringBitmap32(), nullptr, ranges, dv),
                    "Precise file-index evaluation requires bitmap32 deletion vectors");
            }
        }
    }
}

TEST_F(PreciseFileIndexReadTest, TestPreciseEvaluationRejectsRowsAboveBitmap32Limit) {
    for (const std::string& search_type :
         std::vector<std::string>{"none", "scalar", "match", "full-text", "vector"}) {
        ReadContextBuilder builder(SearchTablePath());
        builder.EnablePreciseFileIndexEvaluate(true);
        builder.SetReadFieldNames({"f0", "f3"});
        if (search_type == "scalar") {
            builder.SetPredicate(PredicateBuilder::IsNotNull(0, "f0", FieldType::STRING));
        } else if (search_type == "match") {
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<Predicate> predicate,
                PredicateBuilder::FullTextMatch(
                    0, std::make_shared<FullTextMatch>("f0", R"({"match":{"query":"all"}})")));
            builder.SetPredicate(predicate);
        } else if (search_type == "full-text") {
            builder.SetFullTextSearch(std::make_shared<FullTextSearch>(
                "f0", R"({"match":{"query":"all"}})", /*limit=*/1));
        } else if (search_type == "vector") {
            builder.SetVectorSearch(std::make_shared<VectorSearch>(
                "f3", /*limit=*/1, std::vector<float>{1.0f}, nullptr, nullptr,
                VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{}));
        }
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
        ASSERT_NOK_WITH_MSG(
            ReadWithFileIndex(context, /*index=*/nullptr, RoaringBitmap32(), nullptr,
                              /*row_ranges=*/std::nullopt,
                              /*deletion_vector=*/nullptr, int64_t{RoaringBitmap32::MAX_VALUE} + 1),
            "Precise file-index evaluation requires bitmap32 physical row positions");
    }
}

TEST_F(PreciseFileIndexReadTest, TestPreciseEvaluationAtBitmap32Limit) {
    for (bool has_predicate : {false, true}) {
        ReadContextBuilder builder(SearchTablePath());
        builder.SetReadFieldNames({"f0", "f3"}).EnablePreciseFileIndexEvaluate(true);
        if (has_predicate) {
            builder.SetPredicate(
                PredicateBuilder::GreaterThan(1, "f3", FieldType::DOUBLE, Literal(13.0)));
        }
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::ChunkedArray> result,
            ReadWithFileIndex(context, /*index=*/nullptr, RoaringBitmap32(), nullptr,
                              std::vector<Range>{Range(0, 2)},
                              /*deletion_vector=*/nullptr, RoaringBitmap32::MAX_VALUE));
        CheckSearchResult(result,
                          {arrow::field("f0", arrow::utf8()), arrow::field("f3", arrow::float64())},
                          has_predicate ? R"([["Emily",13.1],["Tony",14.1]])"
                                        : R"([["Bob",12.1],["Emily",13.1],["Tony",14.1]])");
    }
}

}  // namespace paimon::test
