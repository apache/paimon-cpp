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

#include "paimon/core/operation/internal_read_context.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/data/shredding/map_shared_shredding_schema_utils.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(InternalReadContext, TestReadWithUnspecifiedSchema) {
    // no read schema is specified, read all fields
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithSpecifiedSchema) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"f3", "f0"});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestHeaderOptionSurvivesIntoCoreOptions) {
    // The `header.` option a read through a REST catalog reports its starting table with has to
    // still be among the options a dependency table is read through: `DataEvolutionSplitRead`
    // builds that catalog context out of `CoreOptions::ToMap()`, so an option dropped on the way
    // here would quietly drop the `X-Paimon-Read-Via` delegation header along with it.
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

    // The merge `TableRead::Create` performs: the table's own options first, then the ones the read
    // context carries over them, which is where a header option arrives from.
    const std::string read_via = "%7B%22database%22%3A%22db1%22%2C%22object%22%3A%22t1%22%7D";
    std::map<std::string, std::string> options = table_schema->Options();
    options["header.X-Paimon-Read-Via"] = read_via;
    ASSERT_OK_AND_ASSIGN(auto internal_context, InternalReadContext::Create(std::move(read_context),
                                                                            table_schema, options));
    ASSERT_EQ(internal_context->GetCoreOptions().ToMap().at("header.X-Paimon-Read-Via"), read_via);
}

TEST(InternalReadContext, TestFileSystemSchemeMapReachesTheDependencyRead) {
    // A blob view reads its dependency table through a catalog of its own, which builds a file
    // system out of the options and this map rather than borrowing the one of the read it descends
    // from. Nothing reading a local path would notice the map going missing: a location is
    // resolved through it only when its scheme is not the default of the options, so dropping it
    // surfaces as a `dfs://` dependency table that cannot be opened, far away from the cause.
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    const std::map<std::string, std::string> scheme_map = {{"file", "local"}};
    ReadContextBuilder context_builder(path);
    context_builder.WithFileSystemSchemeToIdentifierMap(scheme_map);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    ASSERT_EQ(internal_context->GetFileSystemSchemeToIdentifierMap(), scheme_map);
}

TEST(InternalReadContext, TestReadWithSpecifiedFieldId) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldIds({3, 0});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithSpecifiedFieldIdAndSchema) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    // read schema is specified, read fields in schema
    // will use field ids instead of field names.
    context_builder.SetReadFieldNames({"f0"});
    context_builder.SetReadFieldIds({3, 0});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithRowTrackingAndScoreFields) {
    {
        // test simple
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames(
            {"f3", "f0", "_ROW_ID", "_SEQUENCE_NUMBER", "_INDEX_SCORE"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        auto new_options = table_schema->Options();
        new_options[Options::ROW_TRACKING_ENABLED] = "true";
        new_options[Options::DATA_EVOLUTION_ENABLED] = "true";
        ASSERT_OK_AND_ASSIGN(
            auto internal_context,
            InternalReadContext::Create(std::move(read_context), table_schema, new_options));
        std::vector<DataField> read_fields = {
            DataField(3, arrow::field("f3", arrow::float64())),
            DataField(0, arrow::field("f0", arrow::utf8())), SpecialFields::RowId(),
            SpecialFields::SequenceNumber(), SpecialFields::IndexScore()};
        auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
        ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
    }
    {
        // test invalid case: disable row tracking while read row tracking fields
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f3", "f0", "_ROW_ID", "_SEQUENCE_NUMBER"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        ASSERT_NOK_WITH_MSG(InternalReadContext::Create(std::move(read_context), table_schema,
                                                        table_schema->Options()),
                            "Get field _ROW_ID failed: not exist in table schema");
    }
    {
        // test invalid case: disable data evolution while read score fields
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f3", "f0", "_INDEX_SCORE"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        ASSERT_NOK_WITH_MSG(InternalReadContext::Create(std::move(read_context), table_schema,
                                                        table_schema->Options()),
                            "Get field _INDEX_SCORE failed: not exist in table schema");
    }
}

TEST(InternalReadContext, TestVectorSearchIndexScoreProjection) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    std::shared_ptr<VectorSearch> vector_search = std::make_shared<VectorSearch>(
        "f3", /*limit=*/2, std::vector<float>{1.0f}, nullptr, nullptr,
        VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});

    {
        ReadContextBuilder builder(path);
        builder.SetReadFieldNames({"f0", "_INDEX_SCORE"}).SetVectorSearch(vector_search);
        ASSERT_OK_AND_ASSIGN(auto read_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto internal_context,
                             InternalReadContext::Create(std::move(read_context), table_schema,
                                                         table_schema->Options()));
        std::shared_ptr<arrow::Schema> expected = DataField::ConvertDataFieldsToArrowSchema(
            {DataField(0, arrow::field("f0", arrow::utf8())), SpecialFields::IndexScore()});
        EXPECT_TRUE(internal_context->GetReadSchema()->Equals(expected));
    }
    {
        ReadContextBuilder builder(path);
        builder.SetReadFieldIds({0, SpecialFieldIds::INDEX_SCORE}).SetVectorSearch(vector_search);
        ASSERT_OK_AND_ASSIGN(auto read_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto internal_context,
                             InternalReadContext::Create(std::move(read_context), table_schema,
                                                         table_schema->Options()));
        EXPECT_EQ(arrow::Type::FLOAT, internal_context->GetReadSchema()->field(1)->type()->id());
    }
    {
        std::shared_ptr<arrow::Schema> projected = arrow::schema(
            {arrow::field("_INDEX_SCORE", arrow::float32()), arrow::field("f0", arrow::utf8())});
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*projected, c_schema.get()).ok());
        ReadContextBuilder builder(path);
        builder.SetReadSchema(std::move(c_schema)).SetVectorSearch(vector_search);
        ASSERT_OK_AND_ASSIGN(auto read_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto internal_context,
                             InternalReadContext::Create(std::move(read_context), table_schema,
                                                         table_schema->Options()));
        EXPECT_EQ(std::vector<std::string>({"_INDEX_SCORE", "f0"}),
                  internal_context->GetReadSchema()->field_names());
    }
}

TEST(InternalReadContext, TestPrimaryKeyVectorSearchRequiresDeletionVectors) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality";
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), path);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema, schema_manager.ReadSchema(0));
    std::shared_ptr<VectorSearch> search =
        std::make_shared<VectorSearch>("f3", /*limit=*/1, std::vector<float>{1.0f}, nullptr,
                                       nullptr, std::nullopt, std::map<std::string, std::string>{});
    ReadContextBuilder builder(path);
    builder.SetReadFieldNames({"f0", "_INDEX_SCORE"}).SetVectorSearch(search);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<InternalReadContext> internal,
        InternalReadContext::Create(context, table_schema, table_schema->Options()));
    EXPECT_TRUE(internal->HasVectorOrFullTextSearch());

    std::map<std::string, std::string> options = table_schema->Options();
    options[Options::DELETION_VECTORS_ENABLED] = "false";
    ASSERT_NOK_WITH_MSG(InternalReadContext::Create(context, table_schema, options),
                        "primary-key tables requires deletion vectors");
}

TEST(InternalReadContext, TestFileFullTextSearchScoreIsNotSupported) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), path);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema, schema_manager.ReadSchema(0));
    auto search =
        std::make_shared<FullTextSearch>("f0", R"({"match":{"query":"query"}})", /*limit=*/1);
    ReadContextBuilder builder(path);
    builder.SetReadFieldNames({"f0", "_INDEX_SCORE"}).SetFullTextSearch(search);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
    ASSERT_NOK_WITH_MSG(InternalReadContext::Create(context, table_schema, table_schema->Options()),
                        "does not support score output");
}

TEST(InternalReadContext, TestFullTextMatchValidatesProjection) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), path);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema, schema_manager.ReadSchema(0));
    struct TestCase {
        int32_t field_index;
        std::string field_name;
        std::vector<std::string> projection;
        std::string expected_error;
    };
    const std::vector<TestCase> cases = {
        {0, "f0", {"f0"}, ""},
        {0, "f0", {"f1"}, "field f0 does not exist in schema"},
        {1,
         "f0",
         {"f0"},
         "field f0 has field idx 0 in input schema, mismatch field idx 1 in predicate"},
        {0, "f1", {"f1"}, "schema type int32 mismatches predicate field type STRING"}};
    for (const auto& test_case : cases) {
        auto match =
            std::make_shared<FullTextMatch>(test_case.field_name, R"({"match":{"query":"apple"}})");
        ASSERT_OK_AND_ASSIGN(auto predicate,
                             PredicateBuilder::FullTextMatch(test_case.field_index, match));
        ReadContextBuilder builder(path);
        builder.SetReadFieldNames(test_case.projection).SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
        if (test_case.expected_error.empty()) {
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<InternalReadContext> internal,
                InternalReadContext::Create(context, table_schema, table_schema->Options()));
            ASSERT_TRUE(internal->GetIndexScoreBindings().empty());
            ASSERT_FALSE(internal->NeedsScores(
                {InternalReadContext::IndexScoreSource::Type::FULL_TEXT_MATCH, match->field_name}));
            ASSERT_NOK_WITH_MSG(
                InternalReadContext::CreateWithSchema(internal, internal->GetReadSchema()),
                "CreateWithSchema does not support");
        } else {
            ASSERT_NOK_WITH_MSG(
                InternalReadContext::Create(context, table_schema, table_schema->Options()),
                test_case.expected_error);
        }
    }
}

TEST(InternalReadContext, TestPerFieldTopKScoreBindings) {
    using SourceType = InternalReadContext::IndexScoreSource::Type;
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), path);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema, schema_manager.ReadSchema(0));
    for (bool vector_search : {false, true}) {
        for (bool project_score : {false, true}) {
            ReadContextBuilder builder(path);
            std::vector<std::string> projection = {"f1"};
            if (project_score) {
                projection.push_back(vector_search ? "_f3_INDEX_SCORE" : "_f0_INDEX_SCORE");
            }
            builder.SetReadFieldNames(projection);
            const InternalReadContext::IndexScoreSource source = {
                vector_search ? SourceType::VECTOR_SEARCH : SourceType::FULL_TEXT_SEARCH,
                vector_search ? "f3" : "f0"};
            if (vector_search) {
                auto search = std::make_shared<VectorSearch>(
                    "f3", /*limit=*/1, std::vector<float>{1.0f}, nullptr, nullptr, std::nullopt,
                    std::map<std::string, std::string>{});
                builder.SetVectorSearch(search);
            } else {
                auto search = std::make_shared<FullTextSearch>(
                    "f0", R"({"match":{"query":"apple"}})", /*limit=*/1);
                builder.SetFullTextSearch(search);
            }
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<InternalReadContext> internal,
                InternalReadContext::Create(context, table_schema, table_schema->Options()));
            ASSERT_EQ(internal->GetReadSchema()->field_names(), projection);
            ASSERT_TRUE(internal->NeedsScores(source));
            ASSERT_FALSE(internal->NeedsScores(
                {vector_search ? SourceType::FULL_TEXT_SEARCH : SourceType::VECTOR_SEARCH,
                 source.field_name}));
            ASSERT_FALSE(internal->NeedsScores({SourceType::FULL_TEXT_MATCH, source.field_name}));
            ASSERT_FALSE(internal->NeedsScores({source.type, "missing"}));
            const auto& bindings = internal->GetIndexScoreBindings();
            if (project_score) {
                const std::map<int32_t, InternalReadContext::IndexScoreSource> expected = {
                    {1, source}};
                ASSERT_EQ(bindings, expected);
                ASSERT_EQ(internal->GetReadSchema()->field(1)->type()->id(), arrow::Type::FLOAT);
            } else {
                ASSERT_TRUE(bindings.empty());
            }
            ASSERT_NOK_WITH_MSG(
                InternalReadContext::CreateWithSchema(internal, internal->GetReadSchema()),
                "CreateWithSchema does not support");
        }
    }
}

class InternalReadContextScoreTest : public testing::Test {
 protected:
    void SetUp() override {
        ASSERT_OK_AND_ASSIGN(table_schema_,
                             TableSchema::Create(0,
                                                 arrow::schema({arrow::field("F1", arrow::utf8()),
                                                                arrow::field("F2", arrow::utf8())}),
                                                 {}, {}, {}));
    }

    Result<std::unique_ptr<InternalReadContext>> Create(ReadContextBuilder* builder) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> context, builder->Finish());
        return InternalReadContext::Create(context, table_schema_, table_schema_->Options());
    }

    std::shared_ptr<TableSchema> table_schema_;
};

TEST_F(InternalReadContextScoreTest, BindsFullTextScoresByProjection) {
    using SourceType = InternalReadContext::IndexScoreSource::Type;
    auto first = std::make_shared<FullTextMatch>("F1", R"({"match":{"query":"apple"}})");
    auto second = std::make_shared<FullTextMatch>("F2", first->query);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> first_predicate,
                         PredicateBuilder::FullTextMatch(0, first));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> second_predicate,
                         PredicateBuilder::FullTextMatch(1, second));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({first_predicate, second_predicate}));
    struct TestCase {
        int32_t score_count;
        bool explicit_schema;
        bool reverse_scores;
    };
    const std::vector<TestCase> cases = {{0, false, false}, {1, false, false}, {2, false, false},
                                         {2, false, true},  {2, true, false},  {2, true, true}};
    for (const auto& test_case : cases) {
        const int32_t score_count = test_case.score_count;
        ReadContextBuilder builder("/tmp/unused-table-path");
        std::vector<std::string> projection = {"F1", "F2"};
        if (score_count >= 1) {
            projection.push_back(test_case.reverse_scores ? "_F1_INDEX_SCORE" : "_F2_INDEX_SCORE");
        }
        if (score_count == 2) {
            projection.push_back(test_case.reverse_scores ? "_F2_INDEX_SCORE" : "_F1_INDEX_SCORE");
        }
        builder.SetPredicate(predicate);
        if (test_case.explicit_schema) {
            arrow::FieldVector fields = {arrow::field("F1", arrow::utf8()),
                                         arrow::field("F2", arrow::utf8())};
            for (int32_t i = 0; i < score_count; ++i) {
                fields.push_back(arrow::field(projection[i + 2], arrow::float32()));
            }
            auto c_schema = std::make_unique<ArrowSchema>();
            ASSERT_TRUE(arrow::ExportSchema(*arrow::schema(fields), c_schema.get()).ok());
            builder.SetReadSchema(std::move(c_schema));
        } else if (score_count > 0) {
            builder.SetReadFieldNames(projection);
        }
        ASSERT_OK_AND_ASSIGN(auto internal, Create(&builder));
        ASSERT_EQ(internal->GetReadSchema()->field_names(), projection);
        ASSERT_EQ(internal->NeedsScores({SourceType::FULL_TEXT_MATCH, first->field_name}),
                  score_count == 2);
        ASSERT_EQ(internal->NeedsScores({SourceType::FULL_TEXT_MATCH, second->field_name}),
                  score_count >= 1);
        auto copied_match = std::make_shared<FullTextMatch>(*second);
        ASSERT_EQ(internal->NeedsScores({SourceType::FULL_TEXT_MATCH, copied_match->field_name}),
                  score_count >= 1);
        ASSERT_FALSE(internal->NeedsScores({SourceType::FULL_TEXT_SEARCH, second->field_name}));
        ASSERT_FALSE(internal->NeedsScores({SourceType::FULL_TEXT_MATCH, "missing"}));
        const auto& bindings = internal->GetIndexScoreBindings();
        ASSERT_EQ(bindings.size(), score_count);
        ASSERT_OK_AND_ASSIGN(std::vector<DataField> fields,
                             DataField::ConvertArrowSchemaToDataFields(internal->GetReadSchema()));
        for (int32_t i = 0; i < score_count; ++i) {
            const int32_t output_idx = i + 2;
            auto iter = bindings.find(output_idx);
            ASSERT_NE(iter, bindings.end());
            ASSERT_EQ(iter->second.type, SourceType::FULL_TEXT_MATCH);
            ASSERT_EQ(iter->second.field_name,
                      projection[output_idx] == "_F1_INDEX_SCORE" ? "F1" : "F2");
            ASSERT_EQ(fields[output_idx].Type()->id(), arrow::Type::FLOAT);
            ASSERT_LT(fields[output_idx].Id(), 0);
            ASSERT_NE(fields[output_idx].Id(), SpecialFieldIds::INDEX_SCORE);
            for (int32_t previous = 0; previous < output_idx; ++previous) {
                ASSERT_NE(fields[output_idx].Id(), fields[previous].Id());
            }
        }
    }
}

TEST_F(InternalReadContextScoreTest, BindsRootAndPredicateScores) {
    using SourceType = InternalReadContext::IndexScoreSource::Type;
    const InternalReadContext::IndexScoreSource search_source = {SourceType::FULL_TEXT_SEARCH,
                                                                 "F2"};
    const InternalReadContext::IndexScoreSource match_source = {SourceType::FULL_TEXT_MATCH, "F1"};
    auto match = std::make_shared<FullTextMatch>("F1", R"({"match":{"query":"apple"}})");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::FullTextMatch(0, match));
    auto search = std::make_shared<FullTextSearch>("F2", match->query, /*limit=*/10);
    ReadContextBuilder builder("/tmp/unused-table-path");
    builder.SetPredicate(predicate).SetFullTextSearch(search).SetReadFieldNames(
        {"F1", "_F2_INDEX_SCORE", "_F1_INDEX_SCORE"});
    ASSERT_OK_AND_ASSIGN(auto internal, Create(&builder));
    ASSERT_EQ(internal->GetReadSchema()->GetFieldIndex("F2"), -1);
    const auto& bindings = internal->GetIndexScoreBindings();
    const std::map<int32_t, InternalReadContext::IndexScoreSource> expected = {{1, search_source},
                                                                               {2, match_source}};
    ASSERT_EQ(bindings, expected);
    ASSERT_TRUE(internal->NeedsScores(search_source));
    ASSERT_TRUE(internal->NeedsScores(match_source));

    builder.SetPredicate(predicate).SetFullTextSearch(search).SetReadFieldNames(
        {"F1", "_F2_INDEX_SCORE", "F2", "_F1_INDEX_SCORE"});
    ASSERT_OK_AND_ASSIGN(internal, Create(&builder));
    const std::map<int32_t, InternalReadContext::IndexScoreSource> non_contiguous = {
        {1, search_source}, {3, match_source}};
    ASSERT_EQ(internal->GetIndexScoreBindings(), non_contiguous);

    auto scalar = PredicateBuilder::IsNotNull(0, "F1", FieldType::STRING);
    ASSERT_OK_AND_ASSIGN(predicate, PredicateBuilder::Or({predicate, scalar}));
    builder.SetPredicate(predicate).SetFullTextSearch(search).SetReadFieldNames(
        {"F1", "_F2_INDEX_SCORE"});
    ASSERT_OK_AND_ASSIGN(internal, Create(&builder));
    ASSERT_TRUE(internal->NeedsScores(search_source));
    ASSERT_FALSE(internal->NeedsScores(match_source));
    ASSERT_EQ(internal->GetIndexScoreBindings().size(), 1);
}

TEST_F(InternalReadContextScoreTest, RejectsAmbiguousOrConditionalScoreProjection) {
    using SourceType = InternalReadContext::IndexScoreSource::Type;
    auto match = std::make_shared<FullTextMatch>("F1", R"({"match":{"query":"apple"}})");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> text,
                         PredicateBuilder::FullTextMatch(0, match));
    auto scalar = PredicateBuilder::IsNotNull(0, "F1", FieldType::STRING);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> duplicate, PredicateBuilder::And({text, text}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> conditional,
                         PredicateBuilder::Or({text, scalar}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> scalar_or,
                         PredicateBuilder::Or({scalar, scalar}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> valid,
                         PredicateBuilder::And({text, scalar_or}));
    struct TestCase {
        std::shared_ptr<Predicate> predicate;
        bool root_search;
        std::string error;
    };
    const std::vector<TestCase> cases = {
        {duplicate, false, "Score-producing search field 'F1' occurs more than once"},
        {text, true, "Score-producing search field 'F1' occurs more than once"},
        {conditional, false, "Score-producing FullTextMatch cannot appear under OR"},
        {valid, false, ""}};
    for (const auto& test_case : cases) {
        for (bool project_score : {false, true}) {
            ReadContextBuilder builder("/tmp/unused-table-path");
            builder.SetPredicate(test_case.predicate)
                .SetReadFieldNames(project_score ? std::vector<std::string>{"F1", "_F1_INDEX_SCORE"}
                                                 : std::vector<std::string>{"F1"});
            if (test_case.root_search) {
                builder.SetFullTextSearch(
                    std::make_shared<FullTextSearch>("F1", match->query, /*limit=*/1));
            }
            if (project_score && !test_case.error.empty()) {
                ASSERT_NOK_WITH_MSG(Create(&builder), test_case.error);
            } else {
                ASSERT_OK_AND_ASSIGN(auto internal, Create(&builder));
                ASSERT_EQ(internal->GetIndexScoreBindings().empty(), !project_score);
                ASSERT_EQ(internal->NeedsScores({SourceType::FULL_TEXT_SEARCH, "F1"}),
                          test_case.root_search);
                ASSERT_EQ(internal->NeedsScores({SourceType::FULL_TEXT_MATCH, "F1"}),
                          project_score);
            }
        }
    }
}

TEST_F(InternalReadContextScoreTest, RejectsMissingScoreSource) {
    for (const auto& name : {"_F1_INDEX_SCORE", "_missing_INDEX_SCORE"}) {
        ReadContextBuilder builder("/tmp/unused-table-path");
        builder.SetReadFieldNames({name});
        ASSERT_NOK_WITH_MSG(Create(&builder),
                            fmt::format("Index score field '{}' has no search source", name));
    }
}

TEST_F(InternalReadContextScoreTest, ResolvesScoresOnlyFromEffectiveProjection) {
    const InternalReadContext::IndexScoreSource search_source = {
        InternalReadContext::IndexScoreSource::Type::FULL_TEXT_SEARCH, "F2"};
    auto search = std::make_shared<FullTextSearch>("F2", R"({"match":{"query":"apple"}})", 1);
    for (bool explicit_schema : {false, true}) {
        ReadContextBuilder builder("/tmp/unused-table-path");
        builder.SetFullTextSearch(search)
            .SetReadFieldNames({"_missing_INDEX_SCORE"})
            .SetReadFieldIds({0});
        if (explicit_schema) {
            auto c_schema = std::make_unique<ArrowSchema>();
            ASSERT_TRUE(arrow::ExportSchema(
                            *arrow::schema({arrow::field("_F2_INDEX_SCORE", arrow::float32())}),
                            c_schema.get())
                            .ok());
            builder.SetReadSchema(std::move(c_schema));
        }
        ASSERT_OK_AND_ASSIGN(auto internal, Create(&builder));
        ASSERT_EQ(internal->GetReadSchema()->field_names(),
                  explicit_schema ? std::vector<std::string>{"_F2_INDEX_SCORE"}
                                  : std::vector<std::string>{"F1"});
        ASSERT_TRUE(internal->NeedsScores(search_source));
        const auto& bindings = internal->GetIndexScoreBindings();
        ASSERT_EQ(bindings.size(), explicit_schema ? 1 : 0);
        if (explicit_schema) {
            const std::map<int32_t, InternalReadContext::IndexScoreSource> expected = {
                {0, search_source}};
            ASSERT_EQ(bindings, expected);
        }
    }
}

TEST_F(InternalReadContextScoreTest, SchemaReplacementUsesProvidedSchema) {
    ReadContextBuilder builder("/tmp/unused-table-path");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InternalReadContext> internal, Create(&builder));
    for (const auto& schema :
         {arrow::schema({arrow::field("F1", arrow::utf8())}),
          arrow::schema({arrow::field("_F1_INDEX_SCORE", arrow::float32())})}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InternalReadContext> replaced,
                             InternalReadContext::CreateWithSchema(internal, schema));
        ASSERT_TRUE(replaced->GetReadSchema()->Equals(schema));
        ASSERT_TRUE(replaced->GetIndexScoreBindings().empty());
    }
}

TEST(InternalReadContext, TestReadWithValueKindField) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"f3", "_VALUE_KIND", "f0"});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithFieldIdsAndSpecialFields) {
    {
        // test simple
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        // here we use field ids instead of field names, and specify special ids for row id,
        // sequence number and index score.
        context_builder.SetReadFieldIds({3, 0, SpecialFieldIds::ROW_ID,
                                         SpecialFieldIds::SEQUENCE_NUMBER,
                                         SpecialFieldIds::INDEX_SCORE});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        auto new_options = table_schema->Options();
        new_options[Options::ROW_TRACKING_ENABLED] = "true";
        new_options[Options::DATA_EVOLUTION_ENABLED] = "true";
        ASSERT_OK_AND_ASSIGN(
            auto internal_context,
            InternalReadContext::Create(std::move(read_context), table_schema, new_options));
        std::vector<DataField> read_fields = {
            DataField(3, arrow::field("f3", arrow::float64())),
            DataField(0, arrow::field("f0", arrow::utf8())), SpecialFields::RowId(),
            SpecialFields::SequenceNumber(), SpecialFields::IndexScore()};
        auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
        ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
    }
}

TEST(InternalReadContext, TestReadWithProjectedSchemaAndSpecialFields) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";

    std::vector<DataField> projected_fields = {
        DataField(0, arrow::field("f0", arrow::utf8())), SpecialFields::RowId(),
        SpecialFields::SequenceNumber(), SpecialFields::IndexScore()};
    auto schema_manager = SchemaManager(std::make_shared<LocalFileSystem>(), path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

    // Without options, special fields should be rejected in projected-schema path too.
    {
        auto projected_schema = DataField::ConvertDataFieldsToArrowSchema(projected_fields);
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema(std::move(c_schema));
        ASSERT_OK_AND_ASSIGN(auto unique_read_context, context_builder.Finish());
        std::shared_ptr<ReadContext> read_context = std::move(unique_read_context);
        ASSERT_NOK_WITH_MSG(
            InternalReadContext::Create(read_context, table_schema, table_schema->Options()),
            "not exist in table schema");
    }

    // With options enabled, projected-schema path should accept these special fields.
    auto enabled_options = table_schema->Options();
    enabled_options[Options::ROW_TRACKING_ENABLED] = "true";
    enabled_options[Options::DATA_EVOLUTION_ENABLED] = "true";

    {
        auto projected_schema = DataField::ConvertDataFieldsToArrowSchema(projected_fields);
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema(std::move(c_schema));
        ASSERT_OK_AND_ASSIGN(auto unique_read_context, context_builder.Finish());
        std::shared_ptr<ReadContext> read_context = std::move(unique_read_context);
        ASSERT_OK_AND_ASSIGN(
            auto internal_context,
            InternalReadContext::Create(read_context, table_schema, enabled_options));
        auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(projected_fields);
        ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
    }
}

TEST(InternalReadContext, TestReadWithProjectedSchemaWithoutFieldIds) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";

    auto projected_schema =
        arrow::schema({arrow::field("f3", arrow::float64()), arrow::field("f0", arrow::utf8())});
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto unique_read_context, context_builder.Finish());
    std::shared_ptr<ReadContext> read_context = std::move(unique_read_context);

    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

    ASSERT_OK_AND_ASSIGN(
        auto internal_context,
        InternalReadContext::Create(read_context, table_schema, table_schema->Options()));

    std::vector<DataField> expected_fields = {
        DataField(3, arrow::field("f3", arrow::float64())),
        DataField(0, arrow::field("f0", arrow::utf8())),
    };
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(expected_fields);
    ASSERT_TRUE(
        internal_context->GetReadSchema()->Equals(expected_schema, /*check_metadata=*/true));
}

TEST(InternalReadContext, TestProjectedSchemaMetadataWhitelist) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";

    auto read_field =
        arrow::field("f0", arrow::utf8())
            ->WithMetadata(arrow::KeyValueMetadata::Make(
                {DataField::MAP_SELECTED_KEYS, "custom.key"}, {"k1,k2", "should_not_propagate"}));
    auto projected_schema = arrow::schema({read_field});
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());

    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema(std::move(c_schema));
    ASSERT_OK_AND_ASSIGN(auto unique_read_context, context_builder.Finish());
    std::shared_ptr<ReadContext> read_context = std::move(unique_read_context);

    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

    ASSERT_OK_AND_ASSIGN(
        auto internal_context,
        InternalReadContext::Create(read_context, table_schema, table_schema->Options()));

    auto aligned_field = internal_context->GetReadSchema()->GetFieldByName("f0");
    ASSERT_TRUE(aligned_field);
    ASSERT_TRUE(aligned_field->HasMetadata());
    ASSERT_TRUE(aligned_field->metadata());

    auto selected_keys_result = aligned_field->metadata()->Get(DataField::MAP_SELECTED_KEYS);
    ASSERT_TRUE(selected_keys_result.ok());
    ASSERT_EQ(selected_keys_result.ValueUnsafe(), "k1,k2");

    auto custom_metadata_result = aligned_field->metadata()->Get("custom.key");
    ASSERT_FALSE(custom_metadata_result.ok());
}

TEST(InternalReadContext, TestMapSharedShreddingAccessRequiresSharedShreddingLayout) {
    auto map_field = arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64()));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> unique_table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema({map_field}),
                            /*partition_keys=*/{}, /*primary_keys=*/{}, /*options=*/{}));
    std::shared_ptr<TableSchema> table_schema = std::move(unique_table_schema);

    auto c_map_field = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportField(*map_field, c_map_field.get()).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MapSharedShreddingAccessBuilder> access_builder,
                         MapSharedShreddingAccessBuilder::Create(c_map_field.get()));
    ASSERT_OK(access_builder->AddKey("a"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowSchema> c_access_field, access_builder->Build());
    auto imported_access_field = arrow::ImportField(c_access_field.get());
    ASSERT_TRUE(imported_access_field.ok());
    std::shared_ptr<arrow::Field> access_field = imported_access_field.ValueOrDie();

    auto c_read_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*arrow::schema({access_field}), c_read_schema.get()).ok());
    ReadContextBuilder context_builder("/tmp/unused-table-path");
    context_builder.SetReadSchema(std::move(c_read_schema));
    ASSERT_OK_AND_ASSIGN(auto unique_read_context, context_builder.Finish());
    std::shared_ptr<ReadContext> read_context = std::move(unique_read_context);

    ASSERT_NOK_WITH_MSG(
        InternalReadContext::Create(read_context, table_schema, table_schema->Options()),
        "Selected-key MAP pushdown only supports top-level shared-shredding MAP field: tags");
}

}  // namespace paimon::test
