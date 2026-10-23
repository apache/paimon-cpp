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

#include "paimon/table/source/table_read.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/table.h"
#include "paimon/common/catalog/catalog_context.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/abstract_split_read.h"
#include "paimon/core/operation/split_read.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/source/append_only_table_read.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/key_value_table_read.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/table/format/format_table.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(TableReadTest, TestReadWithInvalidContext) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_10.db/append_10/";
    {
        // read with non-exist field
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f0", "f1", "non-exist"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                            "Get field non-exist failed: not exist in table schema");
    }
    {
        // field type and literal type mismatch
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(int64_t{15}));
        ReadContextBuilder context_builder(path);
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(
            TableRead::Create(std::move(read_context)),
            "field f3 has field type BIGINT in literal, mismatch field type DOUBLE in predicate");
    }
    {
        // field type in predicate mismatch schema
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::BIGINT, Literal(int64_t{15}));
        ReadContextBuilder context_builder(path);
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                            "schema type double mismatches predicate field type BIGINT");
    }
    {
        // field idx in predicate mismatch in schema
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(15.0));
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f3", "f0", "f1"});
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(
            TableRead::Create(std::move(read_context)),
            "field f3 has field idx 0 in input schema, mismatch field idx 2 in predicate");
    }
    {
        // literal cannot be null
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(FieldType::DOUBLE));
        ReadContextBuilder context_builder(path);
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                            "literal cannot be null in predicate, field name f3");
    }
    {
        // schema with duplicate field f3
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f3", "f1", "f3"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                            "validate schema failed: read schema has duplicate field f3");
    }
}

TEST(TableReadTest, TestReadWithSpecifiedInvalidSchema) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"field_no_exist"});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                        "Get field field_no_exist failed: not exist in table schema");
}

TEST(TableReadTest, TestFullTextMatchExecutionIsNotImplemented) {
    auto match = std::make_shared<FullTextMatch>("f0", R"({"match":{"query":"apple"}})");
    ASSERT_OK_AND_ASSIGN(auto full_text, PredicateBuilder::FullTextMatch(/*field_index=*/0, match));
    auto scalar = PredicateBuilder::Equal(/*field_index=*/1, "f1", FieldType::INT, Literal(1));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({scalar, full_text}));
    const std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto fs = std::make_shared<LocalFileSystem>();
    SchemaManager schema_manager(fs, table_path);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(fs, table_path, schema, Identifier("append_09.db", "append_09")));
    for (bool table_entry : {false, true}) {
        ReadContextBuilder builder(table_path);
        builder.SetReadFieldNames({"f0", "f1"}).SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        auto result =
            table_entry ? table->NewRead(std::move(context)) : TableRead::Create(std::move(context));
        ASSERT_NOK_WITH_MSG(result, "FullTextMatch predicate execution is not implemented yet");
        ASSERT_TRUE(result.status().IsNotImplemented());

        builder.SetReadFieldNames({"f0", "f1", "_f0_INDEX_SCORE"}).SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(context, builder.Finish());
        result =
            table_entry ? table->NewRead(std::move(context)) : TableRead::Create(std::move(context));
        ASSERT_NOK_WITH_MSG(result, "Score-producing FullTextMatch cannot appear under OR");
    }
}

TEST(TableReadTest, TestPerFieldScoreExecutionIsNotImplemented) {
    const std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto search =
        std::make_shared<FullTextSearch>("f0", R"({"match":{"query":"apple"}})", /*limit=*/1);
    auto fs = std::make_shared<LocalFileSystem>();
    SchemaManager schema_manager(fs, path);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(fs, path, schema, Identifier("append_09.db", "append_09")));
    for (bool table_entry : {false, true}) {
        ReadContextBuilder builder(path);
        builder.SetFullTextSearch(search).SetReadFieldNames({"f1", "_f0_INDEX_SCORE"});
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        auto result =
            table_entry ? table->NewRead(std::move(context)) : TableRead::Create(std::move(context));
        ASSERT_NOK_WITH_MSG(result, "Per-field index score output is not implemented yet");
        ASSERT_TRUE(result.status().IsNotImplemented());
    }
}

TEST(TableReadTest, TestCreateKeyValueTableRead) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"f0", "f1", "f2", "f3"});
    context_builder.AddOption("read.batch-size", "2");
    context_builder.AddOption("orc.read.enable-lazy-decoding", "true");
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto key_value_table_read = dynamic_cast<KeyValueTableRead*>(table_read.get());
    ASSERT_TRUE(key_value_table_read);
}

TEST(TableReadTest, TestCreateCountReaderRejectsFileIndexSearch) {
    {
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        auto search =
            std::make_shared<FullTextSearch>("f0", R"({"match":{"query":"Alice"}})", /*limit=*/1);
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f0"}).SetFullTextSearch(search);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_NOK_WITH_MSG(table_read->CreateCountReader({}), "VectorSearch or FullTextSearch");
    }
    {
        std::string path = paimon::test::GetDataDir() +
                           "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
        auto search = std::make_shared<VectorSearch>("f3", /*limit=*/1, std::vector<float>{1.0f},
                                                     nullptr, nullptr, std::nullopt,
                                                     std::map<std::string, std::string>{});
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f0"}).SetVectorSearch(search);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_NOK_WITH_MSG(table_read->CreateCountReader({}), "VectorSearch or FullTextSearch");
    }
}

TEST(TableReadTest, TestPrimaryKeyFileIndexSearchDoesNotFallBackToMergeReader) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
    auto search =
        std::make_shared<VectorSearch>("f3", /*limit=*/1, std::vector<float>{1.0f}, nullptr,
                                       nullptr, std::nullopt, std::map<std::string, std::string>{});
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"f0"}).SetVectorSearch(search);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> reader, TableRead::Create(std::move(context)));

    BinaryRow partition = BinaryRowGenerator::GenerateRow({10}, GetDefaultPool().get());
    DataSplitImpl::Builder builder(partition, /*bucket=*/0, /*bucket_path=*/"", /*data_files=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> non_raw,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(false).Build());
    ASSERT_NOK_WITH_MSG(reader->CreateReader(non_raw), "merge file reads");

    DataSplitImpl::Builder raw_builder(partition, /*bucket=*/0, /*bucket_path=*/"",
                                       /*data_files=*/{});
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataSplitImpl> raw,
        raw_builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());
    ASSERT_OK(reader->CreateReader(raw));
    std::vector<std::shared_ptr<Split>> splits = {raw, non_raw};
    ASSERT_NOK_WITH_MSG(reader->CreateReader(splits), "merge file reads");
}

TEST(TableReadTest, TestCreateAppendOnlyTableRead) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"f0", "f1", "f2", "f3"});
    context_builder.AddOption("read.batch-size", "2");
    context_builder.AddOption("orc.read.enable-lazy-decoding", "true");
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto append_only_table_read = dynamic_cast<AppendOnlyTableRead*>(table_read.get());
    ASSERT_TRUE(append_only_table_read);
}

TEST(TableReadTest, TestMergeOptions) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    {
        // The options the schema carries stand in the table the load builds, and a setting the
        // context adds on top - the table was loaded with neither of these keys - is served as
        // given.
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f0", "f1", "f2", "f3"});
        context_builder.AddOption("read.batch-size", "2");
        context_builder.AddOption("orc.read.enable-lazy-decoding", "true");
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        auto append_only_table_read = dynamic_cast<AppendOnlyTableRead*>(table_read.get());
        ASSERT_TRUE(append_only_table_read);
        auto* typed_split_read =
            dynamic_cast<AbstractSplitRead*>(append_only_table_read->split_reads_[0].get());
        ASSERT_TRUE(typed_split_read);
        const auto& core_options = typed_split_read->options_;
        std::map<std::string, std::string> expected_options = {
            {"read.batch-size", "2"},
            {"orc.read.enable-lazy-decoding", "true"},
            {"bucket", "2"},
            {"bucket-key", "f2"},
            {"manifest.format", "orc"},
            {"file.format", "orc"}};
        ASSERT_EQ(expected_options, core_options.ToMap());
    }
    {
        // An option the schema also carries speaks at the load: the table the context is
        // handed back is built with the context's value over the schema's, and the read serves
        // the merged options. A caller holding a table already - a load that is over - is the
        // one that gets a refusal instead, which `TableTest` covers.
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames({"f0", "f1", "f2", "f3"});
        context_builder.AddOption("bucket", "10");
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        auto append_only_table_read = dynamic_cast<AppendOnlyTableRead*>(table_read.get());
        ASSERT_TRUE(append_only_table_read);
        auto* typed_split_read =
            dynamic_cast<AbstractSplitRead*>(append_only_table_read->split_reads_[0].get());
        ASSERT_TRUE(typed_split_read);
        ASSERT_EQ(10, typed_split_read->options_.GetBucket());
    }
}

TEST(TableReadTest, TestDependencyReadWithoutCatalogUsesReadOptions) {
    const std::string read_via_option = "header.X-Paimon-Read-Via";
    const std::map<std::string, std::string> table_options = {
        {Options::FILE_FORMAT, "orc"}, {Options::BUCKET, "-1"}, {read_via_option, "table-view"}};
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> table_schema,
        TableSchema::Create(0, arrow::schema({arrow::field("id", arrow::int32())}),
                            /*partition_keys=*/{}, /*primary_keys=*/{}, table_options));
    ASSERT_OK_AND_ASSIGN(std::string schema_json, table_schema->ToJsonString());

    for (bool explicit_header : {false, true}) {
        ReadContextBuilder builder("warehouse/db.db/table");
        builder.SetTableSchema(schema_json);
        if (explicit_header) {
            builder.AddOption(read_via_option, "read-view");
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, builder.Finish());
        // Exercise the actual table/read merge before creating the dependency context.
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                             TableRead::Create(std::move(read_context)));
        auto* append_only_table_read = dynamic_cast<AppendOnlyTableRead*>(table_read.get());
        ASSERT_NE(append_only_table_read, nullptr);
        const auto& internal_context = append_only_table_read->context_;
        auto dependency_context = internal_context->CreateDependencyCatalogContext();

        auto expected_options = table_options;
        expected_options[read_via_option] = explicit_header ? "read-view" : "table-view";
        ASSERT_EQ(dependency_context->root_path, "warehouse");
        ASSERT_EQ(dependency_context->options, expected_options);
        // The load merged the read options into the table it built - a table is the product of
        // the load - so the schema the read serves carries the explicit header over the one the
        // schema was written with, the same value the dependency context sees.
        ASSERT_EQ(internal_context->GetTableSchema()->Options(), expected_options);
    }
}

}  // namespace paimon::test
