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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/catalog/table.h"
#include "paimon/catalog/table_load_context.h"
#include "paimon/common/catalog/catalog_context.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/abstract_split_read.h"
#include "paimon/core/operation/split_read.h"
#include "paimon/core/schema/schema_manager.h"
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
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/mock/mock_search_file_index.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(TableReadTest, TestReadWithInvalidContext) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_10.db/append_10/";
    {
        // read with non-exist field
        TableLoadContext context_builder_load_context{path};
        std::map<std::string, std::string> context_builder_options;
        ReadContextBuilder context_builder;
        context_builder.SetReadFieldNames({"f0", "f1", "non-exist"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(context_builder_load_context, std::move(read_context),
                                              context_builder_options),
                            "Get field non-exist failed: not exist in table schema");
    }
    {
        // field type and literal type mismatch
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(int64_t{15}));
        TableLoadContext context_builder_load_context{path};
        std::map<std::string, std::string> context_builder_options;
        ReadContextBuilder context_builder;
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(
            TableRead::Create(context_builder_load_context, std::move(read_context),
                              context_builder_options),
            "field f3 has field type BIGINT in literal, mismatch field type DOUBLE in predicate");
    }
    {
        // field type in predicate mismatch schema
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::BIGINT, Literal(int64_t{15}));
        TableLoadContext context_builder_load_context{path};
        std::map<std::string, std::string> context_builder_options;
        ReadContextBuilder context_builder;
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(context_builder_load_context, std::move(read_context),
                                              context_builder_options),
                            "schema type double mismatches predicate field type BIGINT");
    }
    {
        // field idx in predicate mismatch in schema
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(15.0));
        TableLoadContext context_builder_load_context{path};
        std::map<std::string, std::string> context_builder_options;
        ReadContextBuilder context_builder;
        context_builder.SetReadFieldNames({"f3", "f0", "f1"});
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(
            TableRead::Create(context_builder_load_context, std::move(read_context),
                              context_builder_options),
            "field f3 has field idx 0 in input schema, mismatch field idx 2 in predicate");
    }
    {
        // literal cannot be null
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(FieldType::DOUBLE));
        TableLoadContext context_builder_load_context{path};
        std::map<std::string, std::string> context_builder_options;
        ReadContextBuilder context_builder;
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(context_builder_load_context, std::move(read_context),
                                              context_builder_options),
                            "literal cannot be null in predicate, field name f3");
    }
    {
        // schema with duplicate field f3
        TableLoadContext context_builder_load_context{path};
        std::map<std::string, std::string> context_builder_options;
        ReadContextBuilder context_builder;
        context_builder.SetReadFieldNames({"f3", "f1", "f3"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(context_builder_load_context, std::move(read_context),
                                              context_builder_options),
                            "validate schema failed: read schema has duplicate field f3");
    }
}

TEST(TableReadTest, TestReadWithSpecifiedInvalidSchema) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    TableLoadContext context_builder_load_context{path};
    std::map<std::string, std::string> context_builder_options;
    ReadContextBuilder context_builder;
    context_builder.SetReadFieldNames({"field_no_exist"});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(context_builder_load_context, std::move(read_context),
                                          context_builder_options),
                        "Get field field_no_exist failed: not exist in table schema");
}

TEST(TableReadTest, TestFullTextMatchReadEntryAndScoreValidation) {
    auto match = std::make_shared<FullTextMatch>("f0", R"({"match":{"query":"apple"}})");
    ASSERT_OK_AND_ASSIGN(auto full_text, PredicateBuilder::FullTextMatch(/*field_index=*/0, match));
    auto scalar = PredicateBuilder::Equal(/*field_index=*/1, "f1", FieldType::INT, Literal(1));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({scalar, full_text}));
    const std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto fs = std::make_shared<LocalFileSystem>();
    SchemaManager manager(fs, table_path);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(fs, table_path, schema, Identifier("append_09.db", "append_09"), "",
                               "main"));
    for (bool table_entry : {false, true}) {
        TableLoadContext builder_load_context{table_path};
        std::map<std::string, std::string> builder_options;
        ReadContextBuilder builder;
        builder.EnablePreciseFileIndexEvaluate(true)
            .SetReadFieldNames({"f0", "f1"})
            .SetPredicate(predicate);
        auto context_load_context = builder_load_context;
        auto context_options = builder_options;
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        ASSERT_OK(table_entry ? ([&]() -> Result<std::unique_ptr<TableRead>> {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> configured, table->Copy(context_options));
            return configured->NewRead(std::move(context));
        })()
                              : TableRead::Create(context_load_context, std::move(context),
                                                  context_options));

        builder.EnablePreciseFileIndexEvaluate(true)
            .SetReadFieldNames({"f0", "f1", "_f0_INDEX_SCORE"})
            .SetPredicate(predicate);
        context_load_context.path = builder_load_context.path;
        context_load_context.file_system = builder_load_context.file_system;
        context_load_context.fs_scheme_to_identifier_map =
            builder_load_context.fs_scheme_to_identifier_map;
        context_load_context.table_schema_json = builder_load_context.table_schema_json;
        context_load_context.catalog = builder_load_context.catalog;
        context_load_context.table_id = builder_load_context.table_id;
        context_load_context.identifier.reset();
        if (builder_load_context.identifier) {
            context_load_context.identifier.emplace(*builder_load_context.identifier);
        }
        context_options = builder_options;
        ASSERT_OK_AND_ASSIGN(context, builder.Finish());
        ASSERT_NOK_WITH_MSG(table_entry ? ([&]() -> Result<std::unique_ptr<TableRead>> {
                                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> configured,
                                                       table->Copy(context_options));
                                return configured->NewRead(std::move(context));
                            })()
                                        : TableRead::Create(context_load_context,
                                                            std::move(context), context_options),
                            "Score-producing FullTextMatch cannot appear under OR");
    }
}

TEST(TableReadTest, TestPerFieldScoreReadEntry) {
    const std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto search =
        std::make_shared<FullTextSearch>("f0", R"({"match":{"query":"apple"}})", /*limit=*/1);
    auto fs = std::make_shared<LocalFileSystem>();
    SchemaManager manager(fs, path);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(fs, path, schema, Identifier("append_09.db", "append_09"), "",
                               "main"));
    for (bool table_entry : {false, true}) {
        TableLoadContext builder_load_context{path};
        std::map<std::string, std::string> builder_options;
        ReadContextBuilder builder;
        builder.EnablePreciseFileIndexEvaluate(true).SetFullTextSearch(search).SetReadFieldNames(
            {"f1", "_f0_INDEX_SCORE"});
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        ASSERT_OK(table_entry ? ([&]() -> Result<std::unique_ptr<TableRead>> {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> configured, table->Copy(builder_options));
            return configured->NewRead(std::move(context));
        })()
                              : TableRead::Create(builder_load_context, std::move(context),
                                                  builder_options));
    }
}

TEST(TableReadTest, TestMixedFilePredicateProducesScoresThroughPublicReadEntry) {
    const std::string path =
        GetDataDir() + "/orc/multi_partition_append_table.db/multi_partition_append_table";
    TableLoadContext scan_builder_load_context{path};
    std::map<std::string, std::string> scan_builder_options;
    ScanContextBuilder scan_builder;
    scan_builder_options[Options::SCAN_SNAPSHOT_ID] = "1";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         TableScan::Create(scan_builder_load_context, std::move(scan_context),
                                           scan_builder_options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    std::shared_ptr<DataSplitImpl> selected_split;
    for (const auto& split : plan->Splits()) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        ASSERT_TRUE(data_split);
        for (const auto& file : data_split->DataFiles()) {
            if (file->file_name != "data-01b6a930-6564-409b-b8f4-ed1307790d72-0.orc") {
                continue;
            }
            auto indexed_file = std::make_shared<DataFileMeta>(*file);
            ASSERT_OK_AND_ASSIGN(
                indexed_file->embedded_index,
                MockSearchFileIndex::MakeIndex("f0", MockSearchFileIndex::FULL_TEXT_IDENTIFIER,
                                               {{0, 0.25f}, {2, 0.75f}}, GetDefaultPool()));
            DataSplitImpl::Builder split_builder(data_split->Partition(), data_split->Bucket(),
                                                 data_split->BucketPath(), {indexed_file});
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplit> single,
                                 split_builder.WithSnapshot(1).RawConvertible(true).Build());
            selected_split = std::dynamic_pointer_cast<DataSplitImpl>(single);
        }
    }
    ASSERT_TRUE(selected_split);
    auto match = std::make_shared<FullTextMatch>("f0", R"({"match":{"query":"names"}})");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> text,
                         PredicateBuilder::FullTextMatch(2, match));
    auto scalar = PredicateBuilder::GreaterThan(1, "f3", FieldType::DOUBLE, Literal(13.0));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({text, scalar}));
    TableLoadContext builder_load_context{path};
    std::map<std::string, std::string> builder_options;
    ReadContextBuilder builder;
    builder_options[Options::READ_BATCH_SIZE] = "1";
    builder.EnablePreciseFileIndexEvaluate(true)
        .SetReadFieldNames({"_f0_INDEX_SCORE", "f3", "f0"})
        .SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableRead> read,
        TableRead::Create(builder_load_context, std::move(context), builder_options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, read->CreateReader(selected_split));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                         ReadResultCollector::CollectResult(std::move(reader)));
    ASSERT_TRUE(actual);
    ASSERT_EQ(actual->length(), 1);
    auto values = std::dynamic_pointer_cast<arrow::StructArray>(actual->chunk(0));
    ASSERT_TRUE(values);
    auto scores = std::dynamic_pointer_cast<arrow::FloatArray>(values->field(1));
    auto names = std::dynamic_pointer_cast<arrow::StringArray>(values->field(3));
    ASSERT_TRUE(scores);
    ASSERT_TRUE(names);
    ASSERT_FLOAT_EQ(scores->Value(0), 0.75f);
    ASSERT_EQ(names->GetString(0), "Tony");
}

TEST(TableReadTest, TestCreateKeyValueTableRead) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
    TableLoadContext context_builder_load_context{path};
    std::map<std::string, std::string> context_builder_options;
    ReadContextBuilder context_builder;
    context_builder.SetReadFieldNames({"f0", "f1", "f2", "f3"});
    context_builder_options["read.batch-size"] = "2";

    context_builder_options["orc.read.enable-lazy-decoding"] = "true";

    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read,
                         TableRead::Create(context_builder_load_context, std::move(read_context),
                                           context_builder_options));
    auto key_value_table_read = dynamic_cast<KeyValueTableRead*>(table_read.get());
    ASSERT_TRUE(key_value_table_read);
}

TEST(TableReadTest, TestCreateCountReaderRejectsFileIndexSearch) {
    {
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        auto search =
            std::make_shared<FullTextSearch>("f0", R"({"match":{"query":"Alice"}})", /*limit=*/1);
        TableLoadContext context_builder_load_context{path};
        std::map<std::string, std::string> context_builder_options;
        ReadContextBuilder context_builder;
        context_builder.EnablePreciseFileIndexEvaluate(true)
            .SetReadFieldNames({"f0"})
            .SetFullTextSearch(search);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read,
                             TableRead::Create(context_builder_load_context,
                                               std::move(read_context), context_builder_options));
        ASSERT_NOK_WITH_MSG(table_read->CreateCountReader({}),
                            "CreateCountReader does not support precise file-index evaluation");
    }
    {
        std::string path = paimon::test::GetDataDir() +
                           "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
        auto search = std::make_shared<VectorSearch>("f3", /*limit=*/1, std::vector<float>{1.0f},
                                                     nullptr, nullptr, std::nullopt,
                                                     std::map<std::string, std::string>{});
        TableLoadContext context_builder_load_context{path};
        std::map<std::string, std::string> context_builder_options;
        ReadContextBuilder context_builder;
        context_builder.EnablePreciseFileIndexEvaluate(true)
            .SetReadFieldNames({"f0"})
            .SetVectorSearch(search);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read,
                             TableRead::Create(context_builder_load_context,
                                               std::move(read_context), context_builder_options));
        ASSERT_NOK_WITH_MSG(table_read->CreateCountReader({}),
                            "CreateCountReader does not support precise file-index evaluation");
    }
}

TEST(TableReadTest, TestPreciseEvaluationRejectsSystemAndFormatTables) {
    std::string path = GetDataDir() + "/orc/append_09.db/append_09";
    TableLoadContext system_builder_load_context{path + "$snapshots"};
    std::map<std::string, std::string> system_builder_options;
    ReadContextBuilder system_builder;
    system_builder.EnablePreciseFileIndexEvaluate(true);
    ASSERT_OK_AND_ASSIGN(auto system_context, system_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(system_builder_load_context, std::move(system_context),
                                          system_builder_options),
                        "Precise file-index evaluation is not supported for system tables");

    auto directory = UniqueTestDirectory::Create();
    SchemaManager schema_manager(directory->GetFileSystem(), directory->Str());
    ASSERT_OK_AND_ASSIGN(
        auto schema, schema_manager.CreateTable(arrow::schema({arrow::field("f0", arrow::int32())}),
                                                /*partition_keys=*/{}, /*primary_keys=*/{},
                                                {{Options::TYPE, "format-table"}}));
    ASSERT_OK_AND_ASSIGN(auto format_table,
                         FormatTable::Create(directory->GetFileSystem(), directory->Str(),
                                             Identifier("db", "table"), {}));
    for (bool loaded_table : {false, true}) {
        ReadContextBuilder builder;
        TableLoadContext builder_load_context{directory->Str()};
        std::map<std::string, std::string> builder_options;
        builder.EnablePreciseFileIndexEvaluate(true);
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        ASSERT_NOK_WITH_MSG(loaded_table
                                ? format_table->NewRead(std::move(context))
                                : TableRead::Create(builder_load_context, std::move(context)),
                            "Precise file-index evaluation is not supported for format tables");
    }
}

TEST(TableReadTest, TestPreciseScalarEvaluationRejectsUnsupportedTableModes) {
    for (bool primary_key : {false, true}) {
        const std::string path =
            GetDataDir() +
            (primary_key ? "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality"
                         : "/orc/append_09.db/append_09");
        TableLoadContext builder_load_context{path};
        std::map<std::string, std::string> builder_options;
        ReadContextBuilder builder;
        builder.EnablePreciseFileIndexEvaluate(true).SetReadFieldNames({"f3"}).SetPredicate(
            PredicateBuilder::GreaterThan(0, "f3", FieldType::DOUBLE, Literal(0.0)));
        if (primary_key) {
            builder_options[Options::DELETION_VECTORS_ENABLED] = "false";

        } else {
            builder_options[Options::DATA_EVOLUTION_ENABLED] = "true";
        }
        // Declare the unsupported mode in the loaded schema, rather than changing an immutable
        // layout option dynamically, so the test reaches precise-evaluation validation.
        SchemaManager manager(std::make_shared<LocalFileSystem>(), path);
        ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> latest, manager.Latest());
        ASSERT_TRUE(latest);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                             latest.value()->MergeOptions(builder_options));
        ASSERT_OK_AND_ASSIGN(builder_load_context.table_schema_json, schema->GetJsonSchema());
        builder_options.clear();
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        const std::string expected_error =
            primary_key ? "Precise file-index evaluation on primary-key tables requires "
                          "deletion vectors"
                        : "Precise file-index evaluation does not support data-evolution or "
                          "real-time reads";
        ASSERT_NOK_WITH_MSG(
            TableRead::Create(builder_load_context, std::move(context), builder_options),
            expected_error);
    }
}

TEST(TableReadTest, TestPrimaryKeyFileIndexSearchDoesNotFallBackToMergeReader) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
    auto search =
        std::make_shared<VectorSearch>("f3", /*limit=*/1, std::vector<float>{1.0f}, nullptr,
                                       nullptr, std::nullopt, std::map<std::string, std::string>{});
    TableLoadContext context_builder_load_context{path};
    std::map<std::string, std::string> context_builder_options;
    ReadContextBuilder context_builder;
    context_builder.EnablePreciseFileIndexEvaluate(true).SetReadFieldNames({"f0"}).SetVectorSearch(
        search);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> reader,
                         TableRead::Create(context_builder_load_context, std::move(context),
                                           context_builder_options));

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
    TableLoadContext context_builder_load_context{path};
    std::map<std::string, std::string> context_builder_options;
    ReadContextBuilder context_builder;
    context_builder.SetReadFieldNames({"f0", "f1", "f2", "f3"});
    context_builder_options["read.batch-size"] = "2";

    context_builder_options["orc.read.enable-lazy-decoding"] = "true";

    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read,
                         TableRead::Create(context_builder_load_context, std::move(read_context),
                                           context_builder_options));
    auto append_only_table_read = dynamic_cast<AppendOnlyTableRead*>(table_read.get());
    ASSERT_TRUE(append_only_table_read);
}

TEST(TableReadTest, TestMergeOptions) {
    std::string path = paimon::test::GetDataDir() + "orc/append_09.db/append_09";
    TableLoadContext context_builder_load_context{path};
    std::map<std::string, std::string> context_builder_options;
    ReadContextBuilder context_builder;
    context_builder.SetReadFieldNames({"f0", "f1", "f2", "f3"});
    context_builder_options["read.batch-size"] = "2";

    context_builder_options["orc.read.enable-lazy-decoding"] = "true";

    context_builder_options["bucket"] = "10";

    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read,
                         TableRead::Create(context_builder_load_context, std::move(read_context),
                                           context_builder_options));
    auto append_only_table_read = dynamic_cast<AppendOnlyTableRead*>(table_read.get());
    ASSERT_TRUE(append_only_table_read);
    auto* typed_split_read =
        dynamic_cast<AbstractSplitRead*>(append_only_table_read->split_reads_[0].get());
    ASSERT_TRUE(typed_split_read);
    const auto& core_options = typed_split_read->options_;
    std::map<std::string, std::string> expected_options = {
        {"read.batch-size", "2"},
        {"orc.read.enable-lazy-decoding", "true"},
        {"bucket", "10"},
        {"bucket-key", "f2"},
        {"manifest.format", "orc"},
        {"file.format", "orc"},
        {"path", path}};
    ASSERT_EQ(expected_options, core_options.ToMap());
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
        TableLoadContext builder_load_context{"warehouse/db.db/table"};
        std::map<std::string, std::string> builder_options;
        ReadContextBuilder builder;
        builder_load_context.table_schema_json = schema_json;

        if (explicit_header) {
            builder_options[read_via_option] = "read-view";
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, builder.Finish());
        // Exercise the actual table/read merge before creating the dependency context.
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<TableRead> table_read,
            TableRead::Create(builder_load_context, std::move(read_context), builder_options));
        auto* append_only_table_read = dynamic_cast<AppendOnlyTableRead*>(table_read.get());
        ASSERT_NE(append_only_table_read, nullptr);
        const auto& internal_context = append_only_table_read->context_;
        auto dependency_context = internal_context->CreateDependencyCatalogContext();

        auto expected_options = table_options;
        expected_options["path"] = builder_load_context.path;
        expected_options[read_via_option] = explicit_header ? "read-view" : "table-view";
        ASSERT_EQ(dependency_context->root_path, "warehouse");
        ASSERT_EQ(dependency_context->options, expected_options);
        // The operator uses the configured copy; the supplied schema stays unchanged.
        ASSERT_EQ(internal_context->GetTableSchema()->Options(), expected_options);
        ASSERT_EQ(table_schema->Options(), table_options);
    }
}

}  // namespace paimon::test
