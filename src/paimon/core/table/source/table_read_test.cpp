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

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/catalog/table.h"
#include "paimon/common/catalog/catalog_context.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/reader/reader_utils.h"
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
#include "paimon/testing/utils/counting_reader_file_system.h"
#include "paimon/testing/utils/read_result_collector.h"
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

TEST(TableReadTest, SuppliedManagedSchemaIsReleasedBeforeReading) {
    const auto path = GetDataDir() + "/parquet/append_types_compatibility.db/rust_time_types";
    LocalFileSystem fs;
    std::string schema;
    ASSERT_OK(fs.ReadFile(path + "/schema/schema-0", &schema));

    ReadContextBuilder builder(path);
    builder.SetReadFieldIds({0}).SetTableSchema("{");
    ASSERT_OK_AND_ASSIGN(auto invalid_context, builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(invalid_context)), "deserialize failed");

    builder.SetReadFieldIds({0}).SetTableSchema(schema);
    ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
    ASSERT_EQ(context->GetSpecificTableSchema(), schema);
    const auto* retained_context = context.get();
    ASSERT_OK_AND_ASSIGN(auto read, TableRead::Create(std::move(context)));
    // AppendOnlyTableRead retains this exact ReadContext through InternalReadContext.
    ASSERT_NE(dynamic_cast<AppendOnlyTableRead*>(read.get()), nullptr);
    EXPECT_FALSE(retained_context->GetSpecificTableSchema());

    ScanContextBuilder scan_builder(path);
    scan_builder.AddOption("scan.mode", "from-snapshot-full").AddOption("scan.snapshot-id", "1");
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(auto plan, scan->CreatePlan());
    const auto splits = plan->Splits();
    ASSERT_FALSE(splits.empty());
    ASSERT_OK_AND_ASSIGN(auto reader, read->CreateReader(splits.front()));
    ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    const auto rows = arrow::ImportRecordBatch(batch.first.get(), batch.second.get());
    ASSERT_TRUE(rows.ok()) << rows.status();
    EXPECT_GT((*rows)->num_rows(), 0);
    EXPECT_NE((*rows)->schema()->GetFieldByName("id"), nullptr);
    EXPECT_EQ((*rows)->schema()->GetFieldByName("f_time_0"), nullptr);
    EXPECT_FALSE(retained_context->GetSpecificTableSchema());
    reader->Close();
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
        ReadContextBuilder builder(table_path);
        builder.EnablePreciseFileIndexEvaluate(true)
            .SetReadFieldNames({"f0", "f1"})
            .SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        ASSERT_OK(table_entry ? table->NewRead(std::move(context))
                              : TableRead::Create(std::move(context)));

        builder.EnablePreciseFileIndexEvaluate(true)
            .SetReadFieldNames({"f0", "f1", "_f0_INDEX_SCORE"})
            .SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(context, builder.Finish());
        ASSERT_NOK_WITH_MSG(table_entry ? table->NewRead(std::move(context))
                                        : TableRead::Create(std::move(context)),
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
        ReadContextBuilder builder(path);
        builder.EnablePreciseFileIndexEvaluate(true).SetFullTextSearch(search).SetReadFieldNames(
            {"f1", "_f0_INDEX_SCORE"});
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        ASSERT_OK(table_entry ? table->NewRead(std::move(context))
                              : TableRead::Create(std::move(context)));
    }
}

TEST(TableReadTest, TestMixedFilePredicateProducesScoresThroughPublicReadEntry) {
    const std::string path =
        GetDataDir() + "/orc/multi_partition_append_table.db/multi_partition_append_table";
    ScanContextBuilder scan_builder(path);
    scan_builder.AddOption(Options::SCAN_SNAPSHOT_ID, "1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         TableScan::Create(std::move(scan_context)));
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
    ReadContextBuilder builder(path);
    builder.EnablePreciseFileIndexEvaluate(true)
        .SetReadFieldNames({"_f0_INDEX_SCORE", "f3", "f0"})
        .SetPredicate(predicate)
        .AddOption(Options::READ_BATCH_SIZE, "1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, TableRead::Create(std::move(context)));
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

TEST(TableReadTest, TestRootTopKEmptyInput) {
    const std::string path = GetDataDir() + "/orc/append_09.db/append_09";
    auto search = std::make_shared<VectorSearch>("f3", 10, std::vector<float>{1.0f}, nullptr,
                                                 nullptr, VectorSearch::DistanceType::EUCLIDEAN,
                                                 std::map<std::string, std::string>{});
    ReadContextBuilder builder(path);
    builder.EnablePreciseFileIndexEvaluate(true).SetReadFieldNames({"f1"}).SetVectorSearch(search);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> public_context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                         TableRead::Create(std::move(public_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> empty,
                         read->CreateReader(std::vector<std::shared_ptr<Split>>{}));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, empty->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));
}

TEST(TableReadTest, TestRootTopKRejectsFallbackBranch) {
    const std::string path = GetDataDir() + "/orc/append_09.db/append_09";
    for (bool vector_search : {false, true}) {
        ReadContextBuilder builder(path);
        builder.EnablePreciseFileIndexEvaluate(true).SetReadFieldNames({"f1"}).AddOption(
            Options::SCAN_FALLBACK_BRANCH, "fallback");
        if (vector_search) {
            builder.SetVectorSearch(std::make_shared<VectorSearch>(
                "f3", 10, std::vector<float>{1.0f}, nullptr, nullptr,
                VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{}));
        } else {
            builder.SetFullTextSearch(std::make_shared<FullTextSearch>("f0", "query", 10));
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(context)),
                            "Global TopK does not support fallback branches");
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
        context_builder.EnablePreciseFileIndexEvaluate(true)
            .SetReadFieldNames({"f0"})
            .SetFullTextSearch(search);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_NOK_WITH_MSG(table_read->CreateCountReader({}),
                            "CreateCountReader does not support precise file-index evaluation");
    }
    {
        std::string path = paimon::test::GetDataDir() +
                           "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
        auto search = std::make_shared<VectorSearch>(
            "f3", /*limit=*/1, std::vector<float>{1.0f}, nullptr, nullptr,
            VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
        ReadContextBuilder context_builder(path);
        context_builder.EnablePreciseFileIndexEvaluate(true)
            .SetReadFieldNames({"f0"})
            .SetVectorSearch(search);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_NOK_WITH_MSG(table_read->CreateCountReader({}),
                            "CreateCountReader does not support precise file-index evaluation");
    }
}

TEST(TableReadTest, TestPreciseEvaluationRejectsSystemAndFormatTables) {
    std::string path = GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder system_builder(path + "$snapshots");
    system_builder.EnablePreciseFileIndexEvaluate(true);
    ASSERT_OK_AND_ASSIGN(auto system_context, system_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(system_context)),
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
        ReadContextBuilder builder =
            loaded_table ? ReadContextBuilder(format_table) : ReadContextBuilder(directory->Str());
        builder.EnablePreciseFileIndexEvaluate(true);
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(context)),
                            "Precise file-index evaluation is not supported for format tables");
    }
}

TEST(TableReadTest, TestPreciseScalarEvaluationRejectsUnsupportedTableModes) {
    for (bool primary_key : {false, true}) {
        const std::string path =
            GetDataDir() +
            (primary_key ? "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality"
                         : "/orc/append_09.db/append_09");
        ReadContextBuilder builder(path);
        builder.EnablePreciseFileIndexEvaluate(true).SetReadFieldNames({"f3"}).SetPredicate(
            PredicateBuilder::GreaterThan(0, "f3", FieldType::DOUBLE, Literal(0.0)));
        if (primary_key) {
            builder.AddOption(Options::DELETION_VECTORS_ENABLED, "false");
        } else {
            builder.AddOption(Options::DATA_EVOLUTION_ENABLED, "true");
        }
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        const std::string expected_error =
            primary_key ? "Precise file-index evaluation on primary-key tables requires "
                          "deletion vectors"
                        : "Precise file-index evaluation does not support data-evolution";
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(context)), expected_error);
    }
}

TEST(TableReadTest, TestPrimaryKeyFileIndexSearchDoesNotFallBackToMergeReader) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
    auto search = std::make_shared<VectorSearch>(
        "f3", /*limit=*/1, std::vector<float>{1.0f}, nullptr, nullptr,
        VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
    ReadContextBuilder context_builder(path);
    context_builder.EnablePreciseFileIndexEvaluate(true).SetReadFieldNames({"f0"}).SetVectorSearch(
        search);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> reader, TableRead::Create(std::move(context)));

    BinaryRow partition = BinaryRowGenerator::GenerateRow({10}, GetDefaultPool().get());
    DataSplitImpl::Builder builder(partition, /*bucket=*/0, /*bucket_path=*/"", /*data_files=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> non_raw,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(false).Build());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> non_raw_reader,
                         reader->CreateReader(non_raw));
    ASSERT_NOK_WITH_MSG(non_raw_reader->NextBatch(), "merge file reads");
    non_raw_reader->Close();

    DataSplitImpl::Builder raw_builder(partition, /*bucket=*/0, /*bucket_path=*/"",
                                       /*data_files=*/{});
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataSplitImpl> raw,
        raw_builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());
    ASSERT_OK(reader->CreateReader(raw));
    std::vector<std::shared_ptr<Split>> splits = {raw, non_raw};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> lazy_reader, reader->CreateReader(splits));
    ASSERT_NOK_WITH_MSG(lazy_reader->NextBatch(), "merge file reads");
    lazy_reader->Close();

    auto* key_value_read = dynamic_cast<KeyValueTableRead*>(reader.get());
    ASSERT_NE(key_value_read, nullptr);
    for (bool keep_delete : {false, true}) {
        SCOPED_TRACE(keep_delete);
        key_value_read->ForceKeepDelete(keep_delete);
        ASSERT_OK_AND_ASSIGN(lazy_reader,
                             reader->CreateReader(std::vector<std::shared_ptr<Split>>{raw}));
        key_value_read->ForceKeepDelete(!keep_delete);
        if (keep_delete) {
            ASSERT_NOK_WITH_MSG(lazy_reader->NextBatch(), "merge file reads");
        } else {
            ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, lazy_reader->NextBatch());
            ASSERT_TRUE(BatchReader::IsEofBatch(batch));
        }
    }
}

TEST(TableReadTest, TestLazySplitsBoundResourcesAndOutliveCreator) {
    constexpr int32_t kWindow = 2;
    constexpr size_t kFilesPerSplit = 7;
    enum class ReadMode { Append, PrimaryKeyRaw, PrimaryKeyMerge };
    for (ReadMode mode : {ReadMode::Append, ReadMode::PrimaryKeyRaw, ReadMode::PrimaryKeyMerge}) {
        SCOPED_TRACE(static_cast<int32_t>(mode));
        const bool primary_key = mode != ReadMode::Append;
        const bool raw_convertible = mode != ReadMode::PrimaryKeyMerge;
        const std::string path =
            GetDataDir() +
            (mode == ReadMode::PrimaryKeyMerge ? "/orc/pk_table_with_mor.db/pk_table_with_mor"
             : primary_key ? "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality"
                           : "/orc/multi_partition_append_table.db/multi_partition_append_table");
        ScanContextBuilder scan_builder(path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                             TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_FALSE(plan->Splits().empty());
        std::vector<std::shared_ptr<Split>> splits;
        size_t total_files = 0;
        size_t max_files_per_split = 0;
        size_t first_split_files = 0;
        for (size_t i = 0; i < 3; ++i) {
            auto split =
                std::dynamic_pointer_cast<DataSplitImpl>(plan->Splits()[i % plan->Splits().size()]);
            ASSERT_NE(split, nullptr);
            ASSERT_FALSE(split->DataFiles().empty());
            if (mode == ReadMode::PrimaryKeyRaw) {
                ASSERT_EQ(split->DataFiles()[0]->delete_row_count, std::optional<int64_t>(0));
            }
            std::vector<std::shared_ptr<DataFileMeta>> files = split->DataFiles();
            if (raw_convertible) {
                files.assign(kFilesPerSplit, split->DataFiles()[0]);
            }
            total_files += files.size();
            max_files_per_split = std::max(max_files_per_split, files.size());
            if (i == 0) {
                first_split_files = files.size();
            }
            DataSplitImpl::Builder builder(split->Partition(), split->Bucket(), split->BucketPath(),
                                           std::move(files));
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> input_split,
                                 builder.WithSnapshot(1).RawConvertible(raw_convertible).Build());
            splits.push_back(std::move(input_split));
        }
        ASSERT_GT(max_files_per_split, kWindow);
        const std::map<std::string, std::string> options = {
            {Options::READ_PREPARATION_WINDOW, std::to_string(kWindow)},
            {Options::FILE_INDEX_READ_ENABLED, "false"}};
        arrow::ArrayVector expected_chunks;
        {
            ReadContextBuilder builder(path);
            builder.SetOptions(options).EnablePrefetch(false);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                                 TableRead::Create(std::move(context)));
            for (const auto& split : splits) {
                ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                                     read->CreateReader(split));
                ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                                     ReadResultCollector::CollectResult(std::move(reader)));
                ASSERT_NE(result, nullptr);
                expected_chunks.insert(expected_chunks.end(), result->chunks().begin(),
                                       result->chunks().end());
            }
        }
        auto expected = std::make_shared<arrow::ChunkedArray>(expected_chunks);
        auto check_reader = [&](std::vector<std::shared_ptr<Split>> input_splits,
                                int32_t batches_before_close) {
            SCOPED_TRACE(batches_before_close);
            auto fs = std::make_shared<CountingReaderFileSystem>(".orc");
            const auto counts = fs->counts;
            std::weak_ptr<FileSystem> weak_fs = fs;
            std::unique_ptr<BatchReader> reader;
            {
                ReadContextBuilder builder(path);
                builder.SetOptions(options).EnablePrefetch(false).WithFileSystem(fs);
                ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
                ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                                     TableRead::Create(std::move(context)));
                ASSERT_OK_AND_ASSIGN(reader, read->CreateReader(input_splits));
            }
            input_splits.clear();
            fs.reset();
            ASSERT_EQ(counts->opens.load(), 0);
            ASSERT_FALSE(weak_fs.expired());
            if (batches_before_close < 0) {
                ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                                     ReadResultCollector::CollectResult(reader.get()));
                ASSERT_NE(actual, nullptr);
                ASSERT_TRUE(actual->Equals(expected)) << actual->ToString();
                ASSERT_EQ(counts->opens.load(), total_files);
                ASSERT_TRUE(weak_fs.expired());
            } else if (batches_before_close == 1) {
                ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
                ASSERT_FALSE(BatchReader::IsEofBatch(batch));
                if (raw_convertible) {
                    ASSERT_EQ(counts->opens.load(), kWindow);
                } else {
                    ASSERT_LE(counts->opens.load(), first_split_files);
                }
                reader->Close();
                ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array,
                                     ReadResultCollector::GetArray(std::move(batch)));
                ASSERT_GT(array->length(), 0);
            }
            const uint64_t opened = counts->opens.load();
            reader->Close();
            reader->Close();
            ASSERT_EQ(counts->opens.load(), opened);
            ASSERT_EQ(counts->live.load(), 0);
            ASSERT_LE(counts->peak.load(), raw_convertible ? kWindow : max_files_per_split);
            ASSERT_TRUE(weak_fs.expired());
            ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap eof,
                                 reader->NextBatchWithBitmap());
            ASSERT_TRUE(BatchReader::IsEofBatch(eof));
        };
        check_reader(splits, 0);
        check_reader(splits, 1);
        check_reader(std::move(splits), -1);
    }
}

TEST(TableReadTest, TestLazySplitFailureLeavesLaterSplitsUnopened) {
    enum class ReadMode { Append, PrimaryKeyRaw, PrimaryKeyMerge };
    for (ReadMode mode : {ReadMode::Append, ReadMode::PrimaryKeyRaw, ReadMode::PrimaryKeyMerge}) {
        SCOPED_TRACE(static_cast<int32_t>(mode));
        const bool raw_convertible = mode != ReadMode::PrimaryKeyMerge;
        const std::string path =
            GetDataDir() +
            (mode == ReadMode::Append
                 ? "/orc/multi_partition_append_table.db/multi_partition_append_table"
                 : "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality");
        ScanContextBuilder scan_builder(path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                             TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_FALSE(plan->Splits().empty());
        auto split = std::dynamic_pointer_cast<DataSplitImpl>(plan->Splits()[0]);
        ASSERT_NE(split, nullptr);
        ASSERT_FALSE(split->DataFiles().empty());
        if (mode == ReadMode::PrimaryKeyRaw) {
            ASSERT_EQ(split->DataFiles()[0]->delete_row_count, std::optional<int64_t>(0));
        }
        auto missing = std::make_shared<DataFileMeta>(*split->DataFiles()[0]);
        missing->file_name = "missing.orc";
        DataSplitImpl::Builder valid_builder(split->Partition(), split->Bucket(),
                                             split->BucketPath(), {split->DataFiles()[0]});
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> valid,
                             valid_builder.WithSnapshot(1).RawConvertible(raw_convertible).Build());
        DataSplitImpl::Builder missing_builder(split->Partition(), split->Bucket(),
                                               split->BucketPath(), {missing});
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<DataSplitImpl> invalid,
            missing_builder.WithSnapshot(1).RawConvertible(raw_convertible).Build());
        auto fs = std::make_shared<CountingReaderFileSystem>(".orc");
        ReadContextBuilder builder(path);
        builder.WithFileSystem(fs).EnablePrefetch(false).AddOption(Options::FILE_INDEX_READ_ENABLED,
                                                                   "false");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                             TableRead::Create(std::move(context)));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<BatchReader> reader,
            read->CreateReader(std::vector<std::shared_ptr<Split>>{valid, invalid, valid}));
        ASSERT_EQ(fs->counts->opens.load(), 0);
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        ReaderUtils::ReleaseReadBatch(std::move(batch));
        ASSERT_NOK_WITH_MSG(ReadResultCollector::CollectResult(reader.get()), "missing.orc");
        ASSERT_NOK_WITH_MSG(reader->NextBatchWithBitmap(), "cannot read split 1");
        reader->Close();
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), "missing.orc");
        ASSERT_EQ(fs->counts->opens.load(), 2);
        ASSERT_EQ(fs->counts->live.load(), 0);
    }
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
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"f0", "f1", "f2", "f3"});
    context_builder.AddOption("read.batch-size", "2");
    context_builder.AddOption("orc.read.enable-lazy-decoding", "true");
    context_builder.AddOption("bucket", "10");
    context_builder.AddOption("bucket-key", "f3");
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto append_only_table_read = dynamic_cast<AppendOnlyTableRead*>(table_read.get());
    ASSERT_TRUE(append_only_table_read);
    auto* typed_split_read =
        dynamic_cast<AbstractSplitRead*>(append_only_table_read->split_reads_[0].get());
    ASSERT_TRUE(typed_split_read);
    const auto& core_options = typed_split_read->options_;
    std::map<std::string, std::string> expected_options = {
        {"read.batch-size", "2"},   {"orc.read.enable-lazy-decoding", "true"},
        {"bucket", "10"},           {"bucket-key", "f3"},
        {"manifest.format", "orc"}, {"file.format", "orc"}};
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
        ASSERT_EQ(internal_context->GetTableSchema()->Options(), table_options);
    }
}

}  // namespace paimon::test
