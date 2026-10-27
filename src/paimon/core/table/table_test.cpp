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

#include "paimon/catalog/table.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/cache/cache.h"
#include "paimon/commit_context.h"
#include "paimon/commit_message.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/catalog/file_system_catalog.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/file_store_table_factory.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/fallback_data_split.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/schema/schema.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/mock/mock_catalog.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/counting_cache_test_utils.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

namespace {

// Creates a managed table directory and a `Table` over it, holding the schema written to
// `branch`'s directory under `identifier` with `options`.
Result<std::shared_ptr<Table>> CreateManagedTable(
    const std::shared_ptr<FileSystem>& file_system, const std::string& path,
    const Identifier& identifier, const std::string& branch = "main",
    const std::map<std::string, std::string>& options = {}) {
    SchemaManager schema_manager(file_system, path, branch);
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false)});
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> created,
                           schema_manager.CreateTable(schema, /*partition_keys=*/{},
                                                      /*primary_keys=*/{}, options));
    return FileStoreTable::Create(
        file_system, path,
        checked_pointer_cast<Schema>(std::shared_ptr<TableSchema>(std::move(created))), identifier,
        /*uuid=*/"", branch, /*schema_manager=*/nullptr, /*snapshot_manager=*/nullptr,
        /*fs_scheme_map=*/{}, /*dynamic_options=*/{});
}

/// Writes two rows through the table's own write and commit, so a scan below has data to find.
void WriteAndCommitTwoRows(const std::shared_ptr<Table>& table, const std::string& root_path) {
    WriteContextBuilder write_builder(root_path, "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         table->NewWrite(std::move(write_context)));
    arrow::Int32Builder ids_builder;
    ASSERT_TRUE(ids_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> ids_array;
    ASSERT_TRUE(ids_builder.Finish(&ids_array).ok());
    auto struct_array = arrow::StructArray::Make({ids_array}, {arrow::field("id", arrow::int32(),
                                                                            /*nullable=*/false)})
                            .ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*struct_array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.SetBucket(0).Finish());
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    CommitContextBuilder commit_builder(root_path, "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         table->NewCommit(std::move(commit_context)));
    ASSERT_OK(commit->Commit(messages));
    ASSERT_OK(write->Close());
}

/// Writes one `id`/`v` row through the table's own write and commit, so a scan below has data
/// to find.
void WriteAndCommitRow(const std::shared_ptr<Table>& table, const std::string& root_path,
                       int32_t id, int32_t v) {
    WriteContextBuilder write_builder(root_path, "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         table->NewWrite(std::move(write_context)));
    arrow::Int32Builder ids_builder;
    arrow::Int32Builder vs_builder;
    ASSERT_TRUE(ids_builder.AppendValues({id}).ok());
    ASSERT_TRUE(vs_builder.AppendValues({v}).ok());
    std::shared_ptr<arrow::Array> ids_array;
    std::shared_ptr<arrow::Array> vs_array;
    ASSERT_TRUE(ids_builder.Finish(&ids_array).ok());
    ASSERT_TRUE(vs_builder.Finish(&vs_array).ok());
    auto struct_array =
        arrow::StructArray::Make({ids_array, vs_array},
                                 {arrow::field("id", arrow::int32(), /*nullable=*/false),
                                  arrow::field("v", arrow::int32())})
            .ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*struct_array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.SetBucket(0).Finish());
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    CommitContextBuilder commit_builder(root_path, "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         table->NewCommit(std::move(commit_context)));
    ASSERT_OK(commit->Commit(messages));
    ASSERT_OK(write->Close());
}

}  // namespace

TEST(TableTest, TestTableCarriesManagersOfItsBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    const std::string path = dir->Str() + "/tbl";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        CreateManagedTable(fs, path, Identifier(Identifier::kUnknownDatabase, "tbl"),
                           /*branch=*/"dev"));
    auto file_store_table = std::dynamic_pointer_cast<FileStoreTable>(table);
    ASSERT_TRUE(file_store_table != nullptr);
    // The branch the table was built for is the one its managers point at.
    EXPECT_EQ(file_store_table->CurrentBranch(), "dev");
    ASSERT_TRUE(file_store_table->GetSchemaManager() != nullptr);
    ASSERT_TRUE(file_store_table->GetSnapshotManager() != nullptr);
    EXPECT_EQ(file_store_table->GetSnapshotManager()->Branch(), "dev");
    EXPECT_EQ(file_store_table->GetSnapshotManager()->RootPath(), path);
    EXPECT_EQ(file_store_table->GetSnapshotManager()->Fs(), fs);
    // A copy serves the same branch through the same managers, whose cache it shares.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> copy, table->Copy({}));
    auto file_store_copy = std::dynamic_pointer_cast<FileStoreTable>(copy);
    ASSERT_TRUE(file_store_copy != nullptr);
    EXPECT_EQ(file_store_copy->CurrentBranch(), "dev");
    EXPECT_EQ(file_store_copy->GetSchemaManager(), file_store_table->GetSchemaManager());
    EXPECT_EQ(file_store_copy->GetSnapshotManager(), file_store_table->GetSnapshotManager());
}

TEST(TableTest, TestCopyKeepsTheBranchTheTableServes) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier, /*branch=*/"main",
                           {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));

    // A copy handed a `branch` option is the table it was copied from: the branch is the
    // table's own - `SwitchToBranch` is the way to change it - so the option moves neither the
    // copy nor its options, and an operator asked for the branch the copy serves - `main` - is
    // served rather than refused by options claiming a `dev` the copy does not serve.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> copy, table->Copy({{Options::BRANCH, "dev"}}));
    auto file_store_copy = std::dynamic_pointer_cast<FileStoreTable>(copy);
    ASSERT_TRUE(file_store_copy != nullptr);
    EXPECT_EQ(file_store_copy->CurrentBranch(), "main");
    {
        ScanContextBuilder scan_builder(dir->Str());
        scan_builder.AddOption(Options::BRANCH, "main");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx, scan_builder.Finish());
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                             file_store_copy->NewScan(std::move(ctx)));
    }

    // The same the other way round: a copy of a branch table serves that branch still, so
    // naming the branch it serves - `dev` - is agreed with rather than refused.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> branch_table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str() + "/branch_tbl",
                                            identifier, /*branch=*/"dev",
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> branch_copy,
                         branch_table->Copy({{Options::BRANCH, "main"}}));
    auto branch_file_store_copy = std::dynamic_pointer_cast<FileStoreTable>(branch_copy);
    ASSERT_TRUE(branch_file_store_copy != nullptr);
    EXPECT_EQ(branch_file_store_copy->CurrentBranch(), "dev");
    {
        ScanContextBuilder scan_builder(dir->Str() + "/branch_tbl");
        scan_builder.AddOption(Options::BRANCH, "dev");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx, scan_builder.Finish());
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                             branch_file_store_copy->NewScan(std::move(ctx)));
    }
}

TEST(TableTest, TestACopyHandsItsDynamicOptionsToTheFallbackReader) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    const std::string path = dir->Str() + "/tbl";
    const Identifier identifier("db", "tbl");

    // A primary-key table whose reads fall back to the `fb` branch, and that branch's schema.
    SchemaManager schema_manager(fs, path, "main");
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false)});
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> created,
        schema_manager.CreateTable(schema, /*partition_keys=*/{}, /*primary_keys=*/{"id"},
                                   {{Options::BUCKET, "1"},
                                    {Options::BUCKET_KEY, "id"},
                                    {Options::SCAN_FALLBACK_BRANCH, "fb"}}));
    SchemaManager fallback_schema_manager(fs, path, "fb");
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] std::unique_ptr<TableSchema> fallback_schema,
        fallback_schema_manager.CreateTable(schema, /*partition_keys=*/{},
                                            /*primary_keys=*/{"id"},
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        FileStoreTable::Create(
            fs, path,
            checked_pointer_cast<Schema>(std::shared_ptr<TableSchema>(std::move(created))),
            identifier, /*uuid=*/"", /*branch=*/"main", /*schema_manager=*/nullptr,
            /*snapshot_manager=*/nullptr, /*fs_scheme_map=*/{}, /*dynamic_options=*/{}));

    // The copy turns the sequence-number column on, and a read projects it through a context
    // that carries no options of its own. The main reader serves the copy's merged options, so
    // it knows the column; the fallback serves the `fb` branch's schema, whose options do not
    // name it, so the copy's own dynamic options are what let the fallback reader know it too -
    // without them the read as a whole fails to build.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> copy,
                         table->Copy({{Options::KEY_VALUE_SEQUENCE_NUMBER_ENABLED, "true"}}));
    ReadContextBuilder read_builder(path);
    read_builder.SetReadFieldNames({"id", "_SEQUENCE_NUMBER"});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         copy->NewRead(std::move(read_context)));
}

TEST(TableTest, TestACopyOfACopyServesTheLatestValueOfARepeatedDynamicOption) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    const std::string path = dir->Str() + "/tbl";
    const Identifier identifier("db", "tbl");

    // A primary-key table whose reads fall back to the `fb` branch, and that branch's schema.
    SchemaManager schema_manager(fs, path, "main");
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false)});
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> created,
        schema_manager.CreateTable(schema, /*partition_keys=*/{}, /*primary_keys=*/{"id"},
                                   {{Options::BUCKET, "1"},
                                    {Options::BUCKET_KEY, "id"},
                                    {Options::SCAN_FALLBACK_BRANCH, "fb"}}));
    SchemaManager fallback_schema_manager(fs, path, "fb");
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] std::unique_ptr<TableSchema> fallback_schema,
        fallback_schema_manager.CreateTable(schema, /*partition_keys=*/{},
                                            /*primary_keys=*/{"id"},
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        FileStoreTable::Create(
            fs, path,
            checked_pointer_cast<Schema>(std::shared_ptr<TableSchema>(std::move(created))),
            identifier, /*uuid=*/"", /*branch=*/"main", /*schema_manager=*/nullptr,
            /*snapshot_manager=*/nullptr, /*fs_scheme_map=*/{}, /*dynamic_options=*/{}));

    // The option is turned off and then on again by the copy the read serves, so the copy's
    // schema carries the latest value. The fallback branch's schema does not carry the option
    // at all, so what lets its reader know the column is the copy's accumulated dynamic
    // options - which must speak the same latest value the schema does, not the older one a
    // first copy left, or the fallback reader fails to build while the main one succeeds.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> off,
                         table->Copy({{Options::KEY_VALUE_SEQUENCE_NUMBER_ENABLED, "false"}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> copy,
                         off->Copy({{Options::KEY_VALUE_SEQUENCE_NUMBER_ENABLED, "true"}}));
    ReadContextBuilder read_builder(path);
    read_builder.SetReadFieldNames({"id", "_SEQUENCE_NUMBER"});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         copy->NewRead(std::move(read_context)));
}

TEST(TableTest, TestAFallbackReaderServesItsBranchsOwnSchema) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    const std::string path = dir->Str() + "/tbl";
    const Identifier identifier("db", "tbl");

    // The main branch is an append-only table whose reads fall back to the `fb` branch, and
    // that branch is laid out as a primary-key table: a second write of the key the first
    // wrote is a merge, not a duplicate.
    SchemaManager main_schema_manager(fs, path, "main");
    auto row_schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                                     arrow::field("v", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> main_created,
                         main_schema_manager.CreateTable(row_schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{},
                                                         {{Options::BUCKET, "1"},
                                                          {Options::BUCKET_KEY, "id"},
                                                          {Options::SCAN_FALLBACK_BRANCH, "fb"}}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> main,
        FileStoreTable::Create(
            fs, path,
            checked_pointer_cast<Schema>(std::shared_ptr<TableSchema>(std::move(main_created))),
            identifier, /*uuid=*/"", /*branch=*/"main", /*schema_manager=*/nullptr,
            /*snapshot_manager=*/nullptr, /*fs_scheme_map=*/{}, /*dynamic_options=*/{}));

    SchemaManager fallback_schema_manager(fs, path, "fb");
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> fallback_created,
        fallback_schema_manager.CreateTable(row_schema, /*partition_keys=*/{},
                                            /*primary_keys=*/{"id"},
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> fallback,
        FileStoreTable::Create(
            fs, path,
            checked_pointer_cast<Schema>(std::shared_ptr<TableSchema>(std::move(fallback_created))),
            identifier, /*uuid=*/"", /*branch=*/"fb", /*schema_manager=*/nullptr,
            /*snapshot_manager=*/nullptr, /*fs_scheme_map=*/{}, /*dynamic_options=*/{}));
    WriteAndCommitRow(fallback, path, /*id=*/1, /*v=*/10);
    WriteAndCommitRow(fallback, path, /*id=*/1, /*v=*/20);

    // The read of the main table carries its fallback reader, which reads the `fb` branch's
    // data through the `fb` branch's own schema - so it is the primary-key read that schema's
    // layout asks for, not the append-only one the main table's kind would pick. Its splits
    // are the `fb` branch's own, scanned through a table that serves `fb` and read as
    // fallback ones, which is how a fallback reader is asked for its rows.
    ReadContextBuilder read_builder(path);
    read_builder.SetReadFieldNames({"id", "v"});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, main->NewRead(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         ScanContextBuilder(path).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> fallback_scan,
                         fallback->NewScan(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> fallback_plan, fallback_scan->CreatePlan());
    ASSERT_FALSE(fallback_plan->Splits().empty());
    std::vector<std::shared_ptr<Split>> fallback_splits;
    for (const std::shared_ptr<Split>& split : fallback_plan->Splits()) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        ASSERT_TRUE(data_split != nullptr);
        fallback_splits.push_back(
            std::make_shared<FallbackDataSplit>(data_split, /*is_fallback=*/true));
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> batch_reader,
                         read->CreateReader(fallback_splits));

    // The one merged row the `fb` branch's primary-key layout leaves behind - at the value
    // the second write carried - is what the fallback reader answers with; the append-only
    // reader the main table's kind would have picked answers with both rows, or refuses the
    // split outright.
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, batch_reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array,
                         ReadResultCollector::GetArray(std::move(batch)));
    ASSERT_TRUE(array);
    ASSERT_EQ(array->length(), 1);
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    ASSERT_TRUE(struct_array != nullptr);
    EXPECT_EQ(struct_array->num_fields(), 3);
    auto value_kind_array = std::dynamic_pointer_cast<arrow::Int8Array>(struct_array->field(0));
    auto id_array = std::dynamic_pointer_cast<arrow::Int32Array>(struct_array->field(1));
    auto v_array = std::dynamic_pointer_cast<arrow::Int32Array>(struct_array->field(2));
    ASSERT_TRUE(value_kind_array != nullptr);
    ASSERT_TRUE(id_array != nullptr);
    ASSERT_TRUE(v_array != nullptr);
    EXPECT_EQ(value_kind_array->Value(0), 0);
    EXPECT_EQ(id_array->Value(0), 1);
    EXPECT_EQ(v_array->Value(0), 20);
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof_batch, batch_reader->NextBatch());
    EXPECT_TRUE(BatchReader::IsEofBatch(eof_batch));
}

TEST(TableTest, TestACopyThatMovesAnS3OptionRebuildsItsManagers) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    const std::string path = dir->Str() + "/tbl";
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        CreateManagedTable(
            fs, path, identifier, /*branch=*/"main",
            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}, {"s3.endpoint", "http://old"}}));
    auto file_store_table = std::dynamic_pointer_cast<FileStoreTable>(table);
    ASSERT_TRUE(file_store_table != nullptr);

    // A native `s3.` option names the file system the S3 backend resolves from, the way an
    // `fs.` one does: a copy that moves it re-resolves, so the managers are rebuilt rather
    // than inherited - otherwise the copy would read its metadata through the old endpoint
    // while its data goes through the new one.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> moved,
                         table->Copy({{"s3.endpoint", "http://new"}}));
    auto moved_file_store_table = std::dynamic_pointer_cast<FileStoreTable>(moved);
    ASSERT_TRUE(moved_file_store_table != nullptr);
    EXPECT_NE(moved_file_store_table->GetSchemaManager(), file_store_table->GetSchemaManager());
    EXPECT_NE(moved_file_store_table->GetSnapshotManager(), file_store_table->GetSnapshotManager());

    // A copy whose `s3.` option says what the schema already says resolves the same file
    // system, so the managers are the ones this table already holds.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> kept, table->Copy({{"s3.endpoint", "http://old"}}));
    auto kept_file_store_table = std::dynamic_pointer_cast<FileStoreTable>(kept);
    ASSERT_TRUE(kept_file_store_table != nullptr);
    EXPECT_EQ(kept_file_store_table->GetSchemaManager(), file_store_table->GetSchemaManager());
    EXPECT_EQ(kept_file_store_table->GetSnapshotManager(), file_store_table->GetSnapshotManager());
}

TEST(TableTest, TestSwitchToBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    const std::string path = dir->Str() + "/tbl";
    const Identifier identifier(Identifier::kUnknownDatabase, "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, CreateManagedTable(fs, path, identifier));
    // A branch of its own, with a schema of its own.
    SchemaManager dev_schema_manager(fs, path, "dev");
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] std::unique_ptr<TableSchema> dev_schema,
        dev_schema_manager.CreateTable(arrow::schema({arrow::field("c0", arrow::int32())}),
                                       /*partition_keys=*/{}, /*primary_keys=*/{}, {}));

    // Switching to a branch serves that branch's schema and managers.
    auto main_file_store_table = std::dynamic_pointer_cast<FileStoreTable>(table);
    ASSERT_TRUE(main_file_store_table != nullptr);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> dev_table,
                         main_file_store_table->SwitchToBranch("dev"));
    auto dev_file_store_table = std::dynamic_pointer_cast<FileStoreTable>(dev_table);
    ASSERT_TRUE(dev_file_store_table != nullptr);
    EXPECT_EQ("dev", dev_file_store_table->CurrentBranch());
    // The identifier names the branch switched to, so `Name()` and `FullName()` answer for the
    // table served, and a caller that reloads through `FullName()` lands on the same branch.
    EXPECT_EQ("tbl$branch_dev", dev_table->Name());
    EXPECT_EQ("tbl$branch_dev", dev_table->FullName());
    ASSERT_EQ((std::vector<std::string>{"c0"}), dev_table->LatestSchema()->FieldNames());
    auto dev_table_schema = std::dynamic_pointer_cast<TableSchema>(dev_table->LatestSchema());
    ASSERT_TRUE(dev_table_schema != nullptr);
    ASSERT_EQ("dev", dev_table_schema->Options().at(Options::BRANCH));
    ASSERT_TRUE(dev_file_store_table->GetSchemaManager() != nullptr);
    ASSERT_TRUE(dev_file_store_table->GetSnapshotManager() != nullptr);
    EXPECT_EQ("dev", dev_file_store_table->GetSnapshotManager()->Branch());

    // Switching to the branch already served hands back the same table, managers shared.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> same_table,
                         dev_file_store_table->SwitchToBranch("dev"));
    auto same_file_store_table = std::dynamic_pointer_cast<FileStoreTable>(same_table);
    ASSERT_TRUE(same_file_store_table != nullptr);
    EXPECT_EQ(same_table->LatestSchema(), dev_table->LatestSchema());
    EXPECT_EQ(same_file_store_table->GetSchemaManager(), dev_file_store_table->GetSchemaManager());
    EXPECT_EQ(same_file_store_table->GetSnapshotManager(),
              dev_file_store_table->GetSnapshotManager());

    // Switching back serves the main branch's schema again.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> main_again,
                         dev_file_store_table->SwitchToBranch("main"));
    auto main_again_file_store_table = std::dynamic_pointer_cast<FileStoreTable>(main_again);
    ASSERT_TRUE(main_again_file_store_table != nullptr);
    EXPECT_EQ("main", main_again_file_store_table->CurrentBranch());
    // Switching back leaves no branch in the name either, the way the main branch is addressed.
    EXPECT_EQ("tbl", main_again->Name());
    EXPECT_EQ("tbl", main_again->FullName());
    ASSERT_EQ((std::vector<std::string>{"id"}), main_again->LatestSchema()->FieldNames());
    auto main_again_schema = std::dynamic_pointer_cast<TableSchema>(main_again->LatestSchema());
    ASSERT_TRUE(main_again_schema != nullptr);
    EXPECT_EQ(0, main_again_schema->Options().count(Options::BRANCH));

    // A branch with no schema does not exist.
    ASSERT_NOK_WITH_MSG(main_file_store_table->SwitchToBranch("missing"), "branch");
}

TEST(TableTest, TestNewWriteServesTheBranchTheTableWasLoadedWith) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(fs, dir->Str(), identifier, /*branch=*/"dev",
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));

    // A context that names no branch is the one a caller hands straight to the table's write,
    // which serves the branch the table was loaded with rather than the context's own main.
    WriteContextBuilder write_builder(dir->Str(), "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         table->NewWrite(std::move(write_context)));
    arrow::Int32Builder ids_builder;
    ASSERT_TRUE(ids_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> ids_array;
    ASSERT_TRUE(ids_builder.Finish(&ids_array).ok());
    auto struct_array = arrow::StructArray::Make({ids_array}, {arrow::field("id", arrow::int32(),
                                                                            /*nullable=*/false)})
                            .ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*struct_array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.SetBucket(0).Finish());
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    ASSERT_OK(write->Close());

    // The files land under the branch's own directory, which a write of the main branch - the
    // branch the context alone would name - would never touch.
    std::vector<BasicFileStatus> branch_entries;
    ASSERT_OK(fs->ListDir(BranchManager::BranchPath(dir->Str(), "dev"), &branch_entries));
    EXPECT_FALSE(branch_entries.empty());
}

TEST(TableTest, TestOperatorsServeTheBranchTheTableWasLoadedWith) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier, /*branch=*/"dev",
                           {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));

    // The write and the commit below carry no branch, and both serve the branch the table was
    // loaded with, so the data they publish lands on it.
    WriteContextBuilder write_builder(dir->Str(), "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         table->NewWrite(std::move(write_context)));
    arrow::Int32Builder ids_builder;
    ASSERT_TRUE(ids_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> ids_array;
    ASSERT_TRUE(ids_builder.Finish(&ids_array).ok());
    auto struct_array = arrow::StructArray::Make({ids_array}, {arrow::field("id", arrow::int32(),
                                                                            /*nullable=*/false)})
                            .ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*struct_array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.SetBucket(0).Finish());
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    ASSERT_OK(write->Close());
    CommitContextBuilder commit_builder(dir->Str(), "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         table->NewCommit(std::move(commit_context)));
    ASSERT_OK(commit->Commit(messages));

    // The scan finds what they wrote, reading through the table's own managers on its branch.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_ctx,
                         ScanContextBuilder(dir->Str()).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan, table->NewScan(std::move(scan_ctx)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    EXPECT_FALSE(plan->Splits().empty());

    // A branch the scan context carries does not win either: the table's own does, so the data
    // it finds is still the branch's.
    ScanContextBuilder main_scan_builder(dir->Str());
    main_scan_builder.AddOption(Options::BRANCH, "main");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> main_scan_ctx, main_scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> main_scan,
                         table->NewScan(std::move(main_scan_ctx)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> main_plan, main_scan->CreatePlan());
    EXPECT_FALSE(main_plan->Splits().empty());
}

TEST(TableTest, TestGetTableWithUnknownDatabase) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    // The database the identifier names lands in its own directory below the warehouse, so the
    // table the catalog reads back is the one that identifier selects.
    const std::string table_dir = dir->Str() + "/unknown.db/tbl1";
    ASSERT_OK(fs->Mkdirs(table_dir));

    SchemaManager schema_manager(fs, table_dir);
    auto schema =
        arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                       arrow::field("name", arrow::utf8()), arrow::field("value", arrow::int64())});
    std::vector<std::string> partition_keys = {"name"};
    std::vector<std::string> primary_keys = {"id"};
    std::map<std::string, std::string> options = {
        {"file.format", "orc"},
        {"commit.force-compact", "true"},
    };

    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> created_schema,
                         schema_manager.CreateTable(schema, partition_keys, primary_keys, options));

    FileSystemCatalog catalog(fs, dir->Str(), /*catalog_options=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         catalog.GetTable(Identifier(Identifier::kUnknownDatabase, "tbl1")));

    EXPECT_EQ(table->Name(), "tbl1");
    EXPECT_EQ(table->FullName(), "tbl1");

    std::shared_ptr<Schema> latest_schema = table->LatestSchema();
    ASSERT_NE(latest_schema, nullptr);
    auto data_schema = std::dynamic_pointer_cast<DataSchema>(latest_schema);
    ASSERT_TRUE(data_schema != nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<TableSchema>(latest_schema) != nullptr);
    EXPECT_EQ(data_schema->Id(), 0);
    EXPECT_EQ(data_schema->PartitionKeys(), partition_keys);
    EXPECT_EQ(data_schema->PrimaryKeys(), primary_keys);
}

TEST(TableTest, TestGetTableWithBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    const std::string table_dir = dir->Str() + "/db1.db/tbl1";
    ASSERT_OK(fs->Mkdirs(table_dir));
    SchemaManager schema_manager(fs, table_dir);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> main_schema,
                         schema_manager.CreateTable(
                             arrow::schema({arrow::field("main_c0", arrow::int32())}), {}, {}, {}));
    SchemaManager branch_schema_manager(fs, table_dir, "b1");
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> branch_schema,
                         branch_schema_manager.CreateTable(
                             arrow::schema({arrow::field("b1_c0", arrow::int32())}), {}, {}, {}));

    FileSystemCatalog catalog(fs, dir->Str(), /*catalog_options=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         catalog.GetTable(Identifier("db1", "tbl1$branch_b1")));
    ASSERT_EQ("tbl1$branch_b1", table->Name());
    ASSERT_EQ((std::vector<std::string>{"b1_c0"}), table->LatestSchema()->FieldNames());
    for (const auto& main_identifier :
         {Identifier("db1", "tbl1$branch_main"), Identifier("db1", "tbl1$branch_MAIN")}) {
        ASSERT_OK_AND_ASSIGN(table, catalog.GetTable(main_identifier));
        ASSERT_EQ((std::vector<std::string>{"main_c0"}), table->LatestSchema()->FieldNames());
    }

    Status missing_branch = catalog.GetTable(Identifier("db1", "tbl1$branch_missing")).status();
    ASSERT_TRUE(missing_branch.IsNotExist()) << missing_branch.ToString();
    ASSERT_NOK_WITH_MSG(catalog.GetTable(Identifier("db1", "tbl1$branch_../outside")), "branch");
}

TEST(TableTest, TestCreateHoldsTheFileSystem) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::shared_ptr<FileSystem> fs = dir->GetFileSystem();

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        CreateManagedTable(fs, dir->Str(), Identifier(Identifier::kUnknownDatabase, "tbl1")));

    EXPECT_EQ(table->GetFileSystem(), fs);
    EXPECT_EQ(table->GetRootPath(), dir->Str());
}

TEST(TableTest, TestMainBranchAliasResolvesToTheMainBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    SchemaManager schema_manager(fs, dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> created,
                         schema_manager.CreateTable(
                             arrow::schema({arrow::field("c0", arrow::int32())}), {}, {}, {}));
    auto schema = checked_pointer_cast<Schema>(std::shared_ptr<TableSchema>(std::move(created)));

    for (const auto& main_identifier :
         {Identifier("db", "tbl"), Identifier("db", "tbl$branch_main"),
          Identifier("db", "tbl$branch_MAIN")}) {
        SCOPED_TRACE(main_identifier.ToString());
        // A catalog that loaded the schema itself builds the table through the factory, which a
        // rest catalog is shaped like. The alias resolves to the main branch as well.
        ASSERT_OK_AND_ASSIGN(std::string branch, main_identifier.GetBranchNameOrDefault());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreTable> table,
                             FileStoreTable::Create(fs, dir->Str(), schema, main_identifier,
                                                    /*uuid=*/"", branch,
                                                    /*schema_manager=*/nullptr,
                                                    /*snapshot_manager=*/nullptr,
                                                    /*fs_scheme_map=*/{},
                                                    /*dynamic_options=*/{}));
        EXPECT_EQ("main", table->CurrentBranch());
    }
}

TEST(TableTest, TestCurrentBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    // A table whose identifier names no branch is a table of the main branch.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> main_table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));
    auto main_file_store_table = std::dynamic_pointer_cast<FileStoreTable>(main_table);
    ASSERT_TRUE(main_file_store_table != nullptr);
    EXPECT_EQ(main_file_store_table->CurrentBranch(), "main");

    // The branch a table was created on is the one it carries.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> branch_table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str() + "/branch_tbl",
                                            Identifier("db", "tbl", "dev"), /*branch=*/"dev"));
    auto branch_file_store_table = std::dynamic_pointer_cast<FileStoreTable>(branch_table);
    ASSERT_TRUE(branch_file_store_table != nullptr);
    EXPECT_EQ(branch_file_store_table->CurrentBranch(), "dev");
}

TEST(TableTest, TestCatalogOpenedTableServesThePerTableFileSystem) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableSchema(table->LatestSchema());

    // A catalog that hands out no per-table file system opens the table through the one its
    // schema's options resolve - never through none, the default local one when they name no
    // other - and one that does opens it with the file system issued for that table.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> opened, catalog->GetTable(identifier));
    EXPECT_NE(opened->GetFileSystem(), nullptr);

    catalog->SetTableFileSystem(dir->GetFileSystem());
    ASSERT_OK_AND_ASSIGN(opened, catalog->GetTable(identifier));
    EXPECT_EQ(opened->GetFileSystem(), dir->GetFileSystem());
    // This mock serves no table location, so the table it opens carries none.
    EXPECT_EQ(opened->GetRootPath(), "");
}

TEST(TableTest, TestGetTableServesEachKindOfTableAsWhatItIs) {
    // The table's `type` option says what it is, so the one call that opens a table hands each
    // kind back as what it is, through the `Table` interface the two kinds share.
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                                 arrow::field("dt", arrow::utf8())});

    FileSystemCatalog catalog(fs, dir->Str(), /*catalog_options=*/{});
    const std::string format_dir = dir->Str() + "/db.db/format";
    ASSERT_OK(fs->Mkdirs(format_dir));
    SchemaManager format_schema_manager(fs, format_dir);
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] std::unique_ptr<TableSchema> format_schema,
        format_schema_manager.CreateTable(schema, /*partition_keys=*/{"dt"},
                                          /*primary_keys=*/{},
                                          {{"type", "format-table"}, {"file.format", "parquet"}}));
    // A format table comes back as the `FormatTable` it is.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> opened,
                         catalog.GetTable(Identifier("db", "format")));
    ASSERT_NE(nullptr, std::dynamic_pointer_cast<FormatTable>(opened));

    // A managed table with no `type` option at all is what a `Table` is for, and still opens.
    const std::string managed_dir = dir->Str() + "/db.db/managed";
    ASSERT_OK(fs->Mkdirs(managed_dir));
    SchemaManager managed_schema_manager(fs, managed_dir);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> managed_schema,
                         managed_schema_manager.CreateTable(schema, /*partition_keys=*/{"dt"},
                                                            /*primary_keys=*/{}, {}));
    ASSERT_OK(catalog.GetTable(Identifier("db", "managed")));
}

TEST(TableTest, TestGetTableFailedWithNonExistSchema) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    const std::string table_dir = dir->Str() + "/unknown.db/tbl1";
    ASSERT_OK(fs->Mkdirs(table_dir));
    SchemaManager schema_manager(fs, table_dir);
    auto schema =
        arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                       arrow::field("name", arrow::utf8()), arrow::field("value", arrow::int64())});
    std::vector<std::string> partition_keys = {"name"};
    std::vector<std::string> primary_keys = {"id"};
    std::map<std::string, std::string> options = {
        {"file.format", "orc"},
        {"commit.force-compact", "true"},
    };

    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> created_schema,
                         schema_manager.CreateTable(schema, partition_keys, primary_keys, options));

    // remove schema
    std::string schema_path = schema_manager.ToSchemaPath(0);
    ASSERT_OK(fs->Delete(schema_path, /*recursive=*/false));
    // check get table failed
    FileSystemCatalog catalog(fs, dir->Str(), /*catalog_options=*/{});
    ASSERT_NOK_WITH_MSG(catalog.GetTable(Identifier(Identifier::kUnknownDatabase, "tbl1")),
                        "not exist");
}

TEST(TableTest, TestNewScanCreatesATableScan) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetFileSystem(dir->GetFileSystem());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx,
                         ScanContextBuilder(dir->Str()).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                         table->NewScan(std::move(ctx)));
}

TEST(TableTest, TestNewScanServesTheSchemaTheTableWasLoadedWith) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(fs, dir->Str(), identifier));

    // With the schema file gone the table still serves a scan: it carries the schema it was
    // loaded with, which is what a scan is built from.
    SchemaManager schema_manager(fs, dir->Str());
    ASSERT_OK(fs->Delete(schema_manager.ToSchemaPath(0), /*recursive=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> held_ctx,
                         ScanContextBuilder(dir->Str()).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                         table->NewScan(std::move(held_ctx)));

    // A context without a table still reads the schema from the directory, which no longer has
    // one.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx, ScanContextBuilder(dir->Str()).Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(ctx)), "not found latest schema");
}

TEST(TableTest, TestNewScanServesTheFileSystemTheTableWasOpenedWith) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(fs, dir->Str(), identifier, /*branch=*/"main",
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));

    // A file system is only observable once a scan has data to read, so two rows are written and
    // committed through the table's own write and commit.
    WriteContextBuilder write_builder(dir->Str(), "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         table->NewWrite(std::move(write_context)));
    arrow::Int32Builder ids_builder;
    ASSERT_TRUE(ids_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> ids_array;
    ASSERT_TRUE(ids_builder.Finish(&ids_array).ok());
    auto struct_array = arrow::StructArray::Make({ids_array}, {arrow::field("id", arrow::int32(),
                                                                            /*nullable=*/false)})
                            .ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*struct_array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.SetBucket(0).Finish());
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    CommitContextBuilder commit_builder(dir->Str(), "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         table->NewCommit(std::move(commit_context)));
    ASSERT_OK(commit->Commit(messages));
    ASSERT_OK(write->Close());

    // The table was opened with the local file system, so a scan serves its data through that
    // one even when the context hands over a mock that can read nothing.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ScanContext> held_ctx,
        ScanContextBuilder(dir->Str()).WithFileSystem(std::make_shared<MockFileSystem>()).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan, table->NewScan(std::move(held_ctx)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_FALSE(plan->Splits().empty());

    // A context without a table resolves the file system itself, which is the mock and reads no
    // schema at all: the file system counterpart of the schema case above.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ScanContext> ctx,
        ScanContextBuilder(dir->Str()).WithFileSystem(std::make_shared<MockFileSystem>()).Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(ctx)), "not found latest schema");
}

TEST(TableTest, TestNewScanServesTheRootPathTheTableWasOpenedWith) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(fs, dir->Str(), identifier, /*branch=*/"main",
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));

    // Two rows are written and committed through the table's own write and commit, so the scan
    // below has data to find.
    WriteContextBuilder write_builder(dir->Str(), "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         table->NewWrite(std::move(write_context)));
    arrow::Int32Builder ids_builder;
    ASSERT_TRUE(ids_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> ids_array;
    ASSERT_TRUE(ids_builder.Finish(&ids_array).ok());
    auto struct_array = arrow::StructArray::Make({ids_array}, {arrow::field("id", arrow::int32(),
                                                                            /*nullable=*/false)})
                            .ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*struct_array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.SetBucket(0).Finish());
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    CommitContextBuilder commit_builder(dir->Str(), "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         table->NewCommit(std::move(commit_context)));
    ASSERT_OK(commit->Commit(messages));
    ASSERT_OK(write->Close());

    // The table was opened with this directory as its root path, so a scan serves its data from
    // there even when the context names a different one.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> held_ctx,
                         ScanContextBuilder(dir->Str() + "/not-the-table").Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan, table->NewScan(std::move(held_ctx)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_FALSE(plan->Splits().empty());

    // A context without a table reads the schema from the directory it names, which is not a
    // table at all: the root path counterpart of the schema case above.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx,
                         ScanContextBuilder(dir->Str() + "/not-the-table").Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(ctx)), "not found latest schema");
}

TEST(TableTest, TestNewReadCreatesATableRead) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetFileSystem(dir->GetFileSystem());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> ctx,
                         ReadContextBuilder(dir->Str()).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         table->NewRead(std::move(ctx)));
}

TEST(TableTest, TestNewWriteCreatesAFileStoreWrite) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetFileSystem(dir->GetFileSystem());
    // A catalog write reads the current schema from the catalog on the main branch.
    catalog->SetTableSchema(table->LatestSchema());

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<WriteContext> ctx,
        WriteContextBuilder(dir->Str(), "commit-user").WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<FileStoreWrite> write,
                         table->NewWrite(std::move(ctx)));
}

TEST(TableTest, TestNewCommitCreatesAFileStoreCommit) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetFileSystem(dir->GetFileSystem());
    // Like a write, a catalog commit reads the current schema from the catalog.
    catalog->SetTableSchema(table->LatestSchema());

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<CommitContext> ctx,
        CommitContextBuilder(dir->Str(), "commit-user").WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<FileStoreCommit> commit,
                         table->NewCommit(std::move(ctx)));
}

TEST(TableTest, TestNewScanReadWriteCommitRejectANullContext) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));

    // The operators report the null context themselves; the table stands in without swallowing
    // the error, so behavior matches `TableScan::Create` and friends.
    ASSERT_NOK_WITH_MSG(table->NewScan(nullptr), "scan context is null pointer");
    ASSERT_NOK_WITH_MSG(table->NewRead(nullptr), "read context is null pointer");
    ASSERT_NOK_WITH_MSG(table->NewWrite(nullptr), "write context is null pointer");
    ASSERT_NOK_WITH_MSG(table->NewCommit(nullptr), "commit context is null pointer");
}

TEST(TableTest, TestOperatorsRefuseAnOptionThatContradictsTheTable) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier, /*branch=*/"main",
                           {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));

    // The table was loaded with `bucket` = 1, so every operator refuses a context that names
    // another value for it: the table serves the options it was loaded with, and a caller that
    // means to change them copies the table.
    {
        ScanContextBuilder scan_builder(dir->Str());
        scan_builder.AddOption(Options::BUCKET, "2");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx, scan_builder.Finish());
        ASSERT_NOK_WITH_MSG(table->NewScan(std::move(ctx)),
                            "option 'bucket' is '2' in the context but '1' on the table");
    }
    {
        ReadContextBuilder read_builder(dir->Str());
        read_builder.AddOption(Options::BUCKET, "2");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> ctx, read_builder.Finish());
        ASSERT_NOK_WITH_MSG(table->NewRead(std::move(ctx)),
                            "option 'bucket' is '2' in the context but '1' on the table");
    }
    {
        WriteContextBuilder write_builder(dir->Str(), "commit-user");
        write_builder.AddOption(Options::BUCKET, "2");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> ctx, write_builder.Finish());
        ASSERT_NOK_WITH_MSG(table->NewWrite(std::move(ctx)),
                            "option 'bucket' is '2' in the context but '1' on the table");
    }
    {
        CommitContextBuilder commit_builder(dir->Str(), "commit-user");
        commit_builder.AddOption(Options::BUCKET, "2");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> ctx, commit_builder.Finish());
        ASSERT_NOK_WITH_MSG(table->NewCommit(std::move(ctx)),
                            "option 'bucket' is '2' in the context but '1' on the table");
    }

    // The copy made with the option in question agrees with the context, so it serves it.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> copied, table->Copy({{Options::BUCKET, "2"}}));
    ScanContextBuilder agreeing_scan_builder(dir->Str());
    agreeing_scan_builder.AddOption(Options::BUCKET, "2");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx, agreeing_scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                         copied->NewScan(std::move(ctx)));
}

TEST(TableTest, TestOperatorsIgnoreARuntimeKeyTheTableWasNotLoadedWith) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier,
                                            /*branch=*/"main",
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));
    WriteAndCommitTwoRows(table, dir->Str());

    // `scan.mode` and `scan.snapshot-id` name no option the table was loaded with, so they name
    // a runtime setting the scan does not take: the scan serves the table's own options and
    // reads its latest snapshot - a merged context would have gone looking for snapshot 999,
    // which was never committed.
    ScanContextBuilder scan_builder(dir->Str());
    scan_builder.AddOption(Options::SCAN_MODE, "from-snapshot")
        .AddOption(Options::SCAN_SNAPSHOT_ID, "999");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_ctx, scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan, table->NewScan(std::move(scan_ctx)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_FALSE(plan->Splits().empty());
    ASSERT_EQ(plan->SnapshotId(), std::optional<int64_t>(1));

    // A read takes a runtime key the same way: the key names nothing the table carries, so the
    // read is served rather than refused.
    ReadContextBuilder read_builder(dir->Str());
    read_builder.AddOption(Options::SCAN_MODE, "from-snapshot")
        .AddOption(Options::SCAN_SNAPSHOT_ID, "999");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_ctx, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         table->NewRead(std::move(read_ctx)));

    // The runtime setting a caller means to serve is the copy's: `Table::Copy` derives the table
    // whose options carry it, and that table's scan reads the snapshot it names - which is not
    // there, as this table only ever committed snapshot 1.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> copied,
        table->Copy({{Options::SCAN_MODE, "from-snapshot"}, {Options::SCAN_SNAPSHOT_ID, "999"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> copied_scan_ctx,
                         ScanContextBuilder(dir->Str()).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> copied_scan,
                         copied->NewScan(std::move(copied_scan_ctx)));
    ASSERT_NOK_WITH_MSG(copied_scan->CreatePlan(), "snapshot");
}

TEST(TableTest, TestCreateFailsWhenTheSchemaOptionsDoNotParse) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    // The schema itself carries no validation, so a schema written with options no
    // `CoreOptions` accepts surfaces when the store parses them - at the load, not at the first
    // operator built on the table.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> created,
        TableSchema::Create(
            /*schema_id=*/0,
            arrow::schema({arrow::field("id", arrow::int32(),
                                        /*nullable=*/false)}),
            /*partition_keys=*/{}, /*primary_keys=*/{}, {{Options::TARGET_FILE_ROW_NUM, "0"}}));
    auto schema = checked_pointer_cast<Schema>(std::shared_ptr<TableSchema>(std::move(created)));
    ASSERT_NOK_WITH_MSG(
        FileStoreTable::Create(dir->GetFileSystem(), dir->Str(), schema, Identifier("db", "tbl"),
                               /*uuid=*/"", /*branch=*/BranchManager::DEFAULT_MAIN_BRANCH,
                               /*schema_manager=*/nullptr, /*snapshot_manager=*/nullptr,
                               /*fs_scheme_map=*/{}, /*dynamic_options=*/{}),
        "target-file-row-num should be at least 1");
}

TEST(TableTest, TestTheCacheAScanContextCarriesReachesTheScan) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier,
                                            /*branch=*/"main",
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));
    WriteAndCommitTwoRows(table, dir->Str());

    // The cache is a runtime setting the scan serves from the context rather than from the
    // table's own options, so a scan built with one reads its manifests through it. A scan reads
    // the manifest list of a snapshot on its executor's threads, so the counter the cache keeps
    // is guarded rather than a plain field.
    auto cache = std::make_shared<CountingRoutingCache>(
        std::map<CacheKind, int64_t>{{CacheKind::MANIFEST, 64 * 1024 * 1024},
                                     {CacheKind::SNAPSHOT_LIVE_MANIFEST, 64 * 1024 * 1024}});
    ScanContextBuilder scan_builder(dir->Str());
    scan_builder.WithCache(cache);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_ctx, scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan, table->NewScan(std::move(scan_ctx)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_FALSE(plan->Splits().empty());
    ASSERT_GT(cache->GetCount(CacheKind::MANIFEST), 0)
        << "the cache the scan context carries never reached the scan";
}

TEST(TableTest, TestCopyServesTheTableWithTheGivenOptionsOnTop) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(fs, dir->Str(), identifier, /*branch=*/"main",
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));

    // Two rows are written and committed through the table's own write and commit, so the copy
    // below has data to find.
    WriteContextBuilder write_builder(dir->Str(), "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         table->NewWrite(std::move(write_context)));
    arrow::Int32Builder ids_builder;
    ASSERT_TRUE(ids_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> ids_array;
    ASSERT_TRUE(ids_builder.Finish(&ids_array).ok());
    auto struct_array = arrow::StructArray::Make({ids_array}, {arrow::field("id", arrow::int32(),
                                                                            /*nullable=*/false)})
                            .ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*struct_array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.SetBucket(0).Finish());
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    CommitContextBuilder commit_builder(dir->Str(), "commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         table->NewCommit(std::move(commit_context)));
    ASSERT_OK(commit->Commit(messages));
    ASSERT_OK(write->Close());

    // The copy overlays what the call gives on the options the schema stored; the schema itself
    // and the identity the table was opened with are copied as they are.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> copied,
                         table->Copy({{Options::BUCKET, "2"}, {Options::TYPE, "format-table"}}));
    std::shared_ptr<DataSchema> copied_schema =
        std::dynamic_pointer_cast<DataSchema>(copied->LatestSchema());
    ASSERT_TRUE(copied_schema != nullptr);
    EXPECT_EQ(copied_schema->Options().at(Options::BUCKET), "2");
    // `type` is structural and read from the schema alone, so the copy carries none of it: the
    // option given at the call does not restyle a managed table.
    EXPECT_EQ(copied_schema->Options().count(Options::TYPE), 0);
    EXPECT_EQ(copied_schema->Options().at(Options::BUCKET_KEY), "id");
    EXPECT_EQ(copied_schema->Id(),
              std::dynamic_pointer_cast<DataSchema>(table->LatestSchema())->Id());
    EXPECT_EQ(copied->FullName(), table->FullName());
    EXPECT_EQ(copied->GetRootPath(), table->GetRootPath());
    EXPECT_EQ(copied->GetFileSystem(), table->GetFileSystem());
    // The original keeps the options it was loaded with.
    EXPECT_EQ(
        std::dynamic_pointer_cast<DataSchema>(table->LatestSchema())->Options().at(Options::BUCKET),
        "1");

    // The copy is a table like the one it came from, and serves its data as one.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_ctx,
                         ScanContextBuilder(dir->Str()).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan, copied->NewScan(std::move(scan_ctx)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_FALSE(plan->Splits().empty());
}

TEST(TableTest, TestCopyRefusesThePhysicalOptionsTheDataWasLaidOutWith) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier,
                                            /*branch=*/"main",
                                            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "id"}}));

    // The data on the path was hashed by the `bucket-key` it was written with: a copy that
    // renamed the key would prune by a hash its rows never matched, dropping them silently, so
    // the change is refused - at `Copy` and at the load a context drives alike, which merges
    // through `Copy` - the way the Java `checkImmutability` refuses it. The same goes for the
    // other options that name the physical layout, which a key the schema does not carry
    // changes as much as one it carries differently. `merge-engine`, `bucket`,
    // `row-tracking.enabled` and `data-evolution.enabled` are the options the Java immutable
    // set holds that stay servable here.
    ASSERT_NOK_WITH_MSG(table->Copy({{Options::BUCKET_KEY, "v"}}),
                        "Change 'bucket-key' is not supported yet.");
    ASSERT_NOK_WITH_MSG(table->Copy({{Options::BUCKET_FUNCTION_TYPE, "hash"}}),
                        "Change 'bucket-function.type' is not supported yet.");
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> restyled,
                         table->Copy({{Options::MERGE_ENGINE, "deduplicate"}}));
    {
        ScanContextBuilder scan_builder(dir->Str());
        scan_builder.AddOption(Options::BUCKET_KEY, "v");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx, scan_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(ctx)),
                            "Change 'bucket-key' is not supported yet.");
    }

    // An option that says what the schema already says moves nothing, and a bucket count is
    // the physical option a rescale legitimately rewrites, so a copy may move it. An
    // `ignore-delete` may only be turned on, never back off.
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> unchanged,
                         table->Copy({{Options::BUCKET_KEY, "id"}, {Options::BUCKET, "1"}}));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> rescaled,
                         table->Copy({{Options::BUCKET, "2"}}));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> ignoring_deletes,
                         table->Copy({{Options::IGNORE_DELETE, "true"}}));
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> ignore_delete_table,
                             CreateManagedTable(dir->GetFileSystem(), dir->Str() + "/ignore_delete",
                                                Identifier("db", "igd"), /*branch=*/"main",
                                                {{Options::BUCKET, "1"},
                                                 {Options::BUCKET_KEY, "id"},
                                                 {Options::IGNORE_DELETE, "true"}}));
        ASSERT_NOK_WITH_MSG(ignore_delete_table->Copy({{Options::IGNORE_DELETE, "false"}}),
                            "Cannot change ignore-delete from true to false.");
    }
}

TEST(TableTest, TestALoadNamesTheDatabaseThePathLaysOut) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                                 arrow::field("dt", arrow::utf8())});

    // A context that names no catalog knows the table by its directory: the directory above
    // the table's names the database when it carries the `.db` suffix, the way the Java
    // `identifierFromPath` reads it, so the table the load serves is named for both.
    const std::string table_dir = dir->Str() + "/my_db.db/my_table";
    ASSERT_OK(fs->Mkdirs(table_dir));
    SchemaManager schema_manager(fs, table_dir);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> managed_schema,
                         schema_manager.CreateTable(schema, /*partition_keys=*/{"dt"},
                                                    /*primary_keys=*/{}, {{"file.format", "orc"}}));
    {
        ScanContextBuilder builder(table_dir);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx, builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, FileStoreTableFactory::Create(*ctx));
        ASSERT_EQ(table->FullName(), "my_db.my_table");
    }

    // A table sitting directly under the root, with no `.db` directory above it, names no
    // database: the table is known by its directory alone, the way it was before.
    const std::string loose_table_dir = dir->Str() + "/loose_table";
    ASSERT_OK(fs->Mkdirs(loose_table_dir));
    SchemaManager loose_schema_manager(fs, loose_table_dir);
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] std::unique_ptr<TableSchema> loose_schema,
        loose_schema_manager.CreateTable(schema, /*partition_keys=*/{"dt"}, /*primary_keys=*/{},
                                         {{"file.format", "orc"}}));
    {
        ScanContextBuilder builder(loose_table_dir);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx, builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, FileStoreTableFactory::Create(*ctx));
        ASSERT_EQ(table->FullName(), "loose_table");
    }
}

TEST(TableTest, TestCreateBuildsTheOperator) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetFileSystem(dir->GetFileSystem());
    // A catalog write and commit read the current schema from the catalog on the main branch.
    catalog->SetTableSchema(table->LatestSchema());

    // `TableScan::Create()` and friends build the operator straight from the context, which
    // carries the catalog and identifier it resolves the table through.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_ctx,
                         ScanContextBuilder(dir->Str()).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                         TableScan::Create(std::move(scan_ctx)));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_ctx,
                         ReadContextBuilder(dir->Str()).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         TableRead::Create(std::move(read_ctx)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<WriteContext> write_ctx,
        WriteContextBuilder(dir->Str(), "commit-user").WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<FileStoreWrite> write,
                         FileStoreWrite::Create(std::move(write_ctx)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<CommitContext> commit_ctx,
        CommitContextBuilder(dir->Str(), "commit-user").WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_ctx)));
}

}  // namespace paimon::test
