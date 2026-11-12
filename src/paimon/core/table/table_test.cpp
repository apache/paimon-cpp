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

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/commit_context.h"
#include "paimon/common/catalog/catalog_context.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/catalog/file_system_catalog.h"
#include "paimon/core/catalog/query_auth_catalog.h"
#include "paimon/core/catalog/table_query_auth_result.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/append_only_file_store_table.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/primary_key_file_store_table.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/append_only_table_read.h"
#include "paimon/core/table/source/fallback_table_read.h"
#include "paimon/core/table/source/query_auth_split.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/table/table_factory.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_index_scan.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/schema/schema.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/split.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/mock/mock_catalog.h"
#include "paimon/testing/utils/deletion_vector_test_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"
#include "paimon/write_context.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon::test {

namespace {

// Creates a managed table directory and a `Table` over it, holding the schema written to
// `branch`'s directory under `identifier`.
Result<std::shared_ptr<Table>> CreateManagedTable(const std::shared_ptr<FileSystem>& file_system,
                                                  const std::string& path,
                                                  const Identifier& identifier,
                                                  const std::string& branch = "main") {
    SchemaManager schema_manager(file_system, path, branch);
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false)});
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> created,
                           schema_manager.CreateTable(schema, /*partition_keys=*/{},
                                                      /*primary_keys=*/{}, /*options=*/{}));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(file_system, path, std::shared_ptr<TableSchema>(std::move(created)),
                               identifier, "", branch));
    return std::shared_ptr<Table>(std::move(table));
}

}  // namespace

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

    std::shared_ptr<Schema> latest_schema = table->GetSchema();
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
    ASSERT_EQ((std::vector<std::string>{"b1_c0"}), table->GetSchema()->FieldNames());
    for (const auto& main_identifier :
         {Identifier("db1", "tbl1$branch_main"), Identifier("db1", "tbl1$branch_MAIN")}) {
        ASSERT_OK_AND_ASSIGN(table, catalog.GetTable(main_identifier));
        ASSERT_EQ((std::vector<std::string>{"main_c0"}), table->GetSchema()->FieldNames());
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
        // A catalog that loaded the schema itself calls the constructor directly, which a
        // rest catalog is shaped like. The alias resolves to the main branch as well.
        ASSERT_OK_AND_ASSIGN(std::string resolved_branch, main_identifier.GetBranchNameOrDefault());
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FileStoreTable> table,
            FileStoreTable::Create(fs, dir->Str(), std::dynamic_pointer_cast<TableSchema>(schema),
                                   main_identifier, "", resolved_branch));
        std::string branch = table->CurrentBranch();
        EXPECT_EQ("main", branch);
    }
}

TEST(TableTest, TestCurrentBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    // A table whose identifier names no branch is a table of the main branch.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> main_table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));
    std::string branch = std::dynamic_pointer_cast<FileStoreTable>(main_table)->CurrentBranch();
    EXPECT_EQ(branch, "main");

    // The identifier a table was opened with names the branch it is for.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> branch_table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str() + "/branch_tbl",
                                            Identifier("db", "tbl", "dev"), /*branch=*/"dev"));
    branch = std::dynamic_pointer_cast<FileStoreTable>(branch_table)->CurrentBranch();
    EXPECT_EQ(branch, "dev");
}

TEST(TableTest, TestCatalogOpenedTableServesThePerTableFileSystem) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableSchema(table->GetSchema());

    catalog->SetTableFileSystem(dir->GetFileSystem());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> opened, catalog->GetTable(identifier));
    EXPECT_EQ(opened->GetFileSystem(), dir->GetFileSystem());
    // This mock serves no table location, so the table it opens carries none.
    EXPECT_EQ(opened->GetRootPath(), "");
}

TEST(TableTest, TestGetTableDispatchesByTableType) {
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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> format_table,
                         catalog.GetTable(Identifier("db", "format")));
    ASSERT_NE(nullptr, std::dynamic_pointer_cast<FormatTable>(format_table));

    const std::string managed_dir = dir->Str() + "/db.db/managed";
    ASSERT_OK(fs->Mkdirs(managed_dir));
    SchemaManager managed_schema_manager(fs, managed_dir);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> managed_schema,
                         managed_schema_manager.CreateTable(schema, /*partition_keys=*/{"dt"},
                                                            /*primary_keys=*/{}, {}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> managed_table,
                         catalog.GetTable(Identifier("db", "managed")));
    ASSERT_NE(nullptr, std::dynamic_pointer_cast<AppendOnlyFileStoreTable>(managed_table));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> system_table,
                         catalog.GetTable(Identifier("db", "managed$options")));
    ASSERT_NE(nullptr, std::dynamic_pointer_cast<SystemTable>(system_table));
    ASSERT_EQ((std::vector<std::string>{"key", "value"}), system_table->GetSchema()->FieldNames());
    ASSERT_EQ(nullptr, std::dynamic_pointer_cast<DataSchema>(system_table->GetSchema()));
    for (bool direct : {false, true}) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                             ReadContextBuilder(managed_dir + "$options")
                                 .EnablePreciseFileIndexEvaluate(true)
                                 .Finish());
        auto read = direct ? system_table->NewRead(std::move(context))
                           : TableRead::Create(std::move(context));
        ASSERT_NOK_WITH_MSG(read,
                            "Precise file-index evaluation is not supported for system tables");
    }
    ASSERT_NOK_WITH_MSG(system_table->NewWrite(nullptr), "read-only");
    ASSERT_NOK_WITH_MSG(system_table->NewCommit(nullptr), "read-only");

    const std::string pk_dir = dir->Str() + "/db.db/pk";
    SchemaManager pk_schema_manager(fs, pk_dir);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> pk_schema,
                         pk_schema_manager.CreateTable(schema, {}, {"id"}, {}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> pk_table, catalog.GetTable(Identifier("db", "pk")));
    ASSERT_NE(nullptr, std::dynamic_pointer_cast<PrimaryKeyFileStoreTable>(pk_table));
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
    catalog->SetTableSchema(table->GetSchema());
    catalog->SetTableLocation(dir->Str());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx,
                         ScanContextBuilder(dir->Str()).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                         table->NewScan(std::move(ctx)));
}

TEST(TableTest, TestNewReadCreatesATableRead) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetFileSystem(dir->GetFileSystem());
    catalog->SetTableSchema(table->GetSchema());
    catalog->SetTableLocation(dir->Str());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> ctx,
                         ReadContextBuilder(dir->Str()).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         table->NewRead(std::move(ctx)));
}

namespace {

class MockQueryAuthCatalog : public MockVersionManagedCatalog, public QueryAuthCatalog {
 public:
    Result<std::shared_ptr<TableQueryAuthResult>> AuthTableQuery(
        const Identifier& identifier,
        const std::optional<std::vector<std::string>>& select) const override {
        auth_identifiers.push_back(identifier);
        auth_selects.push_back(select);
        std::map<std::string, std::string> selected_masking;
        for (const auto& [column, transform] : column_masking) {
            if (!select || std::find(select->begin(), select->end(), column) != select->end()) {
                selected_masking.emplace(column, transform);
            }
        }
        return TableQueryAuthResult::Create(filter, selected_masking);
    }

    Result<std::string> GetTableLocation(const Identifier& identifier) const override {
        location_identifiers.push_back(identifier);
        if (table_location.empty()) {
            return MockVersionManagedCatalog::GetTableLocation(identifier);
        }
        return table_location;
    }

    Result<std::shared_ptr<Schema>> LoadTableSchemaAndLocation(
        const Identifier& identifier, std::string* location) const override {
        *location = table_location;
        return LoadTableSchema(identifier);
    }

    Result<std::shared_ptr<Schema>> LoadTableSchema(const Identifier& identifier) const override {
        Result<std::shared_ptr<Schema>> schema =
            MockVersionManagedCatalog::LoadTableSchema(identifier);
        auto iter = object_schemas.find(identifier.GetObjectName());
        if (iter != object_schemas.end()) {
            return iter->second;
        }
        return schema;
    }

    std::string table_location;
    std::map<std::string, std::shared_ptr<Schema>> object_schemas;
    std::vector<std::string> filter;
    std::map<std::string, std::string> column_masking;
    mutable std::vector<Identifier> auth_identifiers;
    mutable std::vector<std::optional<std::vector<std::string>>> auth_selects;
    mutable std::vector<Identifier> location_identifiers;
};

std::shared_ptr<arrow::Schema> QueryAuthTableSchema() {
    return arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                          arrow::field("name", arrow::utf8()), arrow::field("phone", arrow::utf8()),
                          arrow::field("salary", arrow::int64())});
}

std::map<std::string, std::string> QueryAuthTableOptions(const std::string& file_format) {
    return {{Options::FILE_FORMAT, file_format},
            {Options::BUCKET, "1"},
            {Options::BUCKET_KEY, "id"},
            {Options::QUERY_AUTH_ENABLED, "true"}};
}

Result<std::vector<std::shared_ptr<CommitMessage>>> WriteQueryAuthRows(
    TestHelper* helper, const std::string& rows, int64_t commit_identifier,
    const std::vector<RecordBatch::RowKind>& row_kinds = {}) {
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<RecordBatch> batch,
        TestHelper::MakeRecordBatch(arrow::struct_(QueryAuthTableSchema()->fields()), rows,
                                    /*partition_map=*/{}, /*bucket=*/0, row_kinds));
    return helper->WriteAndCommit(std::move(batch), commit_identifier, std::nullopt);
}

Result<std::unique_ptr<TestHelper>> CreateQueryAuthTable(
    const std::string& root, const std::vector<std::string>& primary_keys,
    const std::map<std::string, std::string>& options, const std::vector<std::string>& commits) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TestHelper> helper,
                           TestHelper::Create(root, QueryAuthTableSchema(), /*partition_keys=*/{},
                                              primary_keys, options, /*is_streaming_mode=*/true));
    for (size_t i = 0; i < commits.size(); ++i) {
        PAIMON_RETURN_NOT_OK(
            WriteQueryAuthRows(helper.get(), commits[i], static_cast<int64_t>(i)).status());
    }
    return helper;
}

Result<std::unique_ptr<TestHelper>> CreateQueryAuthTable(const std::string& root) {
    return CreateQueryAuthTable(root, /*primary_keys=*/{}, QueryAuthTableOptions("orc"),
                                {R"([[1, "alice", "13800001111", 100],
                                     [2, "bob", "13900002222", 200],
                                     [3, "carol", null, 300]])"});
}

Result<std::shared_ptr<MockQueryAuthCatalog>> CreateQueryAuthCatalog(
    const TestHelper& helper, const std::shared_ptr<FileSystem>& file_system) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> table_schema,
                           helper.LatestSchema());
    if (!table_schema) {
        return Status::Invalid("the table has no schema");
    }
    auto catalog = std::make_shared<MockQueryAuthCatalog>();
    catalog->table_location = helper.TablePath();
    catalog->SetFileSystem(file_system);
    catalog->SetTableSchema(checked_pointer_cast<Schema>(table_schema.value()));
    return catalog;
}

const char kPhonePrefixMask[] =
    R"({"name": "SUBSTRING", "inputs": [{"index": 2, "name": "phone", "type": "STRING"}, 1, 3]})";
const char kSalaryAbove150[] =
    R"({"kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {"index": 3,)"
    R"( "name": "salary", "type": "BIGINT"}}, "function": "GREATER_THAN", "literals": [150]})";

std::string ConstantFilter(const std::string& function) {
    return fmt::format(R"({{"kind": "LEAF", "transform": {{"name": "NULL"}},)"
                       R"( "function": "{}", "literals": []}})",
                       function);
}

std::string CompoundFilter(const std::string& function, const std::string& left,
                           const std::string& right) {
    return fmt::format(R"({{"kind": "COMPOUND", "function": "{}", "children": [{}, {}]}})",
                       function, left, right);
}

Result<std::shared_ptr<arrow::StructArray>> ReadAll(
    std::unique_ptr<ReadContext> context, const std::vector<std::shared_ptr<Split>>& splits) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> read, TableRead::Create(std::move(context)));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, read->CreateReader(splits));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> chunks,
                           ReadResultCollector::CollectResult(std::move(reader)));
    read.reset();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> array,
        arrow::Concatenate(chunks->chunks(), arrow::default_memory_pool()));
    return checked_pointer_cast<arrow::StructArray>(array);
}

void AssertColumn(const arrow::StructArray& rows, const std::string& name,
                  const std::string& expected) {
    std::shared_ptr<arrow::Array> column = rows.GetFieldByName(name);
    ASSERT_NE(nullptr, column) << name;
    std::shared_ptr<arrow::Array> expected_column =
        arrow::ipc::internal::json::ArrayFromJSON(column->type(), expected).ValueOrDie();
    ASSERT_TRUE(column->Equals(*expected_column))
        << name << ": " << column->ToString() << " != " << expected;
}

Result<std::shared_ptr<Plan>> PlanQueryAuth(const std::string& path,
                                            const std::shared_ptr<MockQueryAuthCatalog>& catalog,
                                            const std::shared_ptr<Predicate>& predicate,
                                            std::optional<int32_t> limit) {
    ScanContextBuilder builder(path);
    builder.WithCatalog(catalog, Identifier("foo", "bar"));
    if (predicate != nullptr) {
        builder.SetPredicate(predicate);
    }
    if (limit) {
        builder.SetLimit(limit.value());
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> context, builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan, TableScan::Create(std::move(context)));
    return scan->CreatePlan();
}

Result<std::shared_ptr<arrow::StructArray>> PlanAndReadAll(
    const std::string& path, const std::shared_ptr<MockQueryAuthCatalog>& catalog,
    const std::vector<std::string>& read_fields = {}) {
    ScanContextBuilder scan_builder(path);
    scan_builder.WithCatalog(catalog, Identifier("foo", "bar"));
    ReadContextBuilder read_builder(path);
    if (!read_fields.empty()) {
        scan_builder.SetReadFieldNames(read_fields);
        read_builder.SetReadFieldNames(read_fields);
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan,
                           TableScan::Create(std::move(scan_context)));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, scan->CreatePlan());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    return ReadAll(std::move(read_context), plan->Splits());
}

}  // namespace

TEST(TableTest, TestLoadedTableAppliesQueryAuthThroughVirtualOperators) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper, CreateQueryAuthTable(dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    FileSystemCatalog file_catalog(dir->GetFileSystem(), dir->Str(), {});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         file_catalog.GetTable(Identifier("foo", "bar")));
    catalog->filter = {kSalaryAbove150};
    catalog->column_masking = {{"phone", kPhonePrefixMask}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         ScanContextBuilder(table->GetRootPath())
                             .WithCatalog(catalog, Identifier("foo", "bar"))
                             .Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan, table->NewScan(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_FALSE(plan->Splits().empty());
    ASSERT_NE(nullptr, std::dynamic_pointer_cast<QueryAuthSplit>(plan->Splits()[0]));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                         ReadContextBuilder(table->GetRootPath()).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, table->NewRead(std::move(read_context)));
    ASSERT_NOK_WITH_MSG(
        read->CreateReader(
            std::dynamic_pointer_cast<QueryAuthSplit>(plan->Splits()[0])->GetSplit()),
        "must be planned by a scan of the table through its catalog");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> chunks,
                         ReadResultCollector::CollectResult(std::move(reader)));
    auto concatenated = arrow::Concatenate(chunks->chunks(), arrow::default_memory_pool());
    ASSERT_TRUE(concatenated.ok()) << concatenated.status().ToString();
    std::shared_ptr<arrow::Array> rows = concatenated.ValueOrDie();
    auto result = checked_pointer_cast<arrow::StructArray>(rows);
    AssertColumn(*result, "id", "[2, 3]");
    AssertColumn(*result, "phone", R"(["139", null])");
}

TEST(TableTest, TestQueryAuthTableAppliesTheRulesOfItsCatalog) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper, CreateQueryAuthTable(dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    const Identifier identifier("foo", "bar");
    catalog->filter = {
        R"({"kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {"index": 0,)"
        R"( "name": "id", "type": "INT NOT NULL"}}, "function": "GREATER_THAN", "literals": [1]})"};

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         ScanContextBuilder(path).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_TRUE(catalog->auth_identifiers.empty());
    catalog->column_masking = {{"phone", kPhonePrefixMask}, {"salary", R"({"name": "NULL"})"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_EQ(1u, catalog->auth_identifiers.size());
    ASSERT_EQ(identifier, catalog->auth_identifiers[0]);
    ASSERT_FALSE(catalog->auth_selects[0]);
    ASSERT_FALSE(plan->Splits().empty());
    for (const std::shared_ptr<Split>& split : plan->Splits()) {
        ASSERT_NE(nullptr, std::dynamic_pointer_cast<QueryAuthSplit>(split));
    }

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                         ReadContextBuilder(path).Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows,
                         ReadAll(std::move(read_context), plan->Splits()));
    AssertColumn(*rows, "id", "[2, 3]");
    AssertColumn(*rows, "name", R"(["bob", "carol"])");
    AssertColumn(*rows, "phone", R"(["139", null])");
    AssertColumn(*rows, "salary", "[null, null]");

    ASSERT_OK_AND_ASSIGN(read_context,
                         ReadContextBuilder(path).SetReadFieldNames({"name"}).Finish());
    ASSERT_OK_AND_ASSIGN(rows, ReadAll(std::move(read_context), plan->Splits()));
    AssertColumn(*rows, "name", R"(["bob", "carol"])");
    ASSERT_EQ(nullptr, rows->GetFieldByName("id"));
    ASSERT_OK_AND_ASSIGN(read_context, ReadContextBuilder(path).SetReadFieldIds({1}).Finish());
    ASSERT_OK_AND_ASSIGN(rows, ReadAll(std::move(read_context), plan->Splits()));
    AssertColumn(*rows, "name", R"(["bob", "carol"])");
    ASSERT_EQ(nullptr, rows->GetFieldByName("id"));

    ASSERT_OK_AND_ASSIGN(read_context, ReadContextBuilder(path)
                                           .SetReadFieldNames({"name", "phone"})
                                           .SetPredicate(PredicateBuilder::Equal(
                                               /*field_index=*/1, "phone", FieldType::STRING,
                                               Literal(FieldType::STRING, "139", 3)))
                                           .EnablePredicateFilter(true)
                                           .Finish());
    ASSERT_OK_AND_ASSIGN(rows, ReadAll(std::move(read_context), plan->Splits()));
    AssertColumn(*rows, "name", R"(["bob"])");
    AssertColumn(*rows, "phone", R"(["139"])");
    ASSERT_EQ(nullptr, rows->GetFieldByName("id"));

    std::vector<std::shared_ptr<Split>> deserialized;
    for (const std::shared_ptr<Split>& split : plan->Splits()) {
        ASSERT_OK_AND_ASSIGN(std::string bytes, Split::Serialize(split, GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> copy,
                             Split::Deserialize(bytes.data(), bytes.size(), GetDefaultPool()));
        auto auth_split = std::dynamic_pointer_cast<QueryAuthSplit>(copy);
        ASSERT_NE(nullptr, auth_split);
        const auto& rules = std::dynamic_pointer_cast<QueryAuthSplit>(split)->GetAuthResult();
        ASSERT_TRUE(auth_split->GetAuthResult()->SameFilter(*rules));
        ASSERT_EQ(rules->ColumnMasking(), auth_split->GetAuthResult()->ColumnMasking());
        const auto& original = std::dynamic_pointer_cast<QueryAuthSplit>(split);
        ASSERT_TRUE(auth_split->GetScope());
        ASSERT_EQ(original->GetScope(), auth_split->GetScope());
        deserialized.push_back(copy);
    }
    ASSERT_OK_AND_ASSIGN(read_context, ReadContextBuilder(path).Finish());
    ASSERT_OK_AND_ASSIGN(rows, ReadAll(std::move(read_context), deserialized));
    AssertColumn(*rows, "phone", R"(["139", null])");

    const auto& planned = std::dynamic_pointer_cast<QueryAuthSplit>(plan->Splits()[0]);
    ASSERT_OK_AND_ASSIGN(
        std::string java_planned,
        Split::Serialize(std::make_shared<QueryAuthSplit>(planned->GetSplit(),
                                                          planned->GetAuthResult(), std::nullopt),
                         GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Split> java_planned_split,
        Split::Deserialize(java_planned.data(), java_planned.size(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(read_context,
                         ReadContextBuilder(path).SetReadFieldNames({"name"}).Finish());
    ASSERT_NOK_WITH_MSG(ReadAll(std::move(read_context), {java_planned_split}),
                        "The split does not record which columns its query auth rules were "
                        "authorized for");

    catalog->filter = {};
    ASSERT_NOK_WITH_MSG(scan->CreatePlan(), "Query auth rules changed");
    ASSERT_EQ(2u, catalog->auth_identifiers.size());

    ASSERT_OK_AND_ASSIGN(scan_context, ScanContextBuilder(path)
                                           .WithCatalog(catalog, identifier)
                                           .SetPartitionFilter({{{"phone", "139"}}})
                                           .Finish());
    ASSERT_OK_AND_ASSIGN(scan, TableScan::Create(std::move(scan_context)));
    ASSERT_NOK_WITH_MSG(scan->CreatePlan(), "masked partition key 'phone'");

    ASSERT_OK_AND_ASSIGN(read_context, ReadContextBuilder(path).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_NOK_WITH_MSG(
        read->CreateReader(
            std::dynamic_pointer_cast<QueryAuthSplit>(plan->Splits()[0])->GetSplit()),
        "must be planned by a scan of the table through its catalog");
    ASSERT_NOK(read->CreateCountReader(plan->Splits()));
    ASSERT_EQ((std::map<std::string, int32_t>{{"id", 0}, {"phone", 2}, {"salary", 3}}),
              planned->GetScope()->rule_field_ids);
    ASSERT_EQ((std::map<std::string, int32_t>{{"id", 0}, {"name", 1}, {"phone", 2}, {"salary", 3}}),
              planned->GetScope()->authorized_field_ids);
    QueryAuthScope replaced_scope = planned->GetScope().value();
    replaced_scope.rule_field_ids["phone"] = 1;
    ASSERT_NOK_WITH_MSG(
        read->CreateReader(std::make_shared<QueryAuthSplit>(
            planned->GetSplit(), planned->GetAuthResult(), replaced_scope)),
        "(schema 0): Query auth rules reference column 'phone' which the schema being read "
        "exposes as a different column of the same name");
    QueryAuthScope unbound_scope = planned->GetScope().value();
    unbound_scope.rule_field_ids.erase("phone");
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::make_shared<QueryAuthSplit>(
                            planned->GetSplit(), planned->GetAuthResult(), unbound_scope)),
                        "Query auth rules reference column 'phone', which the schema the scan "
                        "planned with did not hold");

    catalog->filter = {
        R"({"kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {"index": 9,)"
        R"( "name": "gone", "type": "INT"}}, "function": "IS_NULL", "literals": []})"};
    ASSERT_OK_AND_ASSIGN(scan_context,
                         ScanContextBuilder(path).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN(scan, TableScan::Create(std::move(scan_context)));
    ASSERT_NOK_WITH_MSG(scan->CreatePlan(),
                        "Query auth of Identifier{database='foo', object='bar'} (branch main, "
                        "schema 0): Row filter references column 'gone' which does not exist");
    catalog->filter = {};
    catalog->column_masking = {{"name", R"({"name": "UPPER", "inputs": []})"}};
    ASSERT_OK_AND_ASSIGN(scan_context,
                         ScanContextBuilder(path).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN(scan, TableScan::Create(std::move(scan_context)));
    ASSERT_NOK_WITH_MSG(scan->CreatePlan(), "Column mask transform 'UPPER' is not supported");
    catalog->column_masking = {};
    for (const std::string literals : {"", R"(, "literals": null)"}) {
        catalog->filter = {fmt::format(
            R"({{"kind": "LEAF", "transform": {{"name": "FIELD_REF", "fieldRef": {{"index": 0,)"
            R"( "name": "id", "type": "INT NOT NULL"}}}}, "function": "NOT_IN"{}}})",
            literals)};
        ASSERT_OK_AND_ASSIGN(scan_context,
                             ScanContextBuilder(path).WithCatalog(catalog, identifier).Finish());
        ASSERT_OK_AND_ASSIGN(scan, TableScan::Create(std::move(scan_context)));
        ASSERT_NOK_WITH_MSG(scan->CreatePlan(),
                            "Query auth row filter NOT_IN requires a non-null 'literals' array.");
    }
}

TEST(TableTest, TestQueryAuthTableRefusesColumnsChangedSinceThePlan) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper, CreateQueryAuthTable(dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    const Identifier identifier("foo", "bar");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    catalog->column_masking = {{"phone", R"({"name": "NULL"})"}};

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         ScanContextBuilder(path)
                             .WithCatalog(catalog, identifier)
                             .SetReadFieldNames({"name"})
                             .Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> name_plan, scan->CreatePlan());
    ASSERT_FALSE(std::dynamic_pointer_cast<QueryAuthSplit>(name_plan->Splits()[0])
                     ->GetAuthResult()
                     ->HasRules());
    catalog->column_masking = {};
    ASSERT_OK_AND_ASSIGN(scan_context,
                         ScanContextBuilder(path).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN(scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> all_plan, scan->CreatePlan());
    catalog->column_masking = {{"phone", R"({"name": "NULL"})"}};
    ASSERT_OK_AND_ASSIGN(scan_context,
                         ScanContextBuilder(path).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN(scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> masked_plan, scan->CreatePlan());
    catalog->column_masking = {};
    catalog->filter = {CompoundFilter(
        "OR", ConstantFilter("TRUE"),
        R"({"kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {"index": 2,)"
        R"( "name": "phone", "type": "STRING"}}, "function": "IS_NULL", "literals": []})")};
    ASSERT_OK_AND_ASSIGN(scan_context,
                         ScanContextBuilder(path).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN(scan, TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> folded_plan, scan->CreatePlan());
    catalog->filter = {};
    catalog->column_masking = {{"phone", R"({"name": "NULL"})"}};
    ASSERT_OK_AND_ASSIGN(scan_context, ScanContextBuilder(path)
                                           .WithCatalog(catalog, identifier)
                                           .SetReadFieldNames({"name"})
                                           .Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> unplanned_scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(scan_context, ScanContextBuilder(path)
                                           .WithCatalog(catalog, identifier)
                                           .SetReadFieldNames({"id"})
                                           .Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> id_scan,
                         TableScan::Create(std::move(scan_context)));

    ASSERT_OK_AND_ASSIGN(std::string schema_json, table_schema.value()->ToJsonString());
    rapidjson::Document document;
    document.Parse(schema_json.c_str());
    ASSERT_FALSE(document.HasParseError());
    auto& allocator = document.GetAllocator();
    document["id"].SetInt64(1);
    rapidjson::Value& fields = document["fields"];
    fields[1]["name"].SetString("nickname", allocator);
    fields[2]["name"].SetString("name", allocator);
    rapidjson::Value secret(rapidjson::kObjectType);
    secret.AddMember("id", 4, allocator);
    secret.AddMember("name", "secret", allocator);
    secret.AddMember("type", "STRING", allocator);
    fields.PushBack(secret, allocator);
    document["highestFieldId"].SetInt(4);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document.Accept(writer);
    ASSERT_OK(TableSchema::CreateFromJson(buffer.GetString()).status());
    ASSERT_OK(dir->GetFileSystem()->AtomicStore(
        PathUtil::JoinPath(SchemaManager(dir->GetFileSystem(), path).SchemaDirectory(), "schema-1"),
        buffer.GetString()));

    ASSERT_NOK_WITH_MSG(unplanned_scan->CreatePlan(),
                        "Column 'name' is field 1 in schema 0 the scan plans with, but field 2 in "
                        "the latest schema 1");
    catalog->filter = {
        R"({"kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {"index": 4,)"
        R"( "name": "secret", "type": "STRING"}}, "function": "IS_NULL", "literals": []})"};
    ASSERT_NOK_WITH_MSG(id_scan->CreatePlan(),
                        "Query auth rules reference column 'secret', which the schema the scan "
                        "planned with did not hold");

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                         ReadContextBuilder(path).SetReadFieldNames({"name"}).Finish());
    ASSERT_NOK_WITH_MSG(ReadAll(std::move(read_context), name_plan->Splits()),
                        "Column 'name' is read as field 2, but the scan authorized field 1 under "
                        "that name");
    ASSERT_OK_AND_ASSIGN(read_context,
                         ReadContextBuilder(path).SetReadFieldNames({"secret"}).Finish());
    ASSERT_NOK_WITH_MSG(ReadAll(std::move(read_context), all_plan->Splits()),
                        "Column 'secret' is read, but the scan did not ask the catalog to "
                        "authorize it");
    ASSERT_OK_AND_ASSIGN(read_context,
                         ReadContextBuilder(path).SetReadFieldNames({"id", "salary"}).Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows,
                         ReadAll(std::move(read_context), all_plan->Splits()));
    AssertColumn(*rows, "id", "[1, 2, 3]");
    ASSERT_OK_AND_ASSIGN(read_context, ReadContextBuilder(path).SetReadFieldNames({"id"}).Finish());
    ASSERT_NOK_WITH_MSG(ReadAll(std::move(read_context), masked_plan->Splits()),
                        "Query auth rules reference column 'phone' which the schema being read "
                        "exposes as 'name' (renamed since)");
    ASSERT_OK_AND_ASSIGN(read_context, ReadContextBuilder(path).SetReadFieldNames({"id"}).Finish());
    ASSERT_NOK_WITH_MSG(ReadAll(std::move(read_context), folded_plan->Splits()),
                        "Query auth rules reference column 'phone' which the schema being read "
                        "exposes as 'name' (renamed since)");
}

TEST(TableTest, TestQueryAuthTableAsksForTheRulesOfTheBranchPlanned) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper, CreateQueryAuthTable(dir->Str()));
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    const std::map<std::string, std::string> table_options = QueryAuthTableOptions("orc");
    std::map<std::string, std::string> dev_options = table_options;
    dev_options[Options::BRANCH] = "dev";
    ASSERT_OK(SchemaManager(dir->GetFileSystem(), path, "dev")
                  .CreateTable(QueryAuthTableSchema(), /*partition_keys=*/{}, /*primary_keys=*/{},
                               table_options)
                  .status());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> dev_helper,
                         TestHelper::Create(path, dev_options, /*is_streaming_mode=*/false));
    ASSERT_OK(WriteQueryAuthRows(dev_helper.get(), R"([[7, "dave", "13700007777", 700]])",
                                 /*commit_identifier=*/0)
                  .status());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    catalog->column_masking = {{"phone", R"({"name": "NULL"})"}};

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         ScanContextBuilder(path)
                             .WithCatalog(catalog, Identifier("foo", "bar"))
                             .AddOption(Options::BRANCH, "dev")
                             .Finish());
    ASSERT_EQ(Identifier("foo", "bar$branch_dev"), scan_context->GetIdentifier().value());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_FALSE(catalog->TableFileSystemRequests().empty());
    for (const Identifier& requested : catalog->TableFileSystemRequests()) {
        ASSERT_EQ(Identifier("foo", "bar$branch_dev"), requested);
    }
    ASSERT_EQ(std::vector<Identifier>({Identifier("foo", "bar$branch_dev")}),
              catalog->auth_identifiers);
    ASSERT_EQ(std::vector<Identifier>({Identifier("foo", "bar$branch_dev")}),
              catalog->location_identifiers);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                         ReadContextBuilder(path).AddOption(Options::BRANCH, "dev").Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows,
                         ReadAll(std::move(read_context), plan->Splits()));
    AssertColumn(*rows, "id", "[7]");
    AssertColumn(*rows, "phone", "[null]");
}

TEST(TableTest, TestQueryAuthTableIsNotSkippedThroughAStaleCachedSchema) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper, CreateQueryAuthTable(dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    ASSERT_OK_AND_ASSIGN(std::string schema_json, table_schema.value()->ToJsonString());
    rapidjson::Document document;
    document.Parse(schema_json.c_str());
    ASSERT_FALSE(document.HasParseError());
    document["options"].RemoveMember(Options::QUERY_AUTH_ENABLED);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document.Accept(writer);
    const std::string stale_schema = buffer.GetString();

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         ScanContextBuilder(path).SetTableSchema(stale_schema).Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                        "without the catalog that serves its row filters and column masks");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    ASSERT_OK_AND_ASSIGN(
        scan_context,
        ScanContextBuilder(path).WithCatalog(catalog, Identifier("foo", "bar")).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                         ReadContextBuilder(path).SetTableSchema(stale_schema).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_NOK_WITH_MSG(
        read->CreateReader(
            std::dynamic_pointer_cast<QueryAuthSplit>(plan->Splits()[0])->GetSplit()),
        "must be planned by a scan of the table through its catalog");

    ASSERT_OK(SchemaManager(dir->GetFileSystem(), path, "dev")
                  .CreateTable(QueryAuthTableSchema(), /*partition_keys=*/{}, /*primary_keys=*/{},
                               {{Options::QUERY_AUTH_ENABLED, "false"}})
                  .status());
    ASSERT_OK_AND_ASSIGN(read_context, ReadContextBuilder(path)
                                           .WithBranch("dev")
                                           .SetTableSchema(stale_schema)
                                           .AddOption(Options::SCAN_FALLBACK_BRANCH, "main")
                                           .Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                        "Cannot read fallback branch 'main', which enables 'query-auth.enabled'");
}

TEST(TableTest, TestQueryAuthScanDoesNotPruneByTheRawValuesOfAMaskedColumn) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::map<std::string, std::string> options = QueryAuthTableOptions("orc");
    options[Options::SOURCE_SPLIT_TARGET_SIZE] = "1 kb";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper,
                         CreateQueryAuthTable(dir->Str(), /*primary_keys=*/{}, options,
                                              {R"([[1, "alice", "13800001111", 100]])",
                                               R"([[3, "carol", "13700003333", 300]])",
                                               R"([[2, "bob", "13900002222", 200]])"}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    auto read_with =
        [&](const std::shared_ptr<Predicate>& predicate, const std::shared_ptr<Plan>& plan,
            bool enable_predicate_filter = true) -> Result<std::shared_ptr<arrow::StructArray>> {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> context,
                               ReadContextBuilder(path)
                                   .SetPredicate(predicate)
                                   .EnablePredicateFilter(enable_predicate_filter)
                                   .Finish());
        return ReadAll(std::move(context), plan->Splits());
    };
    const std::shared_ptr<Predicate> phone_is_139 = PredicateBuilder::Equal(
        /*field_index=*/2, "phone", FieldType::STRING, Literal(FieldType::STRING, "139", 3));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         ScanContextBuilder(path)
                             .WithCatalog(catalog, Identifier("foo", "bar"))
                             .SetPredicate(phone_is_139)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_TRUE(plan->Splits().empty());

    catalog->column_masking = {{"phone", kPhonePrefixMask}};
    ASSERT_NOK_WITH_MSG(scan->CreatePlan(), "Query auth rules changed");
    ASSERT_OK_AND_ASSIGN(plan, PlanQueryAuth(path, catalog, phone_is_139, std::nullopt));
    ASSERT_EQ(3u, plan->Splits().size());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows, read_with(phone_is_139, plan));
    AssertColumn(*rows, "id", "[2]");
    AssertColumn(*rows, "phone", R"(["139"])");

    const std::shared_ptr<Predicate> id_is_1 = PredicateBuilder::Equal(
        /*field_index=*/0, "id", FieldType::INT, Literal(static_cast<int32_t>(1)));
    ASSERT_OK_AND_ASSIGN(rows, read_with(phone_is_139, plan, /*enable_predicate_filter=*/false));
    AssertColumn(*rows, "id", "[2]");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> phone_and_id,
                         PredicateBuilder::And({phone_is_139, id_is_1}));
    ASSERT_OK_AND_ASSIGN(rows, read_with(phone_and_id, plan, /*enable_predicate_filter=*/false));
    AssertColumn(*rows, "id", "[2]");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> phone_or_id,
                         PredicateBuilder::Or({phone_is_139, id_is_1}));
    ASSERT_OK_AND_ASSIGN(plan, PlanQueryAuth(path, catalog, phone_or_id, std::nullopt));
    ASSERT_EQ(3u, plan->Splits().size());
    ASSERT_OK_AND_ASSIGN(rows, read_with(phone_or_id, plan));
    AssertColumn(*rows, "id", "[1, 2]");
    AssertColumn(*rows, "phone", R"(["138", "139"])");

    ASSERT_OK_AND_ASSIGN(plan, PlanQueryAuth(path, catalog, phone_is_139, /*limit=*/1));
    ASSERT_EQ(3u, plan->Splits().size());
    ASSERT_OK_AND_ASSIGN(rows, read_with(phone_is_139, plan));
    AssertColumn(*rows, "id", "[2]");
    AssertColumn(*rows, "phone", R"(["139"])");
    ASSERT_OK_AND_ASSIGN(plan, PlanQueryAuth(path, catalog, /*predicate=*/nullptr, /*limit=*/1));
    ASSERT_EQ(1u, plan->Splits().size());
    catalog->filter = {kSalaryAbove150};
    ASSERT_OK_AND_ASSIGN(plan, PlanQueryAuth(path, catalog, /*predicate=*/nullptr, /*limit=*/1));
    ASSERT_EQ(2u, plan->Splits().size());
}

TEST(TableTest, TestQueryAuthReadRefusesAColumnTheScanDidNotAuthorize) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper, CreateQueryAuthTable(dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    catalog->column_masking = {{"phone", kPhonePrefixMask}};
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    auto read_names = [&](bool enable_predicate_filter,
                          const std::vector<std::shared_ptr<Split>>& splits)
        -> Result<std::shared_ptr<arrow::StructArray>> {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<ReadContext> context,
            ReadContextBuilder(path)
                .SetReadFieldNames({"name"})
                .SetPredicate(PredicateBuilder::Equal(/*field_index=*/1, "phone", FieldType::STRING,
                                                      Literal(FieldType::STRING, "139", 3)))
                .EnablePredicateFilter(enable_predicate_filter)
                .Finish());
        return ReadAll(std::move(context), splits);
    };
    const std::string unauthorized =
        "Column 'phone' is read, but the scan did not ask the catalog to authorize it";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> name_scan_context,
                         ScanContextBuilder(path)
                             .WithCatalog(catalog, Identifier("foo", "bar"))
                             .SetReadFieldNames({"name"})
                             .Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> name_scan,
                         TableScan::Create(std::move(name_scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> name_plan, name_scan->CreatePlan());
    ASSERT_EQ(std::optional<std::vector<std::string>>(std::vector<std::string>{"name"}),
              catalog->auth_selects.back());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> all_columns_context,
                         ReadContextBuilder(path).Finish());
    ASSERT_NOK_WITH_MSG(
        ReadAll(std::move(all_columns_context), name_plan->Splits()),
        "Column 'id' is read, but the scan did not ask the catalog to authorize it");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> name_context,
                         ReadContextBuilder(path).SetReadFieldNames({"name"}).Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows,
                         ReadAll(std::move(name_context), name_plan->Splits()));
    AssertColumn(*rows, "name", R"(["alice", "bob", "carol"])");
    ASSERT_NOK_WITH_MSG(read_names(/*enable_predicate_filter=*/true, name_plan->Splits()),
                        unauthorized);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         PlanQueryAuth(path, catalog, /*predicate=*/nullptr, std::nullopt));
    ASSERT_OK_AND_ASSIGN(rows, read_names(/*enable_predicate_filter=*/false, plan->Splits()));
    AssertColumn(*rows, "name", R"(["bob"])");
    ASSERT_EQ(nullptr, rows->GetFieldByName("phone"));
    auto planned = std::dynamic_pointer_cast<QueryAuthSplit>(plan->Splits()[0]);
    ASSERT_NE(nullptr, planned);
    ASSERT_EQ(1u, planned->GetAuthResult()->MaskedFields().count("phone"));
    QueryAuthScope scope = planned->GetScope().value();
    scope.authorized_field_ids.erase("phone");
    ASSERT_NOK_WITH_MSG(read_names(/*enable_predicate_filter=*/false,
                                   {std::make_shared<QueryAuthSplit>(
                                       planned->GetSplit(), planned->GetAuthResult(), scope)}),
                        unauthorized);
}

TEST(TableTest, TestQueryAuthScanPushesTheLimitPastARowFilterOnPartitionKeys) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TestHelper> helper,
        TestHelper::Create(dir->Str(), QueryAuthTableSchema(), /*partition_keys=*/{"name"},
                           /*primary_keys=*/{}, QueryAuthTableOptions("orc"),
                           /*is_streaming_mode=*/true));
    const std::vector<std::pair<std::string, std::string>> partitions = {
        {"alice", R"([[1, "alice", "13800001111", 100]])"},
        {"bob", R"([[2, "bob", "13900002222", 200]])"},
        {"carol", R"([[3, "carol", "13700003333", 300]])"}};
    for (size_t i = 0; i < partitions.size(); ++i) {
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch,
            TestHelper::MakeRecordBatch(arrow::struct_(QueryAuthTableSchema()->fields()),
                                        partitions[i].second, {{"name", partitions[i].first}},
                                        /*bucket=*/0, /*row_kinds=*/{}));
        ASSERT_OK(helper->WriteAndCommit(std::move(batch), static_cast<int64_t>(i), std::nullopt)
                      .status());
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    catalog->filter = {
        R"({"kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {"index": 1,)"
        R"( "name": "name", "type": "STRING"}}, "function": "IN", "literals": ["alice", "bob"]})"};
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         PlanQueryAuth(path, catalog, /*predicate=*/nullptr, std::nullopt));
    ASSERT_EQ(2u, plan->Splits().size());
    ASSERT_OK_AND_ASSIGN(plan, PlanQueryAuth(path, catalog, /*predicate=*/nullptr, /*limit=*/1));
    ASSERT_EQ(1u, plan->Splits().size());
}

TEST(TableTest, TestQueryAuthTableAppliesTheSameRulesToEachFileFormat) {
    for (const std::string file_format : {"orc", "parquet"}) {
        SCOPED_TRACE(file_format);
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper,
                             CreateQueryAuthTable(dir->Str(), /*primary_keys=*/{},
                                                  QueryAuthTableOptions(file_format),
                                                  {R"([[1, "alice", "13800001111", 100],
                                                       [2, "bob", "13900002222", 200],
                                                       [3, "carol", null, 300]])"}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                             CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
        catalog->filter = {kSalaryAbove150};
        catalog->column_masking = {{"phone", kPhonePrefixMask}, {"salary", R"({"name": "NULL"})"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows,
                             PlanAndReadAll(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), catalog));
        AssertColumn(*rows, "id", "[2, 3]");
        AssertColumn(*rows, "name", R"(["bob", "carol"])");
        AssertColumn(*rows, "phone", R"(["139", null])");
        AssertColumn(*rows, "salary", "[null, null]");
    }
}

TEST(TableTest, TestQueryAuthRefusesToIgnoreAMaskOnALevelColumn) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                                 arrow::field("_LEVEL", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TestHelper> helper,
        TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{}, /*primary_keys=*/{},
                           QueryAuthTableOptions("orc"), /*is_streaming_mode=*/false));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch,
        TestHelper::MakeRecordBatch(arrow::struct_(schema->fields()), R"([[1, "secret"]])",
                                    /*partition_map=*/{}, /*bucket=*/0, /*row_kinds=*/{}));
    ASSERT_OK(
        helper->WriteAndCommit(std::move(batch), /*commit_identifier=*/0, std::nullopt).status());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows, PlanAndReadAll(path, catalog));
    AssertColumn(*rows, "_LEVEL", R"(["secret"])");
    catalog->column_masking = {{"_LEVEL", R"({"name": "NULL"})"}};
    ASSERT_NOK_WITH_MSG(PlanAndReadAll(path, catalog), "refusing to ignore its mask");
}

TEST(TableTest, TestQueryAuthTableReadsNoRowsUnderAJavaAlwaysFalseFilter) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper, CreateQueryAuthTable(dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    auto read_ids =
        [&](const std::vector<std::string>& read_fields) -> Result<std::shared_ptr<arrow::Array>> {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan,
                               PlanQueryAuth(path, catalog, /*predicate=*/nullptr, std::nullopt));
        ReadContextBuilder builder(path);
        builder.SetReadFieldNames(read_fields);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> read,
                               TableRead::Create(std::move(context)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                               read->CreateReader(plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> chunks,
                               ReadResultCollector::CollectResult(std::move(reader)));
        if (chunks == nullptr) {
            return std::shared_ptr<arrow::Array>();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::Concatenate(chunks->chunks(), arrow::default_memory_pool()));
        return checked_pointer_cast<arrow::StructArray>(array)->GetFieldByName("id");
    };
    for (const auto& [filter, expected] :
         std::vector<std::pair<std::vector<std::string>, std::optional<std::string>>>{
             {{ConstantFilter("TRUE")}, "[1, 2, 3]"},
             {{CompoundFilter("OR", ConstantFilter("FALSE"), kSalaryAbove150)}, "[2, 3]"},
             {{ConstantFilter("FALSE")}, std::nullopt},
             {{kSalaryAbove150,
               CompoundFilter("AND", ConstantFilter("TRUE"), ConstantFilter("FALSE"))},
              std::nullopt}}) {
        SCOPED_TRACE(filter[0]);
        catalog->filter = filter;
        for (const std::vector<std::string>& read_fields :
             {std::vector<std::string>{"id"}, std::vector<std::string>{"id", "name"}}) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> ids, read_ids(read_fields));
            if (!expected) {
                ASSERT_EQ(nullptr, ids);
                continue;
            }
            ASSERT_NE(nullptr, ids);
            ASSERT_TRUE(ids->Equals(
                *arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), expected.value())
                     .ValueOrDie()))
                << ids->ToString();
        }
    }
}

TEST(TableTest, TestQueryAuthPrimaryKeyTableAuthorizesTheMergedRows) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TestHelper> helper,
        CreateQueryAuthTable(dir->Str(), /*primary_keys=*/{"id"}, QueryAuthTableOptions("orc"),
                             {R"([[1, "alice", "13800001111", 100],
                                                   [2, "bob", "13900002222", 200],
                                                   [3, "carol", "13700003333", 300]])",
                              R"([[2, "bob", "13900002222", 50]])"}));
    ASSERT_OK(WriteQueryAuthRows(helper.get(), R"([[3, "carol", "13700003333", 50]])",
                                 /*commit_identifier=*/2, {RecordBatch::RowKind::DELETE})
                  .status());
    ASSERT_OK(WriteQueryAuthRows(helper.get(), R"([[1, "alice", "13800001111", 400]])",
                                 /*commit_identifier=*/3)
                  .status());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    catalog->filter = {kSalaryAbove150};
    catalog->column_masking = {{"phone", kPhonePrefixMask}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows,
                         PlanAndReadAll(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), catalog));
    AssertColumn(*rows, "id", "[1]");
    AssertColumn(*rows, "phone", R"(["138"])");
    AssertColumn(*rows, "salary", "[400]");
}

TEST(TableTest, TestQueryAuthDoesNotBringBackRowsADeletionVectorDeletes) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper,
                         CreateQueryAuthTable(dir->Str(), /*primary_keys=*/{},
                                              {{Options::FILE_FORMAT, "orc"},
                                               {Options::BUCKET, "-1"},
                                               {Options::DELETION_VECTORS_ENABLED, "true"},
                                               {Options::QUERY_AUTH_ENABLED, "true"}},
                                              /*commits=*/{}));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> data_messages,
                         WriteQueryAuthRows(helper.get(), R"([[1, "alice", "13800001111", 100],
                                                              [2, "bob", "13900002222", 200],
                                                              [3, "carol", "13700003333", 300]])",
                                            /*commit_identifier=*/0));
    ASSERT_EQ(1u, data_messages.size());
    auto data_message = std::dynamic_pointer_cast<CommitMessageImpl>(data_messages[0]);
    ASSERT_NE(nullptr, data_message);
    ASSERT_EQ(1u, data_message->GetNewFilesIncrement().NewFiles().size());
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitMessage> deletion_message,
                         DeletionVectorTestHelper::CreateDeletionVectorCommitMessage(
                             dir->GetFileSystem(), path, "orc", data_messages[0],
                             {{data_message->GetNewFilesIncrement().NewFiles()[0]->file_name, {1}}},
                             GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         CommitContextBuilder(path, "commit_user").Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit({deletion_message}, /*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    catalog->filter = {kSalaryAbove150};
    catalog->column_masking = {{"phone", kPhonePrefixMask}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows, PlanAndReadAll(path, catalog));
    AssertColumn(*rows, "id", "[3]");
    AssertColumn(*rows, "phone", R"(["137"])");
}

TEST(TableTest, TestQueryAuthAppliesTheRulesToTheColumnsADataEvolutionTableRebuilds) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper,
                         CreateQueryAuthTable(dir->Str(), /*primary_keys=*/{},
                                              {{Options::FILE_FORMAT, "orc"},
                                               {Options::ROW_TRACKING_ENABLED, "true"},
                                               {Options::DATA_EVOLUTION_ENABLED, "true"},
                                               {Options::QUERY_AUTH_ENABLED, "true"}},
                                              {R"([[1, "alice", "13800001111", 100],
                                                   [2, "bob", "13900002222", 200]])"}));
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<WriteContext> write_context,
        WriteContextBuilder(path, "commit_user").WithWriteSchema({"phone", "salary"}).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         FileStoreWrite::Create(std::move(write_context)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch,
        TestHelper::MakeRecordBatch(arrow::struct_({arrow::field("phone", arrow::utf8()),
                                                    arrow::field("salary", arrow::int64())}),
                                    R"([["13700001111", 300], ["13600002222", 50]])",
                                    /*partition_map=*/{}, /*bucket=*/0,
                                    /*row_kinds=*/{}));
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit(/*wait_compaction=*/false, /*commit_identifier=*/1));
    ASSERT_OK(write->Close());
    for (const std::shared_ptr<CommitMessage>& message : messages) {
        auto message_impl = std::dynamic_pointer_cast<CommitMessageImpl>(message);
        ASSERT_NE(nullptr, message_impl);
        for (const std::shared_ptr<DataFileMeta>& file :
             message_impl->GetNewFilesIncrement().NewFiles()) {
            file->AssignFirstRowId(0);
        }
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         CommitContextBuilder(path, "commit_user").Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(messages, /*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    catalog->filter = {kSalaryAbove150};
    catalog->column_masking = {{"phone", kPhonePrefixMask}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows,
                         PlanAndReadAll(path, catalog, {"name", "phone"}));
    AssertColumn(*rows, "name", R"(["alice"])");
    AssertColumn(*rows, "phone", R"(["137"])");
    ASSERT_EQ(nullptr, rows->GetFieldByName("salary"));
    ASSERT_EQ(nullptr, rows->GetFieldByName("id"));
}

TEST(TableTest, TestQueryAuthFiltersDictionaryEncodedParquetColumns) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::map<std::string, std::string> options = QueryAuthTableOptions("parquet");
    options["parquet.read.enable-dictionary-passthrough"] = "true";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper,
                         CreateQueryAuthTable(dir->Str(), /*primary_keys=*/{}, options,
                                              {R"([[1, "alice", "13800001111", 100],
                                                   [2, "bob", "13900002222", 200],
                                                   [3, "carol", null, 300],
                                                   [4, "dave", "13700004444", 400]])"}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    auto read = [&](const std::shared_ptr<Predicate>& predicate,
                    bool enable_predicate_filter) -> Result<std::shared_ptr<arrow::StructArray>> {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan,
                               PlanQueryAuth(path, catalog, predicate, std::nullopt));
        ReadContextBuilder builder(path);
        if (predicate != nullptr) {
            builder.SetPredicate(predicate).EnablePredicateFilter(enable_predicate_filter);
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                               TableRead::Create(std::move(context)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                               table_read->CreateReader(plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            return Status::Invalid("no rows read");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ImportArray(batch.first.get(), batch.second.get()));
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch eof, reader->NextBatch());
        if (!BatchReader::IsEofBatch(eof)) {
            ReaderUtils::ReleaseReadBatch(std::move(eof));
            return Status::Invalid("more than one batch read");
        }
        reader->Close();
        return checked_pointer_cast<arrow::StructArray>(array);
    };
    auto assert_column = [](const arrow::StructArray& rows, const std::string& name,
                            arrow::Type::type type, const std::string& values) {
        std::shared_ptr<arrow::Array> column = rows.GetFieldByName(name);
        ASSERT_NE(nullptr, column) << name;
        ASSERT_EQ(type, column->type_id()) << name << ": " << column->type()->ToString();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> decoded,
                             CastingUtils::DecodeDictionary(column, arrow::default_memory_pool()));
        ASSERT_TRUE(decoded->Equals(
            *arrow::ipc::internal::json::ArrayFromJSON(decoded->type(), values).ValueOrDie()))
            << name << ": " << decoded->ToString();
    };
    auto equal = [](int32_t index, const std::string& name, const std::string& value) {
        return PredicateBuilder::Equal(index, name, FieldType::STRING,
                                       Literal(FieldType::STRING, value.data(), value.size()));
    };

    catalog->filter = {
        R"({"kind": "LEAF", "transform": {"name": "FIELD_REF", "fieldRef": {"index": 1,)"
        R"( "name": "name", "type": "STRING"}}, "function": "NOT_EQUAL", "literals": ["bob"]})"};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows,
                         read(/*predicate=*/nullptr, /*enable_predicate_filter=*/false));
    assert_column(*rows, "id", arrow::Type::INT32, "[1, 3, 4]");
    assert_column(*rows, "name", arrow::Type::STRING, R"(["alice", "carol", "dave"])");
    // Decoded: the dictionary of the column would still hold bob's phone number.
    assert_column(*rows, "phone", arrow::Type::STRING, R"(["13800001111", null, "13700004444"])");

    catalog->filter = {kSalaryAbove150};
    ASSERT_OK_AND_ASSIGN(rows, read(equal(1, "name", "carol"), /*enable_predicate_filter=*/true));
    assert_column(*rows, "id", arrow::Type::INT32, "[3]");
    assert_column(*rows, "name", arrow::Type::STRING, R"(["carol"])");
    assert_column(*rows, "phone", arrow::Type::STRING, "[null]");

    catalog->column_masking = {{"phone", kPhonePrefixMask}};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> phone_or_name,
        PredicateBuilder::Or({equal(2, "phone", "137"), equal(1, "name", "carol")}));
    for (bool enable_predicate_filter : {false, true}) {
        SCOPED_TRACE(enable_predicate_filter);
        ASSERT_OK_AND_ASSIGN(rows, read(phone_or_name, enable_predicate_filter));
        assert_column(*rows, "id", arrow::Type::INT32, "[3, 4]");
        assert_column(*rows, "name", arrow::Type::STRING, R"(["carol", "dave"])");
        assert_column(*rows, "phone", arrow::Type::STRING, R"([null, "137"])");
    }

    catalog->filter = {};
    catalog->column_masking = {};
    ASSERT_OK_AND_ASSIGN(rows, read(/*predicate=*/nullptr, /*enable_predicate_filter=*/false));
    assert_column(*rows, "phone", arrow::Type::DICTIONARY,
                  R"(["13800001111", "13900002222", null, "13700004444"])");
}

TEST(TableTest, TestQueryAuthScanReadsOnlyTheLocationOfTheAuthorizedTable) {
    auto authorized_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(authorized_dir);
    auto other_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(other_dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> authorized,
                         CreateQueryAuthTable(authorized_dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> other, CreateQueryAuthTable(other_dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*authorized, authorized_dir->GetFileSystem()));
    const Identifier identifier("foo", "bar");

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ScanContext> scan_context,
        ScanContextBuilder(other->TablePath()).WithCatalog(catalog, identifier).Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                        fmt::format("Invalid: The scan targets {}, but the catalog keeps table "
                                    "foo.bar, whose query auth settings it would apply, at {}",
                                    other->TablePath(), authorized->TablePath()));
    ASSERT_TRUE(catalog->auth_identifiers.empty());

    for (const std::string& path :
         {authorized->TablePath() + "/",
          PathUtil::JoinPath(authorized_dir->Str() + "/", "foo.db//bar")}) {
        SCOPED_TRACE(path);
        ASSERT_OK_AND_ASSIGN(scan_context,
                             ScanContextBuilder(path).WithCatalog(catalog, identifier).Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                             TableScan::Create(std::move(scan_context)));
        ASSERT_OK(scan->CreatePlan().status());
    }

    // The identifier is rebound to another table between the scan and its first plan.
    ASSERT_OK_AND_ASSIGN(
        scan_context,
        ScanContextBuilder(authorized->TablePath()).WithCatalog(catalog, identifier).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> rebound_scan,
                         TableScan::Create(std::move(scan_context)));
    catalog->table_location = other->TablePath();
    const size_t auth_requests = catalog->auth_identifiers.size();
    ASSERT_NOK_WITH_MSG(rebound_scan->CreatePlan(),
                        fmt::format("The scan targets {}, but the catalog keeps table foo.bar, "
                                    "whose query auth settings it would apply, at {}",
                                    authorized->TablePath(), other->TablePath()));
    ASSERT_EQ(auth_requests + 1, catalog->auth_identifiers.size());

    catalog->table_location.clear();
    ASSERT_OK_AND_ASSIGN(
        scan_context,
        ScanContextBuilder(authorized->TablePath()).WithCatalog(catalog, identifier).Finish());
    ASSERT_NOK_WITH_MSG(
        TableScan::Create(std::move(scan_context)),
        "Cannot check that " + authorized->TablePath() + " is the location of table foo.bar");
}

TEST(TableTest, TestQueryAuthReadRejectsSplitsAuthorizedForAnotherTable) {
    auto authorized_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(authorized_dir);
    auto other_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(other_dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> authorized,
                         CreateQueryAuthTable(authorized_dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> other, CreateQueryAuthTable(other_dir->Str()));
    // Restored or copied tables can have the same field ids and data file names.
    ASSERT_TRUE(TestUtil::CopyDirectory(authorized->TablePath(), other->TablePath()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> authorized_catalog,
                         CreateQueryAuthCatalog(*authorized, authorized_dir->GetFileSystem()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> other_catalog,
                         CreateQueryAuthCatalog(*other, other_dir->GetFileSystem()));
    other_catalog->column_masking = {{"phone", R"({"name": "NULL"})"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> masked,
                         PlanAndReadAll(other->TablePath(), other_catalog));
    AssertColumn(*masked, "phone", "[null, null, null]");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> authorized_plan,
                         PlanQueryAuth(authorized->TablePath(), authorized_catalog,
                                       /*predicate=*/nullptr, std::nullopt));
    std::vector<std::shared_ptr<Split>> splits;
    for (const std::shared_ptr<Split>& split : authorized_plan->Splits()) {
        ASSERT_OK_AND_ASSIGN(std::string bytes, Split::Serialize(split, GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Split> restored,
                             Split::Deserialize(bytes.data(), bytes.size(), GetDefaultPool()));
        splits.push_back(std::move(restored));
    }
    for (bool with_catalog : {false, true}) {
        ReadContextBuilder builder(other->TablePath());
        if (with_catalog) {
            builder.WithCatalog(other_catalog, Identifier("foo", "bar"));
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                             TableRead::Create(std::move(context)));
        ASSERT_FALSE(splits.empty());
        ASSERT_NOK_WITH_MSG(read->CreateReader(splits.front()), "split was authorized for");
        ASSERT_NOK_WITH_MSG(read->CreateReader(splits), "split was authorized for");
    }

    // A worker can use an equivalent spelling of the authorized table's location.
    for (const std::string& path :
         {authorized->TablePath() + "/", "file://" + authorized->TablePath()}) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                             ReadContextBuilder(path).Finish());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> rows,
                             ReadAll(std::move(context), splits));
        AssertColumn(*rows, "phone", R"(["13800001111", "13900002222", null])");
    }
}

TEST(TableTest, TestQueryAuthReadRejectsSplitsAuthorizedForAnotherBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper, CreateQueryAuthTable(dir->Str()));
    const std::string path = helper->TablePath();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    ASSERT_OK(SchemaManager(dir->GetFileSystem(), path, "dev")
                  .CreateTable(QueryAuthTableSchema(), /*partition_keys=*/{}, /*primary_keys=*/{},
                               QueryAuthTableOptions("orc"))
                  .status());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         PlanQueryAuth(path, catalog, /*predicate=*/nullptr, std::nullopt));
    ASSERT_FALSE(plan->Splits().empty());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                         ReadContextBuilder(path).WithBranch("dev").Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, TableRead::Create(std::move(context)));
    ASSERT_NOK_WITH_MSG(read->CreateReader(plan->Splits().front()),
                        "split was authorized for table " + path + " (branch main)");
    ASSERT_NOK_WITH_MSG(read->CreateReader(plan->Splits()),
                        "but the read targets " + path + " (branch dev)");

    auto planned = std::dynamic_pointer_cast<QueryAuthSplit>(plan->Splits().front());
    ASSERT_NE(nullptr, planned);
    for (bool missing_path : {false, true}) {
        QueryAuthScope incomplete_scope = planned->GetScope().value();
        if (missing_path) {
            incomplete_scope.table_path.clear();
        } else {
            incomplete_scope.branch.clear();
        }
        ASSERT_NOK_WITH_MSG(read->CreateReader(std::make_shared<QueryAuthSplit>(
                                planned->GetSplit(), planned->GetAuthResult(), incomplete_scope)),
                            "does not record the table path and branch");
    }
}

TEST(TableTest, TestCatalogDisablesQueryAuthOnlyForTheTableAtItsLocation) {
    auto disabled_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(disabled_dir);
    auto other_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(other_dir);
    std::map<std::string, std::string> options = QueryAuthTableOptions("orc");
    options.erase(Options::QUERY_AUTH_ENABLED);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TestHelper> disabled,
        CreateQueryAuthTable(disabled_dir->Str(), /*primary_keys=*/{}, options, /*commits=*/{}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TestHelper> other,
        CreateQueryAuthTable(other_dir->Str(), /*primary_keys=*/{}, options, /*commits=*/{}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> auth_catalog,
                         CreateQueryAuthCatalog(*disabled, disabled_dir->GetFileSystem()));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> disabled_schema,
                         disabled->LatestSchema());
    ASSERT_TRUE(disabled_schema);
    auto plain_catalog = std::make_shared<MockVersionManagedCatalog>();
    plain_catalog->SetFileSystem(disabled_dir->GetFileSystem());
    plain_catalog->SetTableSchema(checked_pointer_cast<Schema>(disabled_schema.value()));
    plain_catalog->SetTableLocation(disabled->TablePath());
    const Identifier identifier("foo", "bar");
    auto mismatch = [&](const std::string& operation) {
        return fmt::format(
            "Invalid: The {} targets {}, but the catalog keeps table foo.bar, whose query auth "
            "settings it would apply, at {}",
            operation, other->TablePath(), disabled->TablePath());
    };

    for (const std::shared_ptr<Catalog>& catalog :
         {std::shared_ptr<Catalog>(auth_catalog), std::shared_ptr<Catalog>(plain_catalog)}) {
        for (const std::string& suffix : {std::string(), std::string("$files")}) {
            SCOPED_TRACE(suffix);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                                 ScanContextBuilder(other->TablePath() + suffix)
                                     .WithCatalog(catalog, identifier)
                                     .Finish());
            ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)), mismatch("scan"));
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                                 ReadContextBuilder(other->TablePath() + suffix)
                                     .WithCatalog(catalog, identifier)
                                     .Finish());
            ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)), mismatch("read"));

            ASSERT_OK_AND_ASSIGN(scan_context, ScanContextBuilder(disabled->TablePath() + suffix)
                                                   .WithCatalog(catalog, identifier)
                                                   .Finish());
            ASSERT_OK(TableScan::Create(std::move(scan_context)).status());
            ASSERT_OK_AND_ASSIGN(read_context, ReadContextBuilder(disabled->TablePath() + suffix)
                                                   .WithCatalog(catalog, identifier)
                                                   .Finish());
            ASSERT_OK(TableRead::Create(std::move(read_context)).status());
        }
    }
    ASSERT_TRUE(auth_catalog->auth_identifiers.empty());
}

TEST(TableTest, TestQueryAuthTableRefusesWhatCannotApplyItsRules) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper, CreateQueryAuthTable(dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    const Identifier identifier("foo", "bar");
    const std::string disabled =
        "Invalid: Option 'query-auth.enabled' is not allowed to be disabled dynamically";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         ScanContextBuilder(path).Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                        "NotImplemented: Cannot read a table with 'query-auth.enabled=true' "
                        "without the catalog");
    auto plain_catalog = std::make_shared<MockVersionManagedCatalog>();
    plain_catalog->SetFileSystem(dir->GetFileSystem());
    plain_catalog->SetTableSchema(checked_pointer_cast<Schema>(table_schema.value()));
    ASSERT_OK_AND_ASSIGN(scan_context,
                         ScanContextBuilder(path).WithCatalog(plain_catalog, identifier).Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                        "its catalog does not support the query auth interface");

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ScanContext> disabling_scan_context,
        ScanContextBuilder(path).AddOption(Options::QUERY_AUTH_ENABLED, "false").Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(disabling_scan_context)), disabled);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadContext> disabling_read_context,
        ReadContextBuilder(path).AddOption(Options::QUERY_AUTH_ENABLED, "false").Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(disabling_read_context)), disabled);

    auto explicit_read_schema = std::make_unique<::ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*arrow::schema({arrow::field("name", arrow::utf8())}),
                                    explicit_read_schema.get())
                    .ok());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadContext> explicit_schema_read_context,
        ReadContextBuilder(path).SetReadSchema(std::move(explicit_read_schema)).Finish());
    Status explicit_schema_status =
        TableRead::Create(std::move(explicit_schema_read_context)).status();
    ASSERT_TRUE(explicit_schema_status.IsNotImplemented()) << explicit_schema_status.ToString();
    ASSERT_NOK_WITH_MSG(explicit_schema_status,
                        "An explicit read schema is not supported on a query-auth table");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> search_read_context,
                         ReadContextBuilder(path).EnablePreciseFileIndexEvaluate(true).Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(search_read_context)),
                        "Search is not supported on a query-auth table");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> realtime_read_context,
                         ReadContextBuilder(path).WithRealtimeContext(realtime_context).Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(realtime_read_context)),
                        "Real-time reads are not supported on a query-auth table");
    const std::string row_id = SpecialFields::RowId().Name();
    const std::string not_in_schema =
        fmt::format("NotImplemented: Column '{}' is not a column of the table schema", row_id);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> row_id_read_context,
                         ReadContextBuilder(path).SetReadFieldNames({"id", row_id}).Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(row_id_read_context)), not_in_schema);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadContext> row_id_by_id_read_context,
        ReadContextBuilder(path).SetReadFieldIds({0, SpecialFields::RowId().Id()}).Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(row_id_by_id_read_context)),
                        fmt::format("NotImplemented: Field {} is not a column of the table schema",
                                    SpecialFields::RowId().Id()));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadContext> row_id_filter_read_context,
        ReadContextBuilder(path)
            .SetReadFieldNames({"id"})
            .SetPredicate(PredicateBuilder::IsNotNull(/*field_index=*/1, row_id, FieldType::BIGINT))
            .EnablePredicateFilter(true)
            .Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(row_id_filter_read_context)), not_in_schema);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadContext> row_id_unfiltered_read_context,
        ReadContextBuilder(path)
            .SetReadFieldNames({"id"})
            .SetPredicate(PredicateBuilder::IsNotNull(/*field_index=*/1, row_id, FieldType::BIGINT))
            .EnablePredicateFilter(false)
            .Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(row_id_unfiltered_read_context)),
                        not_in_schema);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MockQueryAuthCatalog> auth_catalog,
                         CreateQueryAuthCatalog(*helper, dir->GetFileSystem()));
    ASSERT_OK_AND_ASSIGN(scan_context, ScanContextBuilder(path)
                                           .WithCatalog(auth_catalog, identifier)
                                           .SetReadFieldNames({"id", row_id})
                                           .Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)), not_in_schema);
    ASSERT_OK_AND_ASSIGN(scan_context, ScanContextBuilder(path)
                                           .WithCatalog(auth_catalog, identifier)
                                           .SetPredicate(PredicateBuilder::IsNotNull(
                                               /*field_index=*/4, row_id, FieldType::BIGINT))
                                           .Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)), not_in_schema);
    ASSERT_OK_AND_ASSIGN(scan_context, ScanContextBuilder(path)
                                           .WithCatalog(auth_catalog, identifier)
                                           .SetPartitionFilter({{{"gone", "1"}}})
                                           .Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                        "NotImplemented: Column 'gone' is not a column of the table schema");
    ASSERT_OK_AND_ASSIGN(
        scan_context, ScanContextBuilder(path)
                          .WithCatalog(auth_catalog, identifier)
                          .SetGlobalIndexResult(BitmapGlobalIndexResult::FromRanges({Range(0, 1)}))
                          .Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                        "Search is not supported on a query-auth table");
    ASSERT_OK_AND_ASSIGN(scan_context, ScanContextBuilder(path)
                                           .WithCatalog(auth_catalog, identifier)
                                           .WithRealtimeContext(realtime_context)
                                           .Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                        "Real-time reads are not supported on a query-auth table");
    ASSERT_TRUE(auth_catalog->auth_identifiers.empty());
    const std::string global_index_refused =
        "NotImplemented: Global index search is not supported on a table with "
        "'query-auth.enabled=true'";
    ASSERT_NOK_WITH_MSG(
        GlobalIndexScan::Create(path, /*snapshot_id=*/std::nullopt, /*partitions=*/std::nullopt,
                                /*options=*/{}, dir->GetFileSystem(), CreateDefaultExecutor(),
                                /*pool=*/nullptr),
        global_index_refused);
    ASSERT_NOK_WITH_MSG(GlobalIndexScan::Create(path, /*snapshot_id=*/std::nullopt,
                                                /*partition_filters=*/std::shared_ptr<Predicate>(),
                                                /*options=*/{}, dir->GetFileSystem(),
                                                CreateDefaultExecutor(), /*pool=*/nullptr),
                        global_index_refused);

    for (const std::string system_table : {"files", "manifests", "binlog", "audit_log", "ro"}) {
        ASSERT_NOK_WITH_MSG(
            SystemTableLoader::LoadFromPath(dir->GetFileSystem(), path + "$" + system_table, {}),
            fmt::format("NotImplemented: System table '{}' is not supported on a table with "
                        "'query-auth.enabled=true'",
                        system_table));
    }
    ASSERT_OK(
        SystemTableLoader::LoadFromPath(dir->GetFileSystem(), path + "$options", {}).status());
}

TEST(TableTest, TestQueryAuthRefusesFallbackReadsAcrossBranches) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper, CreateQueryAuthTable(dir->Str()));
    const std::string path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    for (const std::string enabled : {"false", "true"}) {
        const std::string branch = "fallback_" + enabled;
        ASSERT_OK(SchemaManager(dir->GetFileSystem(), path, branch)
                      .CreateTable(QueryAuthTableSchema(), /*partition_keys=*/{},
                                   /*primary_keys=*/{}, {{Options::QUERY_AUTH_ENABLED, enabled}})
                      .status());
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ReadContext> context,
            ReadContextBuilder(path).AddOption(Options::SCAN_FALLBACK_BRANCH, branch).Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(context)),
                            "Fallback reads are not supported on a query-auth table");
    }

    const std::string fallback_path = PathUtil::JoinPath(dir->Str(), "fallback_tbl");
    auto fallback_schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false)});
    ASSERT_OK(SchemaManager(dir->GetFileSystem(), fallback_path)
                  .CreateTable(fallback_schema, /*partition_keys=*/{}, /*primary_keys=*/{},
                               /*options=*/{})
                  .status());
    ASSERT_OK(SchemaManager(dir->GetFileSystem(), fallback_path, "fallback")
                  .CreateTable(fallback_schema, /*partition_keys=*/{}, /*primary_keys=*/{},
                               /*options=*/{{Options::QUERY_AUTH_ENABLED, "true"}})
                  .status());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> fallback_read_context,
                         ReadContextBuilder(fallback_path)
                             .AddOption(Options::SCAN_FALLBACK_BRANCH, "fallback")
                             .Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(fallback_read_context)),
                        "Cannot read fallback branch 'fallback', which enables "
                        "'query-auth.enabled', of a table that does not");
}

TEST(TableTest, TestQueryAuthSystemTablesValidateTheBranchRead) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string path = dir->Str();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> main_schema,
                         SchemaManager(dir->GetFileSystem(), path)
                             .CreateTable(QueryAuthTableSchema(), /*partition_keys=*/{},
                                          /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK(SchemaManager(dir->GetFileSystem(), path, "dev")
                  .CreateTable(QueryAuthTableSchema(), /*partition_keys=*/{}, /*primary_keys=*/{},
                               {{Options::QUERY_AUTH_ENABLED, "true"}})
                  .status());
    ASSERT_OK(SchemaManager(dir->GetFileSystem(), path, "restricted")
                  .CreateTable(QueryAuthTableSchema(), /*partition_keys=*/{}, /*primary_keys=*/{},
                               /*options=*/{})
                  .status());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> restricted_catalog_schema,
                         TableSchema::Create(/*schema_id=*/0, QueryAuthTableSchema(),
                                             /*partition_keys=*/{}, /*primary_keys=*/{},
                                             {{Options::QUERY_AUTH_ENABLED, "true"}}));
    auto catalog = std::make_shared<MockQueryAuthCatalog>();
    catalog->table_location = path;
    catalog->SetFileSystem(dir->GetFileSystem());
    catalog->SetTableSchema(
        checked_pointer_cast<Schema>(std::shared_ptr<TableSchema>(std::move(main_schema))));
    catalog->object_schemas["bar$branch_restricted"] = checked_pointer_cast<Schema>(
        std::shared_ptr<TableSchema>(std::move(restricted_catalog_schema)));
    const Identifier identifier("foo", "bar");
    for (const std::string name : {"files", "manifests", "binlog", "audit_log", "ro"}) {
        SCOPED_TRACE(name);
        const std::string system_path = path + "$" + name;
        const std::string refused = fmt::format(
            "NotImplemented: System table '{}' is not supported on a table with "
            "'query-auth.enabled=true'",
            name);
        ASSERT_NOK_WITH_MSG(SystemTableLoader::LoadFromPath(dir->GetFileSystem(), system_path,
                                                            {{Options::BRANCH, "dev"}}),
                            refused);
        ASSERT_NOK_WITH_MSG(
            SystemTableLoader::LoadFromPath(dir->GetFileSystem(), path + "$branch_dev$" + name, {}),
            refused);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ScanContext> scan_context,
            ScanContextBuilder(system_path).AddOption(Options::BRANCH, "dev").Finish());
        ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)), refused);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             ReadContextBuilder(system_path).WithBranch("dev").Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)), refused);

        const std::string restricted_path = path + "$branch_restricted$" + name;
        ASSERT_OK_AND_ASSIGN(
            scan_context,
            ScanContextBuilder(restricted_path).WithCatalog(catalog, identifier).Finish());
        ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)), refused);
        ASSERT_OK_AND_ASSIGN(
            read_context,
            ReadContextBuilder(restricted_path).WithCatalog(catalog, identifier).Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)), refused);
        ASSERT_EQ(Identifier("foo", "bar$branch_restricted"),
                  catalog->LoadTableSchemaIdentifiers().back());
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> main_read_context,
                         ReadContextBuilder(path + "$files").Finish());
    ASSERT_OK(TableRead::Create(std::move(main_read_context)).status());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ScanContext> main_scan_context,
        ScanContextBuilder(path + "$files").WithCatalog(catalog, identifier).Finish());
    ASSERT_OK(TableScan::Create(std::move(main_scan_context)).status());
    ASSERT_OK_AND_ASSIGN(
        main_read_context,
        ReadContextBuilder(path + "$files").WithCatalog(catalog, identifier).Finish());
    ASSERT_OK(TableRead::Create(std::move(main_read_context)).status());
    ASSERT_NOK_WITH_MSG(
        SystemTableLoader::LoadFromPath(dir->GetFileSystem(), path + "$branch_dev$files",
                                        {{Options::BRANCH, "other"}}),
        "a system table is aimed at one branch, but both 'dev' and 'other' were named");
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
    catalog->SetTableSchema(table->GetSchema());

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
    catalog->SetTableSchema(table->GetSchema());

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

TEST(TableTest, TestCreateBuildsTheOperator) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetFileSystem(dir->GetFileSystem());
    // A catalog write and commit read the current schema from the catalog on the main branch.
    catalog->SetTableSchema(table->GetSchema());
    catalog->SetTableLocation(dir->Str());

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

TEST(TableTest, TestLoadedSchemaAndPathAreUsedWithoutReloading) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl")));
    auto schema = table->GetSchema();
    ASSERT_OK(fs->Delete(dir->Str() + "/schema", true));
    ASSERT_EQ(schema, table->GetSchema());
    ASSERT_EQ((std::vector<std::string>{"id"}), schema->FieldNames());

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadContext> read_context,
        ReadContextBuilder(dir->Str() + "/other").SetReadFieldNames({"id"}).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         table->NewRead(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         ScanContextBuilder(dir->Str() + "/other").Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan, table->NewScan(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_TRUE(plan->Splits().empty());
}

TEST(TableTest, TestContextOptionsCannotChangeLoadedTypeOrBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));
    for (const auto& options : {std::map<std::string, std::string>{{Options::TYPE, "format-table"}},
                                std::map<std::string, std::string>{{Options::BRANCH, "dev"}}}) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                             ScanContextBuilder(dir->Str()).SetOptions(options).Finish());
        ASSERT_NOK_WITH_MSG(table->NewScan(std::move(scan_context)),
                            "contradicts the loaded table");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             ReadContextBuilder(dir->Str()).SetOptions(options).Finish());
        ASSERT_NOK_WITH_MSG(table->NewRead(std::move(read_context)),
                            "contradicts the loaded table");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                             WriteContextBuilder(dir->Str(), "user").SetOptions(options).Finish());
        ASSERT_NOK_WITH_MSG(table->NewWrite(std::move(write_context)),
                            "contradicts the loaded table");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                             CommitContextBuilder(dir->Str(), "user").SetOptions(options).Finish());
        ASSERT_NOK_WITH_MSG(table->NewCommit(std::move(commit_context)),
                            "contradicts the loaded table");
    }
}

TEST(TableTest, TestOperationOptionsDoNotMutateTheLoadedTable) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));
    auto schema = std::dynamic_pointer_cast<DataSchema>(table->GetSchema());
    ASSERT_NE(nullptr, schema);
    auto options = schema->Options();
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ScanContext> invalid_context,
        ScanContextBuilder(dir->Str()).SetOptions({{Options::BUCKET, "0"}}).Finish());
    ASSERT_NOK_WITH_MSG(table->NewScan(std::move(invalid_context)), "bucket=0");
    ASSERT_EQ(options, schema->Options());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context,
                         ScanContextBuilder(dir->Str()).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                         table->NewScan(std::move(context)));
}

TEST(TableTest, TestPathIdentifierPreservesBranch) {
    Identifier catalog_table = CatalogUtils::IdentifierFromPath("/warehouse/db.db/tbl/", "dev");
    ASSERT_EQ("db", catalog_table.GetDatabaseName());
    ASSERT_EQ("tbl$branch_dev", catalog_table.GetObjectName());
    for (const std::string path : {"tbl", "/warehouse/db.db.extra/tbl"}) {
        Identifier table = CatalogUtils::IdentifierFromPath(path, "dev");
        ASSERT_EQ(Identifier::kUnknownDatabase, table.GetDatabaseName());
        ASSERT_EQ("tbl$branch_dev", table.GetObjectName());
    }
}

TEST(TableTest, TestGlobalSystemTableRetainsCatalog) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> unique_catalog,
                         Catalog::Create(dir->Str(), {}, dir->GetFileSystem()));
    ASSERT_OK_AND_ASSIGN(bool exists, unique_catalog->TableExists(Identifier("sys", "tables")));
    ASSERT_TRUE(exists);
    ASSERT_NOK_WITH_MSG(unique_catalog->GetTable(Identifier("sys", "tables")), "std::shared_ptr");
    ASSERT_OK(unique_catalog->LoadTableSchema(Identifier("sys", "tables")));
    std::shared_ptr<Catalog> catalog(std::move(unique_catalog));
    std::weak_ptr<Catalog> weak_catalog = catalog;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         catalog->GetTable(Identifier("sys", "tables")));
    ASSERT_NE(nullptr, std::dynamic_pointer_cast<SystemTable>(table));
    ASSERT_EQ("sys.tables", table->FullName());
    ASSERT_EQ("sys.tables", table->Uuid());
    ASSERT_EQ(nullptr, std::dynamic_pointer_cast<DataSchema>(table->GetSchema()));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         ScanContextBuilder(table->GetRootPath()).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan, table->NewScan(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                         ReadContextBuilder(table->GetRootPath()).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, table->NewRead(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, read->CreateReader(plan->Splits()));
    catalog.reset();
    table.reset();
    ASSERT_FALSE(weak_catalog.expired());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
    reader->Close();
    reader.reset();
    read.reset();
    ASSERT_TRUE(weak_catalog.expired());
}

TEST(TableTest, TestMetadataSystemTablesRejectConflictingBranches) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string path = dir->Str() + "/db.db/tbl";
    ASSERT_OK(CreateManagedTable(dir->GetFileSystem(), path, Identifier("db", "tbl")));
    ASSERT_OK(
        CreateManagedTable(dir->GetFileSystem(), path, Identifier("db", "tbl", "dev"), "dev"));
    FileSystemCatalog catalog(dir->GetFileSystem(), dir->Str(), {});
    for (const std::string branch : {"main", "dev"}) {
        for (const std::string kind : {"snapshots", "schemas", "tags", "branches", "consumers"}) {
            SCOPED_TRACE(branch + ":" + kind);
            const std::string name = branch == "main" ? "tbl$" + kind : "tbl$branch_dev$" + kind;
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                                 catalog.GetTable(Identifier("db", name)));
            for (const std::string requested : {"main", "dev"}) {
                ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                                     ReadContextBuilder(path).WithBranch(requested).Finish());
                ASSERT_OK_AND_ASSIGN(
                    std::unique_ptr<ScanContext> scan_context,
                    ScanContextBuilder(path).AddOption(Options::BRANCH, requested).Finish());
                auto read = table->NewRead(std::move(read_context));
                auto scan = table->NewScan(std::move(scan_context));
                if (requested == branch) {
                    ASSERT_OK(read);
                    ASSERT_OK(scan);
                } else {
                    ASSERT_NOK_WITH_MSG(read, "context branch contradicts the loaded table");
                    ASSERT_NOK_WITH_MSG(scan, "context branch contradicts the loaded table");
                }
            }
        }
    }
}

TEST(TableTest, TestOperatorFactoryPreservesLoadedSchemaCache) {
    for (bool commit : {false, true}) {
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK(CreateManagedTable(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));
        std::shared_ptr<Table> table;
        if (commit) {
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> context,
                                 CommitContextBuilder(dir->Str(), "user").Finish());
            ASSERT_OK_AND_ASSIGN(table, TableFactory::Create(context.get()));
        } else {
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> context,
                                 WriteContextBuilder(dir->Str(), "user").Finish());
            ASSERT_OK_AND_ASSIGN(table, TableFactory::Create(context.get()));
        }
        auto managed = checked_pointer_cast<FileStoreTable>(table);
        ASSERT_OK(dir->GetFileSystem()->Delete(dir->Str() + "/schema/schema-0", false));
        // The factory already read schema-0. Repeated accesses must use its warmed cache.
        ASSERT_OK(managed->GetSchemaManager()->ReadSchema(0));
        ASSERT_OK(managed->GetSchemaManager()->ReadSchema(0));
    }
}

TEST(TableTest, TestMainBranchAliasesUseMainSchemaForAllOperatorEntryPoints) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    const std::string path = dir->Str() + "/db.db/tbl";
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> created,
                         CreateManagedTable(fs, path, Identifier("db", "tbl")));
    auto catalog =
        std::make_shared<FileSystemCatalog>(fs, dir->Str(), std::map<std::string, std::string>{});
    for (const std::string alias : {"main", "MAIN", "Main", "mAiN"}) {
        const Identifier identifier("db", "tbl$branch_" + alias);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(identifier));
        for (bool static_factory : {false, true}) {
            SCOPED_TRACE(alias + (static_factory ? ":static" : ":loaded"));
            ScanContextBuilder builder(path);
            builder.WithCatalog(catalog, identifier);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
            std::unique_ptr<TableScan> scan;
            if (static_factory) {
                ASSERT_OK_AND_ASSIGN(scan, TableScan::Create(std::move(context)));
            } else {
                ASSERT_OK_AND_ASSIGN(scan, table->NewScan(std::move(context)));
            }
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
            ASSERT_TRUE(plan->Splits().empty());

            ReadContextBuilder read_builder(path);
            read_builder.WithCatalog(catalog, identifier);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
            std::unique_ptr<TableRead> read;
            if (static_factory) {
                ASSERT_OK_AND_ASSIGN(read, TableRead::Create(std::move(read_context)));
            } else {
                ASSERT_OK_AND_ASSIGN(read, table->NewRead(std::move(read_context)));
            }

            WriteContextBuilder write_builder(path, "user");
            write_builder.WithCatalog(catalog, identifier);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                                 write_builder.Finish());
            std::unique_ptr<FileStoreWrite> write;
            if (static_factory) {
                ASSERT_OK_AND_ASSIGN(write, FileStoreWrite::Create(std::move(write_context)));
            } else {
                ASSERT_OK_AND_ASSIGN(write, table->NewWrite(std::move(write_context)));
            }
            ASSERT_OK(write->Close());

            CommitContextBuilder commit_builder(path, "user");
            commit_builder.WithCatalog(catalog, identifier);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                                 commit_builder.Finish());
            std::unique_ptr<FileStoreCommit> commit;
            if (static_factory) {
                ASSERT_OK_AND_ASSIGN(commit, FileStoreCommit::Create(std::move(commit_context)));
            } else {
                ASSERT_OK_AND_ASSIGN(commit, table->NewCommit(std::move(commit_context)));
            }
        }
    }
}

TEST(TableTest, TestExplicitContextBranchMustMatchLoadedTable) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    for (const std::string table_branch : {"main", "dev"}) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<Table> table,
            CreateManagedTable(dir->GetFileSystem(), dir->Str() + "/" + table_branch,
                               Identifier("db", "tbl", table_branch), table_branch));
        for (const std::optional<std::string>& requested :
             {std::optional<std::string>(), std::optional<std::string>("main"),
              std::optional<std::string>("dev"), std::optional<std::string>(" ")}) {
            SCOPED_TRACE(table_branch + ":" + requested.value_or("unset"));
            WriteContextBuilder write_builder(table->GetRootPath(), "user");
            ReadContextBuilder read_builder(table->GetRootPath());
            if (requested) {
                write_builder.WithBranch(*requested);
                read_builder.WithBranch(*requested);
            }
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                                 write_builder.Finish());
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
            auto write = table->NewWrite(std::move(write_context));
            auto read = table->NewRead(std::move(read_context));
            if (requested && BranchManager::NormalizeBranch(*requested) != table_branch) {
                ASSERT_NOK_WITH_MSG(write, "context branch contradicts the loaded table");
                ASSERT_NOK_WITH_MSG(read, "context branch contradicts the loaded table");
            } else {
                ASSERT_OK(write);
                ASSERT_OK(read);
            }
        }
    }
}

TEST(TableTest, TestPreloadedSchemaBranchMatchesScanAndRead) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    const std::string path = dir->Str() + "/table";
    ASSERT_TRUE(TestUtil::CopyDirectory(
        std::filesystem::path(
            GetDataDir() + "/parquet/append_table_with_rt_branch.db/append_table_with_rt_branch"),
        std::filesystem::path(path)));
    SnapshotManager main_snapshots(fs, path);
    ASSERT_OK_AND_ASSIGN(Snapshot original, main_snapshots.LoadSnapshot(1));
    rapidjson::Document snapshot_json;
    snapshot_json.SetObject();
    snapshot_json.CopyFrom(original.ToJson(&snapshot_json.GetAllocator()),
                           snapshot_json.GetAllocator());
    snapshot_json[Snapshot::FIELD_ID].SetInt64(2);
    std::string json;
    ASSERT_TRUE(RapidJsonUtil::ToJson(snapshot_json, &json));
    ASSERT_OK(fs->AtomicStore(main_snapshots.SnapshotPath(2), json));
    ASSERT_OK(main_snapshots.CommitLatestHint(2));
    SchemaManager schemas(fs, path, "rt");
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> schema, schemas.Latest());
    ASSERT_TRUE(schema);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> preloaded,
                         schema.value()->MergeOptions({{Options::BRANCH, "rt"}}));
    ASSERT_OK_AND_ASSIGN(std::string schema_json, preloaded->ToJsonString());
    for (const auto& overrides : {std::map<std::string, std::string>{},
                                  std::map<std::string, std::string>{{Options::BRANCH, "rt"}},
                                  std::map<std::string, std::string>{{Options::BRANCH, "main"}}}) {
        const std::string branch = overrides.empty() ? "rt" : overrides.at(Options::BRANCH);
        SCOPED_TRACE(branch);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ScanContext> scan_context,
            ScanContextBuilder(path).SetTableSchema(schema_json).SetOptions(overrides).Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                             TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_EQ(branch == "rt" ? 1 : 2, plan->SnapshotId());
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ReadContext> read_context,
            ReadContextBuilder(path).SetTableSchema(schema_json).SetOptions(overrides).Finish());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> loaded,
                             TableFactory::Create(read_context.get()));
        ASSERT_EQ(branch, std::dynamic_pointer_cast<FileStoreTable>(loaded)->CurrentBranch());
        // Explicit non-main loads keep using that branch's persisted schema.
        const auto& expected_options = overrides.count(Options::BRANCH) != 0 && branch == "rt"
                                           ? schema.value()->Options()
                                           : preloaded->Options();
        ASSERT_EQ(expected_options,
                  std::dynamic_pointer_cast<TableSchema>(loaded->GetSchema())->Options());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                             loaded->NewRead(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                             read->CreateReader(plan->Splits()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> rows,
                             ReadResultCollector::CollectResult(std::move(reader)));
        SnapshotManager snapshots(fs, path, branch);
        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, snapshots.LatestSnapshot());
        ASSERT_TRUE(snapshot);
        ASSERT_EQ(snapshot->TotalRecordCount(), rows->length());
    }
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadContext> explicit_main,
        ReadContextBuilder(path).SetTableSchema(schema_json).WithBranch("main").Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> loaded, TableFactory::Create(explicit_main.get()));
    ASSERT_EQ("main", std::dynamic_pointer_cast<FileStoreTable>(loaded)->CurrentBranch());
}

TEST(TableTest, TestResolvedStoreUsesOptionsBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::BRANCH, "dev"}}, dir->GetFileSystem()));
    auto store = std::make_unique<FileStore>(dir->Str(), options);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStoreTable> loaded,
        FileStoreTable::Create(std::move(store),
                               std::dynamic_pointer_cast<TableSchema>(table->GetSchema()),
                               Identifier("db", "tbl", "dev"), ""));
    ASSERT_EQ("dev", loaded->CurrentBranch());
    ASSERT_EQ("dev", loaded->GetSnapshotManager()->Branch());
}

TEST(TableTest, TestLoadedTableReadRetainsOperationSchemeMap) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string path = dir->Str() + "/db.db/tbl";
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> created,
                         CreateManagedTable(dir->GetFileSystem(), path, Identifier("db", "tbl")));
    FileSystemCatalog catalog(dir->GetFileSystem(), dir->Str(), {});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog.GetTable(Identifier("db", "tbl")));
    const std::map<std::string, std::string> scheme_map = {{"custom", "local"}};
    for (bool direct : {false, true}) {
        SCOPED_TRACE(direct);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ReadContext> context,
            ReadContextBuilder(path).WithFileSystemSchemeToIdentifierMap(scheme_map).Finish());
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<TableRead> read,
            direct ? table->NewRead(std::move(context)) : TableRead::Create(std::move(context)));
        auto* append_read = dynamic_cast<AppendOnlyTableRead*>(read.get());
        ASSERT_NE(nullptr, append_read);
        ASSERT_EQ(
            scheme_map,
            append_read->context_->CreateDependencyCatalogContext()->fs_scheme_to_identifier_map);
        if (direct) {
            ASSERT_EQ(table->GetFileSystem(),
                      append_read->context_->GetCoreOptions().GetFileSystem());
        }
    }
}

TEST(TableTest, TestReadMergesSchemeMapsForMainAndFallback) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> main,
                         CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl")));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> dev,
                         CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl", "dev"), "dev"));
    const std::map<std::string, std::string> table_map = {{"base", "local"}, {"shared", "mock_fs"}};
    const std::map<std::string, std::string> read_map = {{"custom", "local"}, {"shared", "local"}};
    const std::map<std::string, std::string> expected = {
        {"base", "local"}, {"custom", "local"}, {"shared", "local"}};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(fs, dir->Str(),
                               std::dynamic_pointer_cast<TableSchema>(main->GetSchema()),
                               Identifier("db", "tbl"), "", "main", table_map));
    for (bool fallback : {false, true}) {
        SCOPED_TRACE(fallback);
        ReadContextBuilder builder("unused_path");
        builder.WithFileSystemSchemeToIdentifierMap(read_map);
        if (fallback) {
            builder.AddOption(Options::SCAN_FALLBACK_BRANCH, "dev");
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, table->NewRead(std::move(context)));
        std::vector<TableRead*> reads;
        if (fallback) {
            auto* fallback_read = dynamic_cast<FallbackTableRead*>(read.get());
            ASSERT_NE(nullptr, fallback_read);
            reads = {fallback_read->main_table_.get(), fallback_read->fallback_table_.get()};
        } else {
            reads = {read.get()};
        }
        for (TableRead* data_read : reads) {
            auto* append_read = dynamic_cast<AppendOnlyTableRead*>(data_read);
            ASSERT_NE(nullptr, append_read);
            const auto& internal = append_read->context_;
            ASSERT_EQ(expected,
                      internal->CreateDependencyCatalogContext()->fs_scheme_to_identifier_map);
            ASSERT_EQ(fs, internal->GetCoreOptions().GetFileSystem());
            ASSERT_EQ(dir->Str(), internal->GetPath());
        }
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                         ReadContextBuilder("unused_path").Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, table->NewRead(std::move(context)));
    auto* append_read = dynamic_cast<AppendOnlyTableRead*>(read.get());
    ASSERT_NE(nullptr, append_read);
    ASSERT_EQ(table_map,
              append_read->context_->CreateDependencyCatalogContext()->fs_scheme_to_identifier_map);
}

TEST(TableTest, TestSystemTablePathRetainsSchemeMapForDependencyReads) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] std::shared_ptr<Table> data_table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));
    const std::map<std::string, std::string> scheme_map = {{"file", "local"}};
    for (bool reload_context : {false, true}) {
        SCOPED_TRACE(reload_context);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                             ReadContextBuilder(dir->Str() + "$ro")
                                 .WithFileSystemSchemeToIdentifierMap(scheme_map)
                                 .Finish());
        std::unique_ptr<TableRead> read;
        if (reload_context) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, TableFactory::Create(context.get()));
            ASSERT_OK_AND_ASSIGN(context, ReadContextBuilder(dir->Str()).Finish());
            ASSERT_OK_AND_ASSIGN(read, table->NewRead(std::move(context)));
        } else {
            ASSERT_OK_AND_ASSIGN(read, TableRead::Create(std::move(context)));
        }
        auto* append_read = dynamic_cast<AppendOnlyTableRead*>(read.get());
        ASSERT_NE(nullptr, append_read);
        const auto& internal = append_read->context_;
        ASSERT_EQ(scheme_map, internal->GetFileSystemSchemeToIdentifierMap());
        ASSERT_EQ(scheme_map,
                  internal->CreateDependencyCatalogContext()->fs_scheme_to_identifier_map);
    }
}

TEST(TableTest, TestSystemTableReadMergesSchemeMapsWithoutReplacingFileSystem) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> data_table,
                         CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl")));
    const std::map<std::string, std::string> table_map = {{"base", "local"}};
    const std::map<std::string, std::string> read_map = {{"file", "local"}};
    const std::map<std::string, std::string> expected = {{"base", "local"}, {"file", "local"}};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<SystemTable> table,
        SystemTableLoader::Load("ro", fs, dir->Str(),
                                std::dynamic_pointer_cast<TableSchema>(data_table->GetSchema()), {},
                                "tbl", table_map));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ReadContext> context,
        ReadContextBuilder("unused_path").WithFileSystemSchemeToIdentifierMap(read_map).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, table->NewRead(std::move(context)));
    auto* append_read = dynamic_cast<AppendOnlyTableRead*>(read.get());
    ASSERT_NE(nullptr, append_read);
    const auto& internal = append_read->context_;
    ASSERT_EQ(fs, internal->GetCoreOptions().GetFileSystem());
    ASSERT_EQ(dir->Str(), internal->GetPath());
    ASSERT_EQ(expected, internal->CreateDependencyCatalogContext()->fs_scheme_to_identifier_map);
}

TEST(TableTest, TestRealtimeFallbackOnlyAllowsMainBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    auto arrow_schema = arrow::schema({arrow::field("id", arrow::int32(), false)});
    const std::map<std::string, std::string> table_options = {{Options::BUCKET, "1"},
                                                              {Options::BUCKET_KEY, "id"}};
    SchemaManager main_schemas(fs, dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> main_schema,
                         main_schemas.CreateTable(arrow_schema, {}, {}, table_options));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(fs, dir->Str(), std::shared_ptr<TableSchema>(std::move(main_schema)),
                               Identifier("db", "tbl"), "", "main"));
    SchemaManager dev_schemas(fs, dir->Str(), "dev");
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> dev_schema,
                         dev_schemas.CreateTable(arrow_schema, {}, {}, table_options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime, RealtimeContext::Create());
    for (bool realtime_enabled : {false, true}) {
        for (const std::string fallback : {"main", "dev"}) {
            SCOPED_TRACE(fallback + (realtime_enabled ? ":realtime" : ":disk"));
            ReadContextBuilder builder(dir->Str());
            builder.SetOptions({{Options::SCAN_FALLBACK_BRANCH, fallback}});
            if (realtime_enabled) {
                builder.WithRealtimeContext(realtime).AddOption(Options::REALTIME_ENABLED, "true");
            }
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
            auto read = table->NewRead(std::move(context));
            if (realtime_enabled && fallback != "main") {
                ASSERT_NOK_WITH_MSG(read, "real-time operations only support the main branch");
            } else {
                ASSERT_OK(read);
            }
        }
    }
}

TEST(TableTest, SystemTablePathUsesEffectiveBranchSchema) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    auto main_schema = arrow::schema({arrow::field("id", arrow::int32())});
    auto dev_schema =
        arrow::schema({arrow::field("id", arrow::int32()), arrow::field("extra", arrow::int32())});
    SchemaManager main_manager(fs, dir->Str(), "main");
    SchemaManager dev_manager(fs, dir->Str(), "dev");
    ASSERT_OK(main_manager.CreateTable(main_schema, {}, {}, {}));
    ASSERT_OK(dev_manager.CreateTable(dev_schema, {}, {}, {}));
    for (bool path_branch : {false, true}) {
        SCOPED_TRACE(path_branch);
        const std::string path = dir->Str() + (path_branch ? "$branch_dev$ro" : "$ro");
        const std::map<std::string, std::string> options =
            path_branch ? std::map<std::string, std::string>()
                        : std::map<std::string, std::string>{{Options::BRANCH, "dev"}};
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ReadContext> context,
            ReadContextBuilder(path).SetOptions(options).SetReadFieldNames({"extra"}).Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                             TableRead::Create(std::move(context)));
        auto* append_read = dynamic_cast<AppendOnlyTableRead*>(read.get());
        ASSERT_NE(nullptr, append_read);
        ASSERT_EQ("dev", append_read->context_->GetCoreOptions().GetBranch());
        ASSERT_EQ(dev_schema->field_names(), append_read->context_->GetTableSchema()->FieldNames());
        ASSERT_EQ((std::vector<std::string>{"extra"}),
                  append_read->context_->GetReadSchema()->field_names());
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ScanContext> scan_context,
            ScanContextBuilder(path)
                .SetOptions(options)
                .SetPredicate(PredicateBuilder::GreaterThan(1, "extra", FieldType::INT, Literal(0)))
                .Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                             TableScan::Create(std::move(scan_context)));
        ASSERT_OK(scan->CreatePlan());
    }
    ASSERT_NOK(SystemTableLoader::LoadFromPath(fs, dir->Str() + "$branch_dev$ro",
                                               {{Options::BRANCH, "main"}}, {}));
}

TEST(TableTest, SystemTableMainAliasReturnsExistingSnapshots) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    const std::string path = dir->Str() + "/db.db/tbl";
    ASSERT_TRUE(TestUtil::CopyDirectory(
        std::filesystem::path(
            GetDataDir() + "/parquet/append_table_with_rt_branch.db/append_table_with_rt_branch"),
        std::filesystem::path(path)));
    FileSystemCatalog catalog(fs, dir->Str(), {});
    std::shared_ptr<arrow::ChunkedArray> expected;
    for (const std::string& name :
         std::vector<std::string>{"tbl$snapshots", "tbl$branch_MAIN$snapshots"}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                             catalog.GetTable(Identifier("db", name)));
        ASSERT_EQ("db." + name, table->FullName());
        ASSERT_EQ(table->FullName(), table->Uuid());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                             ScanContextBuilder(table->GetRootPath()).Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                             table->NewScan(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             ReadContextBuilder(table->GetRootPath()).Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                             table->NewRead(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                             read->CreateReader(plan->Splits()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> rows,
                             ReadResultCollector::CollectResult(std::move(reader)));
        ASSERT_GT(rows->length(), 0);
        if (expected) {
            ASSERT_TRUE(expected->Equals(rows));
        } else {
            expected = rows;
        }
    }
}

TEST(TableTest, StaticFallbackReadUsesSuppliedMainSchema) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> main,
                         CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl"), "main"));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> dev,
                         CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl", "dev"), "dev"));
    ASSERT_OK_AND_ASSIGN(std::string main_json, main->GetSchema()->GetJsonSchema());
    ASSERT_OK(fs->Delete(dir->Str() + "/schema", true));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                         ReadContextBuilder(dir->Str())
                             .WithBranch("dev")
                             .AddOption(Options::SCAN_FALLBACK_BRANCH, "main")
                             .SetTableSchema(main_json)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, TableRead::Create(std::move(context)));
    auto* fallback = dynamic_cast<FallbackTableRead*>(read.get());
    ASSERT_NE(nullptr, fallback);
    auto* fallback_read = dynamic_cast<AppendOnlyTableRead*>(fallback->fallback_table_.get());
    ASSERT_NE(nullptr, fallback_read);
    ASSERT_EQ(main->GetSchema()->FieldNames(),
              fallback_read->context_->GetTableSchema()->FieldNames());
    ASSERT_EQ("main", fallback_read->context_->GetCoreOptions().GetBranch());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> missing_schema,
                         ReadContextBuilder(dir->Str())
                             .WithBranch("dev")
                             .AddOption(Options::SCAN_FALLBACK_BRANCH, "main")
                             .Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(missing_schema)), "fallback schema not found");
}

TEST(TableTest, LoadedFallbackReadIgnoresForeignContextSchema) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    ASSERT_OK(CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl"), "main"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> dev,
                         CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl", "dev"), "dev"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                         ReadContextBuilder(dir->Str() + "/other")
                             .WithBranch("dev")
                             .AddOption(Options::SCAN_FALLBACK_BRANCH, "main")
                             .SetTableSchema("invalid json")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, dev->NewRead(std::move(context)));
    ASSERT_NE(nullptr, dynamic_cast<FallbackTableRead*>(read.get()));
}

TEST(TableTest, CatalogMetadataDoesNotRequireDataFormatPlugin) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    const std::string path = dir->Str() + "/db.db/tbl";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> schema,
        TableSchema::Create(0, arrow::schema({arrow::field("id", arrow::int32())}), {}, {},
                            {{Options::FILE_FORMAT, "unavailable_format_for_metadata_test"}}));
    ASSERT_OK_AND_ASSIGN(std::string json, schema->GetJsonSchema());
    ASSERT_OK(fs->Mkdirs(path + "/schema"));
    ASSERT_OK(fs->AtomicStore(path + "/schema/schema-0", json));
    FileSystemCatalog catalog(fs, dir->Str(), {});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog.GetTable(Identifier("db", "tbl")));
    ASSERT_EQ(path, table->GetRootPath());
    ASSERT_EQ((std::vector<std::string>{"id"}), table->GetSchema()->FieldNames());
    ASSERT_EQ("db.tbl", table->Uuid());
    auto managed = std::dynamic_pointer_cast<FileStoreTable>(table);
    ASSERT_NE(nullptr, managed);
    ASSERT_OK(managed->GetSchemaManager()->Latest());
    ASSERT_OK(managed->GetSnapshotManager()->LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, ReadContextBuilder(path).Finish());
    ASSERT_NOK_WITH_MSG(table->NewRead(std::move(context)), "FileFormatFactory");
}

TEST(TableTest, SystemTableFullNamesDistinguishDatabases) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    FileSystemCatalog catalog(fs, dir->Str(), {});
    for (const std::string& db : std::vector<std::string>{"db1", "db2"}) {
        ASSERT_OK(CreateManagedTable(fs, dir->Str() + "/" + db + ".db/tbl", Identifier(db, "tbl"),
                                     "main"));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                             catalog.GetTable(Identifier(db, "tbl$options")));
        ASSERT_EQ("tbl$options", table->Name());
        ASSERT_EQ(db + ".tbl$options", table->FullName());
        ASSERT_EQ(table->FullName(), table->Uuid());
    }
}

}  // namespace paimon::test
