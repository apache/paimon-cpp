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
#include "gtest/gtest.h"
#include "paimon/commit_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/schema/schema.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/mock/mock_catalog.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

namespace {

// Creates a managed table directory and opens it as a `Table` under `identifier`. The
// schema goes to `branch`'s directory, which a branch identifier reads it back from.
Result<std::shared_ptr<Table>> CreateManagedTable(const std::shared_ptr<FileSystem>& file_system,
                                                  const std::string& path,
                                                  const Identifier& identifier,
                                                  const std::string& branch = "main") {
    SchemaManager schema_manager(file_system, path, branch);
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false)});
    PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] std::unique_ptr<TableSchema> created,
                           schema_manager.CreateTable(schema, /*partition_keys=*/{},
                                                      /*primary_keys=*/{}, /*options=*/{}));
    return Table::Create(file_system, path, identifier);
}

}  // namespace

TEST(TableTest, TestCreateWithUnknownDatabase) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
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

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         Table::Create(dir->GetFileSystem(), dir->Str(),
                                       Identifier(Identifier::kUnknownDatabase, "tbl1")));

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

TEST(TableTest, TestCreateWithBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    SchemaManager schema_manager(fs, dir->Str());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> main_schema,
                         schema_manager.CreateTable(
                             arrow::schema({arrow::field("main_c0", arrow::int32())}), {}, {}, {}));
    SchemaManager branch_schema_manager(fs, dir->Str(), "b1");
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> branch_schema,
                         branch_schema_manager.CreateTable(
                             arrow::schema({arrow::field("b1_c0", arrow::int32())}), {}, {}, {}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         Table::Create(fs, dir->Str(), Identifier("db1", "tbl1$branch_b1")));
    ASSERT_EQ("tbl1$branch_b1", table->Name());
    ASSERT_EQ((std::vector<std::string>{"b1_c0"}), table->LatestSchema()->FieldNames());
    for (const auto& main_identifier :
         {Identifier("db1", "tbl1$branch_main"), Identifier("db1", "tbl1$branch_MAIN")}) {
        ASSERT_OK_AND_ASSIGN(table, Table::Create(fs, dir->Str(), main_identifier));
        ASSERT_EQ((std::vector<std::string>{"main_c0"}), table->LatestSchema()->FieldNames());
    }

    Status missing_branch =
        Table::Create(fs, dir->Str(), Identifier("db1", "tbl1$branch_missing")).status();
    ASSERT_TRUE(missing_branch.IsNotExist()) << missing_branch.ToString();
    ASSERT_NOK_WITH_MSG(Table::Create(fs, dir->Str(), Identifier("db1", "tbl1$branch_../outside")),
                        "branch");
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
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> schema,
                         schema_manager.CreateTable(
                             arrow::schema({arrow::field("c0", arrow::int32())}), {}, {}, {}));

    for (const auto& main_identifier :
         {Identifier("db", "tbl"), Identifier("db", "tbl$branch_main"),
          Identifier("db", "tbl$branch_MAIN")}) {
        SCOPED_TRACE(main_identifier.ToString());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                             Table::Create(fs, dir->Str(), main_identifier));
        ASSERT_OK_AND_ASSIGN(std::string branch, table->CurrentBranch());
        EXPECT_EQ("main", branch);
    }

    // A catalog that loaded the schema itself calls the constructor directly, which a
    // rest catalog is shaped like. The alias resolves to the main branch as well.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> plain,
                         Table::Create(fs, dir->Str(), Identifier("db", "tbl")));
    Table alias_table(fs, dir->Str(), plain->LatestSchema(), Identifier("db", "tbl$branch_MAIN"));
    ASSERT_OK_AND_ASSIGN(std::string alias_branch, alias_table.CurrentBranch());
    EXPECT_EQ("main", alias_branch);
}

TEST(TableTest, TestCurrentBranch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    // A table whose identifier names no branch is a table of the main branch.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Table> main_table,
        CreateManagedTable(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));
    ASSERT_OK_AND_ASSIGN(std::string branch, main_table->CurrentBranch());
    EXPECT_EQ(branch, "main");

    // The identifier a table was opened with names the branch it is for.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> branch_table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str() + "/branch_tbl",
                                            Identifier("db", "tbl", "dev"), /*branch=*/"dev"));
    ASSERT_OK_AND_ASSIGN(branch, branch_table->CurrentBranch());
    EXPECT_EQ(branch, "dev");
}

TEST(TableTest, TestCatalogOpenedTableServesThePerTableFileSystem) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableSchema(table->LatestSchema());

    // A catalog that hands out no per-table file system opens the table without one, and one
    // that does opens it with the file system issued for that table.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> opened, catalog->GetTable(identifier));
    EXPECT_EQ(opened->GetFileSystem(), nullptr);

    catalog->SetTableFileSystem(dir->GetFileSystem());
    ASSERT_OK_AND_ASSIGN(opened, catalog->GetTable(identifier));
    EXPECT_EQ(opened->GetFileSystem(), dir->GetFileSystem());
    // This mock serves no table location, so the table it opens carries none.
    EXPECT_EQ(opened->GetRootPath(), "");
}

TEST(TableTest, TestCreateRejectsATableTypeThatIsNotAManagedTable) {
    // A `Table` describes a managed paimon table. Handing back another type as one would promise
    // snapshots and manifests it never had, so the failure belongs at the call that opens the
    // table rather than at the first read. This is the one place every `Table` built from a table
    // directory passes through, catalogs included.
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                                 arrow::field("dt", arrow::utf8())});

    SchemaManager format_schema_manager(dir->GetFileSystem(), dir->Str() + "/format");
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] std::unique_ptr<TableSchema> format_schema,
        format_schema_manager.CreateTable(schema, /*partition_keys=*/{"dt"},
                                          /*primary_keys=*/{},
                                          {{"type", "format-table"}, {"file.format", "parquet"}}));
    ASSERT_NOK_WITH_MSG(
        Table::Create(dir->GetFileSystem(), dir->Str() + "/format", Identifier("db", "fmt")),
        "Cannot open format table");

    // A managed table with no `type` option at all is what a `Table` is for, and still opens.
    SchemaManager managed_schema_manager(dir->GetFileSystem(), dir->Str() + "/managed");
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> managed_schema,
                         managed_schema_manager.CreateTable(schema, /*partition_keys=*/{"dt"},
                                                            /*primary_keys=*/{}, {}));
    ASSERT_OK(
        Table::Create(dir->GetFileSystem(), dir->Str() + "/managed", Identifier("db", "managed")));
}

TEST(TableTest, TestCreateFailedWithNonExistSchema) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    SchemaManager schema_manager(fs, dir->Str());
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
    // check create table failed
    ASSERT_NOK_WITH_MSG(
        Table::Create(fs, dir->Str(), Identifier(Identifier::kUnknownDatabase, "tbl1")),
        "load table schema for Identifier{database='unknown', object='tbl1'} failed");
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
