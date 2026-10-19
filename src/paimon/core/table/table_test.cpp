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
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/catalog/file_system_catalog.h"
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
    return std::make_shared<Table>(
        file_system, path,
        checked_pointer_cast<Schema>(std::shared_ptr<TableSchema>(std::move(created))), identifier);
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
        // A catalog that loaded the schema itself calls the constructor directly, which a
        // rest catalog is shaped like. The alias resolves to the main branch as well.
        Table table(fs, dir->Str(), schema, main_identifier);
        ASSERT_OK_AND_ASSIGN(std::string branch, table.CurrentBranch());
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

TEST(TableTest, TestGetTableRejectsATableTypeThatIsNotAManagedTable) {
    // A `Table` describes a managed paimon table. Handing back another type as one would promise
    // snapshots and manifests it never had, so the failure belongs at the call that opens the
    // table rather than at the first read.
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
    ASSERT_NOK_WITH_MSG(catalog.GetTable(Identifier("db", "format")), "Cannot open format table");

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
