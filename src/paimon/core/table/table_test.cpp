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

#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/catalog/table_load_options.h"
#include "paimon/commit_context.h"
#include "paimon/common/catalog/catalog_context.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/catalog/file_system_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/append_only_file_store_table.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/primary_key_file_store_table.h"
#include "paimon/core/table/source/append_only_table_read.h"
#include "paimon/core/table/source/fallback_table_read.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/table/table_factory.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/scan_context.h"
#include "paimon/schema/schema.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/mock/mock_catalog.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/read_result_collector.h"
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

TEST(TableTest, TestScanWithMainBranchAlias) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    const std::string table_path = dir->Str() + "/db.db/tbl";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> main_table,
                         CreateManagedTable(fs, table_path, Identifier("db", "tbl")));
    auto catalog =
        std::make_shared<FileSystemCatalog>(fs, dir->Str(), std::map<std::string, std::string>());
    for (const std::string alias : {"main", "MAIN", "Main", "mAiN"}) {
        SCOPED_TRACE(alias);
        const Identifier identifier("db", "tbl$branch_" + alias);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(identifier));
        for (bool explicit_main : {false, true}) {
            SCOPED_TRACE(explicit_main);
            TableLoadOptions load_options{table_path};
            load_options.catalog = catalog;
            load_options.identifier.emplace(identifier);
            std::map<std::string, std::string> dynamic_options;
            if (explicit_main) {
                dynamic_options[Options::BRANCH] = "main";
            }
            ScanContextBuilder builder;
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> static_context, builder.Finish());
            ASSERT_OK_AND_ASSIGN(
                std::unique_ptr<TableScan> static_scan,
                TableScan::Create(load_options, std::move(static_context), dynamic_options));
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> static_plan, static_scan->CreatePlan());
            ASSERT_TRUE(static_plan->Splits().empty());

            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> loaded_context, builder.Finish());
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> loaded_scan,
                                 table->NewScan(std::move(loaded_context), dynamic_options));
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> loaded_plan, loaded_scan->CreatePlan());
            ASSERT_TRUE(loaded_plan->Splits().empty());
        }
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
        TableLoadOptions context_builder_load_options{managed_dir + "$options"};
        std::map<std::string, std::string> context_builder_options;
        ReadContextBuilder context_builder;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                             context_builder.EnablePreciseFileIndexEvaluate(true).Finish());
        auto read = direct ? system_table->NewRead(std::move(context), context_builder_options)
                           : TableRead::Create(context_builder_load_options, std::move(context),
                                               context_builder_options);
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

    std::map<std::string, std::string> ctx_builder_options;
    ScanContextBuilder ctx_builder;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> ctx, ctx_builder.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                         table->NewScan(std::move(ctx), ctx_builder_options));
}

TEST(TableTest, TestNewReadCreatesATableRead) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const Identifier identifier("db", "tbl");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         CreateManagedTable(dir->GetFileSystem(), dir->Str(), identifier));

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetFileSystem(dir->GetFileSystem());

    std::map<std::string, std::string> ctx_builder_18211_options;
    ReadContextBuilder ctx_builder_18211;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> ctx, ctx_builder_18211.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         table->NewRead(std::move(ctx), ctx_builder_18211_options));
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

    std::map<std::string, std::string> ctx_builder_18974_options;
    WriteContextBuilder ctx_builder_18974("commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> ctx, ctx_builder_18974.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<FileStoreWrite> write,
                         table->NewWrite(std::move(ctx), ctx_builder_18974_options));
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

    std::map<std::string, std::string> ctx_builder_19886_options;
    CommitContextBuilder ctx_builder_19886("commit-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> ctx, ctx_builder_19886.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<FileStoreCommit> commit,
                         table->NewCommit(std::move(ctx), ctx_builder_19886_options));
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

    // `TableScan::Create()` and friends build the operator straight from the context, which
    // carries the catalog and identifier it resolves the table through.
    TableLoadOptions scan_ctx_builder_load_options{dir->Str()};
    std::map<std::string, std::string> scan_ctx_builder_options;
    ScanContextBuilder scan_ctx_builder;
    scan_ctx_builder_load_options.catalog = catalog;
    scan_ctx_builder_load_options.identifier.emplace(identifier);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_ctx, scan_ctx_builder.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                         TableScan::Create(scan_ctx_builder_load_options, std::move(scan_ctx),
                                           scan_ctx_builder_options));

    TableLoadOptions read_ctx_builder_load_options{dir->Str()};
    std::map<std::string, std::string> read_ctx_builder_options;
    ReadContextBuilder read_ctx_builder;
    read_ctx_builder_load_options.catalog = catalog;
    read_ctx_builder_load_options.identifier.emplace(identifier);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_ctx, read_ctx_builder.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         TableRead::Create(read_ctx_builder_load_options, std::move(read_ctx),
                                           read_ctx_builder_options));

    TableLoadOptions write_ctx_builder_load_options{dir->Str()};
    std::map<std::string, std::string> write_ctx_builder_options;
    WriteContextBuilder write_ctx_builder("commit-user");
    write_ctx_builder_load_options.catalog = catalog;
    write_ctx_builder_load_options.identifier.emplace(identifier);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_ctx, write_ctx_builder.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<FileStoreWrite> write,
                         FileStoreWrite::Create(write_ctx_builder_load_options,
                                                std::move(write_ctx), write_ctx_builder_options));

    TableLoadOptions commit_ctx_builder_load_options{dir->Str()};
    std::map<std::string, std::string> commit_ctx_builder_options;
    CommitContextBuilder commit_ctx_builder("commit-user");
    commit_ctx_builder_load_options.catalog = catalog;
    commit_ctx_builder_load_options.identifier.emplace(identifier);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_ctx, commit_ctx_builder.Finish());
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] std::unique_ptr<FileStoreCommit> commit,
        FileStoreCommit::Create(commit_ctx_builder_load_options, std::move(commit_ctx),
                                commit_ctx_builder_options));
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

    std::map<std::string, std::string> read_context_builder_options;
    ReadContextBuilder read_context_builder;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                         read_context_builder.SetReadFieldNames({"id"}).Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         table->NewRead(std::move(read_context), read_context_builder_options));
    std::map<std::string, std::string> scan_context_builder_options;
    ScanContextBuilder scan_context_builder;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         table->NewScan(std::move(scan_context), scan_context_builder_options));
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
        std::map<std::string, std::string> scan_context_builder_25016_options;
        ScanContextBuilder scan_context_builder_25016;
        scan_context_builder_25016_options = options;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                             scan_context_builder_25016.Finish());
        ASSERT_NOK_WITH_MSG(
            table->NewScan(std::move(scan_context), scan_context_builder_25016_options),
            "contradicts the loaded table");
        std::map<std::string, std::string> read_context_builder_25926_options;
        ReadContextBuilder read_context_builder_25926;
        read_context_builder_25926_options = options;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder_25926.Finish());
        ASSERT_NOK_WITH_MSG(
            table->NewRead(std::move(read_context), read_context_builder_25926_options),
            "contradicts the loaded table");
        std::map<std::string, std::string> write_context_builder_options;
        WriteContextBuilder write_context_builder("user");
        write_context_builder_options = options;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                             write_context_builder.Finish());
        ASSERT_NOK_WITH_MSG(
            table->NewWrite(std::move(write_context), write_context_builder_options),
            "contradicts the loaded table");
        std::map<std::string, std::string> commit_context_builder_options;
        CommitContextBuilder commit_context_builder("user");
        commit_context_builder_options = options;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                             commit_context_builder.Finish());
        ASSERT_NOK_WITH_MSG(
            table->NewCommit(std::move(commit_context), commit_context_builder_options),
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
    std::map<std::string, std::string> invalid_context_builder_options;
    ScanContextBuilder invalid_context_builder;
    invalid_context_builder_options = {{Options::BUCKET, "0"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> invalid_context,
                         invalid_context_builder.Finish());
    ASSERT_NOK_WITH_MSG(table->NewScan(std::move(invalid_context), invalid_context_builder_options),
                        "bucket=0");
    ASSERT_EQ(options, schema->Options());
    std::map<std::string, std::string> context_builder_27781_options;
    ScanContextBuilder context_builder_27781;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, context_builder_27781.Finish());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableScan> scan,
                         table->NewScan(std::move(context), context_builder_27781_options));
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
    std::shared_ptr<Catalog> catalog(std::move(unique_catalog));
    std::weak_ptr<Catalog> weak_catalog = catalog;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                         catalog->GetTable(Identifier("sys", "tables")));
    ASSERT_NE(nullptr, std::dynamic_pointer_cast<SystemTable>(table));
    ASSERT_EQ(nullptr, std::dynamic_pointer_cast<DataSchema>(table->GetSchema()));
    std::map<std::string, std::string> scan_context_builder_28121_options;
    ScanContextBuilder scan_context_builder_28121;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                         scan_context_builder_28121.Finish());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableScan> scan,
        table->NewScan(std::move(scan_context), scan_context_builder_28121_options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    std::map<std::string, std::string> read_context_builder_29694_options;
    ReadContextBuilder read_context_builder_29694;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                         read_context_builder_29694.Finish());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableRead> read,
        table->NewRead(std::move(read_context), read_context_builder_29694_options));
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
            std::map<std::string, std::string> write_builder_options;
            WriteContextBuilder write_builder("user");
            std::map<std::string, std::string> read_builder_options;
            ReadContextBuilder read_builder;
            if (requested) {
                write_builder_options["branch"] = *requested;

                read_builder_options["branch"] = *requested;
            }
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                                 write_builder.Finish());
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
            auto write = table->NewWrite(std::move(write_context), write_builder_options);
            auto read = table->NewRead(std::move(read_context), read_builder_options);
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
        TableLoadOptions scan_context_builder_30072_load_options{path};
        std::map<std::string, std::string> scan_context_builder_30072_options;
        ScanContextBuilder scan_context_builder_30072;
        scan_context_builder_30072_load_options.table_schema_json = schema_json;
        scan_context_builder_30072_options = overrides;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                             scan_context_builder_30072.Finish());
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<TableScan> scan,
            TableScan::Create(scan_context_builder_30072_load_options, std::move(scan_context),
                              scan_context_builder_30072_options));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_EQ(branch == "rt" ? 1 : 2, plan->SnapshotId());
        TableLoadOptions read_context_builder_34363_load_options{path};
        std::map<std::string, std::string> read_context_builder_34363_options;
        ReadContextBuilder read_context_builder_34363;
        read_context_builder_34363_load_options.table_schema_json = schema_json;
        read_context_builder_34363_options = overrides;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder_34363.Finish());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> loaded,
                             TableFactory::Create(read_context_builder_34363_load_options,
                                                  read_context_builder_34363_options, "read"));
        ASSERT_EQ(branch, std::dynamic_pointer_cast<FileStoreTable>(loaded)->CurrentBranch());
        // Explicit non-main loads keep using that branch's persisted schema.
        const auto& expected_options = overrides.count(Options::BRANCH) != 0 && branch == "rt"
                                           ? schema.value()->Options()
                                           : preloaded->Options();
        ASSERT_EQ(expected_options,
                  std::dynamic_pointer_cast<TableSchema>(loaded->GetSchema())->Options());
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<TableRead> read,
            loaded->NewRead(std::move(read_context), read_context_builder_34363_options));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                             read->CreateReader(plan->Splits()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> rows,
                             ReadResultCollector::CollectResult(std::move(reader)));
        SnapshotManager snapshots(fs, path, branch);
        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, snapshots.LatestSnapshot());
        ASSERT_TRUE(snapshot);
        ASSERT_EQ(snapshot->TotalRecordCount(), rows->length());
    }
    TableLoadOptions explicit_main_builder_load_options{path};
    std::map<std::string, std::string> explicit_main_builder_options;
    ReadContextBuilder explicit_main_builder;
    explicit_main_builder_load_options.table_schema_json = schema_json;
    explicit_main_builder_options["branch"] = "main";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> explicit_main,
                         explicit_main_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> loaded,
                         TableFactory::Create(explicit_main_builder_load_options,
                                              explicit_main_builder_options, "read"));
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

TEST(TableTest, TestLoadedTableReadRetainsLoadSchemeMap) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string path = dir->Str() + "/db.db/tbl";
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> created,
                         CreateManagedTable(dir->GetFileSystem(), path, Identifier("db", "tbl")));
    const std::map<std::string, std::string> scheme_map = {{"custom", "local"}};
    FileSystemCatalog catalog(dir->GetFileSystem(), dir->Str(), {}, scheme_map);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog.GetTable(Identifier("db", "tbl")));
    for (bool direct : {false, true}) {
        SCOPED_TRACE(direct);
        TableLoadOptions context_builder_36442_load_options{path};
        std::map<std::string, std::string> context_builder_36442_options;
        ReadContextBuilder context_builder_36442;
        context_builder_36442_load_options.fs_scheme_to_identifier_map = scheme_map;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, context_builder_36442.Finish());
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<TableRead> read,
            direct ? table->NewRead(std::move(context), context_builder_36442_options)
                   : TableRead::Create(context_builder_36442_load_options, std::move(context),
                                       context_builder_36442_options));
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

TEST(TableTest, TestReadRetainsLoadSchemeMapForMainAndFallback) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> main,
                         CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl")));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> dev,
                         CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl", "dev"), "dev"));
    const std::map<std::string, std::string> table_map = {{"base", "local"}, {"shared", "mock_fs"}};

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStoreTable> table,
        FileStoreTable::Create(fs, dir->Str(),
                               std::dynamic_pointer_cast<TableSchema>(main->GetSchema()),
                               Identifier("db", "tbl"), "", "main", table_map));
    for (bool fallback : {false, true}) {
        SCOPED_TRACE(fallback);
        std::map<std::string, std::string> builder_options;
        ReadContextBuilder builder;

        if (fallback) {
            builder_options[Options::SCAN_FALLBACK_BRANCH] = "dev";
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                             table->NewRead(std::move(context), builder_options));
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
            ASSERT_EQ(table_map,
                      internal->CreateDependencyCatalogContext()->fs_scheme_to_identifier_map);
            ASSERT_EQ(fs, internal->GetCoreOptions().GetFileSystem());
            ASSERT_EQ(dir->Str(), internal->GetPath());
        }
    }
    std::map<std::string, std::string> context_builder_38424_options;
    ReadContextBuilder context_builder_38424;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, context_builder_38424.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                         table->NewRead(std::move(context), context_builder_38424_options));
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
        TableLoadOptions context_builder_41670_load_options{dir->Str() + "$ro"};
        std::map<std::string, std::string> context_builder_41670_options;
        ReadContextBuilder context_builder_41670;
        context_builder_41670_load_options.fs_scheme_to_identifier_map = scheme_map;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, context_builder_41670.Finish());
        std::unique_ptr<TableRead> read;
        if (reload_context) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                                 TableFactory::Create(context_builder_41670_load_options,
                                                      context_builder_41670_options, "read"));
            TableLoadOptions context_builder_42682_load_options{dir->Str()};
            std::map<std::string, std::string> context_builder_42682_options;
            ReadContextBuilder context_builder_42682;
            context_builder_41670_load_options.path = context_builder_42682_load_options.path;
            context_builder_41670_load_options.file_system =
                context_builder_42682_load_options.file_system;
            context_builder_41670_load_options.fs_scheme_to_identifier_map =
                context_builder_42682_load_options.fs_scheme_to_identifier_map;
            context_builder_41670_load_options.table_schema_json =
                context_builder_42682_load_options.table_schema_json;
            context_builder_41670_load_options.catalog = context_builder_42682_load_options.catalog;
            context_builder_41670_load_options.table_id =
                context_builder_42682_load_options.table_id;
            context_builder_41670_load_options.identifier.reset();
            if (context_builder_42682_load_options.identifier) {
                context_builder_41670_load_options.identifier.emplace(
                    *context_builder_42682_load_options.identifier);
            }
            context_builder_41670_options = context_builder_42682_options;
            ASSERT_OK_AND_ASSIGN(context, context_builder_42682.Finish());
            ASSERT_OK_AND_ASSIGN(read,
                                 table->NewRead(std::move(context), context_builder_41670_options));
        } else {
            ASSERT_OK_AND_ASSIGN(
                read, TableRead::Create(context_builder_41670_load_options, std::move(context),
                                        context_builder_41670_options));
        }
        auto* append_read = dynamic_cast<AppendOnlyTableRead*>(read.get());
        ASSERT_NE(nullptr, append_read);
        const auto& internal = append_read->context_;
        ASSERT_EQ(scheme_map, internal->GetFileSystemSchemeToIdentifierMap());
        ASSERT_EQ(scheme_map,
                  internal->CreateDependencyCatalogContext()->fs_scheme_to_identifier_map);
    }
}

TEST(TableTest, TestSystemTableReadRetainsLoadSchemeMapWithoutReplacingFileSystem) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> data_table,
                         CreateManagedTable(fs, dir->Str(), Identifier("db", "tbl")));
    const std::map<std::string, std::string> table_map = {{"base", "local"}};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<SystemTable> table,
        SystemTableLoader::Load("ro", fs, dir->Str(),
                                std::dynamic_pointer_cast<TableSchema>(data_table->GetSchema()), {},
                                "tbl", table_map));
    std::map<std::string, std::string> context_builder_43111_options;
    ReadContextBuilder context_builder_43111;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, context_builder_43111.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                         table->NewRead(std::move(context), context_builder_43111_options));
    auto* append_read = dynamic_cast<AppendOnlyTableRead*>(read.get());
    ASSERT_NE(nullptr, append_read);
    const auto& internal = append_read->context_;
    ASSERT_EQ(fs, internal->GetCoreOptions().GetFileSystem());
    ASSERT_EQ(dir->Str(), internal->GetPath());
    ASSERT_EQ(table_map, internal->CreateDependencyCatalogContext()->fs_scheme_to_identifier_map);
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
            std::map<std::string, std::string> builder_options;
            ReadContextBuilder builder;
            builder_options = {{Options::SCAN_FALLBACK_BRANCH, fallback}};

            if (realtime_enabled) {
                builder_options[Options::REALTIME_ENABLED] = "true";
                builder.WithRealtimeContext(realtime);
            }
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
            auto read = table->NewRead(std::move(context), builder_options);
            if (realtime_enabled && fallback != "main") {
                ASSERT_NOK_WITH_MSG(read, "real-time operations only support the main branch");
            } else {
                ASSERT_OK(read);
            }
        }
    }
}

}  // namespace paimon::test

namespace paimon::test {

TEST(TableTest, TestLoadOptionsValidateIdentityBeforeMetadataAccess) {
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    TableLoadOptions load{"unused/table"};
    load.identifier.emplace("db", "tbl");
    ASSERT_NOK_WITH_MSG(TableFactory::Create(load, {}, "read"), "non-null catalog");
    load.catalog = catalog;
    load.identifier.reset();
    ASSERT_NOK_WITH_MSG(TableFactory::Create(load, {}, "write"), "table identifier");
    load.identifier.emplace("db", "tbl", "dev");
    ASSERT_NOK(TableFactory::Create(load, {{Options::BRANCH, "other"}}, "read"));
    ASSERT_NOK_WITH_MSG(TableFactory::Create(load, {{Options::BRANCH, "bad/branch"}}, "write"),
                        "branch");
    ASSERT_EQ(catalog->GetTableCalls(), 0u);
    ASSERT_TRUE(catalog->TableFileSystemRequests().empty());
}

TEST(TableTest, TestLoadOptionsRejectConflictingFileSystemResolution) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    TableLoadOptions load{dir->Str()};
    load.file_system = dir->GetFileSystem();
    load.fs_scheme_to_identifier_map = {{"file", "local"}};
    ASSERT_NOK_WITH_MSG(TableFactory::Create(load, {}, "read"), "cannot be specified together");
}

TEST(TableTest, TestPreloadedSchemaIsIndependentOfReadProjection) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> schema,
                         TableSchema::Create(0,
                                             arrow::schema({arrow::field("id", arrow::int32()),
                                                            arrow::field("value", arrow::utf8())}),
                                             {}, {}, {}));
    TableLoadOptions load{dir->Str() + "/without-metadata"};
    load.file_system = dir->GetFileSystem();
    ASSERT_OK_AND_ASSIGN(load.table_schema_json, schema->GetJsonSchema());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context,
                         ReadContextBuilder().SetReadFieldNames({"value"}).Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, TableFactory::Create(load, {}, "read"));
    ASSERT_EQ(table->GetSchema()->FieldNames(), (std::vector<std::string>{"id", "value"}));
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableRead> read,
                         table->NewRead(std::move(context)));
}

TEST(TableTest, TestLoadOptionsValidateExpectedCatalogUuid) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> schema,
        TableSchema::Create(0, arrow::schema({arrow::field("id", arrow::int32())}), {}, {}, {}));
    auto catalog = std::make_shared<MockVersionManagedCatalog>(true, "current-uuid");
    catalog->SetTableSchema(schema);
    catalog->SetFileSystem(dir->GetFileSystem());
    TableLoadOptions load{dir->Str()};
    load.catalog = catalog;
    load.identifier.emplace("db", "tbl");
    load.table_id = "previous-uuid";
    ASSERT_NOK_WITH_MSG(TableFactory::Create(load, {}, "commit"), "does not match");
    load.table_id = "current-uuid";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, TableFactory::Create(load, {}, "commit"));
    ASSERT_EQ(table->CatalogUuid(), load.table_id);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> context,
                         CommitContextBuilder("writer").Finish());
    ASSERT_NOK_WITH_MSG(table->NewCommit(std::move(context), {{Options::BRANCH, "dev"}}),
                        "branch contradicts");
}

TEST(TableTest, TestExplicitPathAndUuidRetainCatalogFileSystemWithRouting) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> schema,
        TableSchema::Create(0, arrow::schema({arrow::field("id", arrow::int32())}), {}, {}, {}));
    ASSERT_OK_AND_ASSIGN(std::string schema_json, schema->GetJsonSchema());
    for (bool with_routing : {false, true}) {
        std::map<std::string, std::string> routes;
        if (with_routing) {
            routes["dependency"] = "local";
        }
        auto scoped_fs = std::make_shared<MockFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FileStoreTable> catalog_table,
            FileStoreTable::Create(scoped_fs, dir->Str(), schema, Identifier("db", "tbl"),
                                   "table-uuid", "main", routes));
        auto catalog = std::make_shared<MockVersionManagedCatalog>();
        catalog->SetTable(catalog_table);
        for (bool override_file_system : {false, true}) {
            auto supplied_fs = std::make_shared<MockFileSystem>();
            TableLoadOptions load{dir->Str()};
            load.catalog = catalog;
            load.identifier.emplace("db", "tbl");
            load.table_id = "table-uuid";
            load.table_schema_json = schema_json;
            if (override_file_system) {
                load.file_system = supplied_fs;
            }
            for (const std::string operation : {"scan", "read", "write", "commit"}) {
                SCOPED_TRACE(operation + (with_routing ? ":routing" : ":no-routing") +
                             (override_file_system ? ":override" : ":catalog"));
                ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                                     TableFactory::Create(load, {}, operation.c_str()));
                ASSERT_EQ(override_file_system ? supplied_fs : scoped_fs, table->GetFileSystem());
                auto managed = std::dynamic_pointer_cast<FileStoreTable>(table);
                ASSERT_NE(managed, nullptr);
                ASSERT_EQ(routes, managed->GetFileSystemSchemeToIdentifierMap());
            }
        }
    }
}

TEST(TableTest, TestExplicitPathRetainsMissingSchemaErrorWithCatalog) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    TableLoadOptions load{dir->Str() + "/missing-table"};
    load.catalog = std::make_shared<FileSystemCatalog>(dir->GetFileSystem(), dir->Str(),
                                                       std::map<std::string, std::string>{});
    load.identifier.emplace("db", "missing-table");
    for (const std::string operation : {"scan", "read"}) {
        SCOPED_TRACE(operation);
        auto result = TableFactory::Create(load, {}, operation.c_str());
        ASSERT_TRUE(result.status().IsInvalid()) << result.status().ToString();
        ASSERT_NOK_WITH_MSG(result, "not found latest schema in branch main");
    }
}

}  // namespace paimon::test

namespace paimon::test {
TEST(TableTest, TestLoadedTableRetainsCatalogAfterStackCatalogIsDestroyed) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string path = dir->Str() + "/db.db/tbl";
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<Table> created,
                         CreateManagedTable(dir->GetFileSystem(), path, Identifier("db", "tbl")));
    std::shared_ptr<Table> table;
    const std::map<std::string, std::string> catalog_options = {{"dependency.setting", "retained"}};
    {
        FileSystemCatalog catalog(dir->GetFileSystem(), dir->Str(), catalog_options);
        ASSERT_OK_AND_ASSIGN(table, catalog.GetTable(Identifier("db", "tbl")));
    }
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, ReadContextBuilder().Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read, table->NewRead(std::move(context)));
    auto* append_read = dynamic_cast<AppendOnlyTableRead*>(read.get());
    ASSERT_NE(append_read, nullptr);
    auto dependency = append_read->context_->CreateDependencyCatalogContext();
    ASSERT_EQ(dependency->root_path, dir->Str());
    ASSERT_EQ(dependency->options.at("dependency.setting"), "retained");
}
}  // namespace paimon::test
