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

#include "paimon/core/schema/schema_manager.h"

#include <set>
#include <utility>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(SchemaManagerTest, TestSimple) {
    auto fs = std::make_shared<LocalFileSystem>();
    std::string table_root =
        paimon::test::GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table/";
    SchemaManager manager(fs, table_root);
    ASSERT_EQ(manager.ToSchemaPath(/*schema_id=*/0),
              paimon::test::GetDataDir() +
                  "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table/schema/schema-0");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> ret, manager.ReadSchema(/*schema_id=*/1));
    std::string schema_json = R"({
        "version" : 3,
        "id" : 1,
        "fields" : [ {
                "id" : 1,
                "name" : "key1",
                "type" : "INT NOT NULL"
        }, {
                "id" : 7,
                "name" : "k",
                "type" : "STRING"
        }, {
                "id" : 2,
                "name" : "key_2",
                "type" : "INT NOT NULL"
        }, {
                "id" : 4,
                "name" : "c",
                "type" : "INT"
        }, {
                "id" : 8,
                "name" : "d",
                "type" : "INT",
                "description" : ""
        }, {
                "id" : 6,
                "name" : "a",
                "type" : "INT"
        }, {
                "id" : 0,
                "name" : "key0",
                "type" : "INT NOT NULL"
        }, {
                "id" : 9,
                "name" : "e",
                "type" : "INT"
        } ],
        "highestFieldId" : 9,
        "partitionKeys" : [ "key0", "key1" ],
        "primaryKeys" : [ "key0", "key1", "key_2" ],
        "options" : {
                "bucket" : "1",
                "manifest.format" : "orc",
                "file.format" : "orc",
                "deletion-vectors.enabled" : "true",
                "commit.force-compact" : "true"
        },
        "timeMillis" : 1730516111087
    })";
    ASSERT_OK_AND_ASSIGN(auto expected_schema, TableSchema::CreateFromJson(schema_json));
    ASSERT_EQ(*ret, *expected_schema);
    ASSERT_FALSE(manager.schema_cache_.empty());
    ASSERT_EQ(*manager.ReadSchema(/*schema_id=*/1).value(), *expected_schema);
    ASSERT_EQ(*(manager.Latest().value().value()), *expected_schema);
}

TEST(SchemaManagerTest, TestNonExistTable) {
    auto fs = std::make_shared<LocalFileSystem>();
    std::string table_root = paimon::test::GetDataDir() + "/non-exist.db/non-exist/";
    SchemaManager manager(fs, table_root);
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> latest, manager.Latest());
    ASSERT_EQ(latest, std::nullopt);
    auto ret = manager.ReadSchema(/*schema_id=*/100);
    ASSERT_FALSE(ret.ok());
}

TEST(SchemaManagerTest, TestSchemaDirectory) {
    auto fs = std::make_shared<LocalFileSystem>();
    std::string table_root = paimon::test::GetDataDir() + "/sample_table/";
    SchemaManager manager(fs, table_root);
    ASSERT_EQ(manager.SchemaDirectory(), paimon::test::GetDataDir() + "/sample_table/schema");
}

TEST(SchemaManagerTest, TestSchemaDirectoryWithBranch) {
    auto fs = std::make_shared<LocalFileSystem>();
    std::string table_root = paimon::test::GetDataDir() + "/sample_table/";
    {
        SchemaManager manager(fs, table_root, /*branch=*/"data");
        ASSERT_EQ(manager.SchemaDirectory(),
                  paimon::test::GetDataDir() + "/sample_table/branch/branch-data/schema");
    }
    {
        SchemaManager manager(fs, table_root, /*branch=*/"main");
        ASSERT_EQ(manager.SchemaDirectory(), paimon::test::GetDataDir() + "/sample_table/schema");
    }
}

TEST(SchemaManagerTest, TestCreateTableWithInvalidInput) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto dir = UniqueTestDirectory::Create();
    SchemaManager manager(fs, dir->Str());

    // Create an Arrow schema
    auto field1 = std::make_shared<arrow::Field>("id", arrow::int32(), false);
    auto field2 = std::make_shared<arrow::Field>("name", arrow::utf8());
    auto field3 = std::make_shared<arrow::Field>("value", arrow::int64());
    auto schema = arrow::schema(arrow::FieldVector{field1, field2, field3});

    std::vector<std::string> partition_keys = {"id"};
    std::vector<std::string> primary_keys = {"id"};
    std::map<std::string, std::string> options = {{"file.format", "orc"},
                                                  {"commit.force-compact", "true"}};

    // Create table
    auto result = manager.CreateTable(schema, partition_keys, primary_keys, options);
    ASSERT_NOK(result);
}

TEST(SchemaManagerTest, TestCreateTable) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto dir = UniqueTestDirectory::Create();
    SchemaManager manager(fs, dir->Str());

    // Create an Arrow schema
    auto field1 = std::make_shared<arrow::Field>("id", arrow::int32(), false);
    auto field2 = std::make_shared<arrow::Field>("name", arrow::utf8());
    auto field3 = std::make_shared<arrow::Field>("value", arrow::int64());
    auto schema = arrow::schema(arrow::FieldVector{field1, field2, field3});

    std::vector<std::string> partition_keys = {"name"};
    std::vector<std::string> primary_keys = {"id"};
    std::map<std::string, std::string> options = {{"file.format", "orc"},
                                                  {"commit.force-compact", "true"}};

    // Create table
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> result,
                         manager.CreateTable(schema, partition_keys, primary_keys, options));

    // Verify schema was created
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> latest_result,
                         manager.Latest());
    ASSERT_TRUE(latest_result);
    auto created_schema = latest_result.value();
    ASSERT_EQ(created_schema->Id(), 0);
    ASSERT_EQ(created_schema->PartitionKeys(), partition_keys);
    ASSERT_EQ(created_schema->PrimaryKeys(), primary_keys);
}

TEST(SchemaManagerTest, TestCreateTableAlreadyExists) {
    auto fs = std::make_shared<LocalFileSystem>();
    std::string table_root =
        paimon::test::GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table/";
    SchemaManager manager(fs, table_root);

    // Create an Arrow schema
    auto field = std::make_shared<arrow::Field>("dummy", arrow::int32());
    auto schema = arrow::schema(arrow::FieldVector{field});

    // Try to create table where schema already exists
    ASSERT_NOK_WITH_MSG(manager.CreateTable(schema, {}, {}, {}), "Schema in filesystem exists");
}

TEST(SchemaManagerTest, TestListAllIds) {
    auto fs = std::make_shared<LocalFileSystem>();
    std::string table_root =
        paimon::test::GetDataDir() + "/orc/pk_table_with_mor.db/pk_table_with_mor/";
    SchemaManager manager(fs, table_root);
    ASSERT_OK_AND_ASSIGN(auto ids, manager.ListAllIds());
    ASSERT_EQ(std::set<int64_t>(ids.begin(), ids.end()), std::set<int64_t>({0, 1, 2, 3, 4}));
}
}  // namespace paimon::test
