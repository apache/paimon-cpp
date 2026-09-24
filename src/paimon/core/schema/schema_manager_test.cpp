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

#include <future>
#include <set>
#include <utility>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_type.h"
#include "paimon/common/types/data_type_json_parser.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(SchemaManagerTest, TimePrecisionRoundTrip) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<LocalFileSystem>();
    SchemaManager manager(fs, dir->Str());
    arrow::FieldVector fields;
    for (int32_t precision = 0; precision <= 9; ++precision) {
        for (bool nullable : {true, false}) {
            std::string name = "t" + std::to_string(fields.size());
            std::string type =
                "TIME(" + std::to_string(precision) + ")" + (nullable ? "" : " NOT NULL");
            rapidjson::Document doc;
            rapidjson::Value value(type.c_str(), doc.GetAllocator());
            ASSERT_OK_AND_ASSIGN(auto field, DataTypeJsonParser::ParseType(name, value));
            fields.push_back(field);
        }
    }
    fields.push_back(arrow::field("default_time", arrow::time32(arrow::TimeUnit::MILLI)));
    fields.push_back(arrow::field("times", arrow::list(fields[6]->WithName("item"))));
    fields.push_back(
        arrow::field("mapping", std::make_shared<arrow::MapType>(fields[13]->WithName("key"),
                                                                 fields[18]->WithName("value"))));
    fields.push_back(arrow::field("nested", arrow::struct_({fields[6], fields[13], fields[18]})));
    ASSERT_OK_AND_ASSIGN(auto created,
                         manager.CreateTable(arrow::schema(fields), {}, {},
                                             {{"file.format", "parquet"}, {"bucket", "-1"}}));
    ASSERT_OK_AND_ASSIGN(auto serialized, created->ToJsonString());
    SchemaManager reloaded_manager(fs, dir->Str());
    ASSERT_OK_AND_ASSIGN(auto reloaded, reloaded_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto reserialized, reloaded->ToJsonString());
    ASSERT_EQ(serialized, reserialized);
    const auto& restored_fields = reloaded->Fields();
    for (int32_t i = 0; i < 20; ++i) {
        ASSERT_OK_AND_ASSIGN(auto precision,
                             DataType::GetTimePrecision(*restored_fields[i].ArrowField()));
        ASSERT_EQ(precision, i / 2);
        ASSERT_EQ(restored_fields[i].ArrowField()->nullable(), i % 2 == 0);
    }
    ASSERT_OK_AND_ASSIGN(auto default_precision,
                         DataType::GetTimePrecision(*restored_fields[20].ArrowField()));
    ASSERT_EQ(default_precision, 0);
    for (int32_t i = 21; i < 24; ++i) {
        SCOPED_TRACE(i);
        ASSERT_TRUE(DataField::ConvertDataFieldToArrowField(created->Fields()[i])
                        ->Equals(DataField::ConvertDataFieldToArrowField(restored_fields[i]),
                                 /*check_metadata=*/true));
    }
}

TEST(SchemaManagerTest, RejectTimePartitionKey) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    SchemaManager manager(std::make_shared<LocalFileSystem>(), dir->Str());
    auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("time", arrow::time32(arrow::TimeUnit::MILLI))});
    ASSERT_NOK_WITH_MSG(
        manager.CreateTable(schema, {"time"}, {}, {{"file.format", "parquet"}, {"bucket", "-1"}}),
        "partition field time cannot be TIME");
    ASSERT_OK_AND_ASSIGN(auto latest, manager.Latest());
    ASSERT_FALSE(latest.has_value());
}

TEST(SchemaManagerTest, ConcurrentHistoricalSchemaReads) {
    SchemaManager manager(
        std::make_shared<LocalFileSystem>(),
        GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table/");
    std::vector<std::future<Result<std::shared_ptr<TableSchema>>>> reads;
    for (int32_t i = 0; i < 16; ++i) {
        reads.push_back(
            std::async(std::launch::async, [&manager, i]() { return manager.ReadSchema(i % 2); }));
    }
    for (int32_t i = 0; i < 16; ++i) {
        ASSERT_OK_AND_ASSIGN(auto schema, reads[i].get());
        ASSERT_EQ(schema->Id(), i % 2);
    }
    ASSERT_EQ(manager.schema_cache_.Size(), 2);
}

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
    ASSERT_GT(manager.schema_cache_.Size(), 0);
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
