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

#include "paimon/common/types/data_type_json_parser.h"

#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon::test {

namespace {

void CheckFieldId(const std::shared_ptr<arrow::Field>& field, int32_t expected_id) {
    ASSERT_OK_AND_ASSIGN(DataField data_field, DataField::ConvertArrowFieldToDataField(field));
    ASSERT_EQ(data_field.Id(), expected_id);
}

}  // namespace

TEST(DataTypeJsonParserTest, ParseTypeArrayTypeSuccess) {
    const std::string name = "array_field";
    const char* json = R"({
        "type": "ARRAY",
        "element": "INT"
    })";
    rapidjson::Document doc;
    doc.Parse(json);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                         DataTypeJsonParser::ParseType(name, doc));
    ASSERT_NE(field, nullptr);
}

TEST(DataTypeJsonParserTest, ParseVectorTypeSuccess) {
    const char* json = R"({
        "type": "VECTOR NOT NULL",
        "element": "FLOAT",
        "length": 3
    })";
    rapidjson::Document doc;
    doc.Parse(json);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                         DataTypeJsonParser::ParseType("embedding", doc));
    ASSERT_FALSE(field->nullable());
    ASSERT_EQ(field->type()->id(), arrow::Type::FIXED_SIZE_LIST);
    auto vector_type = checked_pointer_cast<arrow::FixedSizeListType>(field->type());
    ASSERT_EQ(vector_type->list_size(), 3);
    ASSERT_TRUE(vector_type->value_type()->Equals(arrow::float32()));

    rapidjson::Document sql_doc;
    rapidjson::Value sql_value("VECTOR<BIGINT NOT NULL, 5>", sql_doc.GetAllocator());
    ASSERT_OK_AND_ASSIGN(field, DataTypeJsonParser::ParseType("embedding", sql_value));
    vector_type = checked_pointer_cast<arrow::FixedSizeListType>(field->type());
    ASSERT_TRUE(field->nullable());
    ASSERT_EQ(vector_type->list_size(), 5);
    ASSERT_FALSE(vector_type->value_field()->nullable());
    ASSERT_TRUE(vector_type->value_type()->Equals(arrow::int64()));
}

TEST(DataTypeJsonParserTest, ParseVectorTypeFailure) {
    for (const char* json : {
             R"({"type":"VECTOR","element":"FLOAT","length":0})",
             R"({"type":"VECTOR","element":"STRING","length":3})",
             R"({"type":"VECTOR","element":"FLOAT"})",
             R"({"type":"VECTOR","element":"FLOAT","length":"3"})",
         }) {
        rapidjson::Document doc;
        doc.Parse(json);
        ASSERT_NOK(DataTypeJsonParser::ParseType("embedding", doc));
    }

    rapidjson::Document sql_doc;
    rapidjson::Value sql_value("VECTOR<STRING, 3>", sql_doc.GetAllocator());
    ASSERT_NOK_WITH_MSG(DataTypeJsonParser::ParseType("embedding", sql_value),
                        "Invalid element type for vector");
    sql_value.SetString("VECTOR<DOUBLE, 3>", sql_doc.GetAllocator());
    ASSERT_OK(DataTypeJsonParser::ParseType("embedding", sql_value));
    sql_value.SetString("VECTOR<FLOAT, 999999999999999999999999>", sql_doc.GetAllocator());
    ASSERT_NOK_WITH_MSG(DataTypeJsonParser::ParseType("embedding", sql_value),
                        "Vector length must be between 1 and 2147483647");
}

TEST(DataTypeJsonParserTest, ParseTypeMapTypeSuccess) {
    const std::string name = "map_field";
    const char* json = R"({
        "type": "MAP",
        "key": "STRING",
        "value": "INT"
    })";
    rapidjson::Document doc;
    doc.Parse(json);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                         DataTypeJsonParser::ParseType(name, doc));
    ASSERT_NE(field, nullptr);
    auto map_type = checked_pointer_cast<arrow::MapType>(field->type());
    ASSERT_FALSE(map_type->key_field()->nullable());
}

TEST(DataTypeJsonParserTest, ParseTypeRowTypeSuccess) {
    const std::string name = "row_field";
    const char* json = R"({
      "type" : "ROW",
      "fields" : [ {
        "id" : 1,
        "name" : "sub1",
        "type" : "DATE"
      }, {
        "id" : 4,
        "name" : "sub4",
        "type" : "BYTES"
      }, {
        "id" : 7,
        "name" : "sub7",
        "type" : {
          "type" : "ROW",
          "fields" : [ { "id" : 9, "name" : "nested", "type" : "INT" } ]
        }
      }]})";
    rapidjson::Document doc;
    doc.Parse(json);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                         DataTypeJsonParser::ParseType(name, doc));
    ASSERT_NE(field, nullptr);
    ASSERT_EQ(field->type()->num_fields(), 3);
    ASSERT_NO_FATAL_FAILURE(CheckFieldId(field->type()->field(0), 1));
    ASSERT_NO_FATAL_FAILURE(CheckFieldId(field->type()->field(1), 4));
    ASSERT_NO_FATAL_FAILURE(CheckFieldId(field->type()->field(2), 7));
    ASSERT_NO_FATAL_FAILURE(CheckFieldId(field->type()->field(2)->type()->field(0), 9));
}

TEST(DataTypeJsonParserTest, ParseTypeRowTypeGeneratesMissingFieldIds) {
    const char* json = R"({
      "type" : "ROW",
      "fields" : [ {
        "name" : "payload",
        "type" : {
          "type" : "ROW",
          "fields" : [ {
            "name" : "age",
            "type" : "INT"
          }, {
            "name" : "profile",
            "type" : {
              "type" : "ROW",
              "fields" : [ { "name" : "city", "type" : "STRING" } ]
            }
          } ]
        }
      }, {
        "name" : "tags",
        "type" : {
          "type" : "ARRAY",
          "element" : {
            "type" : "ROW",
            "fields" : [ { "name" : "tag", "type" : "STRING" } ]
          }
        }
      }, {
        "name" : "props",
        "type" : {
          "type" : "MAP",
          "key" : {
            "type" : "ROW",
            "fields" : [ { "name" : "code", "type" : "STRING" } ]
          },
          "value" : {
            "type" : "ROW",
            "fields" : [ { "name" : "weight", "type" : "DOUBLE" } ]
          }
        }
      } ]})";
    rapidjson::Document doc;
    doc.Parse(json);

    for (int32_t attempt = 0; attempt < 2; ++attempt) {
        SCOPED_TRACE(attempt);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                             DataTypeJsonParser::ParseType("row_field", doc));
        const std::shared_ptr<arrow::DataType>& row = field->type();
        ASSERT_EQ(row->num_fields(), 3);
        ASSERT_NO_FATAL_FAILURE(CheckFieldId(row->field(0), 0));
        const std::shared_ptr<arrow::DataType>& payload = row->field(0)->type();
        ASSERT_EQ(payload->num_fields(), 2);
        ASSERT_NO_FATAL_FAILURE(CheckFieldId(payload->field(0), 1));
        ASSERT_NO_FATAL_FAILURE(CheckFieldId(payload->field(1), 2));
        ASSERT_NO_FATAL_FAILURE(CheckFieldId(payload->field(1)->type()->field(0), 3));
        ASSERT_NO_FATAL_FAILURE(CheckFieldId(row->field(1), 4));
        const std::shared_ptr<arrow::DataType>& element = row->field(1)->type()->field(0)->type();
        ASSERT_NO_FATAL_FAILURE(CheckFieldId(element->field(0), 5));
        ASSERT_NO_FATAL_FAILURE(CheckFieldId(row->field(2), 6));
        const std::shared_ptr<arrow::DataType>& entries = row->field(2)->type()->field(0)->type();
        const std::shared_ptr<arrow::DataType>& map_key = entries->field(0)->type();
        ASSERT_NO_FATAL_FAILURE(CheckFieldId(map_key->field(0), 7));
        const std::shared_ptr<arrow::DataType>& map_value = entries->field(1)->type();
        ASSERT_NO_FATAL_FAILURE(CheckFieldId(map_value->field(0), 8));
    }
}

TEST(DataTypeJsonParserTest, ParseTypeComplexTypeFailure) {
    const std::vector<std::pair<const char*, const char*>> test_cases = {
        {R"({"type":"ROW"})", "row data type must have fields"},
        {R"({"type":"ROW","fields":{}})", "row data type must have fields"},
        {R"({"type":"ROW","fields":[null]})", "data field must be an object"},
        {R"({"type":"ROW","fields":[{"name":"a","type":"INT"},
                                    {"id":1,"name":"b","type":"INT"}]})",
         "Partial field id is not allowed."},
        {R"({"type":"ROW","fields":[{"id":0,"name":"a","type":"INT"},
                                    {"name":"b","type":"INT"}]})",
         "Partial field id is not allowed."},
        {R"({"type":"ROW","fields":[{"id":0,"name":"a","type":{
               "type":"ROW","fields":[{"name":"b","type":"INT"}]}}]})",
         "Partial field id is not allowed."},
        {R"({"type":"ROW","fields":[{"name":"a","type":{
               "type":"ROW","fields":[{"id":1,"name":"b","type":"INT"}]}}]})",
         "Partial field id is not allowed."},
        {R"({"type":"ROW","fields":[{"name":"a","type":{"type":"ARRAY",
               "element":{"type":"ROW","fields":[{"id":1,"name":"b","type":"INT"}]}}}]})",
         "Partial field id is not allowed."},
        {R"({"type":"MAP",
               "key":{"type":"ROW","fields":[{"id":0,"name":"a","type":"INT"}]},
               "value":{"type":"ROW","fields":[{"name":"b","type":"INT"}]}})",
         "Partial field id is not allowed."},
        {R"({"type":"MAP",
               "key":{"type":"ROW","fields":[{"name":"a","type":"INT"}]},
               "value":{"type":"ROW","fields":[{"id":1,"name":"b","type":"INT"}]}})",
         "Partial field id is not allowed."},
    };
    for (const auto& [json, error_msg] : test_cases) {
        SCOPED_TRACE(json);
        rapidjson::Document doc;
        doc.Parse(json);
        ASSERT_NOK_WITH_MSG(DataTypeJsonParser::ParseType("row_field", doc), error_msg);
    }
}

TEST(DataTypeJsonParserTest, ParseTypeAtomicTypeSuccess) {
    // List of atomic types and their expected Arrow types
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> test_cases = {
        {"BOOLEAN", arrow::boolean()},
        {"TINYINT", arrow::int8()},
        {"SMALLINT", arrow::int16()},
        {"INT", arrow::int32()},
        {"INTEGER", arrow::int32()},
        {"BIGINT", arrow::int64()},
        {"FLOAT", arrow::float32()},
        {"DOUBLE", arrow::float64()},
        {"DOUBLE PRECISION", arrow::float64()},
        {"DEC", arrow::decimal128(10, 0)},
        {"DEC(10)", arrow::decimal128(10, 0)},
        {"DEC(10, 3)", arrow::decimal128(10, 3)},
        {"DECIMAL", arrow::decimal128(10, 0)},
        {"DECIMAL(10)", arrow::decimal128(10, 0)},
        {"DECIMAL(10, 3)", arrow::decimal128(10, 3)},
        {"NUMERIC", arrow::decimal128(10, 0)},
        {"NUMERIC(10)", arrow::decimal128(10, 0)},
        {"NUMERIC(10, 3)", arrow::decimal128(10, 3)},
        {"TIMESTAMP(0)", arrow::timestamp(arrow::TimeUnit::SECOND)},
        {"TIMESTAMP(3)", arrow::timestamp(arrow::TimeUnit::MILLI)},
        {"TIMESTAMP(6)", arrow::timestamp(arrow::TimeUnit::MICRO)},
        {"TIMESTAMP(9)", arrow::timestamp(arrow::TimeUnit::NANO)},
        {"TIMESTAMP(9) WITHOUT TIME ZONE", arrow::timestamp(arrow::TimeUnit::NANO)},
        {"TIMESTAMP(9) WITH", arrow::timestamp(arrow::TimeUnit::NANO)},
        {"TIMESTAMP(9) WITH LOCAL TIME ZONE", arrow::timestamp(arrow::TimeUnit::NANO, timezone)},
        {"TIMESTAMP_LTZ(9)", arrow::timestamp(arrow::TimeUnit::NANO, timezone)},
        {"BYTES", arrow::binary()},
        {"STRING", arrow::utf8()},
        {"CHAR", arrow::utf8()},
        {"CHAR(10)", arrow::utf8()},
        {"VARCHAR", arrow::utf8()},
        {"VARCHAR(10)", arrow::utf8()},
        {"BINARY", arrow::binary()},
        {"BINARY(10)", arrow::binary()},
        {"VARBINARY", arrow::binary()},
        {"VARBINARY(10)", arrow::binary()},
        {"CHAR(1)", arrow::utf8()},
        {"VARCHAR(2147483647)", arrow::utf8()},
    };

    for (const auto& test_case : test_cases) {
        const std::string& type_str = test_case.first;

        rapidjson::Document doc;
        rapidjson::Value value(type_str.data(), doc.GetAllocator());

        // Parse type and verify the result
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                             DataTypeJsonParser::ParseType("field_name", value));
        ASSERT_TRUE(field->type()->Equals(test_case.second));
    }

    // VARIANT parses to a variant-marked struct<value, metadata> field.
    for (const char* variant_str : {"VARIANT", "VARIANT NOT NULL"}) {
        rapidjson::Document doc;
        rapidjson::Value value(variant_str, doc.GetAllocator());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Field> field,
                             DataTypeJsonParser::ParseType("variant_field", value));
        ASSERT_TRUE(VariantTypeUtils::IsVariantField(field));
        ASSERT_EQ(field->nullable(), std::string(variant_str) == "VARIANT");
        ASSERT_TRUE(field->type()->Equals(VariantTypeUtils::UnshreddedStructType()));
    }

    // Invalid case
    {
        rapidjson::Document invalid_doc;
        rapidjson::Value value("VARCHAR(test)", invalid_doc.GetAllocator());
        ASSERT_NOK(DataTypeJsonParser::ParseType("field_name", value));
    }
    for (const char* invalid_type : {"VARCHAR(0)", "VARBINARY(0)", "VARCHAR(2147483648)"}) {
        rapidjson::Document invalid_doc;
        rapidjson::Value value(invalid_type, invalid_doc.GetAllocator());
        ASSERT_NOK_WITH_MSG(DataTypeJsonParser::ParseType("field_name", value),
                            "length must be between 1 and 2147483647");
    }
    {
        rapidjson::Document invalid_doc;
        rapidjson::Value value("TIMESTAMP(4)", invalid_doc.GetAllocator());
        ASSERT_NOK_WITH_MSG(DataTypeJsonParser::ParseType("field_name", value),
                            "only support precision 0/3/6/9 in timestamp type");
    }
    {
        rapidjson::Document invalid_doc;
        rapidjson::Value value("TIMESTAMP(8) WITH LOCAL TIME ZONE", invalid_doc.GetAllocator());
        ASSERT_NOK_WITH_MSG(DataTypeJsonParser::ParseType("field_name", value),
                            "only support precision 0/3/6/9 in timestamp type");
    }
}

}  // namespace paimon::test
