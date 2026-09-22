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

#include "paimon/common/types/data_field.h"

#include <stdexcept>
#include <utility>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon::test {

class DataFieldTest : public ::testing::Test {
 protected:
    void SetUp() override {
        auto arrow_field1 = arrow::field("field1", arrow::int32());
        auto arrow_field2 = arrow::field("field2", arrow::utf8());

        field1_ = DataField(1, arrow_field1, "description1");
        field2_ = DataField(2, arrow_field2);

        auto arrow_field3 = arrow::field("field3", arrow::struct_({arrow_field1, arrow_field2}));

        field3_ = DataField(0, arrow_field3);
    }

    DataField field1_;
    DataField field2_;
    DataField field3_;
};

TEST_F(DataFieldTest, FieldAttributes) {
    EXPECT_EQ(field1_.Id(), 1);
    EXPECT_EQ(field1_.Name(), "field1");
    EXPECT_EQ(field1_.Type()->id(), arrow::Type::INT32);
    EXPECT_EQ(field1_.Description().value(), "description1");

    EXPECT_EQ(field2_.Id(), 2);
    EXPECT_EQ(field2_.Name(), "field2");
    EXPECT_EQ(field2_.Type()->id(), arrow::Type::STRING);
    EXPECT_EQ(field2_.Description(), std::nullopt);
}

TEST_F(DataFieldTest, EqualityOperator) {
    DataField field1_copy = field1_;
    EXPECT_TRUE(field1_ == field1_copy);
    EXPECT_FALSE(field1_ == field2_);
}

TEST_F(DataFieldTest, ConvertDataFieldToArrowField) {
    auto arrow_field = DataField::ConvertDataFieldToArrowField(field1_);
    EXPECT_EQ(arrow_field->name(), "field1");
    EXPECT_EQ(arrow_field->type()->id(), arrow::Type::INT32);
    EXPECT_TRUE(arrow_field->nullable());
    EXPECT_EQ(arrow_field->metadata()->Get(DataField::FIELD_ID).ValueOrDie(), "1");
}

TEST_F(DataFieldTest, ConvertDataFieldsToArrowStructType) {
    std::vector<DataField> data_fields = {field1_, field2_};
    auto arrow_struct_type = DataField::ConvertDataFieldsToArrowStructType(data_fields);
    EXPECT_EQ(arrow_struct_type->num_fields(), 2);
    EXPECT_EQ(arrow_struct_type->field(0)->name(), "field1");
    EXPECT_EQ(arrow_struct_type->field(1)->name(), "field2");
}

TEST_F(DataFieldTest, ConvertDataFieldsToArrowSchema) {
    std::vector<DataField> data_fields = {field1_, field2_};
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(data_fields);
    EXPECT_EQ(arrow_schema->num_fields(), 2);
    EXPECT_EQ(arrow_schema->field(0)->name(), "field1");
    EXPECT_EQ(arrow_schema->field(1)->name(), "field2");
}

TEST_F(DataFieldTest, ConvertArrowSchemaToDataFields) {
    std::vector<DataField> data_fields = {field1_, field2_};
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(data_fields);
    ASSERT_OK_AND_ASSIGN(auto converted_data_fields,
                         DataField::ConvertArrowSchemaToDataFields(arrow_schema));
    EXPECT_EQ(converted_data_fields.size(), 2);
    EXPECT_EQ(converted_data_fields[0], field1_);
    EXPECT_EQ(converted_data_fields[1], field2_);
}

TEST_F(DataFieldTest, GetAllFieldIds) {
    std::vector<DataField> data_fields = {field1_, field2_, field3_};
    auto fields_ids = DataField::GetAllFieldIds(data_fields);
    ASSERT_EQ(fields_ids, std::vector<int32_t>({field1_.Id(), field2_.Id(), field3_.Id()}));
}

TEST_F(DataFieldTest, ConvertArrowFieldToDataField) {
    {
        auto arrow_field = DataField::ConvertDataFieldToArrowField(field1_);
        ASSERT_OK_AND_ASSIGN(auto converted_data_field,
                             DataField::ConvertArrowFieldToDataField(arrow_field));
        ASSERT_EQ(converted_data_field, field1_);
    }
    {
        std::vector<std::string> keys = {"invalid_field_id"};
        std::vector<std::string> values = {"1"};
        std::shared_ptr<arrow::KeyValueMetadata> meta = arrow::KeyValueMetadata::Make(keys, values);
        auto arrow_field = arrow::field("field1", arrow::int32())->WithMetadata(meta);
        ASSERT_NOK_WITH_MSG(DataField::ConvertArrowFieldToDataField(arrow_field),
                            "Key error: paimon.id");
    }
    {
        std::vector<std::string> keys = {std::string(DataField::FIELD_ID)};
        std::vector<std::string> values = {"1--"};
        std::shared_ptr<arrow::KeyValueMetadata> meta = arrow::KeyValueMetadata::Make(keys, values);
        auto arrow_field = arrow::field("field1", arrow::int32())->WithMetadata(meta);
        ASSERT_NOK_WITH_MSG(DataField::ConvertArrowFieldToDataField(arrow_field),
                            "invalid read schema, cannot cast field id 1-- to int");
    }
}

TEST_F(DataFieldTest, FromJson) {
    const char* json = R"({
    "id" : 0,
    "name" : "f0\u0000tail",
    "description" : "d\u0000tail",
    "type" : {
      "type" : "ROW",
      "fields" : [ {
        "id" : 1,
        "name" : "sub1",
        "type" : "DATE"
       }, {
        "id" : 4,
        "name" : "sub4",
        "type" : "BYTES"
       },
       {
        "id" : 5,
        "name" : "sub5",
        "type" : "BLOB"
       }]
    }
})";
    rapidjson::Document doc;
    doc.Parse(json);
    DataField field;
    field.FromJson(doc);
    EXPECT_EQ(field.Id(), 0);
    ASSERT_EQ(field.Name(), std::string("f0\0tail", 7));
    ASSERT_EQ(field.Description(), std::string("d\0tail", 6));
    EXPECT_EQ(field.Type()->id(), arrow::Type::STRUCT);

    auto sub_fields = field.Type()->fields();
    ASSERT_EQ(sub_fields.size(), 3);
    ASSERT_OK_AND_ASSIGN(auto sub1, DataField::ConvertArrowFieldToDataField(sub_fields[0]));
    ASSERT_EQ(sub1, DataField(1, arrow::field("sub1", arrow::date32())));
    ASSERT_OK_AND_ASSIGN(auto sub4, DataField::ConvertArrowFieldToDataField(sub_fields[1]));
    ASSERT_EQ(sub4, DataField(4, arrow::field("sub4", arrow::binary())));
    ASSERT_OK_AND_ASSIGN(auto sub5, DataField::ConvertArrowFieldToDataField(sub_fields[2]));
    ASSERT_TRUE(BlobUtils::IsBlobField(sub5.ArrowField()));
}

TEST_F(DataFieldTest, FromJsonFailed) {
    auto check_result = [&](const std::string& json_str, const std::string& error_message) {
        try {
            rapidjson::Document doc;
            doc.Parse(json_str);
            DataField field;
            field.FromJson(doc);
            FAIL() << "Expected std::invalid_argument";
        } catch (const std::invalid_argument& e) {
            // Validate the exception type and message
            std::string msg(e.what());
            ASSERT_TRUE(msg.find(error_message) != std::string::npos) << e.what();
        } catch (...) {
            // Handle unexpected exception types
            ASSERT_TRUE(false);
        }
    };
    {
        std::string json_str = R"({
    "id" : 0,
    "name" : "f0",
    "type" : {
      "type" : "ROW",
      "fields" : [ {
        "id" : 1,
        "name" : "sub1"
       }, {
        "id" : 4,
        "name" : "sub4",
        "type" : "BYTES"
       }]}
})";
        check_result(json_str, "key 'type' must exist");
    }
    {
        std::string json_str = R"({
    "id" : 0,
    "name" : "f0",
    "type" : {
      "type" : "ROW",
      "fields" : [ {
        "id" : 1,
        "name" : "sub1",
        "type" : "EMPTY_BYTES"
       }, {
        "id" : 4,
        "name" : "sub4",
        "type" : "BYTES"
       }]}
})";
        check_result(json_str, "parse data type failed, error msg: ");
    }
    const std::vector<std::pair<const char*, const char*>> test_cases = {
        {R"([{"id":0,"name":"a","type":"INT"}])", "data field must be an object"},
        {R"({"id":"0","name":"a","type":"INT"})", "value of key 'id' must be int"},
        {R"({"name":"a","type":"INT"})", "key 'id' must exist"},
        {R"({"id":null,"name":"a","type":"INT"})", "key 'id' must exist"},
        {R"({"id":0,"type":"INT"})", "key 'name' must exist"},
        {R"({"id":0,"name":0,"type":"INT"})", "value of key 'name' must be string"},
        {R"({"id":0,"name":"a"})", "key 'type' must exist"},
        {R"({"id":0,"name":"a","type":"INT","description":0})",
         "value of key 'description' must be string"},
        {R"({"id":0,"name":"a","type":{"type":"ROW",
               "fields":[{"name":"b","type":"INT"}]}})",
         "key 'id' must exist"},
        {R"({"id":0,"name":"a","type":{"type":"ARRAY","element":{"type":"ROW",
               "fields":[{"name":"b","type":"INT"}]}}})",
         "key 'id' must exist"},
        {R"({"id":0,"name":"a","type":{"type":"MAP","key":"STRING",
               "value":{"type":"ROW","fields":[{"name":"b","type":"INT"}]}}})",
         "key 'id' must exist"},
    };
    for (const auto& [json, error_msg] : test_cases) {
        SCOPED_TRACE(json);
        ASSERT_NO_FATAL_FAILURE(check_result(json, error_msg));
    }
}

TEST_F(DataFieldTest, ToJson) {
    std::string expected_field1_json = R"({
    "id": 1,
    "name": "field1",
    "type": "INT",
    "description": "description1"
})";

    ASSERT_OK_AND_ASSIGN(std::string actual_field1_json, field1_.ToJsonString());
    ASSERT_EQ(expected_field1_json, actual_field1_json);

    std::string expected_field2_json = R"({
    "id": 2,
    "name": "field2",
    "type": "STRING"
})";

    ASSERT_OK_AND_ASSIGN(std::string actual_field2_json, field2_.ToJsonString());
    ASSERT_EQ(expected_field2_json, actual_field2_json);

    std::string expected_field3_json = R"({
    "id": 0,
    "name": "field3",
    "type": {
        "type": "ROW",
        "fields": [
            {
                "id": -1,
                "name": "field1",
                "type": "INT"
            },
            {
                "id": -1,
                "name": "field2",
                "type": "STRING"
            }
        ]
    }
})";
    ASSERT_OK_AND_ASSIGN(std::string actual_field3_json, field3_.ToJsonString());
    ASSERT_EQ(expected_field3_json, actual_field3_json);
}

TEST_F(DataFieldTest, TestProjectFields) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f0", arrow::boolean())),
        DataField(1, arrow::field("f1", arrow::int8())),
        DataField(2, arrow::field("f2", arrow::int16())),
        DataField(3, arrow::field("f3", arrow::int32())),
        DataField(4, arrow::field("f4", arrow::int64())),
        DataField(5, arrow::field("f5", arrow::float32())),
        DataField(6, arrow::field("f6", arrow::float64())),
        DataField(7, arrow::field("f7", arrow::utf8())),
        DataField(8, arrow::field("f8", arrow::binary())),
        DataField(9, arrow::field("f9", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(10, arrow::field("f10", arrow::date32())),
        DataField(11, arrow::field("f11", arrow::decimal128(2, 2))),
    };
    {
        ASSERT_OK_AND_ASSIGN(std::vector<DataField> projected_fields,
                             DataField::ProjectFields(fields, std::nullopt));
        ASSERT_EQ(projected_fields, fields);
    }
    {
        ASSERT_OK_AND_ASSIGN(
            std::vector<DataField> projected_fields,
            DataField::ProjectFields(fields, std::vector<std::string>({"f0", "f2", "f10", "f4"})));
        ASSERT_EQ(projected_fields,
                  std::vector<DataField>({fields[0], fields[2], fields[10], fields[4]}));
    }
    {
        ASSERT_OK_AND_ASSIGN(std::vector<DataField> projected_fields,
                             DataField::ProjectFields(fields, std::vector<std::string>({})));
        ASSERT_TRUE(projected_fields.empty());
    }
    {
        ASSERT_NOK_WITH_MSG(
            DataField::ProjectFields(fields, std::vector<std::string>({"f0", "non-exist"})),
            "projected field non-exist not in src field set");
    }
}

}  // namespace paimon::test
