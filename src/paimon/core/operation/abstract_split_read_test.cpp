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

#include "paimon/core/operation/abstract_split_read.h"

#include <cstdint>
#include <limits>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(AbstractSplitReadTest, TestNeedCompleteRowTrackingFields) {
    std::vector<DataField> data_fields = {DataField(0, arrow::field("name", arrow::utf8())),
                                          DataField(1, arrow::field("sex", arrow::utf8())),
                                          DataField(2, arrow::field("age", arrow::int32())),
                                          SpecialFields::RowId(), SpecialFields::SequenceNumber()};
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(data_fields);
    auto fields = arrow_schema->fields();

    ASSERT_TRUE(AbstractSplitRead::NeedCompleteRowTrackingFields(/*row_tracking_enabled=*/true,
                                                                 arrow::schema(fields)));
    ASSERT_TRUE(AbstractSplitRead::NeedCompleteRowTrackingFields(
        /*row_tracking_enabled=*/true, arrow::schema({fields[0], fields[3]})));
    ASSERT_TRUE(AbstractSplitRead::NeedCompleteRowTrackingFields(
        /*row_tracking_enabled=*/true, arrow::schema({fields[0], fields[4]})));
    ASSERT_FALSE(AbstractSplitRead::NeedCompleteRowTrackingFields(
        /*row_tracking_enabled=*/true, arrow::schema({fields[0], fields[1]})));
    ASSERT_FALSE(AbstractSplitRead::NeedCompleteRowTrackingFields(/*row_tracking_enabled=*/false,
                                                                  arrow::schema(fields)));
}

TEST(AbstractSplitReadTest, TestProjectFieldsForRowTrackingAndDataEvolution) {
    {
        // test no partition
        std::vector<DataField> fields = {DataField(0, arrow::field("name", arrow::utf8())),
                                         DataField(1, arrow::field("sex", arrow::utf8())),
                                         DataField(2, arrow::field("age", arrow::int32()))};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, DataField::ConvertDataFieldsToArrowSchema(fields),
                                /*partition_keys=*/{},
                                /*primary_keys=*/{}, /*options=*/{}));

        {
            // test write_cols is std::nullopt
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, /*write_cols=*/std::nullopt));
            std::vector<DataField> expected = fields;
            expected.push_back(SpecialFields::RowId());
            expected.push_back(SpecialFields::SequenceNumber());
            ASSERT_EQ(result, expected);
        }
        {
            // test with write_cols
            std::vector<std::string> write_cols = {"name", "age"};
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, write_cols));
            std::vector<DataField> expected = {fields[0], fields[2], SpecialFields::RowId(),
                                               SpecialFields::SequenceNumber()};
            ASSERT_EQ(result, expected);
        }
        {
            // test with empty write_cols
            std::vector<std::string> write_cols = {};
            ASSERT_NOK_WITH_MSG(AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                    table_schema, write_cols),
                                "write cols cannot be empty");
        }
    }
    {
        // test with partition
        std::vector<DataField> fields = {DataField(0, arrow::field("name", arrow::utf8())),
                                         DataField(1, arrow::field("ds", arrow::utf8())),
                                         DataField(2, arrow::field("sex", arrow::utf8())),
                                         DataField(3, arrow::field("age", arrow::int32()))};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, DataField::ConvertDataFieldsToArrowSchema(fields),
                                /*partition_keys=*/{"ds"},
                                /*primary_keys=*/{}, /*options=*/{}));

        {
            // test write_cols is std::nullopt
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, /*write_cols=*/std::nullopt));
            std::vector<DataField> expected = fields;
            expected.push_back(SpecialFields::RowId());
            expected.push_back(SpecialFields::SequenceNumber());
            ASSERT_EQ(result, expected);
        }
        {
            // test with write_cols and write_cols not contain partition fields
            std::vector<std::string> write_cols = {"name", "age"};
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, write_cols));
            std::vector<DataField> expected = {fields[0], fields[3], fields[1],
                                               SpecialFields::RowId(),
                                               SpecialFields::SequenceNumber()};
            ASSERT_EQ(result, expected);
        }
        {
            // test with write_cols and write_cols contain partition fields
            std::vector<std::string> write_cols = {"age", "name", "ds"};
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, write_cols));
            std::vector<DataField> expected = {fields[3], fields[0], fields[1],
                                               SpecialFields::RowId(),
                                               SpecialFields::SequenceNumber()};
            ASSERT_EQ(result, expected);
        }
        {
            // test with write_cols and write_cols contain row tracking fields
            std::vector<std::string> write_cols = {
                "age",
                "name",
                SpecialFields::RowId().Name(),
                SpecialFields::SequenceNumber().Name(),
            };
            ASSERT_OK_AND_ASSIGN(auto result,
                                 AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                     table_schema, write_cols));
            std::vector<DataField> expected = {fields[3], fields[0], fields[1],
                                               SpecialFields::RowId(),
                                               SpecialFields::SequenceNumber()};
            ASSERT_EQ(result, expected);
        }
        {
            // test with empty write_cols
            std::vector<std::string> write_cols = {};
            ASSERT_NOK_WITH_MSG(AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
                                    table_schema, write_cols),
                                "write cols cannot be empty");
        }
    }
}

}  // namespace paimon::test
