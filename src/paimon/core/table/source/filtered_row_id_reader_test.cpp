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

#include "paimon/core/table/source/filtered_row_id_reader.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class FilteredRowIdReaderTest : public ::testing::Test {
 protected:
    void SetUp() override {
        std::vector<DataField> fields = {
            DataField(0, arrow::field("a", arrow::int32())),
            DataField(1, arrow::field("b", arrow::utf8())),
        };
        table_schema_ = std::make_shared<TableSchema>(
            /*version=*/1, /*id=*/0, fields, /*highest_field_id=*/1,
            /*partition_keys=*/std::vector<std::string>(),
            /*primary_keys=*/std::vector<std::string>(),
            /*options=*/std::map<std::string, std::string>(), /*comment=*/std::nullopt,
            /*time_millis=*/0);
    }

    Result<bool> IsExact(const std::shared_ptr<Predicate>& filter,
                         const std::set<int32_t>& contributing_field_ids) const {
        return FilteredRowIdReader::IsExact(*table_schema_, filter, contributing_field_ids);
    }

    static std::shared_ptr<Predicate> AEquals(int32_t value) {
        return PredicateBuilder::Equal(0, "a", FieldType::INT, Literal(value));
    }

    static Literal BLiteral() {
        return Literal(FieldType::STRING, "x", 1);
    }

    std::shared_ptr<TableSchema> table_schema_;
};

TEST_F(FilteredRowIdReaderTest, TestExactWhenEveryFilterFieldContributed) {
    ASSERT_OK_AND_ASSIGN(bool exact, IsExact(AEquals(1), {0}));
    ASSERT_TRUE(exact);
    ASSERT_OK_AND_ASSIGN(exact, IsExact(AEquals(1), {}));
    ASSERT_FALSE(exact);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> two_fields,
        PredicateBuilder::And(
            {AEquals(1), PredicateBuilder::Equal(1, "b", FieldType::STRING, BLiteral())}));
    ASSERT_OK_AND_ASSIGN(exact, IsExact(two_fields, {0}));
    ASSERT_FALSE(exact);
    ASSERT_OK_AND_ASSIGN(exact, IsExact(two_fields, {0, 1}));
    ASSERT_TRUE(exact);
}

TEST_F(FilteredRowIdReaderTest, TestCandidateOnlyLeavesAreNotExact) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> contains,
                         PredicateBuilder::Contains(1, "b", FieldType::STRING, BLiteral()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> ends_with,
                         PredicateBuilder::EndsWith(1, "b", FieldType::STRING, BLiteral()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> like,
                         PredicateBuilder::Like(1, "b", FieldType::STRING, BLiteral()));
    for (const auto& candidate_only : {contains, ends_with, like}) {
        ASSERT_OK_AND_ASSIGN(bool exact, IsExact(candidate_only, {1}));
        ASSERT_FALSE(exact);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> nested,
                             PredicateBuilder::Or({AEquals(1), candidate_only}));
        ASSERT_OK_AND_ASSIGN(exact, IsExact(nested, {0, 1}));
        ASSERT_FALSE(exact);
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> starts_with,
                         PredicateBuilder::StartsWith(1, "b", FieldType::STRING, BLiteral()));
    ASSERT_OK_AND_ASSIGN(bool exact, IsExact(starts_with, {1}));
    ASSERT_TRUE(exact);
}

}  // namespace paimon::test
