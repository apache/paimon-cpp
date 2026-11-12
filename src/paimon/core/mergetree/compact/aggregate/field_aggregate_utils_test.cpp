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

#include "paimon/core/mergetree/compact/aggregate/field_aggregate_utils.h"

#include <cstdint>
#include <memory>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/generic_array.h"
#include "paimon/common/data/generic_map.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(FieldAggregateUtilsTest, OwnedBinaryReusesOwnedBuffer) {
    auto pool = GetDefaultPool();
    auto bytes = std::make_shared<Bytes>("owned-value", pool.get());
    VariantType result = FieldAggregateUtils::OwnedBinary(VariantType(bytes), pool.get());
    ASSERT_EQ(DataDefine::GetVariantValue<std::shared_ptr<Bytes>>(result), bytes);
    ASSERT_EQ(DataDefine::GetStringView(result), "owned-value");
}

TEST(FieldAggregateUtilsTest, OwnedBinaryCopiesEmptyView) {
    auto pool = GetDefaultPool();
    VariantType result =
        FieldAggregateUtils::OwnedBinary(VariantType(std::string_view()), pool.get());
    const auto* bytes = DataDefine::GetVariantPtr<std::shared_ptr<Bytes>>(result);
    ASSERT_TRUE(bytes);
    ASSERT_TRUE(*bytes);
    ASSERT_EQ((*bytes)->size(), 0);
}

TEST(FieldAggregateUtilsTest, PrimitiveExtractionEqualityAndHashing) {
    struct TestCase {
        std::shared_ptr<arrow::DataType> type;
        VariantType value;
        VariantType other;
    };
    std::vector<TestCase> cases = {
        {arrow::boolean(), true, false},
        {arrow::int8(), char{7}, char{8}},
        {arrow::int16(), int16_t{-1234}, int16_t{1234}},
        {arrow::date32(), int32_t{20000}, int32_t{20001}},
        {arrow::time32(arrow::TimeUnit::MILLI), int32_t{0}, int32_t{86399999}},
        {arrow::int64(), int64_t{1} << 40, -(int64_t{1} << 40)}};
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.type->ToString());
        GenericRow row(2);
        row.SetField(0, test_case.value);
        row.SetField(1, test_case.other);
        ASSERT_OK_AND_ASSIGN(VariantType value,
                             FieldAggregateUtils::GetValue(row, 0, test_case.type));
        ASSERT_OK_AND_ASSIGN(VariantType other,
                             FieldAggregateUtils::GetValue(row, 1, test_case.type));
        ASSERT_EQ(value, test_case.value);
        ASSERT_EQ(other, test_case.other);

        ASSERT_OK_AND_ASSIGN(bool equal,
                             FieldAggregateUtils::Equals(value, test_case.value, test_case.type));
        ASSERT_TRUE(equal);
        ASSERT_OK_AND_ASSIGN(bool different,
                             FieldAggregateUtils::Equals(value, other, test_case.type));
        ASSERT_FALSE(different);

        ASSERT_TRUE(FieldAggregateUtils::IsHashableType(test_case.type));
        std::unordered_set<VariantType, detail::SemanticHash, detail::SemanticEqual> values(
            0, detail::SemanticHash{test_case.type}, detail::SemanticEqual{test_case.type});
        ASSERT_TRUE(values.insert(test_case.value).second);
        ASSERT_FALSE(values.insert(value).second);
        ASSERT_TRUE(values.insert(other).second);
        ASSERT_EQ(values.size(), 2);
    }
}

TEST(FieldAggregateUtilsTest, RowsWithDifferentFieldCountsAreNotEqual) {
    std::shared_ptr<InternalRow> lhs = std::make_shared<GenericRow>(1);
    std::shared_ptr<InternalRow> rhs = std::make_shared<GenericRow>(2);
    auto type = arrow::struct_({arrow::field("f", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(bool equal,
                         FieldAggregateUtils::Equals(VariantType(lhs), VariantType(rhs), type));
    ASSERT_FALSE(equal);
}

TEST(FieldAggregateUtilsTest, ArraysWithDifferentLengthsAreNotEqual) {
    std::shared_ptr<InternalArray> lhs =
        std::make_shared<GenericArray>(std::vector<VariantType>{int32_t{1}});
    std::shared_ptr<InternalArray> rhs =
        std::make_shared<GenericArray>(std::vector<VariantType>{int32_t{1}, int32_t{2}});
    ASSERT_OK_AND_ASSIGN(bool equal, FieldAggregateUtils::Equals(VariantType(lhs), VariantType(rhs),
                                                                 arrow::list(arrow::int32())));
    ASSERT_FALSE(equal);
}

TEST(FieldAggregateUtilsTest, MapsWithMultipleEntriesCompareByContent) {
    auto make_map = []() -> std::shared_ptr<InternalMap> {
        auto keys =
            std::make_shared<GenericArray>(std::vector<VariantType>{int32_t{1}, int32_t{2}});
        auto values =
            std::make_shared<GenericArray>(std::vector<VariantType>{int32_t{10}, int32_t{20}});
        return std::make_shared<GenericMap>(keys, values);
    };
    VariantType lhs(make_map());
    VariantType rhs(make_map());
    ASSERT_OK_AND_ASSIGN(bool equal, FieldAggregateUtils::Equals(
                                         lhs, rhs, arrow::map(arrow::int32(), arrow::int32())));
    ASSERT_TRUE(equal);
}

}  // namespace paimon::test
