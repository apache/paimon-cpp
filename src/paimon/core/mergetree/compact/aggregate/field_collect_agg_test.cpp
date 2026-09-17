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

#include "paimon/core/mergetree/compact/aggregate/field_collect_agg.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/generic_array.h"
#include "paimon/common/data/generic_map.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/data/serializer/binary_serializer_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/core_options.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

VariantType IntArray(std::vector<VariantType> values) {
    return VariantType(
        checked_pointer_cast<InternalArray>(std::make_shared<GenericArray>(std::move(values))));
}

std::vector<int32_t> Values(const VariantType& value) {
    auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(value);
    std::vector<int32_t> values;
    for (int32_t i = 0; i < array->Size(); ++i) {
        values.push_back(array->GetInt(i));
    }
    return values;
}

Result<std::unique_ptr<FieldCollectAgg>> MakeCollectAgg(bool distinct) {
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions options,
        CoreOptions::FromMap({{"fields.f.distinct", distinct ? "true" : "false"}}));
    return FieldCollectAgg::Create(arrow::list(arrow::int32()), options, "f", GetDefaultPool());
}

Result<std::unique_ptr<FieldCollectAgg>> MakeDistinctAgg(
    const std::shared_ptr<arrow::DataType>& element_type) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                           CoreOptions::FromMap({{"fields.f.distinct", "true"}}));
    return FieldCollectAgg::Create(arrow::list(element_type), options, "f", GetDefaultPool());
}

VariantType Array(std::vector<VariantType> values) {
    return VariantType(
        checked_pointer_cast<InternalArray>(std::make_shared<GenericArray>(std::move(values))));
}

}  // namespace

TEST(FieldCollectAggTest, ConcatenatesWithoutReversing) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeCollectAgg(false));
    VariantType left = IntArray({int32_t{1}, int32_t{2}});
    VariantType right = IntArray({int32_t{3}, int32_t{4}});

    ASSERT_OK_AND_ASSIGN(VariantType result, agg->AggReversed(left, right));
    ASSERT_EQ((std::vector<int32_t>{1, 2, 3, 4}), Values(result));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BinaryArray> binary_result,
                         BinarySerializerUtils::WriteBinaryArray(
                             DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result),
                             arrow::list(arrow::int32()), GetDefaultPool().get()));
    ASSERT_EQ((std::vector<int32_t>{1, 2, 3, 4}), binary_result->ToIntArray().value());

    ASSERT_OK_AND_ASSIGN(VariantType null_result,
                         agg->Agg(VariantType(NullType()), VariantType(NullType())));
    ASSERT_TRUE(DataDefine::IsVariantNull(null_result));
}

TEST(FieldCollectAggTest, DistinctAndRetractOneOccurrence) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> distinct_agg, MakeCollectAgg(true));
    ASSERT_OK_AND_ASSIGN(VariantType distinct_result,
                         distinct_agg->Agg(IntArray({int32_t{1}, int32_t{2}, int32_t{2}}),
                                           IntArray({int32_t{2}, int32_t{3}})));
    ASSERT_EQ((std::vector<int32_t>{1, 2, 3}), Values(distinct_result));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeCollectAgg(false));
    ASSERT_OK_AND_ASSIGN(VariantType retract_result,
                         agg->Retract(IntArray({int32_t{1}, int32_t{2}, int32_t{2}, int32_t{3}}),
                                      IntArray({int32_t{2}})));
    ASSERT_EQ((std::vector<int32_t>{1, 2, 3}), Values(retract_result));
}

TEST(FieldCollectAggTest, HashPathPreservesDuplicatesOverlapAndOrder) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeCollectAgg(true));
    std::vector<VariantType> accumulator;
    std::vector<VariantType> input;
    for (int32_t i = 0; i < 100; ++i) {
        accumulator.emplace_back(i % 40);
        input.emplace_back(20 + (i % 40));
    }

    ASSERT_OK_AND_ASSIGN(VariantType result,
                         agg->Agg(IntArray(std::move(accumulator)), IntArray(std::move(input))));
    std::vector<int32_t> expected;
    for (int32_t i = 0; i < 60; ++i) {
        expected.push_back(i);
    }
    ASSERT_EQ(expected, Values(result));
}

TEST(FieldCollectAggTest, HashPathHandlesNullsAndStringContent) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> int_agg, MakeCollectAgg(true));
    std::vector<VariantType> accumulator;
    std::vector<VariantType> input;
    for (int32_t i = 0; i < 100; ++i) {
        accumulator.emplace_back(i == 0 ? VariantType(NullType()) : VariantType(int32_t{1}));
        input.emplace_back(i == 0 ? VariantType(NullType()) : VariantType(int32_t{2}));
    }
    ASSERT_OK_AND_ASSIGN(VariantType int_result, int_agg->Agg(IntArray(std::move(accumulator)),
                                                              IntArray(std::move(input))));
    auto int_array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(int_result);
    ASSERT_EQ(3, int_array->Size());
    ASSERT_TRUE(int_array->IsNullAt(0));
    ASSERT_EQ(1, int_array->GetInt(1));
    ASSERT_EQ(2, int_array->GetInt(2));

    for (const auto& element_type : {arrow::utf8(), arrow::binary()}) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> string_agg,
                             MakeDistinctAgg(element_type));
        std::vector<std::string> left(100, "same-content");
        std::vector<std::string> right(100, "same-content");
        std::vector<VariantType> left_values;
        std::vector<VariantType> right_values;
        for (const auto& value : left) {
            left_values.emplace_back(std::string_view(value));
        }
        for (const auto& value : right) {
            right_values.emplace_back(std::string_view(value));
        }
        ASSERT_OK_AND_ASSIGN(
            VariantType string_result,
            string_agg->Agg(IntArray(std::move(left_values)), IntArray(std::move(right_values))));
        auto string_array =
            DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(string_result);
        ASSERT_EQ(1, string_array->Size());
        ASSERT_EQ("same-content", string_array->GetStringView(0));
    }
}

TEST(FieldCollectAggTest, HashPathPreservesFloatingPointAndTimestampSemantics) {
    uint32_t float_bits1 = 0x7fc00001U;
    uint32_t float_bits2 = 0x7fc00002U;
    uint64_t double_bits1 = 0x7ff8000000000001ULL;
    uint64_t double_bits2 = 0x7ff8000000000002ULL;
    float float_nan1;
    float float_nan2;
    double double_nan1;
    double double_nan2;
    std::memcpy(&float_nan1, &float_bits1, sizeof(float_nan1));
    std::memcpy(&float_nan2, &float_bits2, sizeof(float_nan2));
    std::memcpy(&double_nan1, &double_bits1, sizeof(double_nan1));
    std::memcpy(&double_nan2, &double_bits2, sizeof(double_nan2));

    auto verify_floating_point = [&](const std::shared_ptr<arrow::DataType>& type,
                                     const VariantType& negative_zero, const VariantType& nan1,
                                     const VariantType& positive_zero, const VariantType& nan2) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeDistinctAgg(type));
        std::vector<VariantType> accumulator;
        std::vector<VariantType> input;
        for (int32_t i = 0; i < 100; ++i) {
            accumulator.emplace_back(i % 2 == 0 ? negative_zero : nan1);
            input.emplace_back(i % 2 == 0 ? positive_zero : nan2);
        }
        ASSERT_OK_AND_ASSIGN(VariantType result, agg->Agg(IntArray(std::move(accumulator)),
                                                          IntArray(std::move(input))));
        auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result);
        ASSERT_EQ(3, array->Size());
        if (type->id() == arrow::Type::FLOAT) {
            ASSERT_EQ(0, FieldsComparator::CompareFloatingPoint(array->GetFloat(0), -0.0F));
            ASSERT_TRUE(std::isnan(array->GetFloat(1)));
            ASSERT_EQ(0, FieldsComparator::CompareFloatingPoint(array->GetFloat(2), +0.0F));
        } else {
            ASSERT_EQ(0, FieldsComparator::CompareFloatingPoint(array->GetDouble(0), -0.0));
            ASSERT_TRUE(std::isnan(array->GetDouble(1)));
            ASSERT_EQ(0, FieldsComparator::CompareFloatingPoint(array->GetDouble(2), +0.0));
        }
    };
    verify_floating_point(arrow::float32(), VariantType(-0.0F), VariantType(float_nan1),
                          VariantType(+0.0F), VariantType(float_nan2));
    verify_floating_point(arrow::float64(), VariantType(-0.0), VariantType(double_nan1),
                          VariantType(+0.0), VariantType(double_nan2));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> timestamp_agg,
                         MakeDistinctAgg(arrow::timestamp(arrow::TimeUnit::NANO)));
    Timestamp first = Timestamp::FromEpochMillis(100, 7);
    Timestamp second = Timestamp::FromEpochMillis(101, 0);
    std::vector<VariantType> timestamps;
    std::vector<VariantType> more_timestamps;
    for (int32_t i = 0; i < 100; ++i) {
        timestamps.emplace_back(first);
        more_timestamps.emplace_back(i == 0 ? first : second);
    }
    ASSERT_OK_AND_ASSIGN(
        VariantType timestamp_result,
        timestamp_agg->Agg(IntArray(std::move(timestamps)), IntArray(std::move(more_timestamps))));
    auto timestamp_array =
        DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(timestamp_result);
    ASSERT_EQ(2, timestamp_array->Size());
    ASSERT_EQ(first, timestamp_array->GetTimestamp(0, 9));
    ASSERT_EQ(second, timestamp_array->GetTimestamp(1, 9));
}

TEST(FieldCollectAggTest, FallbackDecimalPathPreservesEquality) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg,
                         MakeDistinctAgg(arrow::decimal128(10, 2)));
    Decimal one = Decimal::FromUnscaledLong(100, 10, 2);
    Decimal same_value = Decimal::FromUnscaledLong(1, 10, 0);
    Decimal two = Decimal::FromUnscaledLong(200, 10, 2);
    std::vector<VariantType> accumulator;
    std::vector<VariantType> input;
    for (int32_t i = 0; i < 100; ++i) {
        accumulator.emplace_back(one);
        input.emplace_back(i == 0 ? same_value : two);
    }
    ASSERT_OK_AND_ASSIGN(VariantType result,
                         agg->Agg(IntArray(std::move(accumulator)), IntArray(std::move(input))));
    auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result);
    ASSERT_EQ(2, array->Size());
    ASSERT_EQ(one, array->GetDecimal(0, 10, 2));
    ASSERT_EQ(two, array->GetDecimal(1, 10, 2));
}

TEST(FieldCollectAggTest, FallbackConstructedPathPreservesEquality) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg,
                         MakeDistinctAgg(arrow::list(arrow::int32())));
    std::vector<VariantType> accumulator;
    std::vector<VariantType> input;
    for (int32_t i = 0; i < 100; ++i) {
        accumulator.emplace_back(Array({int32_t{1}, int32_t{2}}));
        input.emplace_back(i == 0 ? Array({int32_t{1}, int32_t{2}})
                                  : Array({int32_t{2}, int32_t{3}}));
    }
    ASSERT_OK_AND_ASSIGN(VariantType result,
                         agg->Agg(IntArray(std::move(accumulator)), IntArray(std::move(input))));
    auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result);
    ASSERT_EQ(2, array->Size());
    ASSERT_EQ(1, array->GetArray(0)->GetInt(0));
    ASSERT_EQ(2, array->GetArray(0)->GetInt(1));
    ASSERT_EQ(2, array->GetArray(1)->GetInt(0));
    ASSERT_EQ(3, array->GetArray(1)->GetInt(1));
}

TEST(FieldCollectAggTest, RejectsNonArrayType) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    ASSERT_NOK(FieldCollectAgg::Create(arrow::int32(), options, "f", GetDefaultPool()));
}

// Ported from Java FieldAggregatorTest#testFiledCollectAggWith{Row,Array,Map}Type: distinct
// collection over composite element types.
namespace {

VariantType IntStringRow(int32_t id, std::string_view name) {
    std::shared_ptr<GenericRow> row = std::make_shared<GenericRow>(2);
    row->SetField(0, id);
    row->SetField(1, name);
    return VariantType(checked_pointer_cast<InternalRow>(row));
}

VariantType IntStringMap(std::vector<std::pair<int32_t, std::string_view>> entries) {
    std::vector<VariantType> keys;
    std::vector<VariantType> values;
    for (const auto& entry : entries) {
        keys.emplace_back(entry.first);
        values.emplace_back(entry.second);
    }
    return VariantType(checked_pointer_cast<InternalMap>(
        std::make_shared<GenericMap>(std::make_shared<GenericArray>(std::move(keys)),
                                     std::make_shared<GenericArray>(std::move(values)))));
}

/// Decode without going through FieldAggregateUtils, so the assertions stay independent of the
/// equality code under test.
std::vector<std::string> SortedRows(const VariantType& result) {
    auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result);
    std::vector<std::string> out;
    for (int32_t i = 0; i < array->Size(); ++i) {
        std::shared_ptr<InternalRow> row = array->GetRow(i, 2);
        out.push_back(std::to_string(row->GetInt(0)) + ":" + std::string(row->GetStringView(1)));
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<std::string> SortedArrays(const VariantType& result) {
    auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result);
    std::vector<std::string> out;
    for (int32_t i = 0; i < array->Size(); ++i) {
        std::shared_ptr<InternalArray> inner = array->GetArray(i);
        std::string encoded;
        for (int32_t j = 0; j < inner->Size(); ++j) {
            encoded += std::to_string(inner->GetInt(j)) + ",";
        }
        out.push_back(encoded);
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<std::string> SortedMaps(const VariantType& result) {
    auto array = DataDefine::GetVariantValue<std::shared_ptr<InternalArray>>(result);
    std::vector<std::string> out;
    for (int32_t i = 0; i < array->Size(); ++i) {
        std::shared_ptr<InternalMap> map = array->GetMap(i);
        std::shared_ptr<InternalArray> keys = map->KeyArray();
        std::shared_ptr<InternalArray> values = map->ValueArray();
        std::vector<std::string> entries;
        for (int32_t j = 0; j < map->Size(); ++j) {
            entries.push_back(std::to_string(keys->GetInt(j)) + "=" +
                              std::string(values->GetStringView(j)));
        }
        std::sort(entries.begin(), entries.end());
        std::string encoded;
        for (const std::string& entry : entries) {
            encoded += entry + ";";
        }
        out.push_back(encoded);
    }
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

TEST(FieldCollectAggTest, DistinctOverRowElements) {
    std::shared_ptr<arrow::DataType> row_type =
        arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeDistinctAgg(row_type));

    ASSERT_OK_AND_ASSIGN(VariantType empty,
                         agg->Agg(VariantType(NullType()), VariantType(NullType())));
    ASSERT_TRUE(DataDefine::IsVariantNull(empty));

    VariantType input1 = Array({IntStringRow(1, "A"), IntStringRow(1, "B")});
    ASSERT_OK_AND_ASSIGN(VariantType first, agg->Agg(VariantType(NullType()), input1));
    ASSERT_EQ((std::vector<std::string>{"1:A", "1:B"}), SortedRows(first));

    VariantType input2 = Array({IntStringRow(1, "A"), IntStringRow(2, "A")});
    ASSERT_OK_AND_ASSIGN(VariantType merged, agg->Agg(input1, input2));
    ASSERT_EQ((std::vector<std::string>{"1:A", "1:B", "2:A"}), SortedRows(merged));
}

TEST(FieldCollectAggTest, DistinctOverArrayElements) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg,
                         MakeDistinctAgg(arrow::list(arrow::int32())));

    ASSERT_OK_AND_ASSIGN(VariantType empty,
                         agg->Agg(VariantType(NullType()), VariantType(NullType())));
    ASSERT_TRUE(DataDefine::IsVariantNull(empty));

    VariantType input1 = Array({Array({int32_t{1}, int32_t{1}}), Array({int32_t{1}, int32_t{2}})});
    ASSERT_OK_AND_ASSIGN(VariantType first, agg->Agg(VariantType(NullType()), input1));
    ASSERT_EQ((std::vector<std::string>{"1,1,", "1,2,"}), SortedArrays(first));

    VariantType input2 = Array({Array({int32_t{1}, int32_t{1}}), Array({int32_t{1}, int32_t{2}}),
                                Array({int32_t{2}, int32_t{1}})});
    ASSERT_OK_AND_ASSIGN(VariantType merged, agg->Agg(input1, input2));
    ASSERT_EQ((std::vector<std::string>{"1,1,", "1,2,", "2,1,"}), SortedArrays(merged));
}

TEST(FieldCollectAggTest, DistinctOverMapElements) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg,
                         MakeDistinctAgg(arrow::map(arrow::int32(), arrow::utf8())));

    ASSERT_OK_AND_ASSIGN(VariantType empty,
                         agg->Agg(VariantType(NullType()), VariantType(NullType())));
    ASSERT_TRUE(DataDefine::IsVariantNull(empty));

    VariantType input1 = Array({IntStringMap({{1, "A"}}), IntStringMap({{1, "A"}, {2, "B"}})});
    ASSERT_OK_AND_ASSIGN(VariantType first, agg->Agg(VariantType(NullType()), input1));
    ASSERT_EQ((std::vector<std::string>{"1=A;", "1=A;2=B;"}), SortedMaps(first));

    // the second entry has the same content as input1's, only inserted in a different order
    VariantType input2 = Array(
        {IntStringMap({{1, "A"}}), IntStringMap({{2, "B"}, {1, "A"}}), IntStringMap({{1, "C"}})});
    ASSERT_OK_AND_ASSIGN(VariantType merged, agg->Agg(input1, input2));
    ASSERT_EQ((std::vector<std::string>{"1=A;", "1=A;2=B;", "1=C;"}), SortedMaps(merged));
}

// Ported from Java FieldAggregatorTest#testFieldCollectAggRetractWith{,out}Distinct: retraction
// removes one occurrence per retracted element, for every element type.
TEST(FieldCollectAggTest, RetractRemovesOneOccurrencePerElement) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> int_agg, MakeCollectAgg(true));
    ASSERT_OK_AND_ASSIGN(
        VariantType ints,
        int_agg->Retract(IntArray({int32_t{1}, int32_t{2}, int32_t{3}}), IntArray({int32_t{1}})));
    ASSERT_EQ((std::vector<int32_t>{2, 3}), Values(ints));
    // duplicates in the accumulator are retracted one at a time
    ASSERT_OK_AND_ASSIGN(
        VariantType dups,
        int_agg->Retract(IntArray({int32_t{1}, int32_t{1}, int32_t{2}, int32_t{2}, int32_t{3}}),
                         IntArray({int32_t{1}, int32_t{2}, int32_t{3}})));
    ASSERT_EQ((std::vector<int32_t>{1, 2}), Values(dups));

    std::shared_ptr<arrow::DataType> row_type =
        arrow::struct_({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> row_agg, MakeDistinctAgg(row_type));
    ASSERT_OK_AND_ASSIGN(VariantType rows,
                         row_agg->Retract(Array({IntStringRow(1, "A"), IntStringRow(1, "A"),
                                                 IntStringRow(1, "B"), IntStringRow(2, "B")}),
                                          Array({IntStringRow(1, "A"), IntStringRow(2, "B")})));
    ASSERT_EQ((std::vector<std::string>{"1:A", "1:B"}), SortedRows(rows));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> array_agg,
                         MakeDistinctAgg(arrow::list(arrow::int32())));
    ASSERT_OK_AND_ASSIGN(
        VariantType arrays,
        array_agg->Retract(
            Array({Array({int32_t{1}, int32_t{1}}), Array({int32_t{1}, int32_t{1}}),
                   Array({int32_t{1}, int32_t{2}}), Array({int32_t{2}, int32_t{1}})}),
            Array({Array({int32_t{1}, int32_t{1}}), Array({int32_t{1}, int32_t{2}})})));
    ASSERT_EQ((std::vector<std::string>{"1,1,", "2,1,"}), SortedArrays(arrays));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> map_agg,
                         MakeDistinctAgg(arrow::map(arrow::int32(), arrow::utf8())));
    // the retracted {1=A,2=B} matches the accumulator entry written as {2=B,1=A}
    ASSERT_OK_AND_ASSIGN(
        VariantType maps,
        map_agg->Retract(Array({IntStringMap({{1, "A"}}), IntStringMap({{1, "A"}}),
                                IntStringMap({{2, "B"}, {1, "A"}}), IntStringMap({{1, "C"}})}),
                         Array({IntStringMap({{1, "A"}}), IntStringMap({{1, "A"}, {2, "B"}})})));
    ASSERT_EQ((std::vector<std::string>{"1=A;", "1=C;"}), SortedMaps(maps));
}

// Ported from Java FieldAggregatorRetractNullTest: retraction is supported and returns a value.
TEST(FieldCollectAggTest, RetractOnEmptyArraysIsSupported) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldCollectAgg> agg, MakeCollectAgg(false));
    ASSERT_OK_AND_ASSIGN(VariantType result, agg->Retract(IntArray({}), IntArray({})));
    ASSERT_TRUE(Values(result).empty());
}

}  // namespace paimon::test
