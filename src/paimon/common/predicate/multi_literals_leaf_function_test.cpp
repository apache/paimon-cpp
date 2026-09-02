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

#include "paimon/common/predicate/multi_literals_leaf_function.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_dict.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/predicate/in.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/predicate/not_in.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class MultiLiteralsLeafFunctionTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    static Literal StringLiteral(const std::string& value) {
        return Literal(FieldType::STRING, value.data(), value.size());
    }

    static Literal BinaryLiteral(const std::string& value) {
        return Literal(FieldType::BINARY, value.data(), value.size());
    }

    // A decimal literal of `unscaled` as written by the digits of `precision` and `scale`.
    static Literal DecimalLiteral(int32_t precision, int32_t scale, const std::string& unscaled) {
        Result<Decimal::int128_t> value = DecimalUtils::StrToInt128(unscaled);
        EXPECT_OK(value.status());
        return Literal(Decimal(precision, scale, value.ok() ? value.value() : 0));
    }

    // A timestamp literal of `millis` since the epoch and `nanos` within the millisecond.
    static Literal TimestampLiteral(int64_t millis, int32_t nanos = 0) {
        return Literal(Timestamp::FromEpochMillis(millis, nanos));
    }

    // Evaluates the whole batch and returns the per row result, asserting the call succeeded.
    static std::vector<char> Eval(const LeafFunction& function,
                                  const std::vector<Literal>& literals,
                                  const std::shared_ptr<arrow::Array>& array) {
        Result<std::vector<char>> result = function.Test(*array, literals);
        EXPECT_OK(result.status());
        if (!result.ok()) {
            return {};
        }
        return std::move(result).value();
    }

    static std::vector<char> EvalIn(const std::vector<Literal>& literals,
                                    const std::shared_ptr<arrow::Array>& array) {
        return Eval(In::Instance(), literals, array);
    }

    static std::vector<char> EvalNotIn(const std::vector<Literal>& literals,
                                       const std::shared_ptr<arrow::Array>& array) {
        return Eval(NotIn::Instance(), literals, array);
    }
};

TEST_F(MultiLiteralsLeafFunctionTest, TestInt) {
    const std::vector<Literal> literals = {Literal(1), Literal(2), Literal(3), Literal(5)};
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1, 2, 3, 4, 5, 6, null])")
            .ValueOrDie();
    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({0, 1, 1, 1, 0, 1, 0, 0}));
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({1, 0, 0, 0, 1, 0, 1, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestBigInt) {
    const std::vector<Literal> literals = {Literal(int64_t{-1000000}), Literal(int64_t{0}),
                                           Literal(int64_t{1000000})};
    auto array = arrow::ipc::internal::json::ArrayFromJSON(
                     arrow::int64(), R"([-1000000, -999999, 0, 1, 1000000, null])")
                     .ValueOrDie();
    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({1, 0, 1, 0, 1, 0}));
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({0, 1, 0, 1, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestInt64Boundaries) {
    const std::vector<Literal> literals = {Literal(std::numeric_limits<int64_t>::min()),
                                           Literal(std::numeric_limits<int64_t>::max())};
    auto array = arrow::ipc::internal::json::ArrayFromJSON(
                     arrow::int64(), R"([-9223372036854775808, 0, 9223372036854775807, null])")
                     .ValueOrDie();
    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({1, 0, 1, 0}));
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({0, 1, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestTinyIntAndSmallInt) {
    auto tinyint_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int8(), R"([-128, -127, 0, 126, 127])")
            .ValueOrDie();
    ASSERT_EQ(
        EvalIn({Literal(int8_t{-128}), Literal(int8_t{0}), Literal(int8_t{127})}, tinyint_array),
        std::vector<char>({1, 0, 1, 0, 1}));

    auto smallint_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int16(), R"([-30000, 0, 30000, null])")
            .ValueOrDie();
    ASSERT_EQ(EvalIn({Literal(int16_t{-30000}), Literal(int16_t{30000})}, smallint_array),
              std::vector<char>({1, 0, 1, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestDate) {
    const std::vector<Literal> literals = {Literal(FieldType::DATE, 100),
                                           Literal(FieldType::DATE, 20000)};
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::date32(), R"([100, 101, 20000, null])")
            .ValueOrDie();
    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({1, 0, 1, 0}));
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({0, 1, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestBoolean) {
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::boolean(), R"([true, false, null])")
            .ValueOrDie();
    ASSERT_EQ(EvalIn({Literal(true)}, array), std::vector<char>({1, 0, 0}));
    ASSERT_EQ(EvalNotIn({Literal(true)}, array), std::vector<char>({0, 1, 0}));

    const std::vector<Literal> both = {Literal(true), Literal(false)};
    ASSERT_EQ(EvalIn(both, array), std::vector<char>({1, 1, 0}));
    ASSERT_EQ(EvalNotIn(both, array), std::vector<char>({0, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestString) {
    const std::vector<Literal> literals = {StringLiteral("apple"), StringLiteral(""),
                                           StringLiteral("banana")};
    auto array = arrow::ipc::internal::json::ArrayFromJSON(
                     arrow::utf8(), R"(["apple", "", "banana", "cherry", "app", null])")
                     .ValueOrDie();
    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({1, 1, 1, 0, 0, 0}));
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({0, 0, 0, 1, 1, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestStringWithoutEmptyLiteral) {
    // An empty column value must not match when no empty literal was given.
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "", "b"])").ValueOrDie();
    ASSERT_EQ(EvalIn({StringLiteral("a")}, array), std::vector<char>({1, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestBinary) {
    // Binary literals keep their embedded zero bytes, the value set must not truncate them. The
    // JSON reader cannot spell a value like that, so the column is built with a builder.
    const std::vector<Literal> literals = {BinaryLiteral(std::string("\x00\x01", 2)),
                                           BinaryLiteral("xyz")};

    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append(std::string("\x00\x01", 2)).ok());
    ASSERT_TRUE(builder.Append("xyz").ok());
    ASSERT_TRUE(builder.Append("xyw").ok());
    ASSERT_TRUE(builder.Append(std::string("\x00", 1)).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({1, 1, 0, 0, 0}));
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({0, 0, 1, 1, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestDictionaryString) {
    const std::vector<Literal> literals = {StringLiteral("a"), StringLiteral("c")};
    auto dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "c"])").ValueOrDie();
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1, 2, 2, null, 0])")
            .ValueOrDie();
    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    std::shared_ptr<arrow::Array> array =
        arrow::DictionaryArray::FromArrays(dict_type, indices, dictionary).ValueOrDie();

    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({1, 0, 1, 1, 0, 1}));
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({0, 1, 0, 0, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestLargeStringDictionary) {
    auto dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::large_utf8(), R"(["a", "b", "c"])")
            .ValueOrDie();
    auto indices = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([0, 2, null, 1])")
                       .ValueOrDie();
    auto dict_type = arrow::dictionary(arrow::int64(), arrow::large_utf8());
    std::shared_ptr<arrow::Array> array =
        arrow::DictionaryArray::FromArrays(dict_type, indices, dictionary).ValueOrDie();

    ASSERT_EQ(EvalIn({StringLiteral("c")}, array), std::vector<char>({0, 1, 0, 0}));
    ASSERT_EQ(EvalNotIn({StringLiteral("c")}, array), std::vector<char>({1, 0, 0, 1}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestDictionaryWithNullValue) {
    // `is_in` decodes the dictionary, so a row pointing at a null dictionary value becomes a null
    // row and is false for both `IN` and `NOT IN`. An empty literal must not match it.
    auto dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"([null, "a"])").ValueOrDie();
    auto indices = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1, null, 0])")
                       .ValueOrDie();
    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    std::shared_ptr<arrow::Array> array =
        arrow::DictionaryArray::FromArrays(dict_type, indices, dictionary).ValueOrDie();

    const std::vector<Literal> with_empty = {StringLiteral("a"), StringLiteral("")};
    ASSERT_EQ(EvalIn(with_empty, array), std::vector<char>({0, 1, 0, 0}));
    ASSERT_EQ(EvalNotIn(with_empty, array), std::vector<char>({0, 0, 0, 0}));

    ASSERT_EQ(EvalIn({StringLiteral("a")}, array), std::vector<char>({0, 1, 0, 0}));
    ASSERT_EQ(EvalNotIn({StringLiteral("a")}, array), std::vector<char>({0, 0, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestNullLiteralIgnoredForIn) {
    const std::vector<Literal> literals = {Literal(int64_t{1}), Literal(FieldType::BIGINT),
                                           Literal(int64_t{3})};
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([1, 2, 3, null])")
                     .ValueOrDie();
    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({1, 0, 1, 0}));
    // A null literal makes `NOT IN` false for every row.
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({0, 0, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestOnlyNullLiterals) {
    const std::vector<Literal> int_literals = {Literal(FieldType::BIGINT)};
    auto int_array = arrow::ipc::internal::json::ArrayFromJSON(
                         arrow::int64(), R"([-9007199254740993, 0, 9007199254740993, null])")
                         .ValueOrDie();
    ASSERT_EQ(EvalIn(int_literals, int_array), std::vector<char>({0, 0, 0, 0}));
    ASSERT_EQ(EvalNotIn(int_literals, int_array), std::vector<char>({0, 0, 0, 0}));

    auto string_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["", "a", null])").ValueOrDie();
    ASSERT_EQ(EvalIn({Literal(FieldType::STRING)}, string_array), std::vector<char>({0, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestEmptyLiterals) {
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([1, 2, null])").ValueOrDie();
    ASSERT_EQ(EvalIn({}, array), std::vector<char>({0, 0, 0}));
    ASSERT_EQ(EvalNotIn({}, array), std::vector<char>({1, 1, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestTimestamp) {
    // A timestamp literal settles the time unit of its value set itself, the finest unit that
    // keeps every literal, so a column of that very unit is probed by the value set.
    auto milli_array = arrow::ipc::internal::json::ArrayFromJSON(
                           arrow::timestamp(arrow::TimeUnit::MILLI), R"([1000, 2000, 3000, null])")
                           .ValueOrDie();
    const std::vector<Literal> milli_literals = {TimestampLiteral(1000), TimestampLiteral(3000)};
    ASSERT_EQ(EvalIn(milli_literals, milli_array), std::vector<char>({1, 0, 1, 0}));
    ASSERT_EQ(EvalNotIn(milli_literals, milli_array), std::vector<char>({0, 1, 0, 0}));

    // A nanos-of-millisecond outside a microsecond forces the nanosecond unit, which the values
    // keep through the value set.
    auto nano_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::timestamp(arrow::TimeUnit::NANO),
                                                  R"([1000000000000, 1000000456789, null])")
            .ValueOrDie();
    const std::vector<Literal> nano_literals = {TimestampLiteral(1000000, 456789)};
    ASSERT_EQ(EvalIn(nano_literals, nano_array), std::vector<char>({0, 1, 0}));
    ASSERT_EQ(EvalNotIn(nano_literals, nano_array), std::vector<char>({1, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestDecimal) {
    // A decimal literal carries the precision and the scale of its own value, so a column of that
    // scale is probed by the value set.
    const std::vector<Literal> literals = {DecimalLiteral(10, 2, "100"),
                                           DecimalLiteral(10, 2, "-250")};
    auto array = arrow::ipc::internal::json::ArrayFromJSON(
                     arrow::decimal128(10, 2), R"(["1.00", "1.01", "-2.50", "0.00", null])")
                     .ValueOrDie();
    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({1, 0, 1, 0, 0}));
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({0, 1, 0, 1, 0}));

    // A column of the same scale but another precision is probed too, the value set is widened to
    // it without ever changing a value.
    auto wider_array = arrow::ipc::internal::json::ArrayFromJSON(
                           arrow::decimal128(38, 2), R"(["1.00", "1.01", "-2.50", null])")
                           .ValueOrDie();
    ASSERT_EQ(EvalIn(literals, wider_array), std::vector<char>({1, 0, 1, 0}));
    ASSERT_EQ(EvalNotIn(literals, wider_array), std::vector<char>({0, 1, 0, 0}));

    // A value beyond 64 bits keeps its high bits through the value set.
    const std::vector<Literal> wide_literals = {DecimalLiteral(38, 2, "12345678998765432134567"),
                                                DecimalLiteral(38, 2, "-12345678998765432134567")};
    ASSERT_EQ(EvalIn(wide_literals,
                     arrow::ipc::internal::json::ArrayFromJSON(
                         arrow::decimal128(38, 2),
                         R"(["123456789987654321345.67", "-123456789987654321345.67", "1.00"])")
                         .ValueOrDie()),
              std::vector<char>({1, 1, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestDecimalNullLiterals) {
    const std::vector<Literal> literals = {DecimalLiteral(10, 2, "100"),
                                           Literal(FieldType::DECIMAL)};
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::decimal128(10, 2),
                                                           R"(["1.00", "2.00", null])")
                     .ValueOrDie();
    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({1, 0, 0}));
    // A null literal makes `NOT IN` false for every row.
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({0, 0, 0}));

    // Nothing but null literals leaves no precision and scale to write the value set with, so the
    // row by row path takes over and matches nothing either way.
    ASSERT_EQ(EvalIn({Literal(FieldType::DECIMAL)}, array), std::vector<char>({0, 0, 0}));
    ASSERT_EQ(EvalNotIn({Literal(FieldType::DECIMAL)}, array), std::vector<char>({0, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestTimestampNullLiterals) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::timestamp(arrow::TimeUnit::MILLI),
                                                           R"([1000, 2000, null])")
                     .ValueOrDie();
    const std::vector<Literal> literals = {TimestampLiteral(1000), Literal(FieldType::TIMESTAMP)};
    ASSERT_EQ(EvalIn(literals, array), std::vector<char>({1, 0, 0}));
    // A null literal makes `NOT IN` false for every row.
    ASSERT_EQ(EvalNotIn(literals, array), std::vector<char>({0, 0, 0}));

    // Nothing but null literals leaves no time unit to write the value set with, so the row by row
    // path takes over and matches nothing either way.
    ASSERT_EQ(EvalIn({Literal(FieldType::TIMESTAMP)}, array), std::vector<char>({0, 0, 0}));
    ASSERT_EQ(EvalNotIn({Literal(FieldType::TIMESTAMP)}, array), std::vector<char>({0, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestDecimalOffTheValueSetPath) {
    // `Literal::CompareTo` compares two decimals by value, so a literal of another scale still
    // matches the same number. The value set would have `is_in` cast one side to the other, so a
    // column of another scale keeps the row by row path and the result stays the same either way.
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::decimal128(10, 2),
                                                           R"(["1.00", "2.00", null])")
                     .ValueOrDie();
    ASSERT_EQ(EvalIn({DecimalLiteral(20, 4, "10000")}, array), std::vector<char>({1, 0, 0}));
    ASSERT_EQ(EvalNotIn({DecimalLiteral(20, 4, "10000")}, array), std::vector<char>({0, 1, 0}));

    // Literals that do not share one precision and scale cannot be one arrow array, so they keep
    // the row by row path as well.
    ASSERT_EQ(EvalIn({DecimalLiteral(10, 2, "100"), DecimalLiteral(20, 4, "20000")}, array),
              std::vector<char>({1, 1, 0}));

    // Casting between those scales is what the row by row path spares: neither side of this pair
    // fits the scale of the other, which would make `is_in` report an error where comparing by
    // value merely finds no match.
    auto lossy_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::decimal128(38, 2), R"(["1.01", "2.00"])")
            .ValueOrDie();
    const std::vector<Literal> huge_literals = {
        DecimalLiteral(38, 0, "99999999999999999999999999999999999999")};
    ASSERT_EQ(EvalIn(huge_literals, lossy_array), std::vector<char>({0, 0}));
    ASSERT_EQ(EvalNotIn(huge_literals, lossy_array), std::vector<char>({1, 1}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestTimestampOffTheValueSetPath) {
    // A column of a coarser unit than the literals need keeps the row by row path, because `is_in`
    // would cast the value set to the column unit and fail on a value the unit does not keep. A
    // value that needs the finer unit cannot exist in the coarser column, so comparing by value
    // merely finds no match.
    auto milli_array = arrow::ipc::internal::json::ArrayFromJSON(
                           arrow::timestamp(arrow::TimeUnit::MILLI), R"([1000, 2000, null])")
                           .ValueOrDie();
    ASSERT_EQ(EvalIn({TimestampLiteral(1, 500000)}, milli_array), std::vector<char>({0, 0, 0}));
    ASSERT_EQ(EvalNotIn({TimestampLiteral(1, 500000)}, milli_array), std::vector<char>({1, 1, 0}));

    // A column of a finer unit than the literals need keeps the row by row path as well, casting
    // to it can overflow int64 where comparing by value still matches the same instant.
    auto nano_array = arrow::ipc::internal::json::ArrayFromJSON(
                          arrow::timestamp(arrow::TimeUnit::NANO), R"([1000000000, null])")
                          .ValueOrDie();
    ASSERT_EQ(EvalIn({TimestampLiteral(1000)}, nano_array), std::vector<char>({1, 0}));
    ASSERT_EQ(EvalNotIn({TimestampLiteral(1000)}, nano_array), std::vector<char>({0, 0}));

    // A column with a time zone as well: `is_in` refuses to compare a zoned timestamp against an
    // unzoned value set, so the row by row path compares by value instead.
    auto zoned_array = arrow::ipc::internal::json::ArrayFromJSON(
                           arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"), R"([1000, 2000, null])")
                           .ValueOrDie();
    ASSERT_EQ(EvalIn({TimestampLiteral(1000)}, zoned_array), std::vector<char>({1, 0, 0}));
    ASSERT_EQ(EvalNotIn({TimestampLiteral(1000)}, zoned_array), std::vector<char>({0, 1, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestFloatAndDouble) {
    auto double_array = arrow::ipc::internal::json::ArrayFromJSON(
                            arrow::float64(), R"([1.0, 2.5, -3.25, Inf, -Inf, null])")
                            .ValueOrDie();
    const std::vector<Literal> double_literals = {Literal(1.0), Literal(-3.25),
                                                  Literal(std::numeric_limits<double>::infinity())};
    ASSERT_EQ(EvalIn(double_literals, double_array), std::vector<char>({1, 0, 1, 1, 0, 0}));
    ASSERT_EQ(EvalNotIn(double_literals, double_array), std::vector<char>({0, 1, 0, 0, 1, 0}));

    auto float_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::float32(), R"([1.5, 2.5, -Inf, null])")
            .ValueOrDie();
    const std::vector<Literal> float_literals = {Literal(1.5f),
                                                 Literal(-std::numeric_limits<float>::infinity())};
    ASSERT_EQ(EvalIn(float_literals, float_array), std::vector<char>({1, 0, 1, 0}));
    ASSERT_EQ(EvalNotIn(float_literals, float_array), std::vector<char>({0, 1, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestNegativeZeroDoesNotMatchPositiveZero) {
    // `FieldsComparator::CompareFloatingPoint` orders `-0.0 < +0.0`, so they are two distinct
    // values. `is_in` agrees only because it hashes the raw bits of a float, which arrow's own
    // comment marks as something it would rather change. Should it ever hash equal floats alike,
    // `-0.0` would start matching `IN (0.0)` and this assertion is what catches the divergence.
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::float64(), R"([0.0, -0.0, 1.0])")
                     .ValueOrDie();
    ASSERT_EQ(EvalIn({Literal(0.0)}, array), std::vector<char>({1, 0, 0}));
    ASSERT_EQ(EvalIn({Literal(-0.0)}, array), std::vector<char>({0, 1, 0}));
    ASSERT_EQ(EvalNotIn({Literal(0.0)}, array), std::vector<char>({0, 1, 1}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestNanLiteralStaysOffTheValueSetPath) {
    // `is_in` hashes the raw bits of a float, so a NaN literal would only match the column NaNs
    // carrying the very same bit pattern, while `FieldsComparator::CompareFloatingPoint` makes
    // every NaN equal. A NaN literal therefore keeps the row by row path, where the sign flipped
    // NaN below still matches. The JSON reader cannot spell a sign flipped NaN, so the column is
    // built with a builder.
    const double canonical_nan = std::numeric_limits<double>::quiet_NaN();
    arrow::DoubleBuilder builder;
    ASSERT_TRUE(builder.Append(canonical_nan).ok());
    ASSERT_TRUE(builder.Append(-canonical_nan).ok());
    ASSERT_TRUE(builder.Append(1.0).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ASSERT_EQ(EvalIn({Literal(canonical_nan)}, array), std::vector<char>({1, 1, 0, 0}));
    ASSERT_EQ(EvalNotIn({Literal(canonical_nan)}, array), std::vector<char>({0, 0, 1, 0}));
    ASSERT_EQ(EvalIn({Literal(1.0), Literal(canonical_nan)}, array),
              std::vector<char>({1, 1, 1, 0}));

    // With no NaN in the value set the two paths agree, because a column NaN then matches no
    // literal either way.
    ASSERT_EQ(EvalIn({Literal(1.0)}, array), std::vector<char>({0, 0, 1, 0}));
    ASSERT_EQ(EvalNotIn({Literal(1.0)}, array), std::vector<char>({1, 1, 0, 0}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestMixedLiteralTypesReportTheError) {
    // Literals of mixed types make `Literal::CompareTo` fail, so they must keep reporting the error
    // through the row by row path instead of being built into one typed value set. The column value
    // matches none of them, otherwise the comparison would stop before reaching the odd literal.
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0])").ValueOrDie();
    ASSERT_NOK(In::Instance().Test(*array, {Literal(1), Literal(int64_t{2})}));
    ASSERT_NOK(In::Instance().Test(*array, {Literal(1), StringLiteral("a")}));
    ASSERT_NOK(NotIn::Instance().Test(*array, {Literal(1), Literal(int64_t{2})}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestDictionaryLayoutsLiteralConversionRejects) {
    // Dictionary layouts that `LiteralConverter::ConvertLiteralsFromArray` rejects are decoded by
    // `is_in`, so they no longer fail the whole evaluation.
    auto int64_dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([10, 20])").ValueOrDie();
    auto int32_indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1])").ValueOrDie();
    std::shared_ptr<arrow::Array> int64_dict_array =
        arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::int64()),
                                           int32_indices, int64_dictionary)
            .ValueOrDie();
    ASSERT_NOK(LiteralConverter::ConvertLiteralsFromArray(*int64_dict_array, /*own_data=*/false));
    ASSERT_EQ(EvalIn({Literal(int64_t{10})}, int64_dict_array), std::vector<char>({1, 0}));
    ASSERT_EQ(EvalNotIn({Literal(int64_t{10})}, int64_dict_array), std::vector<char>({0, 1}));

    auto string_dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b"])").ValueOrDie();
    auto int8_indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int8(), R"([0, 1])").ValueOrDie();
    std::shared_ptr<arrow::Array> int8_dict_array =
        arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int8(), arrow::utf8()),
                                           int8_indices, string_dictionary)
            .ValueOrDie();
    ASSERT_NOK(LiteralConverter::ConvertLiteralsFromArray(*int8_dict_array, /*own_data=*/false));
    ASSERT_EQ(EvalIn({StringLiteral("a")}, int8_dict_array), std::vector<char>({1, 0}));
    ASSERT_EQ(EvalNotIn({StringLiteral("a")}, int8_dict_array), std::vector<char>({0, 1}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestProbeFailsOnUnrelatedArrowType) {
    // Nothing casts an int32 value set to a string column, so the probe reports the mismatch
    // instead of matching by chance. `PredicateValidator` already rejects such a predicate against
    // the schema, this only pins what the evaluation does if one reaches it anyway.
    auto string_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["1"])").ValueOrDie();
    ASSERT_NOK(In::Instance().Test(*string_array, {Literal(1)}));
}

TEST_F(MultiLiteralsLeafFunctionTest, TestSlicedArray) {
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([1, 2, 3, 4, 5, null])")
            .ValueOrDie();
    auto sliced = array->Slice(2, 4);
    ASSERT_EQ(EvalIn({Literal(2), Literal(4)}, sliced), std::vector<char>({0, 1, 0, 0}));
    ASSERT_EQ(EvalNotIn({Literal(2), Literal(4)}, sliced), std::vector<char>({1, 0, 1, 0}));

    auto string_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "c", "d"])")
            .ValueOrDie();
    auto sliced_string = string_array->Slice(1, 3);
    ASSERT_EQ(EvalIn({StringLiteral("c")}, sliced_string), std::vector<char>({0, 1, 0}));
}
}  // namespace paimon::test
