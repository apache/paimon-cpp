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

#include "paimon/common/predicate/null_false_leaf_binary_function.h"

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
#include "paimon/common/predicate/contains.h"
#include "paimon/common/predicate/equal.h"
#include "paimon/common/predicate/greater_or_equal.h"
#include "paimon/common/predicate/greater_than.h"
#include "paimon/common/predicate/less_or_equal.h"
#include "paimon/common/predicate/less_than.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/predicate/not_equal.h"
#include "paimon/common/predicate/starts_with.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class NullFalseLeafBinaryFunctionTest : public ::testing::Test {
 public:
    static Literal StringLiteral(const std::string& value) {
        return Literal(FieldType::STRING, value.data(), value.size());
    }

    static Literal BinaryLiteral(const std::string& value) {
        return Literal(FieldType::BINARY, value.data(), value.size());
    }

    // A decimal literal of `unscaled` as written by the digits of `precision` and `scale`.
    static Literal DecimalLiteral(int32_t precision, int32_t scale, const std::string& unscaled) {
        EXPECT_OK_AND_ASSIGN(Decimal::int128_t value, DecimalUtils::StrToInt128(unscaled));
        return Literal(Decimal(precision, scale, value));
    }

    // A timestamp literal of `millis` since the epoch and `nanos` within the millisecond.
    static Literal TimestampLiteral(int64_t millis, int32_t nanos = 0) {
        return Literal(Timestamp::FromEpochMillis(millis, nanos));
    }

    // Evaluates the whole batch and returns the per row result, asserting the call succeeded. Both
    // go through `LeafFunction`, because the `Test` overloads a subclass declares hide the batch
    // one.
    static std::vector<char> Eval(const LeafFunction& function, const Literal& literal,
                                  const std::shared_ptr<arrow::Array>& array) {
        EXPECT_OK_AND_ASSIGN(std::vector<char> is_valid,
                             function.Test(*array, {literal}, arrow::default_memory_pool()));
        return is_valid;
    }

    static void AssertEvalFails(const LeafFunction& function, const std::vector<Literal>& literals,
                                const std::shared_ptr<arrow::Array>& array) {
        ASSERT_NOK(function.Test(*array, literals, arrow::default_memory_pool()));
    }
};

TEST_F(NullFalseLeafBinaryFunctionTest, TestInt) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1, 2, 3, null])")
                     .ValueOrDie();
    ASSERT_EQ(Eval(Equal::Instance(), Literal(2), array), std::vector<char>({0, 0, 1, 0, 0}));
    ASSERT_EQ(Eval(NotEqual::Instance(), Literal(2), array), std::vector<char>({1, 1, 0, 1, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), Literal(2), array), std::vector<char>({1, 1, 0, 0, 0}));
    ASSERT_EQ(Eval(LessOrEqual::Instance(), Literal(2), array), std::vector<char>({1, 1, 1, 0, 0}));
    ASSERT_EQ(Eval(GreaterThan::Instance(), Literal(2), array), std::vector<char>({0, 0, 0, 1, 0}));
    ASSERT_EQ(Eval(GreaterOrEqual::Instance(), Literal(2), array),
              std::vector<char>({0, 0, 1, 1, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestTinyIntSmallIntAndBigInt) {
    auto tinyint_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int8(), R"([-128, 0, 127, null])")
            .ValueOrDie();
    ASSERT_EQ(Eval(LessThan::Instance(), Literal(int8_t{0}), tinyint_array),
              std::vector<char>({1, 0, 0, 0}));
    ASSERT_EQ(Eval(GreaterThan::Instance(), Literal(int8_t{0}), tinyint_array),
              std::vector<char>({0, 0, 1, 0}));

    auto smallint_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int16(), R"([-30000, 0, 30000, null])")
            .ValueOrDie();
    ASSERT_EQ(Eval(Equal::Instance(), Literal(int16_t{-30000}), smallint_array),
              std::vector<char>({1, 0, 0, 0}));

    // The int64 boundaries have to survive the trip through the scalar the kernel compares against.
    auto bigint_array =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::int64(), R"([-9223372036854775808, 0, 9223372036854775807, null])")
            .ValueOrDie();
    ASSERT_EQ(
        Eval(LessOrEqual::Instance(), Literal(std::numeric_limits<int64_t>::min()), bigint_array),
        std::vector<char>({1, 0, 0, 0}));
    ASSERT_EQ(Eval(GreaterOrEqual::Instance(), Literal(std::numeric_limits<int64_t>::max()),
                   bigint_array),
              std::vector<char>({0, 0, 1, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestBoolean) {
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::boolean(), R"([true, false, null])")
            .ValueOrDie();
    ASSERT_EQ(Eval(Equal::Instance(), Literal(true), array), std::vector<char>({1, 0, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), Literal(true), array), std::vector<char>({0, 1, 0}));
    ASSERT_EQ(Eval(GreaterThan::Instance(), Literal(false), array), std::vector<char>({1, 0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestDate) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::date32(), R"([1, 2, 3, null])")
                     .ValueOrDie();
    const Literal second{FieldType::DATE, 2};
    ASSERT_EQ(Eval(Equal::Instance(), second, array), std::vector<char>({0, 1, 0, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), second, array), std::vector<char>({1, 0, 0, 0}));
    ASSERT_EQ(Eval(GreaterOrEqual::Instance(), second, array), std::vector<char>({0, 1, 1, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestString) {
    // `Literal::CompareTo` orders two strings by `std::string_view::compare`, which is the byte by
    // byte order a kernel compares them in, a prefix included.
    auto array = arrow::ipc::internal::json::ArrayFromJSON(
                     arrow::utf8(), R"(["apple", "", "app", "banana", null])")
                     .ValueOrDie();
    const Literal apple = StringLiteral("apple");
    ASSERT_EQ(Eval(Equal::Instance(), apple, array), std::vector<char>({1, 0, 0, 0, 0}));
    ASSERT_EQ(Eval(NotEqual::Instance(), apple, array), std::vector<char>({0, 1, 1, 1, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), apple, array), std::vector<char>({0, 1, 1, 0, 0}));
    ASSERT_EQ(Eval(LessOrEqual::Instance(), apple, array), std::vector<char>({1, 1, 1, 0, 0}));
    ASSERT_EQ(Eval(GreaterThan::Instance(), apple, array), std::vector<char>({0, 0, 0, 1, 0}));
    ASSERT_EQ(Eval(GreaterOrEqual::Instance(), apple, array), std::vector<char>({1, 0, 0, 1, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestBinary) {
    // Binary values keep their embedded zero bytes, which a kernel compares as bytes and
    // `std::string_view::compare` does too. The JSON reader cannot spell a value like that, so the
    // column is built with a builder.
    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append(std::string("\x00\x01", 2)).ok());
    ASSERT_TRUE(builder.Append(std::string("\x00", 1)).ok());
    ASSERT_TRUE(builder.Append("xyz").ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ASSERT_EQ(Eval(Equal::Instance(), BinaryLiteral(std::string("\x00\x01", 2)), array),
              std::vector<char>({1, 0, 0, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), BinaryLiteral("xyz"), array),
              std::vector<char>({1, 1, 0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestDecimal) {
    // A column carrying the scale of the literal is compared by the kernel, which compares two
    // decimals of one scale by their unscaled value, exactly what `Decimal::CompareTo` finds.
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::decimal128(10, 2),
                                                           R"(["1.00", "2.00", "3.00", null])")
                     .ValueOrDie();
    const Literal two = DecimalLiteral(10, 2, "200");
    ASSERT_EQ(Eval(Equal::Instance(), two, array), std::vector<char>({0, 1, 0, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), two, array), std::vector<char>({1, 0, 0, 0}));
    ASSERT_EQ(Eval(GreaterOrEqual::Instance(), two, array), std::vector<char>({0, 1, 1, 0}));

    // Another precision carrying the same scale is widened without losing a digit, so the kernel
    // still compares what `Decimal::CompareTo` compares.
    ASSERT_EQ(Eval(Equal::Instance(), DecimalLiteral(20, 2, "200"), array),
              std::vector<char>({0, 1, 0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestDecimalOffTheKernelPath) {
    // `Decimal::CompareTo` rescales two decimals and compares them by value, so a literal of
    // another scale still orders against the same number. A kernel would cast one side to the
    // other, which fails on a value that does not fit the other scale, so a column of another
    // scale keeps the row by row path and the result stays the same either way.
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::decimal128(10, 2),
                                                           R"(["1.00", "2.00", null])")
                     .ValueOrDie();
    const Literal one_of_scale_four = DecimalLiteral(20, 4, "10000");
    ASSERT_EQ(Eval(Equal::Instance(), one_of_scale_four, array), std::vector<char>({1, 0, 0}));
    ASSERT_EQ(Eval(GreaterThan::Instance(), one_of_scale_four, array),
              std::vector<char>({0, 1, 0}));

    // Casting between those scales is what the row by row path spares: the literal does not fit the
    // scale of the column, which would make a kernel report an error where comparing by value
    // merely orders the two.
    auto lossy_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::decimal128(38, 2), R"(["1.01", "2.00"])")
            .ValueOrDie();
    const Literal huge = DecimalLiteral(38, 0, "99999999999999999999999999999999999999");
    ASSERT_EQ(Eval(LessThan::Instance(), huge, lossy_array), std::vector<char>({1, 1}));
    ASSERT_EQ(Eval(GreaterThan::Instance(), huge, lossy_array), std::vector<char>({0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestTimestamp) {
    // A column of the unit the literal needs is compared by the kernel, both sides naming the very
    // same instant without a cast in between.
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::timestamp(arrow::TimeUnit::MILLI),
                                                           R"([1000, 2000, 3000, null])")
                     .ValueOrDie();
    const Literal two_seconds = TimestampLiteral(2000);
    ASSERT_EQ(Eval(Equal::Instance(), two_seconds, array), std::vector<char>({0, 1, 0, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), two_seconds, array), std::vector<char>({1, 0, 0, 0}));
    ASSERT_EQ(Eval(GreaterOrEqual::Instance(), two_seconds, array),
              std::vector<char>({0, 1, 1, 0}));

    // A literal that needs microseconds is compared against a column of microseconds.
    auto micro_array = arrow::ipc::internal::json::ArrayFromJSON(
                           arrow::timestamp(arrow::TimeUnit::MICRO), R"([1500000, 2000000, null])")
                           .ValueOrDie();
    ASSERT_EQ(Eval(LessThan::Instance(), TimestampLiteral(2000), micro_array),
              std::vector<char>({1, 0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestTimestampOffTheKernelPath) {
    // A column of a coarser unit than the literal needs keeps the row by row path, because a kernel
    // would cast the literal to the column unit and fail on a value the unit does not keep. Every
    // value of the coarser column is earlier than one needing the finer unit, so comparing by the
    // instant merely orders the two.
    auto milli_array = arrow::ipc::internal::json::ArrayFromJSON(
                           arrow::timestamp(arrow::TimeUnit::MILLI), R"([1000, 2000, null])")
                           .ValueOrDie();
    const Literal one_and_a_half = TimestampLiteral(1, 500000);
    ASSERT_EQ(Eval(GreaterThan::Instance(), one_and_a_half, milli_array),
              std::vector<char>({1, 1, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), one_and_a_half, milli_array),
              std::vector<char>({0, 0, 0}));

    // A column of a finer unit as well, casting the column to the unit of the literal can overflow
    // int64 where comparing by the instant still orders the same two instants.
    auto nano_array = arrow::ipc::internal::json::ArrayFromJSON(
                          arrow::timestamp(arrow::TimeUnit::NANO), R"([1000000000, null])")
                          .ValueOrDie();
    ASSERT_EQ(Eval(Equal::Instance(), TimestampLiteral(1000), nano_array),
              std::vector<char>({1, 0}));

    // A column with a time zone too: a kernel refuses to compare a zoned timestamp against an
    // unzoned scalar, so the row by row path compares the instants the values name instead.
    auto zoned_array = arrow::ipc::internal::json::ArrayFromJSON(
                           arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"), R"([1000, 2000, null])")
                           .ValueOrDie();
    ASSERT_EQ(Eval(Equal::Instance(), TimestampLiteral(1000), zoned_array),
              std::vector<char>({1, 0, 0}));
    ASSERT_EQ(Eval(GreaterThan::Instance(), TimestampLiteral(1000), zoned_array),
              std::vector<char>({0, 1, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestFloatAndDoubleStayOffTheKernelPath) {
    // `FieldsComparator::CompareFloatingPoint` orders `-0.0 < +0.0` and makes every NaN equal to
    // every NaN and greater than every other value, where a kernel follows IEEE 754 and says
    // `-0.0 == +0.0` and that no NaN compares to anything. That divergence is a property of the
    // column rather than of a literal one could look for, so a float column keeps the row by row
    // path. The JSON reader cannot spell a sign flipped NaN, so the column is built with a builder.
    const double canonical_nan = std::numeric_limits<double>::quiet_NaN();
    arrow::DoubleBuilder builder;
    ASSERT_TRUE(builder.Append(-0.0).ok());
    ASSERT_TRUE(builder.Append(0.0).ok());
    ASSERT_TRUE(builder.Append(canonical_nan).ok());
    ASSERT_TRUE(builder.Append(-canonical_nan).ok());
    ASSERT_TRUE(builder.Append(1.0).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ASSERT_EQ(Eval(Equal::Instance(), Literal(0.0), array), std::vector<char>({0, 1, 0, 0, 0, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), Literal(0.0), array),
              std::vector<char>({1, 0, 0, 0, 0, 0}));
    ASSERT_EQ(Eval(GreaterThan::Instance(), Literal(0.0), array),
              std::vector<char>({0, 0, 1, 1, 1, 0}));

    // Every NaN equals every NaN and is greater than every other value.
    ASSERT_EQ(Eval(Equal::Instance(), Literal(canonical_nan), array),
              std::vector<char>({0, 0, 1, 1, 0, 0}));
    ASSERT_EQ(Eval(GreaterThan::Instance(), Literal(canonical_nan), array),
              std::vector<char>({0, 0, 0, 0, 0, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), Literal(canonical_nan), array),
              std::vector<char>({1, 1, 0, 0, 1, 0}));

    auto float_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::float32(), R"([1.5, 2.5, -0.0, null])")
            .ValueOrDie();
    ASSERT_EQ(Eval(LessThan::Instance(), Literal(2.5f), float_array),
              std::vector<char>({1, 0, 1, 0}));
    ASSERT_EQ(Eval(Equal::Instance(), Literal(0.0f), float_array), std::vector<char>({0, 0, 0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestDictionaryString) {
    auto dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "c"])").ValueOrDie();
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1, 2, 2, null, 0])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> array =
        arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::utf8()),
                                           indices, dictionary)
            .ValueOrDie();

    const Literal b = StringLiteral("b");
    ASSERT_EQ(Eval(Equal::Instance(), b, array), std::vector<char>({0, 1, 0, 0, 0, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), b, array), std::vector<char>({1, 0, 0, 0, 0, 1}));
    ASSERT_EQ(Eval(GreaterOrEqual::Instance(), b, array), std::vector<char>({0, 1, 1, 1, 0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestLargeStringDictionary) {
    auto dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::large_utf8(), R"(["a", "b", "c"])")
            .ValueOrDie();
    auto indices = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([0, 2, null, 1])")
                       .ValueOrDie();
    std::shared_ptr<arrow::Array> array =
        arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int64(), arrow::large_utf8()),
                                           indices, dictionary)
            .ValueOrDie();

    ASSERT_EQ(Eval(Equal::Instance(), StringLiteral("c"), array), std::vector<char>({0, 1, 0, 0}));
    ASSERT_EQ(Eval(GreaterThan::Instance(), StringLiteral("b"), array),
              std::vector<char>({0, 1, 0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestDictionaryWithNullValue) {
    // A kernel decodes the dictionary, so a row pointing at a null dictionary value becomes a null
    // row and is false for every comparison. The row by row path reads the slot the index names
    // instead, which holds no value, and an empty literal used to match it. An empty literal must
    // not.
    auto dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"([null, "a"])").ValueOrDie();
    auto indices = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1, null, 0])")
                       .ValueOrDie();
    std::shared_ptr<arrow::Array> array =
        arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::utf8()),
                                           indices, dictionary)
            .ValueOrDie();

    ASSERT_EQ(Eval(Equal::Instance(), StringLiteral(""), array), std::vector<char>({0, 0, 0, 0}));
    ASSERT_EQ(Eval(Equal::Instance(), StringLiteral("a"), array), std::vector<char>({0, 1, 0, 0}));
    ASSERT_EQ(Eval(NotEqual::Instance(), StringLiteral("a"), array),
              std::vector<char>({0, 0, 0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestNullLiteralIsFalseForEveryRow) {
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1, null])").ValueOrDie();
    const Literal null_int{FieldType::INT};
    ASSERT_EQ(Eval(Equal::Instance(), null_int, array), std::vector<char>({0, 0, 0}));
    ASSERT_EQ(Eval(NotEqual::Instance(), null_int, array), std::vector<char>({0, 0, 0}));
    ASSERT_EQ(Eval(LessThan::Instance(), null_int, array), std::vector<char>({0, 0, 0}));

    const Literal null_string{FieldType::STRING};
    auto string_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", null])").ValueOrDie();
    ASSERT_EQ(Eval(GreaterThan::Instance(), null_string, string_array), std::vector<char>({0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestEmptyLiteralsReportTheError) {
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1])").ValueOrDie();
    AssertEvalFails(Equal::Instance(), {}, array);
    AssertEvalFails(LessThan::Instance(), {}, array);
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestMismatchedLiteralTypeReportsTheError) {
    // A literal typed differently from the column makes `Literal::CompareTo` fail, so it has to
    // keep reporting the error instead of reaching a kernel that would cast one side to the other
    // and compare by chance.
    auto int_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0])").ValueOrDie();
    AssertEvalFails(Equal::Instance(), {Literal(int64_t{0})}, int_array);
    AssertEvalFails(LessThan::Instance(), {Literal(int64_t{0})}, int_array);

    auto string_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["1"])").ValueOrDie();
    AssertEvalFails(Equal::Instance(), {Literal(1)}, string_array);

    // A string literal against a binary column is a mismatch too, the two are different field types
    // even though both are bytes.
    auto binary_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::binary(), R"(["1"])").ValueOrDie();
    AssertEvalFails(Equal::Instance(), {StringLiteral("1")}, binary_array);
    ASSERT_EQ(Eval(Equal::Instance(), BinaryLiteral("1"), binary_array), std::vector<char>({1}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestLayoutsLiteralConversionRejects) {
    // A layout `LiteralConverter::ConvertLiteralsFromArray` rejects keeps reporting through the row
    // by row path, because no `FieldType` says what a kernel would have to compare it as.
    auto large_string_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::large_utf8(), R"(["a"])").ValueOrDie();
    ASSERT_NOK(LiteralConverter::ConvertLiteralsFromArray(*large_string_array, /*own_data=*/false));
    AssertEvalFails(Equal::Instance(), {StringLiteral("a")}, large_string_array);

    // A dictionary of anything but the two string layouts is rejected as well, even though a kernel
    // would decode it, so that the row by row path stays the one that says no.
    auto int64_dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([10, 20])").ValueOrDie();
    auto int32_indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1])").ValueOrDie();
    std::shared_ptr<arrow::Array> int64_dict_array =
        arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::int64()),
                                           int32_indices, int64_dictionary)
            .ValueOrDie();
    ASSERT_NOK(LiteralConverter::ConvertLiteralsFromArray(*int64_dict_array, /*own_data=*/false));
    AssertEvalFails(Equal::Instance(), {Literal(int64_t{10})}, int64_dict_array);
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestStringPatternFunctionsKeepTheRowByRowPath) {
    // `STARTS_WITH`, `ENDS_WITH`, `CONTAINS` and `LIKE` are `NullFalseLeafBinaryFunction`s too, but
    // they match a pattern instead of ordering two values, so no kernel stands in for them.
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(),
                                                           R"(["apple", "pineapple", "app", null])")
                     .ValueOrDie();
    ASSERT_EQ(Eval(StartsWith::Instance(), StringLiteral("app"), array),
              std::vector<char>({1, 0, 1, 0}));
    ASSERT_EQ(Eval(Contains::Instance(), StringLiteral("apple"), array),
              std::vector<char>({1, 1, 0, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestSlicedArray) {
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([1, 2, 3, 4, 5, null])")
            .ValueOrDie();
    auto sliced = array->Slice(2, 4);
    ASSERT_EQ(Eval(LessThan::Instance(), Literal(5), sliced), std::vector<char>({1, 1, 0, 0}));
    ASSERT_EQ(Eval(Equal::Instance(), Literal(4), sliced), std::vector<char>({0, 1, 0, 0}));

    auto string_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "c", "d"])")
            .ValueOrDie();
    auto sliced_string = string_array->Slice(1, 3);
    ASSERT_EQ(Eval(GreaterThan::Instance(), StringLiteral("b"), sliced_string),
              std::vector<char>({0, 1, 1}));
    ASSERT_EQ(Eval(LessThan::Instance(), StringLiteral("d"), sliced_string),
              std::vector<char>({1, 1, 0}));
}

TEST_F(NullFalseLeafBinaryFunctionTest, TestEmptyArray) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"([])").ValueOrDie();
    ASSERT_EQ(Eval(Equal::Instance(), StringLiteral("a"), array), std::vector<char>({}));
}
}  // namespace paimon::test
