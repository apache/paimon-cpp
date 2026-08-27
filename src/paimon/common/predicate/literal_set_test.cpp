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

#include "paimon/common/predicate/literal_set.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_dict.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class LiteralSetTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    static Literal StringLiteral(const std::string& value) {
        return Literal(FieldType::STRING, value.data(), value.size());
    }

    static Literal BinaryLiteral(const std::string& value) {
        return Literal(FieldType::BINARY, value.data(), value.size());
    }

    // Probes `array` and returns the per row result, asserting the whole call succeeded.
    static std::vector<char> Probe(const LiteralSet& literal_set,
                                   const std::shared_ptr<arrow::Array>& array, bool negate) {
        std::vector<char> is_valid(array->length(), 0);
        Status status = literal_set.TestArray(*array, negate, &is_valid);
        EXPECT_TRUE(status.ok()) << status.ToString();
        return is_valid;
    }
};

TEST_F(LiteralSetTest, TestDenseIntegers) {
    // 1..4 out of a span of 5 stays within the dense threshold.
    auto literal_set =
        LiteralSet::CreateOrNull(FieldType::INT, {Literal(1), Literal(2), Literal(3), Literal(5)});
    ASSERT_TRUE(literal_set);

    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1, 2, 3, 4, 5, 6, null])")
            .ValueOrDie();
    ASSERT_TRUE(literal_set->MatchesArrowType(*array));
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/false),
              std::vector<char>({0, 1, 1, 1, 0, 1, 0, 0}));
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/true),
              std::vector<char>({1, 0, 0, 0, 1, 0, 1, 0}));
}

TEST_F(LiteralSetTest, TestSparseIntegers) {
    // A span of 2000001 for 3 literals is far beyond the dense threshold.
    auto literal_set = LiteralSet::CreateOrNull(
        FieldType::BIGINT,
        {Literal(int64_t{-1000000}), Literal(int64_t{0}), Literal(int64_t{1000000})});
    ASSERT_TRUE(literal_set);

    auto array = arrow::ipc::internal::json::ArrayFromJSON(
                     arrow::int64(), R"([-1000000, -999999, 0, 1, 1000000, null])")
                     .ValueOrDie();
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/false), std::vector<char>({1, 0, 1, 0, 1, 0}));
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/true), std::vector<char>({0, 1, 0, 1, 0, 0}));
}

TEST_F(LiteralSetTest, TestTinyIntAndSmallInt) {
    auto tinyint_set = LiteralSet::CreateOrNull(
        FieldType::TINYINT, {Literal(int8_t{-128}), Literal(int8_t{0}), Literal(int8_t{127})});
    ASSERT_TRUE(tinyint_set);
    auto tinyint_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int8(), R"([-128, -127, 0, 126, 127])")
            .ValueOrDie();
    ASSERT_TRUE(tinyint_set->MatchesArrowType(*tinyint_array));
    ASSERT_EQ(Probe(*tinyint_set, tinyint_array, /*negate=*/false),
              std::vector<char>({1, 0, 1, 0, 1}));

    auto smallint_set = LiteralSet::CreateOrNull(
        FieldType::SMALLINT, {Literal(int16_t{-30000}), Literal(int16_t{30000})});
    ASSERT_TRUE(smallint_set);
    auto smallint_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int16(), R"([-30000, 0, 30000, null])")
            .ValueOrDie();
    ASSERT_TRUE(smallint_set->MatchesArrowType(*smallint_array));
    ASSERT_EQ(Probe(*smallint_set, smallint_array, /*negate=*/false),
              std::vector<char>({1, 0, 1, 0}));
}

TEST_F(LiteralSetTest, TestDate) {
    auto literal_set = LiteralSet::CreateOrNull(
        FieldType::DATE, {Literal(FieldType::DATE, 100), Literal(FieldType::DATE, 20000)});
    ASSERT_TRUE(literal_set);

    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::date32(), R"([100, 101, 20000, null])")
            .ValueOrDie();
    ASSERT_TRUE(literal_set->MatchesArrowType(*array));
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/false), std::vector<char>({1, 0, 1, 0}));

    ASSERT_OK_AND_ASSIGN(bool hit,
                         literal_set->TestValue(Literal(FieldType::DATE, 100), /*negate=*/false));
    ASSERT_TRUE(hit);
    ASSERT_OK_AND_ASSIGN(bool miss,
                         literal_set->TestValue(Literal(FieldType::DATE, 101), /*negate=*/false));
    ASSERT_FALSE(miss);
}

TEST_F(LiteralSetTest, TestBoolean) {
    auto true_only = LiteralSet::CreateOrNull(FieldType::BOOLEAN, {Literal(true)});
    ASSERT_TRUE(true_only);
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::boolean(), R"([true, false, null])")
            .ValueOrDie();
    ASSERT_TRUE(true_only->MatchesArrowType(*array));
    ASSERT_EQ(Probe(*true_only, array, /*negate=*/false), std::vector<char>({1, 0, 0}));
    ASSERT_EQ(Probe(*true_only, array, /*negate=*/true), std::vector<char>({0, 1, 0}));

    auto both = LiteralSet::CreateOrNull(FieldType::BOOLEAN, {Literal(true), Literal(false)});
    ASSERT_TRUE(both);
    ASSERT_EQ(Probe(*both, array, /*negate=*/false), std::vector<char>({1, 1, 0}));
    ASSERT_EQ(Probe(*both, array, /*negate=*/true), std::vector<char>({0, 0, 0}));
}

TEST_F(LiteralSetTest, TestString) {
    auto literal_set = LiteralSet::CreateOrNull(
        FieldType::STRING, {StringLiteral("apple"), StringLiteral(""), StringLiteral("banana")});
    ASSERT_TRUE(literal_set);

    auto array = arrow::ipc::internal::json::ArrayFromJSON(
                     arrow::utf8(), R"(["apple", "", "banana", "cherry", "app", null])")
                     .ValueOrDie();
    ASSERT_TRUE(literal_set->MatchesArrowType(*array));
    // "cherry" is rejected by the first byte bitmap, "app" by the length range.
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/false), std::vector<char>({1, 1, 1, 0, 0, 0}));
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/true), std::vector<char>({0, 0, 0, 1, 1, 0}));

    ASSERT_OK_AND_ASSIGN(bool hit, literal_set->TestValue(StringLiteral("banana"),
                                                          /*negate=*/false));
    ASSERT_TRUE(hit);
    ASSERT_OK_AND_ASSIGN(bool empty_hit, literal_set->TestValue(StringLiteral(""),
                                                                /*negate=*/false));
    ASSERT_TRUE(empty_hit);
    ASSERT_OK_AND_ASSIGN(bool miss, literal_set->TestValue(StringLiteral("apples"),
                                                           /*negate=*/false));
    ASSERT_FALSE(miss);
}

TEST_F(LiteralSetTest, TestStringWithoutEmptyLiteral) {
    // An empty column value must not match when no empty literal was given, even though it passes
    // the first byte bitmap trivially.
    auto literal_set = LiteralSet::CreateOrNull(FieldType::STRING, {StringLiteral("a")});
    ASSERT_TRUE(literal_set);
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "", "b"])").ValueOrDie();
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/false), std::vector<char>({1, 0, 0}));
}

TEST_F(LiteralSetTest, TestBinary) {
    auto literal_set = LiteralSet::CreateOrNull(
        FieldType::BINARY, {BinaryLiteral(std::string("\x00\x01", 2)), BinaryLiteral("xyz")});
    ASSERT_TRUE(literal_set);

    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append(std::string("\x00\x01", 2)).ok());
    ASSERT_TRUE(builder.Append("xyz").ok());
    ASSERT_TRUE(builder.Append("xyw").ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ASSERT_TRUE(literal_set->MatchesArrowType(*array));
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/false), std::vector<char>({1, 1, 0, 0}));
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/true), std::vector<char>({0, 0, 1, 0}));
}

TEST_F(LiteralSetTest, TestDictionaryString) {
    auto literal_set =
        LiteralSet::CreateOrNull(FieldType::STRING, {StringLiteral("a"), StringLiteral("c")});
    ASSERT_TRUE(literal_set);

    auto dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "c"])").ValueOrDie();
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1, 2, 2, null, 0])")
            .ValueOrDie();
    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    auto array = arrow::DictionaryArray::FromArrays(dict_type, indices, dictionary).ValueOrDie();

    ASSERT_TRUE(literal_set->MatchesArrowType(*array));
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/false), std::vector<char>({1, 0, 1, 1, 0, 1}));
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/true), std::vector<char>({0, 1, 0, 0, 0, 0}));
}

TEST_F(LiteralSetTest, TestNullLiteralIgnoredForIn) {
    auto literal_set = LiteralSet::CreateOrNull(
        FieldType::BIGINT, {Literal(int64_t{1}), Literal(FieldType::BIGINT), Literal(int64_t{3})});
    ASSERT_TRUE(literal_set);

    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([1, 2, 3, null])")
                     .ValueOrDie();
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/false), std::vector<char>({1, 0, 1, 0}));
    // A null literal makes NOT IN false for every row.
    ASSERT_EQ(Probe(*literal_set, array, /*negate=*/true), std::vector<char>({0, 0, 0, 0}));

    ASSERT_OK_AND_ASSIGN(bool not_in, literal_set->TestValue(Literal(int64_t{2}),
                                                             /*negate=*/true));
    ASSERT_FALSE(not_in);
}

TEST_F(LiteralSetTest, TestOnlyNullLiterals) {
    auto int_set = LiteralSet::CreateOrNull(FieldType::BIGINT, {Literal(FieldType::BIGINT)});
    ASSERT_TRUE(int_set);
    auto int_array = arrow::ipc::internal::json::ArrayFromJSON(
                         arrow::int64(), R"([-9007199254740993, 0, 9007199254740993, null])")
                         .ValueOrDie();
    ASSERT_EQ(Probe(*int_set, int_array, /*negate=*/false), std::vector<char>({0, 0, 0, 0}));
    ASSERT_EQ(Probe(*int_set, int_array, /*negate=*/true), std::vector<char>({0, 0, 0, 0}));

    auto string_set = LiteralSet::CreateOrNull(FieldType::STRING, {Literal(FieldType::STRING)});
    ASSERT_TRUE(string_set);
    auto string_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["", "a", null])").ValueOrDie();
    ASSERT_EQ(Probe(*string_set, string_array, /*negate=*/false), std::vector<char>({0, 0, 0}));
}

TEST_F(LiteralSetTest, TestCreateOrNullUnsupported) {
    // Empty literals.
    ASSERT_FALSE(LiteralSet::CreateOrNull(FieldType::INT, {}));
    // Literal type differs from the field type, `Literal::CompareTo` must keep reporting the error.
    ASSERT_FALSE(LiteralSet::CreateOrNull(FieldType::BIGINT, {Literal(1)}));
    ASSERT_FALSE(LiteralSet::CreateOrNull(FieldType::STRING, {Literal(int64_t{1})}));
    // Types whose equality is not byte-wise.
    ASSERT_FALSE(LiteralSet::CreateOrNull(FieldType::DOUBLE, {Literal(1.0)}));
    ASSERT_FALSE(LiteralSet::CreateOrNull(FieldType::FLOAT, {Literal(1.0f)}));
    ASSERT_FALSE(LiteralSet::CreateOrNull(
        FieldType::DECIMAL,
        {Literal(Decimal::FromUnscaledLong(/*unscaled_long=*/10, /*precision=*/10, /*scale=*/1))}));
    ASSERT_FALSE(
        LiteralSet::CreateOrNull(FieldType::TIMESTAMP, {Literal(Timestamp::FromEpochMillis(1))}));
}

TEST_F(LiteralSetTest, TestMatchesArrowTypeMismatch) {
    auto literal_set = LiteralSet::CreateOrNull(FieldType::BIGINT, {Literal(int64_t{1})});
    ASSERT_TRUE(literal_set);

    auto int32_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([1, 2])").ValueOrDie();
    ASSERT_FALSE(literal_set->MatchesArrowType(*int32_array));
    std::vector<char> is_valid(int32_array->length(), 0);
    ASSERT_FALSE(literal_set->TestArray(*int32_array, /*negate=*/false, &is_valid).ok());

    auto double_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::float64(), R"([1.0])").ValueOrDie();
    ASSERT_FALSE(literal_set->MatchesArrowType(*double_array));

    // Unsupported dictionary layout falls back as well.
    auto dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), R"([1, 2])").ValueOrDie();
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([0, 1])").ValueOrDie();
    auto dict_array = arrow::DictionaryArray::FromArrays(
                          arrow::dictionary(arrow::int32(), arrow::int64()), indices, dictionary)
                          .ValueOrDie();
    ASSERT_FALSE(literal_set->MatchesArrowType(*dict_array));

    // A value of a different type must not be silently probed either.
    ASSERT_FALSE(literal_set->TestValue(Literal(1), /*negate=*/false).ok());
}

TEST_F(LiteralSetTest, TestValueNull) {
    auto literal_set =
        LiteralSet::CreateOrNull(FieldType::BIGINT, {Literal(int64_t{1}), Literal(int64_t{3})});
    ASSERT_TRUE(literal_set);
    ASSERT_OK_AND_ASSIGN(bool in, literal_set->TestValue(Literal(FieldType::BIGINT),
                                                         /*negate=*/false));
    ASSERT_FALSE(in);
    ASSERT_OK_AND_ASSIGN(bool not_in, literal_set->TestValue(Literal(FieldType::BIGINT),
                                                             /*negate=*/true));
    ASSERT_FALSE(not_in);
}

TEST_F(LiteralSetTest, TestSlicedArray) {
    auto literal_set = LiteralSet::CreateOrNull(FieldType::INT, {Literal(2), Literal(4)});
    ASSERT_TRUE(literal_set);
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([1, 2, 3, 4, 5, null])")
            .ValueOrDie();
    auto sliced = array->Slice(2, 4);
    ASSERT_EQ(Probe(*literal_set, sliced, /*negate=*/false), std::vector<char>({0, 1, 0, 0}));

    auto string_set = LiteralSet::CreateOrNull(FieldType::STRING, {StringLiteral("c")});
    ASSERT_TRUE(string_set);
    auto string_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "c", "d"])")
            .ValueOrDie();
    auto sliced_string = string_array->Slice(1, 3);
    ASSERT_EQ(Probe(*string_set, sliced_string, /*negate=*/false), std::vector<char>({0, 1, 0}));
}

TEST_F(LiteralSetTest, TestOutputBufferValidation) {
    auto literal_set = LiteralSet::CreateOrNull(FieldType::INT, {Literal(1)});
    ASSERT_TRUE(literal_set);
    auto array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), R"([1, 2])").ValueOrDie();
    ASSERT_FALSE(literal_set->TestArray(*array, /*negate=*/false, nullptr).ok());
    std::vector<char> too_small(1, 0);
    ASSERT_FALSE(literal_set->TestArray(*array, /*negate=*/false, &too_small).ok());
}
}  // namespace paimon::test
