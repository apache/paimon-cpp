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

#include "paimon/core/mergetree/compact/aggregate/field_product_agg.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
Decimal MakeDecimal(int32_t precision, int32_t scale, const std::string& unscaled) {
    return Decimal(precision, scale, DecimalUtils::StrToInt128(unscaled).value());
}
}  // namespace

TEST(FieldProductAggTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                         FieldProductAgg::Create(arrow::int32(), GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(VariantType agg_ret, field_product_agg->Agg(5, 10));
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 50);

    ASSERT_OK_AND_ASSIGN(VariantType retract_ret, field_product_agg->Retract(50, 10));
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(retract_ret), 5);
}

TEST(FieldProductAggTest, TestNull) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                         FieldProductAgg::Create(arrow::int32(), GetDefaultPool()));
    {
        ASSERT_OK_AND_ASSIGN(VariantType agg_ret, field_product_agg->Agg(5, NullType()));
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 5);
    }
    {
        ASSERT_OK_AND_ASSIGN(VariantType agg_ret, field_product_agg->Agg(NullType(), 10));
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(agg_ret), 10);
    }
    {
        ASSERT_OK_AND_ASSIGN(VariantType agg_ret, field_product_agg->Agg(NullType(), NullType()));
        ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));
    }

    // retraction keeps the accumulator whenever either side is null
    {
        ASSERT_OK_AND_ASSIGN(VariantType retract_ret, field_product_agg->Retract(5, NullType()));
        ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(retract_ret), 5);
    }
    {
        ASSERT_OK_AND_ASSIGN(VariantType retract_ret, field_product_agg->Retract(NullType(), 10));
        ASSERT_TRUE(DataDefine::IsVariantNull(retract_ret));
    }
    {
        ASSERT_OK_AND_ASSIGN(VariantType retract_ret,
                             field_product_agg->Retract(NullType(), NullType()));
        ASSERT_TRUE(DataDefine::IsVariantNull(retract_ret));
    }
}

TEST(FieldProductAggTest, TestSupportedTypes) {
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::int8(), GetDefaultPool()));
        // The variant holds TINYINT as a plain char, so read back the signed value it stands for
        // rather than comparing a char that is unsigned under some ABIs against a negative literal.
        ASSERT_OK_AND_ASSIGN(VariantType agg_ret,
                             field_product_agg->Agg(static_cast<char>(-5), static_cast<char>(20)));
        ASSERT_EQ(static_cast<int8_t>(DataDefine::GetVariantValue<char>(agg_ret)), -100);
        ASSERT_OK_AND_ASSIGN(
            VariantType retract_ret,
            field_product_agg->Retract(static_cast<char>(-100), static_cast<char>(20)));
        ASSERT_EQ(static_cast<int8_t>(DataDefine::GetVariantValue<char>(retract_ret)), -5);
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::int16(), GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(VariantType agg_ret, field_product_agg->Agg(static_cast<int16_t>(100),
                                                                         static_cast<int16_t>(15)));
        ASSERT_EQ(DataDefine::GetVariantValue<int16_t>(agg_ret), 1500);
        ASSERT_OK_AND_ASSIGN(
            VariantType retract_ret,
            field_product_agg->Retract(static_cast<int16_t>(1500), static_cast<int16_t>(15)));
        ASSERT_EQ(DataDefine::GetVariantValue<int16_t>(retract_ret), 100);
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::int64(), GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(VariantType agg_ret, field_product_agg->Agg(static_cast<int64_t>(100),
                                                                         static_cast<int64_t>(15)));
        ASSERT_EQ(DataDefine::GetVariantValue<int64_t>(agg_ret), 1500);
        ASSERT_OK_AND_ASSIGN(
            VariantType retract_ret,
            field_product_agg->Retract(static_cast<int64_t>(1500), static_cast<int64_t>(15)));
        ASSERT_EQ(DataDefine::GetVariantValue<int64_t>(retract_ret), 100);
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::float32(), GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(VariantType agg_ret, field_product_agg->Agg(static_cast<float>(1.5),
                                                                         static_cast<float>(2.5)));
        ASSERT_NEAR(DataDefine::GetVariantValue<float>(agg_ret), 3.75, 0.0001);
        ASSERT_OK_AND_ASSIGN(
            VariantType retract_ret,
            field_product_agg->Retract(static_cast<float>(3.75), static_cast<float>(2.5)));
        ASSERT_NEAR(DataDefine::GetVariantValue<float>(retract_ret), 1.5, 0.0001);
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::float64(), GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(VariantType agg_ret, field_product_agg->Agg(1.5, 2.5));
        ASSERT_NEAR(DataDefine::GetVariantValue<double>(agg_ret), 3.75, 0.0001);
        ASSERT_OK_AND_ASSIGN(VariantType retract_ret, field_product_agg->Retract(3.75, 2.5));
        ASSERT_NEAR(DataDefine::GetVariantValue<double>(retract_ret), 1.5, 0.0001);
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::decimal128(10, 2), GetDefaultPool()));
        // 1.50 * 2.00 = 3.00
        ASSERT_OK_AND_ASSIGN(
            VariantType agg_ret,
            field_product_agg->Agg(MakeDecimal(10, 2, "150"), MakeDecimal(10, 2, "200")));
        ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(agg_ret), MakeDecimal(10, 2, "300"));
        // 3.00 / 2.00 = 1.50
        ASSERT_OK_AND_ASSIGN(
            VariantType retract_ret,
            field_product_agg->Retract(MakeDecimal(10, 2, "300"), MakeDecimal(10, 2, "200")));
        ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(retract_ret), MakeDecimal(10, 2, "150"));
    }
}

TEST(FieldProductAggTest, TestIntegerRetractTruncatesTowardsZero) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                         FieldProductAgg::Create(arrow::int32(), GetDefaultPool()));
    // a negative divisor flips the sign of the quotient
    ASSERT_OK_AND_ASSIGN(VariantType negative_divisor_ret, field_product_agg->Retract(10, -5));
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(negative_divisor_ret), -2);
    // 10 / 3 drops the fraction rather than rounding it, unlike the decimal quotients
    ASSERT_OK_AND_ASSIGN(VariantType inexact_ret, field_product_agg->Retract(10, 3));
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(inexact_ret), 3);
    // -10 / 3 truncates towards zero to -3 rather than flooring to -4
    ASSERT_OK_AND_ASSIGN(VariantType negative_inexact_ret, field_product_agg->Retract(-10, 3));
    ASSERT_EQ(DataDefine::GetVariantValue<int32_t>(negative_inexact_ret), -3);
}

TEST(FieldProductAggTest, TestDecimalRoundsHalfUp) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                         FieldProductAgg::Create(arrow::decimal128(10, 2), GetDefaultPool()));
    // 1.05 * 1.10 = 1.1550, rounded half up to 1.16
    ASSERT_OK_AND_ASSIGN(VariantType agg_ret, field_product_agg->Agg(MakeDecimal(10, 2, "105"),
                                                                     MakeDecimal(10, 2, "110")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(agg_ret), MakeDecimal(10, 2, "116"));
    // 1.01 * 1.01 = 1.0201, the dropped digits stay below half so the result truncates to 1.02
    ASSERT_OK_AND_ASSIGN(
        VariantType truncated_agg_ret,
        field_product_agg->Agg(MakeDecimal(10, 2, "101"), MakeDecimal(10, 2, "101")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(truncated_agg_ret), MakeDecimal(10, 2, "102"));
    // -1.05 * 1.10 = -1.1550, rounded half up (away from zero) to -1.16
    ASSERT_OK_AND_ASSIGN(
        VariantType negative_agg_ret,
        field_product_agg->Agg(MakeDecimal(10, 2, "-105"), MakeDecimal(10, 2, "110")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(negative_agg_ret), MakeDecimal(10, 2, "-116"));

    // 1.00 / 8.00 = 0.125 exactly, landing on a tie that rounds away from zero to 0.13
    ASSERT_OK_AND_ASSIGN(
        VariantType tie_retract_ret,
        field_product_agg->Retract(MakeDecimal(10, 2, "100"), MakeDecimal(10, 2, "800")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(tie_retract_ret), MakeDecimal(10, 2, "13"));
    // -1.00 / 8.00 = -0.125, the tie rounds away from zero to -0.13
    ASSERT_OK_AND_ASSIGN(
        VariantType negative_tie_retract_ret,
        field_product_agg->Retract(MakeDecimal(10, 2, "-100"), MakeDecimal(10, 2, "800")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(negative_tie_retract_ret),
              MakeDecimal(10, 2, "-13"));
    // 1.00 / -8.00 = -0.125, a negative divisor rounds away from zero the same way
    ASSERT_OK_AND_ASSIGN(
        VariantType negative_divisor_retract_ret,
        field_product_agg->Retract(MakeDecimal(10, 2, "100"), MakeDecimal(10, 2, "-800")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(negative_divisor_retract_ret),
              MakeDecimal(10, 2, "-13"));
    // 1.00 / 16.00 = 0.0625, below the tie so it rounds down to 0.06
    ASSERT_OK_AND_ASSIGN(
        VariantType below_tie_retract_ret,
        field_product_agg->Retract(MakeDecimal(10, 2, "100"), MakeDecimal(10, 2, "1600")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(below_tie_retract_ret), MakeDecimal(10, 2, "6"));

    // 0.01 / 1.25 = 0.008, rounded half up to 0.01. The truncated quotient is zero here, so the
    // rounding step has to take the sign from the operands rather than from the quotient.
    ASSERT_OK_AND_ASSIGN(
        VariantType rounded_up_from_zero_ret,
        field_product_agg->Retract(MakeDecimal(10, 2, "1"), MakeDecimal(10, 2, "125")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(rounded_up_from_zero_ret),
              MakeDecimal(10, 2, "1"));
    // -0.01 / 1.25 = -0.008, rounded away from zero to -0.01
    ASSERT_OK_AND_ASSIGN(
        VariantType rounded_down_from_zero_ret,
        field_product_agg->Retract(MakeDecimal(10, 2, "-1"), MakeDecimal(10, 2, "125")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(rounded_down_from_zero_ret),
              MakeDecimal(10, 2, "-1"));
}

TEST(FieldProductAggTest, TestDecimalRejectsNonTerminatingQuotient) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                         FieldProductAgg::Create(arrow::decimal128(10, 2), GetDefaultPool()));
    // 2.00 / 3.00 repeats forever, which is what Java's BigDecimal.divide() refuses
    ASSERT_NOK_WITH_MSG(
        field_product_agg->Retract(MakeDecimal(10, 2, "200"), MakeDecimal(10, 2, "300")),
        "decimal 2.00 / 3.00 has no finite decimal expansion");
    ASSERT_NOK_WITH_MSG(
        field_product_agg->Retract(MakeDecimal(10, 2, "100"), MakeDecimal(10, 2, "700")),
        "has no finite decimal expansion");
    // 0.01 / 1.50 = 0.00666..., rejected even though it would round to a representable 0.01
    ASSERT_NOK_WITH_MSG(
        field_product_agg->Retract(MakeDecimal(10, 2, "1"), MakeDecimal(10, 2, "150")),
        "has no finite decimal expansion");

    // A divisor that is not a power of two or five is fine as long as the quotient still
    // terminates: 3.00 / 6.00 = 0.5 reduces to 1/2.
    ASSERT_OK_AND_ASSIGN(
        VariantType retract_ret,
        field_product_agg->Retract(MakeDecimal(10, 2, "300"), MakeDecimal(10, 2, "600")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(retract_ret), MakeDecimal(10, 2, "50"));
    // Zero divided by the very divisor rejected above still terminates, at zero.
    ASSERT_OK_AND_ASSIGN(
        VariantType zero_retract_ret,
        field_product_agg->Retract(MakeDecimal(10, 2, "0"), MakeDecimal(10, 2, "300")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(zero_retract_ret), MakeDecimal(10, 2, "0"));
}

TEST(FieldProductAggTest, TestDecimalWithZeroScale) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                         FieldProductAgg::Create(arrow::decimal128(10, 0), GetDefaultPool()));
    // a zero scale rescales by nothing in either direction, 3 * 4 = 12
    ASSERT_OK_AND_ASSIGN(VariantType agg_ret,
                         field_product_agg->Agg(MakeDecimal(10, 0, "3"), MakeDecimal(10, 0, "4")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(agg_ret), MakeDecimal(10, 0, "12"));
    // 10 / 4 = 2.5, a tie that rounds away from zero to 3 once the scale leaves no decimals
    ASSERT_OK_AND_ASSIGN(
        VariantType retract_ret,
        field_product_agg->Retract(MakeDecimal(10, 0, "10"), MakeDecimal(10, 0, "4")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(retract_ret), MakeDecimal(10, 0, "3"));
}

TEST(FieldProductAggTest, TestDecimalWiderThanInt128Intermediate) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                         FieldProductAgg::Create(arrow::decimal128(38, 18), GetDefaultPool()));
    // 20 * 30 = 600, the unscaled intermediate 6e38 does not fit into an int128
    ASSERT_OK_AND_ASSIGN(VariantType agg_ret,
                         field_product_agg->Agg(MakeDecimal(38, 18, "20000000000000000000"),
                                                MakeDecimal(38, 18, "30000000000000000000")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(agg_ret),
              MakeDecimal(38, 18, "600000000000000000000"));

    ASSERT_OK_AND_ASSIGN(VariantType retract_ret,
                         field_product_agg->Retract(MakeDecimal(38, 18, "600000000000000000000"),
                                                    MakeDecimal(38, 18, "30000000000000000000")));
    ASSERT_EQ(DataDefine::GetVariantValue<Decimal>(retract_ret),
              MakeDecimal(38, 18, "20000000000000000000"));
}

TEST(FieldProductAggTest, TestIntegerArithmeticErrors) {
    // every width computes in its own type, so a product that only fits a wider type overflows
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::int8(), GetDefaultPool()));
        ASSERT_NOK_WITH_MSG(field_product_agg->Agg(static_cast<char>(100), static_cast<char>(2)),
                            "int8 overflow: 100 * 2");
        ASSERT_NOK_WITH_MSG(
            field_product_agg->Retract(static_cast<char>(-128), static_cast<char>(-1)),
            "int8 overflow: -128 / -1");
        ASSERT_NOK_WITH_MSG(field_product_agg->Retract(static_cast<char>(10), static_cast<char>(0)),
                            "int8 division by zero: 10 / 0");
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::int16(), GetDefaultPool()));
        ASSERT_NOK_WITH_MSG(
            field_product_agg->Agg(static_cast<int16_t>(300), static_cast<int16_t>(200)),
            "int16 overflow: 300 * 200");
        ASSERT_NOK_WITH_MSG(field_product_agg->Retract(std::numeric_limits<int16_t>::min(),
                                                       static_cast<int16_t>(-1)),
                            "int16 overflow: -32768 / -1");
        ASSERT_NOK_WITH_MSG(
            field_product_agg->Retract(static_cast<int16_t>(10), static_cast<int16_t>(0)),
            "int16 division by zero: 10 / 0");
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::int32(), GetDefaultPool()));
        ASSERT_NOK_WITH_MSG(field_product_agg->Agg(std::numeric_limits<int32_t>::max(), 2),
                            "int32 overflow: 2147483647 * 2");
        ASSERT_NOK_WITH_MSG(field_product_agg->Retract(std::numeric_limits<int32_t>::min(), -1),
                            "int32 overflow: -2147483648 / -1");
        ASSERT_NOK_WITH_MSG(field_product_agg->Retract(5, 0), "int32 division by zero: 5 / 0");
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::int64(), GetDefaultPool()));
        ASSERT_NOK_WITH_MSG(
            field_product_agg->Agg(std::numeric_limits<int64_t>::max(), static_cast<int64_t>(2)),
            "int64 overflow: 9223372036854775807 * 2");
        ASSERT_NOK_WITH_MSG(field_product_agg->Retract(std::numeric_limits<int64_t>::min(),
                                                       static_cast<int64_t>(-1)),
                            "int64 overflow: -9223372036854775808 / -1");
        ASSERT_NOK_WITH_MSG(
            field_product_agg->Retract(static_cast<int64_t>(10), static_cast<int64_t>(0)),
            "int64 division by zero: 10 / 0");
    }
}

TEST(FieldProductAggTest, TestDecimalOverflowsToNull) {
    {
        // 99.99 * 99.99 = 9998.0001, which rounds to 9998.00 and still needs a precision of 6
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::decimal128(4, 2), GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(
            VariantType agg_ret,
            field_product_agg->Agg(MakeDecimal(4, 2, "9999"), MakeDecimal(4, 2, "9999")));
        ASSERT_TRUE(DataDefine::IsVariantNull(agg_ret));
    }
    {
        // 99999999.99 / 0.01 needs 12 digits
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::decimal128(10, 2), GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(
            VariantType retract_ret,
            field_product_agg->Retract(MakeDecimal(10, 2, "9999999999"), MakeDecimal(10, 2, "1")));
        ASSERT_TRUE(DataDefine::IsVariantNull(retract_ret));
    }
}

TEST(FieldProductAggTest, TestDecimalDivisionByZero) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                         FieldProductAgg::Create(arrow::decimal128(10, 2), GetDefaultPool()));
    ASSERT_NOK_WITH_MSG(
        field_product_agg->Retract(MakeDecimal(10, 2, "150"), MakeDecimal(10, 2, "0")),
        "decimal division by zero: 1.50 / 0.00");
}

TEST(FieldProductAggTest, TestFloatingPointDivisionByZero) {
    // unlike the integer types, the floating point types divide by zero the way IEEE 754 says to
    // rather than reporting an error
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::float64(), GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(VariantType positive_ret, field_product_agg->Retract(3.75, 0.0));
        ASSERT_EQ(DataDefine::GetVariantValue<double>(positive_ret),
                  std::numeric_limits<double>::infinity());
        ASSERT_OK_AND_ASSIGN(VariantType negative_ret, field_product_agg->Retract(-3.75, 0.0));
        ASSERT_EQ(DataDefine::GetVariantValue<double>(negative_ret),
                  -std::numeric_limits<double>::infinity());
        // A negative zero flips the sign just as a negative dividend does, and product itself
        // produces one whenever a negative value is multiplied by zero.
        ASSERT_OK_AND_ASSIGN(VariantType negative_zero_ret, field_product_agg->Retract(3.75, -0.0));
        ASSERT_EQ(DataDefine::GetVariantValue<double>(negative_zero_ret),
                  -std::numeric_limits<double>::infinity());
        // zero over zero is the one case that is not an infinity
        ASSERT_OK_AND_ASSIGN(VariantType nan_ret, field_product_agg->Retract(0.0, 0.0));
        ASSERT_TRUE(std::isnan(DataDefine::GetVariantValue<double>(nan_ret)));
    }
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldProductAgg> field_product_agg,
                             FieldProductAgg::Create(arrow::float32(), GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(
            VariantType positive_ret,
            field_product_agg->Retract(static_cast<float>(3.75), static_cast<float>(0.0)));
        ASSERT_EQ(DataDefine::GetVariantValue<float>(positive_ret),
                  std::numeric_limits<float>::infinity());
        ASSERT_OK_AND_ASSIGN(
            VariantType nan_ret,
            field_product_agg->Retract(static_cast<float>(0.0), static_cast<float>(0.0)));
        ASSERT_TRUE(std::isnan(DataDefine::GetVariantValue<float>(nan_ret)));
    }
}

TEST(FieldProductAggTest, TestInvalidType) {
    ASSERT_NOK_WITH_MSG(FieldProductAgg::Create(arrow::boolean(), GetDefaultPool()),
                        "type bool not support in FieldProductAgg");
    ASSERT_NOK_WITH_MSG(FieldProductAgg::Create(arrow::utf8(), GetDefaultPool()),
                        "not support in FieldProductAgg");
    // rescaling by the field scale needs a scale within [0, precision]
    ASSERT_NOK_WITH_MSG(FieldProductAgg::Create(arrow::decimal128(20, 22), GetDefaultPool()),
                        "precision must >= scale");
    ASSERT_NOK_WITH_MSG(FieldProductAgg::Create(arrow::decimal128(10, -2), GetDefaultPool()),
                        "scale must >= 0");
}
}  // namespace paimon::test
