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

#include "paimon/core/bucket/hive_hasher.h"

#include <cstdint>

#include "gtest/gtest.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(HiveHasherTest, TestHashIntPreservesBits) {
    for (uint32_t value : {0U, 1U, 0x7fffffffU, 0x80000000U, 0xffffffffU}) {
        ASSERT_EQ(HiveHasher::HashInt(value), value);
    }
}

TEST(HiveHasherTest, TestHashLongCombinesBothHalves) {
    struct TestCase {
        uint64_t value;
        uint32_t expected;
    };
    const TestCase cases[] = {{0, 0},
                              {1, 1},
                              {0x100000000ULL, 1},
                              {0x0123456789abcdefULL, 0x88888888U},
                              {0x7fffffffffffffffULL, 0x80000000U},
                              {0x8000000000000000ULL, 0x80000000U},
                              {0xffffffffffffffffULL, 0}};
    for (const auto& test_case : cases) {
        ASSERT_EQ(HiveHasher::HashLong(test_case.value), test_case.expected) << test_case.value;
    }
}

TEST(HiveHasherTest, TestHashBytesUsesSignedBytesAndExplicitLength) {
    ASSERT_EQ(HiveHasher::HashBytes("", 0), 0U);
    ASSERT_EQ(HiveHasher::HashBytes("hello", 5), 99162322U);
    ASSERT_EQ(HiveHasher::HashBytes("a\0b", 3), 93315U);
    ASSERT_EQ(HiveHasher::HashBytes("a\0b", 1), 97U);
    ASSERT_EQ(HiveHasher::HashBytes("\x7f", 1), 127U);
    ASSERT_EQ(HiveHasher::HashBytes("\x80", 1), 4294967168U);
    ASSERT_EQ(HiveHasher::HashBytes("\xff", 1), 4294967295U);
    ASSERT_EQ(HiveHasher::HashBytes("\x7f\x80\xff\x00\x01", 5), 113472959U);
    ASSERT_EQ(HiveHasher::HashBytes("\xff\x80\x7f\x00", 4), 4294818434U);
}

TEST(HiveHasherTest, TestHashZeroDecimalIgnoresScale) {
    for (int32_t scale : {0, 1, 18, 38}) {
        ASSERT_EQ(HiveHasher::HashDecimal(Decimal(38, scale, 0)), 0U) << scale;
    }
}

TEST(HiveHasherTest, TestHashDecimalNormalizesTrailingZeros) {
    ASSERT_EQ(HiveHasher::HashDecimal(Decimal(10, 2, 1234)), 38256U);
    ASSERT_EQ(HiveHasher::HashDecimal(Decimal(10, 4, 123400)), 38256U);
    ASSERT_EQ(HiveHasher::HashDecimal(Decimal(10, 2, -1234)), 4294929044U);
    ASSERT_EQ(HiveHasher::HashDecimal(Decimal(10, 4, -123400)), 4294929044U);
    ASSERT_EQ(HiveHasher::HashDecimal(Decimal(10, 0, 1000)), 31000U);
    ASSERT_EQ(HiveHasher::HashDecimal(Decimal(10, 3, 1000000)), 31000U);
    ASSERT_EQ(HiveHasher::HashDecimal(Decimal(38, 38, 1)), 69U);
    ASSERT_EQ(HiveHasher::HashDecimal(Decimal(38, 38, -1)), 7U);
}

TEST(HiveHasherTest, TestHashNonCompactDecimalNormalizesTrailingZeros) {
    ASSERT_OK_AND_ASSIGN(Decimal::int128_t unscaled,
                         DecimalUtils::StrToInt128("12340000000000000000000000000000000000"));
    ASSERT_EQ(HiveHasher::HashDecimal(Decimal(38, 36, unscaled)), 38256U);
    ASSERT_EQ(HiveHasher::HashDecimal(Decimal(38, 36, -unscaled)), 4294929044U);
}

TEST(HiveHasherTest, TestHashDecimalIncludesAllMagnitudeWords) {
    struct TestCase {
        const char* unscaled;
        int32_t scale;
        uint32_t expected;
    };
    // Verified with Java normalizeDecimal(...).hashCode(), including values with identical
    // low 32 bits and values outside the signed long range.
    const TestCase cases[] = {{"1", 0, 31U},
                              {"4294967296", 0, 961U},
                              {"-4294967296", 0, 4294966335U},
                              {"4294967297", 0, 992U},
                              {"8589934593", 0, 1953U},
                              {"4294967296", 2, 963U},
                              {"-4294967296", 2, 4294966337U},
                              {"9223372036854775807", 0, 2147482656U},
                              {"-9223372036854775808", 0, 2147483648U},
                              {"9223372036854775808", 0, 2147483648U},
                              {"18446744073709551616", 0, 29791U},
                              {"-18446744073709551616", 0, 4294937505U},
                              {"79228162514264337593543950336", 0, 923521U},
                              {"-79228162514264337593543950336", 0, 4294043775U},
                              {"99999999999999999999999999999999999999", 0, 3867768910U},
                              {"-99999999999999999999999999999999999999", 0, 427198386U}};
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.unscaled);
        SCOPED_TRACE(test_case.scale);
        ASSERT_OK_AND_ASSIGN(Decimal::int128_t unscaled,
                             DecimalUtils::StrToInt128(test_case.unscaled));
        ASSERT_EQ(HiveHasher::HashDecimal(Decimal(38, test_case.scale, unscaled)),
                  test_case.expected);
    }
}

TEST(HiveHasherTest, TestHashDecimalRejectsIntegerPrecisionOverflow) {
    // Decimal's raw constructor permits values beyond Hive's 38-digit limit.
    // Java normalizeDecimal returns null here. Its bucket function then calls hashCode()
    // without a null check and throws NullPointerException. These assertions document C++'s
    // existing zero fallback, rather than matching Java hash values.
    for (const char* value :
         {"100000000000000000000000000000000000000", "-100000000000000000000000000000000000000",
          "170141183460469231731687303715884105727", "-170141183460469231731687303715884105728"}) {
        SCOPED_TRACE(value);
        ASSERT_OK_AND_ASSIGN(Decimal::int128_t unscaled, DecimalUtils::StrToInt128(value));
        ASSERT_EQ(HiveHasher::HashDecimal(Decimal(39, 0, unscaled)), 0U);
    }
}

TEST(HiveHasherTest, TestHashDecimalRoundsHalfUpToHiveScale) {
    struct TestCase {
        int64_t unscaled;
        int32_t scale;
        uint32_t expected;
    };
    // Verified with Java normalizeDecimal(...).hashCode().
    const TestCase cases[] = {
        // HALF_UP boundaries, rounding to zero, and trailing zeros after rounding.
        {4, 39, 0U},
        {-4, 39, 0U},
        {5, 39, 69U},
        {-5, 39, 7U},
        {14, 39, 69U},
        {-14, 39, 7U},
        {15, 39, 100U},
        {-15, 39, 4294967272U},
        {16, 39, 100U},
        {-16, 39, 4294967272U},
        {995, 39, 67U},
        {-995, 39, 5U},
        // Multiple discarded digits must be rounded once to avoid a second carry.
        {123, 40, 69U},
        {-123, 40, 7U},
        {149, 40, 69U},
        {-149, 40, 7U},
        {49, 40, 0U},
        {-49, 40, 0U},
        {50, 40, 69U},
        {-50, 40, 7U},
        {150, 40, 100U},
        {-150, 40, 4294967272U},
        {449, 40, 162U},
        {-449, 40, 4294967210U},
        {499, 41, 0U},
        {-499, 41, 0U},
        {1499, 41, 69U},
        {-1499, 41, 7U},
        {99949, 40, 31007U},
        {-99949, 40, 4294936365U},
        {99950, 40, 66U},
        {-99950, 40, 4U}};
    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.unscaled);
        SCOPED_TRACE(test_case.scale);
        ASSERT_EQ(HiveHasher::HashDecimal(Decimal(41, test_case.scale, test_case.unscaled)),
                  test_case.expected);
    }
}

}  // namespace paimon::test
