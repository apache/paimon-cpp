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

#include "paimon/common/utils/math.h"

#include <array>
#include <cstring>
#include <limits>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(MathTest, FloatingPointNaNCanonicalization) {
    auto float_from_bits = [](uint32_t bits) {
        float value;
        std::memcpy(&value, &bits, sizeof(value));
        return value;
    };
    auto double_from_bits = [](uint64_t bits) {
        double value;
        std::memcpy(&value, &bits, sizeof(value));
        return value;
    };

    const float float_nan = CanonicalizeFloatingPoint(float_from_bits(0xffc12345));
    uint32_t float_nan_bits;
    std::memcpy(&float_nan_bits, &float_nan, sizeof(float_nan_bits));
    ASSERT_EQ(kCanonicalFloatNaNBits, float_nan_bits);
    ASSERT_EQ(static_cast<int32_t>(kCanonicalFloatNaNBits),
              CanonicalizeFloatToIntBits(float_from_bits(0x7fa12345)));

    const double double_nan = CanonicalizeFloatingPoint(double_from_bits(0xfff8123456789abc));
    uint64_t double_nan_bits;
    std::memcpy(&double_nan_bits, &double_nan, sizeof(double_nan_bits));
    ASSERT_EQ(kCanonicalDoubleNaNBits, double_nan_bits);
    ASSERT_EQ(static_cast<int64_t>(kCanonicalDoubleNaNBits),
              CanonicalizeDoubleToLongBits(double_from_bits(0x7ff123456789abcd)));

    const float negative_zero = CanonicalizeFloatingPoint(-0.0f);
    uint32_t negative_zero_bits;
    std::memcpy(&negative_zero_bits, &negative_zero, sizeof(negative_zero_bits));
    ASSERT_EQ(0x80000000U, negative_zero_bits);
    ASSERT_EQ(0x3ff0000000000000, CanonicalizeDoubleToLongBits(1.0));
}

// Test case: Test EndianSwapValue for different integral types
TEST(MathTest, EndianSwapValue) {
    // Test 16-bit value
    uint16_t value16 = 0x1234;
    uint16_t swapped16 = EndianSwapValue(value16);
    ASSERT_EQ(swapped16, 0x3412);

    // Test 32-bit value
    uint32_t value32 = 0x12345678;
    uint32_t swapped32 = EndianSwapValue(value32);
    ASSERT_EQ(swapped32, 0x78563412);

    // Test 64-bit value
    uint64_t value64 = 0x123456789ABCDEF0;
    uint64_t swapped64 = EndianSwapValue(value64);
    ASSERT_EQ(swapped64, 0xF0DEBC9A78563412);
}

TEST(MathTest, ToEndian) {
    constexpr uint32_t kValue = 0x12345678;

    const uint32_t big_endian = ToBigEndian(kValue);
    std::array<uint8_t, sizeof(big_endian)> big_endian_bytes{};
    std::memcpy(big_endian_bytes.data(), &big_endian, sizeof(big_endian));
    ASSERT_EQ((std::array<uint8_t, 4>{0x12, 0x34, 0x56, 0x78}), big_endian_bytes);
    ASSERT_EQ(kValue, FromBigEndian(big_endian));

    const uint32_t little_endian = ToLittleEndian(kValue);
    std::array<uint8_t, sizeof(little_endian)> little_endian_bytes{};
    std::memcpy(little_endian_bytes.data(), &little_endian, sizeof(little_endian));
    ASSERT_EQ((std::array<uint8_t, 4>{0x78, 0x56, 0x34, 0x12}), little_endian_bytes);
    ASSERT_EQ(kValue, FromLittleEndian(little_endian));
}

TEST(MathTest, InRange) {
    // signed -> unsigned: negative values out of range, boundary values in range
    ASSERT_TRUE(InRange<uint32_t>(0));
    ASSERT_TRUE(InRange<uint32_t>(std::numeric_limits<int32_t>::max()));
    ASSERT_FALSE(InRange<uint32_t>(std::numeric_limits<int32_t>::lowest()));
    ASSERT_FALSE(InRange<uint32_t>(-1));

    // unsigned -> signed: values beyond signed max are out of range
    ASSERT_TRUE(InRange<int32_t>(static_cast<uint32_t>(0)));
    ASSERT_TRUE(InRange<int32_t>(static_cast<uint32_t>(std::numeric_limits<int32_t>::max())));
    ASSERT_FALSE(InRange<int32_t>(static_cast<uint32_t>(std::numeric_limits<int32_t>::max()) + 1U));
    ASSERT_FALSE(InRange<int32_t>(std::numeric_limits<uint32_t>::max()));

    // wider signed -> narrower signed: overflow detection
    ASSERT_TRUE(InRange<int32_t>(static_cast<int64_t>(std::numeric_limits<int32_t>::lowest())));
    ASSERT_TRUE(InRange<int32_t>(static_cast<int64_t>(std::numeric_limits<int32_t>::max())));
    ASSERT_FALSE(
        InRange<int32_t>(static_cast<int64_t>(std::numeric_limits<int32_t>::lowest()) - 1));
    ASSERT_FALSE(InRange<int32_t>(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1));

    // wider unsigned -> narrower unsigned: overflow detection
    ASSERT_TRUE(InRange<uint32_t>(static_cast<uint64_t>(0)));
    ASSERT_TRUE(InRange<uint32_t>(static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())));
    ASSERT_FALSE(
        InRange<uint32_t>(static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1ULL));

    // mixed width: narrower -> wider always in range, wider -> narrower may overflow
    ASSERT_TRUE(InRange<int32_t>(static_cast<int16_t>(12)));
    ASSERT_FALSE(InRange<int16_t>(std::numeric_limits<int32_t>::max()));
    ASSERT_TRUE(InRange<uint32_t>(std::numeric_limits<uint32_t>::max()));
    ASSERT_FALSE(InRange<uint32_t>(static_cast<int64_t>(std::numeric_limits<uint32_t>::max()) + 1));

    ASSERT_OK(ValidateValueInRange<int32_t>(
        static_cast<int64_t>(std::numeric_limits<int32_t>::lowest()), "signed value"));
    ASSERT_NOK_WITH_MSG(ValidateValueInRange<uint32_t>(-1, "negative value"),
                        "negative value -1 is out of bound of type");

    ASSERT_OK(ValidateValueNonNegative(0, "non-negative value"));
    ASSERT_NOK_WITH_MSG(ValidateValueNonNegative(-1, "negative value"),
                        "negative value -1 is less than 0");
}

}  // namespace paimon::test
