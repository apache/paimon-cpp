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

#include "paimon/common/file_index/bloomfilter/fast_hash.h"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class FastHashTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    void CheckResult(const FastHash::HashFunction& hash_function,
                     const std::vector<Literal>& literals,
                     const std::vector<uint64_t> expected_hash) {
        ASSERT_EQ(literals.size(), expected_hash.size());
        for (int32_t i = 0; i < static_cast<int32_t>(literals.size()); i++) {
            ASSERT_EQ(expected_hash[i], hash_function(literals[i]));
        }
    }
};

TEST_F(FastHashTest, TestCompatibleWithJava) {
    {
        // test tiny int
        ASSERT_OK_AND_ASSIGN(auto hash_function, FastHash::GetHashFunction(arrow::int8()));
        CheckResult(hash_function,
                    {Literal(static_cast<int8_t>(-128)), Literal(static_cast<int8_t>(-100)),
                     Literal(static_cast<int8_t>(-1)), Literal(static_cast<int8_t>(0)),
                     Literal(static_cast<int8_t>(1)), Literal(static_cast<int8_t>(10)),
                     Literal(static_cast<int8_t>(127))},
                    {0xe547e8444a8fcdd1, 0xdb213b4e3642747d, 0x5bca868437950d03, 0x0,
                     0x5bca7c69b794f8ce, 0x95ea2955abd45275, 0x897cf79712f9ec7c});
    }
    {
        // test small int
        ASSERT_OK_AND_ASSIGN(auto hash_function, FastHash::GetHashFunction(arrow::int16()));
        CheckResult(hash_function,
                    {Literal(static_cast<int16_t>(-32768)), Literal(static_cast<int16_t>(-100)),
                     Literal(static_cast<int16_t>(-1)), Literal(static_cast<int16_t>(0)),
                     Literal(static_cast<int16_t>(1)), Literal(static_cast<int16_t>(10)),
                     Literal(static_cast<int16_t>(32767))},
                    {0x47ed1a480fd77cfb, 0xdb213b4e3642747d, 0x5bca868437950d03, 0x0,
                     0x5bca7c69b794f8ce, 0x95ea2955abd45275, 0xe968161ed2cd74ae});
    }
    {
        // test int
        ASSERT_OK_AND_ASSIGN(auto hash_function, FastHash::GetHashFunction(arrow::int32()));
        CheckResult(
            hash_function,
            {Literal(static_cast<int32_t>(-2147483648)), Literal(static_cast<int32_t>(-1034556)),
             Literal(static_cast<int32_t>(-1)), Literal(static_cast<int32_t>(0)),
             Literal(static_cast<int32_t>(1)), Literal(static_cast<int32_t>(49647)),
             Literal(static_cast<int32_t>(2147483647))},
            {0x111ec0fd6aa8626c, 0xfb5d8d6df66551c8, 0x5bca868437950d03, 0x0, 0x5bca7c69b794f8ce,
             0x94147f05a824e009, 0xc6d8bcc4d61c69a4});
    }
    {
        // test date
        ASSERT_OK_AND_ASSIGN(auto hash_function, FastHash::GetHashFunction(arrow::date32()));
        CheckResult(
            hash_function,
            {Literal(static_cast<int32_t>(-2147483648)), Literal(static_cast<int32_t>(-1034556)),
             Literal(static_cast<int32_t>(-1)), Literal(static_cast<int32_t>(0)),
             Literal(static_cast<int32_t>(1)), Literal(static_cast<int32_t>(49647)),
             Literal(static_cast<int32_t>(2147483647))},
            {0x111ec0fd6aa8626c, 0xfb5d8d6df66551c8, 0x5bca868437950d03, 0x0, 0x5bca7c69b794f8ce,
             0x94147f05a824e009, 0xc6d8bcc4d61c69a4});
    }
    {
        // test bit int
        ASSERT_OK_AND_ASSIGN(auto hash_function, FastHash::GetHashFunction(arrow::int64()));
        CheckResult(
            hash_function,
            {Literal(static_cast<int64_t>(std::numeric_limits<int64_t>::min())),
             Literal(static_cast<int64_t>(-4598654206466ll)), Literal(static_cast<int64_t>(-1)),
             Literal(static_cast<int64_t>(0)), Literal(static_cast<int64_t>(1)),
             Literal(static_cast<int64_t>(8548553896418ll)),
             Literal(static_cast<int64_t>(9223372036854775807ll))},
            {0x3be7d0f7780de548, 0xebd8376102414af8, 0x5bca868437950d03, 0x0, 0x5bca7c69b794f8ce,
             0xe7f3590a09b6693a, 0x81ad52718398e837});
    }
    {
        // test Timestamp type
        ASSERT_OK_AND_ASSIGN(auto hash_function,
                             FastHash::GetHashFunction(arrow::timestamp(arrow::TimeUnit::NANO)));
        CheckResult(
            hash_function,
            {Literal(Timestamp(1745542802000l, 123000)), Literal(Timestamp(1745542902000l, 123000)),
             Literal(Timestamp(1745542602000l, 123000)), Literal(Timestamp(-1745l, 123000)),
             Literal(Timestamp(-1765l, 123000)), Literal(Timestamp(1745542802000l, 123000)),
             Literal(Timestamp(-1725l, 123000))},
            {0x3fa6477403e32e14, 0x1ec9ecd0a1b07aea, 0xd74122b773e5f45c, 0xe7f352e64f55f259,
             0xbe8a32867c820cf2, 0x3fa6477403e32e14, 0xd3ffe009a770b6a0});
    }
    {
        // test float
        ASSERT_OK_AND_ASSIGN(auto hash_function, FastHash::GetHashFunction(arrow::float32()));
        CheckResult(hash_function,
                    {Literal(std::numeric_limits<float>::lowest()), Literal(-123.45f),
                     Literal(-12345.6f), Literal(0.0f), Literal(2.1f), Literal(345.12f),
                     Literal(std::numeric_limits<float>::max())},
                    {0xecf6796dd7355dbc, 0x1f4dcc5b8a502b70, 0xe678035506c03314, 0x0,
                     0x8df65966db697d6, 0xdc9b5b8828f877ee, 0xf9b567bea590d8d});
    }
    {
        // test double
        ASSERT_OK_AND_ASSIGN(auto hash_function, FastHash::GetHashFunction(arrow::float64()));
        CheckResult(hash_function,
                    {Literal(std::numeric_limits<double>::lowest()), Literal(-123.45),
                     Literal(-12345.6), Literal(0.0), Literal(2.1), Literal(345.12),
                     Literal(std::numeric_limits<double>::max())},
                    {0xb3c148792fed6cb9, 0x989ea602f70c211, 0x8040a6007c7b22e0, 0x0,
                     0xfca8098dd6548561, 0x477acb9b5361fc7f, 0x939ba81c9dffe90});
    }
    {
        // test string
        ASSERT_OK_AND_ASSIGN(auto hash_function, FastHash::GetHashFunction(arrow::utf8()));
        CheckResult(hash_function,
                    {Literal(FieldType::STRING, "", 0), Literal(FieldType::STRING, "example", 7),
                     Literal(FieldType::STRING, "Have a nice day!", 16)},
                    {0xef46db3751d8e999, 0xe6eda53558c41c5e, 0xe663266d57d776c2});
    }
    {
        // test binary
        ASSERT_OK_AND_ASSIGN(auto hash_function, FastHash::GetHashFunction(arrow::binary()));
        std::string str = "我是一个粉刷匠";
        CheckResult(
            hash_function,
            {Literal(FieldType::STRING, "", 0), Literal(FieldType::STRING, "example", 7),
             Literal(FieldType::STRING, "Have a nice day!", 16),
             Literal(FieldType::STRING, str.data(), str.size())},
            {0xef46db3751d8e999, 0xe6eda53558c41c5e, 0xe663266d57d776c2, 0x9f01fcd3f19877e6});
    }
    {
        // test invalid type
        ASSERT_NOK_WITH_MSG(FastHash::GetHashFunction(arrow::boolean()),
                            "bloom filter index does not support BOOLEAN");
    }
}

TEST_F(FastHashTest, TestNaNCompatibleWithJava) {
    auto float_from_bits = [](uint32_t bits) {
        float value;
        std::memcpy(&value, &bits, sizeof(value));
        return value;
    };
    const float float_nan = float_from_bits(0x7fc12345);
    const float negative_float_nan = float_from_bits(0xffc54321);
    ASSERT_TRUE(std::isnan(float_nan));
    ASSERT_TRUE(std::isnan(negative_float_nan));
    ASSERT_OK_AND_ASSIGN(auto float_hash_function, FastHash::GetHashFunction(arrow::float32()));
    CheckResult(float_hash_function, {Literal(float_nan), Literal(negative_float_nan)},
                {0x67c27c6d9936ae63, 0x67c27c6d9936ae63});

    auto double_from_bits = [](uint64_t bits) {
        double value;
        std::memcpy(&value, &bits, sizeof(value));
        return value;
    };
    const double double_nan = double_from_bits(0x7ff8123456789abc);
    const double negative_double_nan = double_from_bits(0xfff8abcdef012345);
    ASSERT_TRUE(std::isnan(double_nan));
    ASSERT_TRUE(std::isnan(negative_double_nan));
    ASSERT_OK_AND_ASSIGN(auto double_hash_function, FastHash::GetHashFunction(arrow::float64()));
    CheckResult(double_hash_function, {Literal(double_nan), Literal(negative_double_nan)},
                {0x13d2d3f2cc0e846e, 0x13d2d3f2cc0e846e});
}

}  // namespace paimon::test
