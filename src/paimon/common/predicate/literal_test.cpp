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

#include "paimon/predicate/literal.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class LiteralTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    void CheckResult(const Literal& literal1, const Literal& literal2, FieldType type) {
        ASSERT_FALSE(literal1.IsNull());
        ASSERT_EQ(literal1.GetType(), type);
        ASSERT_FALSE(literal2.IsNull());
        ASSERT_EQ(literal2.GetType(), type);

        // literal1 < literal2
        ASSERT_OK_AND_ASSIGN(auto compare_result, literal1.CompareTo(literal2));
        ASSERT_EQ(compare_result, -1);
        ASSERT_OK_AND_ASSIGN(compare_result, literal2.CompareTo(literal1));
        ASSERT_EQ(compare_result, 1);
        ASSERT_FALSE(literal1 == literal2);
        ASSERT_TRUE(literal1 != literal2);

        // literal1 == literal1
        ASSERT_OK_AND_ASSIGN(compare_result, literal1.CompareTo(literal1));
        ASSERT_EQ(compare_result, 0);
        ASSERT_TRUE(literal1 == literal1);
        ASSERT_FALSE(literal1 != literal1);

        // literal1 == copy_literal1
        Literal copy_literal1 = literal1;
        ASSERT_OK_AND_ASSIGN(compare_result, copy_literal1.CompareTo(literal1));
        ASSERT_EQ(compare_result, 0);
        ASSERT_TRUE(literal1 == copy_literal1);
        ASSERT_FALSE(literal1 != copy_literal1);
    }
};

TEST_F(LiteralTest, TestSimple) {
    {
        Literal literal1(false);
        Literal literal2(true);
        CheckResult(literal1, literal2, FieldType::BOOLEAN);
        ASSERT_EQ(literal1.ToString(), "false");
        ASSERT_EQ(literal2.ToString(), "true");
    }
    {
        Literal literal1(static_cast<int8_t>(10));
        Literal literal2(static_cast<int8_t>(20));
        CheckResult(literal1, literal2, FieldType::TINYINT);
        ASSERT_EQ(literal1.ToString(), std::string(1, 10));
        ASSERT_EQ(literal2.ToString(), std::string(1, 20));
    }
    {
        Literal literal1(static_cast<int16_t>(100));
        Literal literal2(static_cast<int16_t>(200));
        CheckResult(literal1, literal2, FieldType::SMALLINT);
        ASSERT_EQ(literal1.ToString(), "100");
        ASSERT_EQ(literal2.ToString(), "200");
    }
    {
        Literal literal1(static_cast<int32_t>(10000));
        Literal literal2(static_cast<int32_t>(20000));
        CheckResult(literal1, literal2, FieldType::INT);
        ASSERT_EQ(literal1.ToString(), "10000");
        ASSERT_EQ(literal2.ToString(), "20000");
    }
    {
        Literal literal1(static_cast<int64_t>(1000000));
        Literal literal2(static_cast<int64_t>(2000000));
        CheckResult(literal1, literal2, FieldType::BIGINT);
        ASSERT_EQ(literal1.ToString(), "1000000");
        ASSERT_EQ(literal2.ToString(), "2000000");
    }
    {
        Literal literal1(static_cast<float>(1000.5));
        Literal literal2(static_cast<float>(2000.6));
        CheckResult(literal1, literal2, FieldType::FLOAT);
        ASSERT_EQ(literal1.ToString(), "1000.5");
        ASSERT_EQ(literal2.ToString(), "2000.6");
    }
    {
        Literal literal1(1000.5555);
        Literal literal2(2000.6666);
        CheckResult(literal1, literal2, FieldType::DOUBLE);
        ASSERT_EQ(literal1.ToString(), "1000.56");
        ASSERT_EQ(literal2.ToString(), "2000.67");
    }
    {
        std::string str1("abandon");
        std::string str2("abandon1");
        Literal literal1(FieldType::STRING, str1.data(), str1.size());
        Literal literal2(FieldType::STRING, str2.data(), str2.size());
        CheckResult(literal1, literal2, FieldType::STRING);
        ASSERT_EQ(literal1.ToString(), "abandon");
        ASSERT_EQ(literal2.ToString(), "abandon1");
    }
    {
        std::string str1("快乐每一天");
        std::string str2("快乐每一天！");
        Literal literal1(FieldType::BINARY, str1.data(), str1.size());
        Literal literal2(FieldType::BINARY, str2.data(), str2.size());
        CheckResult(literal1, literal2, FieldType::BINARY);
        ASSERT_EQ(literal1.ToString(), "快乐每一天");
        ASSERT_EQ(literal2.ToString(), "快乐每一天！");
    }
    {
        Literal literal1(FieldType::DATE, 10000);
        Literal literal2(FieldType::DATE, 20000);
        CheckResult(literal1, literal2, FieldType::DATE);
        ASSERT_EQ(literal1.ToString(), "10000");
        ASSERT_EQ(literal2.ToString(), "20000");
    }
    {
        Literal literal1(FieldType::TIME, 123);
        Literal literal2(FieldType::TIME, 86399999);
        CheckResult(literal1, literal2, FieldType::TIME);
        ASSERT_EQ(literal1.ToString(), "123");
        ASSERT_EQ(literal1.HashCode(), Literal(FieldType::TIME, 123).HashCode());
        ASSERT_NOK(literal1.CompareTo(Literal(123)));
    }
    {
        Literal literal1(Timestamp(1725875365442l, 120000));
        Literal literal2(Timestamp(1725875365442l, 120001));
        CheckResult(literal1, literal2, FieldType::TIMESTAMP);
        ASSERT_EQ(literal1.ToString(), "2024-09-09 09:49:25.442120000");
        ASSERT_EQ(literal2.ToString(), "2024-09-09 09:49:25.442120001");
    }
    {
        Literal literal1(Timestamp(1725875365442l, 120000));
        Literal literal2(Timestamp(1725875365443l, 110000));
        CheckResult(literal1, literal2, FieldType::TIMESTAMP);
        ASSERT_EQ(literal1.ToString(), "2024-09-09 09:49:25.442120000");
        ASSERT_EQ(literal2.ToString(), "2024-09-09 09:49:25.443110000");
    }
    {
        // 1234.56 vs. 1235.56
        Literal literal1(Decimal(6, 2, 123456));
        Literal literal2(Decimal(6, 2, 123556));
        CheckResult(literal1, literal2, FieldType::DECIMAL);
        ASSERT_EQ(literal1.ToString(), "1234.56");
        ASSERT_EQ(literal2.ToString(), "1235.56");
    }
    {
        // 1234.56 vs. 1234.567
        Literal literal1(Decimal(6, 2, 123456));
        Literal literal2(Decimal(7, 3, 1234567));
        CheckResult(literal1, literal2, FieldType::DECIMAL);
        ASSERT_EQ(literal1.ToString(), "1234.56");
        ASSERT_EQ(literal2.ToString(), "1234.567");
    }
    {
        // 1234.56 vs. 123456789987654321.45678
        Literal literal1(Decimal(6, 2, 123456));
        Literal literal2(
            Decimal(23, 5, DecimalUtils::StrToInt128("12345678998765432145678").value()));
        CheckResult(literal1, literal2, FieldType::DECIMAL);
        ASSERT_EQ(literal1.ToString(), "1234.56");
        ASSERT_EQ(literal2.ToString(), "123456789987654321.45678");
    }
}

TEST_F(LiteralTest, TestWithNull) {
    {
        Literal literal1(FieldType::BIGINT);
        Literal literal2(FieldType::BIGINT);
        ASSERT_TRUE(literal1.IsNull());
        ASSERT_EQ(literal1.GetType(), FieldType::BIGINT);
        ASSERT_TRUE(literal2.IsNull());
        ASSERT_EQ(literal2.GetType(), FieldType::BIGINT);
        ASSERT_OK_AND_ASSIGN(auto compare_result, literal1.CompareTo(literal2));
        ASSERT_EQ(compare_result, 0);
    }
    {
        Literal literal1(FieldType::BIGINT);
        Literal literal2(FieldType::STRING);
        ASSERT_TRUE(literal1.IsNull());
        ASSERT_EQ(literal1.GetType(), FieldType::BIGINT);
        ASSERT_TRUE(literal2.IsNull());
        ASSERT_EQ(literal2.GetType(), FieldType::STRING);
        ASSERT_NOK(literal1.CompareTo(literal2));
    }
    {
        Literal literal1(FieldType::INT);
        Literal literal2(static_cast<int32_t>(10000));
        ASSERT_NOK(literal1.CompareTo(literal2));
    }
    {
        Literal literal1(FieldType::BIGINT);
        Literal literal2(10000.5);
        ASSERT_NOK(literal1.CompareTo(literal2));
    }
    {
        // type mismatch
        Literal literal1(static_cast<int64_t>(10000));
        Literal literal2(10000.5);
        ASSERT_NOK(literal1.CompareTo(literal2));
    }
}

TEST_F(LiteralTest, TestOwnData) {
    {
        std::string data = "hello world";
        // literal points to data
        Literal literal(FieldType::STRING, data.data(), data.size(), /*own data*/ false);
        ASSERT_EQ(data, literal.GetValue<std::string>());
        // literal and literal2 point to data
        Literal literal2 = literal;
        ASSERT_EQ(data, literal2.GetValue<std::string>());
        ASSERT_EQ(data, literal.GetValue<std::string>());

        std::string data3 = "copy hello world";
        Literal literal3(FieldType::STRING, data3.data(), data3.size(), /*own data*/ true);
        ASSERT_EQ(data3, literal3.GetValue<std::string>());
        // literal3 point to data
        literal3 = literal2;
        ASSERT_EQ(data, literal3.GetValue<std::string>());
        ASSERT_EQ(data, literal2.GetValue<std::string>());
    }
    {
        std::string data = "hello world";
        // literal points to data
        Literal literal(FieldType::BINARY, data.data(), data.size(), /*own data*/ false);
        ASSERT_EQ(data, literal.GetValue<std::string>());
        // literal and literal2 point to data
        Literal literal2 = literal;
        ASSERT_EQ(data, literal2.GetValue<std::string>());
        ASSERT_EQ(data, literal.GetValue<std::string>());

        std::string data3 = "copy hello world";
        Literal literal3(FieldType::BINARY, data3.data(), data3.size(), /*own data*/ true);
        ASSERT_EQ(data3, literal3.GetValue<std::string>());
        // literal3 point to data
        literal3 = literal2;
        ASSERT_EQ(data, literal3.GetValue<std::string>());
        ASSERT_EQ(data, literal2.GetValue<std::string>());
    }
    {
        std::string data = "hello world";
        // literal points to data
        Literal literal(FieldType::STRING, data.data(), data.size(), /*own data*/ false);
        ASSERT_EQ(data, literal.GetValue<std::string>());
        Literal literal2 = std::move(literal);
        // literal2 point to data, literal point to null
        ASSERT_EQ(data, literal2.GetValue<std::string>());

        std::string data3 = "copy hello world";
        Literal literal3(FieldType::STRING, data3.data(), data3.size(), /*own data*/ true);
        ASSERT_EQ(data3, literal3.GetValue<std::string>());
        // literal3 point to data, literal2 point to null
        literal3 = std::move(literal2);
        ASSERT_EQ(data, literal3.GetValue<std::string>());
    }
    {
        // test empty string
        std::string data = "";
        Literal literal(FieldType::STRING, data.data(), data.size(), /*own data*/ false);
        ASSERT_EQ(data, literal.GetValue<std::string>());
        Literal literal2 = literal;
        ASSERT_EQ(data, literal.GetValue<std::string>());

        std::string data3 = "copy hello world";
        Literal literal3(FieldType::STRING, data3.data(), data3.size(), /*own data*/ true);
        ASSERT_EQ(data3, literal3.GetValue<std::string>());
        literal3 = std::move(literal2);
        ASSERT_EQ(data, literal3.GetValue<std::string>());

        Literal literal4(FieldType::STRING, data.data(), data.size(), /*own data*/ true);
        ASSERT_EQ(data, literal4.GetValue<std::string>());
        literal4 = literal3;
        ASSERT_EQ(data, literal4.GetValue<std::string>());
    }
}
}  // namespace paimon::test
