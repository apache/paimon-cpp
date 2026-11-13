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

#include "paimon/core/casting/cast_executor_factory.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/data_type.h"
#include "paimon/core/casting/binary_to_blob_cast_executor.h"
#include "paimon/core/casting/binary_to_string_cast_executor.h"
#include "paimon/core/casting/boolean_to_decimal_cast_executor.h"
#include "paimon/core/casting/boolean_to_numeric_cast_executor.h"
#include "paimon/core/casting/boolean_to_string_cast_executor.h"
#include "paimon/core/casting/date_to_string_cast_executor.h"
#include "paimon/core/casting/date_to_timestamp_cast_executor.h"
#include "paimon/core/casting/decimal_to_decimal_cast_executor.h"
#include "paimon/core/casting/decimal_to_numeric_primitive_cast_executor.h"
#include "paimon/core/casting/numeric_primitive_cast_executor.h"
#include "paimon/core/casting/numeric_primitive_to_decimal_cast_executor.h"
#include "paimon/core/casting/numeric_primitive_to_timestamp_cast_executor.h"
#include "paimon/core/casting/numeric_to_boolean_cast_executor.h"
#include "paimon/core/casting/numeric_to_string_cast_executor.h"
#include "paimon/core/casting/string_to_binary_cast_executor.h"
#include "paimon/core/casting/string_to_boolean_cast_executor.h"
#include "paimon/core/casting/string_to_date_cast_executor.h"
#include "paimon/core/casting/string_to_decimal_cast_executor.h"
#include "paimon/core/casting/string_to_numeric_primitive_cast_executor.h"
#include "paimon/core/casting/string_to_time_cast_executor.h"
#include "paimon/core/casting/string_to_timestamp_cast_executor.h"
#include "paimon/core/casting/time_to_string_cast_executor.h"
#include "paimon/core/casting/time_to_timestamp_cast_executor.h"
#include "paimon/core/casting/timestamp_to_date_cast_executor.h"
#include "paimon/core/casting/timestamp_to_numeric_primitive_cast_executor.h"
#include "paimon/core/casting/timestamp_to_string_cast_executor.h"
#include "paimon/core/casting/timestamp_to_time_cast_executor.h"
#include "paimon/core/casting/timestamp_to_timestamp_cast_executor.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::test {
TEST(CastExecutorFactoryTest, TestRegister) {
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::TINYINT, FieldType::INT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<NumericPrimitiveCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::INT, FieldType::BIGINT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<NumericPrimitiveCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::DECIMAL, FieldType::TIMESTAMP);
        ASSERT_FALSE(cast_executor);
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BOOLEAN, FieldType::TINYINT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<BooleanToNumericCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BOOLEAN, FieldType::STRING);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<BooleanToStringCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::INT, FieldType::BOOLEAN);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<NumericToBooleanCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::BOOLEAN);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToBooleanCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::BIGINT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToNumericPrimitiveCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::FLOAT, FieldType::STRING);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<NumericToStringCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::BINARY);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToBinaryCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BINARY, FieldType::STRING);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<BinaryToStringCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BINARY, FieldType::BLOB);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<BinaryToBlobCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::DATE, FieldType::STRING);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<DateToStringCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::DATE, FieldType::TIMESTAMP);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<DateToTimestampCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::INT, FieldType::TIMESTAMP);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(
            std::dynamic_pointer_cast<NumericPrimitiveToTimestampCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::DATE);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToDateCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::TIMESTAMP, FieldType::STRING);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<TimestampToStringCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::TIMESTAMP, FieldType::DATE);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<TimestampToDateCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::TIMESTAMP, FieldType::BIGINT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(
            std::dynamic_pointer_cast<TimestampToNumericPrimitiveCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::TIMESTAMP);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToTimestampCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::DECIMAL, FieldType::TINYINT);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(
            std::dynamic_pointer_cast<DecimalToNumericPrimitiveCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BIGINT, FieldType::DECIMAL);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(
            std::dynamic_pointer_cast<NumericPrimitiveToDecimalCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::STRING, FieldType::DECIMAL);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<StringToDecimalCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::DECIMAL, FieldType::DECIMAL);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<DecimalToDecimalCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::TIMESTAMP, FieldType::TIMESTAMP);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<TimestampToTimestampCastExecutor>(cast_executor));
    }
    {
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::BOOLEAN, FieldType::DECIMAL);
        ASSERT_TRUE(cast_executor);
        ASSERT_TRUE(std::dynamic_pointer_cast<BooleanToDecimalCastExecutor>(cast_executor));
    }
    {
        // test non-exist cast executor
        auto* factory = CastExecutorFactory::GetCastExecutorFactory();
        ASSERT_FALSE(factory->executor_map_.empty());
        auto cast_executor = factory->GetCastExecutor(FieldType::ARRAY, FieldType::MAP);
        ASSERT_FALSE(cast_executor);
    }
}

TEST(CastExecutorFactoryTest, TestTimeCastExecutors) {
    auto* factory = CastExecutorFactory::GetCastExecutorFactory();
    ASSERT_TRUE(std::dynamic_pointer_cast<StringToTimeCastExecutor>(
        factory->GetCastExecutor(FieldType::STRING, FieldType::TIME)));
    ASSERT_TRUE(std::dynamic_pointer_cast<TimeToStringCastExecutor>(
        factory->GetCastExecutor(FieldType::TIME, FieldType::STRING)));
    ASSERT_TRUE(std::dynamic_pointer_cast<TimestampToTimeCastExecutor>(
        factory->GetCastExecutor(FieldType::TIMESTAMP, FieldType::TIME)));
    ASSERT_TRUE(std::dynamic_pointer_cast<TimeToTimestampCastExecutor>(
        factory->GetCastExecutor(FieldType::TIME, FieldType::TIMESTAMP)));
    ASSERT_FALSE(factory->GetCastExecutor(FieldType::INT, FieldType::TIME));
    ASSERT_FALSE(factory->GetCastExecutor(FieldType::TIME, FieldType::DATE));

    auto time_field = [](const std::string& precision) {
        return DataField(
            0, arrow::field("t", arrow::time32(arrow::TimeUnit::MILLI),
                            /*nullable=*/true,
                            arrow::key_value_metadata({DataType::kTimePrecision}, {precision})));
    };
    std::vector<std::pair<std::string, std::string>> cases = {{"0", "01:02:03"},
                                                              {"3", "01:02:03.0"}};
    for (const auto& [precision, expected] : cases) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<CastExecutor> executor,
                             factory->GetCastExecutor(time_field(precision), FieldType::STRING));
        ASSERT_TRUE(executor);
        ASSERT_OK_AND_ASSIGN(Literal literal,
                             executor->Cast(Literal(FieldType::TIME, 3723000), arrow::utf8()));
        ASSERT_EQ(expected, literal.GetValue<std::string>());
    }
    ASSERT_NOK(factory->GetCastExecutor(time_field("x"), FieldType::STRING));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CastExecutor> executor,
                         factory->GetCastExecutor(DataField(1, arrow::field("i", arrow::int32())),
                                                  FieldType::STRING));
    ASSERT_TRUE(std::dynamic_pointer_cast<NumericToStringCastExecutor>(executor));

    TimezoneGuard tz_guard("Asia/Shanghai");
    std::vector<std::pair<std::shared_ptr<arrow::DataType>, int32_t>> ts_cases = {
        {arrow::timestamp(arrow::TimeUnit::MILLI), 0},
        {arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Shanghai"), 28800000}};
    for (const auto& [type, expected] : ts_cases) {
        ASSERT_OK_AND_ASSIGN(
            executor,
            factory->GetCastExecutor(DataField(2, arrow::field("ts", type)), FieldType::TIME));
        ASSERT_TRUE(std::dynamic_pointer_cast<TimestampToTimeCastExecutor>(executor));
        ASSERT_OK_AND_ASSIGN(
            Literal literal,
            executor->Cast(Literal(Timestamp(0, 0)), arrow::time32(arrow::TimeUnit::MILLI)));
        ASSERT_EQ(Literal(FieldType::TIME, expected), literal) << type->ToString();
    }
}
}  // namespace paimon::test
