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

#include "paimon/common/file_index/rangebitmap/range_bitmap_type_adapter.h"

#include <gtest/gtest.h>

#include "arrow/api.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(RangeBitmapTypeAdapterTest, TestStorageType) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RangeBitmapTypeAdapter> int_adapter,
                         RangeBitmapTypeAdapter::Create(arrow::int32()));
    ASSERT_EQ(FieldType::INT, int_adapter->GetStorageType());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RangeBitmapTypeAdapter> string_adapter,
                         RangeBitmapTypeAdapter::Create(arrow::utf8()));
    ASSERT_EQ(FieldType::STRING, string_adapter->GetStorageType());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RangeBitmapTypeAdapter> decimal_adapter,
                         RangeBitmapTypeAdapter::Create(arrow::decimal128(18, 2)));
    ASSERT_EQ(FieldType::BIGINT, decimal_adapter->GetStorageType());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RangeBitmapTypeAdapter> timestamp_adapter,
                         RangeBitmapTypeAdapter::Create(arrow::timestamp(arrow::TimeUnit::MICRO)));
    ASSERT_EQ(FieldType::BIGINT, timestamp_adapter->GetStorageType());

    ASSERT_NOK_WITH_MSG(RangeBitmapTypeAdapter::Create(arrow::decimal128(19, 2)),
                        "DECIMAL with precision in [1, 18]");
    ASSERT_NOK_WITH_MSG(RangeBitmapTypeAdapter::Create(arrow::timestamp(arrow::TimeUnit::NANO)),
                        "TIMESTAMP with precision in [0, 6]");
}

TEST(RangeBitmapTypeAdapterTest, TestDecimalLiteralConversion) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RangeBitmapTypeAdapter> adapter,
                         RangeBitmapTypeAdapter::Create(arrow::decimal128(10, 2)));

    ASSERT_OK_AND_ASSIGN(Literal converted,
                         adapter->ToStorageLiteral(Literal(Decimal(10, 2, 12345))));
    ASSERT_EQ(FieldType::BIGINT, converted.GetType());
    ASSERT_EQ(12345, converted.GetValue<int64_t>());

    ASSERT_OK_AND_ASSIGN(Literal converted_null,
                         adapter->ToStorageLiteral(Literal(FieldType::DECIMAL)));
    ASSERT_EQ(FieldType::BIGINT, converted_null.GetType());
    ASSERT_TRUE(converted_null.IsNull());

    ASSERT_NOK_WITH_MSG(adapter->ToStorageLiteral(Literal(int64_t{12345})),
                        "DECIMAL field requires a DECIMAL literal");
}

TEST(RangeBitmapTypeAdapterTest, TestTimestampLiteralConversion) {
    const Timestamp timestamp(1234, 567000);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RangeBitmapTypeAdapter> millis_adapter,
                         RangeBitmapTypeAdapter::Create(arrow::timestamp(arrow::TimeUnit::MILLI)));
    ASSERT_OK_AND_ASSIGN(Literal millis, millis_adapter->ToStorageLiteral(Literal(timestamp)));
    ASSERT_EQ(FieldType::BIGINT, millis.GetType());
    ASSERT_EQ(1234, millis.GetValue<int64_t>());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RangeBitmapTypeAdapter> micros_adapter,
                         RangeBitmapTypeAdapter::Create(arrow::timestamp(arrow::TimeUnit::MICRO)));
    ASSERT_OK_AND_ASSIGN(Literal micros, micros_adapter->ToStorageLiteral(Literal(timestamp)));
    ASSERT_EQ(FieldType::BIGINT, micros.GetType());
    ASSERT_EQ(1234567, micros.GetValue<int64_t>());

    ASSERT_OK_AND_ASSIGN(Literal converted_null,
                         micros_adapter->ToStorageLiteral(Literal(FieldType::TIMESTAMP)));
    ASSERT_EQ(FieldType::BIGINT, converted_null.GetType());
    ASSERT_TRUE(converted_null.IsNull());

    ASSERT_NOK_WITH_MSG(micros_adapter->ToStorageLiteral(Literal(int64_t{1234567})),
                        "TIMESTAMP field requires a TIMESTAMP literal");
}

TEST(RangeBitmapTypeAdapterTest, TestLiteralBatchConversion) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RangeBitmapTypeAdapter> adapter,
                         RangeBitmapTypeAdapter::Create(arrow::int32()));
    const std::vector<Literal> literals = {Literal(int32_t{1}), Literal(FieldType::INT),
                                           Literal(int32_t{3})};
    ASSERT_OK_AND_ASSIGN(std::vector<Literal> converted, adapter->ToStorageLiterals(literals));
    ASSERT_EQ(3, converted.size());
    ASSERT_EQ(1, converted[0].GetValue<int32_t>());
    ASSERT_TRUE(converted[1].IsNull());
    ASSERT_EQ(3, converted[2].GetValue<int32_t>());

    ASSERT_NOK_WITH_MSG(adapter->ToStorageLiterals({Literal(int32_t{1}), Literal(int64_t{2})}),
                        "literal type BIGINT does not match field type INT");
}

}  // namespace paimon::test
