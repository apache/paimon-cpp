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

#include "paimon/core/io/row_to_arrow_array_converter.h"

#include <memory>
#include <string>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

// The concrete converters select view access, so expose the owning-value appender here.
class RowToArrowArrayConverterTestAccess
    : public RowToArrowArrayConverter<BinaryRow, BatchReader::ReadBatch> {
 public:
    using RowToArrowArrayConverter::AppendField;
};

}  // namespace

TEST(RowToArrowArrayConverterTest, AppendStringWithoutView) {
    auto pool = GetDefaultPool();
    auto arrow_pool = GetArrowPool(pool);
    arrow::StringBuilder builder(arrow_pool.get());
    int32_t reserve_count = 0;
    ASSERT_OK_AND_ASSIGN(auto append, RowToArrowArrayConverterTestAccess::AppendField(
                                          /*use_view=*/false, &builder, &reserve_count));

    const std::string embedded_null("a\0b", 3);
    auto row = BinaryRowGenerator::GenerateRow(
        {std::string("paimon"), std::string(), NullType(), embedded_null}, pool.get());
    for (int32_t i = 0; i < row.GetFieldCount(); ++i) {
        ASSERT_TRUE(append(row, i).ok());
    }

    std::shared_ptr<arrow::Array> result;
    ASSERT_TRUE(builder.Finish(&result).ok());
    auto strings = std::dynamic_pointer_cast<arrow::StringArray>(result);
    ASSERT_TRUE(strings);
    ASSERT_EQ(strings->length(), 4);
    ASSERT_EQ(strings->null_count(), 1);
    ASSERT_EQ(strings->GetString(0), "paimon");
    ASSERT_TRUE(strings->IsValid(1));
    ASSERT_EQ(strings->GetString(1), "");
    ASSERT_TRUE(strings->IsNull(2));
    ASSERT_EQ(strings->GetString(3), embedded_null);
}

TEST(RowToArrowArrayConverterTest, AppendBinaryWithoutView) {
    auto pool = GetDefaultPool();
    auto arrow_pool = GetArrowPool(pool);
    arrow::BinaryBuilder builder(arrow_pool.get());
    int32_t reserve_count = 0;
    ASSERT_OK_AND_ASSIGN(auto append, RowToArrowArrayConverterTestAccess::AppendField(
                                          /*use_view=*/false, &builder, &reserve_count));

    const std::string binary_value("\0\xff\x01", 3);
    auto row = BinaryRowGenerator::GenerateRow(
        {std::string("paimon"), std::string(), NullType(), binary_value}, pool.get());
    for (int32_t i = 0; i < row.GetFieldCount(); ++i) {
        ASSERT_TRUE(append(row, i).ok());
    }

    std::shared_ptr<arrow::Array> result;
    ASSERT_TRUE(builder.Finish(&result).ok());
    auto binaries = std::dynamic_pointer_cast<arrow::BinaryArray>(result);
    ASSERT_TRUE(binaries);
    ASSERT_EQ(binaries->length(), 4);
    ASSERT_EQ(binaries->null_count(), 1);
    ASSERT_EQ(binaries->GetString(0), "paimon");
    ASSERT_TRUE(binaries->IsValid(1));
    ASSERT_EQ(binaries->GetString(1), "");
    ASSERT_TRUE(binaries->IsNull(2));
    ASSERT_EQ(binaries->GetString(3), binary_value);
}

}  // namespace paimon::test
