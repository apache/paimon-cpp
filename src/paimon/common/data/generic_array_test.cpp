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

#include "paimon/common/data/generic_array.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/generic_map.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(GenericArrayTest, TestGetters) {
    const Decimal decimal(/*precision=*/10, /*scale=*/2, 12345);
    const Timestamp timestamp(/*millisecond=*/1234, /*nano_of_millisecond=*/567890);
    std::shared_ptr<InternalArray> nested_array =
        std::make_shared<GenericArray>(std::vector<VariantType>{int32_t{42}});
    std::shared_ptr<InternalMap> map = std::make_shared<GenericMap>(nested_array, nested_array);
    auto row = std::make_shared<GenericRow>(1);
    row->SetField(0, int32_t{7});
    GenericArray array({true, false, char{42}, int16_t{-1234}, int32_t{-56789},
                        int64_t{12345678900}, -1.5F, 2.25, decimal, timestamp, nested_array, map,
                        std::shared_ptr<InternalRow>(row), NullType{}});

    ASSERT_EQ(array.Size(), 14);
    for (int32_t i = 0; i < array.Size() - 1; ++i) {
        ASSERT_FALSE(array.IsNullAt(i)) << i;
    }
    ASSERT_TRUE(array.IsNullAt(13));
    ASSERT_TRUE(array.GetBoolean(0));
    ASSERT_FALSE(array.GetBoolean(1));
    ASSERT_EQ(array.GetByte(2), char{42});
    ASSERT_EQ(array.GetShort(3), -1234);
    ASSERT_EQ(array.GetInt(4), -56789);
    ASSERT_EQ(array.GetDate(4), -56789);
    ASSERT_EQ(array.GetLong(5), int64_t{12345678900});
    ASSERT_FLOAT_EQ(array.GetFloat(6), -1.5F);
    ASSERT_DOUBLE_EQ(array.GetDouble(7), 2.25);
    ASSERT_EQ(array.GetDecimal(8, /*precision=*/10, /*scale=*/2), decimal);
    ASSERT_EQ(array.GetTimestamp(9, /*precision=*/9), timestamp);
    ASSERT_EQ(array.GetArray(10), nested_array);
    ASSERT_EQ(array.GetArray(10)->GetInt(0), 42);
    ASSERT_EQ(array.GetMap(11), map);
    ASSERT_EQ(array.GetMap(11)->ValueArray()->GetInt(0), 42);
    ASSERT_EQ(array.GetRow(12, /*num_fields=*/1), row);
    ASSERT_EQ(array.GetRow(12, /*num_fields=*/1)->GetInt(0), 7);
}

TEST(GenericArrayTest, TestStringAndBinaryRepresentations) {
    auto pool = GetDefaultPool();
    for (const std::string& value :
         {std::string(), std::string("hello world"), std::string("a\0b", 3)}) {
        SCOPED_TRACE(value);
        BinaryString string = BinaryString::FromString(value, pool.get());
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(value, pool.get());
        GenericArray array({string, bytes, std::string_view(value)});

        ASSERT_EQ(array.Size(), 3);
        for (int32_t i = 0; i < array.Size(); ++i) {
            ASSERT_FALSE(array.IsNullAt(i));
            ASSERT_EQ(array.GetString(i).ToString(), value) << i;
            ASSERT_EQ(array.GetStringView(i), value) << i;
            auto result = array.GetBinary(i);
            ASSERT_TRUE(result);
            ASSERT_EQ(*result, *bytes) << i;
        }
        ASSERT_EQ(array.GetBinary(1), bytes);
        ASSERT_EQ(array.GetStringView(2).data(), value.data());
    }
}

TEST(GenericArrayTest, TestHoldersKeepBorrowedValuesAlive) {
    auto pool = GetDefaultPool();
    const std::string string_value = "string owned by the source array";
    const std::string binary_value("binary\0value", 12);
    std::weak_ptr<InternalArray> source_ref;
    std::unique_ptr<GenericArray> array;
    {
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(binary_value, pool.get());
        auto source = std::make_shared<GenericArray>(
            std::vector<VariantType>{BinaryString::FromString(string_value, pool.get()), bytes});
        source_ref = source;
        array = std::make_unique<GenericArray>(
            std::vector<VariantType>{source->GetStringView(0), source->GetStringView(1)},
            std::vector<std::shared_ptr<InternalArray>>{source});
    }

    ASSERT_FALSE(source_ref.expired());
    ASSERT_EQ(array->GetStringView(0), string_value);
    ASSERT_EQ(array->GetString(0).ToString(), string_value);
    auto bytes = array->GetBinary(1);
    ASSERT_TRUE(bytes);
    ASSERT_EQ(std::string(bytes->data(), bytes->size()), binary_value);
    array.reset();
    ASSERT_TRUE(source_ref.expired());
    ASSERT_EQ(std::string(bytes->data(), bytes->size()), binary_value);
}

TEST(GenericArrayTest, TestToPrimitiveArrays) {
    {
        GenericArray array({true, false, true});
        ASSERT_OK_AND_ASSIGN(std::vector<char> result, array.ToBooleanArray());
        ASSERT_EQ(result, (std::vector<char>{1, 0, 1}));
    }
    {
        GenericArray array({char{0}, char{42}, std::numeric_limits<char>::max()});
        ASSERT_OK_AND_ASSIGN(std::vector<char> result, array.ToByteArray());
        ASSERT_EQ(result, (std::vector<char>{0, 42, std::numeric_limits<char>::max()}));
    }
    {
        GenericArray array(
            {std::numeric_limits<int16_t>::min(), int16_t{0}, std::numeric_limits<int16_t>::max()});
        ASSERT_OK_AND_ASSIGN(std::vector<int16_t> result, array.ToShortArray());
        ASSERT_EQ(result, (std::vector<int16_t>{std::numeric_limits<int16_t>::min(), 0,
                                                std::numeric_limits<int16_t>::max()}));
    }
    {
        GenericArray array(
            {std::numeric_limits<int32_t>::min(), int32_t{0}, std::numeric_limits<int32_t>::max()});
        ASSERT_OK_AND_ASSIGN(std::vector<int32_t> result, array.ToIntArray());
        ASSERT_EQ(result, (std::vector<int32_t>{std::numeric_limits<int32_t>::min(), 0,
                                                std::numeric_limits<int32_t>::max()}));
    }
    {
        GenericArray array(
            {std::numeric_limits<int64_t>::min(), int64_t{0}, std::numeric_limits<int64_t>::max()});
        ASSERT_OK_AND_ASSIGN(std::vector<int64_t> result, array.ToLongArray());
        ASSERT_EQ(result, (std::vector<int64_t>{std::numeric_limits<int64_t>::min(), 0,
                                                std::numeric_limits<int64_t>::max()}));
    }
    {
        GenericArray array({-1.5F, 0.0F, 2.25F});
        ASSERT_OK_AND_ASSIGN(std::vector<float> result, array.ToFloatArray());
        ASSERT_EQ(result, (std::vector<float>{-1.5F, 0.0F, 2.25F}));
    }
    {
        GenericArray array({-1.5, 0.0, 2.25});
        ASSERT_OK_AND_ASSIGN(std::vector<double> result, array.ToDoubleArray());
        ASSERT_EQ(result, (std::vector<double>{-1.5, 0.0, 2.25}));
    }
}

TEST(GenericArrayTest, TestEmptyArray) {
    GenericArray array({});
    ASSERT_EQ(array.Size(), 0);
    ASSERT_OK_AND_ASSIGN(std::vector<char> booleans, array.ToBooleanArray());
    ASSERT_TRUE(booleans.empty());
    ASSERT_OK_AND_ASSIGN(std::vector<char> bytes, array.ToByteArray());
    ASSERT_TRUE(bytes.empty());
    ASSERT_OK_AND_ASSIGN(std::vector<int16_t> shorts, array.ToShortArray());
    ASSERT_TRUE(shorts.empty());
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> ints, array.ToIntArray());
    ASSERT_TRUE(ints.empty());
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> longs, array.ToLongArray());
    ASSERT_TRUE(longs.empty());
    ASSERT_OK_AND_ASSIGN(std::vector<float> floats, array.ToFloatArray());
    ASSERT_TRUE(floats.empty());
    ASSERT_OK_AND_ASSIGN(std::vector<double> doubles, array.ToDoubleArray());
    ASSERT_TRUE(doubles.empty());
}

TEST(GenericArrayTest, TestPrimitiveArraysRejectNulls) {
    GenericArray null_array({NullType{}});
    ASSERT_TRUE(null_array.IsNullAt(0));
    const std::string message = "Primitive array must not contain a null value.";
    ASSERT_NOK_WITH_MSG(null_array.ToBooleanArray(), message);
    ASSERT_NOK_WITH_MSG(null_array.ToByteArray(), message);
    ASSERT_NOK_WITH_MSG(null_array.ToShortArray(), message);
    ASSERT_NOK_WITH_MSG(null_array.ToIntArray(), message);
    ASSERT_NOK_WITH_MSG(null_array.ToLongArray(), message);
    ASSERT_NOK_WITH_MSG(null_array.ToFloatArray(), message);
    ASSERT_NOK_WITH_MSG(null_array.ToDoubleArray(), message);

    GenericArray booleans({true, NullType{}, false});
    ASSERT_NOK_WITH_MSG(booleans.ToBooleanArray(), message);
    GenericArray ints({int32_t{1}, NullType{}, int32_t{2}});
    ASSERT_NOK_WITH_MSG(ints.ToIntArray(), message);
}

}  // namespace paimon::test
