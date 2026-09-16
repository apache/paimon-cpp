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

#include "paimon/common/global_index/key_serializer.h"

#include <cstdint>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/utils/math.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class KeySerializerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    Result<std::shared_ptr<Bytes>> Serialize(const Literal& literal,
                                             const std::shared_ptr<arrow::DataType>& type,
                                             MemoryPool* /*pool*/) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> serializer,
                               KeySerializer::Create(type, pool_));
        return serializer->Serialize(literal);
    }

    Result<Literal> Deserialize(const MemorySlice& slice,
                                const std::shared_ptr<arrow::DataType>& type,
                                MemoryPool* /*pool*/) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> serializer,
                               KeySerializer::Create(type, pool_));
        return serializer->Deserialize(slice);
    }

    Status ValidateSerializedKey(const MemorySlice& slice,
                                 const std::shared_ptr<arrow::DataType>& type) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> serializer,
                               KeySerializer::Create(type, pool_));
        return serializer->ValidateSerializedKey(slice);
    }

    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(KeySerializerTest, SerializeAndDeserializeAllTypes) {
    // BOOLEAN
    {
        Literal literal(true);
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, arrow::boolean(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, arrow::boolean(), pool_.get()));
        ASSERT_EQ(result.GetValue<bool>(), true);

        Literal literal_false(false);
        ASSERT_OK_AND_ASSIGN(bytes, Serialize(literal_false, arrow::boolean(), pool_.get()));
        slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(result, Deserialize(slice, arrow::boolean(), pool_.get()));
        ASSERT_EQ(result.GetValue<bool>(), false);
    }

    // TINYINT (int8)
    {
        Literal literal(static_cast<int8_t>(-42));
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, arrow::int8(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, arrow::int8(), pool_.get()));
        ASSERT_EQ(result.GetValue<int8_t>(), -42);
    }

    // SMALLINT (int16)
    {
        Literal literal(static_cast<int16_t>(12345));
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, arrow::int16(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, arrow::int16(), pool_.get()));
        ASSERT_EQ(result.GetValue<int16_t>(), 12345);
    }

    // INT (int32)
    {
        Literal literal(42);
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, arrow::int32(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, arrow::int32(), pool_.get()));
        ASSERT_EQ(result.GetValue<int32_t>(), 42);
    }

    // DATE (stored as int32)
    {
        Literal literal(FieldType::DATE, 18000);
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, arrow::date32(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, arrow::date32(), pool_.get()));
        ASSERT_EQ(result.GetType(), FieldType::DATE);
        ASSERT_EQ(result.GetValue<int32_t>(), 18000);
    }

    // BIGINT (int64)
    {
        Literal literal(static_cast<int64_t>(123456789012345LL));
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, arrow::int64(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, arrow::int64(), pool_.get()));
        ASSERT_EQ(result.GetValue<int64_t>(), 123456789012345LL);
    }

    // FLOAT
    {
        Literal literal(3.14f);
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, arrow::float32(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, arrow::float32(), pool_.get()));
        ASSERT_FLOAT_EQ(result.GetValue<float>(), 3.14f);
    }

    // DOUBLE
    {
        Literal literal(2.718281828);
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, arrow::float64(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, arrow::float64(), pool_.get()));
        ASSERT_DOUBLE_EQ(result.GetValue<double>(), 2.718281828);
    }

    // STRING
    {
        Literal literal(FieldType::STRING, "hello world", 11);
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, arrow::utf8(), pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, arrow::utf8(), pool_.get()));
        ASSERT_EQ(result.GetValue<std::string>(), "hello world");
    }

    // TIMESTAMP (compact, millis precision)
    {
        auto ts_type = arrow::timestamp(arrow::TimeUnit::MILLI);
        Literal literal(Timestamp::FromEpochMillis(1234567890));
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, ts_type, pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, ts_type, pool_.get()));
        ASSERT_EQ(result.GetValue<Timestamp>().GetMillisecond(), 1234567890);
    }

    // TIMESTAMP (non-compact, nano precision)
    {
        auto ts_type = arrow::timestamp(arrow::TimeUnit::NANO);
        Literal literal(Timestamp(5000, 123456));
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, ts_type, pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, ts_type, pool_.get()));
        ASSERT_EQ(result.GetValue<Timestamp>().GetMillisecond(), 5000);
        ASSERT_EQ(result.GetValue<Timestamp>().GetNanoOfMillisecond(), 123456);
    }

    // DECIMAL (compact, precision <= 18)
    {
        auto decimal_type = arrow::decimal128(10, 2);
        Literal literal(Decimal::FromUnscaledLong(12345, 10, 2));
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, decimal_type, pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, decimal_type, pool_.get()));
        ASSERT_EQ(result.GetValue<Decimal>().ToUnscaledLong(), 12345);
    }

    // DECIMAL (non-compact, precision > 18)
    {
        auto decimal_type = arrow::decimal128(25, 3);
        Literal literal(Decimal(25, 3, 9999999));
        ASSERT_OK_AND_ASSIGN(auto bytes, Serialize(literal, decimal_type, pool_.get()));
        auto slice = MemorySlice::Wrap(MemorySegment::Wrap(bytes));
        ASSERT_OK_AND_ASSIGN(auto result, Deserialize(slice, decimal_type, pool_.get()));
        ASSERT_EQ(result.GetValue<Decimal>().ToString(), literal.GetValue<Decimal>().ToString());
    }

    // NULL should fail
    {
        Literal null_literal(FieldType::INT);
        ASSERT_NOK_WITH_MSG(Serialize(null_literal, arrow::int32(), pool_.get()),
                            "cannot serialize null in KeySerializer");
    }

    // unsupported type
    {
        ASSERT_NOK_WITH_MSG(KeySerializer::Create(arrow::binary(), pool_),
                            "is not supported by global index now");
    }
}

TEST_F(KeySerializerTest, CreateRejectsMissingDependencies) {
    ASSERT_NOK_WITH_MSG(KeySerializer::Create(nullptr, pool_),
                        "Cannot create KeySerializer without a key type");
    ASSERT_NOK_WITH_MSG(KeySerializer::Create(arrow::int32(), nullptr),
                        "Cannot create KeySerializer without a memory pool");
}

TEST_F(KeySerializerTest, CanonicalizesFloatingPointNaN) {
    const auto float_nan = FloatingPointFromBits<float>(0xffc12345U);
    const auto canonical_float_nan = FloatingPointFromBits<float>(kCanonicalFloatNaNBits);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> float_bytes,
                         Serialize(Literal(float_nan), arrow::float32(), pool_.get()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> canonical_float_bytes,
                         Serialize(Literal(canonical_float_nan), arrow::float32(), pool_.get()));
    ASSERT_EQ(std::string(float_bytes->data(), float_bytes->size()),
              std::string(canonical_float_bytes->data(), canonical_float_bytes->size()));

    const auto double_nan = FloatingPointFromBits<double>(0xfff8123456789abcULL);
    const auto canonical_double_nan = FloatingPointFromBits<double>(kCanonicalDoubleNaNBits);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> double_bytes,
                         Serialize(Literal(double_nan), arrow::float64(), pool_.get()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> canonical_double_bytes,
                         Serialize(Literal(canonical_double_nan), arrow::float64(), pool_.get()));
    ASSERT_EQ(std::string(double_bytes->data(), double_bytes->size()),
              std::string(canonical_double_bytes->data(), canonical_double_bytes->size()));
}

TEST_F(KeySerializerTest, RejectsMalformedSerializedKeys) {
    auto wrap = [this](const std::string& value) {
        return MemorySlice::Wrap(std::make_shared<Bytes>(value, pool_.get()));
    };

    ASSERT_NOK(ValidateSerializedKey(wrap(std::string(3, '\0')), arrow::int32()));
    ASSERT_NOK(Deserialize(wrap(std::string(3, '\0')), arrow::int32(), pool_.get()));

    std::string invalid_boolean(1, static_cast<char>(2));
    ASSERT_NOK(ValidateSerializedKey(wrap(invalid_boolean), arrow::boolean()));

    auto nanos_timestamp = arrow::timestamp(arrow::TimeUnit::NANO);
    std::string unterminated_timestamp(9, '\0');
    unterminated_timestamp[8] = static_cast<char>(0x80);
    ASSERT_NOK(ValidateSerializedKey(wrap(unterminated_timestamp), nanos_timestamp));

    std::string out_of_range_timestamp(11, '\0');
    out_of_range_timestamp[8] = static_cast<char>(0xC0);
    out_of_range_timestamp[9] = static_cast<char>(0x84);
    out_of_range_timestamp[10] = static_cast<char>(0x3D);
    ASSERT_NOK(ValidateSerializedKey(wrap(out_of_range_timestamp), nanos_timestamp));

    auto non_compact_decimal = arrow::decimal128(25, 3);
    ASSERT_NOK(ValidateSerializedKey(wrap(""), non_compact_decimal));
    ASSERT_NOK(ValidateSerializedKey(wrap(std::string(17, '\0')), non_compact_decimal));

    auto narrow_decimal = arrow::decimal128(1, 0);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Bytes> out_of_range_decimal,
        Serialize(Literal(Decimal::FromUnscaledLong(10, 1, 0)), narrow_decimal, pool_.get()));
    ASSERT_NOK(ValidateSerializedKey(MemorySlice::Wrap(out_of_range_decimal), narrow_decimal));
}

TEST_F(KeySerializerTest, CreateComparator) {
    // INT comparator
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<KeySerializer> serializer,
                             KeySerializer::Create(arrow::int32(), pool_));
        auto comparator = serializer->CreateComparator();
        Literal literal_1(1);
        Literal literal_2(2);
        Literal literal_3(1);
        ASSERT_OK_AND_ASSIGN(auto bytes_1, Serialize(literal_1, arrow::int32(), pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto bytes_2, Serialize(literal_2, arrow::int32(), pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto bytes_3, Serialize(literal_3, arrow::int32(), pool_.get()));
        auto slice_1 = MemorySlice::Wrap(MemorySegment::Wrap(bytes_1));
        auto slice_2 = MemorySlice::Wrap(MemorySegment::Wrap(bytes_2));
        auto slice_3 = MemorySlice::Wrap(MemorySegment::Wrap(bytes_3));

        ASSERT_OK_AND_ASSIGN(auto cmp_result, comparator(slice_1, slice_2));
        ASSERT_LT(cmp_result, 0);
        ASSERT_OK_AND_ASSIGN(cmp_result, comparator(slice_2, slice_1));
        ASSERT_GT(cmp_result, 0);
        ASSERT_OK_AND_ASSIGN(cmp_result, comparator(slice_1, slice_3));
        ASSERT_EQ(cmp_result, 0);
    }

    // STRING comparator
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<KeySerializer> serializer,
                             KeySerializer::Create(arrow::utf8(), pool_));
        auto comparator = serializer->CreateComparator();
        Literal literal_a(FieldType::STRING, "apple", 5);
        Literal literal_b(FieldType::STRING, "banana", 6);
        Literal literal_c(FieldType::STRING, "apple", 5);
        ASSERT_OK_AND_ASSIGN(auto bytes_a, Serialize(literal_a, arrow::utf8(), pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto bytes_b, Serialize(literal_b, arrow::utf8(), pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto bytes_c, Serialize(literal_c, arrow::utf8(), pool_.get()));
        auto slice_a = MemorySlice::Wrap(MemorySegment::Wrap(bytes_a));
        auto slice_b = MemorySlice::Wrap(MemorySegment::Wrap(bytes_b));
        auto slice_c = MemorySlice::Wrap(MemorySegment::Wrap(bytes_c));

        ASSERT_OK_AND_ASSIGN(auto cmp_result, comparator(slice_a, slice_b));
        ASSERT_LT(cmp_result, 0);
        ASSERT_OK_AND_ASSIGN(cmp_result, comparator(slice_b, slice_a));
        ASSERT_GT(cmp_result, 0);
        ASSERT_OK_AND_ASSIGN(cmp_result, comparator(slice_a, slice_c));
        ASSERT_EQ(cmp_result, 0);
    }
}

}  // namespace paimon::test
