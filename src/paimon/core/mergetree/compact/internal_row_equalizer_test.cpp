/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/mergetree/compact/internal_row_equalizer.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string_view>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_map.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<InternalArray> CreateIntArray(const std::vector<int32_t>& values,
                                              MemoryPool* pool) {
    return std::make_shared<BinaryArray>(BinaryArray::FromIntArray(values, pool));
}

std::shared_ptr<InternalArray> CreateNullableIntArray(const std::vector<int32_t>& values,
                                                      int32_t null_pos, MemoryPool* pool) {
    auto array = std::make_shared<BinaryArray>();
    BinaryArrayWriter writer(array.get(), static_cast<int32_t>(values.size()), sizeof(int32_t),
                             pool);
    for (int32_t i = 0; i < static_cast<int32_t>(values.size()); ++i) {
        if (i == null_pos) {
            writer.SetNullValue<int32_t>(i);
        } else {
            writer.WriteInt(i, values[i]);
        }
    }
    writer.Complete();
    return array;
}

std::shared_ptr<InternalMap> CreateIntMap(const std::vector<int32_t>& keys,
                                          const std::vector<int32_t>& values, MemoryPool* pool) {
    BinaryArray key_array = BinaryArray::FromIntArray(keys, pool);
    BinaryArray value_array = BinaryArray::FromIntArray(values, pool);
    return BinaryMap::ValueOf(key_array, value_array, pool);
}

std::shared_ptr<InternalRow> CreateNestedRow(int32_t value, double floating_point) {
    return GenericRow::Of({value, floating_point});
}

}  // namespace

TEST(InternalRowEqualizerTest, PrimitiveTypesAndIgnoreFields) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {arrow::field("boolean", arrow::boolean()), arrow::field("tinyint", arrow::int8()),
         arrow::field("smallint", arrow::int16()), arrow::field("int", arrow::int32()),
         arrow::field("date", arrow::date32()), arrow::field("bigint", arrow::int64()),
         arrow::field("float", arrow::float32()), arrow::field("double", arrow::float64()),
         arrow::field("string", arrow::utf8()), arrow::field("binary", arrow::binary()),
         arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO)),
         arrow::field("decimal", arrow::decimal128(10, 2)),
         arrow::field("ignored", arrow::int32())});

    const float float_nan = std::numeric_limits<float>::quiet_NaN();
    const double double_nan = std::numeric_limits<double>::quiet_NaN();
    auto binary = std::make_shared<Bytes>("binary", pool.get());
    Decimal decimal(10, 2, DecimalUtils::StrToInt128("12345").value());
    std::vector<VariantType> left_values = {true,
                                            static_cast<char>(1),
                                            static_cast<int16_t>(2),
                                            static_cast<int32_t>(3),
                                            int32_t{4},
                                            int64_t{5},
                                            float_nan,
                                            double_nan,
                                            std::string_view("string"),
                                            binary,
                                            Timestamp(1234, 567000),
                                            decimal,
                                            int32_t{10}};
    std::vector<VariantType> right_values = left_values;
    right_values.back() = int32_t{20};

    std::unique_ptr<GenericRow> left = GenericRow::Of(left_values);
    std::unique_ptr<GenericRow> right = GenericRow::Of(right_values);
    ASSERT_OK_AND_ASSIGN(InternalRowEqualizer::Equalizer equalizer,
                         InternalRowEqualizer::Create(schema, {"ignored"}));
    ASSERT_TRUE(equalizer(*left, *right));

    right->SetField(/*pos=*/3, int32_t{30});
    ASSERT_FALSE(equalizer(*left, *right));
}

TEST(InternalRowEqualizerTest, NullAndFloatingPointSemantics) {
    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {arrow::field("value", arrow::float64()), arrow::field("nullable", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(InternalRowEqualizer::Equalizer equalizer,
                         InternalRowEqualizer::Create(schema, {}));

    std::unique_ptr<GenericRow> negative_zero =
        GenericRow::Of({static_cast<double>(-0.0), NullType()});
    std::unique_ptr<GenericRow> positive_zero =
        GenericRow::Of({static_cast<double>(0.0), NullType()});
    ASSERT_FALSE(equalizer(*negative_zero, *positive_zero));

    double nan1 = std::numeric_limits<double>::quiet_NaN();
    double nan2 = -std::numeric_limits<double>::quiet_NaN();
    std::unique_ptr<GenericRow> left_nan = GenericRow::Of({nan1, NullType()});
    std::unique_ptr<GenericRow> right_nan = GenericRow::Of({nan2, NullType()});
    ASSERT_TRUE(equalizer(*left_nan, *right_nan));

    right_nan->SetField(/*pos=*/1, int32_t{1});
    ASSERT_FALSE(equalizer(*left_nan, *right_nan));
}

TEST(InternalRowEqualizerTest, NestedTypes) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {arrow::field("array", arrow::list(arrow::int32())),
         arrow::field("map", arrow::map(arrow::int32(), arrow::int32())),
         arrow::field("row", arrow::struct_({arrow::field("value", arrow::int32()),
                                             arrow::field("floating", arrow::float64())}))});
    ASSERT_OK_AND_ASSIGN(InternalRowEqualizer::Equalizer equalizer,
                         InternalRowEqualizer::Create(schema, {}));

    std::unique_ptr<GenericRow> left =
        GenericRow::Of({CreateNullableIntArray({1, 0, 3}, /*null_pos=*/1, pool.get()),
                        CreateIntMap({1, 2}, {10, 20}, pool.get()),
                        CreateNestedRow(100, std::numeric_limits<double>::quiet_NaN())});
    std::unique_ptr<GenericRow> right =
        GenericRow::Of({CreateNullableIntArray({1, 9, 3}, /*null_pos=*/1, pool.get()),
                        CreateIntMap({1, 2}, {10, 20}, pool.get()),
                        CreateNestedRow(100, -std::numeric_limits<double>::quiet_NaN())});
    ASSERT_TRUE(equalizer(*left, *right));

    right->SetField(/*pos=*/0, CreateIntArray({1, 2}, pool.get()));
    ASSERT_FALSE(equalizer(*left, *right));

    right->SetField(/*pos=*/0, CreateNullableIntArray({1, 0, 3}, /*null_pos=*/1, pool.get()));
    right->SetField(/*pos=*/0, CreateNullableIntArray({1, 0, 4}, /*null_pos=*/1, pool.get()));
    ASSERT_FALSE(equalizer(*left, *right));

    right->SetField(/*pos=*/0, CreateNullableIntArray({1, 0, 3}, /*null_pos=*/1, pool.get()));
    right->SetField(/*pos=*/1, CreateIntMap({1, 3}, {10, 20}, pool.get()));
    ASSERT_FALSE(equalizer(*left, *right));

    right->SetField(/*pos=*/1, CreateIntMap({1, 2}, {10, 20}, pool.get()));
    right->SetField(/*pos=*/2, CreateNestedRow(101, 1.0));
    ASSERT_FALSE(equalizer(*left, *right));
}

TEST(InternalRowEqualizerTest, MapEqualityDoesNotDependOnEntryOrder) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("map", arrow::map(arrow::int32(), arrow::int32()))});
    ASSERT_OK_AND_ASSIGN(InternalRowEqualizer::Equalizer equalizer,
                         InternalRowEqualizer::Create(schema, {}));

    std::unique_ptr<GenericRow> left =
        GenericRow::Of({CreateIntMap({1, 2, 3}, {10, 20, 30}, pool.get())});
    std::unique_ptr<GenericRow> reordered =
        GenericRow::Of({CreateIntMap({3, 1, 2}, {30, 10, 20}, pool.get())});
    ASSERT_TRUE(equalizer(*left, *reordered));

    reordered->SetField(/*pos=*/0, CreateIntMap({3, 1, 2}, {30, 10, 21}, pool.get()));
    ASSERT_FALSE(equalizer(*left, *reordered));
}

TEST(InternalRowEqualizerTest, UnsupportedType) {
    ASSERT_NOK_WITH_MSG(InternalRowEqualizer::Create(
                            arrow::schema({arrow::field("unsupported", arrow::null())}), {}),
                        "Do not support equality for type null");
}

}  // namespace paimon::test
