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

#include "paimon/core/mergetree/compact/aggregate/field_merge_map_agg.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/generic_array.h"
#include "paimon/common/data/generic_map.h"
#include "paimon/common/data/serializer/binary_serializer_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

VariantType IntMap(std::vector<VariantType> keys, std::vector<VariantType> values) {
    std::shared_ptr<InternalArray> key_array = std::make_shared<GenericArray>(std::move(keys));
    std::shared_ptr<InternalArray> value_array = std::make_shared<GenericArray>(std::move(values));
    return VariantType(checked_pointer_cast<InternalMap>(
        std::make_shared<GenericMap>(std::move(key_array), std::move(value_array))));
}

int32_t FindValue(const VariantType& value, int32_t key) {
    auto map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(value);
    for (int32_t i = 0; i < map->Size(); ++i) {
        if (map->KeyArray()->GetInt(i) == key) {
            return map->ValueArray()->GetInt(i);
        }
    }
    return -1;
}

std::vector<int32_t> Keys(const VariantType& value) {
    auto map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(value);
    std::vector<int32_t> keys;
    for (int32_t i = 0; i < map->Size(); ++i) {
        keys.push_back(map->KeyArray()->GetInt(i));
    }
    return keys;
}

}  // namespace

TEST(FieldMergeMapAggTest, InputOverwritesAndRetractUsesKeys) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldMergeMapAgg> agg,
                         FieldMergeMapAgg::Create(arrow::map(arrow::int32(), arrow::int32()), "f",
                                                  GetDefaultPool()));

    ASSERT_OK_AND_ASSIGN(VariantType merged,
                         agg->Agg(IntMap({int32_t{1}, int32_t{2}}, {int32_t{10}, int32_t{20}}),
                                  IntMap({int32_t{2}, int32_t{3}}, {int32_t{200}, int32_t{30}})));
    auto merged_map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(merged);
    ASSERT_EQ(3, merged_map->Size());
    ASSERT_EQ(10, FindValue(merged, 1));
    ASSERT_EQ(200, FindValue(merged, 2));
    ASSERT_EQ(30, FindValue(merged, 3));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BinaryMap> binary_merged,
        BinarySerializerUtils::WriteBinaryMap(
            merged_map, arrow::map(arrow::int32(), arrow::int32()), GetDefaultPool().get()));
    ASSERT_EQ(3, binary_merged->Size());

    ASSERT_OK_AND_ASSIGN(VariantType retracted,
                         agg->Retract(merged, IntMap({int32_t{2}}, {int32_t{-999}})));
    auto retracted_map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(retracted);
    ASSERT_EQ(2, retracted_map->Size());
    ASSERT_EQ(-1, FindValue(retracted, 2));
}

TEST(FieldMergeMapAggTest, HashPathDeduplicatesAccumulatorAndPreservesOrder) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldMergeMapAgg> agg,
                         FieldMergeMapAgg::Create(arrow::map(arrow::int32(), arrow::int32()), "f",
                                                  GetDefaultPool()));
    std::vector<VariantType> accumulator_keys;
    std::vector<VariantType> accumulator_values;
    for (int32_t i = 0; i <= 100; ++i) {
        accumulator_keys.emplace_back(i);
        accumulator_values.emplace_back(100 + i);
    }
    accumulator_keys.emplace_back(10);
    accumulator_values.emplace_back(1010);

    std::vector<VariantType> input_keys;
    std::vector<VariantType> input_values;
    for (int32_t i = 50; i <= 140; ++i) {
        input_keys.emplace_back(i);
        input_values.emplace_back(2000 + i);
    }
    input_keys.emplace_back(60);
    input_values.emplace_back(2060);

    ASSERT_OK_AND_ASSIGN(
        VariantType result,
        agg->Agg(IntMap(std::move(accumulator_keys), std::move(accumulator_values)),
                 IntMap(std::move(input_keys), std::move(input_values))));
    std::vector<int32_t> expected_keys;
    for (int32_t i = 0; i <= 140; ++i) {
        expected_keys.push_back(i);
    }
    ASSERT_EQ(expected_keys, Keys(result));
    ASSERT_EQ(1010, FindValue(result, 10));
    ASSERT_EQ(2050, FindValue(result, 50));
    ASSERT_EQ(2060, FindValue(result, 60));
    ASSERT_EQ(2140, FindValue(result, 140));
}

TEST(FieldMergeMapAggTest, HashPathUsesStringContentAndOverwrites) {
    std::vector<std::shared_ptr<arrow::DataType>> key_types{arrow::utf8(), arrow::binary()};
    for (const auto& key_type : key_types) {
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FieldMergeMapAgg> agg,
            FieldMergeMapAgg::Create(arrow::map(key_type, arrow::int32()), "f", GetDefaultPool()));
        std::vector<std::string> left(100, "same-content-with-a-longer-buffer");
        std::vector<std::string> right(100, "same-content-with-a-longer-buffer");
        std::vector<VariantType> left_keys;
        std::vector<VariantType> right_keys;
        std::vector<VariantType> left_values(100, VariantType(int32_t{10}));
        std::vector<VariantType> right_values(100, VariantType(int32_t{20}));
        for (const auto& value : left) {
            left_keys.emplace_back(std::string_view(value));
        }
        for (const auto& value : right) {
            right_keys.emplace_back(std::string_view(value));
        }

        ASSERT_OK_AND_ASSIGN(VariantType result,
                             agg->Agg(IntMap(std::move(left_keys), std::move(left_values)),
                                      IntMap(std::move(right_keys), std::move(right_values))));
        auto result_map = DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(result);
        ASSERT_EQ(1, result_map->Size());
        ASSERT_EQ("same-content-with-a-longer-buffer", result_map->KeyArray()->GetStringView(0));
        ASSERT_EQ(20, result_map->ValueArray()->GetInt(0));
    }
}

TEST(FieldMergeMapAggTest, NullAndTypeValidation) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldMergeMapAgg> agg,
                         FieldMergeMapAgg::Create(arrow::map(arrow::int32(), arrow::int32()), "f",
                                                  GetDefaultPool()));
    VariantType map = IntMap({int32_t{1}}, {int32_t{10}});
    ASSERT_OK_AND_ASSIGN(VariantType result, agg->Agg(VariantType(NullType()), map));
    ASSERT_EQ(10, FindValue(result, 1));
    ASSERT_OK_AND_ASSIGN(VariantType accumulator_result, agg->Agg(map, VariantType(NullType())));
    ASSERT_EQ(10, FindValue(accumulator_result, 1));
    ASSERT_OK_AND_ASSIGN(VariantType null_result,
                         agg->Agg(VariantType(NullType()), VariantType(NullType())));
    ASSERT_TRUE(DataDefine::IsVariantNull(null_result));
    ASSERT_NOK(FieldMergeMapAgg::Create(arrow::int32(), "f", GetDefaultPool()));
}

// Ported from Java FieldAggregatorRetractNullTest: retraction is supported and returns a value.
TEST(FieldMergeMapAggTest, RetractOnEmptyMapIsSupported) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldMergeMapAgg> agg,
                         FieldMergeMapAgg::Create(arrow::map(arrow::int32(), arrow::int32()), "f",
                                                  GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(VariantType result, agg->Retract(IntMap({}, {}), IntMap({}, {})));
    ASSERT_EQ(0, DataDefine::GetVariantValue<std::shared_ptr<InternalMap>>(result)->Size());
}

}  // namespace paimon::test
