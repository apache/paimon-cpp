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

#include "paimon/core/stats/simple_stats_collector.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <variant>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/geospatial_type.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(SimpleStatsCollectorTest, TestSimple) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),
        arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),
        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::utf8()),
        arrow::field("f6", arrow::date32()),
        arrow::field("f7", arrow::time32(arrow::TimeUnit::MILLI)),
    };

    auto schema = arrow::schema(fields);
    SimpleStatsCollector collector(schema);
    auto pool = GetDefaultPool();
    ASSERT_OK(collector.Collect(BinaryRowGenerator::GenerateRow(
        {true, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(1),
         static_cast<int64_t>(1), std::string("abc"), 2025, static_cast<int32_t>(86399999)},
        pool.get())));
    ASSERT_OK(collector.Collect(BinaryRowGenerator::GenerateRow(
        {false, static_cast<int8_t>(2), static_cast<int16_t>(2), static_cast<int32_t>(2),
         static_cast<int64_t>(2), std::string("bcd"), 2026, static_cast<int32_t>(123)},
        pool.get())));
    ASSERT_OK_AND_ASSIGN(auto col_stats, collector.GetResult());
    ASSERT_EQ(col_stats[7]->GetFieldType(), FieldType::INT);
    ASSERT_OK_AND_ASSIGN(SimpleStats stats, SimpleStatsConverter::ToBinary(col_stats, pool.get()));

    auto expected_stats = BinaryRowGenerator::GenerateStats(
        {false, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(1),
         static_cast<int64_t>(1), std::string("abc"), 2025, static_cast<int32_t>(123)},
        {true, static_cast<int8_t>(2), static_cast<int16_t>(2), static_cast<int32_t>(2),
         static_cast<int64_t>(2), std::string("bcd"), 2026, static_cast<int32_t>(86399999)},
        std::vector<int64_t>({0, 0, 0, 0, 0, 0, 0, 0}), GetDefaultPool().get());

    ASSERT_EQ(stats, expected_stats);
    for (int64_t value : {1, 3}) {
        auto predicate = std::dynamic_pointer_cast<PredicateFilter>(
            PredicateBuilder::Equal(4, "f4", FieldType::BIGINT, Literal(value)));
        ASSERT_TRUE(predicate);
        ASSERT_OK_AND_ASSIGN(bool row_matches, predicate->Test(nullptr, stats.MinValues()));
        ASSERT_EQ(row_matches, value == 1);
        ASSERT_OK_AND_ASSIGN(
            bool stats_match,
            predicate->Test(nullptr, 2, stats.MinValues(), stats.MaxValues(), stats.NullCounts()));
        ASSERT_EQ(stats_match, value == 1);
    }
}

TEST(SimpleStatsCollectorTest, TestNull) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),   arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),   arrow::field("f5", arrow::utf8()),
        arrow::field("f6", arrow::date32()),  arrow::field("key", arrow::int32()),
    };

    auto schema = arrow::schema(fields);
    SimpleStatsCollector collector(schema);
    auto pool = GetDefaultPool();
    ASSERT_OK(collector.Collect(BinaryRowGenerator::GenerateRow(
        {NullType(), NullType(), NullType(), NullType(), NullType(), NullType(), NullType(), 100},
        pool.get())));
    ASSERT_OK_AND_ASSIGN(auto col_stats, collector.GetResult());
    ASSERT_EQ(8, col_stats.size());
    ASSERT_OK_AND_ASSIGN(SimpleStats stats, SimpleStatsConverter::ToBinary(col_stats, pool.get()));

    ASSERT_EQ(stats.MinValues(), stats.MaxValues());
    for (size_t i = 0; i < 7; ++i) {
        ASSERT_TRUE(stats.MinValues().IsNullAt(i));
    }
    ASSERT_EQ(stats.MinValues().GetInt(7), 100);
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> expected, stats.NullCounts().ToLongArray());
    ASSERT_EQ(expected, std::vector<int64_t>({1l, 1l, 1l, 1l, 1l, 1l, 1l, 0l}));
}

TEST(SimpleStatsCollectorTest, TestInvalidPartition) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::boolean())};

    auto schema = arrow::schema(fields);
    SimpleStatsCollector collector(schema);
    auto pool = GetDefaultPool();
    ASSERT_NOK_WITH_MSG(collector.Collect(BinaryRow::EmptyRow()), "partition schema not equal");
}

TEST(SimpleStatsCollectorTest, GeospatialNullCountsWithoutBounds) {
    ASSERT_OK_AND_ASSIGN(auto geometry, GeospatialType::GeometryField("geo"));
    ASSERT_OK_AND_ASSIGN(auto geography, GeospatialType::GeographyField("geo"));
    auto pool = GetDefaultPool();
    for (const auto& geo : {geometry, geography}) {
        SCOPED_TRACE(geo->ToString(true));
        auto schema = arrow::schema({geo, arrow::field("bytes", arrow::binary())});
        ASSERT_OK_AND_ASSIGN(auto type, GeospatialType::FromField(*geo));
        auto is_null = std::dynamic_pointer_cast<PredicateFilter>(
            PredicateBuilder::IsNull(0, "geo", type.Type()));
        auto is_not_null = std::dynamic_pointer_cast<PredicateFilter>(
            PredicateBuilder::IsNotNull(0, "geo", type.Type()));
        ASSERT_TRUE(is_null);
        ASSERT_TRUE(is_not_null);
        for (int64_t null_count : {0, 1, 3}) {
            SCOPED_TRACE(null_count);
            SimpleStatsCollector collector(schema);
            for (int64_t i = 0; i < 3; ++i) {
                auto row =
                    i < null_count
                        ? BinaryRowGenerator::GenerateRow({NullType(), NullType()}, pool.get())
                        : BinaryRowGenerator::GenerateRow(
                              {std::string(i == 2 ? "a" : "z"), std::string(i == 2 ? "a" : "z")},
                              pool.get());
                ASSERT_OK(collector.Collect(row));
                ASSERT_OK_AND_ASSIGN(bool row_is_null, is_null->Test(schema, row));
                ASSERT_EQ(row_is_null, i < null_count);
                ASSERT_OK_AND_ASSIGN(bool row_is_not_null, is_not_null->Test(schema, row));
                ASSERT_EQ(row_is_not_null, i >= null_count);
            }
            ASSERT_OK_AND_ASSIGN(auto columns, collector.GetResult());
            ASSERT_OK_AND_ASSIGN(auto stats, SimpleStatsConverter::ToBinary(columns, pool.get()));
            ASSERT_TRUE(stats.MinValues().IsNullAt(0));
            ASSERT_TRUE(stats.MaxValues().IsNullAt(0));
            for (int32_t i = 0; i < 2; ++i) {
                ASSERT_FALSE(stats.NullCounts().IsNullAt(i));
                ASSERT_EQ(stats.NullCounts().GetLong(i), null_count);
            }
            if (null_count == 3) {
                ASSERT_TRUE(stats.MinValues().IsNullAt(1));
                ASSERT_TRUE(stats.MaxValues().IsNullAt(1));
            } else {
                ASSERT_EQ(stats.MinValues().GetString(1).ToString(), "a");
                ASSERT_EQ(stats.MaxValues().GetString(1).ToString(), "z");
            }
            ASSERT_OK_AND_ASSIGN(
                bool contains_null,
                is_null->Test(schema, 3, stats.MinValues(), stats.MaxValues(), stats.NullCounts()));
            ASSERT_EQ(contains_null, null_count > 0);
            ASSERT_OK_AND_ASSIGN(bool contains_non_null,
                                 is_not_null->Test(schema, 3, stats.MinValues(), stats.MaxValues(),
                                                   stats.NullCounts()));
            ASSERT_EQ(contains_non_null, null_count < 3);
        }
        auto unknown_counts = BinaryRowGenerator::FromLongArrayWithNull({-1, -1}, pool.get());
        auto binary_bounds =
            BinaryRowGenerator::GenerateRow({std::string("a"), std::string("a")}, pool.get());
        auto comparison = std::dynamic_pointer_cast<PredicateFilter>(PredicateBuilder::Equal(
            0, "geo", FieldType::BINARY, Literal(FieldType::BINARY, "z", 1)));
        ASSERT_TRUE(comparison);
        ASSERT_NOK_WITH_MSG(comparison->Test(schema, binary_bounds),
                            "only supports null predicates");
        for (const auto& predicate : {is_null, is_not_null, comparison}) {
            ASSERT_OK_AND_ASSIGN(bool matches, predicate->Test(schema, 3, binary_bounds,
                                                               binary_bounds, unknown_counts));
            ASSERT_TRUE(matches);
        }
        auto known_counts = BinaryArray::FromLongArray({0, 0}, pool.get());
        ASSERT_OK_AND_ASSIGN(
            bool matches, comparison->Test(schema, 3, binary_bounds, binary_bounds, known_counts));
        ASSERT_TRUE(matches);
    }
}

}  // namespace paimon::test
