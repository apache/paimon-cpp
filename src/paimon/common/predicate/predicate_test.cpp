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

#include "paimon/predicate/predicate.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/predicate/equal.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::test {
namespace {
// Retain the Literal-based batch path as an independent reference for typed equality.
class LiteralBatchEqual : public NullFalseLeafBinaryFunction {
 public:
    using NullFalseLeafBinaryFunction::Test;
    Result<bool> Test(const Literal& value, const Literal& literal) const override {
        return Equal::Instance().Test(value, literal);
    }
    Result<bool> Test(int64_t rows, const Literal& min, const Literal& max,
                      const std::optional<int64_t>& nulls, const Literal& literal) const override {
        return Equal::Instance().Test(rows, min, max, nulls, literal);
    }
    Type GetType() const override {
        return Type::EQUAL;
    }
    std::string ToString() const override {
        return "LiteralBatchEqual";
    }
    const LeafFunction* Negate() const override {
        return Equal::Instance().Negate();
    }
};

void CheckEqualBatch(const std::shared_ptr<arrow::Array>& array,
                     const std::vector<Literal>& literals) {
    const LiteralBatchEqual reference;
    const LeafFunction& candidate = Equal::Instance();
    for (int64_t offset = 0; offset <= array->length(); ++offset) {
        for (int64_t length = 0; length <= array->length() - offset; ++length) {
            const auto slice = array->Slice(offset, length);
            auto expected = reference.Test(*slice, literals, arrow::default_memory_pool());
            auto actual = candidate.Test(*slice, literals, arrow::default_memory_pool());
            ASSERT_EQ(actual.ok(), expected.ok());
            if (expected.ok()) {
                ASSERT_EQ(actual.value(), expected.value());
            } else {
                ASSERT_EQ(actual.status().ToString(), expected.status().ToString());
            }
        }
    }
}

// Reference implementation of the eager batch evaluation used before candidate selection.
Result<std::vector<char>> TestEager(const std::shared_ptr<Predicate>& predicate,
                                    const arrow::Array& array, arrow::MemoryPool* pool) {
    auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
    if (!compound) {
        return std::dynamic_pointer_cast<PredicateFilter>(predicate)->Test(array, pool);
    }
    const bool is_and = predicate->GetFunction().GetType() == Function::Type::AND;
    std::vector<char> result(array.length(), is_and);
    for (const auto& child : compound->Children()) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<char> matches, TestEager(child, array, pool));
        for (size_t i = 0; i < result.size(); ++i) {
            result[i] = is_and ? (result[i] & matches[i]) : (result[i] | matches[i]);
        }
    }
    return result;
}

class RecordingBatchEqual : public LiteralBatchEqual {
 public:
    using LiteralBatchEqual::Test;

    Result<std::vector<char>> Test(const arrow::Array& array, const std::vector<Literal>& literals,
                                   arrow::MemoryPool* pool) const override {
        batch_lengths.push_back(array.length());
        return LiteralBatchEqual::Test(array, literals, pool);
    }

    mutable std::vector<int64_t> batch_lengths;
};

class RecordingEqualPredicate : public LeafPredicateImpl {
 public:
    RecordingEqualPredicate(int32_t index, const std::string& name, FieldType type,
                            const Literal& literal)
        : LeafPredicateImpl(Equal::Instance(), index, name, type, {literal}) {}

    Result<std::vector<char>> TestSelected(const arrow::Array& array,
                                           const std::vector<int64_t>& selection,
                                           arrow::MemoryPool* pool) const override {
        selections.push_back(selection);
        return LeafPredicateImpl::TestSelected(array, selection, pool);
    }

    mutable std::vector<std::vector<int64_t>> selections;
};
}  // namespace

class PredicateTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}
    struct FieldStats {
        FieldStats(const std::optional<int64_t>& _min_value,
                   const std::optional<int64_t>& _max_value, int64_t _null_count)
            : min_value(_min_value), max_value(_max_value), null_count(_null_count) {}
        std::optional<int64_t> min_value;
        std::optional<int64_t> max_value;
        int64_t null_count;
    };

    bool StatsCheck(const PredicateFilter& predicate, int64_t row_count,
                    const std::vector<FieldStats>& field_stats) const {
        auto pool = GetDefaultPool();
        BinaryRow min_row(/*arity=*/field_stats.size());
        BinaryRowWriter min_row_writer(&min_row, 0, pool.get());
        BinaryRow max_row(/*arity=*/field_stats.size());
        BinaryRowWriter max_row_writer(&max_row, 0, pool.get());
        std::vector<int64_t> nulls;
        arrow::FieldVector fields;
        for (uint32_t i = 0; i < field_stats.size(); i++) {
            const auto& stats = field_stats[i];
            if (stats.min_value == std::nullopt) {
                min_row_writer.SetNullAt(i);
            } else {
                min_row_writer.WriteLong(i, stats.min_value.value());
            }
            if (stats.max_value == std::nullopt) {
                max_row_writer.SetNullAt(i);
            } else {
                max_row_writer.WriteLong(i, stats.max_value.value());
            }
            nulls.emplace_back(stats.null_count);
            fields.emplace_back(arrow::field("f" + std::to_string(i), arrow::int64()));
        }
        min_row_writer.Complete();
        max_row_writer.Complete();
        auto null_counts = BinaryArray::FromLongArray(nulls, pool.get());
        auto arrow_schema = arrow::schema(fields);
        EXPECT_OK_AND_ASSIGN(
            auto ret, predicate.Test(arrow_schema, row_count, min_row, max_row, null_counts));
        return ret;
    }

    struct StringFieldStats {
        StringFieldStats(const std::optional<std::string>& _min_value,
                         const std::optional<std::string>& _max_value, int64_t _null_count)
            : min_value(_min_value), max_value(_max_value), null_count(_null_count) {}
        std::optional<std::string> min_value;
        std::optional<std::string> max_value;
        int64_t null_count;
    };

    bool StringStatsCheck(const PredicateFilter& predicate, int64_t row_count,
                          const std::vector<StringFieldStats>& field_stats) const {
        auto pool = GetDefaultPool();
        BinaryRow min_row(/*arity=*/field_stats.size());
        BinaryRowWriter min_row_writer(&min_row, 0, pool.get());
        BinaryRow max_row(/*arity=*/field_stats.size());
        BinaryRowWriter max_row_writer(&max_row, 0, pool.get());
        std::vector<int64_t> nulls;
        arrow::FieldVector fields;
        for (uint32_t i = 0; i < field_stats.size(); i++) {
            const auto& stats = field_stats[i];
            if (stats.min_value == std::nullopt) {
                min_row_writer.SetNullAt(i);
            } else {
                min_row_writer.WriteString(
                    i, BinaryString::FromString(stats.min_value.value(), pool.get()));
            }
            if (stats.max_value == std::nullopt) {
                max_row_writer.SetNullAt(i);
            } else {
                max_row_writer.WriteString(
                    i, BinaryString::FromString(stats.max_value.value(), pool.get()));
            }
            nulls.emplace_back(stats.null_count);
            fields.emplace_back(arrow::field("f" + std::to_string(i), arrow::utf8()));
        }
        min_row_writer.Complete();
        max_row_writer.Complete();
        auto null_counts = BinaryArray::FromLongArray(nulls, pool.get());
        auto arrow_schema = arrow::schema(fields);
        EXPECT_OK_AND_ASSIGN(
            auto ret, predicate.Test(arrow_schema, row_count, min_row, max_row, null_counts));
        return ret;
    }

    BinaryRow CreateBigIntRow(const std::vector<std::optional<int64_t>>& value) const {
        auto pool = GetDefaultPool();
        BinaryRow row(/*arity=*/value.size());
        BinaryRowWriter row_writer(&row, 0, pool.get());
        for (size_t i = 0; i < value.size(); ++i) {
            if (value[i] == std::nullopt) {
                row_writer.SetNullAt(i);
            } else {
                row_writer.WriteLong(i, value[i].value());
            }
        }
        row_writer.Complete();
        return row;
    }

    BinaryRow CreateStringRow(const std::vector<std::optional<std::string>>& value) const {
        auto pool = GetDefaultPool();
        BinaryRow row(/*arity=*/value.size());
        BinaryRowWriter row_writer(&row, 0, pool.get());
        for (size_t i = 0; i < value.size(); ++i) {
            if (value[i] == std::nullopt) {
                row_writer.SetNullAt(i);
            } else {
                row_writer.WriteString(i, BinaryString::FromString(value[i].value(), pool.get()));
            }
        }
        row_writer.Complete();
        return row;
    }
};

TEST_F(PredicateTest, TestInvalidFieldIndex) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f0",
                                                  FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);

    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});
    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    // with array
    ASSERT_NOK_WITH_MSG(predicate->Test(*struct_array, arrow::default_memory_pool()),
                        "field index 2 exceed field count 2 in struct array");

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_NOK_WITH_MSG(predicate->Test(arrow_schema, CreateBigIntRow({4})),
                        "field index 2 exceed field count 1 in row");
}

TEST_F(PredicateTest, TestEqual) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                  FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                          Literal(5l)));

    ASSERT_FALSE(*predicate->Negate() ==
                 *PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                             FieldType::BIGINT, Literal(10l)));
    ASSERT_FALSE(*predicate->Negate() == *PredicateBuilder::Equal(/*field_index=*/0,
                                                                  /*field_name=*/"f0",
                                                                  FieldType::BIGINT, Literal(10l)));
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 6ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestEqualNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                  FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestNotEqual) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                     FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 0}));

    auto predicate_negate = std::dynamic_pointer_cast<PredicateFilter>(predicate->Negate());
    ASSERT_EQ(*predicate_negate, *PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::BIGINT, Literal(5l)));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    ASSERT_TRUE(predicate_negate->Test(arrow_schema, CreateBigIntRow({5})).value());

    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 6ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(5ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestNotEqualNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                     FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestGreater) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, 6, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 1, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::LessOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                             FieldType::BIGINT, Literal(5l)));
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({6})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 4ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 6ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestGreaterNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::GreaterThan(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 4ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestGreaterOrEqual) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    ASSERT_EQ(predicate->GetFunction().ToString(), "GreaterOrEqual");
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, 6, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1, 1, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                          Literal(5l)));
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({6})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 4ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 6ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestGreaterOrEqualNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 4ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLess) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0",
                                                     FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, 6, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 0, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::GreaterOrEqual(
                  /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(5l)));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({6})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(5ll, 7ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(4ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLessNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0",
                                                     FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLessOrEqual) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::LessOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::BIGINT, Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 5, 6, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({1, 1, 0, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                             FieldType::BIGINT, Literal(5l)));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({6})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(5ll, 7ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(4ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLessOrEqualNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::LessOrEqual(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, Literal(FieldType::BIGINT));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestIsNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base =
        PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1}));

    ASSERT_EQ(*predicate->Negate(), *PredicateBuilder::IsNotNull(
                                        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(5ll, 7ll, 1ll)}));
}

TEST_F(PredicateTest, TestIsNotNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base =
        PredicateBuilder::IsNotNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(5ll, 7ll, 1ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(std::nullopt, std::nullopt, 3ll)}));
}

TEST_F(PredicateTest, TestIn) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0",
                                               FieldType::BIGINT, {Literal(1l), Literal(3l)});
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 1, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                       {Literal(1l), Literal(3l)}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestInNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::In(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
        {Literal(1l), Literal(FieldType::BIGINT), Literal(3l)});
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 1, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestNotIn) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0",
                                                  FieldType::BIGINT, {Literal(1l), Literal(3l)});
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1, 0, 0}));

    ASSERT_EQ(*predicate->Negate(),
              *PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                    {Literal(1l), Literal(3l)}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 1ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(3ll, 3ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 3ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestNotInNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::NotIn(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
        {Literal(1l), Literal(FieldType::BIGINT), Literal(3l)});
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 1ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(3ll, 3ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 3ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLargeIn) {
    auto bigint_type = arrow::int64();
    std::vector<Literal> literals;
    literals.reserve(30);
    literals.emplace_back(1l);
    literals.emplace_back(3l);
    for (int64_t i = 10; i < 30; i++) {
        literals.emplace_back(i);
    }
    auto predicate_base =
        PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, literals);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 1, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(29ll, 32ll, 0ll)}));
}

TEST_F(PredicateTest, TestLargeInNull) {
    auto bigint_type = arrow::int64();
    std::vector<Literal> literals;
    literals.reserve(30);
    literals.emplace_back(1l);
    literals.emplace_back(FieldType::BIGINT);
    literals.emplace_back(3l);
    for (int64_t i = 10; i < 30; i++) {
        literals.emplace_back(i);
    }
    auto predicate_base =
        PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, literals);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 1, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(29ll, 32ll, 0ll)}));
}

TEST_F(PredicateTest, TestLargeNotIn) {
    auto bigint_type = arrow::int64();
    std::vector<Literal> literals;
    literals.reserve(30);
    literals.emplace_back(1l);
    literals.emplace_back(3l);
    for (int64_t i = 10; i < 30; i++) {
        literals.emplace_back(i);
    }
    auto predicate_base = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0",
                                                  FieldType::BIGINT, literals);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1, 0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 1ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(3ll, 3ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 3ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(29ll, 32ll, 0ll)}));
}

TEST_F(PredicateTest, TestLargeNotInNull) {
    auto bigint_type = arrow::int64();
    std::vector<Literal> literals;
    literals.reserve(30);
    literals.emplace_back(1l);
    literals.emplace_back(FieldType::BIGINT);
    literals.emplace_back(3l);
    for (int64_t i = 10; i < 30; i++) {
        literals.emplace_back(i);
    }
    auto predicate_base = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0",
                                                  FieldType::BIGINT, literals);
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({2})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({3})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 1ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(3ll, 3ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 3ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(0ll, 5ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(29ll, 32ll, 0ll)}));
}

TEST_F(PredicateTest, TestLargeStringIn) {
    auto string_type = arrow::utf8();
    std::vector<Literal> literals;
    literals.reserve(1000);
    for (int32_t i = 0; i < 1000; i++) {
        std::string value = fmt::format("key-{}", i);
        literals.emplace_back(FieldType::STRING, value.data(), value.size());
    }
    auto in_base =
        PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING, literals);
    auto in_predicate = std::dynamic_pointer_cast<PredicateFilter>(in_base);
    ASSERT_TRUE(in_predicate);
    auto not_in_base = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0",
                                               FieldType::STRING, literals);
    auto not_in_predicate = std::dynamic_pointer_cast<PredicateFilter>(not_in_base);
    ASSERT_TRUE(not_in_predicate);

    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(
                  string_type, R"(["key-0", "key-999", "key-1000", "other", "", null])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type = arrow::struct_({arrow::field("f0", string_type)});
    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto in_valid,
                         in_predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(in_valid, std::vector<char>({1, 1, 0, 0, 0, 0}));
    ASSERT_OK_AND_ASSIGN(auto not_in_valid,
                         not_in_predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(not_in_valid, std::vector<char>({0, 0, 1, 1, 1, 0}));
}

TEST_F(PredicateTest, TestInAfterRebind) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0",
                                               FieldType::BIGINT, {Literal(1l), Literal(3l)});
    auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicateImpl>(predicate_base);
    ASSERT_TRUE(leaf_predicate);

    // Rebinding builds a new predicate around the same literals, results must stay identical.
    auto renamed = leaf_predicate->NewLeafPredicate(/*new_field_name=*/"f1");
    ASSERT_EQ(renamed->FieldName(), "f1");
    auto rebound = renamed->NewLeafPredicate(/*new_field_index=*/1);
    ASSERT_EQ(rebound->FieldIndex(), 1);

    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 2, 1, 0])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, null])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});
    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid, rebound->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({1, 0, 1, 0}));

    auto arrow_schema = arrow::schema(
        arrow::FieldVector({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)}));
    ASSERT_TRUE(rebound->Test(arrow_schema, CreateBigIntRow({0, 1})).value());
    ASSERT_FALSE(rebound->Test(arrow_schema, CreateBigIntRow({1, 2})).value());
}

TEST_F(PredicateTest, TestInt64BoundaryIn) {
    // The value set carries the whole int64 range, the boundary values included.
    std::vector<Literal> literals = {Literal(std::numeric_limits<int64_t>::min()),
                                     Literal(std::numeric_limits<int64_t>::max())};
    auto in_base =
        PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT, literals);
    auto in_predicate = std::dynamic_pointer_cast<PredicateFilter>(in_base);
    ASSERT_TRUE(in_predicate);
    auto not_in_base = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0",
                                               FieldType::BIGINT, literals);
    auto not_in_predicate = std::dynamic_pointer_cast<PredicateFilter>(not_in_base);
    ASSERT_TRUE(not_in_predicate);

    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(
                  arrow::int64(), R"([-9223372036854775808, 0, 9223372036854775807, null])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", arrow::int64())});
    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto in_valid,
                         in_predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(in_valid, std::vector<char>({1, 0, 1, 0}));
    ASSERT_OK_AND_ASSIGN(auto not_in_valid,
                         not_in_predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(not_in_valid, std::vector<char>({0, 1, 0, 0}));
}

TEST_F(PredicateTest, TestAnd) {
    auto bigint_type = arrow::int64();
    ASSERT_OK_AND_ASSIGN(
        auto predicate_base,
        PredicateBuilder::And({PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                       FieldType::BIGINT, Literal(3l)),
                               PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::BIGINT, Literal(5l))}));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 3, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([5, 6, 5, 5])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 1, 0}));

    ASSERT_OK_AND_ASSIGN(
        auto negate_predicate,
        PredicateBuilder::Or({PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                         FieldType::BIGINT, Literal(3l)),
                              PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                         FieldType::BIGINT, Literal(5l))}));
    ASSERT_EQ(*predicate->Negate(), *negate_predicate);

    // with internal row
    auto arrow_schema = arrow::schema(
        arrow::FieldVector({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4, 5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({3, 6})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3, 5})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt, 5})).value());
    // with stats
    ASSERT_TRUE(
        StatsCheck(*predicate, 3ll, {FieldStats(3ll, 6ll, 0ll), FieldStats(4ll, 6ll, 0ll)}));
    ASSERT_FALSE(
        StatsCheck(*predicate, 3ll, {FieldStats(3ll, 6ll, 0ll), FieldStats(6ll, 8ll, 0ll)}));
    ASSERT_FALSE(
        StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll), FieldStats(4ll, 6ll, 0ll)}));
}

TEST_F(PredicateTest, TestOr) {
    auto bigint_type = arrow::int64();
    ASSERT_OK_AND_ASSIGN(
        auto predicate_base,
        PredicateBuilder::Or({PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                      FieldType::BIGINT, Literal(3l)),
                              PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                      FieldType::BIGINT, Literal(5l))}));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, 3, 3, null])").ValueOrDie();
    auto f1 =
        arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([6, 6, 5, 5])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 1, 1, 1}));

    ASSERT_OK_AND_ASSIGN(
        auto negate_predicate,
        PredicateBuilder::And({PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::BIGINT, Literal(3l)),
                               PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                          FieldType::BIGINT, Literal(5l))}));
    ASSERT_EQ(*predicate->Negate(), *negate_predicate);

    // with internal row
    auto arrow_schema = arrow::schema(
        arrow::FieldVector({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4, 6})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3, 6})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({3, 5})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt, 5})).value());
    // with stats
    ASSERT_TRUE(
        StatsCheck(*predicate, 3ll, {FieldStats(3ll, 6ll, 0ll), FieldStats(4ll, 6ll, 0ll)}));
    ASSERT_TRUE(
        StatsCheck(*predicate, 3ll, {FieldStats(3ll, 6ll, 0ll), FieldStats(6ll, 8ll, 0ll)}));
    ASSERT_FALSE(
        StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll), FieldStats(8ll, 10ll, 0ll)}));
}

TEST_F(PredicateTest, TestNestedCandidateSelectionWithSliceAndNulls) {
    for (const auto& type : {arrow::utf8(), arrow::binary()}) {
        const FieldType field_type =
            type->id() == arrow::Type::STRING ? FieldType::STRING : FieldType::BINARY;
        auto keys = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[9,1,0,1,1,null,9]")
                        .ValueOrDie();
        auto values =
            arrow::ipc::internal::json::ArrayFromJSON(type, R"(["pad","a","b","b",null,"a","pad"])")
                .ValueOrDie();
        auto batch =
            arrow::StructArray::Make({keys, values}, std::vector<std::string>{"key", "value"})
                .ValueOrDie();
        auto sliced = batch->Slice(1, 5);
        auto key = std::make_shared<RecordingEqualPredicate>(0, "key", FieldType::BIGINT,
                                                             Literal(int64_t{1}));
        auto a = std::make_shared<RecordingEqualPredicate>(1, "value", field_type,
                                                           Literal(field_type, "a", 1));
        auto b = std::make_shared<RecordingEqualPredicate>(1, "value", field_type,
                                                           Literal(field_type, "b", 1));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> alternatives, PredicateBuilder::Or({a, b}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                             PredicateBuilder::And({key, alternatives}));
        auto filter = std::dynamic_pointer_cast<PredicateFilter>(predicate);
        arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
        ASSERT_OK_AND_ASSIGN(std::vector<char> actual, filter->Test(*sliced, &pool));
        ASSERT_EQ(actual, std::vector<char>({1, 0, 1, 0, 0}));
        ASSERT_EQ(key->selections, std::vector<std::vector<int64_t>>({{0, 1, 2, 3, 4}}));
        ASSERT_EQ(a->selections, std::vector<std::vector<int64_t>>({{0, 2, 3}}));
        ASSERT_EQ(b->selections, std::vector<std::vector<int64_t>>({{2, 3}}));
        ASSERT_GT(pool.max_memory(), 0);
        ASSERT_EQ(pool.bytes_allocated(), 0);

        ASSERT_OK_AND_ASSIGN(std::vector<char> selected,
                             filter->TestSelected(*sliced, {2, 4}, &pool));
        ASSERT_EQ(selected, std::vector<char>({1, 0}));
        ASSERT_OK_AND_ASSIGN(std::vector<char> empty, filter->Test(*sliced->Slice(0, 0), &pool));
        ASSERT_TRUE(empty.empty());
    }
}

TEST_F(PredicateTest, TestLeafFieldBoundsOnWideSlicedBatch) {
    auto values =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[9,1,null,2,9]").ValueOrDie();
    arrow::ArrayVector columns(64, values);
    std::vector<std::string> names;
    for (int32_t i = 0; i < 64; ++i) {
        names.push_back(fmt::format("field_{}", i));
    }
    auto batch = arrow::StructArray::Make(columns, names).ValueOrDie();
    auto sliced = batch->Slice(1, 3);
    auto pool = arrow::default_memory_pool();
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(
        PredicateBuilder::Equal(63, "field_63", FieldType::BIGINT, Literal(int64_t{2})));
    ASSERT_OK_AND_ASSIGN(auto full, predicate->Test(*sliced, pool));
    ASSERT_EQ(full, std::vector<char>({0, 0, 1}));
    ASSERT_OK_AND_ASSIGN(auto selected, predicate->TestSelected(*sliced, {1, 2}, pool));
    ASSERT_EQ(selected, std::vector<char>({0, 1}));
    auto invalid = std::dynamic_pointer_cast<PredicateFilter>(
        PredicateBuilder::Equal(64, "missing", FieldType::BIGINT, Literal(int64_t{2})));
    ASSERT_NOK_WITH_MSG(invalid->Test(*sliced, pool), "field index 64 exceed field count 64");
    ASSERT_NOK_WITH_MSG(invalid->TestSelected(*sliced, {}, pool),
                        "field index 64 exceed field count 64");
}

TEST_F(PredicateTest, TestEmptyCandidatesStillValidateFields) {
    auto keys = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[1,2]").ValueOrDie();
    auto batch = arrow::StructArray::Make({keys}, std::vector<std::string>{"key"}).ValueOrDie();
    auto invalid = PredicateBuilder::Equal(1, "missing", FieldType::BIGINT, Literal(int64_t{1}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> conjunction,
        PredicateBuilder::And(
            {PredicateBuilder::Equal(0, "key", FieldType::BIGINT, Literal(int64_t{9})), invalid}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> disjunction,
        PredicateBuilder::Or({PredicateBuilder::IsNotNull(0, "key", FieldType::BIGINT), invalid}));
    for (const auto& predicate : {conjunction, disjunction}) {
        auto filter = std::dynamic_pointer_cast<PredicateFilter>(predicate);
        ASSERT_NOK_WITH_MSG(filter->Test(*batch, arrow::default_memory_pool()), "field index");
    }
}

TEST_F(PredicateTest, TestCandidateSelectionPreservesEvaluationErrors) {
    auto keys = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[1,2]").ValueOrDie();
    auto unsupported =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::uint64(), "[1,2]").ValueOrDie();
    auto strings =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a","b"])").ValueOrDie();
    auto batch = arrow::StructArray::Make({keys, unsupported, strings},
                                          std::vector<std::string>{"key", "unsupported", "text"})
                     .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> invalid_like,
        PredicateBuilder::Like(2, "text", FieldType::STRING, Literal(FieldType::STRING, "\\q", 2)));
    std::vector<std::shared_ptr<Predicate>> invalid_predicates = {
        std::make_shared<LeafPredicateImpl>(Equal::Instance(), 0, "key", FieldType::BIGINT,
                                            std::vector<Literal>{}),
        PredicateBuilder::Equal(1, "unsupported", FieldType::BIGINT, Literal(int64_t{1})),
        PredicateBuilder::Equal(0, "key", FieldType::BIGINT,
                                Literal(FieldType::STRING, "wrong-type", 10)),
        invalid_like};
    for (const auto& invalid : invalid_predicates) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<Predicate> conjunction,
            PredicateBuilder::And(
                {PredicateBuilder::Equal(0, "key", FieldType::BIGINT, Literal(int64_t{9})),
                 invalid}));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<Predicate> disjunction,
            PredicateBuilder::Or(
                {PredicateBuilder::IsNotNull(0, "key", FieldType::BIGINT), invalid}));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<Predicate> partial,
            PredicateBuilder::And(
                {PredicateBuilder::Equal(0, "key", FieldType::BIGINT, Literal(int64_t{2})),
                 invalid}));
        for (const auto& predicate : {conjunction, disjunction, partial}) {
            auto filter = std::dynamic_pointer_cast<PredicateFilter>(predicate);
            auto expected = TestEager(predicate, *batch, arrow::default_memory_pool());
            auto actual = filter->Test(*batch, arrow::default_memory_pool());
            ASSERT_FALSE(expected.ok());
            ASSERT_FALSE(actual.ok());
            ASSERT_EQ(actual.status().ToString(), expected.status().ToString());
        }
    }
}

TEST_F(PredicateTest, TestCandidateSelectionDictionaryColumn) {
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0,1,null,0,1]").ValueOrDie();
    auto dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a","b"])").ValueOrDie();
    auto values = arrow::DictionaryArray::FromArrays(indices, dictionary).ValueOrDie();
    auto batch = arrow::StructArray::Make({values}, std::vector<std::string>{"value"}).ValueOrDie();
    auto a = std::make_shared<RecordingEqualPredicate>(0, "value", FieldType::STRING,
                                                       Literal(FieldType::STRING, "a", 1));
    auto b = std::make_shared<RecordingEqualPredicate>(0, "value", FieldType::STRING,
                                                       Literal(FieldType::STRING, "b", 1));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate, PredicateBuilder::Or({a, b}));
    auto filter = std::dynamic_pointer_cast<PredicateFilter>(predicate);
    ASSERT_OK_AND_ASSIGN(std::vector<char> actual,
                         filter->Test(*batch, arrow::default_memory_pool()));
    ASSERT_EQ(actual, std::vector<char>({1, 1, 0, 1, 1}));
    ASSERT_EQ(b->selections, std::vector<std::vector<int64_t>>({{1, 2, 4}}));
}

TEST_F(PredicateTest, TestCandidateSelectionMatchesEagerEvaluation) {
    auto keys = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[1,2,null,1,2,1,3,null]")
                    .ValueOrDie();
    auto values = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::utf8(), R"(["a","b",null,"b","c",null,"a","c"])")
                      .ValueOrDie();
    auto batch = arrow::StructArray::Make({keys, values}, std::vector<std::string>{"key", "value"})
                     .ValueOrDie();
    for (int64_t key = 0; key <= 3; ++key) {
        auto key_pred = PredicateBuilder::Equal(0, "key", FieldType::BIGINT, Literal(key));
        auto a = PredicateBuilder::Equal(1, "value", FieldType::STRING,
                                         Literal(FieldType::STRING, "a", 1));
        auto b = PredicateBuilder::Equal(1, "value", FieldType::STRING,
                                         Literal(FieldType::STRING, "b", 1));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> inner_or, PredicateBuilder::Or({a, b}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> inner_and,
                             PredicateBuilder::And({key_pred, a}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> root_and,
                             PredicateBuilder::And({key_pred, inner_or}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> root_or,
                             PredicateBuilder::Or({inner_and, b}));
        for (const auto& predicate : {root_and, root_or}) {
            auto filter = std::dynamic_pointer_cast<PredicateFilter>(predicate);
            for (int64_t offset = 0; offset <= batch->length(); ++offset) {
                auto slice = batch->Slice(offset);
                ASSERT_OK_AND_ASSIGN(std::vector<char> expected,
                                     TestEager(predicate, *slice, arrow::default_memory_pool()));
                ASSERT_OK_AND_ASSIGN(std::vector<char> actual,
                                     filter->Test(*slice, arrow::default_memory_pool()));
                ASSERT_EQ(actual, expected);
            }
        }
    }
}

TEST_F(PredicateTest, TestTemporalAndDecimalCandidateSelection) {
    const std::vector<std::pair<std::shared_ptr<arrow::DataType>, std::string>> cases = {
        {arrow::timestamp(arrow::TimeUnit::SECOND), "[-1001,null,0,1001,2002]"},
        {arrow::timestamp(arrow::TimeUnit::MILLI), "[-1001,null,0,1001,2002]"},
        {arrow::timestamp(arrow::TimeUnit::MICRO), "[-1001,null,0,1001,2002]"},
        {arrow::timestamp(arrow::TimeUnit::NANO), "[-1001,null,0,1001,2002]"},
        {arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), "[-1001,null,0,1001,2002]"},
        {arrow::decimal128(10, 2), R"(["-1.25",null,"0.00","1.25","2.50"])"},
        {arrow::decimal128(15, 5), R"(["-1.25001",null,"0.00000","1.25001","2.50002"])"}};
    auto pool = arrow::default_memory_pool();
    for (const auto& [type, json] : cases) {
        SCOPED_TRACE(type->ToString());
        auto values = arrow::ipc::internal::json::ArrayFromJSON(type, json).ValueOrDie();
        auto keys =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[1,0,1,0,1]").ValueOrDie();
        auto batch =
            arrow::StructArray::Make({keys, values}, std::vector<std::string>{"key", "value"})
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto literals,
                             LiteralConverter::ConvertLiteralsFromArray(*values, false));
        const FieldType field_type = literals[0].GetType();
        // Observe the batch received by the leaf function: equal results alone would also
        // pass with the old eager fallback and would not protect candidate-only evaluation.
        RecordingBatchEqual recording;
        LeafPredicateImpl recorded(recording, 1, "value", field_type, {literals[3]});
        auto slice = batch->Slice(1, 4);
        ASSERT_OK_AND_ASSIGN(auto selected, recorded.TestSelected(*slice, {0, 2}, pool));
        ASSERT_EQ(selected, std::vector<char>({0, 1}));
        ASSERT_OK_AND_ASSIGN(auto empty, recorded.TestSelected(*slice, {}, pool));
        ASSERT_TRUE(empty.empty());
        ASSERT_EQ(recording.batch_lengths, std::vector<int64_t>({2, 0}));

        // Include a literal whose timestamp precision or decimal scale differs from the column.
        literals.push_back(field_type == FieldType::TIMESTAMP ? Literal(Timestamp(1, 123456))
                                                              : Literal(Decimal(12, 3, 1251)));
        for (const Literal& literal : literals) {
            const std::vector<std::shared_ptr<Predicate>> leaves = {
                PredicateBuilder::Equal(1, "value", field_type, literal),
                PredicateBuilder::LessThan(1, "value", field_type, literal),
                PredicateBuilder::In(1, "value", field_type, {literal, literals[3]}),
                PredicateBuilder::NotIn(1, "value", field_type, {literal, literals[3]})};
            for (const auto& leaf : leaves) {
                SCOPED_TRACE(leaf->ToString());
                auto key =
                    PredicateBuilder::Equal(0, "key", FieldType::BIGINT, Literal(int64_t{1}));
                ASSERT_OK_AND_ASSIGN(auto conjunction, PredicateBuilder::And({key, leaf}));
                ASSERT_OK_AND_ASSIGN(auto disjunction, PredicateBuilder::Or({key, leaf}));
                for (const auto& predicate : {conjunction, disjunction}) {
                    auto filter = std::dynamic_pointer_cast<PredicateFilter>(predicate);
                    for (int64_t offset = 0; offset <= batch->length(); ++offset) {
                        auto input = batch->Slice(offset);
                        ASSERT_OK_AND_ASSIGN(auto expected, TestEager(predicate, *input, pool));
                        ASSERT_OK_AND_ASSIGN(auto actual, filter->Test(*input, pool));
                        ASSERT_EQ(actual, expected);
                        for (const std::vector<int64_t>& selection :
                             {std::vector<int64_t>{}, input->length() > 1
                                                          ? std::vector<int64_t>{1}
                                                          : std::vector<int64_t>{}}) {
                            ASSERT_OK_AND_ASSIGN(auto subset,
                                                 filter->TestSelected(*input, selection, pool));
                            std::vector<char> expected_subset;
                            for (int64_t row : selection) {
                                expected_subset.push_back(expected[row]);
                            }
                            ASSERT_EQ(subset, expected_subset);
                        }
                    }
                }
            }
        }
    }
}

TEST_F(PredicateTest, TestBetween) {
    auto bigint_type = arrow::int64();
    auto predicate_base = PredicateBuilder::Between(/*field_index=*/0, /*field_name=*/"f0",
                                                    FieldType::BIGINT, Literal(3l), Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([3, 4, 5, 100, 1, null])")
                  .ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2, 3, 4, 5, 6])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({1, 1, 1, 0, 0, 0}));

    auto less_than = PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"f0",
                                                FieldType::BIGINT, Literal(3l));
    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                      FieldType::BIGINT, Literal(5l));
    ASSERT_OK_AND_ASSIGN(auto or_predicate, PredicateBuilder::Or({less_than, greater_than}));

    auto predicate_negate = std::dynamic_pointer_cast<PredicateFilter>(predicate->Negate());
    ASSERT_EQ(*predicate_negate, *or_predicate);
    ASSERT_FALSE(*predicate_negate == *predicate_base);

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({1})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());
    ASSERT_TRUE(predicate_negate->Test(arrow_schema, CreateBigIntRow({1})).value());

    // with stats
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 10ll, 0ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(3ll, 4ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(6ll, 7ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
    ASSERT_TRUE(StatsCheck(*predicate, 3ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestBetweenNull) {
    auto bigint_type = arrow::int64();
    auto predicate_base =
        PredicateBuilder::Between(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT,
                                  Literal(FieldType::BIGINT), Literal(5l));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([4, null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(bigint_type, R"([1, 2])").ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", bigint_type), arrow::field("f1", bigint_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", bigint_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({4})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateBigIntRow({std::nullopt})).value());

    // with stats
    ASSERT_FALSE(StatsCheck(*predicate, 3ll, {FieldStats(1ll, 10ll, 0ll)}));
    ASSERT_FALSE(StatsCheck(*predicate, 1ll, {FieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestStartsWith) {
    auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::StartsWith(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                     Literal(FieldType::STRING, "aab", 3)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(string_type,
                                                        R"(["ccddee", "bbccdd", "aabbcc", null])")
                  .ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                  string_type, R"(["gghhii", "ffgghh", "eeffgg", "ddeeff"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 1, 0}));

    ASSERT_EQ(predicate->Negate(), nullptr);
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"ccddee"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"bbccdd"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"aabbcc"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats
    // min="aaa", max="aaz" covers prefix "aab"
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "aaz", 0ll)}));
    // min="aab", max="aab" exact match prefix
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aab", "aab", 0ll)}));
    // min="aabxxx", max="aabzzz" both start with "aab"
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aabxxx", "aabzzz", 0ll)}));
    // min="bbb", max="ccc" entirely above prefix "aab"
    ASSERT_FALSE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("bbb", "ccc", 0ll)}));
    // min="aaa", max="aaa" entirely below prefix "aab"
    ASSERT_FALSE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "aaa", 0ll)}));
    // all nulls
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestStartsWithNull) {
    const auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::StartsWith(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING, Literal(FieldType::STRING)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["bbccdd", null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["ffgghh", "ccddee"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"bbccdd"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats: null literal always returns false
    ASSERT_FALSE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestEndsWith) {
    auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::EndsWith(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "bcc", 3)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(string_type,
                                                        R"(["ccddee", "bbccdd", "aabbcc", null])")
                  .ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                  string_type, R"(["gghhii", "ffgghh", "eeffgg", "ddeeff"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 1, 0}));

    ASSERT_EQ(predicate->Negate(), nullptr);
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"ccddee"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"bbccdd"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"aabbcc"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats: EndsWith base class always returns true for non-null stats
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("xxx", "yyy", 0ll)}));
    // all nulls
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestEndsWithNull) {
    const auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::EndsWith(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING, Literal(FieldType::STRING)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["bbccdd", null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["ffgghh", "ccddee"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"bbccdd"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats: null literal always returns false
    ASSERT_FALSE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestContains) {
    auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::Contains(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "cde", 3)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 = arrow::ipc::internal::json::ArrayFromJSON(string_type,
                                                        R"(["ghijkl", "defghi", "abcdef", null])")
                  .ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(
                  string_type, R"(["stuvwx", "pqrstu", "mnopqr", "jklmno"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0, 1, 0}));

    ASSERT_EQ(predicate->Negate(), nullptr);
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"ghijkl"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"defghi"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"abcdef"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats: Contains base class always returns true for non-null stats
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("xxx", "yyy", 0ll)}));
    // all nulls
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestContainsNull) {
    const auto string_type = arrow::utf8();
    ASSERT_OK_AND_ASSIGN(
        const auto predicate_base,
        PredicateBuilder::Contains(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING, Literal(FieldType::STRING)));
    const auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    auto f0 =
        arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["defghi", null])").ValueOrDie();
    auto f1 = arrow::ipc::internal::json::ArrayFromJSON(string_type, R"(["pqrstu", "jklmno"])")
                  .ValueOrDie();
    std::shared_ptr<arrow::DataType> src_type =
        arrow::struct_({arrow::field("f0", string_type), arrow::field("f1", string_type)});

    std::shared_ptr<arrow::Array> struct_array =
        arrow::StructArray::Make({f0, f1}, src_type->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto is_valid,
                         predicate->Test(*struct_array, arrow::default_memory_pool()));
    ASSERT_EQ(is_valid, std::vector<char>({0, 0}));

    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", string_type)}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"defghi"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({std::nullopt})).value());

    // with stats: null literal always returns false
    ASSERT_FALSE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLike) {
    ASSERT_OK_AND_ASSIGN(auto predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a.c", 3)));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate);
    ASSERT_EQ(predicate->Negate(), nullptr);
    // with internal row
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"abc"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"a.c"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a.*d", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"abcd"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "%c.e", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"abcde"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a\\_c", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"a-c"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"a_c"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "start%", 6)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"startX"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"not_startX"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "%middle%", 8)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"xxmiddleyy"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"xxmidxdleyy"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "%end", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"xxend"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"xxendyy"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "equal", 5)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"equal"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"equalxx"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "st_rt%", 6)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"startxx"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"stbrtxx"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"xxstbrtxx"})).value());

    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "abc%def%", 8)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"abchahadefxx"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"abchahadafxx"})).value());

    // with stats: Like base class always returns true for non-null stats
    ASSERT_TRUE(StringStatsCheck(*predicate, 3ll, {StringFieldStats("aaa", "zzz", 0ll)}));
    // all nulls
    ASSERT_FALSE(
        StringStatsCheck(*predicate, 1ll, {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestLikeConsecutivePercent) {
    // consecutive '%' should be merged into one '%'
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));

    // "%%" should behave the same as "%"
    ASSERT_OK_AND_ASSIGN(auto predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "%%", 2)));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"anything"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({""})).value());

    // "a%%%b" should behave the same as "a%b"
    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a%%%b", 5)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"axyzb"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"ab"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"axyzc"})).value());
}

TEST_F(PredicateTest, TestLikeEmptyField) {
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));

    // empty field should match "%"
    ASSERT_OK_AND_ASSIGN(auto predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "%", 1)));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({""})).value());

    // empty field should NOT match "_"
    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "_", 1)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({""})).value());

    // empty field should NOT match a fixed pattern "abc"
    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "abc", 3)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({""})).value());
}

TEST_F(PredicateTest, TestLikeMinLenExceedsField) {
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));

    // pattern "abcdef" requires 6 literal chars, field "ab" has only 2
    ASSERT_OK_AND_ASSIGN(auto predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "abcdef", 6)));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"ab"})).value());

    // pattern "a_b_c" requires 3 literal chars, wildcards = min_len 3, field "ab" has 2
    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a_b_c", 5)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"ab"})).value());

    // field length equals min_len should still be possible to match
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"axbxc"})).value());
}

TEST_F(PredicateTest, TestLikeLongPatternHeapAlloc) {
    // pattern length > STACK_LIMIT(128) uses heap allocation
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));

    // Build a pattern with > 128 characters: "a" + 130 * "_" + "z"
    std::string long_pattern = "a";
    for (int i = 0; i < 130; ++i) {
        long_pattern += '_';
    }
    long_pattern += 'z';

    ASSERT_OK_AND_ASSIGN(auto predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, long_pattern.data(), long_pattern.size())));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);

    // Build a matching field: "a" + 130 * "x" + "z"
    std::string matching_field = "a";
    for (int i = 0; i < 130; ++i) {
        matching_field += 'x';
    }
    matching_field += 'z';
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({matching_field})).value());

    // Non-matching: wrong ending
    std::string non_matching_field = "a";
    for (int i = 0; i < 130; ++i) {
        non_matching_field += 'x';
    }
    non_matching_field += 'y';
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({non_matching_field})).value());
}

TEST_F(PredicateTest, TestLikeInvalidEscapeSequence) {
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));

    // Trailing backslash is invalid (Java throws "Invalid escape sequence")
    ASSERT_OK_AND_ASSIGN(auto predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "abc\\", 4)));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_NOK_WITH_MSG(predicate->Test(arrow_schema, CreateStringRow({"abc"})),
                        "Invalid escape sequence");

    // Backslash followed by non-special char is invalid (only \_, \%, \\ are legal)
    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a\\bc", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_NOK_WITH_MSG(predicate->Test(arrow_schema, CreateStringRow({"abc"})),
                        "Invalid escape sequence");

    // \n is not a valid escape
    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a\\nf", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_NOK_WITH_MSG(predicate->Test(arrow_schema, CreateStringRow({"anf"})),
                        "Invalid escape sequence");
}

TEST_F(PredicateTest, TestLikeEscapeBackslash) {
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));

    // \\\\ in C++ string literal = "\\" in the pattern = escaped backslash
    ASSERT_OK_AND_ASSIGN(auto predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a\\\\b", 4)));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    // Field "a\b" should match pattern "a\\b" (escaped backslash)
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"a\\b"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"axb"})).value());

    // Escaped percent: "a\%b" matches literal "a%b"
    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a\\%b", 4)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"a%b"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"axb"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"axxb"})).value());
}

TEST_F(PredicateTest, TestLikeUtf8MultibyteUnderscore) {
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));

    // Single '_' should match one Unicode character, not one byte.
    ASSERT_OK_AND_ASSIGN(auto predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "_", 1)));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"中"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"中文"})).value());

    // "a_c" where _ matches one Chinese character
    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "a_c", 3)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"a中c"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"a中文c"})).value());

    // "___" should match exactly 3 Unicode characters
    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "___", 3)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"中文字"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"中文"})).value());

    // '%' should still work with multi-byte characters
    std::string pattern_contains = std::string("%") + "中" + "%";
    ASSERT_OK_AND_ASSIGN(
        predicate_base,
        PredicateBuilder::Like(
            /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
            Literal(FieldType::STRING, pattern_contains.data(), pattern_contains.size())));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"hello中world"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"helloworld"})).value());
}

TEST_F(PredicateTest, TestLikeJavaRegexLineTerminatorSemantics) {
    auto arrow_schema = arrow::schema(arrow::FieldVector({arrow::field("f0", arrow::utf8())}));

    // Java regex '.' does not match line terminators, so '_' should not match them either.
    ASSERT_OK_AND_ASSIGN(auto predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "_", 1)));
    auto predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"\n"})).value());
    ASSERT_FALSE(predicate->Test(arrow_schema, CreateStringRow({"\r"})).value());

    // Java LIKE '%' uses (?s:.*), so it should still match line terminators.
    ASSERT_OK_AND_ASSIGN(predicate_base,
                         PredicateBuilder::Like(
                             /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                             Literal(FieldType::STRING, "%", 1)));
    predicate = std::dynamic_pointer_cast<PredicateFilter>(predicate_base);
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"\n"})).value());
    ASSERT_TRUE(predicate->Test(arrow_schema, CreateStringRow({"\r"})).value());
}

TEST_F(PredicateTest, TestCompound) {
    ASSERT_OK_AND_ASSIGN(
        const auto startswith_predicate,
        PredicateBuilder::StartsWith(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                     Literal(FieldType::STRING, "aab", 3)));
    ASSERT_OK_AND_ASSIGN(
        const auto endswith_predicate,
        PredicateBuilder::EndsWith(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                   Literal(FieldType::STRING, "bcc", 3)));
    ASSERT_OK_AND_ASSIGN(const auto compound_predicate,
                         PredicateBuilder::And({startswith_predicate, endswith_predicate}));
    ASSERT_NOK_WITH_MSG(
        PredicateBuilder::Not(compound_predicate),
        "Could not construct A NOT predicate from And([StartsWith(f0, aab), EndsWith(f0, bcc)])");

    // with stats: And of StartsWith + EndsWith
    auto compound_filter = std::dynamic_pointer_cast<PredicateFilter>(compound_predicate);
    ASSERT_TRUE(compound_filter);
    // min="aab", max="aaz" covers StartsWith("aab"), EndsWith("bcc") returns true by default
    ASSERT_TRUE(StringStatsCheck(*compound_filter, 3ll, {StringFieldStats("aab", "aaz", 0ll)}));
    // min="bbb", max="ccc" does not cover StartsWith("aab")
    ASSERT_FALSE(StringStatsCheck(*compound_filter, 3ll, {StringFieldStats("bbb", "ccc", 0ll)}));
    // all nulls
    ASSERT_FALSE(StringStatsCheck(*compound_filter, 1ll,
                                  {StringFieldStats(std::nullopt, std::nullopt, 1ll)}));
}

TEST_F(PredicateTest, TestPredicateToString) {
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::BIGINT, Literal(5l));
        ASSERT_EQ(predicate->ToString(), "Equal(f0, 5)");
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                       FieldType::BIGINT, Literal(5l));
        ASSERT_EQ(predicate->ToString(), "GreaterThan(f0, 5)");
    }
    {
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::BIGINT);
        ASSERT_EQ(predicate->ToString(), "IsNotNull(f0)");
    }
    {
        std::vector<Literal> literals;
        literals.reserve(30);
        for (int64_t i = 1; i <= 21; i++) {
            literals.emplace_back(i);
        }
        auto predicate = PredicateBuilder::In(/*field_index=*/0, /*field_name=*/"f0",
                                              FieldType::BIGINT, literals);
        ASSERT_TRUE(predicate);
        ASSERT_EQ(
            predicate->ToString(),
            "In(f0, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21])");
    }
    {
        std::vector<Literal> literals;
        literals.reserve(30);
        for (int64_t i = 1; i <= 21; i++) {
            literals.emplace_back(i);
        }
        auto predicate = PredicateBuilder::NotIn(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::BIGINT, literals);
        ASSERT_TRUE(predicate);
        ASSERT_EQ(predicate->ToString(),
                  "NotIn(f0, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, "
                  "20, 21])");
    }

    {
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::And({PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                           FieldType::BIGINT, Literal(3l)),
                                   PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                           FieldType::BIGINT, Literal(5l))}));
        ASSERT_EQ(predicate->ToString(), "And([Equal(f0, 3), Equal(f1, 5)])");
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto predicate,
            PredicateBuilder::Or({PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::BIGINT, Literal(3l)),
                                  PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                          FieldType::BIGINT, Literal(5l))}));
        ASSERT_EQ(predicate->ToString(), "Or([Equal(f0, 3), Equal(f1, 5)])");
    }
}

TEST_F(PredicateTest, TestTypedEqualBatchMatchesLiteralEvaluation) {
    const std::vector<std::pair<std::shared_ptr<arrow::DataType>, std::string>> cases = {
        {arrow::boolean(), "[true, null, false, true]"},
        {arrow::int8(), "[-128, null, 0, 127]"},
        {arrow::int16(), "[-32768, null, 0, 32767]"},
        {arrow::int32(), "[-2147483648, null, 0, 2147483647]"},
        {arrow::int64(), "[-9223372036854775808, null, 0, 9223372036854775807]"},
        {arrow::date32(), "[-100, null, 0, 100]"},
        {arrow::utf8(), R"(["", null, "a\u0000b", "a\u0000c", "é"])"},
        {arrow::binary(), R"(["", null, "a\u0000b", "a\u0000c", "é"])"},
        {arrow::float64(), "[-0.0, null, 0.0, 1.5]"},
        {arrow::timestamp(arrow::TimeUnit::MICRO), "[-1, null, 0, 1001]"},
        {arrow::decimal128(10, 2), R"(["-1.25", null, "0.00", "1.25"])"}};
    for (const auto& [type, json] : cases) {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(type, json).ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto values,
                             LiteralConverter::ConvertLiteralsFromArray(*array, false));
        CheckEqualBatch(array, {});
        for (const Literal& value : values) {
            CheckEqualBatch(array, {value});
            // The binary function has historically used only the first literal.
            CheckEqualBatch(array, {value, Literal(int64_t{999})});
        }
        CheckEqualBatch(array, {Literal(FieldType::STRING, "mismatch", 8)});
        CheckEqualBatch(array, {Literal(int32_t{123})});
    }
}

TEST_F(PredicateTest, TestTypedEqualFallback) {
    arrow::DoubleBuilder builder;
    ASSERT_TRUE(builder
                    .AppendValues({std::numeric_limits<double>::quiet_NaN(),
                                   std::numeric_limits<double>::infinity(), -0.0, 0.0})
                    .ok());
    auto doubles = builder.Finish().ValueOrDie();
    CheckEqualBatch(doubles, {Literal(std::numeric_limits<double>::quiet_NaN())});
    CheckEqualBatch(doubles, {Literal(-0.0)});
    auto indices = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, null, 2, 0]")
                       .ValueOrDie();
    auto dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", null])")
            .ValueOrDie();
    auto encoded = arrow::DictionaryArray::FromArrays(indices, dictionary).ValueOrDie();
    CheckEqualBatch(encoded, {Literal(FieldType::STRING, "a", 1)});
    auto unsupported =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::uint32(), "[1, null, 2]").ValueOrDie();
    CheckEqualBatch(unsupported, {Literal(int32_t{1})});
    CheckEqualBatch(unsupported, {Literal(FieldType::INT)});
}

TEST_F(PredicateTest, TestTypedEqualBinaryAndBitmapOffsets) {
    arrow::BinaryBuilder builder;
    const std::string bytes("\x00\x80\xff", 3);
    for (int32_t i = 0; i < 19; ++i) {
        if (i % 3 == 0) {
            ASSERT_TRUE(builder.AppendNull().ok());
        } else {
            ASSERT_TRUE(builder.Append(i % 2 == 0 ? bytes : std::string()).ok());
        }
    }
    auto array = builder.Finish().ValueOrDie();
    CheckEqualBatch(array, {Literal(FieldType::BINARY, bytes.data(), bytes.size())});
    CheckEqualBatch(array, {Literal(FieldType::BINARY, "", 0)});
}

TEST_F(PredicateTest, TestBuildAndOr) {
    {
        // literals cannot be empty
        ASSERT_NOK(PredicateBuilder::Or({}));
    }
    {
        // literals cannot be empty
        ASSERT_NOK(PredicateBuilder::And({}));
    }
}
}  // namespace paimon::test
