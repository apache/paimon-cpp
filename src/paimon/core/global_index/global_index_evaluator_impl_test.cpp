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

#include "paimon/core/global_index/global_index_evaluator_impl.h"

#include <cstdint>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
class RecordingGlobalIndexReader : public GlobalIndexReader {
 public:
    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        is_not_null_calls_++;
        return Bitmap({0, 1});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        is_null_calls_++;
        return Bitmap({2});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override {
        equal_calls_++;
        return Bitmap({1, 3});
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(
        const Literal& literal) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override {
        return NotEvaluable();
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override {
        return Status::Invalid("not supported");
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override {
        return Status::Invalid("not supported");
    }

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return "test";
    }

    int32_t EqualCalls() const {
        return equal_calls_;
    }

    int32_t IsNullCalls() const {
        return is_null_calls_;
    }

    int32_t IsNotNullCalls() const {
        return is_not_null_calls_;
    }

 private:
    static std::shared_ptr<GlobalIndexResult> Bitmap(std::initializer_list<int64_t> positions) {
        RoaringBitmap64 bitmap;
        for (int64_t position : positions) {
            bitmap.Add(position);
        }
        return std::make_shared<BitmapGlobalIndexResult>(
            [bitmap = std::move(bitmap)]() -> Result<RoaringBitmap64> { return bitmap; });
    }

    static std::shared_ptr<GlobalIndexResult> NotEvaluable() {
        return nullptr;
    }

    int32_t equal_calls_ = 0;
    int32_t is_null_calls_ = 0;
    int32_t is_not_null_calls_ = 0;
};

std::set<int64_t> CollectPositions(const std::shared_ptr<GlobalIndexResult>& result) {
    std::set<int64_t> positions;
    EXPECT_TRUE(result != nullptr);
    if (result == nullptr) {
        return positions;
    }
    EXPECT_OK_AND_ASSIGN(std::unique_ptr<GlobalIndexResult::Iterator> iterator,
                         result->CreateIterator());
    while (iterator->HasNext()) {
        positions.insert(iterator->Next());
    }
    return positions;
}
}  // namespace

class GlobalIndexEvaluatorImplTest : public ::testing::Test {
 protected:
    void SetUp() override {
        std::vector<DataField> fields = {
            DataField(0, arrow::field("a", arrow::int64())),
            DataField(1, arrow::field("b", arrow::int64())),
        };
        table_schema_ = std::make_shared<TableSchema>(
            /*version=*/1, /*id=*/0, fields, /*highest_field_id=*/1,
            /*partition_keys=*/std::vector<std::string>(),
            /*primary_keys=*/std::vector<std::string>(),
            /*options=*/std::map<std::string, std::string>(), /*comment=*/std::nullopt,
            /*time_millis=*/0);
        reader_ = std::make_shared<RecordingGlobalIndexReader>();
    }

    GlobalIndexEvaluatorImpl CreateEvaluator() const {
        std::shared_ptr<RecordingGlobalIndexReader> reader = reader_;
        return GlobalIndexEvaluatorImpl(
            table_schema_,
            [reader](int32_t field_id) -> Result<std::vector<std::shared_ptr<GlobalIndexReader>>> {
                if (field_id == 0) {
                    return std::vector<std::shared_ptr<GlobalIndexReader>>{reader};
                }
                return std::vector<std::shared_ptr<GlobalIndexReader>>();
            });
    }

    std::shared_ptr<Predicate> Equal(const std::string& field_name, int32_t field_index) const {
        return PredicateBuilder::Equal(field_index, field_name, FieldType::BIGINT,
                                       Literal(static_cast<int64_t>(42)));
    }

    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<RecordingGlobalIndexReader> reader_;
};

TEST_F(GlobalIndexEvaluatorImplTest, SupportedAndUnsupportedLeavesCombineSafely) {
    GlobalIndexEvaluatorImpl evaluator = CreateEvaluator();
    std::shared_ptr<Predicate> indexed = Equal("a", 0);
    std::shared_ptr<Predicate> unindexed = Equal("b", 1);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> leaf_result,
                         evaluator.Evaluate(indexed));
    ASSERT_EQ((std::set<int64_t>{1, 3}), CollectPositions(leaf_result));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> unsupported_result,
                         evaluator.Evaluate(unindexed));
    ASSERT_EQ(nullptr, unsupported_result);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_predicate,
                         PredicateBuilder::And({indexed, unindexed}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> and_result,
                         evaluator.Evaluate(and_predicate));
    ASSERT_EQ((std::set<int64_t>{1, 3}), CollectPositions(and_result));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_predicate,
                         PredicateBuilder::Or({indexed, unindexed}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> or_result,
                         evaluator.Evaluate(or_predicate));
    ASSERT_EQ(nullptr, or_result);
}

TEST_F(GlobalIndexEvaluatorImplTest, NormalizationFlattensNestedCompounds) {
    std::shared_ptr<Predicate> first = Equal("a", 0);
    std::shared_ptr<Predicate> second = PredicateBuilder::IsNull(0, "a", FieldType::BIGINT);
    std::shared_ptr<Predicate> third = Equal("b", 1);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> nested, PredicateBuilder::And({first, second}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({nested, third}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> normalized,
                         GlobalIndexEvaluatorImpl::NormalizePredicate(predicate));
    auto compound = std::dynamic_pointer_cast<CompoundPredicate>(normalized);
    ASSERT_TRUE(compound != nullptr);
    ASSERT_EQ(Function::Type::AND, compound->GetFunction().GetType());
    ASSERT_EQ(3, compound->Children().size());
}

TEST_F(GlobalIndexEvaluatorImplTest, RedundantIsNotNullIsPrunedFromAnd) {
    std::shared_ptr<Predicate> equal = Equal("a", 0);
    std::shared_ptr<Predicate> is_not_null = PredicateBuilder::IsNotNull(0, "a", FieldType::BIGINT);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({equal, is_not_null}));

    GlobalIndexEvaluatorImpl evaluator = CreateEvaluator();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> result, evaluator.Evaluate(predicate));
    ASSERT_EQ((std::set<int64_t>{1, 3}), CollectPositions(result));
    ASSERT_EQ(1, reader_->EqualCalls());
    ASSERT_EQ(0, reader_->IsNotNullCalls());
}

TEST_F(GlobalIndexEvaluatorImplTest, IsNullDoesNotMakeIsNotNullRedundant) {
    std::shared_ptr<Predicate> is_null = PredicateBuilder::IsNull(0, "a", FieldType::BIGINT);
    std::shared_ptr<Predicate> is_not_null = PredicateBuilder::IsNotNull(0, "a", FieldType::BIGINT);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({is_null, is_not_null}));

    GlobalIndexEvaluatorImpl evaluator = CreateEvaluator();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> result, evaluator.Evaluate(predicate));
    ASSERT_TRUE(CollectPositions(result).empty());
    ASSERT_EQ(1, reader_->IsNullCalls());
    ASSERT_EQ(1, reader_->IsNotNullCalls());
}

TEST_F(GlobalIndexEvaluatorImplTest, OrDoesNotPruneIsNotNull) {
    std::shared_ptr<Predicate> equal = Equal("a", 0);
    std::shared_ptr<Predicate> is_not_null = PredicateBuilder::IsNotNull(0, "a", FieldType::BIGINT);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::Or({equal, is_not_null}));

    GlobalIndexEvaluatorImpl evaluator = CreateEvaluator();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> result, evaluator.Evaluate(predicate));
    ASSERT_EQ((std::set<int64_t>{0, 1, 3}), CollectPositions(result));
    ASSERT_EQ(1, reader_->EqualCalls());
    ASSERT_EQ(1, reader_->IsNotNullCalls());
}

TEST_F(GlobalIndexEvaluatorImplTest, NullPredicateIsNotEvaluated) {
    GlobalIndexEvaluatorImpl evaluator = CreateEvaluator();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexResult> result,
                         evaluator.Evaluate(/*predicate=*/nullptr));
    ASSERT_EQ(nullptr, result);
}

}  // namespace paimon::test
