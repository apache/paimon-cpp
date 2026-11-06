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

#include "paimon/common/reader/predicate_batch_reader.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::test {
class PredicateBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        fields_ = {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int64()),
                   arrow::field("f2", arrow::boolean())};
        data_type_ = arrow::struct_(fields_);
    }

    void TearDown() override {}

    std::shared_ptr<arrow::Array> PrepareArray(int32_t length, int32_t offset = 0) {
        arrow::StructBuilder struct_builder(
            data_type_, arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int64Builder>(),
             std::make_shared<arrow::BooleanBuilder>()});
        auto string_builder = checked_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto big_int_builder = checked_cast<arrow::Int64Builder*>(struct_builder.field_builder(1));
        auto bool_builder = checked_cast<arrow::BooleanBuilder*>(struct_builder.field_builder(2));
        for (int32_t i = 0 + offset; i < length + offset; ++i) {
            EXPECT_TRUE(struct_builder.Append().ok());
            EXPECT_TRUE(string_builder->Append("str_" + std::to_string(i)).ok());
            EXPECT_TRUE(big_int_builder->Append(i).ok());
            EXPECT_TRUE(bool_builder->Append(static_cast<bool>(i % 2)).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    void CheckResult(std::unique_ptr<BatchReader>&& reader,
                     const std::shared_ptr<Predicate>& predicate,
                     const std::shared_ptr<arrow::ChunkedArray>& expected_array) const {
        ASSERT_OK_AND_ASSIGN(auto predicate_reader,
                             PredicateBatchReader::Create(std::move(reader), predicate,
                                                          GetArrowPool(GetDefaultPool())));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result_array,
                             ReadResultCollector::CollectResult(std::move(predicate_reader)));
        if (expected_array) {
            ASSERT_TRUE(result_array->Equals(expected_array));
        } else {
            ASSERT_FALSE(result_array);
        }
    }

 private:
    arrow::FieldVector fields_;
    std::shared_ptr<arrow::DataType> data_type_;
};

TEST_F(PredicateBatchReaderTest, TestSimple) {
    auto data_array = PrepareArray(100);
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(24l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(0, 24));
        CheckResult(std::move(reader), predicate, expected_array);
    }
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(1l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(0, 1));
        CheckResult(std::move(reader), predicate, expected_array);
    }
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(99l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(0, 99));
        CheckResult(std::move(reader), predicate, expected_array);
    }
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::BIGINT, Literal(0l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(1, 99));
        CheckResult(std::move(reader), predicate, expected_array);
    }
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::BIGINT, Literal(98l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(99, 1));
        CheckResult(std::move(reader), predicate, expected_array);
    }
}

TEST_F(PredicateBatchReaderTest, TestVariousBatchSize) {
    auto data_array = arrow::ipc::internal::json::ArrayFromJSON(data_type_, R"([
        ["str_-1", -1, false],
        ["str_0", 0, false], ["str_1", 1, true],
        ["str_-1", -1, false],
        ["str_2", 2, false], ["str_3", 3, true],
        ["str_4", 4, false], ["str_5", 5, true], ["str_6", 6, false],
        ["str_-1", -1, false],
        ["str_7", 7, true],
        ["str_-1", -1, false]
    ])")
                          .ValueOrDie();
    auto expected_array = std::make_shared<arrow::ChunkedArray>(PrepareArray(8));
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                   FieldType::BIGINT, Literal(-1l));
    for (auto batch_size : {5, 10, 11, 20}) {
        auto reader = std::make_unique<MockFileBatchReader>(data_array, data_type_, batch_size);
        CheckResult(std::move(reader), predicate, expected_array);
    }
}

TEST_F(PredicateBatchReaderTest, TestOneByOneCase) {
    auto data_array = PrepareArray(8);
    auto reader = std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
    auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                             FieldType::BOOLEAN, Literal(true));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(data_type_, {R"([
        ["str_1", 1, true], ["str_3", 3, true], ["str_5", 5, true], ["str_7", 7, true]
    ])"},
                                                                         &expected_array);
    CheckResult(std::move(reader), predicate, expected_array);
}

TEST_F(PredicateBatchReaderTest, TestBindFieldIndexByName) {
    auto data_array = PrepareArray(8);
    auto reader = std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
    auto predicate = PredicateBuilder::LessThan(
        /*field_index=*/0, /*field_name=*/"f1", FieldType::BIGINT, Literal(3l));
    auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array->Slice(0, 3));
    CheckResult(std::move(reader), predicate, expected_array);
}

TEST_F(PredicateBatchReaderTest, TestFullAndEmptyCase) {
    auto data_array = PrepareArray(15);
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f1",
                                                    FieldType::BIGINT, Literal(20l));
        auto expected_array = std::make_shared<arrow::ChunkedArray>(data_array);
        CheckResult(std::move(reader), predicate, expected_array);
    }
    {
        auto reader =
            std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::BIGINT, Literal(20l));
        CheckResult(std::move(reader), predicate, nullptr);
    }
}

TEST_F(PredicateBatchReaderTest, TestPreservesSelectedRowPositions) {
    const std::shared_ptr<arrow::Array> source = PrepareArray(12)->Slice(2, 8);
    RoaringBitmap32 full;
    full.AddRange(0, 8);
    const std::vector<RoaringBitmap32> selections = {full, RoaringBitmap32::From({1, 2, 3, 4, 5}),
                                                     RoaringBitmap32::From({0, 1, 4, 5, 7}),
                                                     RoaringBitmap32::From({0, 2, 5, 7})};
    for (const RoaringBitmap32& selection : selections) {
        SCOPED_TRACE(selection.ToString());
        // The index is intentionally wrong: binding must still resolve the field by name.
        const std::shared_ptr<Predicate> predicate =
            PredicateBuilder::GreaterThan(0, "f1", FieldType::BIGINT, Literal(int64_t{4}));
        auto input = std::make_unique<MockFileBatchReader>(source, source->type(), selection,
                                                           /*read_batch_size=*/8);
        input->EnableRandomizeBatchSize(false);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<PredicateBatchReader> reader,
                             PredicateBatchReader::Create(std::move(input), predicate,
                                                          GetArrowPool(GetDefaultPool())));
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, reader->NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        RoaringBitmap32 expected_selection;
        for (auto iter = selection.Begin(); iter != selection.End(); ++iter) {
            if (*iter + 2 > 4) {
                expected_selection.Add(*iter);
            }
        }
        ASSERT_EQ(expected_selection, batch.second);
        arrow::Result<std::shared_ptr<arrow::Array>> imported =
            arrow::ImportArray(batch.first.first.get(), batch.first.second.get());
        ASSERT_TRUE(imported.ok()) << imported.status().ToString();
        const std::shared_ptr<arrow::Array> output = std::move(imported).ValueOrDie();
        ASSERT_TRUE(source->Equals(*output));
        ASSERT_OK_AND_ASSIGN(batch, reader->NextBatchWithBitmap());
        ASSERT_TRUE(BatchReader::IsEofBatch(batch));
    }
}

TEST_F(PredicateBatchReaderTest, TestFragmentedSelectionsWithLargeInList) {
    constexpr int32_t kRowCount = 8192;
    const std::shared_ptr<arrow::Array> source = PrepareArray(kRowCount + 2)->Slice(2, kRowCount);
    std::vector<Literal> literals;
    for (int64_t i = 0; i < 1024; ++i) {
        literals.emplace_back(2 + i * 4);
    }
    const std::shared_ptr<Predicate> predicate =
        PredicateBuilder::In(1, "f1", FieldType::BIGINT, literals);
    // Exercise dense, threshold, and sparse selections against the same predicate and sliced
    // input. Matching unselected rows must never be added by whole-batch evaluation.
    for (int32_t stride : {2, 3, 4, 64}) {
        SCOPED_TRACE(stride);
        RoaringBitmap32 selection;
        RoaringBitmap32 expected;
        for (int32_t row = 0; row < kRowCount; row += stride) {
            selection.Add(row);
            if (row < 4096 && row % 4 == 0) {
                expected.Add(row);
            }
        }
        auto input = std::make_unique<MockFileBatchReader>(source, source->type(), selection,
                                                           /*read_batch_size=*/kRowCount);
        input->EnableRandomizeBatchSize(false);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<PredicateBatchReader> reader,
                             PredicateBatchReader::Create(std::move(input), predicate,
                                                          GetArrowPool(GetDefaultPool())));
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, reader->NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        ASSERT_EQ(expected, batch.second);
        auto imported = arrow::ImportArray(batch.first.first.get(), batch.first.second.get());
        ASSERT_TRUE(imported.ok()) << imported.status().ToString();
        ASSERT_TRUE(source->Equals(*imported.ValueOrDie()));
        ASSERT_OK_AND_ASSIGN(batch, reader->NextBatchWithBitmap());
        ASSERT_TRUE(BatchReader::IsEofBatch(batch));
    }
}

TEST_F(PredicateBatchReaderTest, TestEmptySelectionsAndPredicateResults) {
    const std::shared_ptr<arrow::Array> source = PrepareArray(12);
    const std::shared_ptr<Predicate> predicate =
        PredicateBuilder::GreaterThan(1, "f1", FieldType::BIGINT, Literal(int64_t{6}));
    const RoaringBitmap32 selection = RoaringBitmap32::From({4, 5, 8, 10});
    auto input = std::make_unique<MockFileBatchReader>(source, source->type(), selection,
                                                       /*read_batch_size=*/4);
    input->EnableRandomizeBatchSize(false);
    const auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ArrayVector{source->Slice(8, 1), source->Slice(10, 1)});
    ASSERT_NO_FATAL_FAILURE(CheckResult(std::move(input), predicate, expected));

    input = std::make_unique<MockFileBatchReader>(source, source->type(), RoaringBitmap32(),
                                                  /*read_batch_size=*/4);
    input->EnableRandomizeBatchSize(false);
    ASSERT_NO_FATAL_FAILURE(CheckResult(std::move(input), predicate, nullptr));
}

TEST_F(PredicateBatchReaderTest, TestSelectedNullsAndNextBatchMaterialization) {
    const std::shared_ptr<arrow::Array> source =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({arrow::field("f1", arrow::int64())}), "[[null], [2], [null], [4], [5]]")
            .ValueOrDie();
    const RoaringBitmap32 selection = RoaringBitmap32::From({0, 2, 3});
    auto input = std::make_unique<MockFileBatchReader>(source, source->type(), selection,
                                                       /*read_batch_size=*/5);
    input->EnableRandomizeBatchSize(false);
    const std::shared_ptr<Predicate> predicate =
        PredicateBuilder::IsNull(0, "f1", FieldType::BIGINT);
    const std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(source->type(), "[[null], [null]]").ValueOrDie();
    ASSERT_NO_FATAL_FAILURE(
        CheckResult(std::move(input), predicate, std::make_shared<arrow::ChunkedArray>(expected)));
}

TEST_F(PredicateBatchReaderTest, TestInvalidInput) {
    auto data_array = PrepareArray(8);
    auto reader = std::make_unique<MockFileBatchReader>(data_array, data_type_, /*batch_size=*/10);
    ASSERT_NOK_WITH_MSG(
        PredicateBatchReader::Create(std::move(reader), nullptr, GetArrowPool(GetDefaultPool())),
        "create predicate batch reader failed. predicate is nullptr");
}

}  // namespace paimon::test
