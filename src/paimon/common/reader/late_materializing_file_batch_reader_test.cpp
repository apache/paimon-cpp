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

#include "paimon/common/reader/late_materializing_file_batch_reader.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/reader/late_materializing_reader_builder.h"
#include "paimon/common/reader/prefetch_file_batch_reader_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/read_ahead_cache.h"
#include "paimon/executor.h"
#include "paimon/format/reader_builder.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/reader/prefetch_file_batch_reader.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/mock/mock_format_reader_builder.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {

class LateMaterializingFileBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        k_field_ = arrow::field("k", arrow::int64());
        v_field_ = arrow::field("v", arrow::utf8());
        full_fields_ = {k_field_, v_field_};
        full_type_ = arrow::struct_(full_fields_);
    }

    // Build a struct array with column k (int64, values = ks) and column v (utf8, "v_<index>").
    std::shared_ptr<arrow::Array> BuildData(const std::vector<int64_t>& ks) {
        arrow::StructBuilder builder(
            full_type_, arrow::default_memory_pool(),
            {std::make_shared<arrow::Int64Builder>(), std::make_shared<arrow::StringBuilder>()});
        auto* k_builder = checked_cast<arrow::Int64Builder*>(builder.field_builder(0));
        auto* v_builder = checked_cast<arrow::StringBuilder*>(builder.field_builder(1));
        for (size_t i = 0; i < ks.size(); ++i) {
            EXPECT_TRUE(builder.Append().ok());
            EXPECT_TRUE(k_builder->Append(ks[i]).ok());
            EXPECT_TRUE(v_builder->Append("v_" + std::to_string(i)).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder.Finish(&array).ok());
        return array;
    }

    struct Row {
        int64_t k;
        std::string v;
        uint64_t file_row;
    };

    // Drive the reader through NextBatchWithBitmap to EOF, decoding the full-schema output rows.
    Result<std::vector<Row>> Collect(LateMaterializingFileBatchReader* reader) {
        std::vector<Row> rows;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                                   reader->NextBatchWithBitmap());
            if (BatchReader::IsEofBatch(batch_with_bitmap)) {
                break;
            }
            auto& [batch, bitmap] = batch_with_bitmap;
            auto& [c_array, c_schema] = batch;
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                              arrow::ImportArray(c_array.get(), c_schema.get()));
            auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(array);
            EXPECT_EQ(bitmap.Cardinality(), static_cast<int32_t>(struct_array->length()));
            auto k_array = arrow::internal::checked_pointer_cast<arrow::Int64Array>(
                struct_array->GetFieldByName("k"));
            if (!k_array) {
                return Status::Invalid("output batch missing k column");
            }
            // v is only present when it belongs to the read schema (payload projection).
            auto v_array = arrow::internal::checked_pointer_cast<arrow::StringArray>(
                struct_array->GetFieldByName("v"));
            for (int64_t i = 0; i < struct_array->length(); ++i) {
                PAIMON_ASSIGN_OR_RAISE(uint64_t file_row,
                                       reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
                rows.push_back(Row{k_array->Value(i),
                                   v_array ? v_array->GetString(i) : std::string(), file_row});
            }
        }
        return rows;
    }

    Status SetReadSchema(LateMaterializingFileBatchReader* reader,
                         const std::shared_ptr<arrow::Schema>& schema,
                         const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection) {
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &c_schema));
        return reader->SetReadSchema(&c_schema, predicate, selection);
    }

    // Collect all output rows as a single concatenated struct array (for schema/nested checks),
    // reusing the shared collector so the batch-offset and bitmap contracts are checked too.
    Result<std::shared_ptr<arrow::StructArray>> CollectStruct(FileBatchReader* reader) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> chunked,
                               ReadResultCollector::CollectResult(reader));
        if (chunked == nullptr) {
            return std::shared_ptr<arrow::StructArray>();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> combined,
                                          arrow::Concatenate(chunked->chunks()));
        return arrow::internal::checked_pointer_cast<arrow::StructArray>(combined);
    }

    // Build a struct with 5 columns [a:int64, b:utf8, c:int64, d:utf8, e:int64], each carrying a
    // distinct value pattern so any column reordering is detected.
    std::shared_ptr<arrow::Array> BuildMultiFieldData(int32_t n) {
        auto type =
            arrow::struct_({arrow::field("a", arrow::int64()), arrow::field("b", arrow::utf8()),
                            arrow::field("c", arrow::int64()), arrow::field("d", arrow::utf8()),
                            arrow::field("e", arrow::int64())});
        arrow::StructBuilder builder(
            type, arrow::default_memory_pool(),
            {std::make_shared<arrow::Int64Builder>(), std::make_shared<arrow::StringBuilder>(),
             std::make_shared<arrow::Int64Builder>(), std::make_shared<arrow::StringBuilder>(),
             std::make_shared<arrow::Int64Builder>()});
        auto* a = checked_cast<arrow::Int64Builder*>(builder.field_builder(0));
        auto* b = checked_cast<arrow::StringBuilder*>(builder.field_builder(1));
        auto* c = checked_cast<arrow::Int64Builder*>(builder.field_builder(2));
        auto* d = checked_cast<arrow::StringBuilder*>(builder.field_builder(3));
        auto* e = checked_cast<arrow::Int64Builder*>(builder.field_builder(4));
        for (int32_t i = 0; i < n; ++i) {
            EXPECT_TRUE(builder.Append().ok());
            EXPECT_TRUE(a->Append(i).ok());
            EXPECT_TRUE(b->Append("b_" + std::to_string(i)).ok());
            EXPECT_TRUE(c->Append(static_cast<int64_t>(i) * 100).ok());
            EXPECT_TRUE(d->Append("d_" + std::to_string(i)).ok());
            EXPECT_TRUE(e->Append(static_cast<int64_t>(i) * 10000).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder.Finish(&array).ok());
        return array;
    }

    // Build a struct with a nested payload column [k:int64, arr:list<int64>, tag:utf8].
    std::shared_ptr<arrow::Array> BuildNestedData(int32_t n) {
        auto type = arrow::struct_({arrow::field("k", arrow::int64()),
                                    arrow::field("arr", arrow::list(arrow::int64())),
                                    arrow::field("tag", arrow::utf8())});
        auto arr_value_builder = std::make_shared<arrow::Int64Builder>();
        arrow::StructBuilder builder(
            type, arrow::default_memory_pool(),
            {std::make_shared<arrow::Int64Builder>(),
             std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), arr_value_builder),
             std::make_shared<arrow::StringBuilder>()});
        auto* k = checked_cast<arrow::Int64Builder*>(builder.field_builder(0));
        auto* arr = checked_cast<arrow::ListBuilder*>(builder.field_builder(1));
        auto* arr_values = checked_cast<arrow::Int64Builder*>(arr->value_builder());
        auto* tag = checked_cast<arrow::StringBuilder*>(builder.field_builder(2));
        for (int32_t i = 0; i < n; ++i) {
            EXPECT_TRUE(builder.Append().ok());
            EXPECT_TRUE(k->Append(i).ok());
            EXPECT_TRUE(arr->Append().ok());
            EXPECT_TRUE(arr_values->Append(i).ok());
            EXPECT_TRUE(arr_values->Append(i + 1).ok());
            EXPECT_TRUE(tag->Append("t_" + std::to_string(i)).ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder.Finish(&array).ok());
        return array;
    }

 protected:
    std::shared_ptr<arrow::Field> k_field_;
    std::shared_ptr<arrow::Field> v_field_;
    arrow::FieldVector full_fields_;
    std::shared_ptr<arrow::DataType> full_type_;
};

// No predicate: the reader must pass through the inner reader unchanged (all rows, all columns).
TEST_F(LateMaterializingFileBatchReaderTest, PassThroughWhenNoPredicate) {
    auto data = BuildData({0, 1, 2, 3, 4});
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/2);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(full_fields_), /*predicate=*/nullptr,
                            std::nullopt));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows, Collect(reader.get()));
    ASSERT_EQ(rows.size(), 5u);
    for (int64_t i = 0; i < 5; ++i) {
        EXPECT_EQ(rows[i].k, i);
        EXPECT_EQ(rows[i].v, "v_" + std::to_string(i));
        EXPECT_EQ(rows[i].file_row, static_cast<uint64_t>(i));
    }
}

// The predicate references every projected column, so the payload set is empty: no late
// materialization, plain pass-through.
TEST_F(LateMaterializingFileBatchReaderTest, PassThroughWhenPayloadEmpty) {
    auto data = BuildData({0, 1, 2, 3, 4});
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/2);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    // read schema is just {k}; the predicate on k covers all columns -> payload empty
    auto predicate = PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"k",
                                                      FieldType::BIGINT, Literal(0l));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema({k_field_}), predicate, std::nullopt));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows, Collect(reader.get()));
    ASSERT_EQ(rows.size(), 5u);
    for (size_t idx = 0; idx < rows.size(); ++idx) {
        EXPECT_EQ(rows[idx].k, static_cast<int64_t>(idx));
        // v is outside the read schema, so the pass-through output must not carry it
        EXPECT_EQ(rows[idx].v, "");
        EXPECT_EQ(rows[idx].file_row, static_cast<uint64_t>(idx));
    }
}

// Contiguous matched subset spanning multiple batches.
TEST_F(LateMaterializingFileBatchReaderTest, ContiguousSubsetAcrossBatches) {
    auto data = BuildData({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/3);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"k",
                                                   FieldType::BIGINT, Literal(4l));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(full_fields_), predicate, std::nullopt));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows, Collect(reader.get()));
    ASSERT_EQ(rows.size(), 5u);  // k = 5..9
    for (size_t idx = 0; idx < rows.size(); ++idx) {
        int64_t expected = 5 + static_cast<int64_t>(idx);
        EXPECT_EQ(rows[idx].k, expected);
        EXPECT_EQ(rows[idx].v, "v_" + std::to_string(expected));
        EXPECT_EQ(rows[idx].file_row, static_cast<uint64_t>(expected));
    }
}

// Scattered (alternating) matched rows: predicate matches every other row.
TEST_F(LateMaterializingFileBatchReaderTest, ScatteredAlternatingMatch) {
    // k = 0,1,0,1,... ; predicate k == 1 matches all odd file rows.
    std::vector<int64_t> ks;
    for (int i = 0; i < 12; ++i) {
        ks.push_back(i % 2);
    }
    auto data = BuildData(ks);
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/3);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"k",
                                             FieldType::BIGINT, Literal(1l));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(full_fields_), predicate, std::nullopt));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows, Collect(reader.get()));
    ASSERT_EQ(rows.size(), 6u);  // odd rows 1,3,5,7,9,11
    for (size_t idx = 0; idx < rows.size(); ++idx) {
        uint64_t expected_row = 2 * idx + 1;
        EXPECT_EQ(rows[idx].k, 1);
        EXPECT_EQ(rows[idx].v, "v_" + std::to_string(expected_row));
        EXPECT_EQ(rows[idx].file_row, expected_row);
    }
}

// The selection bitmap further restricts the matched rows: matched must be a subset of selection.
TEST_F(LateMaterializingFileBatchReaderTest, MatchedIntersectsSelection) {
    std::vector<int64_t> ks;
    for (int i = 0; i < 12; ++i) {
        ks.push_back(i % 2);
    }
    auto data = BuildData(ks);
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/4);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"k",
                                             FieldType::BIGINT, Literal(1l));
    // predicate hits {1,3,5,7,9,11}; selection keeps only {1,5,9}
    RoaringBitmap32 selection;
    selection.Add(1);
    selection.Add(5);
    selection.Add(9);
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(full_fields_), predicate,
                            std::optional<RoaringBitmap32>(selection)));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows, Collect(reader.get()));
    ASSERT_EQ(rows.size(), 3u);
    const std::vector<uint64_t> expected_rows = {1u, 5u, 9u};
    for (size_t idx = 0; idx < rows.size(); ++idx) {
        EXPECT_EQ(rows[idx].k, 1);
        EXPECT_EQ(rows[idx].v, "v_" + std::to_string(expected_rows[idx]));
        EXPECT_EQ(rows[idx].file_row, expected_rows[idx]);
    }
}

// No matched rows: the reader returns EOF immediately.
TEST_F(LateMaterializingFileBatchReaderTest, EmptyMatchReturnsEof) {
    auto data = BuildData({0, 1, 2, 3, 4});
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/2);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"k",
                                                   FieldType::BIGINT, Literal(100l));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(full_fields_), predicate, std::nullopt));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows, Collect(reader.get()));
    ASSERT_TRUE(rows.empty());
}

// SeekToRow during payload emission must re-align the probe cursor so probe/payload stay matched.
TEST_F(LateMaterializingFileBatchReaderTest, SeekToRowRealignsProbeCursor) {
    auto data = BuildData({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/4);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    auto predicate = PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"k",
                                                      FieldType::BIGINT, Literal(5l));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(full_fields_), predicate, std::nullopt));

    // First payload batch triggers the probe scan; matched rows are 5..9.
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap first, reader->NextBatchWithBitmap());
    ASSERT_FALSE(BatchReader::IsEofBatch(first));
    ReaderUtils::ReleaseReadBatch(std::move(first.first));

    // Seek forward to file row 8: subsequent output must be exactly rows 8 and 9, correctly paired.
    ASSERT_OK(reader->SeekToRow(8));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows, Collect(reader.get()));
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].k, 8);
    EXPECT_EQ(rows[0].v, "v_8");
    EXPECT_EQ(rows[0].file_row, 8u);
    EXPECT_EQ(rows[1].k, 9);
    EXPECT_EQ(rows[1].v, "v_9");
    EXPECT_EQ(rows[1].file_row, 9u);
}

// SetReadRanges must be cached and re-forwarded to the inner reader across the probe/payload
// schema switches (SetReadSchema resets the inner reader's ranges).
TEST_F(LateMaterializingFileBatchReaderTest, ReadRangesForwardedAcrossPhases) {
    auto data = BuildData({0, 1, 2, 3, 4, 5, 6, 7});
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/4);
    auto* mock_ptr = mock.get();
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    auto predicate = PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"k",
                                                      FieldType::BIGINT, Literal(2l));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(full_fields_), predicate, std::nullopt));

    std::vector<std::pair<uint64_t, uint64_t>> ranges = {{0, 8}};
    ASSERT_OK(reader->SetReadRanges(ranges));

    // Drive to EOF; this performs the probe pass and the payload schema switch.
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows, Collect(reader.get()));
    ASSERT_EQ(rows.size(), 6u);  // k = 2..7
    for (size_t idx = 0; idx < rows.size(); ++idx) {
        int64_t expected = 2 + static_cast<int64_t>(idx);
        EXPECT_EQ(rows[idx].k, expected);
        EXPECT_EQ(rows[idx].v, "v_" + std::to_string(expected));
        EXPECT_EQ(rows[idx].file_row, static_cast<uint64_t>(expected));
    }

    // The inner reader must have received the cached ranges again after the payload switch.
    // SetReadSchema (invoked on the payload switch) clears the inner reader's ranges, so the
    // cached ranges still being present at the end proves LM re-forwarded them after the switch.
    ASSERT_EQ(mock_ptr->GetReadRanges(), ranges);
}

// SetReadSchema is re-entrant: a second call with a different predicate resets probe state.
TEST_F(LateMaterializingFileBatchReaderTest, ReentrantSetReadSchema) {
    auto data = BuildData({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/3);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));

    auto predicate1 = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"k",
                                                    FieldType::BIGINT, Literal(7l));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(full_fields_), predicate1, std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows1, Collect(reader.get()));
    ASSERT_EQ(rows1.size(), 2u);  // k = 8,9
    for (size_t idx = 0; idx < rows1.size(); ++idx) {
        int64_t expected = 8 + static_cast<int64_t>(idx);
        EXPECT_EQ(rows1[idx].k, expected);
        EXPECT_EQ(rows1[idx].v, "v_" + std::to_string(expected));
        EXPECT_EQ(rows1[idx].file_row, static_cast<uint64_t>(expected));
    }

    auto predicate2 = PredicateBuilder::LessThan(/*field_index=*/0, /*field_name=*/"k",
                                                 FieldType::BIGINT, Literal(3l));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(full_fields_), predicate2, std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows2, Collect(reader.get()));
    ASSERT_EQ(rows2.size(), 3u);  // k = 0,1,2
    for (size_t idx = 0; idx < rows2.size(); ++idx) {
        EXPECT_EQ(rows2[idx].k, static_cast<int64_t>(idx));
        EXPECT_EQ(rows2[idx].v, "v_" + std::to_string(idx));
        EXPECT_EQ(rows2[idx].file_row, static_cast<uint64_t>(idx));
    }
}

// Forwarded metadata accessors should reflect the inner reader.
TEST_F(LateMaterializingFileBatchReaderTest, ForwardsRowCountAndFileSchema) {
    auto data = BuildData({0, 1, 2, 3});
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/2);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));

    ASSERT_OK_AND_ASSIGN(uint64_t num_rows, reader->GetNumberOfRows());
    EXPECT_EQ(num_rows, 4u);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<::ArrowSchema> c_file_schema, reader->GetFileSchema());
    auto import_result = arrow::ImportType(c_file_schema.get());
    ASSERT_TRUE(import_result.ok());
    EXPECT_TRUE(import_result.ValueOrDie()->Equals(full_type_));
}

// With many columns and a predicate over two non-adjacent probe columns, the output must keep the
// full read-schema field order (and each probe/payload column's values must not be scrambled).
TEST_F(LateMaterializingFileBatchReaderTest, MultiFieldPreservesColumnOrder) {
    auto data = BuildMultiFieldData(10);
    auto type = data->type();
    auto mock = std::make_unique<MockFileBatchReader>(data, type, /*batch_size=*/3);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    // probe columns = {a (idx0), c (idx2)}; payload columns = {b, d, e}
    auto pred_a =
        PredicateBuilder::GreaterOrEqual(/*field_index=*/0, "a", FieldType::BIGINT, Literal(3l));
    auto pred_c =
        PredicateBuilder::LessThan(/*field_index=*/2, "c", FieldType::BIGINT, Literal(700l));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({pred_a, pred_c}));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(type->fields()), predicate, std::nullopt));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result, CollectStruct(reader.get()));
    ASSERT_TRUE(result);
    // a >= 3 and c(=i*100) < 700 -> i in {3,4,5,6}
    ASSERT_EQ(result->length(), 4);
    // output field order must equal the requested full schema order
    ASSERT_EQ(result->num_fields(), 5);
    auto out_type = arrow::internal::checked_pointer_cast<arrow::StructType>(result->type());
    EXPECT_EQ(out_type->field(0)->name(), "a");
    EXPECT_EQ(out_type->field(1)->name(), "b");
    EXPECT_EQ(out_type->field(2)->name(), "c");
    EXPECT_EQ(out_type->field(3)->name(), "d");
    EXPECT_EQ(out_type->field(4)->name(), "e");
    auto a = arrow::internal::checked_pointer_cast<arrow::Int64Array>(result->GetFieldByName("a"));
    auto b = arrow::internal::checked_pointer_cast<arrow::StringArray>(result->GetFieldByName("b"));
    auto c = arrow::internal::checked_pointer_cast<arrow::Int64Array>(result->GetFieldByName("c"));
    auto d = arrow::internal::checked_pointer_cast<arrow::StringArray>(result->GetFieldByName("d"));
    auto e = arrow::internal::checked_pointer_cast<arrow::Int64Array>(result->GetFieldByName("e"));
    const std::vector<int64_t> expected = {3, 4, 5, 6};
    for (size_t j = 0; j < expected.size(); ++j) {
        int64_t i = expected[j];
        EXPECT_EQ(a->Value(j), i);
        EXPECT_EQ(b->GetString(j), "b_" + std::to_string(i));
        EXPECT_EQ(c->Value(j), i * 100);
        EXPECT_EQ(d->GetString(j), "d_" + std::to_string(i));
        EXPECT_EQ(e->Value(j), i * 10000);
    }
}

// A nested (list) payload column must round-trip unchanged for the matched rows.
TEST_F(LateMaterializingFileBatchReaderTest, NestedPayloadColumn) {
    auto data = BuildNestedData(8);
    auto type = data->type();
    auto mock = std::make_unique<MockFileBatchReader>(data, type, /*batch_size=*/3);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    // probe = {k}; payload = {arr (list<int64>), tag}
    auto predicate =
        PredicateBuilder::GreaterOrEqual(/*field_index=*/0, "k", FieldType::BIGINT, Literal(5l));
    ASSERT_OK(SetReadSchema(reader.get(), arrow::schema(type->fields()), predicate, std::nullopt));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result, CollectStruct(reader.get()));
    ASSERT_TRUE(result);
    ASSERT_EQ(result->length(), 3);  // k = 5,6,7
    auto k = arrow::internal::checked_pointer_cast<arrow::Int64Array>(result->GetFieldByName("k"));
    auto arr =
        arrow::internal::checked_pointer_cast<arrow::ListArray>(result->GetFieldByName("arr"));
    auto tag =
        arrow::internal::checked_pointer_cast<arrow::StringArray>(result->GetFieldByName("tag"));
    ASSERT_TRUE(k && arr && tag);
    for (int64_t j = 0; j < result->length(); ++j) {
        int64_t i = 5 + j;
        EXPECT_EQ(k->Value(j), i);
        EXPECT_EQ(tag->GetString(j), "t_" + std::to_string(i));
        auto sub = arrow::internal::checked_pointer_cast<arrow::Int64Array>(arr->value_slice(j));
        ASSERT_EQ(sub->length(), 2);
        EXPECT_EQ(sub->Value(0), i);
        EXPECT_EQ(sub->Value(1), i + 1);
    }
}

// The late-materialization reader must work correctly as an inner reader driven by
// PrefetchFileBatchReaderImpl (schema broadcast, range dispatch, seek, row-id tracking).
TEST_F(LateMaterializingFileBatchReaderTest, WorksAsInnerOfPrefetchReader) {
    auto data = BuildData({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    LateMaterializingReaderBuilder builder(
        std::make_unique<MockFormatReaderBuilder>(data, full_type_, /*batch_size=*/3),
        GetDefaultPool());
    auto mock_fs = std::make_shared<MockFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(2));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PrefetchFileBatchReaderImpl> impl,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", /*data_file_size=*/0, &builder, mock_fs,
            /*prefetch_max_parallel_num=*/1, /*batch_size=*/3, /*prefetch_batch_count=*/2,
            /*enable_adaptive_prefetch_strategy=*/false, executor,
            /*initialize_read_ranges=*/false, /*read_ahead_cache_enabled=*/false, CacheConfig(),
            GetDefaultPool()));
    auto predicate =
        PredicateBuilder::GreaterOrEqual(/*field_index=*/0, "k", FieldType::BIGINT, Literal(4l));
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*arrow::schema(full_fields_), &c_schema).ok());
    ASSERT_OK(impl->SetReadSchema(&c_schema, predicate, std::nullopt));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result, CollectStruct(impl.get()));
    ASSERT_TRUE(result);
    ASSERT_EQ(result->length(), 6);  // k = 4..9
    auto k = arrow::internal::checked_pointer_cast<arrow::Int64Array>(result->GetFieldByName("k"));
    auto v = arrow::internal::checked_pointer_cast<arrow::StringArray>(result->GetFieldByName("v"));
    ASSERT_TRUE(k && v);
    for (int64_t j = 0; j < result->length(); ++j) {
        EXPECT_EQ(k->Value(j), 4 + j);
        EXPECT_EQ(v->GetString(j), "v_" + std::to_string(4 + j));
    }
    impl->Close();
}

// Re-setting the read schema on the prefetch impl (which re-broadcasts to the inner LM readers and
// re-plans ranges) must reset the probe state and produce correct results for the new predicate.
TEST_F(LateMaterializingFileBatchReaderTest, PrefetchInnerReentrantSetReadSchema) {
    auto data = BuildData({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    LateMaterializingReaderBuilder builder(
        std::make_unique<MockFormatReaderBuilder>(data, full_type_, /*batch_size=*/3),
        GetDefaultPool());
    auto mock_fs = std::make_shared<MockFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(2));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PrefetchFileBatchReaderImpl> impl,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", /*data_file_size=*/0, &builder, mock_fs,
            /*prefetch_max_parallel_num=*/1, /*batch_size=*/3, /*prefetch_batch_count=*/2,
            /*enable_adaptive_prefetch_strategy=*/false, executor,
            /*initialize_read_ranges=*/false, /*read_ahead_cache_enabled=*/false, CacheConfig(),
            GetDefaultPool()));

    auto full_schema = arrow::schema(full_fields_);
    auto predicate1 =
        PredicateBuilder::GreaterThan(/*field_index=*/0, "k", FieldType::BIGINT, Literal(6l));
    ::ArrowSchema c_schema1;
    ASSERT_TRUE(arrow::ExportSchema(*full_schema, &c_schema1).ok());
    ASSERT_OK(impl->SetReadSchema(&c_schema1, predicate1, std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result1, CollectStruct(impl.get()));
    ASSERT_TRUE(result1);
    ASSERT_EQ(result1->length(), 3);  // k = 7,8,9
    auto k1 =
        arrow::internal::checked_pointer_cast<arrow::Int64Array>(result1->GetFieldByName("k"));
    auto v1 =
        arrow::internal::checked_pointer_cast<arrow::StringArray>(result1->GetFieldByName("v"));
    ASSERT_TRUE(k1 && v1);
    for (int64_t j = 0; j < result1->length(); ++j) {
        EXPECT_EQ(k1->Value(j), 7 + j);
        EXPECT_EQ(v1->GetString(j), "v_" + std::to_string(7 + j));
    }

    auto predicate2 =
        PredicateBuilder::LessThan(/*field_index=*/0, "k", FieldType::BIGINT, Literal(3l));
    ::ArrowSchema c_schema2;
    ASSERT_TRUE(arrow::ExportSchema(*full_schema, &c_schema2).ok());
    ASSERT_OK(impl->SetReadSchema(&c_schema2, predicate2, std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result2, CollectStruct(impl.get()));
    ASSERT_TRUE(result2);
    ASSERT_EQ(result2->length(), 3);  // k = 0,1,2
    auto k2 =
        arrow::internal::checked_pointer_cast<arrow::Int64Array>(result2->GetFieldByName("k"));
    auto v2 =
        arrow::internal::checked_pointer_cast<arrow::StringArray>(result2->GetFieldByName("v"));
    for (int64_t j = 0; j < result2->length(); ++j) {
        EXPECT_EQ(k2->Value(j), j);
        EXPECT_EQ(v2->GetString(j), "v_" + std::to_string(j));
    }
    impl->Close();
}

// With multiple parallel inner readers and per-batch ranges, the prefetch impl dispatches disjoint
// ranges to each LM reader and drives them via EnsureReaderPosition/SeekToRow. The merged output
// must still be exactly the matched rows in ascending file order.
TEST_F(LateMaterializingFileBatchReaderTest, PrefetchInnerParallelReadersWithSeek) {
    std::vector<int64_t> ks;
    for (int i = 0; i < 20; ++i) {
        ks.push_back(i);
    }
    auto data = BuildData(ks);
    // Per-batch ranges (the mock's default) let the impl split work across the parallel readers,
    // and each range-honoring reader only reads its assigned slice.
    LateMaterializingReaderBuilder builder(
        std::make_unique<MockFormatReaderBuilder>(data, full_type_, /*batch_size=*/3),
        GetDefaultPool());
    auto mock_fs = std::make_shared<MockFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(3));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PrefetchFileBatchReaderImpl> impl,
        PrefetchFileBatchReaderImpl::Create(
            /*data_file_path=*/"", /*data_file_size=*/0, &builder, mock_fs,
            /*prefetch_max_parallel_num=*/3, /*batch_size=*/3, /*prefetch_batch_count=*/6,
            /*enable_adaptive_prefetch_strategy=*/false, executor,
            /*initialize_read_ranges=*/false, /*read_ahead_cache_enabled=*/false, CacheConfig(),
            GetDefaultPool()));
    auto predicate =
        PredicateBuilder::GreaterOrEqual(/*field_index=*/0, "k", FieldType::BIGINT, Literal(5l));
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*arrow::schema(full_fields_), &c_schema).ok());
    ASSERT_OK(impl->SetReadSchema(&c_schema, predicate, std::nullopt));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result, CollectStruct(impl.get()));
    ASSERT_TRUE(result);
    ASSERT_EQ(result->length(), 15);  // k = 5..19
    auto k = arrow::internal::checked_pointer_cast<arrow::Int64Array>(result->GetFieldByName("k"));
    auto v = arrow::internal::checked_pointer_cast<arrow::StringArray>(result->GetFieldByName("v"));
    ASSERT_TRUE(k && v);
    for (int64_t j = 0; j < result->length(); ++j) {
        EXPECT_EQ(k->Value(j), 5 + j);
        EXPECT_EQ(v->GetString(j), "v_" + std::to_string(5 + j));
    }
    impl->Close();
}

// When the predicate's field type does not match the probe schema, the
// ValidatePredicateWithSchema check must fail with a clear error instead
// of silently producing incorrect results.
TEST_F(LateMaterializingFileBatchReaderTest, FailsOnPredicateTypeMismatch) {
    auto data = BuildData({0, 1, 2, 3, 4});
    auto mock = std::make_unique<MockFileBatchReader>(data, full_type_, /*batch_size=*/2);
    ASSERT_OK_AND_ASSIGN(
        auto reader, LateMaterializingFileBatchReader::Create(std::move(mock), GetDefaultPool()));
    // k is int64 in the schema, but the predicate claims FieldType::INT (int32).
    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"k", FieldType::INT, Literal(10));
    ASSERT_NOK_WITH_MSG(
        SetReadSchema(reader.get(), arrow::schema(full_fields_), predicate, std::nullopt),
        "mismatches");
}

}  // namespace paimon::test
