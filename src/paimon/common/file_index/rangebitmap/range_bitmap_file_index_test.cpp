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

#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <numeric>
#include <set>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class RangeBitmapFileIndexTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        fs_ = std::make_shared<LocalFileSystem>();
    }

    void TearDown() override {
        index_buffer_.reset();
        pool_.reset();
        fs_.reset();
    }

    static void CheckResult(const std::shared_ptr<FileIndexResult>& result,
                            const std::vector<int32_t>& expected) {
        const auto typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap32* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        const RoaringBitmap32 expected_bitmap = RoaringBitmap32::From(expected);
        ASSERT_EQ(*bitmap, expected_bitmap)
            << "result=" << bitmap->ToString() << ", expected=" << expected_bitmap.ToString();
    }

    // Helper function to create writer, serialize, and create reader
    template <typename ArrowBuilder, typename ValueType>
    Result<std::shared_ptr<RangeBitmapFileIndexReader>> CreateReaderForTest(
        const std::shared_ptr<arrow::DataType>& arrow_type, const std::vector<ValueType>& test_data,
        PAIMON_UNIQUE_PTR<Bytes>* serialized_bytes_out) {
        return CreateReaderForTest<ArrowBuilder, ValueType>(arrow_type, test_data, {}, {},
                                                            serialized_bytes_out);
    }

    // Overload with NULL support - null_indices specifies which positions are NULL
    template <typename ArrowBuilder, typename ValueType>
    Result<std::shared_ptr<RangeBitmapFileIndexReader>> CreateReaderForTest(
        const std::shared_ptr<arrow::DataType>& arrow_type, const std::vector<ValueType>& test_data,
        const std::set<int32_t>& null_indices, PAIMON_UNIQUE_PTR<Bytes>* serialized_bytes_out) {
        return CreateReaderForTest<ArrowBuilder, ValueType>(arrow_type, test_data, null_indices, {},
                                                            serialized_bytes_out);
    }

    // Overload with options to exercise writer configuration such as chunk size.
    template <typename ArrowBuilder, typename ValueType>
    Result<std::shared_ptr<RangeBitmapFileIndexReader>> CreateReaderForTest(
        const std::shared_ptr<arrow::DataType>& arrow_type, const std::vector<ValueType>& test_data,
        const std::map<std::string, std::string>& options,
        PAIMON_UNIQUE_PTR<Bytes>* serialized_bytes_out) {
        return CreateReaderForTest<ArrowBuilder, ValueType>(arrow_type, test_data, {}, options,
                                                            serialized_bytes_out);
    }

    // Full overload with NULL support and options
    template <typename ArrowBuilder, typename ValueType>
    Result<std::shared_ptr<RangeBitmapFileIndexReader>> CreateReaderForTest(
        const std::shared_ptr<arrow::DataType>& arrow_type, const std::vector<ValueType>& test_data,
        const std::set<int32_t>& null_indices, const std::map<std::string, std::string>& options,
        PAIMON_UNIQUE_PTR<Bytes>* serialized_bytes_out);

    Result<std::shared_ptr<RangeBitmapFileIndexReader>> CreateReaderFromJson(
        const std::shared_ptr<arrow::DataType>& arrow_type, const std::string& json,
        const std::map<std::string, std::string>& options,
        PAIMON_UNIQUE_PTR<Bytes>* serialized_bytes_out);

    Result<std::shared_ptr<RangeBitmapFileIndexReader>> CreateReaderFromArray(
        const std::shared_ptr<arrow::DataType>& arrow_type,
        const std::shared_ptr<arrow::Array>& array,
        const std::map<std::string, std::string>& options,
        PAIMON_UNIQUE_PTR<Bytes>* serialized_bytes_out);

 protected:
    std::shared_ptr<MemoryPool> pool_;

 private:
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<Bytes> index_buffer_;
};

template <typename ArrowBuilder, typename ValueType>
Result<std::shared_ptr<RangeBitmapFileIndexReader>> RangeBitmapFileIndexTest::CreateReaderForTest(
    const std::shared_ptr<arrow::DataType>& arrow_type, const std::vector<ValueType>& test_data,
    const std::set<int32_t>& null_indices, const std::map<std::string, std::string>& options,
    PAIMON_UNIQUE_PTR<Bytes>* serialized_bytes_out) {
    // Create Arrow array from test data, replacing values at null_indices with NULL
    auto builder = std::make_shared<ArrowBuilder>();
    for (size_t i = 0; i < test_data.size(); ++i) {
        if (null_indices.count(static_cast<int32_t>(i)) > 0) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->AppendNull());
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(test_data[i]));
        }
    }
    std::shared_ptr<arrow::Array> arrow_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Finish(&arrow_array));
    return CreateReaderFromArray(arrow_type, arrow_array, options, serialized_bytes_out);
}

Result<std::shared_ptr<RangeBitmapFileIndexReader>> RangeBitmapFileIndexTest::CreateReaderFromArray(
    const std::shared_ptr<arrow::DataType>& arrow_type, const std::shared_ptr<arrow::Array>& array,
    const std::map<std::string, std::string>& options,
    PAIMON_UNIQUE_PTR<Bytes>* serialized_bytes_out) {
    // Wrap in StructArray (single field) as required by RangeBitmapFileIndexWriter
    auto field = arrow::field("test_field", arrow_type);
    arrow::FieldVector fields = {field};
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> struct_array,
                                      arrow::StructArray::Make({array}, fields));
    auto c_array = std::make_unique<::ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
    // Create writer
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RangeBitmapFileIndexWriter> writer,
                           RangeBitmapFileIndexWriter::Create(field, options, pool_));
    // Add the batch
    PAIMON_RETURN_NOT_OK(writer->AddBatch(c_array.get()));
    // Get serialized payload
    PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> serialized_bytes, writer->SerializedBytes());
    if (!serialized_bytes || serialized_bytes->size() == 0) {
        return Status::Invalid("Serialized bytes is empty");
    }
    *serialized_bytes_out = std::move(serialized_bytes);
    const auto input_stream = std::make_shared<ByteArrayInputStream>(
        (*serialized_bytes_out)->data(), (*serialized_bytes_out)->size());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RangeBitmapFileIndexReader> reader,
                           RangeBitmapFileIndexReader::Create(
                               arrow_type, 0, static_cast<int32_t>((*serialized_bytes_out)->size()),
                               input_stream, pool_));
    return reader;
}

Result<std::shared_ptr<RangeBitmapFileIndexReader>> RangeBitmapFileIndexTest::CreateReaderFromJson(
    const std::shared_ptr<arrow::DataType>& arrow_type, const std::string& json,
    const std::map<std::string, std::string>& options,
    PAIMON_UNIQUE_PTR<Bytes>* serialized_bytes_out) {
    const auto field = arrow::field("test_field", arrow_type);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> array,
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), json));
    const auto struct_array = checked_pointer_cast<arrow::StructArray>(array);
    return CreateReaderFromArray(arrow_type, struct_array->field(0), options, serialized_bytes_out);
}

// Test with all NULL values
TEST_F(RangeBitmapFileIndexTest, TestAllNullValues) {
    constexpr int32_t num_rows = 10;
    std::vector<int32_t> test_data(num_rows, 0);  // placeholders, all will be NULL
    std::set<int32_t> null_indices;
    for (int32_t i = 0; i < num_rows; ++i) {
        null_indices.insert(i);
    }
    const auto& arrow_type = arrow::int32();
    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int32Builder, int32_t>(
                                          arrow_type, test_data, null_indices, &serialized_bytes)));

    // Test IsNull - should return all positions
    std::vector<int32_t> all_positions(num_rows);
    std::iota(all_positions.begin(), all_positions.end(), 0);
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, all_positions);

    // Test IsNotNull - should return empty
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, {});

    // Test other queries with all NULL data - should return empty
    ASSERT_OK_AND_ASSIGN(auto eq_result, reader->VisitEqual(Literal(FieldType::INT, 42)));
    CheckResult(eq_result, {});

    ASSERT_OK_AND_ASSIGN(auto gt_result, reader->VisitGreaterThan(Literal(FieldType::INT, 0)));
    CheckResult(gt_result, {});

    ASSERT_OK_AND_ASSIGN(auto lt_result, reader->VisitLessThan(Literal(FieldType::INT, 100)));
    CheckResult(lt_result, {});
}

// Test with empty data - no rows at all
TEST_F(RangeBitmapFileIndexTest, TestEmptyRangeBitmap) {
    std::vector<int32_t> test_data;  // empty
    const auto& arrow_type = arrow::int32();
    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int32Builder, int32_t>(
                                          arrow_type, test_data, &serialized_bytes)));

    // All queries should return empty results for empty bitmap
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});

    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, {});

    ASSERT_OK_AND_ASSIGN(auto eq_result, reader->VisitEqual(Literal(FieldType::INT, 42)));
    CheckResult(eq_result, {});

    ASSERT_OK_AND_ASSIGN(auto gt_result, reader->VisitGreaterThan(Literal(FieldType::INT, 0)));
    CheckResult(gt_result, {});

    ASSERT_OK_AND_ASSIGN(auto lt_result, reader->VisitLessThan(Literal(FieldType::INT, 100)));
    CheckResult(lt_result, {});

    ASSERT_OK_AND_ASSIGN(auto gte_result, reader->VisitGreaterOrEqual(Literal(FieldType::INT, 0)));
    CheckResult(gte_result, {});

    ASSERT_OK_AND_ASSIGN(auto lte_result, reader->VisitLessOrEqual(Literal(FieldType::INT, 100)));
    CheckResult(lte_result, {});
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndexMultiChunk) {
    // Use many distinct values and a very small chunk size to force multiple
    // dictionary chunks when writing the range bitmap index.
    std::vector<int32_t> test_data(100);
    std::iota(test_data.begin(), test_data.end(), 0);

    const auto& arrow_type = arrow::int32();
    std::map<std::string, std::string> options;
    // Configure a very small chunk size in bytes so that the dictionary must
    // be split into multiple chunks.
    options[RangeBitmapFileIndex::kChunkSize] = "86b";

    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int32Builder, int32_t>(
                                          arrow_type, test_data, options, &serialized_bytes)));
    ASSERT_OK_AND_ASSIGN(auto eq_0_result, reader->VisitEqual(Literal(static_cast<int32_t>(0))));
    CheckResult(eq_0_result, {0});

    ASSERT_OK_AND_ASSIGN(auto eq_50_result, reader->VisitEqual(Literal(static_cast<int32_t>(50))));
    CheckResult(eq_50_result, {50});
    ASSERT_OK_AND_ASSIGN(auto eq_51_result, reader->VisitEqual(Literal(static_cast<int32_t>(51))));
    CheckResult(eq_51_result, {51});
    ASSERT_OK_AND_ASSIGN(auto eq_99_result, reader->VisitEqual(Literal(static_cast<int32_t>(99))));
    CheckResult(eq_99_result, {99});

    ASSERT_OK_AND_ASSIGN(auto gt_49_result,
                         reader->VisitGreaterThan(Literal(static_cast<int32_t>(49))));
    // Positions 50..99
    std::vector<int32_t> expected_gt_49(50);
    std::iota(expected_gt_49.begin(), expected_gt_49.end(), 50);
    CheckResult(gt_49_result, expected_gt_49);

    ASSERT_OK_AND_ASSIGN(auto lt_10_result,
                         reader->VisitLessThan(Literal(static_cast<int32_t>(10))));
    // Positions 0..9
    std::vector<int32_t> expected_lt_10(10);
    std::iota(expected_lt_10.begin(), expected_lt_10.end(), 0);
    CheckResult(lt_10_result, expected_lt_10);

    // is_not_null should cover all rows.
    std::vector<int32_t> all_positions(100);
    std::iota(all_positions.begin(), all_positions.end(), 0);
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

// Test that chunk size larger than 2GB is rejected during serialization
TEST_F(RangeBitmapFileIndexTest, TestChunkSizeTooLarge) {
    std::vector<int32_t> test_data = {10, 20, 30};
    const auto& arrow_type = arrow::int32();
    // Configure a chunk size larger than 2GB (INT32_MAX)
    std::map<std::string, std::string> options;
    options[RangeBitmapFileIndex::kChunkSize] = "3gb";

    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    auto reader_result = CreateReaderForTest<arrow::Int32Builder, int32_t>(
        arrow_type, test_data, options, &serialized_bytes);

    // Expect failure due to chunk size exceeding 2GB limit
    ASSERT_NOK_WITH_MSG(reader_result.status(), "Chunk size cannot be larger than 2GB");
}

// test data mixed with NULLs
TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndexBigInt) {
    // Data: 10, NULL, 10, 30, NULL, 40, 50 (NULLs at positions 1 and 4)
    std::vector<int64_t> test_data = {10, 20, 10, 30, 20, 40, 50};  // 20 at pos 1,4 will be NULL
    std::set<int32_t> null_indices = {1, 4};
    const auto& arrow_type = arrow::int64();
    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int64Builder, int64_t>(
                                          arrow_type, test_data, null_indices, &serialized_bytes)));

    // Test equality queries (NULL positions excluded)
    ASSERT_OK_AND_ASSIGN(auto eq_10_result, reader->VisitEqual(Literal(static_cast<int64_t>(10))));
    CheckResult(eq_10_result, {0, 2});  // positions 0 and 2 have value 10
    ASSERT_OK_AND_ASSIGN(auto eq_20_result, reader->VisitEqual(Literal(static_cast<int64_t>(20))));
    CheckResult(eq_20_result, {});  // no value 20 (positions 1 and 4 are NULL)
    ASSERT_OK_AND_ASSIGN(auto eq_30_result, reader->VisitEqual(Literal(static_cast<int64_t>(30))));
    CheckResult(eq_30_result, {3});  // position 3 has value 30
    ASSERT_OK_AND_ASSIGN(auto eq_40_result, reader->VisitEqual(Literal(static_cast<int64_t>(40))));
    CheckResult(eq_40_result, {5});  // position 5 has value 40
    ASSERT_OK_AND_ASSIGN(auto eq_50_result, reader->VisitEqual(Literal(static_cast<int64_t>(50))));
    CheckResult(eq_50_result, {6});  // position 6 has value 50

    // Test range queries (NULL positions excluded)
    ASSERT_OK_AND_ASSIGN(auto gt_25_result,
                         reader->VisitGreaterThan(Literal(static_cast<int64_t>(25))));
    CheckResult(gt_25_result, {3, 5, 6});  // values > 25: 30, 40, 50

    ASSERT_OK_AND_ASSIGN(auto lt_35_result,
                         reader->VisitLessThan(Literal(static_cast<int64_t>(35))));
    CheckResult(lt_35_result, {0, 2, 3});  // values < 35: 10, 10, 30 (NULLs excluded)
    ASSERT_OK_AND_ASSIGN(auto gte_20_result,
                         reader->VisitGreaterOrEqual(Literal(static_cast<int64_t>(20))));
    CheckResult(gte_20_result, {3, 5, 6});  // values >= 20: 30, 40, 50 (NULLs excluded)
    ASSERT_OK_AND_ASSIGN(auto lte_40_result,
                         reader->VisitLessOrEqual(Literal(static_cast<int64_t>(40))));
    CheckResult(lte_40_result, {0, 2, 3, 5});  // values <= 40: 10, 10, 30, 40 (NULLs excluded)

    // Test IN queries (NULL positions excluded)
    std::vector<Literal> in_values = {Literal(static_cast<int64_t>(10)),
                                      Literal(static_cast<int64_t>(30))};
    ASSERT_OK_AND_ASSIGN(auto in_result, reader->VisitIn(in_values));
    CheckResult(in_result, {0, 2, 3});  // positions with values 10 or 30
    ASSERT_OK_AND_ASSIGN(auto not_in_result, reader->VisitNotIn(in_values));
    CheckResult(not_in_result, {5, 6});  // positions with values NOT 10 or 30 (NULLs excluded)
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {1, 4});  // positions 1 and 4 are NULL
    std::vector<int32_t> not_null_positions = {0, 2, 3, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, not_null_positions);  // non-NULL positions
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndexInt) {
    std::vector<int32_t> test_data = {10, 20, 10, 30, 20, 40, 50};
    const auto& arrow_type = arrow::int32();
    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int32Builder, int32_t>(
                                          arrow_type, test_data, &serialized_bytes)));

    // Test equality queries
    ASSERT_OK_AND_ASSIGN(auto eq_10_result, reader->VisitEqual(Literal(static_cast<int32_t>(10))));
    CheckResult(eq_10_result, {0, 2});
    ASSERT_OK_AND_ASSIGN(auto eq_20_result, reader->VisitEqual(Literal(static_cast<int32_t>(20))));
    CheckResult(eq_20_result, {1, 4});
    ASSERT_OK_AND_ASSIGN(auto eq_30_result, reader->VisitEqual(Literal(static_cast<int32_t>(30))));
    CheckResult(eq_30_result, {3});

    // Test range queries
    ASSERT_OK_AND_ASSIGN(auto gt_25_result,
                         reader->VisitGreaterThan(Literal(static_cast<int32_t>(25))));
    CheckResult(gt_25_result, {3, 5, 6});  // values > 25: 30, 40, 50
    ASSERT_OK_AND_ASSIGN(auto lt_35_result,
                         reader->VisitLessThan(Literal(static_cast<int32_t>(35))));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});
    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
    ASSERT_OK_AND_ASSIGN(auto gte_20_result,
                         reader->VisitGreaterOrEqual(Literal(static_cast<int32_t>(20))));
    CheckResult(gte_20_result, {1, 3, 4, 5, 6});
    ASSERT_OK_AND_ASSIGN(auto lte_40_result,
                         reader->VisitLessOrEqual(Literal(static_cast<int32_t>(40))));
    CheckResult(lte_40_result, {0, 1, 2, 3, 4, 5});

    // Test empty result cases for INT values that don't exist
    ASSERT_OK_AND_ASSIGN(auto eq_nonexistent_int_result,
                         reader->VisitEqual(Literal(static_cast<int32_t>(25))));
    CheckResult(eq_nonexistent_int_result, {});  // 25 doesn't exist in data {10,20,30,40,50}

    ASSERT_OK_AND_ASSIGN(auto eq_out_of_range_high_int_result,
                         reader->VisitEqual(Literal(static_cast<int32_t>(100))));
    CheckResult(eq_out_of_range_high_int_result, {});  // Value above maximum (50)

    ASSERT_OK_AND_ASSIGN(auto eq_out_of_range_low_int_result,
                         reader->VisitEqual(Literal(static_cast<int32_t>(5))));
    CheckResult(eq_out_of_range_low_int_result, {});  // Value below minimum (10)

    // Test NotEqual operations
    ASSERT_OK_AND_ASSIGN(auto ne_10_result,
                         reader->VisitNotEqual(Literal(static_cast<int32_t>(10))));
    CheckResult(ne_10_result, {1, 3, 4, 5, 6});  // All positions except {0, 2} where 10 appears

    ASSERT_OK_AND_ASSIGN(auto ne_nonexistent_result,
                         reader->VisitNotEqual(Literal(static_cast<int32_t>(99))));
    CheckResult(ne_nonexistent_result, {0, 1, 2, 3, 4, 5, 6});  // All positions (non-empty result)

    // Test NotIn operations
    ASSERT_OK_AND_ASSIGN(auto not_in_single_result,
                         reader->VisitNotIn({Literal(static_cast<int32_t>(10))}));
    CheckResult(not_in_single_result, {1, 3, 4, 5, 6});  // All positions except where 10 appears

    ASSERT_OK_AND_ASSIGN(
        auto not_in_multiple_result,
        reader->VisitNotIn({Literal(static_cast<int32_t>(10)), Literal(static_cast<int32_t>(20))}));
    CheckResult(not_in_multiple_result, {3, 5, 6});  // Positions not containing 10 or 20

    ASSERT_OK_AND_ASSIGN(auto not_in_nonexistent_result,
                         reader->VisitNotIn({Literal(static_cast<int32_t>(99))}));
    CheckResult(not_in_nonexistent_result,
                {0, 1, 2, 3, 4, 5, 6});  // All positions (non-empty result)

    // Test NotIn with empty result - all values are NOT IN the complete set
    std::vector<Literal> all_values = {
        Literal(static_cast<int32_t>(10)), Literal(static_cast<int32_t>(20)),
        Literal(static_cast<int32_t>(30)), Literal(static_cast<int32_t>(40)),
        Literal(static_cast<int32_t>(50))};
    ASSERT_OK_AND_ASSIGN(auto not_in_all_result, reader->VisitNotIn(all_values));
    CheckResult(not_in_all_result,
                {});  // Empty result - no positions left when excluding all existing values
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndexSmallInt) {
    std::vector<int16_t> test_data = {10, 20, 10, 30, 20, 40, 50};
    const auto& arrow_type = arrow::int16();
    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int16Builder, int16_t>(
                                          arrow_type, test_data, &serialized_bytes)));
    ASSERT_OK_AND_ASSIGN(auto eq_10_result, reader->VisitEqual(Literal(static_cast<int16_t>(10))));
    CheckResult(eq_10_result, {0, 2});
    ASSERT_OK_AND_ASSIGN(auto eq_20_result, reader->VisitEqual(Literal(static_cast<int16_t>(20))));
    CheckResult(eq_20_result, {1, 4});
    ASSERT_OK_AND_ASSIGN(auto eq_30_result, reader->VisitEqual(Literal(static_cast<int16_t>(30))));
    CheckResult(eq_30_result, {3});
    ASSERT_OK_AND_ASSIGN(auto gt_25_result,
                         reader->VisitGreaterThan(Literal(static_cast<int16_t>(25))));
    CheckResult(gt_25_result, {3, 5, 6});  // values > 25: 30, 40, 50
    ASSERT_OK_AND_ASSIGN(auto lt_35_result,
                         reader->VisitLessThan(Literal(static_cast<int16_t>(35))));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});
    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndexTinyInt) {
    std::vector<int8_t> test_data = {10, 20, 10, 30, 20, 40, 50};
    const auto& arrow_type = arrow::int8();
    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int8Builder, int8_t>(
                                          arrow_type, test_data, &serialized_bytes)));
    ASSERT_OK_AND_ASSIGN(auto eq_10_result, reader->VisitEqual(Literal(static_cast<int8_t>(10))));
    CheckResult(eq_10_result, {0, 2});
    ASSERT_OK_AND_ASSIGN(auto eq_20_result, reader->VisitEqual(Literal(static_cast<int8_t>(20))));
    CheckResult(eq_20_result, {1, 4});
    ASSERT_OK_AND_ASSIGN(auto eq_30_result, reader->VisitEqual(Literal(static_cast<int8_t>(30))));
    CheckResult(eq_30_result, {3});
    ASSERT_OK_AND_ASSIGN(auto gt_25_result,
                         reader->VisitGreaterThan(Literal(static_cast<int8_t>(25))));
    CheckResult(gt_25_result, {3, 5, 6});  // values > 25: 30, 40, 50
    ASSERT_OK_AND_ASSIGN(auto lt_35_result,
                         reader->VisitLessThan(Literal(static_cast<int8_t>(35))));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});
    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndexBoolean) {
    std::vector<bool> test_data = {true, false, true, true, false, true, false};
    const auto& arrow_type = arrow::boolean();
    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::BooleanBuilder, bool>(
                                          arrow_type, test_data, &serialized_bytes)));
    ASSERT_OK_AND_ASSIGN(auto eq_true_result, reader->VisitEqual(Literal(true)));
    CheckResult(eq_true_result, {0, 2, 3, 5});  // positions with value true
    ASSERT_OK_AND_ASSIGN(auto eq_false_result, reader->VisitEqual(Literal(false)));
    CheckResult(eq_false_result, {1, 4, 6});  // positions with value false
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});
    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndexFloat) {
    std::vector<float> test_data = {10.5f, 20.3f, 10.5f, 30.7f, 20.3f, 40.1f, 50.9f};
    const auto& arrow_type = arrow::float32();
    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::FloatBuilder, float>(
                                          arrow_type, test_data, &serialized_bytes)));
    ASSERT_OK_AND_ASSIGN(auto eq_10_5_result, reader->VisitEqual(Literal(10.5f)));
    CheckResult(eq_10_5_result, {0, 2});  // positions with value 10.5
    ASSERT_OK_AND_ASSIGN(auto eq_20_3_result, reader->VisitEqual(Literal(20.3f)));
    CheckResult(eq_20_3_result, {1, 4});  // positions with value 20.3
    ASSERT_OK_AND_ASSIGN(auto eq_30_7_result, reader->VisitEqual(Literal(30.7f)));
    CheckResult(eq_30_7_result, {3});  // position with value 30.7
    ASSERT_OK_AND_ASSIGN(auto gt_24_9_result, reader->VisitGreaterThan(Literal(24.9f)));
    CheckResult(gt_24_9_result, {3, 5, 6});  // values > 25.0: 30.7, 40.1, 50.9
    ASSERT_OK_AND_ASSIGN(auto lt_35_result, reader->VisitLessThan(Literal(35.0f)));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35.0

    // Test empty result cases for float values that don't exist
    ASSERT_OK_AND_ASSIGN(auto eq_nonexistent_float_result, reader->VisitEqual(Literal(25.0f)));
    CheckResult(eq_nonexistent_float_result, {});  // 25.0 doesn't exist in data

    ASSERT_OK_AND_ASSIGN(auto eq_out_of_range_high_result, reader->VisitEqual(Literal(100.0f)));
    CheckResult(eq_out_of_range_high_result, {});  // Value above maximum

    ASSERT_OK_AND_ASSIGN(auto eq_out_of_range_low_result, reader->VisitEqual(Literal(5.0f)));
    CheckResult(eq_out_of_range_low_result, {});  // Value below minimum

    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});
    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndexDouble) {
    std::vector<double> test_data = {10.5, 20.3, 10.5, 30.7, 20.3, 40.1, 50.9};
    const auto& arrow_type = arrow::float64();
    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::DoubleBuilder, double>(
                                          arrow_type, test_data, &serialized_bytes)));
    ASSERT_OK_AND_ASSIGN(auto eq_10_5_result, reader->VisitEqual(Literal(10.5)));
    CheckResult(eq_10_5_result, {0, 2});  // positions with value 10.5
    ASSERT_OK_AND_ASSIGN(auto eq_20_3_result, reader->VisitEqual(Literal(20.3)));
    CheckResult(eq_20_3_result, {1, 4});  // positions with value 20.3
    ASSERT_OK_AND_ASSIGN(auto eq_30_7_result, reader->VisitEqual(Literal(30.7)));
    CheckResult(eq_30_7_result, {3});  // position with value 30.7
    ASSERT_OK_AND_ASSIGN(auto gt_24_9_result, reader->VisitGreaterThan(Literal(24.9)));
    CheckResult(gt_24_9_result, {3, 5, 6});  // values > 25.0: 30.7, 40.1, 50.9
    ASSERT_OK_AND_ASSIGN(auto lt_35_result, reader->VisitLessThan(Literal(35.0)));
    CheckResult(lt_35_result, {0, 1, 2, 3, 4});  // values < 35.0
    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});
    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestFloatingPointSpecialValues) {
    const std::vector<int32_t> nan_positions = {0, 1, 2, 5};
    const std::vector<int32_t> negative_zero_positions = {3, 6};
    const std::vector<int32_t> positive_zero_positions = {4, 7};
    const std::vector<int32_t> non_nan_positions = {3, 4, 6, 7, 8, 9, 10, 11};
    const std::vector<int32_t> less_than_positive_zero_positions = {3, 6, 8, 10};
    const std::vector<int32_t> nan_and_negative_zero_positions = {0, 1, 2, 3, 5, 6};
    const std::vector<int32_t> non_nan_and_non_negative_zero_positions = {4, 7, 8, 9, 10, 11};

    const auto check_reader = [&](const std::shared_ptr<RangeBitmapFileIndexReader>& reader,
                                  const std::vector<Literal>& nan_literals,
                                  const Literal& negative_zero, const Literal& positive_zero,
                                  const Literal& positive_infinity) {
        for (const Literal& nan_literal : nan_literals) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> result,
                                 reader->VisitEqual(nan_literal));
            CheckResult(result, nan_positions);
        }

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> negative_zero_result,
                             reader->VisitEqual(negative_zero));
        CheckResult(negative_zero_result, negative_zero_positions);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> positive_zero_result,
                             reader->VisitEqual(positive_zero));
        CheckResult(positive_zero_result, positive_zero_positions);

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> less_than_nan_result,
                             reader->VisitLessThan(nan_literals.front()));
        CheckResult(less_than_nan_result, non_nan_positions);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> greater_than_infinity_result,
                             reader->VisitGreaterThan(positive_infinity));
        CheckResult(greater_than_infinity_result, nan_positions);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> less_than_positive_zero_result,
                             reader->VisitLessThan(positive_zero));
        CheckResult(less_than_positive_zero_result, less_than_positive_zero_positions);

        const std::vector<Literal> nan_and_negative_zero = {nan_literals[1], negative_zero};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> in_result,
                             reader->VisitIn(nan_and_negative_zero));
        CheckResult(in_result, nan_and_negative_zero_positions);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexResult> not_in_result,
                             reader->VisitNotIn(nan_and_negative_zero));
        CheckResult(not_in_result, non_nan_and_non_negative_zero_positions);
    };

    const auto float_nan = FloatingPointFromBits<float>(kCanonicalFloatNaNBits);
    const auto float_positive_payload_nan = FloatingPointFromBits<float>(uint32_t{0x7fc12345});
    const auto float_negative_payload_nan = FloatingPointFromBits<float>(uint32_t{0xffc54321});
    const std::vector<float> float_values = {
        float_nan,
        float_positive_payload_nan,
        float_negative_payload_nan,
        -0.0f,
        +0.0f,
        float_negative_payload_nan,
        -0.0f,
        +0.0f,
        -std::numeric_limits<float>::infinity(),
        std::numeric_limits<float>::infinity(),
        -1.0f,
        +1.0f,
    };
    PAIMON_UNIQUE_PTR<Bytes> float_serialized_bytes;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RangeBitmapFileIndexReader> float_reader,
                         (CreateReaderForTest<arrow::FloatBuilder, float>(
                             arrow::float32(), float_values, &float_serialized_bytes)));
    check_reader(float_reader,
                 {Literal(float_nan), Literal(float_positive_payload_nan),
                  Literal(float_negative_payload_nan)},
                 Literal(-0.0f), Literal(+0.0f), Literal(std::numeric_limits<float>::infinity()));

    const auto double_nan = FloatingPointFromBits<double>(kCanonicalDoubleNaNBits);
    const auto double_positive_payload_nan =
        FloatingPointFromBits<double>(uint64_t{0x7ff8123456789abc});
    const auto double_negative_payload_nan =
        FloatingPointFromBits<double>(uint64_t{0xfff8abcdef012345});
    const std::vector<double> double_values = {
        double_nan,
        double_positive_payload_nan,
        double_negative_payload_nan,
        -0.0,
        +0.0,
        double_negative_payload_nan,
        -0.0,
        +0.0,
        -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::infinity(),
        -1.0,
        +1.0,
    };
    PAIMON_UNIQUE_PTR<Bytes> double_serialized_bytes;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RangeBitmapFileIndexReader> double_reader,
                         (CreateReaderForTest<arrow::DoubleBuilder, double>(
                             arrow::float64(), double_values, &double_serialized_bytes)));
    check_reader(double_reader,
                 {Literal(double_nan), Literal(double_positive_payload_nan),
                  Literal(double_negative_payload_nan)},
                 Literal(-0.0), Literal(+0.0), Literal(std::numeric_limits<double>::infinity()));
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadRangeBitmapIndexDate) {
    std::vector<int32_t> test_data = {42432, 24649, 42432, 38001, 24649, 50000, 12000};
    const auto& arrow_type = arrow::date32();
    PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
    ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Date32Builder, int32_t>(
                                          arrow_type, test_data, &serialized_bytes)));
    ASSERT_OK_AND_ASSIGN(auto eq_42432_result, reader->VisitEqual(Literal(FieldType::DATE, 42432)));
    CheckResult(eq_42432_result, {0, 2});
    ASSERT_OK_AND_ASSIGN(auto eq_24649_result, reader->VisitEqual(Literal(FieldType::DATE, 24649)));
    CheckResult(eq_24649_result, {1, 4});
    ASSERT_OK_AND_ASSIGN(auto eq_38001_result, reader->VisitEqual(Literal(FieldType::DATE, 38001)));
    CheckResult(eq_38001_result, {3});
    ASSERT_OK_AND_ASSIGN(auto gt_result,
                         reader->VisitGreaterOrEqual(Literal(FieldType::DATE, 30000)));
    CheckResult(gt_result, {0, 2, 3, 5});  // 42432, 38001, 50000

    ASSERT_OK_AND_ASSIGN(auto lt_result, reader->VisitLessThan(Literal(FieldType::DATE, 40000)));
    CheckResult(lt_result, {1, 3, 4, 6});  // 24649, 38001, 12000

    // Test empty result cases - values that don't exist in the data
    ASSERT_OK_AND_ASSIGN(auto eq_nonexistent_low_result,
                         reader->VisitEqual(Literal(FieldType::DATE, 47432)));
    CheckResult(eq_nonexistent_low_result, {});

    ASSERT_OK_AND_ASSIGN(auto eq_nonexistent_mid_result,
                         reader->VisitEqual(Literal(FieldType::DATE, 30000)));
    CheckResult(eq_nonexistent_mid_result, {});  // Value in middle range but doesn't exist

    ASSERT_OK_AND_ASSIGN(auto eq_nonexistent_high_result,
                         reader->VisitEqual(Literal(FieldType::DATE, 60000)));
    CheckResult(eq_nonexistent_high_result, {});  // Value above maximum (50000)

    // Test range queries that should return empty results
    ASSERT_OK_AND_ASSIGN(auto gt_all_result,
                         reader->VisitGreaterOrEqual(Literal(FieldType::DATE, 60000)));
    CheckResult(gt_all_result, {});  // Greater than maximum should return empty

    ASSERT_OK_AND_ASSIGN(auto lt_all_result,
                         reader->VisitLessThan(Literal(FieldType::DATE, 10000)));
    CheckResult(lt_all_result, {});  // Less than minimum should return empty

    ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
    CheckResult(is_null_result, {});
    std::vector<int32_t> all_positions = {0, 1, 2, 3, 4, 5, 6};
    ASSERT_OK_AND_ASSIGN(auto is_not_null_result, reader->VisitIsNotNull());
    CheckResult(is_not_null_result, all_positions);
}

TEST_F(RangeBitmapFileIndexTest, TestWriteAndReadStringDecimalAndTimestamp) {
    {
        const auto type = arrow::utf8();
        PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            CreateReaderFromJson(type, R"([["pear"], ["apple"], [null], ["banana"], ["apple"]])",
                                 {{"chunk-size", "12b"}}, &serialized_bytes));
        const Literal apple(FieldType::STRING, "apple", 5);
        const Literal banana(FieldType::STRING, "banana", 6);
        CheckResult(reader->VisitEqual(apple).value(), {1, 4});
        CheckResult(reader->VisitGreaterOrEqual(banana).value(), {0, 3});
        CheckResult(reader->VisitIsNull().value(), {2});
    }
    {
        const auto type = arrow::decimal128(10, 2);
        PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            CreateReaderFromJson(type, R"([["1.00"], ["2.50"], [null], ["-1.25"], ["2.50"]])", {},
                                 &serialized_bytes));
        CheckResult(reader->VisitEqual(Literal(Decimal(10, 2, 250))).value(), {1, 4});
        CheckResult(reader->VisitLessThan(Literal(Decimal(10, 2, 0))).value(), {3});
        CheckResult(reader->VisitIsNull().value(), {2});
    }
    {
        const auto type = arrow::timestamp(arrow::TimeUnit::MICRO);
        PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            CreateReaderFromJson(type, R"([[1000001], [2000002], [null], [-1000001], [2000002]])",
                                 {}, &serialized_bytes));
        CheckResult(reader->VisitEqual(Literal(Timestamp(2000, 2000))).value(), {1, 4});
        CheckResult(reader->VisitLessThan(Literal(Timestamp(0, 0))).value(), {3});
        CheckResult(reader->VisitIsNull().value(), {2});
    }

    ASSERT_NOK_WITH_MSG(
        RangeBitmapFileIndexWriter::Create(arrow::field("f0", arrow::decimal128(19, 2)), {}, pool_),
        "DECIMAL with precision in [1, 18]");
    ASSERT_NOK_WITH_MSG(RangeBitmapFileIndexWriter::Create(
                            arrow::field("f0", arrow::timestamp(arrow::TimeUnit::NANO)), {}, pool_),
                        "TIMESTAMP with precision in [0, 6]");
}

TEST_F(RangeBitmapFileIndexTest, TestRangeBitmapEdgeCases) {
    // Scope 1: All values identical
    {
        std::vector<int32_t> same_values = {42, 42, 42, 42, 42};
        const auto& arrow_type = arrow::int32();
        PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
        ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int32Builder, int32_t>(
                                              arrow_type, same_values, &serialized_bytes)));

        ASSERT_OK_AND_ASSIGN(auto eq_42_result,
                             reader->VisitEqual(Literal(static_cast<int32_t>(42))));
        CheckResult(eq_42_result, {0, 1, 2, 3, 4});

        ASSERT_OK_AND_ASSIGN(auto lt_100_result,
                             reader->VisitLessThan(Literal(static_cast<int32_t>(100))));
        CheckResult(lt_100_result, {0, 1, 2, 3, 4});

        ASSERT_OK_AND_ASSIGN(auto gt_10_result,
                             reader->VisitGreaterThan(Literal(static_cast<int32_t>(10))));
        CheckResult(gt_10_result, {0, 1, 2, 3, 4});

        ASSERT_OK_AND_ASSIGN(auto gt_42_result,
                             reader->VisitGreaterThan(Literal(static_cast<int32_t>(42))));
        CheckResult(gt_42_result, {});

        ASSERT_OK_AND_ASSIGN(auto gt_100_result,
                             reader->VisitGreaterThan(Literal(static_cast<int32_t>(100))));
        CheckResult(gt_100_result, {});

        ASSERT_OK_AND_ASSIGN(auto neq_42_result,
                             reader->VisitNotEqual(Literal(static_cast<int32_t>(42))));
        CheckResult(neq_42_result, {});
    }

    // Scope 2: Empty data (cardinality == 0)
    {
        std::vector<int32_t> empty_data;
        const auto& arrow_type = arrow::int32();
        PAIMON_UNIQUE_PTR<Bytes> serialized_bytes;
        ASSERT_OK_AND_ASSIGN(auto reader, (CreateReaderForTest<arrow::Int32Builder, int32_t>(
                                              arrow_type, empty_data, &serialized_bytes)));

        // Neq with cardinality <= 0
        ASSERT_OK_AND_ASSIGN(auto neq_result,
                             reader->VisitNotEqual(Literal(static_cast<int32_t>(1))));
        CheckResult(neq_result, {});

        // In with cardinality <= 0
        std::vector<Literal> in_values = {Literal(static_cast<int32_t>(1)),
                                          Literal(static_cast<int32_t>(2))};
        ASSERT_OK_AND_ASSIGN(auto in_result, reader->VisitIn(in_values));
        CheckResult(in_result, {});

        // NotIn with cardinality <= 0
        ASSERT_OK_AND_ASSIGN(auto not_in_result, reader->VisitNotIn(in_values));
        CheckResult(not_in_result, {});
    }
}

}  // namespace paimon::test
