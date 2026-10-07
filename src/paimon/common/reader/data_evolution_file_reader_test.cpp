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

#include "paimon/common/reader/data_evolution_file_reader.h"

#include <map>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "arrow/util/range.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class DataEvolutionFileReaderTest : public ::testing::Test,
                                    public ::testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void TearDown() override {
        pool_.reset();
    }

    void CheckResult(const arrow::ArrayVector& src_array_vec,
                     const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::vector<int32_t>& reader_offsets,
                     const std::vector<int32_t>& field_offsets,
                     const std::shared_ptr<arrow::Array>& expected_array,
                     const std::optional<RoaringBitmap32>& selection_bitmap = std::nullopt) const {
        for (auto batch_size : arrow::internal::Iota(1, 10)) {
            int32_t total_row_count = 0;
            std::vector<std::unique_ptr<BatchReader>> readers;
            for (const auto& array : src_array_vec) {
                if (array == nullptr) {
                    // simulate no fields read from current reader
                    readers.push_back(nullptr);
                    continue;
                }
                total_row_count += array->length();
                std::unique_ptr<MockFileBatchReader> file_batch_reader;
                if (selection_bitmap) {
                    file_batch_reader = std::make_unique<MockFileBatchReader>(
                        array, array->type(), selection_bitmap.value(), batch_size);
                } else {
                    file_batch_reader =
                        std::make_unique<MockFileBatchReader>(array, array->type(), batch_size);
                }
                auto enable_randomize_batch_size = GetParam();
                file_batch_reader->EnableRandomizeBatchSize(enable_randomize_batch_size);
                readers.push_back(std::move(file_batch_reader));
            }
            ASSERT_OK_AND_ASSIGN(auto data_evolution_file_reader,
                                 DataEvolutionFileReader::Create(
                                     std::move(readers), read_schema, batch_size, reader_offsets,
                                     field_offsets, GetArrowPool(pool_)));
            // check metrics, data_evolution_file_reader collects all row of each
            // MockFileBatchReader
            auto metrics = data_evolution_file_reader->GetReaderMetrics();
            ASSERT_EQ(metrics->ToString(),
                      "{\"mock.number.of.rows\":" + std::to_string(total_row_count) + "}");

            // check result array
            ASSERT_OK_AND_ASSIGN(auto result_array,
                                 paimon::test::ReadResultCollector::CollectResult(
                                     std::move(data_evolution_file_reader)));
            auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
            ASSERT_TRUE(result_array->Equals(expected_chunk_array));
        }
    }

    Result<std::shared_ptr<arrow::Array>> ReadNextOutput(DataEvolutionFileReader* reader) const {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                               reader->NextBatchWithBitmap());
        if (BatchReader::IsEofBatch(batch_with_bitmap)) {
            return std::shared_ptr<arrow::Array>();
        }
        auto& [batch, bitmap] = batch_with_bitmap;
        auto& [c_array, c_schema] = batch;
        if (bitmap.Cardinality() != c_array->length) {
            return Status::Invalid("data evolution output bitmap should select every row");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        return array;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(DataEvolutionFileReaderTest, TestInvalid) {
    {
        arrow::FieldVector read_fields;
        auto read_schema = arrow::schema(read_fields);
        ASSERT_NOK_WITH_MSG(DataEvolutionFileReader::Create({}, read_schema, /*read_batch_size=*/10,
                                                            {}, {}, GetArrowPool(pool_)),
                            "read schema must not be empty");
    }
    {
        arrow::FieldVector read_fields = {
            arrow::field("f0", arrow::int32()),
            arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::utf8()),
            arrow::field("f3", arrow::int32()),
        };
        auto read_schema = arrow::schema(read_fields);
        std::vector<int32_t> reader_offsets = {0, 0, 1};
        std::vector<int32_t> field_offsets = {0, 1, 0};
        ASSERT_NOK_WITH_MSG(
            DataEvolutionFileReader::Create({}, read_schema, /*read_batch_size=*/10, reader_offsets,
                                            field_offsets, GetArrowPool(pool_)),
            "read schema, row offsets and field offsets must have the same size");
    }
    {
        arrow::FieldVector read_fields = {
            arrow::field("f0", arrow::int32()),
            arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::utf8()),
            arrow::field("f3", arrow::int32()),
        };
        auto read_schema = arrow::schema(read_fields);
        std::vector<int32_t> reader_offsets = {0, 0, 1, 1};
        std::vector<int32_t> field_offsets = {0, 1, 1, 0};
        ASSERT_NOK_WITH_MSG(
            DataEvolutionFileReader::Create({}, read_schema, /*read_batch_size=*/10, reader_offsets,
                                            field_offsets, GetArrowPool(pool_)),
            "readers must not be empty");
    }
    {
        std::vector<std::unique_ptr<BatchReader>> readers;
        readers.push_back(nullptr);

        arrow::FieldVector read_fields = {
            arrow::field("f0", arrow::int32()),
            arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::utf8()),
            arrow::field("f3", arrow::int32()),
        };
        auto read_schema = arrow::schema(read_fields);
        std::vector<int32_t> reader_offsets = {0, 0, 1, 1};
        std::vector<int32_t> field_offsets = {0, 1, 1, 0};
        ASSERT_NOK_WITH_MSG(
            DataEvolutionFileReader::Create(std::move(readers), read_schema, /*read_batch_size=*/10,
                                            reader_offsets, field_offsets, GetArrowPool(pool_)),
            "reader offset is out of range of readers");
    }
}

TEST_F(DataEvolutionFileReaderTest, TestNoActiveReader) {
    auto read_schema = arrow::schema({arrow::field("missing", arrow::int32())});
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.push_back(nullptr);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         DataEvolutionFileReader::Create(
                             std::move(readers), read_schema, /*read_batch_size=*/10,
                             /*reader_offsets=*/{-1}, /*field_offsets=*/{-1}, GetArrowPool(pool_)));
    ASSERT_NOK_WITH_MSG(reader->NextBatchWithBitmap(),
                        "data evolution reader has no active inner reader");
}

TEST_F(DataEvolutionFileReaderTest, TestDifferentInnerBatchSizes) {
    auto f0 = arrow::field("f0", arrow::int32());
    auto f1 = arrow::field("f1", arrow::int32());
    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({f0}), R"([
        [0], [1], [2], [3], [4], [5]
    ])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({f1}), R"([
        [10], [11], [12], [13], [14], [15]
    ])")
                      .ValueOrDie();

    std::vector<std::unique_ptr<BatchReader>> readers;
    auto reader0 = std::make_unique<MockFileBatchReader>(array0, array0->type(),
                                                         /*read_batch_size=*/5);
    reader0->EnableRandomizeBatchSize(false);
    readers.push_back(std::move(reader0));
    auto reader1 = std::make_unique<MockFileBatchReader>(array1, array1->type(),
                                                         /*read_batch_size=*/2);
    reader1->EnableRandomizeBatchSize(false);
    readers.push_back(std::move(reader1));
    ASSERT_OK_AND_ASSIGN(auto reader, DataEvolutionFileReader::Create(
                                          std::move(readers), arrow::schema({f0, f1}),
                                          /*read_batch_size=*/10, /*reader_offsets=*/{0, 1},
                                          /*field_offsets=*/{0, 0}, GetArrowPool(pool_)));

    arrow::ArrayVector batches;
    for (int64_t expected_length : {2, 2, 1, 1}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch, ReadNextOutput(reader.get()));
        ASSERT_NE(batch, nullptr);
        ASSERT_EQ(batch->length(), expected_length);
        ASSERT_EQ(batch->offset(), 0);
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(batch);
        ASSERT_TRUE(struct_array);
        ASSERT_EQ(struct_array->field(0)->offset(), 0);
        ASSERT_EQ(struct_array->field(1)->offset(), 0);
        batches.push_back(std::move(batch));
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> eof, ReadNextOutput(reader.get()));
    ASSERT_EQ(eof, nullptr);
    ASSERT_OK_AND_ASSIGN(eof, ReadNextOutput(reader.get()));
    ASSERT_EQ(eof, nullptr);

    auto expected = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({f0, f1}), R"([
        [0, 10], [1, 11], [2, 12], [3, 13], [4, 14], [5, 15]
    ])")
                        .ValueOrDie();
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(batches)->Equals(
        std::make_shared<arrow::ChunkedArray>(expected)));
}

TEST_F(DataEvolutionFileReaderTest, TestBitmapSkipsWholeBatches) {
    auto f0 = arrow::field("f0", arrow::int32());
    auto f1 = arrow::field("f1", arrow::int32());
    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({f0}), R"([
        [0], [1], [2], [3], [4], [5], [6], [7]
    ])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({f1}), R"([
        [100], [101], [102], [103], [104], [105], [106], [107]
    ])")
                      .ValueOrDie();
    RoaringBitmap32 bitmap = RoaringBitmap32::From({4, 7});

    std::vector<std::unique_ptr<BatchReader>> readers;
    auto reader0 = std::make_unique<MockFileBatchReader>(array0, array0->type(), bitmap,
                                                         /*read_batch_size=*/2);
    reader0->EnableRandomizeBatchSize(false);
    readers.push_back(std::move(reader0));
    auto reader1 = std::make_unique<MockFileBatchReader>(array1, array1->type(), bitmap,
                                                         /*read_batch_size=*/3);
    reader1->EnableRandomizeBatchSize(false);
    readers.push_back(std::move(reader1));
    ASSERT_OK_AND_ASSIGN(auto reader, DataEvolutionFileReader::Create(
                                          std::move(readers), arrow::schema({f0, f1}),
                                          /*read_batch_size=*/10, /*reader_offsets=*/{0, 1},
                                          /*field_offsets=*/{0, 0}, GetArrowPool(pool_)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                         ReadResultCollector::CollectResult(std::move(reader)));

    auto expected = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({f0, f1}), R"([
        [4, 104], [7, 107]
    ])")
                        .ValueOrDie();
    ASSERT_TRUE(actual->Equals(std::make_shared<arrow::ChunkedArray>(expected)));
}

TEST_P(DataEvolutionFileReaderTest, TestSimple) {
    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()),  arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::utf8()),  arrow::field("f5", arrow::int32()),
    };
    auto read_schema = arrow::schema(read_fields);

    std::vector<int32_t> reader_offsets = {0, 2, 0, 1, 2, 1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0};

    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[0], read_fields[2]}), R"([
        [0, "00"],
        [1, "01"],
        [2, "02"],
        [3, "03"],
        [4, "04"],
        [5, "05"]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[5], read_fields[3]}), R"([
        [10, 110],
        [11, 111],
        [12, 112],
        [13, 113],
        [14, 114],
        [15, 115]
])")
                      .ValueOrDie();
    auto array2 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[4]}), R"([
        [20, "20"],
        [21, "21"],
        [22, "22"],
        [23, "23"],
        [24, "24"],
        [25, "25"]
])")
                      .ValueOrDie();

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(read_fields), R"([
        [0, 20, "00", 110, "20", 10],
        [1, 21, "01", 111, "21", 11],
        [2, 22, "02", 112, "22", 12],
        [3, 23, "03", 113, "23", 13],
        [4, 24, "04", 114, "24", 14],
        [5, 25, "05", 115, "25", 15]
])")
            .ValueOrDie();
    CheckResult({array0, array1, array2}, read_schema, reader_offsets, field_offsets,
                expected_array);
}

TEST_P(DataEvolutionFileReaderTest, TestWithNonExistField) {
    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()),        arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()),         arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::utf8()),         arrow::field("f5", arrow::int32()),
        arrow::field("non-field", arrow::int32()),
    };
    auto read_schema = arrow::schema(read_fields);

    std::vector<int32_t> reader_offsets = {0, 2, 0, 1, 2, 1, -1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0, -1};

    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[0], read_fields[2]}), R"([
        [0, "00"],
        [1, "01"],
        [2, "02"],
        [3, "03"],
        [4, "04"],
        [5, "05"]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[5], read_fields[3]}), R"([
        [10, 110],
        [11, 111],
        [12, 112],
        [13, 113],
        [14, 114],
        [15, 115]
])")
                      .ValueOrDie();
    auto array2 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[4]}), R"([
        [20, "20"],
        [21, "21"],
        [22, "22"],
        [23, "23"],
        [24, "24"],
        [25, "25"]
])")
                      .ValueOrDie();

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(read_fields), R"([
        [0, 20, "00", 110, "20", 10, null],
        [1, 21, "01", 111, "21", 11, null],
        [2, 22, "02", 112, "22", 12, null],
        [3, 23, "03", 113, "23", 13, null],
        [4, 24, "04", 114, "24", 14, null],
        [5, 25, "05", 115, "25", 15, null]
])")
            .ValueOrDie();
    CheckResult({array0, array1, array2}, read_schema, reader_offsets, field_offsets,
                expected_array);
}

TEST_P(DataEvolutionFileReaderTest, TestReadFromPartialReaders) {
    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()),  arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::utf8()),  arrow::field("f5", arrow::int32()),
    };
    auto read_schema = arrow::schema(read_fields);
    // simulate reader2 has no field to read
    std::vector<int32_t> reader_offsets = {0, 3, 0, 1, 3, 1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0};

    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[0], read_fields[2]}), R"([
        [0, "00"],
        [1, "01"],
        [2, "02"],
        [3, "03"],
        [4, "04"],
        [5, "05"]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[5], read_fields[3]}), R"([
        [10, 110],
        [11, 111],
        [12, 112],
        [13, 113],
        [14, 114],
        [15, 115]
])")
                      .ValueOrDie();
    auto array3 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[4]}), R"([
        [20, "20"],
        [21, "21"],
        [22, "22"],
        [23, "23"],
        [24, "24"],
        [25, "25"]
])")
                      .ValueOrDie();

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(read_fields), R"([
        [0, 20, "00", 110, "20", 10],
        [1, 21, "01", 111, "21", 11],
        [2, 22, "02", 112, "22", 12],
        [3, 23, "03", 113, "23", 13],
        [4, 24, "04", 114, "24", 14],
        [5, 25, "05", 115, "25", 15]
])")
            .ValueOrDie();

    CheckResult({array0, array1, nullptr, array3}, read_schema, reader_offsets, field_offsets,
                expected_array);
}

TEST_P(DataEvolutionFileReaderTest, TestNestedType) {
    arrow::FieldVector read_fields = {
        arrow::field("f1", arrow::map(arrow::int8(), arrow::int16())),
        arrow::field("f2", arrow::list(arrow::float32())),
        arrow::field("f3", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                           arrow::field("f1", arrow::int64())})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f5", arrow::date32()),
        arrow::field("f6", arrow::decimal128(2, 2))};
    auto read_schema = arrow::schema(read_fields);

    std::vector<int32_t> reader_offsets = {0, 1, 0, 1, 0, 1};
    std::vector<int32_t> field_offsets = {2, 0, 1, 1, 0, 2};

    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[4], read_fields[2], read_fields[0]}), R"([
        [2456, [true, 2], [[0, 0]]],
        [24, [true, 1], [[0, 1]]],
        [2456, [false, 12], [[10, 10]]],
        [245, [false, 2222], [[127, 32767], [-128, -32768]]],
        [24, [true, 2], [[1, 64], [2, 32]]],
        [24, [true, 2], [[11, 64], [12, 32]]]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[3], read_fields[5]}), R"([
        [[0.1, 0.2], "1970-01-01 00:02:03.123123", "0.22"],
        [[0.1, 0.3], "1970-01-01 00:02:03.999999", "0.28"],
        [[1.1, 1.2], "1970-01-01 00:02:03.123123", "0.22"],
        [[1.1, 1.2], "1970-01-01 00:02:03.123123", "0.12"],
        [[2.2, 3.2], "1970-01-01 00:00:00.0", "0.78"],
        [[2.2, 3.2], "1970-01-01 00:00:00.123123", "0.78"]
])")
                      .ValueOrDie();

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(read_fields), R"([
        [[[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999999", 24, "0.28"],
        [[[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123123", 245, "0.12"],
        [[[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [[[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123123", 24, "0.78"]
])")
            .ValueOrDie();
    CheckResult({array0, array1}, read_schema, reader_offsets, field_offsets, expected_array);
}

TEST_P(DataEvolutionFileReaderTest, TestWithBitmap) {
    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()),       arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()),        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::utf8()),        arrow::field("f5", arrow::int32()),
        arrow::field("non-exist", arrow::int32())};
    auto read_schema = arrow::schema(read_fields);

    std::vector<int32_t> reader_offsets = {0, 2, 0, 1, 2, 1, -1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1, 1, 0, -1};

    RoaringBitmap32 selection_bitmap = RoaringBitmap32::From({1, 3, 5});
    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[0], read_fields[2]}), R"([
        [0, "00"],
        [1, "01"],
        [2, "02"],
        [3, "03"],
        [4, "04"],
        [5, "05"]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[5], read_fields[3]}), R"([
        [10, 110],
        [11, 111],
        [12, 112],
        [13, 113],
        [14, 114],
        [15, 115]
])")
                      .ValueOrDie();
    auto array2 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[4]}), R"([
        [20, "20"],
        [21, "21"],
        [22, "22"],
        [23, "23"],
        [24, "24"],
        [25, "25"]
])")
                      .ValueOrDie();

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(read_fields), R"([
        [1, 21, "01", 111, "21", 11, null],
        [3, 23, "03", 113, "23", 13, null],
        [5, 25, "05", 115, "25", 15, null]
])")
            .ValueOrDie();
    CheckResult({array0, array1, array2}, read_schema, reader_offsets, field_offsets,
                expected_array, selection_bitmap);
}

TEST_P(DataEvolutionFileReaderTest, TestSingleReaderRowCountMismatch) {
    arrow::FieldVector read_fields = {
        arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()), arrow::field("f3", arrow::int32())};
    auto read_schema = arrow::schema(read_fields);

    std::vector<int32_t> reader_offsets = {0, 1, 0, 1};
    std::vector<int32_t> field_offsets = {0, 0, 1, 1};

    auto array0 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[0], read_fields[2]}), R"([
        [0, "00"],
        [1, "01"],
        [2, "02"],
        [3, "03"],
        [4, "04"],
        [5, "05"]
])")
                      .ValueOrDie();
    auto array1 = arrow::ipc::internal::json::ArrayFromJSON(
                      arrow::struct_({read_fields[1], read_fields[3]}), R"([
        [10, 110],
        [11, 111],
        [12, 112],
        [13, 113],
        [14, 114]
])")
                      .ValueOrDie();
    std::vector<std::unique_ptr<BatchReader>> readers;
    for (const auto& array : {array0, array1}) {
        auto file_batch_reader =
            std::make_unique<MockFileBatchReader>(array, array->type(), /*read_batch_size=*/10);
        auto enable_randomize_batch_size = GetParam();
        file_batch_reader->EnableRandomizeBatchSize(enable_randomize_batch_size);
        readers.push_back(std::move(file_batch_reader));
    }
    ASSERT_OK_AND_ASSIGN(
        auto data_evolution_file_reader,
        DataEvolutionFileReader::Create(std::move(readers), read_schema, /*read_batch_size=*/10,
                                        reader_offsets, field_offsets, GetArrowPool(pool_)));
    // array0 has 6 rows but array1 only has 5 rows
    ASSERT_NOK_WITH_MSG(
        paimon::test::ReadResultCollector::CollectResult(std::move(data_evolution_file_reader)),
        "array for single reader length mismatch others");
}

INSTANTIATE_TEST_SUITE_P(EnableRandomizeBatchSize, DataEvolutionFileReaderTest,
                         ::testing::ValuesIn({true, false}));

}  // namespace paimon::test
