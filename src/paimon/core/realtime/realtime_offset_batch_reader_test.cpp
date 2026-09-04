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

#include "paimon/core/realtime/realtime_offset_batch_reader.h"

#include <memory>
#include <string>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {
namespace {

std::shared_ptr<arrow::DataType> MakeDataType(bool offset_nullable = false) {
    return arrow::struct_({
        arrow::field("id", arrow::int32()),
        arrow::field(SpecialFields::RealtimeOffset().Name(), arrow::int64(), offset_nullable),
    });
}

std::shared_ptr<arrow::Array> MakeArray(const std::shared_ptr<arrow::DataType>& type,
                                        const std::string& json) {
    return arrow::ipc::internal::json::ArrayFromJSON(type, json).ValueOrDie();
}

}  // namespace

TEST(RealtimeOffsetBatchReaderTest, TestFilterBitmapAndRemoveOffset) {
    std::shared_ptr<arrow::DataType> type = MakeDataType();
    std::shared_ptr<arrow::Array> data =
        MakeArray(type, R"([[10, 0], [11, 1], [12, 2], [13, 3], [14, 4], [15, 5]])");
    RoaringBitmap32 input_bitmap;
    input_bitmap.AddRange(0, static_cast<int32_t>(data->length()));
    auto input =
        std::make_unique<MockFileBatchReader>(data, type, input_bitmap, /*read_batch_size=*/2);
    RealtimeOffsetBatchReader reader(std::move(input), OffsetRange(2, 5));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(&reader));
    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_({arrow::field("id", arrow::int32())}),
                    {R"([[12], [13]])", R"([[14]])"}, &expected)
                    .ok());
    ASSERT_TRUE(result->Equals(expected))
        << "expected: " << expected->ToString() << "\nactual: " << result->ToString();
}

TEST(RealtimeOffsetBatchReaderTest, TestRejectsPartialBitmap) {
    std::shared_ptr<arrow::DataType> type = MakeDataType();
    std::shared_ptr<arrow::Array> data = MakeArray(type, R"([[10, 0], [11, 1]])");
    RoaringBitmap32 input_bitmap;
    input_bitmap.Add(0);
    auto input =
        std::make_unique<MockFileBatchReader>(data, type, input_bitmap, /*read_batch_size=*/2);
    input->EnableRandomizeBatchSize(false);
    RealtimeOffsetBatchReader reader(std::move(input), OffsetRange(0, 2));

    ASSERT_NOK_WITH_MSG(ReadResultCollector::CollectResult(&reader),
                        "must cover every raw transport row");
}

TEST(RealtimeOffsetBatchReaderTest, TestFilterWithoutInputBitmap) {
    std::shared_ptr<arrow::DataType> type = MakeDataType();
    std::shared_ptr<arrow::Array> data =
        MakeArray(type, R"([[10, 0], [11, 1], [12, 2], [13, 3], [14, 4]])");
    auto input = std::make_unique<MockFileBatchReader>(data, type, /*read_batch_size=*/3);
    RealtimeOffsetBatchReader reader(std::move(input), OffsetRange(1, 4));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(&reader));
    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_({arrow::field("id", arrow::int32())}),
                    {R"([[11], [12]])", R"([[13]])"}, &expected)
                    .ok());
    ASSERT_TRUE(result->Equals(expected))
        << "expected: " << expected->ToString() << "\nactual: " << result->ToString();
}

TEST(RealtimeOffsetBatchReaderTest, TestRejectsNullOffsetInBatch) {
    std::shared_ptr<arrow::DataType> type = MakeDataType(/*offset_nullable=*/true);
    std::shared_ptr<arrow::Array> data = MakeArray(type, R"([[10, 0], [11, null]])");
    auto input = std::make_unique<MockFileBatchReader>(data, type, /*read_batch_size=*/2);
    RealtimeOffsetBatchReader reader(std::move(input), OffsetRange(0, 1));

    ASSERT_NOK_WITH_MSG(ReadResultCollector::CollectResult(&reader), "offset column contains null");
}

TEST(RealtimeOffsetBatchReaderTest, TestNextBatchIsUnsupported) {
    std::shared_ptr<arrow::DataType> type = MakeDataType();
    std::shared_ptr<arrow::Array> data = MakeArray(type, R"([[10, 0]])");
    auto input = std::make_unique<MockFileBatchReader>(data, type, /*read_batch_size=*/1);
    RealtimeOffsetBatchReader reader(std::move(input), OffsetRange(0, 1));

    ASSERT_NOK_WITH_MSG(reader.NextBatch(), "should use NextBatchWithBitmap");
}

}  // namespace paimon::test
