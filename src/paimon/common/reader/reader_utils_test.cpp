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

#include "paimon/common/reader/reader_utils.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {
namespace {

class ErrorAfterOneBatchReader : public BatchReader {
 public:
    explicit ErrorAfterOneBatchReader(ReadBatch batch) : batch_(std::move(batch)) {}

    Result<ReadBatch> NextBatch() override {
        if (batch_.first) {
            return std::move(batch_);
        }
        return Status::IOError("expected test error");
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return nullptr;
    }

    void Close() override {}

 private:
    ReadBatch batch_;
};

}  // namespace

TEST(ReaderUtilsTest, TestAddAllValidBitmap) {
    auto check_result = [](const std::string& src_str) {
        if (src_str.empty()) {
            auto batch_with_bitmap = ReaderUtils::AddAllValidBitmap(BatchReader::MakeEofBatch());
            ASSERT_TRUE(BatchReader::IsEofBatch(batch_with_bitmap));
            auto& [_, bitmap] = batch_with_bitmap;
            ASSERT_TRUE(bitmap.IsEmpty());
            return;
        }
        auto array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), src_str).ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto batch, ReadResultCollector::GetReadBatch(array));
        auto batch_with_bitmap = ReaderUtils::AddAllValidBitmap(std::move(batch));
        auto& [c_batch, bitmap] = batch_with_bitmap;
        ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::GetArray(std::move(c_batch)));
        ASSERT_EQ(bitmap.Cardinality(), result_array->length());
        ASSERT_TRUE(result_array->Equals(array));
    };

    check_result("[0, 1, 2, 3, 4]");
    check_result("[10, 20, 30]");
    check_result("");
}

TEST(ReaderUtilsTest, TestCollectResultReleasesBufferedBatchesOnError) {
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1]").ValueOrDie();
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, ReadResultCollector::GetReadBatch(array));

    std::shared_ptr<int32_t> lifetime = std::make_shared<int32_t>(1);
    std::weak_ptr<int32_t> weak_lifetime = lifetime;
    ASSERT_OK(AddArrowArrayLifetime(batch.first.get(), batch.second.get(), lifetime));
    lifetime.reset();

    auto reader = std::make_unique<ErrorAfterOneBatchReader>(std::move(batch));
    ASSERT_NOK_WITH_MSG(ReadResultCollector::CollectResult(std::move(reader)),
                        "expected test error");
    ASSERT_TRUE(weak_lifetime.expired());
}

TEST(ReaderUtilsTest, TestApplyBitmapToReadBatch) {
    auto check_result = [](const std::string& src_str, const std::vector<int32_t>& bitmap_vec,
                           const std::string& target_str, const std::string& erro_msg = "") {
        std::shared_ptr<arrow::MemoryPool> arrow_pool(arrow::default_memory_pool(),
                                                      [](arrow::MemoryPool*) {});
        auto bitmap = RoaringBitmap32::From(bitmap_vec);
        if (src_str.empty()) {
            auto batch_with_bitmap = std::make_pair(BatchReader::MakeEofBatch(), std::move(bitmap));
            ASSERT_OK_AND_ASSIGN(auto result_batch, ReaderUtils::ApplyBitmapToReadBatch(
                                                        std::move(batch_with_bitmap), arrow_pool));
            ASSERT_TRUE(BatchReader::IsEofBatch(result_batch));
            return;
        }
        auto src_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), src_str).ValueOrDie();
        auto target_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), target_str).ValueOrDie();
        ASSERT_OK_AND_ASSIGN(auto src_batch, ReadResultCollector::GetReadBatch(src_array));
        auto batch_with_bitmap = std::make_pair(std::move(src_batch), std::move(bitmap));
        if (!erro_msg.empty()) {
            ASSERT_NOK_WITH_MSG(
                ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool),
                erro_msg);
            return;
        }
        ASSERT_OK_AND_ASSIGN(auto result_batch, ReaderUtils::ApplyBitmapToReadBatch(
                                                    std::move(batch_with_bitmap), arrow_pool));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::GetArray(std::move(result_batch)));
        ASSERT_TRUE(result_array->Equals(target_array));
    };
    check_result("[10, 11, 12, 13, 14]", {1}, "[11]");
    check_result("[10, 11, 12, 13, 14]", {0, 1}, "[10, 11]");
    check_result("[10, 11, 12, 13, 14]", {2, 4}, "[12, 14]");
    check_result("[10, 11, 12, 13, 14]", {2, 3}, "[12, 13]");
    check_result("[10, 11, 12, 13, 14]", {0, 1, 3, 4}, "[10, 11, 13, 14]");
    check_result("[10, 11, 12, 13, 14]", {0, 1, 2, 3, 4}, "[10, 11, 12, 13, 14]");
    // eof batch
    check_result("", {}, "");
    // bitmap is empty, invalid
    check_result("[10, 11, 12, 13, 14]", {}, "[]",
                 "NextBatchWithBitmap should always return the result with at least one valid row "
                 "except eof");
}

TEST(ReaderUtilsTest, TestApplyBitmapToReadBatchKeepsDictionaryEncoding) {
    // A deletion vector on an append table routes the Parquet dictionary passthrough through here:
    // ParquetFileBatchReader reports SupportPreciseBitmapSelection() == false, so RawFileSplitRead
    // wraps it and the surviving rows are cut out by slicing and concatenating. The encoding
    // survives that only because every slice shares one dictionary and arrow::Concatenate has a
    // fast path for it; if it ever unified or densified instead, a compaction with deletion
    // vectors would quietly stop forwarding the encoding the rewrite asked for.
    auto dictionary_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    auto make_encoded = [&dictionary_type](const std::string& indices_json) {
        return arrow::DictionaryArray::FromArrays(
                   dictionary_type,
                   arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), indices_json)
                       .ValueOrDie(),
                   arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b"])")
                       .ValueOrDie())
            .ValueOrDie();
    };
    std::shared_ptr<arrow::Array> ids =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 2, 3, 4]").ValueOrDie();
    auto src_array = arrow::StructArray::Make({make_encoded("[0, 1, 0, 1, 0]"), ids},
                                              std::vector<std::string>{"s", "id"})
                         .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto src_batch, ReadResultCollector::GetReadBatch(src_array));
    // Two disjoint runs, so the filter has to concatenate rather than hand back a single slice.
    auto batch_with_bitmap =
        std::make_pair(std::move(src_batch), RoaringBitmap32::From(std::vector<int32_t>{0, 1, 4}));
    std::shared_ptr<arrow::MemoryPool> arrow_pool(arrow::default_memory_pool(),
                                                  [](arrow::MemoryPool*) {});
    ASSERT_OK_AND_ASSIGN(auto result_batch, ReaderUtils::ApplyBitmapToReadBatch(
                                                std::move(batch_with_bitmap), arrow_pool));
    // Imported directly rather than through ReadResultCollector::GetArray, which decodes
    // dictionaries on the way out and would hide the very thing being asserted.
    auto& [c_array, c_schema] = result_batch;
    std::shared_ptr<arrow::Array> result =
        arrow::ImportArray(c_array.get(), c_schema.get()).ValueOrDie();
    ASSERT_EQ(3, result->length());
    auto result_struct = checked_pointer_cast<arrow::StructArray>(result);
    ASSERT_EQ(arrow::Type::DICTIONARY, result_struct->field(0)->type()->id());
    ASSERT_TRUE(result_struct->field(0)->Equals(*make_encoded("[0, 1, 0]")))
        << "actual=" << result_struct->field(0)->ToString();
    std::shared_ptr<arrow::Array> expected_ids =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 4]").ValueOrDie();
    ASSERT_TRUE(result_struct->field(1)->Equals(*expected_ids));
}

}  // namespace paimon::test
