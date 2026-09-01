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

#include "paimon/core/realtime/arrow_realtime_store.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/realtime/realtime_offset_batch_reader.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/realtime/arrow_realtime_store_factory.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class ForeignSegment : public RealtimeSegmentHandle {
 public:
    OffsetRange GetOffsetRange() const override {
        return OffsetRange(0, 1);
    }

    int64_t GetRowCount() const override {
        return 1;
    }
};

class ForeignReadView : public RealtimeReadView {
 public:
    std::optional<OffsetRange> GetOffsetRange() const override {
        return OffsetRange(0, 1);
    }

    Result<int64_t> GetRowCount(const OffsetRange& visible_offsets) const override {
        return visible_offsets.begin <= 0 && visible_offsets.end > 0 ? 1 : 0;
    }
};

class ArrowRealtimeStoreTest : public testing::Test {
 public:
    void SetUp() override {
        schema_ = arrow::schema({
            DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
            arrow::field("id", arrow::int64()),
            arrow::field("value", arrow::utf8()),
        });
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
        store_ = CreateStore(StatisticsMode::NONE);
    }

    std::shared_ptr<ArrowRealtimeStore> CreateStore(StatisticsMode statistics_mode) const {
        return std::make_shared<ArrowRealtimeStore>(schema_, statistics_mode, pool_, arrow_pool_);
    }

    std::unique_ptr<RecordBatch> MakeBatch(const std::string& json) const {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json)
                .ValueOrDie();
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        return RecordBatchBuilder(&c_array).Finish().value();
    }

    std::unique_ptr<RecordBatch> MakeSlicedBatch(const std::string& json, int64_t offset,
                                                 int64_t length) const {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json)
                .ValueOrDie()
                ->Slice(offset, length);
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        return RecordBatchBuilder(&c_array).Finish().value();
    }

    std::unique_ptr<ArrowSchema> MakeReadSchema(
        const std::shared_ptr<arrow::Schema>& schema) const {
        auto c_schema = std::make_unique<ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

    std::vector<int64_t> ReadIds(const BatchReader::ReadBatchWithBitmap& batch) const {
        std::shared_ptr<arrow::Array> array =
            arrow::ImportArray(batch.first.first.get(), batch.first.second.get()).ValueOrDie();
        std::shared_ptr<arrow::StructArray> struct_array =
            checked_pointer_cast<arrow::StructArray>(array);
        std::shared_ptr<arrow::Int64Array> ids =
            checked_pointer_cast<arrow::Int64Array>(struct_array->GetFieldByName("id"));
        std::vector<int64_t> result;
        for (RoaringBitmap32::Iterator iter = batch.second.Begin(); iter != batch.second.End();
             ++iter) {
            result.push_back(ids->Value(*iter));
        }
        return result;
    }

 protected:
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<ArrowRealtimeStore> store_;
};

TEST_F(ArrowRealtimeStoreTest, TestWriteValidationAndSeal) {
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> empty_segment,
                         store_->SealForCommit());
    ASSERT_FALSE(empty_segment.has_value());

    ASSERT_NOK_WITH_MSG(store_->Write(RealtimeWriteBatch{nullptr, OffsetRange(0, 1)}),
                        "write batch is null");
    ASSERT_NOK_WITH_MSG(store_->Write(RealtimeWriteBatch{MakeBatch(R"([[0, 0, "a"], [1, 1, "b"]])"),
                                                         OffsetRange(0, 0)}),
                        "offset range is invalid");

    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 0, "a"], [1, 1, "b"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[3, 3, "d"], [4, 4, "e"]])"), OffsetRange(3, 5)}));
    ASSERT_NOK_WITH_MSG(store_->Write(RealtimeWriteBatch{MakeBatch(R"([[4, 4, "e"], [5, 5, "f"]])"),
                                                         OffsetRange(4, 6)}),
                        "offset ranges must be ordered and non-overlapping");

    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(OffsetRange(0, 5), segment.value()->GetOffsetRange());
    ASSERT_EQ(4, segment.value()->GetRowCount());
    ASSERT_OK_AND_ASSIGN(empty_segment, store_->SealForCommit());
    ASSERT_FALSE(empty_segment.has_value());

    ASSERT_GT(store_->GetMemoryUsage(), 0);
}

TEST_F(ArrowRealtimeStoreTest, TestQueryReaderClipsCommittedOffsetWithBitmap) {
    ASSERT_OK(store_->Write(RealtimeWriteBatch{
        MakeBatch(R"([[10, 10, "a"], [11, 11, "b"], [12, 12, "c"]])"), OffsetRange(10, 13)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[13, 13, "d"], [14, 14, "e"]])"), OffsetRange(13, 15)}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(10, 15)), view->GetOffsetRange());

    std::shared_ptr<arrow::Schema> read_schema = arrow::schema({
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
        arrow::field("value", arrow::utf8()),
    });
    {
        std::unique_ptr<ArrowSchema> c_schema = MakeReadSchema(read_schema);
        RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr};
        ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                             store_->CreateQueryReaders(view, context));
        ASSERT_EQ(1, readers.size());
        readers[0] =
            std::make_unique<RealtimeOffsetBatchReader>(std::move(readers[0]), OffsetRange(12, 15));

        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap first,
                             readers[0]->NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(first));
        std::shared_ptr<arrow::Array> first_array =
            arrow::ImportArray(first.first.first.get(), first.first.second.get()).ValueOrDie();
        ASSERT_EQ(3, first_array->length());
        ASSERT_EQ(1, first.second.Cardinality());
        ASSERT_TRUE(first.second.Contains(2));

        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap second,
                             readers[0]->NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(second));
        std::shared_ptr<arrow::Array> second_array =
            arrow::ImportArray(second.first.first.get(), second.first.second.get()).ValueOrDie();
        ASSERT_EQ(2, second_array->length());
        ASSERT_EQ(2, second.second.Cardinality());
        ASSERT_TRUE(second.second.Contains(0));
        ASSERT_TRUE(second.second.Contains(1));

        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap eof,
                             readers[0]->NextBatchWithBitmap());
        ASSERT_TRUE(BatchReader::IsEofBatch(eof));
    }

    std::unique_ptr<ArrowSchema> c_schema = MakeReadSchema(read_schema);
    RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateQueryReaders(view, context));
    ASSERT_EQ(1, readers.size());
    readers[0] =
        std::make_unique<RealtimeOffsetBatchReader>(std::move(readers[0]), OffsetRange(15, 15));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap eof, readers[0]->NextBatchWithBitmap());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));
}

TEST_F(ArrowRealtimeStoreTest, TestCommitReaderPreservesSlicedBatch) {
    ASSERT_OK(store_->Write(RealtimeWriteBatch{
        MakeSlicedBatch(R"([[-1, 0, "a"], [0, 1, null], [1, 2, "c"]])", /*offset=*/1,
                        /*length=*/2),
        OffsetRange(0, 2)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, readers.size());

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    readers.clear();
    arrow::Result<std::shared_ptr<arrow::Array>> import_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> actual_array = std::move(import_result).ValueOrDie();
    std::shared_ptr<arrow::DataType> expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::utf8()),
    });
    std::shared_ptr<arrow::Array> expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type,
                                                  R"([[0, 0, 1, null], [0, 1, 2, "c"]])")
            .ValueOrDie();
    ASSERT_TRUE(actual_array->Equals(*expected_array))
        << "expected: " << expected_array->ToString() << ", actual: " << actual_array->ToString();
}

TEST_F(ArrowRealtimeStoreTest, TestFullStatisticsPrunesNonMatchingBatch) {
    ArrowRealtimeStoreFactory factory;
    std::unique_ptr<ArrowSchema> write_schema = MakeReadSchema(schema_);
    RealtimeStoreCreateRequest request{std::move(write_schema),
                                       /*options=*/{}, pool_, RealtimeStoreMode::APPEND_ONLY,
                                       StatisticsMode::FULL};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeStore> realtime_store,
                         factory.Create(std::move(request)));
    std::shared_ptr<ArrowRealtimeStore> store =
        std::dynamic_pointer_cast<ArrowRealtimeStore>(realtime_store);
    ASSERT_NE(nullptr, store);
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 0, "a"], [1, 1, "b"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store->Write(
        RealtimeWriteBatch{MakeBatch(R"([[2, 10, "c"], [3, 11, "d"]])"), OffsetRange(2, 4)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store->AcquireReadView());

    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(schema_);
    std::shared_ptr<Predicate> predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/1, /*field_name=*/"id", FieldType::BIGINT, Literal(int64_t{5}));
    RealtimeQueryContext context{read_schema.get(), predicate};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store->CreateQueryReaders(view, context));
    ASSERT_EQ(1, readers.size());
    readers[0] =
        std::make_unique<RealtimeOffsetBatchReader>(std::move(readers[0]), OffsetRange(3, 4));

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, readers[0]->NextBatchWithBitmap());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    ASSERT_EQ(std::vector<int64_t>({11}), ReadIds(batch));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap eof, readers[0]->NextBatchWithBitmap());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));

    std::unique_ptr<ArrowSchema> unfiltered_read_schema = MakeReadSchema(schema_);
    RealtimeQueryContext unfiltered_context{unfiltered_read_schema.get(), /*predicate=*/nullptr};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> unfiltered_readers,
                         store->CreateQueryReaders(view, unfiltered_context));
    ASSERT_EQ(1, unfiltered_readers.size());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap unfiltered_batch,
                         unfiltered_readers[0]->NextBatchWithBitmap());
    ASSERT_EQ(std::vector<int64_t>({0, 1}), ReadIds(unfiltered_batch));
}

TEST_F(ArrowRealtimeStoreTest, TestMissingStatisticsRetainsNonMatchingBatch) {
    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[0, 0, "a"], [1, 1, "b"]])"), OffsetRange(0, 2)}));
    ASSERT_OK(store_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[2, 10, "c"], [3, 11, "d"]])"), OffsetRange(2, 4)}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());

    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(schema_);
    std::shared_ptr<Predicate> predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/1, /*field_name=*/"id", FieldType::BIGINT, Literal(int64_t{5}));
    RealtimeQueryContext context{read_schema.get(), predicate};
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateQueryReaders(view, context));
    ASSERT_EQ(1, readers.size());

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, readers[0]->NextBatchWithBitmap());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    ASSERT_EQ(std::vector<int64_t>({0, 1}), ReadIds(batch));
}

TEST_F(ArrowRealtimeStoreTest, TestRejectsHandlesFromAnotherStoreImplementation) {
    ASSERT_NOK_WITH_MSG(store_->CreateCommitReaders(std::make_shared<ForeignSegment>()),
                        "segment was not created by the Arrow real-time store");

    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(schema_);
    RealtimeQueryContext context{read_schema.get(), /*predicate=*/nullptr};
    ASSERT_NOK_WITH_MSG(store_->CreateQueryReaders(std::make_shared<ForeignReadView>(), context),
                        "read view was not created by the Arrow real-time store");
    read_schema->release(read_schema.get());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    context.read_schema = nullptr;
    ASSERT_NOK_WITH_MSG(store_->CreateQueryReaders(view, context), "mem query read schema is null");
}

}  // namespace
}  // namespace paimon::test
