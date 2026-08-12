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

#include "paimon/core/realtime/arrow_mem_indexer.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class ForeignSegment : public RealtimeSegmentHandle {
 public:
    Range GetOffsetRange() const override {
        return Range(0, 0);
    }
};

class ForeignReadView : public MemReadView {
 public:
    std::optional<Range> GetOffsetRange() const override {
        return Range(0, 0);
    }
};

class ArrowMemIndexerTest : public testing::Test {
 public:
    void SetUp() override {
        schema_ = arrow::schema(
            {arrow::field("id", arrow::int64()), arrow::field("value", arrow::utf8())});
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
        indexer_ = std::make_shared<ArrowMemIndexer>(schema_, pool_, arrow_pool_);
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

 protected:
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<ArrowMemIndexer> indexer_;
};

TEST_F(ArrowMemIndexerTest, TestWriteValidationAndSeal) {
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> empty_segment,
                         indexer_->SealForCommit());
    ASSERT_FALSE(empty_segment.has_value());

    ASSERT_NOK_WITH_MSG(indexer_->Write(RealtimeWriteBatch{nullptr, Range(0, 0)}),
                        "write batch is null");
    ASSERT_NOK_WITH_MSG(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[0, "a"], [1, "b"]])"), Range(0, 0)}),
        "offset range does not match batch row count");

    ASSERT_OK(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[0, "a"], [1, "b"]])"), Range(0, 1)}));
    ASSERT_NOK_WITH_MSG(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[3, "d"], [4, "e"]])"), Range(3, 4)}),
        "offset ranges must be contiguous");
    ASSERT_OK(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[2, "c"], [3, "d"]])"), Range(2, 3)}));

    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         indexer_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(Range(0, 3), segment.value()->GetOffsetRange());
    ASSERT_OK_AND_ASSIGN(empty_segment, indexer_->SealForCommit());
    ASSERT_FALSE(empty_segment.has_value());

    ASSERT_GT(indexer_->GetMemoryUsage(), 0);
}

TEST_F(ArrowMemIndexerTest, TestQueryReaderClipsCommittedOffsetWithBitmap) {
    ASSERT_OK(indexer_->Write(
        RealtimeWriteBatch{MakeBatch(R"([[10, "a"], [11, "b"], [12, "c"]])"), Range(10, 12)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         indexer_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK(
        indexer_->Write(RealtimeWriteBatch{MakeBatch(R"([[13, "d"], [14, "e"]])"), Range(13, 14)}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> view, indexer_->AcquireReadView());
    ASSERT_EQ(std::optional<Range>(Range(10, 14)), view->GetOffsetRange());

    std::shared_ptr<arrow::Schema> read_schema =
        arrow::schema({arrow::field("value", arrow::utf8())});
    {
        std::unique_ptr<ArrowSchema> c_schema = MakeReadSchema(read_schema);
        MemQueryContext context{c_schema.get(), /*predicate=*/nullptr,
                                /*enable_predicate_pushdown=*/false};
        ASSERT_OK_AND_ASSIGN(
            std::vector<std::unique_ptr<BatchReader>> readers,
            indexer_->CreateQueryReaders(view, /*offset_lower_exclusive=*/11, context));
        ASSERT_EQ(1, readers.size());

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
    MemQueryContext context{c_schema.get(), /*predicate=*/nullptr,
                            /*enable_predicate_pushdown=*/false};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::unique_ptr<BatchReader>> readers,
        indexer_->CreateQueryReaders(view, /*offset_lower_exclusive=*/14, context));
    ASSERT_TRUE(readers.empty());
}

TEST_F(ArrowMemIndexerTest, TestCommitReaderPreservesSlicedBatch) {
    ASSERT_OK(indexer_->Write(RealtimeWriteBatch{
        MakeSlicedBatch(R"([[0, "a"], [1, null], [2, "c"]])", /*offset=*/1, /*length=*/2),
        Range(0, 1)}));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         indexer_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         indexer_->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, readers.size());

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    arrow::Result<std::shared_ptr<arrow::Array>> import_result =
        arrow::ImportArray(batch.first.get(), batch.second.get());
    ASSERT_TRUE(import_result.ok()) << import_result.status().ToString();
    std::shared_ptr<arrow::Array> actual_array = std::move(import_result).ValueOrDie();
    std::shared_ptr<arrow::DataType> expected_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::utf8()),
    });
    std::shared_ptr<arrow::Array> expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, R"([[0, 1, null], [0, 2, "c"]])")
            .ValueOrDie();
    ASSERT_TRUE(actual_array->Equals(*expected_array))
        << "expected: " << expected_array->ToString() << ", actual: " << actual_array->ToString();
}

TEST_F(ArrowMemIndexerTest, TestRejectsHandlesFromAnotherIndexerImplementation) {
    ASSERT_NOK_WITH_MSG(indexer_->CreateCommitReaders(std::make_shared<ForeignSegment>()),
                        "segment was not created by the Arrow mem indexer");

    std::unique_ptr<ArrowSchema> read_schema = MakeReadSchema(schema_);
    MemQueryContext context{read_schema.get(), /*predicate=*/nullptr,
                            /*enable_predicate_pushdown=*/false};
    ASSERT_NOK_WITH_MSG(indexer_->CreateQueryReaders(std::make_shared<ForeignReadView>(),
                                                     /*offset_lower_exclusive=*/-1, context),
                        "read view was not created by the Arrow mem indexer");
    read_schema->release(read_schema.get());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<MemReadView> view, indexer_->AcquireReadView());
    context.read_schema = nullptr;
    ASSERT_NOK_WITH_MSG(indexer_->CreateQueryReaders(view, /*offset_lower_exclusive=*/-1, context),
                        "mem query read schema is null");
}

}  // namespace
}  // namespace paimon::test
