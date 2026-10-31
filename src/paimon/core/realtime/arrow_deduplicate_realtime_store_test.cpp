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

#include "paimon/core/realtime/arrow_deduplicate_realtime_store.h"

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/realtime/arrow_realtime_store.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/macros.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/blocking_file_system.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class EmptyKeyOffsetLookup final : public KeyOffsetLookup {
 public:
    Result<RoaringBitmap64> LookupOffsets(
        const std::shared_ptr<arrow::StructArray>&) const override {
        return RoaringBitmap64();
    }
};

class BitmapSelectingRealtimeStore final : public RealtimeStore {
 public:
    BitmapSelectingRealtimeStore(std::shared_ptr<RealtimeStore> delegate, RoaringBitmap32 bitmap)
        : delegate_(std::move(delegate)), bitmap_(std::move(bitmap)) {}

    Status Write(RealtimeWriteBatch&& batch) override {
        return delegate_->Write(std::move(batch));
    }
    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() override {
        return delegate_->SealForCommit();
    }
    Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() override {
        return delegate_->AcquireReadView();
    }
    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) override {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> readers,
                               delegate_->CreateCommitReaders(segment));
        return Wrap(std::move(readers));
    }
    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view,
        const RealtimeQueryContext& context) override {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> readers,
                               delegate_->CreateQueryReaders(view, context));
        return Wrap(std::move(readers));
    }
    Status AdvanceCommittedOffset(int64_t offset) override {
        return delegate_->AdvanceCommittedOffset(offset);
    }
    RealtimeStoreDataUsage GetDataUsage() const override {
        return delegate_->GetDataUsage();
    }
    uint64_t GetMemoryUsage() const override {
        return delegate_->GetMemoryUsage();
    }

 private:
    class Reader final : public BatchReader {
     public:
        Reader(std::unique_ptr<BatchReader> inner, RoaringBitmap32 bitmap)
            : inner_(std::move(inner)), bitmap_(std::move(bitmap)) {}

        Result<ReadBatch> NextBatch() override {
            return Status::Invalid("bitmap input must not be materialized");
        }
        Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
            PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, inner_->NextBatchWithBitmap());
            if (!IsEofBatch(batch)) {
                batch.second = bitmap_;
            }
            return batch;
        }
        std::shared_ptr<Metrics> GetReaderMetrics() const override {
            return inner_->GetReaderMetrics();
        }
        void Close() override {
            inner_->Close();
        }

     private:
        std::unique_ptr<BatchReader> inner_;
        RoaringBitmap32 bitmap_;
    };

    std::vector<std::unique_ptr<BatchReader>> Wrap(
        std::vector<std::unique_ptr<BatchReader>>&& readers) const {
        for (std::unique_ptr<BatchReader>& reader : readers) {
            reader = std::make_unique<Reader>(std::move(reader), bitmap_);
        }
        return std::move(readers);
    }

    std::shared_ptr<RealtimeStore> delegate_;
    RoaringBitmap32 bitmap_;
};

class ArrowDeduplicateRealtimeStoreTest : public testing::Test {
 public:
    void SetUp() override {
        schema_ = arrow::schema({
            DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()),
            arrow::field("id", arrow::int64()),
            arrow::field("value", arrow::utf8()),
        });
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
        delegate_ = std::make_shared<ArrowRealtimeStore>(
            schema_, RealtimeStoreMode::APPEND_ONLY, StatisticsMode::NONE,
            /*temp_directory=*/"", /*spill_file_system=*/nullptr,
            /*spill_compression=*/"zstd", /*spill_compression_level=*/1, pool_, arrow_pool_);
        ASSERT_OK_AND_ASSIGN(store_, ArrowDeduplicateRealtimeStore::Create(
                                         schema_, {"id"}, delegate_, pool_, arrow_pool_));
        empty_lookup_ = std::make_shared<EmptyKeyOffsetLookup>();
        ASSERT_OK(store_->AttachCommittedKeyOffsetLookup(empty_lookup_));
    }

    std::shared_ptr<arrow::StructArray> MakeBatch(const std::string& json) const {
        return checked_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json)
                .ValueOrDie());
    }

    std::shared_ptr<arrow::StructArray> MakeKeys(const std::string& json) const {
        return checked_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({schema_->field(1)}), json)
                .ValueOrDie());
    }

    void WriteAndSeal(const std::string& json, const OffsetRange& offset_range,
                      const RoaringBitmap64& previous_offsets) {
        ASSERT_OK(store_->WriteAndLookup(MakeBatch(json), {RecordBatch::RowKind::INSERT},
                                         offset_range, previous_offsets));
        ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                             store_->SealForCommit());
        ASSERT_TRUE(segment.has_value());
    }

    void ResetStoreWithBitmap(const RoaringBitmap32& bitmap) {
        delegate_ = std::make_shared<ArrowRealtimeStore>(
            schema_, RealtimeStoreMode::APPEND_ONLY, StatisticsMode::NONE,
            /*temp_directory=*/"", /*spill_file_system=*/nullptr,
            /*spill_compression=*/"zstd", /*spill_compression_level=*/1, pool_, arrow_pool_);
        auto selecting = std::make_shared<BitmapSelectingRealtimeStore>(delegate_, bitmap);
        ASSERT_OK_AND_ASSIGN(store_, ArrowDeduplicateRealtimeStore::Create(
                                         schema_, {"id"}, selecting, pool_, arrow_pool_));
        ASSERT_OK(store_->AttachCommittedKeyOffsetLookup(empty_lookup_));
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> QueryReaders(
        const std::shared_ptr<RealtimeReadView>& view, const std::shared_ptr<arrow::Schema>& schema,
        int32_t batch_size) const {
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, c_schema.get()));
        RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr, batch_size};
        return store_->CreateQueryReaders(view, context);
    }

    Result<std::shared_ptr<arrow::StructArray>> ImportBatch(BatchReader::ReadBatch* batch) const {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ImportArray(batch->first.get(), batch->second.get()));
        return checked_pointer_cast<arrow::StructArray>(array);
    }

    Result<std::vector<int64_t>> ReadOffsets(const std::shared_ptr<RealtimeReadView>& view) const {
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema_, c_schema.get()));
        RealtimeQueryContext context{c_schema.get(), /*predicate=*/nullptr};
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> readers,
                               store_->CreateQueryReaders(view, context));
        std::vector<int64_t> result;
        for (std::unique_ptr<BatchReader>& reader : readers) {
            while (true) {
                PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
                if (BatchReader::IsEofBatch(batch)) {
                    break;
                }
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> imported,
                    arrow::ImportArray(batch.first.get(), batch.second.get()));
                std::shared_ptr<arrow::StructArray> rows =
                    checked_pointer_cast<arrow::StructArray>(imported);
                std::shared_ptr<arrow::Int64Array> offsets =
                    checked_pointer_cast<arrow::Int64Array>(
                        rows->GetFieldByName(SpecialFields::RealtimeOffset().Name()));
                for (int64_t row = 0; row < offsets->length(); ++row) {
                    result.push_back(offsets->Value(row));
                }
            }
            reader->Close();
        }
        return result;
    }

 protected:
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<ArrowRealtimeStore> delegate_;
    std::shared_ptr<ArrowDeduplicateRealtimeStore> store_;
    std::shared_ptr<const KeyOffsetLookup> empty_lookup_;
};

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestReclaimsOnlyCompleteSegmentPrefix) {
    WriteAndSeal(R"([[2, 1, "old"]])", OffsetRange(0, 10), RoaringBitmap64());
    RoaringBitmap64 overwritten_offsets;
    overwritten_offsets.Add(2);
    WriteAndSeal(R"([[12, 1, "new"]])", OffsetRange(10, 20), overwritten_offsets);

    RealtimeStoreDataUsage usage = store_->GetDataUsage();
    ASSERT_EQ(2, usage.sealed_row_count);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> initial_view, store_->AcquireReadView());
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(0, 20)), initial_view->GetOffsetRange());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> initial_deletions,
                         ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(initial_view));
    ASSERT_EQ("{2}", initial_deletions->ToString());

    ASSERT_OK(store_->RetireCommittedOffsetAndLookup(/*committed_end_offset=*/5, empty_lookup_));
    usage = store_->GetDataUsage();
    ASSERT_EQ(2, usage.sealed_row_count);

    ASSERT_OK(store_->RetireCommittedOffsetAndLookup(/*committed_end_offset=*/10, empty_lookup_));
    usage = store_->GetDataUsage();
    ASSERT_EQ(1, usage.sealed_row_count);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> retained_view,
                         store_->AcquireReadView());
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(10, 20)), retained_view->GetOffsetRange());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> retained_deletions,
                         ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(retained_view));
    ASSERT_EQ("{2}", retained_deletions->ToString());

    ASSERT_OK(store_->RetireCommittedOffsetAndLookup(/*committed_end_offset=*/15, empty_lookup_));
    usage = store_->GetDataUsage();
    ASSERT_EQ(1, usage.sealed_row_count);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> crossing_view,
                         store_->AcquireReadView());
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(10, 20)), crossing_view->GetOffsetRange());

    ASSERT_OK(store_->RetireCommittedOffsetAndLookup(/*committed_end_offset=*/20, empty_lookup_));
    usage = store_->GetDataUsage();
    ASSERT_EQ(0, usage.sealed_row_count);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> reclaimed_view,
                         store_->AcquireReadView());
    ASSERT_FALSE(reclaimed_view->GetOffsetRange().has_value());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> reclaimed_deletions,
                         ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(reclaimed_view));
    ASSERT_TRUE(reclaimed_deletions->IsEmpty());
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestLastWriteWinsAndReadViewIsSnapshot) {
    ASSERT_OK(store_->WriteAndLookup(
        MakeBatch(R"([[1, 1, "old"], [2, 2, "keep"], [3, 1, "new"]])"),
        {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT},
        OffsetRange(1, 4), RoaringBitmap64()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> pinned_view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> pinned_deletions,
                         ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(pinned_view));
    ASSERT_EQ("{1}", pinned_deletions->ToString());

    ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[4, 2, "latest"]])"),
                                     {RecordBatch::RowKind::UPDATE_AFTER}, OffsetRange(4, 5),
                                     RoaringBitmap64()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> current_view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> current_deletions,
                         ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(current_view));
    ASSERT_EQ("{1,2}", current_deletions->ToString());
    ASSERT_EQ("{1}", pinned_deletions->ToString());

    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> pinned_offsets, ReadOffsets(pinned_view));
    ASSERT_EQ((std::vector<int64_t>{2, 3}), pinned_offsets);
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> current_offsets, ReadOffsets(current_view));
    ASSERT_EQ((std::vector<int64_t>{3, 4}), current_offsets);

    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(2, segment.value()->GetRowCount());
    ASSERT_EQ(OffsetRange(1, 5), segment.value()->GetOffsetRange());
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestDeleteProducesNoLiveRows) {
    ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[1, 1, "value"]])"),
                                     {RecordBatch::RowKind::INSERT}, OffsetRange(1, 2),
                                     RoaringBitmap64()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> lookup,
                         store_->AcquireKeyOffsetLookup());
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 previous, lookup->LookupOffsets(MakeKeys(R"([[1]])")));
    ASSERT_EQ("{1}", previous.ToString());
    ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[2, 1, "ignored"]])"),
                                     {RecordBatch::RowKind::DELETE}, OffsetRange(2, 3), previous));

    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(0, segment.value()->GetRowCount());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateCommitReaders(segment.value()));
    ASSERT_EQ(1, readers.size());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestBitmapFilteringComposesInputSelection) {
    struct TestCase {
        std::vector<int64_t> deleted_offsets;
        std::vector<int32_t> expected_rows;
        std::vector<int64_t> expected_offsets;
    };
    const std::vector<TestCase> cases = {
        {{}, {0, 2, 4}, {10, 12, 14}},
        {{12}, {0, 4}, {10, 14}},
        {{10, 12, 14}, {}, {}},
        {{99}, {0, 2, 4}, {10, 12, 14}},
    };
    const RoaringBitmap32 input = RoaringBitmap32::From({0, 2, 4});
    for (const TestCase& test_case : cases) {
        SCOPED_TRACE(RoaringBitmap64::From(test_case.deleted_offsets).ToString());
        ASSERT_NO_FATAL_FAILURE(ResetStoreWithBitmap(input));
        ASSERT_OK(store_->WriteAndLookup(
            MakeBatch(R"([[10, 1, "a"], [11, 2, "b"], [12, 3, "c"],
                         [13, 4, "d"], [14, 5, "e"]])"),
            std::vector<RecordBatch::RowKind>(5, RecordBatch::RowKind::INSERT), OffsetRange(10, 15),
            RoaringBitmap64()));
        RoaringBitmap64 deletions = RoaringBitmap64::From(test_case.deleted_offsets);
        ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[100, 99, "delete"]])"),
                                         {RecordBatch::RowKind::DELETE}, OffsetRange(100, 101),
                                         deletions));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
        auto projection = arrow::schema({schema_->field(2), schema_->field(0)});
        ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                             QueryReaders(view, projection, 5));
        ASSERT_EQ(1, readers.size());
        const RoaringBitmap32 expected = RoaringBitmap32::From(test_case.expected_rows);
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch,
                             readers[0]->NextBatchWithBitmap());
        if (expected.IsEmpty()) {
            ASSERT_TRUE(BatchReader::IsEofBatch(batch));
        } else {
            ASSERT_FALSE(BatchReader::IsEofBatch(batch));
            ASSERT_EQ(expected, batch.second);
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> raw,
                                 ImportBatch(&batch.first));
            ASSERT_EQ(5, raw->length());
            ASSERT_EQ(2, raw->num_fields());
            ASSERT_EQ("value", raw->struct_type()->field(0)->name());
            const auto offsets = checked_pointer_cast<arrow::Int64Array>(raw->field(1));
            ASSERT_EQ(12, offsets->Value(2));
            ASSERT_OK_AND_ASSIGN(batch, readers[0]->NextBatchWithBitmap());
            ASSERT_TRUE(BatchReader::IsEofBatch(batch));
        }
        readers[0]->Close();
        ASSERT_OK_AND_ASSIGN(std::vector<int64_t> visible, ReadOffsets(view));
        ASSERT_EQ(test_case.expected_offsets, visible);

        ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                             store_->SealForCommit());
        ASSERT_TRUE(segment.has_value());
        ASSERT_OK_AND_ASSIGN(readers, store_->CreateCommitReaders(segment.value()));
        ASSERT_EQ(1, readers.size());
        ASSERT_OK_AND_ASSIGN(batch, readers[0]->NextBatchWithBitmap());
        if (expected.IsEmpty()) {
            ASSERT_TRUE(BatchReader::IsEofBatch(batch));
        } else {
            ASSERT_EQ(expected, batch.second);
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> raw,
                                 ImportBatch(&batch.first));
            ASSERT_EQ(5, raw->length());
        }
        readers[0]->Close();
    }
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestBitmapFilteringSkipsDeletedBatches) {
    ASSERT_OK(
        store_->WriteAndLookup(MakeBatch(R"([[10, 1, "a"], [11, 2, "b"], [12, 3, "c"],
                     [13, 4, "d"], [14, 5, "e"], [15, 6, "f"]])"),
                               std::vector<RecordBatch::RowKind>(6, RecordBatch::RowKind::INSERT),
                               OffsetRange(10, 16), RoaringBitmap64()));
    ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[16, 99, "delete"]])"),
                                     {RecordBatch::RowKind::DELETE}, OffsetRange(16, 17),
                                     RoaringBitmap64::From({10, 11, 14, 15})));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         QueryReaders(view, schema_, 2));
    ASSERT_EQ(1, readers.size());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, readers[0]->NextBatchWithBitmap());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    ASSERT_EQ(2, batch.second.Cardinality());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> raw, ImportBatch(&batch.first));
    ASSERT_EQ(2, raw->length());
    const auto offsets = checked_pointer_cast<arrow::Int64Array>(raw->field(0));
    ASSERT_EQ(12, offsets->Value(0));
    ASSERT_EQ(13, offsets->Value(1));
    ASSERT_OK_AND_ASSIGN(batch, readers[0]->NextBatchWithBitmap());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
    readers[0]->Close();
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestBitmapFilteringSkipsEmptyInput) {
    ASSERT_NO_FATAL_FAILURE(ResetStoreWithBitmap(RoaringBitmap32()));
    ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[10, 1, "a"]])"), {RecordBatch::RowKind::INSERT},
                                     OffsetRange(10, 11), RoaringBitmap64()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         QueryReaders(view, schema_, 1));
    ASSERT_EQ(1, readers.size());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch, readers[0]->NextBatchWithBitmap());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
    readers[0]->Close();
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestSpilledBitmapFilteringPreservesRawRows) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, directory);
    auto fs = std::make_shared<LocalFileSystem>();
    delegate_ = std::make_shared<ArrowRealtimeStore>(schema_, RealtimeStoreMode::APPEND_ONLY,
                                                     StatisticsMode::NONE, directory->Str(), fs,
                                                     "zstd", 1, pool_, arrow_pool_);
    ASSERT_OK_AND_ASSIGN(store_, ArrowDeduplicateRealtimeStore::Create(schema_, {"id"}, delegate_,
                                                                       pool_, arrow_pool_));
    ASSERT_OK(store_->AttachCommittedKeyOffsetLookup(empty_lookup_));
    ASSERT_OK(store_->WriteAndLookup(
        MakeBatch(R"([[10, 1, "a"], [11, 2, "b"], [12, 3, "c"], [13, 4, "d"]])"),
        std::vector<RecordBatch::RowKind>(4, RecordBatch::RowKind::INSERT), OffsetRange(10, 14),
        RoaringBitmap64()));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(0, delegate_->GetDataUsage().sealed_memory_bytes);
    ASSERT_OK(
        store_->WriteAndLookup(MakeBatch(R"([[14, 2, "delete"], [15, 4, "delete"]])"),
                               std::vector<RecordBatch::RowKind>(2, RecordBatch::RowKind::DELETE),
                               OffsetRange(14, 16), RoaringBitmap64::From({11, 13})));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         QueryReaders(view, schema_, 2));
    ASSERT_EQ(1, readers.size());
    for (int64_t expected_offset : {10, 12}) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch,
                             readers[0]->NextBatchWithBitmap());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        ASSERT_EQ(1, batch.second.Cardinality());
        ASSERT_TRUE(batch.second.Contains(0));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> raw, ImportBatch(&batch.first));
        ASSERT_EQ(2, raw->length());
        const auto offsets = checked_pointer_cast<arrow::Int64Array>(raw->field(0));
        ASSERT_EQ(expected_offset, offsets->Value(0));
    }
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap eof, readers[0]->NextBatchWithBitmap());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));
    readers[0]->Close();
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> visible, ReadOffsets(view));
    ASSERT_EQ((std::vector<int64_t>{10, 12}), visible);
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestOffsetMergeScanMatchesContains) {
    const char* ordered = R"([[10, 0, "value"], [11, 1, "value"], [12, 2, "value"],
                              [13, 3, "value"], [14, 4, "value"]])";
    const char* gaps = R"([[0, 0, "value"], [10, 1, "value"], [1099511627776, 2, "value"],
                           [1099511627777, 3, "value"], [9223372036854775804, 4, "value"]])";
    const int64_t large = 1099511627776;
    const int64_t high = 9223372036854775804;
    struct TestCase {
        const char* input_json;
        std::vector<int32_t> input_rows;
        std::vector<int64_t> deleted_offsets;
        std::vector<int32_t> expected_rows;
        std::vector<int64_t> expected_offsets;
    };
    const std::vector<TestCase> cases = {
        {ordered, {0, 1, 2, 3, 4}, {}, {0, 1, 2, 3, 4}, {10, 11, 12, 13, 14}},
        {ordered, {0, 2, 4}, {}, {0, 2, 4}, {10, 12, 14}},
        {ordered, {0, 1, 2, 3, 4}, {1}, {0, 1, 2, 3, 4}, {10, 11, 12, 13, 14}},
        {ordered, {0, 2, 4}, {1}, {0, 2, 4}, {10, 12, 14}},
        {ordered, {0, 1, 2, 3, 4}, {10, 12, large, high}, {1, 3, 4}, {11, 13, 14}},
        {ordered, {0, 2, 4}, {10, 12, large, high}, {4}, {14}},
        {ordered, {0, 1, 2, 3, 4}, {0, 10, 11, 12, 13, 14, large, 1099511627777, high}, {}, {}},
        {ordered, {0, 2, 4}, {0, 10, 11, 12, 13, 14, large, 1099511627777, high}, {}, {}},

        {gaps, {0, 1, 2, 3, 4}, {}, {0, 1, 2, 3, 4}, {0, 10, large, 1099511627777, high}},
        {gaps, {0, 2, 4}, {}, {0, 2, 4}, {0, large, high}},
        {gaps, {0, 1, 2, 3, 4}, {1}, {0, 1, 2, 3, 4}, {0, 10, large, 1099511627777, high}},
        {gaps, {0, 2, 4}, {1}, {0, 2, 4}, {0, large, high}},
        {gaps, {0, 1, 2, 3, 4}, {10, 12, large, high}, {0, 3}, {0, 1099511627777}},
        {gaps, {0, 2, 4}, {10, 12, large, high}, {0}, {0}},
        {gaps, {0, 1, 2, 3, 4}, {0, 10, 11, 12, 13, 14, large, 1099511627777, high}, {}, {}},
        {gaps, {0, 2, 4}, {0, 10, 11, 12, 13, 14, large, 1099511627777, high}, {}, {}},
    };
    for (const TestCase& test_case : cases) {
        SCOPED_TRACE(test_case.input_json);
        SCOPED_TRACE(RoaringBitmap32::From(test_case.input_rows).ToString());
        SCOPED_TRACE(RoaringBitmap64::From(test_case.deleted_offsets).ToString());
        ASSERT_NO_FATAL_FAILURE(ResetStoreWithBitmap(RoaringBitmap32::From(test_case.input_rows)));
        const std::shared_ptr<arrow::StructArray> data = MakeBatch(test_case.input_json);
        ASSERT_OK(store_->WriteAndLookup(
            data, std::vector<RecordBatch::RowKind>(5, RecordBatch::RowKind::INSERT),
            OffsetRange(0, 9223372036854775805), RoaringBitmap64::From(test_case.deleted_offsets)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> view, store_->AcquireReadView());
        ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                             QueryReaders(view, schema_, 5));
        ASSERT_EQ(1, readers.size());
        const RoaringBitmap32 expected = RoaringBitmap32::From(test_case.expected_rows);
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch,
                             readers[0]->NextBatchWithBitmap());
        if (expected.IsEmpty()) {
            ASSERT_TRUE(BatchReader::IsEofBatch(batch));
        } else {
            ASSERT_FALSE(BatchReader::IsEofBatch(batch));
            ASSERT_EQ(expected, batch.second);
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> raw,
                                 ImportBatch(&batch.first));
            ASSERT_TRUE(raw->Equals(data));
        }
        readers[0]->Close();
        ASSERT_OK_AND_ASSIGN(std::vector<int64_t> visible, ReadOffsets(view));
        ASSERT_EQ(test_case.expected_offsets, visible);
    }
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestFreezePreservesCheckpointWithoutSpilling) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, directory);
    delegate_ = std::make_shared<ArrowRealtimeStore>(
        schema_, RealtimeStoreMode::APPEND_ONLY, StatisticsMode::NONE, directory->Str(),
        directory->GetFileSystem(), "zstd", 1, pool_, arrow_pool_);
    ASSERT_OK_AND_ASSIGN(store_, ArrowDeduplicateRealtimeStore::Create(schema_, {"id"}, delegate_,
                                                                       pool_, arrow_pool_));
    ASSERT_OK(store_->AttachCommittedKeyOffsetLookup(empty_lookup_));
    ASSERT_OK(store_->WriteAndLookup(
        MakeBatch(R"([[1, 1, "old"], [2, 1, "new"], [3, 2, "two"]])"),
        {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT},
        OffsetRange(1, 4), RoaringBitmap64()));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> first,
                         store_->FreezeBuildingSegment());
    ASSERT_TRUE(first.has_value());
    ASSERT_EQ(OffsetRange(1, 4), first.value()->GetOffsetRange());
    ASSERT_EQ(2, first.value()->GetRowCount());
    ASSERT_EQ(0, TestHelper::CountChannelFiles(directory->GetFileSystem(), directory->Str()));
    ASSERT_EQ(0, delegate_->GetDataUsage().building_memory_bytes);
    ASSERT_GT(delegate_->GetDataUsage().sealed_memory_bytes, 0);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> checkpoint,
                         ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(first.value()));
    ASSERT_EQ("{1}", checkpoint->ToString());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> pinned, store_->AcquireReadView());
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> lookup,
                             store_->AcquireKeyOffsetLookup());
        ASSERT_OK_AND_ASSIGN(RoaringBitmap64 previous,
                             lookup->LookupOffsets(MakeKeys(R"([[1], [2]])")));
        ASSERT_EQ("{2,3}", previous.ToString());
        ASSERT_OK(
            store_->WriteAndLookup(MakeBatch(R"([[4, 1, "latest"], [5, 2, "deleted"]])"),
                                   {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::DELETE},
                                   OffsetRange(4, 6), previous));
    }
    ASSERT_EQ("{1}", checkpoint->ToString());
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> visible, ReadOffsets(pinned));
    ASSERT_EQ((std::vector<int64_t>{2, 3}), visible);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> current, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(visible, ReadOffsets(current));
    ASSERT_EQ((std::vector<int64_t>{4}), visible);
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateCommitReaders(first.value()));
    ASSERT_EQ(1, readers.size());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> committed, ImportBatch(&batch));
    ASSERT_TRUE(committed->Equals(MakeBatch(R"([[2, 1, "new"], [3, 2, "two"]])")));
    readers[0]->Close();

    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> second,
                         store_->SealForCommit());
    ASSERT_TRUE(second.has_value());
    ASSERT_EQ(OffsetRange(4, 6), second.value()->GetOffsetRange());
    ASSERT_EQ(1, second.value()->GetRowCount());
    ASSERT_EQ(1, TestHelper::CountChannelFiles(directory->GetFileSystem(), directory->Str()));
    ASSERT_OK_AND_ASSIGN(checkpoint,
                         ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(second.value()));
    ASSERT_EQ("{1,2,3}", checkpoint->ToString());

    ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[6, 1, "deleted"]])"),
                                     {RecordBatch::RowKind::DELETE}, OffsetRange(6, 7),
                                     RoaringBitmap64::From({4})));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> deletion,
                         store_->FreezeBuildingSegment());
    ASSERT_TRUE(deletion.has_value());
    ASSERT_EQ(OffsetRange(6, 7), deletion.value()->GetOffsetRange());
    ASSERT_EQ(0, deletion.value()->GetRowCount());
    ASSERT_OK_AND_ASSIGN(checkpoint,
                         ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(deletion.value()));
    ASSERT_EQ("{1,2,3,4}", checkpoint->ToString());
    ASSERT_OK_AND_ASSIGN(readers, store_->CreateCommitReaders(deletion.value()));
    ASSERT_TRUE(readers.empty());
    ASSERT_EQ(1, TestHelper::CountChannelFiles(directory->GetFileSystem(), directory->Str()));
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestSpillDoesNotBlockWriteAndPreservesCheckpoint) {
    for (bool fail_close : {false, true}) {
        SCOPED_TRACE(fail_close);
        auto directory = UniqueTestDirectory::Create();
        ASSERT_NE(nullptr, directory);
        auto fs = std::make_shared<BlockingFileSystem>(directory->GetFileSystem());
        auto create_gate = fs->BlockNext(BlockingFileSystem::Operation::CREATE, /*path=*/"");
        if (fail_close) {
            auto close_gate = fs->BlockNext(BlockingFileSystem::Operation::CLOSE, /*path=*/"");
            close_gate->Release(Status::IOError("injected spill close failure"));
        }
        delegate_ = std::make_shared<ArrowRealtimeStore>(schema_, RealtimeStoreMode::APPEND_ONLY,
                                                         StatisticsMode::NONE, directory->Str(), fs,
                                                         "zstd", 1, pool_, arrow_pool_);
        ASSERT_OK_AND_ASSIGN(store_, ArrowDeduplicateRealtimeStore::Create(
                                         schema_, {"id"}, delegate_, pool_, arrow_pool_));
        ASSERT_OK(store_->AttachCommittedKeyOffsetLookup(empty_lookup_));
        ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[1, 1, "old"]])"),
                                         {RecordBatch::RowKind::INSERT}, OffsetRange(1, 2),
                                         RoaringBitmap64()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> pinned, store_->AcquireReadView());
        auto sealing = std::async(std::launch::async, [&]() { return store_->SealForCommit(); });
        std::future<Status> writing;
        // Release the I/O gate before joining either task, including on a failed assertion.
        ScopeGuard finish([&]() {
            create_gate->Release();
            if (writing.valid()) {
                writing.wait();
            }
            if (sealing.valid()) {
                sealing.wait();
            }
        });
        ASSERT_TRUE(create_gate->WaitForArrival());
        writing = std::async(std::launch::async, [&]() -> Status {
            RoaringBitmap64 previous;
            {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const KeyOffsetLookup> lookup,
                                       store_->AcquireKeyOffsetLookup());
                PAIMON_ASSIGN_OR_RAISE(previous, lookup->LookupOffsets(MakeKeys(R"([[1]])")));
            }
            if (previous.ToString() != "{1}") {
                return Status::Invalid("spilling segment disappeared from the key lookup");
            }
            return store_->WriteAndLookup(MakeBatch(R"([[2, 1, "new"]])"),
                                          {RecordBatch::RowKind::INSERT}, OffsetRange(2, 3),
                                          previous);
        });
        ASSERT_EQ(std::future_status::ready, writing.wait_for(std::chrono::seconds(5)));
        ASSERT_OK(writing.get());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> during_spill,
                             store_->AcquireReadView());
        ASSERT_OK_AND_ASSIGN(std::vector<int64_t> visible, ReadOffsets(during_spill));
        ASSERT_EQ((std::vector<int64_t>{2}), visible);
        ASSERT_OK_AND_ASSIGN(visible, ReadOffsets(pinned));
        ASSERT_EQ((std::vector<int64_t>{1}), visible);

        create_gate->Release();
        auto result = sealing.get();
        if (fail_close) {
            ASSERT_NOK_WITH_MSG(result, "injected spill close failure");
            ASSERT_OK_AND_ASSIGN(bool exists, fs->Exists(create_gate->MatchedPath()));
            ASSERT_FALSE(exists);
            ASSERT_GT(delegate_->GetDataUsage().sealed_memory_bytes, 0);
        } else {
            ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                                 std::move(result));
            ASSERT_TRUE(segment.has_value());
            ASSERT_EQ(1, segment.value()->GetRowCount());
            ASSERT_EQ(OffsetRange(1, 2), segment.value()->GetOffsetRange());
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> checkpoint,
                                 ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(segment.value()));
            ASSERT_TRUE(checkpoint->IsEmpty());
            ASSERT_EQ(0, delegate_->GetDataUsage().sealed_memory_bytes);
            ASSERT_GT(store_->GetDataUsage().sealed_memory_bytes, 0);
            ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                                 store_->CreateCommitReaders(segment.value()));
            ASSERT_EQ(1, readers.size());
            ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, readers[0]->NextBatch());
            ASSERT_FALSE(BatchReader::IsEofBatch(batch));
            auto imported = arrow::ImportArray(batch.first.get(), batch.second.get());
            ASSERT_TRUE(imported.ok()) << imported.status().ToString();
            const std::shared_ptr<arrow::Array> committed = std::move(imported).ValueOrDie();
            ASSERT_EQ(1, committed->length());
            const auto committed_rows = checked_pointer_cast<arrow::StructArray>(committed);
            const auto offsets = checked_pointer_cast<arrow::Int64Array>(
                committed_rows->GetFieldByName(SpecialFields::RealtimeOffset().Name()));
            ASSERT_EQ(1, offsets->Value(0));
            readers[0]->Close();
        }
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> after_spill,
                             store_->AcquireReadView());
        ASSERT_OK_AND_ASSIGN(visible, ReadOffsets(after_spill));
        ASSERT_EQ((std::vector<int64_t>{2}), visible);
        ASSERT_OK_AND_ASSIGN(visible, ReadOffsets(during_spill));
        ASSERT_EQ((std::vector<int64_t>{2}), visible);
    }
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestMemoryUsageIncludesIndexesAndDistinctBitmaps) {
    const uint64_t empty_index_bytes = store_->GetMemoryUsage();
    ASSERT_GT(empty_index_bytes, 0);
    ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[1, 1, "old"], [2, 1, "new"]])"),
                                     {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT},
                                     OffsetRange(1, 3), RoaringBitmap64()));
    RoaringBitmap64 rows;
    rows.Add(1);
    rows.Add(2);
    RoaringBitmap64 deleted;
    deleted.Add(1);
    RealtimeStoreDataUsage usage = store_->GetDataUsage();
    ASSERT_EQ(delegate_->GetDataUsage().building_memory_bytes + empty_index_bytes +
                  rows.GetSizeInBytes() + deleted.GetSizeInBytes(),
              usage.building_memory_bytes);
    ASSERT_EQ(0, usage.sealed_memory_bytes);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> pinned_view, store_->AcquireReadView());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> pinned_lookup,
                         store_->AcquireKeyOffsetLookup());
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> first,
                         store_->SealForCommit());
    ASSERT_TRUE(first.has_value());
    usage = store_->GetDataUsage();
    // Pending DV and the sealed checkpoint share one bitmap: charge it to sealed memory only.
    ASSERT_EQ(empty_index_bytes, usage.building_memory_bytes);
    ASSERT_EQ(delegate_->GetDataUsage().sealed_memory_bytes + empty_index_bytes +
                  deleted.GetSizeInBytes(),
              usage.sealed_memory_bytes);

    RoaringBitmap64 previous;
    previous.Add(2);
    ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[3, 1, "latest"]])"),
                                     {RecordBatch::RowKind::INSERT}, OffsetRange(3, 4), previous));
    rows = RoaringBitmap64();
    rows.Add(3);
    deleted.Add(2);
    usage = store_->GetDataUsage();
    ASSERT_EQ(delegate_->GetDataUsage().building_memory_bytes + empty_index_bytes +
                  rows.GetSizeInBytes() + deleted.GetSizeInBytes(),
              usage.building_memory_bytes);
    ASSERT_EQ(usage.building_memory_bytes + usage.sealed_memory_bytes, store_->GetMemoryUsage());
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> second,
                         store_->SealForCommit());
    ASSERT_TRUE(second.has_value());
    ASSERT_OK(store_->RetireCommittedOffsetAndLookup(4, empty_lookup_));
    usage = store_->GetDataUsage();
    ASSERT_EQ(empty_index_bytes, usage.building_memory_bytes);
    ASSERT_EQ(0, usage.sealed_memory_bytes);
    ASSERT_EQ(empty_index_bytes, store_->GetMemoryUsage());
    // Reclaimed data and index snapshots can remain pinned without entering the store metrics.
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> visible, ReadOffsets(pinned_view));
    ASSERT_EQ((std::vector<int64_t>{2}), visible);
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 pinned_offsets,
                         pinned_lookup->LookupOffsets(MakeKeys(R"([[1]])")));
    ASSERT_EQ("{2}", pinned_offsets.ToString());
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestDeleteOnlySegmentHasCheckpointWithoutData) {
    RoaringBitmap64 previous;
    previous.Add(99);
    ASSERT_OK(store_->WriteAndLookup(MakeBatch(R"([[100, 1, "delete"]])"),
                                     {RecordBatch::RowKind::DELETE}, OffsetRange(100, 101),
                                     previous));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<RealtimeSegmentHandle>> segment,
                         store_->SealForCommit());
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(0, segment.value()->GetRowCount());
    ASSERT_EQ(OffsetRange(100, 101), segment.value()->GetOffsetRange());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> checkpoint,
                         ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(segment.value()));
    ASSERT_EQ("{99}", checkpoint->ToString());
    ASSERT_OK_AND_ASSIGN(std::vector<std::unique_ptr<BatchReader>> readers,
                         store_->CreateCommitReaders(segment.value()));
    ASSERT_TRUE(readers.empty());
    ASSERT_EQ(0, store_->GetDataUsage().sealed_row_count);
    ASSERT_GT(store_->GetDataUsage().sealed_memory_bytes, previous.GetSizeInBytes());
}

TEST_F(ArrowDeduplicateRealtimeStoreTest, TestValidationAndSpecializedApis) {
    ASSERT_NOK_WITH_MSG(
        ArrowDeduplicateRealtimeStore::Create(schema_, {}, delegate_, pool_, arrow_pool_),
        "requires exactly one deduplicate key field");
    ASSERT_NOK_WITH_MSG(
        ArrowDeduplicateRealtimeStore::Create(arrow::schema({arrow::field("id", arrow::int64())}),
                                              {"id"}, delegate_, pool_, arrow_pool_),
        "requires non-null int64 _REALTIME_OFFSET");
    ASSERT_NOK_WITH_MSG(store_->Write(RealtimeWriteBatch{nullptr, OffsetRange(0, 1)}),
                        "Write requires key-offset lookup");
    ASSERT_NOK_WITH_MSG(store_->AdvanceCommittedOffset(1),
                        "requires an atomic key-offset lookup replacement");
    ASSERT_NOK_WITH_MSG(store_->RetireCommittedOffsetAndLookup(-1, empty_lookup_),
                        "must not be negative");
    ASSERT_NOK_WITH_MSG(store_->RetireCommittedOffsetAndLookup(1, nullptr), "must not be null");
    ASSERT_NOK_WITH_MSG(
        store_->AttachCommittedKeyOffsetLookup(std::make_shared<EmptyKeyOffsetLookup>()),
        "already attached");
}

}  // namespace paimon::test
