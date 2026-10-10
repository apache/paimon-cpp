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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/realtime/arrow_realtime_store.h"
#include "paimon/macros.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class EmptyKeyOffsetLookup final : public KeyOffsetLookup {
 public:
    Result<RoaringBitmap64> LookupOffsets(
        const std::shared_ptr<arrow::StructArray>&) const override {
        return RoaringBitmap64();
    }
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

    ASSERT_OK(store_->AdvanceCommittedOffsetAndLookup(/*committed_end_offset=*/5, empty_lookup_));
    usage = store_->GetDataUsage();
    ASSERT_EQ(2, usage.sealed_row_count);

    ASSERT_OK(store_->AdvanceCommittedOffsetAndLookup(/*committed_end_offset=*/10, empty_lookup_));
    usage = store_->GetDataUsage();
    ASSERT_EQ(1, usage.sealed_row_count);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> retained_view,
                         store_->AcquireReadView());
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(10, 20)), retained_view->GetOffsetRange());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const RoaringBitmap64> retained_deletions,
                         ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(retained_view));
    ASSERT_EQ("{2}", retained_deletions->ToString());

    ASSERT_OK(store_->AdvanceCommittedOffsetAndLookup(/*committed_end_offset=*/15, empty_lookup_));
    usage = store_->GetDataUsage();
    ASSERT_EQ(1, usage.sealed_row_count);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeReadView> crossing_view,
                         store_->AcquireReadView());
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(10, 20)), crossing_view->GetOffsetRange());

    ASSERT_OK(store_->AdvanceCommittedOffsetAndLookup(/*committed_end_offset=*/20, empty_lookup_));
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
    ASSERT_NOK_WITH_MSG(store_->AdvanceCommittedOffsetAndLookup(-1, empty_lookup_),
                        "must not be negative");
    ASSERT_NOK_WITH_MSG(store_->AdvanceCommittedOffsetAndLookup(1, nullptr), "must not be null");
    ASSERT_NOK_WITH_MSG(
        store_->AttachCommittedKeyOffsetLookup(std::make_shared<EmptyKeyOffsetLookup>()),
        "already attached");
}

}  // namespace paimon::test
