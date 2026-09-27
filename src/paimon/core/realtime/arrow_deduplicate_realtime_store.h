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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/realtime/realtime_key_offset_index.h"
#include "paimon/realtime/realtime_store.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace arrow {
class MemoryPool;
class Schema;
class StructArray;
class Field;
}  // namespace arrow

namespace paimon {

class MemoryPool;

/// Framework-owned Arrow deduplication semantics over a pluggable append-store delegate.
///
/// The user-defined key is an append-table key, not a Paimon primary key. Every add row keeps its
/// `_REALTIME_OFFSET` in memory and in the eventual data file. A read view pins one immutable
/// bitmap, so later writes cannot change the set of rows visible to that query. The delegate owns
/// physical buffering, readers, and spill; this wrapper always owns key indexes and offset DVs.
class ArrowDeduplicateRealtimeStore final : public RealtimeStore {
 public:
    static Result<std::shared_ptr<ArrowDeduplicateRealtimeStore>> Create(
        const std::shared_ptr<arrow::Schema>& write_schema,
        const std::vector<std::string>& business_key_fields,
        const std::shared_ptr<RealtimeStore>& delegate,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    /// The generic API cannot perform the committed `.offset` lookup. Deduplicate writers must
    /// call `AcquireKeyOffsetLookup`, perform lookup outside the store lock, then call
    /// `WriteAndLookup`.
    Status Write(RealtimeWriteBatch&& write_batch) override;

    /// Pins the exact committed, sealed and building key indexes used by one write. File I/O is
    /// intentionally performed by the caller after this short critical section.
    Result<std::shared_ptr<const KeyOffsetLookup>> AcquireKeyOffsetLookup();

    /// Applies last-write-wins semantics. `previous_offsets` is the result from the pinned lookup;
    /// duplicates inside this batch are resolved again in row order.
    Status WriteAndLookup(const std::shared_ptr<arrow::StructArray>& data,
                          const std::vector<RecordBatch::RowKind>& row_kinds,
                          const OffsetRange& offset_range, const RoaringBitmap64& previous_offsets);

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() override;

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) override;

    Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() override;

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view,
        const RealtimeQueryContext& context) override;

    /// DEDUPLICATE advancement also has to atomically replace the committed `.offset` lookup.
    /// Calling the base notification would create a query/write cutover with mismatched state.
    Status AdvanceCommittedOffset(int64_t committed_end_offset) override;

    Status AdvanceCommittedOffsetAndLookup(
        int64_t committed_end_offset,
        const std::shared_ptr<const KeyOffsetLookup>& committed_lookup);

    Status AttachCommittedKeyOffsetLookup(
        const std::shared_ptr<const KeyOffsetLookup>& committed_lookup);

    RealtimeStoreDataUsage GetDataUsage() const override;
    uint64_t GetMemoryUsage() const override;

    static Result<std::shared_ptr<const RoaringBitmap64>> OffsetDeletionsOf(
        const std::shared_ptr<RealtimeSegmentHandle>& segment);
    static Result<std::shared_ptr<const RoaringBitmap64>> OffsetDeletionsOf(
        const std::shared_ptr<RealtimeReadView>& view);

 private:
    struct SealedState {
        OffsetRange offset_range;
        std::shared_ptr<const KeyOffsetLookup> key_lookup;
        std::shared_ptr<const RoaringBitmap64> offset_deletions;
    };

    class SegmentHandle;
    class ReadView;
    class OffsetFilteringBatchReader;

    ArrowDeduplicateRealtimeStore(const std::shared_ptr<arrow::Schema>& write_schema,
                                  int32_t key_field_position,
                                  const std::shared_ptr<arrow::Field>& key_field,
                                  std::shared_ptr<MutableKeyOffsetIndex> building_key_index,
                                  const std::shared_ptr<RealtimeStore>& delegate,
                                  const std::shared_ptr<MemoryPool>& memory_pool,
                                  const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Result<std::shared_ptr<arrow::StructArray>> ExtractKeys(
        const std::shared_ptr<arrow::StructArray>& data) const;
    Status EnsureMutableDeletionBitmap();

    std::shared_ptr<arrow::Schema> write_schema_;
    int32_t key_field_position_;
    std::shared_ptr<arrow::Field> key_field_;
    std::shared_ptr<RealtimeStore> delegate_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;

    mutable std::mutex mutex_;
    std::shared_ptr<MutableKeyOffsetIndex> building_key_index_;
    std::shared_ptr<const KeyOffsetLookup> committed_key_lookup_;
    std::vector<SealedState> sealed_states_;
    std::shared_ptr<RoaringBitmap64> pending_offset_deletions_;
    RoaringBitmap64 building_row_offsets_;
    std::optional<OffsetRange> building_offset_range_;
};

}  // namespace paimon
