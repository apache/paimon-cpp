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

#include "paimon/core/realtime/arrow_deduplicate_realtime_store.h"

#include <algorithm>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/compute/api.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/realtime/realtime_utils.h"
#include "paimon/macros.h"
#include "paimon/metrics.h"
#include "paimon/record_batch.h"

namespace paimon {
namespace {

Result<std::shared_ptr<arrow::StructArray>> ProjectByName(
    const std::shared_ptr<arrow::StructArray>& source,
    const std::shared_ptr<arrow::Schema>& target_schema) {
    arrow::ArrayVector fields;
    fields.reserve(target_schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& field : target_schema->fields()) {
        std::shared_ptr<arrow::Array> array = source->GetFieldByName(field->name());
        if (!array || !array->type()->Equals(field->type())) {
            return Status::Invalid("real-time deduplicate reader cannot project field: ",
                                   field->name());
        }
        fields.push_back(std::move(array));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> result,
                                      arrow::StructArray::Make(fields, target_schema->fields()));
    return result;
}

Result<BatchReader::ReadBatch> ExportBatch(const std::shared_ptr<arrow::StructArray>& data,
                                           const std::shared_ptr<arrow::MemoryPool>& pool) {
    auto array = std::make_unique<ArrowArray>();
    auto schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*data, array.get(), schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(array.get(), schema.get(), pool));
    return std::make_pair(std::move(array), std::move(schema));
}

Result<std::shared_ptr<arrow::StructArray>> FilterDeletedOffsets(
    const std::shared_ptr<arrow::StructArray>& source,
    const std::shared_ptr<const RoaringBitmap64>& deleted_offsets,
    const std::shared_ptr<arrow::MemoryPool>& pool) {
    if (!deleted_offsets || deleted_offsets->IsEmpty() || source->length() == 0) {
        return source;
    }
    std::shared_ptr<arrow::Array> offset_array =
        source->GetFieldByName(SpecialFields::RealtimeOffset().Name());
    if (!offset_array || offset_array->type_id() != arrow::Type::INT64 ||
        offset_array->null_count() != 0) {
        return Status::Invalid(
            "real-time deduplicate data must contain non-null int64 _REALTIME_OFFSET");
    }
    std::shared_ptr<arrow::Int64Array> offsets =
        checked_pointer_cast<arrow::Int64Array>(offset_array);
    arrow::BooleanBuilder selection(pool.get());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(selection.Reserve(source->length()));
    // TODO(xinyu.lxy): Merge-scan the ordered offsets with a RoaringBitmap64 iterator instead of
    // calling Contains for every row.
    for (int64_t row = 0; row < source->length(); ++row) {
        selection.UnsafeAppend(!deleted_offsets->Contains(offsets->Value(row)));
    }
    std::shared_ptr<arrow::Array> selection_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(selection.Finish(&selection_array));
    arrow::compute::ExecContext exec_context(pool.get());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow::Datum filtered,
        arrow::compute::Filter(source, selection_array, arrow::compute::FilterOptions::Defaults(),
                               &exec_context));
    return checked_pointer_cast<arrow::StructArray>(filtered.make_array());
}

}  // namespace

class ArrowDeduplicateRealtimeStore::SegmentHandle final : public RealtimeSegmentHandle {
 public:
    SegmentHandle(std::shared_ptr<RealtimeSegmentHandle> delegate, OffsetRange offset_range,
                  int64_t row_count, std::shared_ptr<const RoaringBitmap64> offset_deletions)
        : delegate_(std::move(delegate)),
          offset_range_(offset_range),
          row_count_(row_count),
          offset_deletions_(std::move(offset_deletions)) {}

    OffsetRange GetOffsetRange() const override {
        return offset_range_;
    }

    int64_t GetRowCount() const override {
        return row_count_;
    }

    const std::shared_ptr<RealtimeSegmentHandle>& Delegate() const {
        return delegate_;
    }

    const std::shared_ptr<const RoaringBitmap64>& OffsetDeletions() const {
        return offset_deletions_;
    }

 private:
    std::shared_ptr<RealtimeSegmentHandle> delegate_;
    OffsetRange offset_range_;
    int64_t row_count_;
    std::shared_ptr<const RoaringBitmap64> offset_deletions_;
};

class ArrowDeduplicateRealtimeStore::ReadView final : public RealtimeReadView {
 public:
    ReadView(std::shared_ptr<RealtimeReadView> delegate, std::optional<OffsetRange> offset_range,
             std::shared_ptr<const RoaringBitmap64> offset_deletions)
        : delegate_(std::move(delegate)),
          offset_range_(offset_range),
          offset_deletions_(std::move(offset_deletions)) {}

    std::optional<OffsetRange> GetOffsetRange() const override {
        return offset_range_;
    }

    const std::shared_ptr<RealtimeReadView>& Delegate() const {
        return delegate_;
    }

    const std::shared_ptr<const RoaringBitmap64>& OffsetDeletions() const {
        return offset_deletions_;
    }

 private:
    std::shared_ptr<RealtimeReadView> delegate_;
    std::optional<OffsetRange> offset_range_;
    std::shared_ptr<const RoaringBitmap64> offset_deletions_;
};

class ArrowDeduplicateRealtimeStore::OffsetFilteringBatchReader final : public BatchReader {
 public:
    OffsetFilteringBatchReader(std::unique_ptr<BatchReader> inner,
                               std::shared_ptr<arrow::Schema> output_schema,
                               std::shared_ptr<const RoaringBitmap64> offset_deletions,
                               std::shared_ptr<arrow::MemoryPool> arrow_pool)
        : inner_(std::move(inner)),
          output_schema_(std::move(output_schema)),
          offset_deletions_(std::move(offset_deletions)),
          arrow_pool_(std::move(arrow_pool)) {}

    Result<ReadBatch> NextBatch() override {
        while (inner_) {
            PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, inner_->NextBatch());
            if (BatchReader::IsEofBatch(batch)) {
                return batch;
            }
            auto& [c_array, c_schema] = batch;
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> imported,
                                              arrow::ImportArray(c_array.get(), c_schema.get()));
            if (!imported || imported->type_id() != arrow::Type::STRUCT) {
                return Status::Invalid("real-time deduplicate reader returned a non-StructArray");
            }
            std::shared_ptr<arrow::StructArray> source =
                checked_pointer_cast<arrow::StructArray>(imported);
            PAIMON_ASSIGN_OR_RAISE(source,
                                   FilterDeletedOffsets(source, offset_deletions_, arrow_pool_));
            if (source->length() == 0) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::StructArray> projected,
                                   ProjectByName(source, output_schema_));
            return ExportBatch(projected, arrow_pool_);
        }
        return MakeEofBatch();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return inner_ ? inner_->GetReaderMetrics() : std::make_shared<MetricsImpl>();
    }

    void Close() override {
        if (inner_) {
            inner_->Close();
            inner_.reset();
        }
    }

 private:
    std::unique_ptr<BatchReader> inner_;
    std::shared_ptr<arrow::Schema> output_schema_;
    std::shared_ptr<const RoaringBitmap64> offset_deletions_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

Result<std::shared_ptr<ArrowDeduplicateRealtimeStore>> ArrowDeduplicateRealtimeStore::Create(
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::vector<std::string>& business_key_fields,
    const std::shared_ptr<RealtimeStore>& delegate, const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (!write_schema || !delegate || !memory_pool || !arrow_pool) {
        return Status::Invalid("real-time deduplicate store is missing a required dependency");
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t key_position, RealtimeUtils::GetDeduplicateBusinessKeyPosition(
                                                     write_schema, business_key_fields));
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(write_schema));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<MutableKeyOffsetIndex> building_key_index,
        MutableKeyOffsetIndex::Create(write_schema->field(key_position), memory_pool));
    return std::shared_ptr<ArrowDeduplicateRealtimeStore>(new ArrowDeduplicateRealtimeStore(
        write_schema, key_position, write_schema->field(key_position),
        std::move(building_key_index), delegate, memory_pool, arrow_pool));
}

ArrowDeduplicateRealtimeStore::ArrowDeduplicateRealtimeStore(
    const std::shared_ptr<arrow::Schema>& write_schema, int32_t key_field_position,
    const std::shared_ptr<arrow::Field>& key_field,
    std::shared_ptr<MutableKeyOffsetIndex> building_key_index,
    const std::shared_ptr<RealtimeStore>& delegate, const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : write_schema_(write_schema),
      key_field_position_(key_field_position),
      key_field_(key_field),
      delegate_(delegate),
      memory_pool_(memory_pool),
      arrow_pool_(arrow_pool),
      building_key_index_(std::move(building_key_index)),
      pending_offset_deletions_(std::make_shared<RoaringBitmap64>()) {}

Status ArrowDeduplicateRealtimeStore::Write(RealtimeWriteBatch&&) {
    return Status::Invalid(
        "deduplicate store Write requires key-offset lookup; use RealtimeDeduplicateWriter");
}

Result<std::shared_ptr<arrow::StructArray>> ArrowDeduplicateRealtimeStore::ExtractKeys(
    const std::shared_ptr<arrow::StructArray>& data) const {
    if (!data || !data->type()->Equals(arrow::struct_(write_schema_->fields()))) {
        return Status::Invalid("deduplicate write batch does not match store schema");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> keys,
        arrow::StructArray::Make({data->field(key_field_position_)}, {key_field_}));
    return keys;
}

Result<std::shared_ptr<const KeyOffsetLookup>>
ArrowDeduplicateRealtimeStore::AcquireKeyOffsetLookup() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!building_key_index_) {
        return Status::Invalid("deduplicate building key-offset index is not initialized");
    }
    std::vector<std::shared_ptr<const KeyOffsetLookup>> lookups;
    if (committed_key_lookup_) {
        lookups.push_back(committed_key_lookup_);
    }
    lookups.reserve(lookups.size() + sealed_states_.size() + 1);
    for (const SealedState& sealed : sealed_states_) {
        lookups.push_back(sealed.key_lookup);
    }
    lookups.push_back(building_key_index_->Snapshot());
    return std::shared_ptr<const KeyOffsetLookup>(new CompositeKeyOffsetLookup(std::move(lookups)));
}

Status ArrowDeduplicateRealtimeStore::EnsureMutableDeletionBitmap() {
    if (!pending_offset_deletions_.unique()) {
        pending_offset_deletions_ = std::make_shared<RoaringBitmap64>(*pending_offset_deletions_);
    }
    return Status::OK();
}

Status ArrowDeduplicateRealtimeStore::WriteAndLookup(
    const std::shared_ptr<arrow::StructArray>& data,
    const std::vector<RecordBatch::RowKind>& row_kinds, const OffsetRange& offset_range,
    const RoaringBitmap64& previous_offsets) {
    if (!data || data->length() <= 0 || static_cast<int64_t>(row_kinds.size()) != data->length()) {
        return Status::Invalid("deduplicate write data and row kinds are not aligned");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::StructArray> keys, ExtractKeys(data));
    std::shared_ptr<arrow::Array> raw_offsets =
        data->GetFieldByName(SpecialFields::RealtimeOffset().Name());
    if (!raw_offsets || raw_offsets->type_id() != arrow::Type::INT64 ||
        raw_offsets->null_count() != 0) {
        return Status::Invalid("deduplicate write requires non-null int64 offsets");
    }
    std::shared_ptr<arrow::Int64Array> offsets =
        checked_pointer_cast<arrow::Int64Array>(raw_offsets);

    std::lock_guard<std::mutex> lock(mutex_);
    if (!building_key_index_) {
        return Status::Invalid("deduplicate building key-offset index is not initialized");
    }
    if (building_offset_range_ && offset_range.begin < building_offset_range_->end) {
        return Status::Invalid("real-time offset ranges must be ordered and non-overlapping");
    }
    PAIMON_RETURN_NOT_OK(EnsureMutableDeletionBitmap());
    *pending_offset_deletions_ |= previous_offsets;

    arrow::BooleanBuilder add_selection(arrow_pool_.get());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(add_selection.Reserve(data->length()));
    int64_t add_count = 0;
    for (int64_t row = 0; row < data->length(); ++row) {
        std::shared_ptr<arrow::StructArray> one_key =
            checked_pointer_cast<arrow::StructArray>(keys->Slice(row, 1));
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 earlier_in_memory,
                               building_key_index_->LookupOffsets(one_key));
        *pending_offset_deletions_ |= earlier_in_memory;

        const RecordBatch::RowKind row_kind = row_kinds[row];
        const bool is_add = row_kind == RecordBatch::RowKind::INSERT ||
                            row_kind == RecordBatch::RowKind::UPDATE_AFTER;
        const bool is_delete = row_kind == RecordBatch::RowKind::DELETE ||
                               row_kind == RecordBatch::RowKind::UPDATE_BEFORE;
        if (!is_add && !is_delete) {
            return Status::Invalid("unknown row kind in real-time deduplicate write");
        }
        add_selection.UnsafeAppend(is_add);
        if (!is_add) {
            PAIMON_RETURN_NOT_OK(building_key_index_->Erase(one_key));
            continue;
        }
        std::shared_ptr<arrow::Int64Array> one_offset =
            checked_pointer_cast<arrow::Int64Array>(offsets->Slice(row, 1));
        PAIMON_RETURN_NOT_OK(building_key_index_->Add(one_key, one_offset));
        building_row_offsets_.Add(offsets->Value(row));
        ++add_count;
    }

    if (add_count > 0) {
        std::shared_ptr<arrow::Array> selection;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(add_selection.Finish(&selection));
        arrow::compute::ExecContext exec_context(arrow_pool_.get());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            arrow::Datum filtered,
            arrow::compute::Filter(data, selection, arrow::compute::FilterOptions::Defaults(),
                                   &exec_context));
        std::shared_ptr<arrow::StructArray> additions =
            checked_pointer_cast<arrow::StructArray>(filtered.make_array());
        auto c_array = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*additions, c_array.get()));
        PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), /*schema=*/nullptr, arrow_pool_));
        RecordBatchBuilder builder(c_array.get());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> append_batch, builder.Finish());
        PAIMON_RETURN_NOT_OK(
            delegate_->Write(RealtimeWriteBatch{std::move(append_batch), offset_range}));
    }
    if (!building_offset_range_) {
        building_offset_range_ = offset_range;
    } else {
        building_offset_range_ = OffsetRange(building_offset_range_->begin, offset_range.end);
    }
    return Status::OK();
}

Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>>
ArrowDeduplicateRealtimeStore::SealForCommit() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!building_offset_range_) {
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
    }
    // Delegate rotation and the key/deletion state rotation must be atomic. Deduplicate mode
    // currently rejects real-time spill, so the delegate only performs its short in-memory
    // rotation here.
    // TODO(xinyu.lxy): Introduce two-phase store sealing before allowing deduplicate spill, so
    // delegate spill I/O can complete without holding this mutex.
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<RealtimeSegmentHandle>> delegate_segment,
                           delegate_->SealForCommit());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MutableKeyOffsetIndex> next_building_index,
                           MutableKeyOffsetIndex::Create(key_field_, memory_pool_));
    std::shared_ptr<const RoaringBitmap64> checkpoint = pending_offset_deletions_;
    RoaringBitmap64 live_rows =
        RoaringBitmap64::AndNot(building_row_offsets_, *pending_offset_deletions_);
    auto handle = std::make_shared<SegmentHandle>(delegate_segment.value_or(nullptr),
                                                  building_offset_range_.value(),
                                                  live_rows.Cardinality(), checkpoint);
    sealed_states_.push_back(
        SealedState{building_offset_range_.value(), building_key_index_->Snapshot(), checkpoint});
    building_key_index_ = std::move(next_building_index);
    building_row_offsets_ = RoaringBitmap64();
    building_offset_range_.reset();
    return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(std::move(handle));
}

Result<std::shared_ptr<const RoaringBitmap64>> ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    std::shared_ptr<SegmentHandle> handle = std::dynamic_pointer_cast<SegmentHandle>(segment);
    if (!handle) {
        return Status::Invalid("segment was not created by the deduplicate real-time store");
    }
    return handle->OffsetDeletions();
}

Result<std::shared_ptr<const RoaringBitmap64>> ArrowDeduplicateRealtimeStore::OffsetDeletionsOf(
    const std::shared_ptr<RealtimeReadView>& view) {
    std::shared_ptr<ReadView> deduplicate_view = std::dynamic_pointer_cast<ReadView>(view);
    if (!deduplicate_view) {
        return Status::Invalid("read view was not created by the deduplicate real-time store");
    }
    return deduplicate_view->OffsetDeletions();
}

Result<std::vector<std::unique_ptr<BatchReader>>>
ArrowDeduplicateRealtimeStore::CreateCommitReaders(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    std::shared_ptr<SegmentHandle> handle = std::dynamic_pointer_cast<SegmentHandle>(segment);
    if (!handle) {
        return Status::Invalid("segment was not created by the deduplicate real-time store");
    }
    if (!handle->Delegate()) {
        return std::vector<std::unique_ptr<BatchReader>>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<BatchReader>> inner,
                           delegate_->CreateCommitReaders(handle->Delegate()));
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.reserve(inner.size());
    for (std::unique_ptr<BatchReader>& reader : inner) {
        readers.push_back(std::make_unique<OffsetFilteringBatchReader>(
            std::move(reader), write_schema_, handle->OffsetDeletions(), arrow_pool_));
    }
    return readers;
}

Result<std::shared_ptr<RealtimeReadView>> ArrowDeduplicateRealtimeStore::AcquireReadView() {
    std::lock_guard<std::mutex> lock(mutex_);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeReadView> delegate_view,
                           delegate_->AcquireReadView());
    std::optional<OffsetRange> range;
    if (!sealed_states_.empty()) {
        range = OffsetRange(sealed_states_.front().offset_range.begin,
                            sealed_states_.back().offset_range.end);
    }
    if (building_offset_range_) {
        range =
            range
                ? std::optional<OffsetRange>(OffsetRange(range->begin, building_offset_range_->end))
                : building_offset_range_;
    }
    return std::shared_ptr<RealtimeReadView>(
        new ReadView(std::move(delegate_view), range, pending_offset_deletions_));
}

Result<std::vector<std::unique_ptr<BatchReader>>> ArrowDeduplicateRealtimeStore::CreateQueryReaders(
    const std::shared_ptr<RealtimeReadView>& view, const RealtimeQueryContext& context) {
    std::shared_ptr<ReadView> deduplicate_view = std::dynamic_pointer_cast<ReadView>(view);
    if (!deduplicate_view) {
        return Status::Invalid("read view was not created by the deduplicate real-time store");
    }
    if (!context.read_schema || !context.read_schema->release) {
        return Status::Invalid("deduplicate query read schema is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> output_schema,
                                      arrow::ImportSchema(context.read_schema));
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(output_schema));
    ArrowSchema inner_c_schema;
    ArrowSchemaMarkReleased(&inner_c_schema);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*output_schema, &inner_c_schema));
    RealtimeQueryContext inner_context{&inner_c_schema, context.predicate, context.read_batch_size};
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<BatchReader>> inner,
        delegate_->CreateQueryReaders(deduplicate_view->Delegate(), inner_context));
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.reserve(inner.size());
    for (std::unique_ptr<BatchReader>& reader : inner) {
        readers.push_back(std::make_unique<OffsetFilteringBatchReader>(
            std::move(reader), output_schema, deduplicate_view->OffsetDeletions(), arrow_pool_));
    }
    return readers;
}

Status ArrowDeduplicateRealtimeStore::AdvanceCommittedOffset(int64_t) {
    return Status::Invalid(
        "deduplicate committed advancement requires an atomic key-offset lookup replacement");
}

Status ArrowDeduplicateRealtimeStore::AdvanceCommittedOffsetAndLookup(
    int64_t committed_end_offset, const std::shared_ptr<const KeyOffsetLookup>& committed_lookup) {
    if (committed_end_offset < 0) {
        return Status::Invalid("committed real-time offset must not be negative");
    }
    if (!committed_lookup) {
        return Status::Invalid("committed key-offset lookup must not be null");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    PAIMON_RETURN_NOT_OK(delegate_->AdvanceCommittedOffset(committed_end_offset));
    std::shared_ptr<const RoaringBitmap64> applied_checkpoint;
    size_t reclaimed_count = 0;
    while (reclaimed_count < sealed_states_.size() &&
           sealed_states_[reclaimed_count].offset_range.end <= committed_end_offset) {
        applied_checkpoint = sealed_states_[reclaimed_count].offset_deletions;
        ++reclaimed_count;
    }
    if (reclaimed_count > 0) {
        sealed_states_.erase(sealed_states_.begin(), sealed_states_.begin() + reclaimed_count);
    }
    if (applied_checkpoint) {
        PAIMON_RETURN_NOT_OK(EnsureMutableDeletionBitmap());
        *pending_offset_deletions_ -= *applied_checkpoint;
    }
    committed_key_lookup_ = committed_lookup;
    return Status::OK();
}

Status ArrowDeduplicateRealtimeStore::AttachCommittedKeyOffsetLookup(
    const std::shared_ptr<const KeyOffsetLookup>& committed_lookup) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (committed_key_lookup_ && committed_key_lookup_ != committed_lookup) {
        return Status::Invalid(
            "committed key-offset lookup is already attached to deduplicate store");
    }
    committed_key_lookup_ = committed_lookup;
    return Status::OK();
}

RealtimeStoreDataUsage ArrowDeduplicateRealtimeStore::GetDataUsage() const {
    return delegate_->GetDataUsage();
}

uint64_t ArrowDeduplicateRealtimeStore::GetMemoryUsage() const {
    return delegate_->GetMemoryUsage();
}

}  // namespace paimon
