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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/core/realtime/arrow_mem_indexer.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/complete_row_kind_batch_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/macros.h"

namespace paimon {
namespace {

uint64_t GetArrayMemoryUsage(const std::shared_ptr<arrow::ArrayData>& data) {
    uint64_t result = 0;
    for (const std::shared_ptr<arrow::Buffer>& buffer : data->buffers) {
        if (buffer) {
            result += static_cast<uint64_t>(buffer->size());
        }
    }
    for (const std::shared_ptr<arrow::ArrayData>& child : data->child_data) {
        result += GetArrayMemoryUsage(child);
    }
    if (data->dictionary) {
        result += GetArrayMemoryUsage(data->dictionary);
    }
    return result;
}

}  // namespace

class ArrowMemIndexer::Segment : public RealtimeSegmentHandle {
 public:
    Segment(const Range& offset_range, std::vector<StoredBatch>&& batches)
        : offset_range_(offset_range), batches_(std::move(batches)) {}

    Range GetOffsetRange() const override {
        return offset_range_;
    }

    const std::vector<StoredBatch>& GetBatches() const {
        return batches_;
    }

    uint64_t GetMemoryUsage() const {
        uint64_t result = 0;
        for (const StoredBatch& batch : batches_) {
            result += batch.memory_usage;
        }
        return result;
    }

 private:
    Range offset_range_;
    std::vector<StoredBatch> batches_;
};

class ArrowMemIndexer::ReadView : public MemReadView {
 public:
    explicit ReadView(std::vector<StoredBatch>&& batches) : batches_(std::move(batches)) {
        if (!batches_.empty()) {
            offset_range_ =
                Range(batches_.front().offset_range.from, batches_.back().offset_range.to);
        }
    }

    std::optional<Range> GetOffsetRange() const override {
        return offset_range_;
    }

    const std::vector<StoredBatch>& GetBatches() const {
        return batches_;
    }

 private:
    std::vector<StoredBatch> batches_;
    std::optional<Range> offset_range_;
};

class ArrowMemIndexer::CommitBatchReader : public BatchReader {
 public:
    CommitBatchReader(const std::shared_ptr<Segment>& segment,
                      const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : segment_(segment), arrow_pool_(arrow_pool), metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (!segment_ || next_batch_ >= segment_->GetBatches().size()) {
            return MakeEofBatch();
        }
        const StoredBatch& stored = segment_->GetBatches()[next_batch_++];
        int64_t row_count = stored.data->length();

        arrow::Int8Builder row_kind_builder(arrow_pool_.get());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(row_kind_builder.Reserve(row_count));
        if (stored.row_kinds.empty()) {
            for (int64_t i = 0; i < row_count; ++i) {
                row_kind_builder.UnsafeAppend(static_cast<int8_t>(RecordBatch::RowKind::INSERT));
            }
        } else {
            for (RecordBatch::RowKind row_kind : stored.row_kinds) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    row_kind_builder.Append(static_cast<int8_t>(row_kind)));
            }
        }
        std::shared_ptr<arrow::Array> row_kind_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(row_kind_builder.Finish(&row_kind_array));

        arrow::ArrayVector fields = {row_kind_array};
        fields.insert(fields.end(), stored.data->fields().begin(), stored.data->fields().end());
        arrow::FieldVector schema_fields = {
            DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())};
        const arrow::FieldVector& data_fields = stored.data->struct_type()->fields();
        schema_fields.insert(schema_fields.end(), data_fields.begin(), data_fields.end());

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> result,
                                          arrow::StructArray::Make(fields, schema_fields));
        auto c_array = std::make_unique<ArrowArray>();
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*result, c_array.get(), c_schema.get()));
        return ReadBatch(std::move(c_array), std::move(c_schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        segment_.reset();
    }

 private:
    std::shared_ptr<Segment> segment_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> metrics_;
    size_t next_batch_ = 0;
};

class ArrowMemIndexer::QueryBatchReader : public BatchReader {
 public:
    QueryBatchReader(const ReadView* view, int64_t offset_lower_exclusive,
                     const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : view_(view),
          offset_lower_exclusive_(offset_lower_exclusive),
          read_schema_(read_schema),
          arrow_pool_(arrow_pool),
          metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        return Status::Invalid(
            "paimon inner reader ArrowMemIndexer::QueryBatchReader should use "
            "NextBatchWithBitmap");
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        // TODO(xinyu.lxy): Memory query reads return complete stored write batches and
        // intentionally ignore the configured read batch size.
        if (offset_lower_exclusive_ == std::numeric_limits<int64_t>::max()) {
            return MakeEofBatchWithBitmap();
        }
        while (view_ && next_batch_ < view_->GetBatches().size()) {
            const StoredBatch& stored = view_->GetBatches()[next_batch_++];
            if (stored.offset_range.to <= offset_lower_exclusive_) {
                continue;
            }
            int64_t begin =
                std::max<int64_t>(0, offset_lower_exclusive_ + 1 - stored.offset_range.from);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::StructArray> output, BuildOutput(stored));
            RoaringBitmap32 candidate_rows;
            candidate_rows.AddRange(static_cast<int32_t>(begin),
                                    static_cast<int32_t>(stored.data->length()));
            auto c_array = std::make_unique<ArrowArray>();
            auto c_schema = std::make_unique<ArrowSchema>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                arrow::ExportArray(*output, c_array.get(), c_schema.get()));
            return ReadBatchWithBitmap(ReadBatch(std::move(c_array), std::move(c_schema)),
                                       std::move(candidate_rows));
        }
        return MakeEofBatchWithBitmap();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        view_ = nullptr;
    }

 private:
    Result<std::shared_ptr<arrow::StructArray>> BuildOutput(const StoredBatch& stored) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Array> projected,
            NestedProjectionUtils::AlignArrayToReadType(
                stored.data, arrow::struct_(read_schema_->fields()), arrow_pool_.get()));
        if (!projected || projected->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("memory query projection did not produce a StructArray");
        }
        std::shared_ptr<arrow::StructArray> projected_struct =
            checked_pointer_cast<arrow::StructArray>(projected);
        return projected_struct;
    }

 private:
    const ReadView* view_;
    int64_t offset_lower_exclusive_;
    std::shared_ptr<arrow::Schema> read_schema_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> metrics_;
    size_t next_batch_ = 0;
};

ArrowMemIndexer::ArrowMemIndexer(const std::shared_ptr<arrow::Schema>& write_schema,
                                 const std::shared_ptr<MemoryPool>& memory_pool,
                                 const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : write_schema_(write_schema), memory_pool_(memory_pool), arrow_pool_(arrow_pool) {}

Status ArrowMemIndexer::Write(RealtimeWriteBatch&& write_batch) {
    if (!write_batch.batch) {
        return Status::Invalid("real-time write batch is null");
    }
    int64_t row_count = write_batch.batch->GetData()->length;
    if (write_batch.offset_range.Count() != row_count) {
        return Status::Invalid("real-time offset range does not match batch row count");
    }
    if (!write_batch.batch->GetRowKind().empty() &&
        static_cast<int64_t>(write_batch.batch->GetRowKind().size()) != row_count) {
        return Status::Invalid("real-time row-kind count does not match batch row count");
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> data,
        arrow::ImportArray(write_batch.batch->GetData(), arrow::struct_(write_schema_->fields())));
    if (!data || data->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("real-time write data is not a StructArray");
    }
    std::shared_ptr<arrow::StructArray> struct_array =
        checked_pointer_cast<arrow::StructArray>(data);

    std::lock_guard<std::mutex> lock(mutex_);
    if (building_range_ && write_batch.offset_range.from != building_range_->to + 1) {
        return Status::Invalid("real-time offset ranges must be contiguous");
    }
    uint64_t memory_usage = GetArrayMemoryUsage(struct_array->data());
    building_memory_usage_ += memory_usage;
    building_batches_.push_back(StoredBatch{std::move(struct_array),
                                            write_batch.batch->GetRowKind(),
                                            write_batch.offset_range, memory_usage});
    if (!building_range_) {
        building_range_ = write_batch.offset_range;
    } else {
        building_range_ = Range(building_range_->from, write_batch.offset_range.to);
    }
    return Status::OK();
}

Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> ArrowMemIndexer::SealForCommit() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (building_batches_.empty()) {
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
    }
    auto segment = std::make_shared<Segment>(building_range_.value(), std::move(building_batches_));
    sealed_segments_.push_back(segment);
    building_batches_.clear();
    building_range_.reset();
    building_memory_usage_ = 0;
    return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(std::move(segment));
}

Result<std::vector<std::unique_ptr<BatchReader>>> ArrowMemIndexer::CreateCommitReaders(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    std::shared_ptr<Segment> arrow_segment = std::dynamic_pointer_cast<Segment>(segment);
    if (!arrow_segment) {
        return Status::Invalid("segment was not created by the Arrow mem indexer");
    }
    std::vector<std::unique_ptr<BatchReader>> readers;
    readers.push_back(std::make_unique<CommitBatchReader>(arrow_segment, arrow_pool_));
    return readers;
}

Result<std::shared_ptr<MemReadView>> ArrowMemIndexer::AcquireReadView() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<StoredBatch> batches;
    for (const std::shared_ptr<Segment>& segment : sealed_segments_) {
        const std::vector<StoredBatch>& segment_batches = segment->GetBatches();
        batches.insert(batches.end(), segment_batches.begin(), segment_batches.end());
    }
    batches.insert(batches.end(), building_batches_.begin(), building_batches_.end());
    return std::shared_ptr<MemReadView>(new ReadView(std::move(batches)));
}

Result<std::vector<std::unique_ptr<RealtimeReader>>> ArrowMemIndexer::CreateQueryReaders(
    const std::shared_ptr<MemReadView>& view, int64_t offset_lower_exclusive,
    const MemQueryContext& context) {
    std::shared_ptr<ReadView> arrow_view = std::dynamic_pointer_cast<ReadView>(view);
    if (!arrow_view) {
        return Status::Invalid("read view was not created by the Arrow mem indexer");
    }
    if (context.read_schema == nullptr || context.read_schema->release == nullptr) {
        return Status::Invalid("mem query read schema is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> read_schema,
                                      arrow::ImportSchema(context.read_schema));
    // TODO(xinyu.lxy): Support predicate pushdown after adding batch statistics or index metadata.
    // The default Arrow indexer currently ignores context.predicate and
    // context.enable_predicate_pushdown, and returns all offset-matching rows as candidates.
    std::vector<std::unique_ptr<RealtimeReader>> readers;
    if (arrow_view->GetOffsetRange() && arrow_view->GetOffsetRange()->to > offset_lower_exclusive) {
        std::unique_ptr<BatchReader> reader = std::make_unique<QueryBatchReader>(
            arrow_view.get(), offset_lower_exclusive, read_schema, arrow_pool_);
        reader = std::make_unique<CompleteRowKindBatchReader>(std::move(reader), memory_pool_);
        readers.push_back(std::make_unique<RealtimeReader>(arrow_view, std::move(reader)));
    }
    return readers;
}

Status ArrowMemIndexer::AdvanceCommittedOffset(int64_t committed_offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    // TODO(xinyu.lxy): Consider deferring segment destruction to a reclamation queue. Existing
    // read views may pin reclaimed batches, so the last query releasing a view can otherwise pay
    // the full buffer destruction cost and observe higher tail latency.
    sealed_segments_.erase(
        std::remove_if(sealed_segments_.begin(), sealed_segments_.end(),
                       [committed_offset](const std::shared_ptr<Segment>& segment) {
                           return segment->GetOffsetRange().to <= committed_offset;
                       }),
        sealed_segments_.end());
    return Status::OK();
}

uint64_t ArrowMemIndexer::GetMemoryUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t result = building_memory_usage_;
    for (const std::shared_ptr<Segment>& segment : sealed_segments_) {
        result += segment->GetMemoryUsage();
    }
    return result;
}

}  // namespace paimon
