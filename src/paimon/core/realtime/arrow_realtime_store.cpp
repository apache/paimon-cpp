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

#include "paimon/core/realtime/arrow_realtime_store.h"

#include <algorithm>
#include <numeric>
#include <optional>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/compute/api.h"
#include "arrow/compute/api_aggregate.h"
#include "paimon/common/data/columnar/columnar_array.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/projected_array.h"
#include "paimon/common/utils/projected_row.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/io/arrow_ipc_file.h"
#include "paimon/core/realtime/index/realtime_index_evaluator.h"
#include "paimon/core/realtime/index/realtime_index_set.h"
#include "paimon/core/realtime/realtime_utils.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"
#include "paimon/realtime/index/realtime_index.h"
#include "paimon/realtime/index/realtime_index_read_source.h"

namespace paimon {
namespace {

bool SupportsMinMax(const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
        case arrow::Type::BOOL:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        case arrow::Type::DATE32:
        case arrow::Type::TIMESTAMP:
        case arrow::Type::DECIMAL128:
            return true;
        default:
            return false;
    }
}

Result<std::shared_ptr<arrow::StructArray>> ProjectBatch(
    const std::shared_ptr<arrow::StructArray>& data,
    const std::shared_ptr<arrow::Schema>& read_schema, arrow::MemoryPool* arrow_pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> projected,
                           NestedProjectionUtils::AlignArrayToReadType(
                               data, arrow::struct_(read_schema->fields()), arrow_pool));
    if (!projected || projected->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("memory query projection did not produce a StructArray");
    }
    return checked_pointer_cast<arrow::StructArray>(projected);
}

Result<std::shared_ptr<arrow::StructArray>> ToStructArray(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        return Status::Invalid("Arrow IPC reader returned a null record batch");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> result,
        arrow::StructArray::Make(batch->columns(), batch->schema()->fields()));
    return result;
}

Result<BatchReader::ReadBatch> ExportStructBatch(
    const std::shared_ptr<arrow::StructArray>& data,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> normalized,
                           ArrowUtils::NormalizeArrayOffsets(data, arrow_pool.get()));
    auto c_array = std::make_unique<ArrowArray>();
    auto c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*normalized, c_array.get(), c_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), arrow_pool));
    return BatchReader::ReadBatch(std::move(c_array), std::move(c_schema));
}

class SlicedQueryBatchReader final : public BatchReader {
 public:
    SlicedQueryBatchReader(std::unique_ptr<BatchReader>&& reader, int32_t read_batch_size,
                           const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : reader_(std::move(reader)), read_batch_size_(read_batch_size), arrow_pool_(arrow_pool) {}

    Result<ReadBatch> NextBatch() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, NextBatchWithBitmap());
        return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch), arrow_pool_);
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        while (true) {
            if (!current_batch_ || next_row_ >= current_batch_->length()) {
                current_batch_.reset();
                next_row_ = 0;
                PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, reader_->NextBatchWithBitmap());
                if (IsEofBatch(batch)) {
                    return batch;
                }
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> array,
                    arrow::ImportArray(batch.first.first.get(), batch.first.second.get()));
                if (!array || array->type_id() != arrow::Type::STRUCT) {
                    return Status::Invalid("real-time query batch must be a StructArray");
                }
                current_batch_ = checked_pointer_cast<arrow::StructArray>(array);
                current_bitmap_ = std::move(batch.second);
            }

            const int64_t length =
                std::min<int64_t>(read_batch_size_, current_batch_->length() - next_row_);
            const int64_t begin = next_row_;
            next_row_ += length;
            RoaringBitmap32 selection;
            if (begin == 0 && length == current_batch_->length()) {
                selection = std::move(current_bitmap_);
            } else {
                for (auto iter = current_bitmap_.EqualOrLarger(begin);
                     iter != current_bitmap_.End() && *iter < next_row_; ++iter) {
                    selection.Add(*iter - begin);
                }
            }
            if (selection.IsEmpty()) {
                continue;
            }
            std::shared_ptr<arrow::StructArray> slice =
                checked_pointer_cast<arrow::StructArray>(current_batch_->Slice(begin, length));
            PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, ExportStructBatch(slice, arrow_pool_));
            return ReadBatchWithBitmap(std::move(batch), std::move(selection));
        }
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        current_batch_.reset();
        current_bitmap_ = RoaringBitmap32();
        arrow_pool_.reset();
        reader_->Close();
    }

 private:
    std::unique_ptr<BatchReader> reader_;
    int32_t read_batch_size_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<arrow::StructArray> current_batch_;
    RoaringBitmap32 current_bitmap_;
    int64_t next_row_ = 0;
};

std::vector<std::unique_ptr<BatchReader>> SliceQueryReaders(
    std::vector<std::unique_ptr<BatchReader>>&& readers, int32_t read_batch_size,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    for (std::unique_ptr<BatchReader>& reader : readers) {
        reader = std::make_unique<SlicedQueryBatchReader>(std::move(reader), read_batch_size,
                                                          arrow_pool);
    }
    return std::move(readers);
}

}  // namespace

class ArrowRealtimeStore::Segment : public RealtimeSegmentHandle {
 public:
    Segment(const OffsetRange& offset_range, int64_t row_count,
            std::shared_ptr<RealtimeIndexSet> index, const std::shared_ptr<MemoryPool>& memory_pool,
            const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : memory_pool_(memory_pool),
          arrow_pool_(arrow_pool),
          offset_range_(offset_range),
          row_count_(row_count),
          index_(std::move(index)) {}

    OffsetRange GetOffsetRange() const override {
        return offset_range_;
    }

    int64_t GetRowCount() const override {
        return row_count_;
    }

    virtual uint64_t GetMemoryUsage() const = 0;

    const std::shared_ptr<RealtimeIndexSet>& GetIndex() const {
        return index_;
    }

 private:
    // Release derived data and indexes before their allocators.
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    OffsetRange offset_range_;
    int64_t row_count_;
    std::shared_ptr<RealtimeIndexSet> index_;
};

class ArrowRealtimeStore::MemorySegment final : public ArrowRealtimeStore::Segment {
 public:
    MemorySegment(const OffsetRange& offset_range, std::vector<StoredBatch>&& batches,
                  std::shared_ptr<RealtimeIndexSet> index,
                  const std::shared_ptr<MemoryPool>& memory_pool,
                  const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : Segment(offset_range, CountRows(batches), std::move(index), memory_pool, arrow_pool),
          batches_(std::move(batches)) {
        for (const StoredBatch& batch : batches_) {
            memory_usage_ += batch.memory_usage;
        }
    }

    const std::vector<StoredBatch>& GetBatches() const {
        return batches_;
    }

    uint64_t GetMemoryUsage() const override {
        return memory_usage_ + (GetIndex() ? GetIndex()->GetMemoryUsage() : 0);
    }

 private:
    static int64_t CountRows(const std::vector<StoredBatch>& batches) {
        int64_t result = 0;
        for (const StoredBatch& batch : batches) {
            result += batch.data->length();
        }
        return result;
    }

    std::vector<StoredBatch> batches_;
    uint64_t memory_usage_ = 0;
};

class ArrowRealtimeStore::SpillFile {
 public:
    SpillFile(const std::shared_ptr<IOManager>& io_manager,
              const std::shared_ptr<FileSystem>& file_system, FileIOChannel::ID channel_id)
        : io_manager_(io_manager), file_system_(file_system), channel_id_(std::move(channel_id)) {}

    ~SpillFile() {
        if (file_system_) {
            [[maybe_unused]] Status status = file_system_->Delete(channel_id_.GetPath());
        }
    }

    const std::shared_ptr<FileSystem>& GetFileSystem() const {
        return file_system_;
    }

    const std::string& GetPath() const {
        return channel_id_.GetPath();
    }

 private:
    // Keep the manager and its root directory alive as long as this file is referenced.
    std::shared_ptr<IOManager> io_manager_;
    std::shared_ptr<FileSystem> file_system_;
    FileIOChannel::ID channel_id_;
};

class ArrowRealtimeStore::SpilledSegment final : public ArrowRealtimeStore::Segment {
 public:
    SpilledSegment(const OffsetRange& offset_range, int64_t row_count,
                   std::vector<SpilledBatch>&& batches, std::shared_ptr<SpillFile> file,
                   std::shared_ptr<RealtimeIndexSet> index,
                   const std::shared_ptr<MemoryPool>& memory_pool,
                   const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : Segment(offset_range, row_count, std::move(index), memory_pool, arrow_pool),
          batches_(std::move(batches)),
          file_(std::move(file)) {
        for (const SpilledBatch& batch : batches_) {
            memory_usage_ += batch.memory_usage;
        }
    }

    const std::vector<SpilledBatch>& GetBatches() const {
        return batches_;
    }

    const std::shared_ptr<SpillFile>& GetFile() const {
        return file_;
    }

    uint64_t GetMemoryUsage() const override {
        return memory_usage_ + (GetIndex() ? GetIndex()->GetMemoryUsage() : 0);
    }

 private:
    std::vector<SpilledBatch> batches_;
    std::shared_ptr<SpillFile> file_;
    uint64_t memory_usage_ = 0;
};

class ArrowRealtimeStore::SharedSpillFileReader {
 public:
    static Result<std::shared_ptr<SharedSpillFileReader>> Create(
        const std::shared_ptr<SpillFile>& spill_file, int32_t expected_batch_count,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<ArrowIpcFileReader> file_reader,
            ArrowIpcFileReader::Open(spill_file->GetFileSystem(), spill_file->GetPath(),
                                     /*use_threads=*/false, arrow_pool));
        if (file_reader->GetRecordBatchCount() != expected_batch_count) {
            return Status::Invalid(
                "real-time spill file batch count does not match segment metadata");
        }
        return std::shared_ptr<SharedSpillFileReader>(
            new SharedSpillFileReader(spill_file, std::move(file_reader)));
    }

    Result<std::shared_ptr<arrow::RecordBatch>> ReadRecordBatch(int32_t batch_index) {
        // TODO(xinyu.lxy): Use independent IPC readers to read the shared spill file in parallel.
        std::lock_guard<std::mutex> lock(mutex_);
        return file_reader_->ReadRecordBatch(batch_index);
    }

 private:
    SharedSpillFileReader(const std::shared_ptr<SpillFile>& spill_file,
                          std::unique_ptr<ArrowIpcFileReader>&& file_reader)
        : spill_file_(spill_file), file_reader_(std::move(file_reader)) {}

    // Keep the spill file alive until the shared IPC reader closes. Members are destroyed in
    // reverse declaration order, so the reader closes before the file can be deleted.
    std::shared_ptr<SpillFile> spill_file_;
    std::unique_ptr<ArrowIpcFileReader> file_reader_;
    std::mutex mutex_;
};

class ArrowRealtimeStore::ReadView : public RealtimeIndexedReadView {
 public:
    ReadView(std::vector<std::shared_ptr<Segment>>&& segments, bool append_mode)
        : segments_(std::move(segments)), append_mode_(append_mode) {
        if (!segments_.empty()) {
            offset_range_ = OffsetRange(segments_.front()->GetOffsetRange().begin,
                                        segments_.back()->GetOffsetRange().end);
        }
    }

    std::optional<OffsetRange> GetOffsetRange() const override {
        return offset_range_;
    }

    Result<std::vector<std::shared_ptr<RealtimeIndexReadSource>>> CreateIndexReadSources(
        const OffsetRange& offset_range) const override;

    const std::vector<std::shared_ptr<Segment>>& GetSegments() const {
        return segments_;
    }

 private:
    std::vector<std::shared_ptr<Segment>> segments_;
    std::optional<OffsetRange> offset_range_;
    bool append_mode_;
};

class ArrowRealtimeStore::AppendCommitBatchReader : public BatchReader {
 public:
    explicit AppendCommitBatchReader(const std::shared_ptr<MemorySegment>& segment)
        : segment_(segment), metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (!segment_ || next_batch_ >= segment_->GetBatches().size()) {
            return MakeEofBatch();
        }
        const StoredBatch& stored = segment_->GetBatches()[next_batch_++];
        auto c_array = std::make_unique<ArrowArray>();
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportArray(*stored.data, c_array.get(), c_schema.get()));
        return ReadBatch(std::move(c_array), std::move(c_schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        segment_.reset();
    }

 private:
    std::shared_ptr<MemorySegment> segment_;
    std::shared_ptr<Metrics> metrics_;
    size_t next_batch_ = 0;
};

class ArrowRealtimeStore::StoredBatchReader : public BatchReader {
 public:
    StoredBatchReader(const std::shared_ptr<arrow::StructArray>& data,
                      const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : data_(data), arrow_pool_(arrow_pool), metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (!data_) {
            return MakeEofBatch();
        }
        PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, ExportStructBatch(data_, arrow_pool_));
        data_.reset();
        arrow_pool_.reset();
        return batch;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        data_.reset();
        arrow_pool_.reset();
    }

 private:
    std::shared_ptr<arrow::StructArray> data_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> metrics_;
};

class ArrowRealtimeStore::SpillBatchReader : public BatchReader {
 public:
    SpillBatchReader(const std::shared_ptr<SharedSpillFileReader>& file_reader,
                     std::vector<int32_t>&& batch_indexes,
                     const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : file_reader_(file_reader),
          batch_indexes_(std::move(batch_indexes)),
          read_schema_(read_schema),
          arrow_pool_(arrow_pool),
          metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (!file_reader_ || next_batch_ >= batch_indexes_.size()) {
            return MakeEofBatch();
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> record_batch,
                               file_reader_->ReadRecordBatch(batch_indexes_[next_batch_]));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::StructArray> data,
                               ToStructArray(record_batch));
        if (read_schema_) {
            PAIMON_ASSIGN_OR_RAISE(data, ProjectBatch(data, read_schema_, arrow_pool_.get()));
        }
        PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, ExportStructBatch(data, arrow_pool_));
        ++next_batch_;
        return batch;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        file_reader_.reset();
        read_schema_.reset();
        arrow_pool_.reset();
    }

 private:
    std::shared_ptr<SharedSpillFileReader> file_reader_;
    std::vector<int32_t> batch_indexes_;
    std::shared_ptr<arrow::Schema> read_schema_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> metrics_;
    size_t next_batch_ = 0;
};

class ArrowRealtimeStore::AppendQueryBatchReader : public BatchReader {
 public:
    AppendQueryBatchReader(
        const std::shared_ptr<ReadView>& view, const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate, std::vector<int32_t>&& statistics_mapping,
        const OffsetRange& offset_range, const std::shared_ptr<const RoaringBitmap64>& selection,
        const std::shared_ptr<const RoaringBitmap64>& offset_deletions, bool enable_index_pruning,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
        const std::shared_ptr<MemoryPool>& memory_pool)
        : view_(view),
          read_schema_(read_schema),
          arrow_pool_(arrow_pool),
          memory_pool_(memory_pool),
          predicate_filter_(std::dynamic_pointer_cast<PredicateFilter>(predicate)),
          statistics_mapping_(std::move(statistics_mapping)),
          predicate_(predicate),
          offset_range_(offset_range),
          selection_(selection),
          offset_deletions_(offset_deletions),
          enable_index_pruning_(enable_index_pruning),
          metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, NextBatchWithBitmap());
        return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch), arrow_pool_);
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        while (view_ && next_segment_ < view_->GetSegments().size()) {
            const std::shared_ptr<Segment>& segment = view_->GetSegments()[next_segment_];
            PAIMON_ASSIGN_OR_RAISE(bool may_match, PrepareSegment(segment));
            if (!may_match) {
                NextSegment();
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(bool has_batch, SeekNextBatch(segment));
            if (!has_batch) {
                NextSegment();
                continue;
            }
            const size_t batch_index = next_batch_++;
            PAIMON_ASSIGN_OR_RAISE(std::optional<ReadBatchWithBitmap> batch,
                                   TryReadBatch(segment, batch_index));
            if (batch) {
                return std::move(*batch);
            }
        }
        return MakeEofBatchWithBitmap();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        view_ = nullptr;
        spill_reader_.reset();
        candidates_.reset();
    }

 private:
    Result<bool> PrepareSegment(const std::shared_ptr<Segment>& segment) {
        const OffsetRange segment_range = segment->GetOffsetRange();
        if (segment_range.end <= offset_range_.begin || segment_range.begin >= offset_range_.end) {
            return false;
        }
        if (!segment_initialized_) {
            if (selection_) {
                candidates_ = *selection_;
            }
            if (enable_index_pruning_ && segment->GetIndex() && predicate_) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::optional<RoaringBitmap64> indexed,
                    RealtimeIndexEvaluator::Evaluate(
                        segment->GetIndex(), predicate_,
                        OffsetRange(std::max(segment_range.begin, offset_range_.begin),
                                    std::min(segment_range.end, offset_range_.end))));
                if (indexed) {
                    if (candidates_) {
                        *candidates_ &= *indexed;
                    } else {
                        candidates_ = std::move(indexed);
                    }
                }
            }
            segment_initialized_ = true;
        }
        return !candidates_ || !candidates_->IsEmpty();
    }

    Result<bool> SeekNextBatch(const std::shared_ptr<Segment>& segment) {
        const std::shared_ptr<MemorySegment> memory_segment =
            std::dynamic_pointer_cast<MemorySegment>(segment);
        if (memory_segment) {
            return SeekNextBatch(memory_segment->GetBatches());
        }
        const std::shared_ptr<SpilledSegment> spilled_segment =
            std::dynamic_pointer_cast<SpilledSegment>(segment);
        if (spilled_segment) {
            return SeekNextBatch(spilled_segment->GetBatches());
        }
        return Status::Invalid("unknown Arrow real-time segment type");
    }

    template <typename Batch>
    bool SeekNextBatch(const std::vector<Batch>& batches) {
        if (next_batch_ >= batches.size()) {
            return false;
        }
        const OffsetRange current_range = batches[next_batch_].offset_range;
        int64_t first_offset = std::max(current_range.begin, offset_range_.begin);
        if (candidates_) {
            const RoaringBitmap64::Iterator first = candidates_->EqualOrLarger(first_offset);
            if (first == candidates_->End() || *first >= offset_range_.end) {
                return false;
            }
            first_offset = *first;
        }
        const auto next = std::lower_bound(
            batches.begin() + next_batch_, batches.end(), first_offset,
            [](const Batch& batch, int64_t offset) { return batch.offset_range.end <= offset; });
        next_batch_ = next - batches.begin();
        return next_batch_ < batches.size();
    }

    Result<std::optional<ReadBatchWithBitmap>> TryReadBatch(const std::shared_ptr<Segment>& segment,
                                                            size_t batch_index) {
        const std::shared_ptr<MemorySegment> memory_segment =
            std::dynamic_pointer_cast<MemorySegment>(segment);
        const std::shared_ptr<SpilledSegment> spilled_segment =
            std::dynamic_pointer_cast<SpilledSegment>(segment);
        int64_t row_count;
        OffsetRange batch_range(0, 0);
        const std::optional<BatchStatistics>* statistics;
        std::shared_ptr<arrow::StructArray> source;
        if (memory_segment) {
            const StoredBatch& stored = memory_segment->GetBatches()[batch_index];
            row_count = stored.data->length();
            statistics = &stored.statistics;
            source = stored.data;
            batch_range = stored.offset_range;
        } else if (spilled_segment) {
            const SpilledBatch& stored = spilled_segment->GetBatches()[batch_index];
            row_count = stored.row_count;
            statistics = &stored.statistics;
            batch_range = stored.offset_range;
        } else {
            return Status::Invalid("unknown Arrow real-time segment type");
        }
        const OffsetRange visible_range(std::max(batch_range.begin, offset_range_.begin),
                                        std::min(batch_range.end, offset_range_.end));
        if (visible_range.begin >= visible_range.end ||
            (candidates_ && !candidates_->ContainsAny(visible_range.begin, visible_range.end))) {
            return std::optional<ReadBatchWithBitmap>();
        }
        PAIMON_ASSIGN_OR_RAISE(bool may_match,
                               ArrowRealtimeStore::MayMatchStatistics(
                                   row_count, *statistics, read_schema_, predicate_filter_,
                                   statistics_mapping_, memory_pool_));
        if (!may_match) {
            return std::optional<ReadBatchWithBitmap>();
        }
        if (spilled_segment) {
            PAIMON_ASSIGN_OR_RAISE(source, ReadSpilledBatch(spilled_segment, batch_index));
        }
        RoaringBitmap32 selection = SelectRows(source, visible_range);
        if (selection.IsEmpty()) {
            return std::optional<ReadBatchWithBitmap>();
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::StructArray> output,
                               ProjectBatch(source, read_schema_, arrow_pool_.get()));
        PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, ExportStructBatch(output, arrow_pool_));
        return std::optional<ReadBatchWithBitmap>(
            ReadBatchWithBitmap(std::move(batch), std::move(selection)));
    }

    Result<std::shared_ptr<arrow::StructArray>> ReadSpilledBatch(
        const std::shared_ptr<SpilledSegment>& segment, size_t batch_index) {
        if (!spill_reader_) {
            PAIMON_ASSIGN_OR_RAISE(spill_reader_,
                                   ArrowIpcFileReader::Open(segment->GetFile()->GetFileSystem(),
                                                            segment->GetFile()->GetPath(),
                                                            /*use_threads=*/false, arrow_pool_));
            if (spill_reader_->GetRecordBatchCount() !=
                static_cast<int32_t>(segment->GetBatches().size())) {
                return Status::Invalid(
                    "real-time spill file batch count does not match segment metadata");
            }
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> record_batch,
                               spill_reader_->ReadRecordBatch(batch_index));
        return ToStructArray(record_batch);
    }

    void NextSegment() {
        ++next_segment_;
        next_batch_ = 0;
        spill_reader_.reset();
        candidates_.reset();
        segment_initialized_ = false;
    }

    RoaringBitmap32 SelectRows(const std::shared_ptr<arrow::StructArray>& source,
                               const OffsetRange& visible_range) const {
        RoaringBitmap32 selection;
        if (source->length() == 0) {
            return selection;
        }
        const std::shared_ptr<arrow::Int64Array> offsets = checked_pointer_cast<arrow::Int64Array>(
            source->GetFieldByName(SpecialFields::RealtimeOffset().Name()));
        const int64_t* values = offsets->raw_values();
        const int64_t* begin =
            std::lower_bound(values, values + offsets->length(), visible_range.begin);
        const int64_t* end = std::lower_bound(begin, values + offsets->length(), visible_range.end);
        if (!candidates_) {
            if (!offset_deletions_ || offset_deletions_->IsEmpty()) {
                selection.AddRange(begin - values, end - values);
            } else {
                for (const int64_t* row = begin; row != end; ++row) {
                    if (!offset_deletions_->Contains(*row)) {
                        selection.Add(row - values);
                    }
                }
            }
            return selection;
        }
        // Offsets may have gaps: sparse candidates seek by offset; dense candidates scan.
        const RoaringBitmap64::Iterator sentinel = candidates_->End();
        const RoaringBitmap64::Iterator first = candidates_->EqualOrLarger(visible_range.begin);
        RoaringBitmap64::Iterator probe = first;
        const int64_t dense_threshold = std::max<int64_t>(1, (end - begin) / 4);
        int64_t candidate_count = 0;
        while (candidate_count < dense_threshold && probe != sentinel &&
               *probe < visible_range.end) {
            ++candidate_count;
            ++probe;
        }
        if (candidate_count < dense_threshold) {
            for (RoaringBitmap64::Iterator iter = first;
                 iter != sentinel && *iter < visible_range.end; ++iter) {
                begin = std::lower_bound(begin, end, *iter);
                if (begin != end && *begin == *iter &&
                    (!offset_deletions_ || !offset_deletions_->Contains(*iter))) {
                    selection.Add(begin - values);
                }
            }
        } else {
            for (const int64_t* row = begin; row != end; ++row) {
                if (candidates_->Contains(*row) &&
                    (!offset_deletions_ || !offset_deletions_->Contains(*row))) {
                    selection.Add(row - values);
                }
            }
        }
        return selection;
    }

    std::shared_ptr<ReadView> view_;
    std::shared_ptr<arrow::Schema> read_schema_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<PredicateFilter> predicate_filter_;
    std::vector<int32_t> statistics_mapping_;
    std::shared_ptr<Predicate> predicate_;
    OffsetRange offset_range_;
    std::shared_ptr<const RoaringBitmap64> selection_;
    std::shared_ptr<const RoaringBitmap64> offset_deletions_;
    bool enable_index_pruning_;
    std::optional<RoaringBitmap64> candidates_;
    bool segment_initialized_ = false;
    std::shared_ptr<Metrics> metrics_;
    std::unique_ptr<ArrowIpcFileReader> spill_reader_;
    size_t next_segment_ = 0;
    size_t next_batch_ = 0;
};

class ArrowRealtimeStore::IndexReadSource final : public RealtimeIndexReadSource {
 public:
    IndexReadSource(const std::shared_ptr<Segment>& segment, const OffsetRange& offset_range)
        : segment_(segment), offset_range_(offset_range) {}

    OffsetRange GetOffsetRange() const override {
        return offset_range_;
    }

    Result<RoaringBitmap64> CreateCandidates() const override {
        RoaringBitmap64 candidates;
        candidates.AddRange(offset_range_.begin, offset_range_.end);
        return candidates;
    }

    Result<std::vector<std::shared_ptr<RealtimeIndexReader>>> CreateIndexReaders(
        const std::string& field_name) const override {
        std::vector<std::shared_ptr<RealtimeIndexReader>> readers;
        if (segment_->GetIndex()) {
            for (const auto& index : segment_->GetIndex()->FindIndexes(field_name)) {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeIndexReader> reader,
                                       index->CreateReader(offset_range_));
                if (reader) {
                    readers.push_back(std::move(reader));
                }
            }
        }
        return readers;
    }

 private:
    std::shared_ptr<Segment> segment_;
    OffsetRange offset_range_;
};

Result<std::vector<std::shared_ptr<RealtimeIndexReadSource>>>
ArrowRealtimeStore::ReadView::CreateIndexReadSources(const OffsetRange& offset_range) const {
    if (!append_mode_) {
        return Status::NotImplemented("Primary-key views do not support precise index sources");
    }
    if (offset_range.begin < 0 || offset_range.begin > offset_range.end) {
        return Status::Invalid("Invalid real-time index source offset range");
    }
    std::vector<std::shared_ptr<RealtimeIndexReadSource>> sources;
    for (const auto& segment : segments_) {
        const OffsetRange bounds = segment->GetOffsetRange();
        const OffsetRange range(std::max(bounds.begin, offset_range.begin),
                                std::min(bounds.end, offset_range.end));
        if (range.begin < range.end) {
            sources.push_back(std::make_shared<IndexReadSource>(segment, range));
        }
    }
    return sources;
}

Result<std::shared_ptr<ArrowRealtimeStore>> ArrowRealtimeStore::Create(
    const std::shared_ptr<arrow::Schema>& write_schema, RealtimeStoreMode mode,
    StatisticsMode statistics_mode, const std::string& temp_directory,
    const std::shared_ptr<FileSystem>& spill_file_system, const std::string& spill_compression,
    int32_t spill_compression_level, const std::map<std::string, std::string>& index_options,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (mode != RealtimeStoreMode::APPEND_ONLY && mode != RealtimeStoreMode::PRIMARY_KEY) {
        return Status::Invalid("invalid real-time store mode: ", static_cast<int32_t>(mode));
    }
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(write_schema));
    const bool has_index = RealtimeIndexSet::IsEnabled(index_options);
    if (has_index && mode != RealtimeStoreMode::APPEND_ONLY) {
        return Status::NotImplemented("real-time query indexes require an append-store delegate");
    }
    std::shared_ptr<IOManager> io_manager;
    if (!temp_directory.empty()) {
        io_manager = std::make_shared<IOManager>(temp_directory, spill_file_system);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeIndexSet> building_index,
                           RealtimeIndexSet::Create(write_schema, index_options, memory_pool));
    auto store = std::shared_ptr<ArrowRealtimeStore>(
        new ArrowRealtimeStore(write_schema, mode, statistics_mode, spill_file_system,
                               spill_compression, spill_compression_level, index_options,
                               io_manager, std::move(building_index), memory_pool, arrow_pool));
    return store;
}

Result<std::shared_ptr<RealtimeIndexSet>> ArrowRealtimeStore::CreateBuildingIndex() const {
    return RealtimeIndexSet::Create(write_schema_, index_options_, memory_pool_);
}

ArrowRealtimeStore::ArrowRealtimeStore(const std::shared_ptr<arrow::Schema>& write_schema,
                                       RealtimeStoreMode mode, StatisticsMode statistics_mode,
                                       const std::shared_ptr<FileSystem>& spill_file_system,
                                       const std::string& spill_compression,
                                       int32_t spill_compression_level,
                                       const std::map<std::string, std::string>& index_options,
                                       const std::shared_ptr<IOManager>& io_manager,
                                       std::shared_ptr<RealtimeIndexSet> building_index,
                                       const std::shared_ptr<MemoryPool>& memory_pool,
                                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : write_schema_(write_schema),
      memory_pool_(memory_pool),
      arrow_pool_(arrow_pool),
      mode_(mode),
      statistics_mode_(statistics_mode),
      io_manager_(io_manager),
      spill_file_system_(spill_file_system),
      spill_compression_(spill_compression),
      spill_compression_level_(spill_compression_level),
      index_options_(index_options),
      building_index_(std::move(building_index)) {}

Result<std::optional<ArrowRealtimeStore::BatchStatistics>> ArrowRealtimeStore::CollectStatistics(
    const std::shared_ptr<arrow::StructArray>& data) const {
    if (statistics_mode_ == StatisticsMode::NONE) {
        return std::optional<BatchStatistics>();
    }

    arrow::ArrayVector min_values;
    arrow::ArrayVector max_values;
    min_values.reserve(data->num_fields());
    max_values.reserve(data->num_fields());
    arrow::Int64Builder null_count_builder(arrow_pool_.get());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(null_count_builder.Reserve(data->num_fields()));
    arrow::compute::ScalarAggregateOptions aggregate_options;
    aggregate_options.skip_nulls = true;
    aggregate_options.min_count = 1;
    arrow::compute::ExecContext exec_context(arrow_pool_.get());

    for (const std::shared_ptr<arrow::Array>& field : data->fields()) {
        null_count_builder.UnsafeAppend(field->null_count());
        if (!SupportsMinMax(field->type())) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> min_value,
                arrow::MakeArrayOfNull(field->type(), /*length=*/1, arrow_pool_.get()));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> max_value,
                arrow::MakeArrayOfNull(field->type(), /*length=*/1, arrow_pool_.get()));
            min_values.push_back(std::move(min_value));
            max_values.push_back(std::move(max_value));
            continue;
        }

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            arrow::Datum min_max, arrow::compute::MinMax(field, aggregate_options, &exec_context));
        std::shared_ptr<arrow::StructScalar> min_max_scalar =
            std::dynamic_pointer_cast<arrow::StructScalar>(min_max.scalar());
        if (!min_max_scalar || min_max_scalar->value.size() != 2) {
            return Status::Invalid("Arrow min_max did not produce min and max scalars");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> min_value,
            arrow::MakeArrayFromScalar(*min_max_scalar->value[0], /*length=*/1, arrow_pool_.get()));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> max_value,
            arrow::MakeArrayFromScalar(*min_max_scalar->value[1], /*length=*/1, arrow_pool_.get()));
        min_values.push_back(std::move(min_value));
        max_values.push_back(std::move(max_value));
    }

    std::shared_ptr<arrow::Array> null_counts_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(null_count_builder.Finish(&null_counts_array));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> min_values_struct,
        arrow::StructArray::Make(min_values, write_schema_->fields()));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> max_values_struct,
        arrow::StructArray::Make(max_values, write_schema_->fields()));
    return std::optional<BatchStatistics>(BatchStatistics{arrow_pool_, std::move(min_values_struct),
                                                          std::move(max_values_struct),
                                                          std::move(null_counts_array)});
}

Result<bool> ArrowRealtimeStore::MayMatchStatistics(
    int64_t row_count, const std::optional<BatchStatistics>& statistics,
    const std::shared_ptr<arrow::Schema>& read_schema,
    const std::shared_ptr<PredicateFilter>& predicate_filter,
    const std::vector<int32_t>& statistics_mapping,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!predicate_filter || !statistics) {
        return true;
    }
    const BatchStatistics& batch_statistics = statistics.value();
    std::shared_ptr<InternalRow> min_row = std::make_shared<ColumnarRow>(
        batch_statistics.min_values, batch_statistics.min_values->fields(), memory_pool,
        /*row_id=*/0);
    std::shared_ptr<InternalRow> max_row = std::make_shared<ColumnarRow>(
        batch_statistics.max_values, batch_statistics.max_values->fields(), memory_pool,
        /*row_id=*/0);
    ProjectedRow projected_min(min_row, statistics_mapping);
    ProjectedRow projected_max(max_row, statistics_mapping);
    std::shared_ptr<InternalArray> null_counts =
        std::make_shared<ColumnarArray>(batch_statistics.null_counts.get(), memory_pool,
                                        /*offset=*/0, batch_statistics.null_counts->length());
    ProjectedArray projected_null_counts(null_counts, statistics_mapping);
    return predicate_filter->Test(read_schema, row_count, projected_min, projected_max,
                                  projected_null_counts);
}

Status ArrowRealtimeStore::Write(RealtimeWriteBatch&& write_batch) {
    if (!write_batch.batch || !write_batch.batch->GetData()) {
        return Status::Invalid("real-time write batch is null");
    }
    if (write_batch.offset_range.begin < 0 ||
        write_batch.offset_range.begin >= write_batch.offset_range.end) {
        return Status::Invalid("real-time offset range is invalid");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> data,
        arrow::ImportArray(write_batch.batch->GetData(), arrow::struct_(write_schema_->fields())));
    if (!data || data->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("real-time write data is not a StructArray");
    }
    std::shared_ptr<arrow::StructArray> struct_array =
        checked_pointer_cast<arrow::StructArray>(data);
    PAIMON_ASSIGN_OR_RAISE(std::optional<BatchStatistics> statistics,
                           CollectStatistics(struct_array));

    std::lock_guard<std::mutex> lock(mutex_);
    if (building_range_ && write_batch.offset_range.begin < building_range_->end) {
        return Status::Invalid("real-time offset ranges must be ordered and non-overlapping");
    }
    if (building_index_) {
        ArrowArray index_batch;
        ArrowArrayMarkReleased(&index_batch);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, &index_batch));
        ScopeGuard index_guard([&index_batch]() { ArrowArrayRelease(&index_batch); });
        PAIMON_RETURN_NOT_OK(building_index_->AddBatch(&index_batch, write_batch.offset_range));
    }
    uint64_t memory_usage = ArrowUtils::GetArrayMemoryUsage(struct_array->data());
    if (statistics) {
        memory_usage += ArrowUtils::GetArrayMemoryUsage(statistics->min_values->data()) +
                        ArrowUtils::GetArrayMemoryUsage(statistics->max_values->data()) +
                        ArrowUtils::GetArrayMemoryUsage(statistics->null_counts->data());
    }
    building_memory_usage_ += memory_usage;
    building_row_count_ += static_cast<uint64_t>(struct_array->length());
    building_batches_.push_back(StoredBatch{std::move(struct_array), write_batch.offset_range,
                                            std::move(statistics), memory_usage});
    if (!building_range_) {
        building_range_ = write_batch.offset_range;
    } else {
        building_range_ = OffsetRange(building_range_->begin, write_batch.offset_range.end);
    }
    return Status::OK();
}

Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> ArrowRealtimeStore::SealForCommit() {
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<RealtimeSegmentHandle>> frozen,
                           FreezeBuildingSegment());
    if (!frozen) {
        return frozen;
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeSegmentHandle> segment,
                           SpillFrozenSegment(frozen.value()));
    return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(std::move(segment));
}

Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>>
ArrowRealtimeStore::FreezeBuildingSegment() {
    std::shared_ptr<MemorySegment> memory_segment;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (building_batches_.empty()) {
            return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeIndexSet> next_index, CreateBuildingIndex());
        memory_segment =
            std::make_shared<MemorySegment>(building_range_.value(), std::move(building_batches_),
                                            std::move(building_index_), memory_pool_, arrow_pool_);
        building_index_ = std::move(next_index);
        sealed_segments_.push_back(memory_segment);
        sealed_memory_usage_ += memory_segment->GetMemoryUsage();
        sealed_row_count_ += building_row_count_;
        building_batches_.clear();
        building_range_.reset();
        building_memory_usage_ = 0;
        building_row_count_ = 0;
    }
    return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(std::move(memory_segment));
}

Result<std::shared_ptr<RealtimeSegmentHandle>> ArrowRealtimeStore::SpillFrozenSegment(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    std::shared_ptr<MemorySegment> memory_segment =
        std::dynamic_pointer_cast<MemorySegment>(segment);
    if (!memory_segment) {
        return Status::Invalid("real-time spill requires a frozen memory segment");
    }
    if (!io_manager_) {
        return segment;
    }

    PAIMON_ASSIGN_OR_RAISE(FileIOChannel::ID channel_id, io_manager_->CreateChannel("realtime"));
    std::unique_ptr<ArrowIpcFileWriter> writer;
    auto cleanup_guard = ScopeGuard([&]() {
        writer.reset();
        [[maybe_unused]] Status status = spill_file_system_->Delete(channel_id.GetPath());
    });
    PAIMON_ASSIGN_OR_RAISE(
        writer, ArrowIpcFileWriter::Create(spill_file_system_, channel_id.GetPath(), write_schema_,
                                           spill_compression_, spill_compression_level_,
                                           /*use_threads=*/false, arrow_pool_));
    std::vector<SpilledBatch> spilled_batches;
    spilled_batches.reserve(memory_segment->GetBatches().size());
    for (const StoredBatch& batch : memory_segment->GetBatches()) {
        std::shared_ptr<arrow::RecordBatch> record_batch =
            arrow::RecordBatch::Make(write_schema_, batch.data->length(), batch.data->fields());
        PAIMON_RETURN_NOT_OK(writer->WriteBatch(record_batch));
        uint64_t statistics_memory_usage = 0;
        if (batch.statistics) {
            statistics_memory_usage =
                ArrowUtils::GetArrayMemoryUsage(batch.statistics->min_values->data()) +
                ArrowUtils::GetArrayMemoryUsage(batch.statistics->max_values->data()) +
                ArrowUtils::GetArrayMemoryUsage(batch.statistics->null_counts->data());
        }
        spilled_batches.push_back(SpilledBatch{batch.offset_range, batch.statistics,
                                               batch.data->length(), statistics_memory_usage});
    }
    PAIMON_RETURN_NOT_OK(writer->Close());
    writer.reset();
    auto spill_file =
        std::make_shared<SpillFile>(io_manager_, spill_file_system_, std::move(channel_id));
    // TODO(xinyu.lxy): Real-time indexes do not support spill yet.
    auto spilled_segment = std::make_shared<SpilledSegment>(
        memory_segment->GetOffsetRange(), memory_segment->GetRowCount(), std::move(spilled_batches),
        std::move(spill_file), memory_segment->GetIndex(), memory_pool_, arrow_pool_);
    cleanup_guard.Release();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto iter = std::find(sealed_segments_.begin(), sealed_segments_.end(), memory_segment);
        if (iter != sealed_segments_.end()) {
            *iter = spilled_segment;
            sealed_memory_usage_ -= memory_segment->GetMemoryUsage();
            sealed_memory_usage_ += spilled_segment->GetMemoryUsage();
        }
    }
    return std::shared_ptr<RealtimeSegmentHandle>(std::move(spilled_segment));
}

Result<std::vector<std::unique_ptr<BatchReader>>> ArrowRealtimeStore::CreateCommitReaders(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    std::shared_ptr<Segment> arrow_segment = std::dynamic_pointer_cast<Segment>(segment);
    if (!arrow_segment) {
        return Status::Invalid("segment was not created by the Arrow real-time store");
    }
    std::vector<std::unique_ptr<BatchReader>> readers;
    std::shared_ptr<MemorySegment> memory_segment =
        std::dynamic_pointer_cast<MemorySegment>(arrow_segment);
    if (memory_segment) {
        if (mode_ == RealtimeStoreMode::APPEND_ONLY) {
            readers.push_back(std::make_unique<AppendCommitBatchReader>(memory_segment));
            return readers;
        }
        readers.reserve(memory_segment->GetBatches().size());
        for (const StoredBatch& batch : memory_segment->GetBatches()) {
            readers.push_back(std::make_unique<StoredBatchReader>(batch.data, arrow_pool_));
        }
        return readers;
    }

    std::shared_ptr<SpilledSegment> spilled_segment =
        std::dynamic_pointer_cast<SpilledSegment>(arrow_segment);
    if (!spilled_segment) {
        return Status::Invalid("unknown Arrow real-time segment type");
    }
    const auto batch_count = static_cast<int32_t>(spilled_segment->GetBatches().size());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SharedSpillFileReader> file_reader,
        SharedSpillFileReader::Create(spilled_segment->GetFile(), batch_count, arrow_pool_));
    if (mode_ == RealtimeStoreMode::APPEND_ONLY) {
        std::vector<int32_t> indexes(batch_count);
        std::iota(indexes.begin(), indexes.end(), 0);
        readers.push_back(std::make_unique<SpillBatchReader>(file_reader, std::move(indexes),
                                                             /*read_schema=*/nullptr, arrow_pool_));
        return readers;
    }
    readers.reserve(spilled_segment->GetBatches().size());
    for (int32_t i = 0; i < batch_count; ++i) {
        readers.push_back(std::make_unique<SpillBatchReader>(file_reader, std::vector<int32_t>{i},
                                                             /*read_schema=*/nullptr, arrow_pool_));
    }
    return readers;
}

Result<std::shared_ptr<RealtimeReadView>> ArrowRealtimeStore::AcquireReadView() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::shared_ptr<Segment>> segments = sealed_segments_;
    if (!building_batches_.empty()) {
        segments.push_back(std::make_shared<MemorySegment>(
            building_range_.value(), std::vector<StoredBatch>(building_batches_), building_index_,
            memory_pool_, arrow_pool_));
    }
    std::shared_ptr<ReadView> view =
        std::make_shared<ReadView>(std::move(segments), mode_ != RealtimeStoreMode::PRIMARY_KEY);
    return std::shared_ptr<RealtimeReadView>(std::move(view));
}

Result<std::vector<std::unique_ptr<BatchReader>>> ArrowRealtimeStore::CreateQueryReaders(
    const std::shared_ptr<RealtimeReadView>& view, const RealtimeQueryContext& context) {
    std::shared_ptr<ReadView> arrow_view = std::dynamic_pointer_cast<ReadView>(view);
    if (!arrow_view) {
        return Status::Invalid("read view was not created by the Arrow real-time store");
    }
    if (context.read_schema == nullptr || context.read_schema->release == nullptr) {
        return Status::Invalid("mem query read schema is null");
    }
    if (context.read_batch_size <= 0) {
        return Status::Invalid("real-time query read batch size must be positive");
    }
    if (context.offset_range.begin < 0 || context.offset_range.begin > context.offset_range.end) {
        return Status::Invalid("real-time query offset range is invalid");
    }
    if (mode_ == RealtimeStoreMode::PRIMARY_KEY && context.selection) {
        return Status::NotImplemented("Offset selection is not supported for primary-key merge");
    }
    if (mode_ == RealtimeStoreMode::PRIMARY_KEY && context.offset_deletions) {
        return Status::NotImplemented("Offset deletions are not supported for primary-key merge");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> read_schema,
                                      arrow::ImportSchema(context.read_schema));
    std::vector<std::unique_ptr<BatchReader>> readers;
    if (!arrow_view->GetOffsetRange()) {
        return SliceQueryReaders(std::move(readers), context.read_batch_size, arrow_pool_);
    }
    std::vector<int32_t> statistics_mapping;
    statistics_mapping.reserve(read_schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& field : read_schema->fields()) {
        statistics_mapping.push_back(write_schema_->GetFieldIndex(field->name()));
    }
    if (mode_ == RealtimeStoreMode::PRIMARY_KEY) {
        auto predicate_filter = std::dynamic_pointer_cast<PredicateFilter>(context.predicate);
        for (const std::shared_ptr<Segment>& segment : arrow_view->GetSegments()) {
            std::shared_ptr<MemorySegment> memory_segment =
                std::dynamic_pointer_cast<MemorySegment>(segment);
            if (memory_segment) {
                for (const StoredBatch& batch : memory_segment->GetBatches()) {
                    PAIMON_ASSIGN_OR_RAISE(
                        bool may_match,
                        MayMatchStatistics(batch.data->length(), batch.statistics, read_schema,
                                           predicate_filter, statistics_mapping, memory_pool_));
                    if (!may_match) {
                        continue;
                    }
                    PAIMON_ASSIGN_OR_RAISE(
                        std::shared_ptr<arrow::StructArray> projected,
                        ProjectBatch(batch.data, read_schema, arrow_pool_.get()));
                    readers.push_back(std::make_unique<StoredBatchReader>(projected, arrow_pool_));
                }
                continue;
            }

            std::shared_ptr<SpilledSegment> spilled_segment =
                std::dynamic_pointer_cast<SpilledSegment>(segment);
            if (!spilled_segment) {
                return Status::Invalid("unknown Arrow real-time segment type");
            }
            std::vector<int32_t> matching_indexes;
            for (int32_t i = 0; i < static_cast<int32_t>(spilled_segment->GetBatches().size());
                 ++i) {
                const SpilledBatch& batch = spilled_segment->GetBatches()[i];
                PAIMON_ASSIGN_OR_RAISE(
                    bool may_match,
                    MayMatchStatistics(batch.row_count, batch.statistics, read_schema,
                                       predicate_filter, statistics_mapping, memory_pool_));
                if (may_match) {
                    matching_indexes.push_back(i);
                }
            }
            if (matching_indexes.empty()) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<SharedSpillFileReader> file_reader,
                SharedSpillFileReader::Create(
                    spilled_segment->GetFile(),
                    static_cast<int32_t>(spilled_segment->GetBatches().size()), arrow_pool_));
            for (int32_t batch_index : matching_indexes) {
                readers.push_back(std::make_unique<SpillBatchReader>(
                    file_reader, std::vector<int32_t>{batch_index}, read_schema, arrow_pool_));
            }
        }
        return SliceQueryReaders(std::move(readers), context.read_batch_size, arrow_pool_);
    }

    OffsetRange visible_range = arrow_view->GetOffsetRange().value();
    visible_range.begin = std::max(visible_range.begin, context.offset_range.begin);
    visible_range.end = std::min(visible_range.end, context.offset_range.end);
    if (visible_range.begin >= visible_range.end) {
        return readers;
    }
    std::unique_ptr<BatchReader> reader = std::make_unique<AppendQueryBatchReader>(
        arrow_view, read_schema, context.predicate, std::move(statistics_mapping), visible_range,
        context.selection, context.offset_deletions, context.enable_index_pruning, arrow_pool_,
        memory_pool_);
    readers.push_back(std::move(reader));
    return SliceQueryReaders(std::move(readers), context.read_batch_size, arrow_pool_);
}

Status ArrowRealtimeStore::AdvanceCommittedOffset(int64_t committed_end_offset) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeReadView> retired,
                           RetireCommittedSegments(committed_end_offset));
    return Status::OK();
}

Result<std::shared_ptr<RealtimeReadView>> ArrowRealtimeStore::RetireCommittedSegments(
    int64_t committed_end_offset) {
    // Drop the store's references only after releasing the write mutex. Segment destruction may
    // release large Arrow buffers or delete a spill file.
    std::vector<std::shared_ptr<Segment>> reclaimed_segments;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        reclaimed_segments.reserve(sealed_segments_.size());
        uint64_t reclaimed_memory_usage = 0;
        uint64_t reclaimed_row_count = 0;
        auto retained_end = sealed_segments_.begin();
        for (auto iter = sealed_segments_.begin(); iter != sealed_segments_.end(); ++iter) {
            if ((*iter)->GetOffsetRange().end <= committed_end_offset) {
                reclaimed_memory_usage += (*iter)->GetMemoryUsage();
                reclaimed_row_count += static_cast<uint64_t>((*iter)->GetRowCount());
                reclaimed_segments.push_back(std::move(*iter));
                continue;
            }
            if (retained_end != iter) {
                *retained_end = std::move(*iter);
            }
            ++retained_end;
        }
        sealed_segments_.erase(retained_end, sealed_segments_.end());
        sealed_memory_usage_ -= reclaimed_memory_usage;
        sealed_row_count_ -= reclaimed_row_count;
    }
    return std::shared_ptr<RealtimeReadView>(
        new ReadView(std::move(reclaimed_segments), mode_ != RealtimeStoreMode::PRIMARY_KEY));
}

RealtimeStoreDataUsage ArrowRealtimeStore::GetDataUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    const uint64_t index_memory = building_index_ ? building_index_->GetMemoryUsage() : 0;
    return RealtimeStoreDataUsage{building_memory_usage_ + index_memory, sealed_memory_usage_,
                                  building_row_count_, sealed_row_count_};
}

uint64_t ArrowRealtimeStore::GetMemoryUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return building_memory_usage_ + sealed_memory_usage_ +
           (building_index_ ? building_index_->GetMemoryUsage() : 0);
}

}  // namespace paimon
