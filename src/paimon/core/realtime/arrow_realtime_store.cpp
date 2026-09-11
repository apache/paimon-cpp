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
#include "arrow/compute/api_aggregate.h"
#include "paimon/common/data/columnar/columnar_array.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/projected_array.h"
#include "paimon/common/utils/projected_row.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/io/arrow_ipc_file.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"

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

}  // namespace

class ArrowRealtimeStore::Segment : public RealtimeSegmentHandle {
 public:
    Segment(const OffsetRange& offset_range, int64_t row_count)
        : offset_range_(offset_range), row_count_(row_count) {}

    OffsetRange GetOffsetRange() const override {
        return offset_range_;
    }

    int64_t GetRowCount() const override {
        return row_count_;
    }

    virtual uint64_t GetMemoryUsage() const = 0;

 private:
    OffsetRange offset_range_;
    int64_t row_count_;
};

class ArrowRealtimeStore::MemorySegment final : public ArrowRealtimeStore::Segment {
 public:
    MemorySegment(const OffsetRange& offset_range, std::vector<StoredBatch>&& batches)
        : Segment(offset_range, CountRows(batches)), batches_(std::move(batches)) {
        for (const StoredBatch& batch : batches_) {
            memory_usage_ += batch.memory_usage;
        }
    }

    const std::vector<StoredBatch>& GetBatches() const {
        return batches_;
    }

    uint64_t GetMemoryUsage() const override {
        return memory_usage_;
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
                   std::vector<SpilledBatch>&& batches, std::shared_ptr<SpillFile> file)
        : Segment(offset_range, row_count), batches_(std::move(batches)), file_(std::move(file)) {
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
        return memory_usage_;
    }

 private:
    std::vector<SpilledBatch> batches_;
    std::shared_ptr<SpillFile> file_;
    uint64_t memory_usage_ = 0;
};

class ArrowRealtimeStore::ReadView : public RealtimeReadView {
 public:
    explicit ReadView(std::vector<std::shared_ptr<Segment>>&& segments)
        : segments_(std::move(segments)) {
        if (!segments_.empty()) {
            offset_range_ = OffsetRange(segments_.front()->GetOffsetRange().begin,
                                        segments_.back()->GetOffsetRange().end);
        }
    }

    std::optional<OffsetRange> GetOffsetRange() const override {
        return offset_range_;
    }

    const std::vector<std::shared_ptr<Segment>>& GetSegments() const {
        return segments_;
    }

 private:
    std::vector<std::shared_ptr<Segment>> segments_;
    std::optional<OffsetRange> offset_range_;
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
    SpillBatchReader(std::unique_ptr<ArrowIpcFileReader>&& file_reader,
                     const std::shared_ptr<SpillFile>& spill_file,
                     std::vector<int32_t>&& batch_indexes,
                     const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : file_reader_(std::move(file_reader)),
          spill_file_(spill_file),
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
        spill_file_.reset();
        read_schema_.reset();
        arrow_pool_.reset();
    }

 private:
    std::unique_ptr<ArrowIpcFileReader> file_reader_;
    std::shared_ptr<SpillFile> spill_file_;
    std::vector<int32_t> batch_indexes_;
    std::shared_ptr<arrow::Schema> read_schema_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> metrics_;
    size_t next_batch_ = 0;
};

class ArrowRealtimeStore::AppendQueryBatchReader : public BatchReader {
 public:
    AppendQueryBatchReader(const std::shared_ptr<ReadView>& view,
                           const std::shared_ptr<arrow::Schema>& read_schema,
                           const std::shared_ptr<PredicateFilter>& predicate_filter,
                           std::vector<int32_t>&& statistics_mapping,
                           const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                           const std::shared_ptr<MemoryPool>& memory_pool)
        : view_(view),
          read_schema_(read_schema),
          arrow_pool_(arrow_pool),
          memory_pool_(memory_pool),
          predicate_filter_(predicate_filter),
          statistics_mapping_(std::move(statistics_mapping)),
          metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        // TODO(xinyu.lxy): Memory query reads return complete stored write batches and
        // intentionally ignore the configured read batch size.
        while (view_ && next_segment_ < view_->GetSegments().size()) {
            const std::shared_ptr<Segment>& segment = view_->GetSegments()[next_segment_];
            std::shared_ptr<MemorySegment> memory_segment =
                std::dynamic_pointer_cast<MemorySegment>(segment);
            std::shared_ptr<SpilledSegment> spilled_segment =
                std::dynamic_pointer_cast<SpilledSegment>(segment);
            if (!memory_segment && !spilled_segment) {
                return Status::Invalid("unknown Arrow real-time segment type");
            }
            size_t batch_count = memory_segment ? memory_segment->GetBatches().size()
                                                : spilled_segment->GetBatches().size();
            if (next_batch_ >= batch_count) {
                ++next_segment_;
                next_batch_ = 0;
                spill_reader_.reset();
                continue;
            }
            int64_t row_count;
            const std::optional<BatchStatistics>* statistics;
            std::shared_ptr<arrow::StructArray> source;
            if (memory_segment) {
                const StoredBatch& stored = memory_segment->GetBatches()[next_batch_];
                row_count = stored.data->length();
                statistics = &stored.statistics;
                source = stored.data;
            } else if (spilled_segment) {
                const SpilledBatch& stored = spilled_segment->GetBatches()[next_batch_];
                row_count = stored.row_count;
                statistics = &stored.statistics;
            } else {
                return Status::Invalid("unknown Arrow real-time segment type");
            }
            PAIMON_ASSIGN_OR_RAISE(bool may_match,
                                   ArrowRealtimeStore::MayMatchStatistics(
                                       row_count, *statistics, read_schema_, predicate_filter_,
                                       statistics_mapping_, memory_pool_));
            if (!may_match) {
                ++next_batch_;
                continue;
            }
            if (spilled_segment) {
                if (!spill_reader_) {
                    PAIMON_ASSIGN_OR_RAISE(
                        spill_reader_,
                        ArrowIpcFileReader::Open(spilled_segment->GetFile()->GetFileSystem(),
                                                 spilled_segment->GetFile()->GetPath(),
                                                 /*use_threads=*/false, arrow_pool_));
                    if (spill_reader_->GetRecordBatchCount() !=
                        static_cast<int32_t>(spilled_segment->GetBatches().size())) {
                        return Status::Invalid(
                            "real-time spill file batch count does not match segment metadata");
                    }
                }
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> record_batch,
                                       spill_reader_->ReadRecordBatch(next_batch_));
                PAIMON_ASSIGN_OR_RAISE(source, ToStructArray(record_batch));
            }
            ++next_batch_;
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::StructArray> output,
                                   ProjectBatch(source, read_schema_, arrow_pool_.get()));
            PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, ExportStructBatch(output, arrow_pool_));
            return batch;
        }
        return MakeEofBatch();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        view_ = nullptr;
        spill_reader_.reset();
    }

 private:
    std::shared_ptr<ReadView> view_;
    std::shared_ptr<arrow::Schema> read_schema_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<PredicateFilter> predicate_filter_;
    std::vector<int32_t> statistics_mapping_;
    std::shared_ptr<Metrics> metrics_;
    std::unique_ptr<ArrowIpcFileReader> spill_reader_;
    size_t next_segment_ = 0;
    size_t next_batch_ = 0;
};

ArrowRealtimeStore::ArrowRealtimeStore(const std::shared_ptr<arrow::Schema>& write_schema,
                                       RealtimeStoreMode mode, StatisticsMode statistics_mode,
                                       const std::string& temp_directory,
                                       const std::shared_ptr<FileSystem>& spill_file_system,
                                       const std::string& spill_compression,
                                       int32_t spill_compression_level,
                                       const std::shared_ptr<MemoryPool>& memory_pool,
                                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : write_schema_(write_schema),
      memory_pool_(memory_pool),
      arrow_pool_(arrow_pool),
      mode_(mode),
      statistics_mode_(statistics_mode),
      spill_file_system_(spill_file_system),
      spill_compression_(spill_compression),
      spill_compression_level_(spill_compression_level) {
    if (!temp_directory.empty()) {
        io_manager_ = std::make_shared<IOManager>(temp_directory, spill_file_system_);
    }
}

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
    int64_t row_count, const std::optional<BatchStatistics>& statistics_optional,
    const std::shared_ptr<arrow::Schema>& read_schema,
    const std::shared_ptr<PredicateFilter>& predicate_filter,
    const std::vector<int32_t>& statistics_mapping,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!predicate_filter || !statistics_optional) {
        return true;
    }
    const BatchStatistics& statistics = statistics_optional.value();
    std::shared_ptr<InternalRow> min_row = std::make_shared<ColumnarRow>(
        statistics.min_values, statistics.min_values->fields(), memory_pool, /*row_id=*/0);
    std::shared_ptr<InternalRow> max_row = std::make_shared<ColumnarRow>(
        statistics.max_values, statistics.max_values->fields(), memory_pool, /*row_id=*/0);
    ProjectedRow projected_min(min_row, statistics_mapping);
    ProjectedRow projected_max(max_row, statistics_mapping);
    std::shared_ptr<InternalArray> null_counts = std::make_shared<ColumnarArray>(
        statistics.null_counts.get(), memory_pool, /*offset=*/0, statistics.null_counts->length());
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
    std::shared_ptr<MemorySegment> memory_segment;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (building_batches_.empty()) {
            return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
        }
        memory_segment =
            std::make_shared<MemorySegment>(building_range_.value(), std::move(building_batches_));
        sealed_segments_.push_back(memory_segment);
        sealed_memory_usage_ += building_memory_usage_;
        sealed_row_count_ += building_row_count_;
        building_batches_.clear();
        building_range_.reset();
        building_memory_usage_ = 0;
        building_row_count_ = 0;
    }

    if (!io_manager_) {
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(memory_segment);
    }

    PAIMON_ASSIGN_OR_RAISE(FileIOChannel::ID channel_id, io_manager_->CreateChannel("realtime"));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<ArrowIpcFileWriter> writer,
        ArrowIpcFileWriter::Create(spill_file_system_, channel_id.GetPath(), write_schema_,
                                   spill_compression_, spill_compression_level_,
                                   /*use_threads=*/false, arrow_pool_));
    auto cleanup_guard = ScopeGuard([&]() {
        writer.reset();
        [[maybe_unused]] Status status = spill_file_system_->Delete(channel_id.GetPath());
    });
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
    auto spilled_segment = std::make_shared<SpilledSegment>(
        memory_segment->GetOffsetRange(), memory_segment->GetRowCount(), std::move(spilled_batches),
        std::move(spill_file));
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
    return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(std::move(spilled_segment));
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
    if (mode_ == RealtimeStoreMode::APPEND_ONLY) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ArrowIpcFileReader> file_reader,
                               ArrowIpcFileReader::Open(spilled_segment->GetFile()->GetFileSystem(),
                                                        spilled_segment->GetFile()->GetPath(),
                                                        /*use_threads=*/false, arrow_pool_));
        if (file_reader->GetRecordBatchCount() !=
            static_cast<int32_t>(spilled_segment->GetBatches().size())) {
            return Status::Invalid(
                "real-time spill file batch count does not match segment metadata");
        }
        std::vector<int32_t> indexes(file_reader->GetRecordBatchCount());
        std::iota(indexes.begin(), indexes.end(), 0);
        readers.push_back(std::make_unique<SpillBatchReader>(
            std::move(file_reader), spilled_segment->GetFile(), std::move(indexes),
            /*read_schema=*/nullptr, arrow_pool_));
        return readers;
    }
    readers.reserve(spilled_segment->GetBatches().size());
    // PK merge-on-read may consume returned readers concurrently. Give every batch reader an
    // independent file handle and RecordBatchFileReader instead of serializing on a shared one.
    for (int32_t i = 0; i < static_cast<int32_t>(spilled_segment->GetBatches().size()); ++i) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ArrowIpcFileReader> file_reader,
                               ArrowIpcFileReader::Open(spilled_segment->GetFile()->GetFileSystem(),
                                                        spilled_segment->GetFile()->GetPath(),
                                                        /*use_threads=*/false, arrow_pool_));
        if (file_reader->GetRecordBatchCount() !=
            static_cast<int32_t>(spilled_segment->GetBatches().size())) {
            return Status::Invalid(
                "real-time spill file batch count does not match segment metadata");
        }
        readers.push_back(std::make_unique<SpillBatchReader>(
            std::move(file_reader), spilled_segment->GetFile(), std::vector<int32_t>{i},
            /*read_schema=*/nullptr, arrow_pool_));
    }
    return readers;
}

Result<std::shared_ptr<RealtimeReadView>> ArrowRealtimeStore::AcquireReadView() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::shared_ptr<Segment>> segments = sealed_segments_;
    if (!building_batches_.empty()) {
        segments.push_back(std::make_shared<MemorySegment>(
            building_range_.value(), std::vector<StoredBatch>(building_batches_)));
    }
    std::shared_ptr<ReadView> view = std::make_shared<ReadView>(std::move(segments));
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
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> read_schema,
                                      arrow::ImportSchema(context.read_schema));
    std::vector<std::unique_ptr<BatchReader>> readers;
    if (!arrow_view->GetOffsetRange()) {
        return readers;
    }
    std::shared_ptr<PredicateFilter> predicate_filter;
    if (context.predicate) {
        predicate_filter = std::dynamic_pointer_cast<PredicateFilter>(context.predicate);
    }
    std::vector<int32_t> statistics_mapping;
    statistics_mapping.reserve(read_schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& field : read_schema->fields()) {
        statistics_mapping.push_back(write_schema_->GetFieldIndex(field->name()));
    }
    if (mode_ == RealtimeStoreMode::PRIMARY_KEY) {
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
            // Readers returned for PK merge-on-read are independent and can be consumed in
            // parallel by upper layers.
            for (int32_t batch_index : matching_indexes) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::unique_ptr<ArrowIpcFileReader> file_reader,
                    ArrowIpcFileReader::Open(spilled_segment->GetFile()->GetFileSystem(),
                                             spilled_segment->GetFile()->GetPath(),
                                             /*use_threads=*/false, arrow_pool_));
                if (file_reader->GetRecordBatchCount() !=
                    static_cast<int32_t>(spilled_segment->GetBatches().size())) {
                    return Status::Invalid(
                        "real-time spill file batch count does not match segment metadata");
                }
                readers.push_back(std::make_unique<SpillBatchReader>(
                    std::move(file_reader), spilled_segment->GetFile(),
                    std::vector<int32_t>{batch_index}, read_schema, arrow_pool_));
            }
        }
        return readers;
    }

    std::unique_ptr<BatchReader> reader = std::make_unique<AppendQueryBatchReader>(
        arrow_view, read_schema, predicate_filter, std::move(statistics_mapping), arrow_pool_,
        memory_pool_);
    readers.push_back(std::move(reader));
    return readers;
}

Status ArrowRealtimeStore::AdvanceCommittedOffset(int64_t committed_end_offset) {
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
    return Status::OK();
}

RealtimeStoreDataUsage ArrowRealtimeStore::GetDataUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return RealtimeStoreDataUsage{building_memory_usage_, sealed_memory_usage_, building_row_count_,
                                  sealed_row_count_};
}

uint64_t ArrowRealtimeStore::GetMemoryUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return building_memory_usage_ + sealed_memory_usage_;
}

}  // namespace paimon
