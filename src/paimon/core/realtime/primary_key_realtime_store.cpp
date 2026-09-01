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

#include "paimon/core/realtime/primary_key_realtime_store.h"

#include <algorithm>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/macros.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

namespace {

struct StoredBatch {
    std::shared_ptr<arrow::StructArray> data;
    OffsetRange offset_range;
    uint64_t memory_usage;
};

class Segment final : public RealtimeSegmentHandle {
 public:
    Segment(const OffsetRange& range, std::vector<StoredBatch>&& batches)
        : range_(range), batches_(std::move(batches)) {}

    OffsetRange GetOffsetRange() const override {
        return range_;
    }
    const std::vector<StoredBatch>& Batches() const {
        return batches_;
    }

 private:
    OffsetRange range_;
    std::vector<StoredBatch> batches_;
};

class ReadView final : public RealtimeReadView {
 public:
    explicit ReadView(std::vector<std::shared_ptr<Segment>>&& segments)
        : segments_(std::move(segments)) {
        if (!segments_.empty()) {
            range_ = OffsetRange(segments_.front()->GetOffsetRange().begin,
                                 segments_.back()->GetOffsetRange().end);
        }
    }

    std::optional<OffsetRange> GetOffsetRange() const override {
        return range_;
    }
    const std::vector<std::shared_ptr<Segment>>& Segments() const {
        return segments_;
    }

 private:
    std::vector<std::shared_ptr<Segment>> segments_;
    std::optional<OffsetRange> range_;
};

class StoredBatchReader final : public BatchReader {
 public:
    explicit StoredBatchReader(const StoredBatch& batch,
                               std::shared_ptr<arrow::MemoryPool> arrow_pool)
        : arrow_pool_(std::move(arrow_pool)),
          data_(batch.data),
          metrics_(std::make_shared<MetricsImpl>()) {}

    Result<ReadBatch> NextBatch() override {
        if (!data_) {
            return MakeEofBatch();
        }
        auto array = std::make_unique<ArrowArray>();
        auto schema = std::make_unique<ArrowSchema>();
        ScopeGuard export_guard([array_ptr = array.get(), schema_ptr = schema.get()]() {
            ArrowArrayRelease(array_ptr);
            ArrowSchemaRelease(schema_ptr);
        });
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::RecordBatch> record_batch,
            arrow::RecordBatch::FromStructArray(data_, arrow_pool_.get()));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::RecordBatch> normalized_batch,
            ArrowUtils::NormalizeRecordBatchOffsets(record_batch, arrow_pool_.get()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportRecordBatch(*normalized_batch, array.get(), schema.get()));
        PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(array.get(), schema.get(), arrow_pool_));
        data_.reset();
        arrow_pool_.reset();
        export_guard.Release();
        return ReadBatch(std::move(array), std::move(schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }
    void Close() override {
        data_.reset();
        arrow_pool_.reset();
    }

 private:
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<arrow::StructArray> data_;
    std::shared_ptr<Metrics> metrics_;
};

}  // namespace

class PrimaryKeyRealtimeStore::Impl {
 public:
    Impl(std::shared_ptr<arrow::Schema> transport_schema,
         std::shared_ptr<arrow::MemoryPool> arrow_pool)
        : transport_schema_(std::move(transport_schema)), arrow_pool_(std::move(arrow_pool)) {}

    Status Write(RealtimeWriteBatch&& write_batch) {
        if (!write_batch.batch || !write_batch.batch->GetData()) {
            return Status::Invalid("PK real-time write batch is null");
        }
        const int64_t row_count = write_batch.batch->GetData()->length;
        if (write_batch.offset_range.begin < 0 || write_batch.offset_range.Count() != row_count ||
            row_count <= 0) {
            return Status::Invalid("PK real-time offset range does not match batch row count");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ImportArray(write_batch.batch->GetData(),
                               arrow::struct_(transport_schema_->fields())));
        if (!array || array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("PK real-time transport batch is not a StructArray");
        }
        std::shared_ptr<arrow::StructArray> transport =
            checked_pointer_cast<arrow::StructArray>(array);
        std::lock_guard<std::mutex> lock(mutex_);
        building_.push_back(StoredBatch{transport, write_batch.offset_range,
                                        ArrowUtils::GetArrayMemoryUsage(transport->data())});
        building_memory_usage_ += building_.back().memory_usage;
        return Status::OK();
    }

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (building_.empty()) {
            return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
        }
        OffsetRange range(building_.front().offset_range.begin, building_.back().offset_range.end);
        std::shared_ptr<Segment> segment = std::make_shared<Segment>(range, std::move(building_));
        sealed_.push_back(segment);
        building_.clear();
        building_memory_usage_ = 0;
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>(std::move(segment));
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& handle) {
        std::shared_ptr<Segment> segment = std::dynamic_pointer_cast<Segment>(handle);
        if (!segment) {
            return Status::Invalid("segment was not created by the PK real-time store");
        }
        std::vector<std::unique_ptr<BatchReader>> readers;
        readers.reserve(segment->Batches().size());
        for (const StoredBatch& batch : segment->Batches()) {
            readers.push_back(std::make_unique<StoredBatchReader>(batch, arrow_pool_));
        }
        return readers;
    }

    Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::shared_ptr<Segment>> segments = sealed_;
        if (!building_.empty()) {
            OffsetRange range(building_.front().offset_range.begin,
                              building_.back().offset_range.end);
            segments.push_back(
                std::make_shared<Segment>(range, std::vector<StoredBatch>(building_)));
        }
        return std::make_shared<ReadView>(std::move(segments));
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view, int64_t,
        const RealtimeQueryContext& context) {
        std::shared_ptr<ReadView> typed = std::dynamic_pointer_cast<ReadView>(view);
        if (!typed) {
            return Status::Invalid("read view was not created by the PK real-time store");
        }
        if (context.read_schema == nullptr || context.read_schema->release == nullptr) {
            return Status::Invalid("PK real-time query read schema is null");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> read_schema,
                                          arrow::ImportSchema(context.read_schema));
        std::vector<std::unique_ptr<BatchReader>> readers;
        for (const std::shared_ptr<Segment>& segment : typed->Segments()) {
            for (const StoredBatch& batch : segment->Batches()) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<arrow::Array> projected,
                    NestedProjectionUtils::AlignArrayToReadType(
                        batch.data, arrow::struct_(read_schema->fields()), arrow_pool_.get()));
                if (!projected || projected->type_id() != arrow::Type::STRUCT) {
                    return Status::Invalid(
                        "PK memory query projection did not produce a StructArray");
                }
                StoredBatch query_batch{checked_pointer_cast<arrow::StructArray>(projected),
                                        batch.offset_range, /*memory_usage=*/0};
                readers.push_back(std::make_unique<StoredBatchReader>(query_batch, arrow_pool_));
            }
        }
        return readers;
    }

    Status AdvanceCommittedOffset(int64_t committed_end_offset) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto first_retained = std::find_if(
            sealed_.begin(), sealed_.end(), [committed_end_offset](const auto& segment) {
                return segment->GetOffsetRange().end > committed_end_offset;
            });
        sealed_.erase(sealed_.begin(), first_retained);
        return Status::OK();
    }

    uint64_t GetMemoryUsage() const {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t total = building_memory_usage_;
        for (const std::shared_ptr<Segment>& segment : sealed_) {
            for (const StoredBatch& batch : segment->Batches()) {
                total += batch.memory_usage;
            }
        }
        return total;
    }

 private:
    std::shared_ptr<arrow::Schema> transport_schema_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    mutable std::mutex mutex_;
    std::vector<StoredBatch> building_;
    std::vector<std::shared_ptr<Segment>> sealed_;
    uint64_t building_memory_usage_ = 0;
};

PrimaryKeyRealtimeStore::PrimaryKeyRealtimeStore(std::unique_ptr<Impl>&& impl)
    : impl_(std::move(impl)) {}
PrimaryKeyRealtimeStore::~PrimaryKeyRealtimeStore() = default;

Result<std::shared_ptr<PrimaryKeyRealtimeStore>> PrimaryKeyRealtimeStore::Create(
    const std::shared_ptr<arrow::Schema>& transport_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!memory_pool) {
        return Status::Invalid("PK real-time store memory pool is null");
    }
    std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(memory_pool);
    return std::shared_ptr<PrimaryKeyRealtimeStore>(new PrimaryKeyRealtimeStore(
        std::make_unique<Impl>(transport_schema, std::move(arrow_pool))));
}
Status PrimaryKeyRealtimeStore::Write(RealtimeWriteBatch&& batch) {
    return impl_->Write(std::move(batch));
}
Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>>
PrimaryKeyRealtimeStore::SealForCommit() {
    return impl_->SealForCommit();
}
Result<std::vector<std::unique_ptr<BatchReader>>> PrimaryKeyRealtimeStore::CreateCommitReaders(
    const std::shared_ptr<RealtimeSegmentHandle>& segment) {
    return impl_->CreateCommitReaders(segment);
}
Result<std::shared_ptr<RealtimeReadView>> PrimaryKeyRealtimeStore::AcquireReadView() {
    return impl_->AcquireReadView();
}
Result<std::vector<std::unique_ptr<BatchReader>>> PrimaryKeyRealtimeStore::CreateQueryReaders(
    const std::shared_ptr<RealtimeReadView>& view, int64_t offset,
    const RealtimeQueryContext& context) {
    return impl_->CreateQueryReaders(view, offset, context);
}
Status PrimaryKeyRealtimeStore::AdvanceCommittedOffset(int64_t committed_end_offset) {
    return impl_->AdvanceCommittedOffset(committed_end_offset);
}
uint64_t PrimaryKeyRealtimeStore::GetMemoryUsage() const {
    return impl_->GetMemoryUsage();
}

}  // namespace paimon
