/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may obtain a copy of the License at
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
#include "paimon/common/reader/warmup_file_batch_reader.h"

#include <utility>

#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"

namespace paimon {

WarmupFileBatchReader::WarmupFileBatchReader(std::shared_ptr<PrefetchFileBatchReader> reader,
                                             std::shared_ptr<FileReadAhead> read_ahead,
                                             WarmupLevel level,
                                             std::shared_ptr<arrow::MemoryPool> arrow_pool)
    : reader_(std::move(reader)),
      read_ahead_(std::move(read_ahead)),
      level_(level),
      arrow_pool_(std::move(arrow_pool)) {}

WarmupFileBatchReader::~WarmupFileBatchReader() {
    ResetWarmup();
    reader_->SetPreBufferRangeCallback(nullptr);
    read_ahead_->ReleaseBuffers();
}

void WarmupFileBatchReader::Join() const {
    if (thread_) {
        thread_->join();
        thread_.reset();
    }
}

void WarmupFileBatchReader::ResetWarmup() {
    Join();
    if (batch_ && batch_->ok()) {
        ReaderUtils::ReleaseReadBatch(std::move(batch_.value()).value().first);
    }
    batch_.reset();
    prepared_ = false;
    started_ = false;
    batch_returned_ = false;
    read_status_ = Status::OK();
}

void WarmupFileBatchReader::Prepare() {
    prepared_ = true;
}

void WarmupFileBatchReader::Warmup() {
    if (!prepared_ || closed_ || started_ || level_ == WarmupLevel::NONE) {
        return;
    }
    if (level_ == WarmupLevel::RAW) {
        (void)read_ahead_->Warmup(reader_.get());
    } else {
        started_ = true;
        thread_ = std::make_unique<std::thread>([this]() { batch_.emplace(ReadOneBatch()); });
    }
}

Result<BatchReader::ReadBatchWithBitmap> WarmupFileBatchReader::ReadOneBatch() {
    PAIMON_RETURN_NOT_OK(read_ahead_->Warmup(reader_.get()));
    PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, reader_->NextBatchWithBitmap());
    Status status = read_ahead_->GetStatus();
    if (!status.ok()) {
        ReaderUtils::ReleaseReadBatch(std::move(batch.first));
        return status;
    }
    return batch;
}

Result<BatchReader::ReadBatchWithBitmap> WarmupFileBatchReader::NextBatchWithBitmap() {
    if (closed_) {
        return Status::Invalid("cannot read a closed warmup reader");
    }
    PAIMON_RETURN_NOT_OK(read_status_);
    // A normal read is synchronous unless Warmup() explicitly prepared the first batch.
    started_ = true;
    Join();
    batch_returned_ = false;
    Result<ReadBatchWithBitmap> result = batch_ ? std::move(batch_.value()) : ReadOneBatch();
    batch_.reset();
    if (result.ok() && !BatchReader::IsEofBatch(result.value())) {
        batch_returned_ = true;
    } else if (!result.ok()) {
        read_status_ = result.status();
    }
    return result;
}

Result<BatchReader::ReadBatch> WarmupFileBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, NextBatchWithBitmap());
    return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch), arrow_pool_);
}

Status WarmupFileBatchReader::SetReadSchema(::ArrowSchema* schema,
                                            const std::shared_ptr<Predicate>& predicate,
                                            const std::optional<RoaringBitmap32>& bitmap) {
    ResetWarmup();
    if (closed_) {
        return Status::Invalid("cannot configure a closed warmup reader");
    }
    read_ahead_->Reset();
    read_status_ = reader_->SetReadSchema(schema, predicate, bitmap);
    PAIMON_RETURN_NOT_OK(read_status_);
    Prepare();
    return Status::OK();
}

std::shared_ptr<Metrics> WarmupFileBatchReader::GetReaderMetrics() const {
    // The underlying reader may be decoding. Do not inspect its mutable metrics until it stops.
    // Joining is reserved for operations needing the format reader, so a live snapshot contains
    // only the thread-safe cache and I/O counters while the first batch is being prepared.
    auto metrics = std::make_shared<MetricsImpl>();
    if (!thread_) {
        metrics->Merge(reader_->GetReaderMetrics());
    }
    metrics->Merge(read_ahead_->GetMetrics());
    metrics->SetGauge(PrefetchMetrics::ENABLED, 0.0);
    metrics->SetGauge(PrefetchMetrics::PARALLELISM, 1.0);
    return metrics;
}

Result<std::unique_ptr<::ArrowSchema>> WarmupFileBatchReader::GetFileSchema() const {
    Join();
    return reader_->GetFileSchema();
}

Result<uint64_t> WarmupFileBatchReader::GetPreviousBatchFileRowId(uint64_t batch_row_id) const {
    if (!batch_returned_) {
        return Status::Invalid("no previous batch returned by warmup reader");
    }
    return reader_->GetPreviousBatchFileRowId(batch_row_id);
}

Result<uint64_t> WarmupFileBatchReader::GetNumberOfRows() const {
    Join();
    return reader_->GetNumberOfRows();
}

bool WarmupFileBatchReader::SupportPreciseBitmapSelection() const {
    Join();
    return reader_->SupportPreciseBitmapSelection();
}

void WarmupFileBatchReader::Close() {
    if (closed_) {
        return;
    }
    ResetWarmup();
    closed_ = true;
    reader_->SetPreBufferRangeCallback(nullptr);
    read_ahead_->ReleaseBuffers();
    reader_->Close();
}

}  // namespace paimon
