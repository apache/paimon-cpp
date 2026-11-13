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

#include <type_traits>
#include <utility>

#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"

namespace paimon {

WarmupFileBatchReader::WarmupFileBatchReader(std::shared_ptr<PrefetchFileBatchReader> reader,
                                             std::shared_ptr<FileReadAhead> read_ahead,
                                             WarmupLevel level)
    : reader_(std::move(reader)), read_ahead_(std::move(read_ahead)), level_(level) {}

WarmupFileBatchReader::~WarmupFileBatchReader() {
    ResetReadState();
    read_ahead_->Close();
}

void WarmupFileBatchReader::ResetReadState() {
    prepared_ = false;
    batch_returned_ = false;
    read_status_ = Status::OK();
}

void WarmupFileBatchReader::Prepare() {
    prepared_ = true;
}

void WarmupFileBatchReader::Warmup() {
    if (!prepared_ || closed_ || level_ != WarmupLevel::RAW) {
        return;
    }
    (void)read_ahead_->Warmup(reader_.get());
}

template <typename Batch>
Result<Batch> WarmupFileBatchReader::ReadOneBatch() {
    if (closed_) {
        return Status::Invalid("cannot read a closed warmup reader");
    }
    PAIMON_RETURN_NOT_OK(read_status_);
    batch_returned_ = false;
    Result<Batch> result = [&]() -> Result<Batch> {
        PAIMON_RETURN_NOT_OK(read_ahead_->Warmup(reader_.get()));
        // Preserve the format reader's mapping for the API used to consume this batch.
        Result<Batch> read = [&]() {
            if constexpr (std::is_same_v<Batch, ReadBatch>) {
                return reader_->NextBatch();
            } else {
                return reader_->NextBatchWithBitmap();
            }
        }();
        PAIMON_ASSIGN_OR_RAISE(Batch batch, std::move(read));
        Status status = read_ahead_->GetStatus();
        if (!status.ok()) {
            if constexpr (std::is_same_v<Batch, ReadBatch>) {
                ReaderUtils::ReleaseReadBatch(std::move(batch));
            } else {
                ReaderUtils::ReleaseReadBatch(std::move(batch.first));
            }
            return status;
        }
        return batch;
    }();
    if (result.ok() && !BatchReader::IsEofBatch(result.value())) {
        batch_returned_ = true;
    } else if (!result.ok()) {
        read_status_ = result.status();
    }
    return result;
}

Result<BatchReader::ReadBatchWithBitmap> WarmupFileBatchReader::NextBatchWithBitmap() {
    return ReadOneBatch<ReadBatchWithBitmap>();
}

Result<BatchReader::ReadBatch> WarmupFileBatchReader::NextBatch() {
    return ReadOneBatch<ReadBatch>();
}

Status WarmupFileBatchReader::SetReadSchema(::ArrowSchema* schema,
                                            const std::shared_ptr<Predicate>& predicate,
                                            const std::optional<RoaringBitmap32>& bitmap) {
    ResetReadState();
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
    auto metrics = std::make_shared<MetricsImpl>();
    metrics->Merge(reader_->GetReaderMetrics());
    metrics->Merge(read_ahead_->GetMetrics());
    metrics->SetGauge(PrefetchMetrics::ENABLED, 0.0);
    metrics->SetGauge(PrefetchMetrics::PARALLELISM, 1.0);
    return metrics;
}

Result<std::unique_ptr<::ArrowSchema>> WarmupFileBatchReader::GetFileSchema() const {
    return reader_->GetFileSchema();
}

Result<uint64_t> WarmupFileBatchReader::GetPreviousBatchFileRowId(uint64_t batch_row_id) const {
    if (!batch_returned_) {
        return Status::Invalid("no previous batch returned by warmup reader");
    }
    return reader_->GetPreviousBatchFileRowId(batch_row_id);
}

Result<uint64_t> WarmupFileBatchReader::GetNumberOfRows() const {
    return reader_->GetNumberOfRows();
}

bool WarmupFileBatchReader::SupportPreciseBitmapSelection() const {
    return reader_->SupportPreciseBitmapSelection();
}

void WarmupFileBatchReader::Close() {
    if (closed_) {
        return;
    }
    ResetReadState();
    closed_ = true;
    read_ahead_->Close();
    reader_->Close();
}

}  // namespace paimon
