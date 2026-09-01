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

#include "paimon/common/reader/concat_batch_reader.h"

#include <utility>

#include "arrow/c/abi.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"

namespace paimon {
class MemoryPool;

ConcatBatchReader::ConcatBatchReader(std::vector<std::unique_ptr<BatchReader>>&& readers,
                                     const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : arrow_pool_(arrow_pool),
      finished_reader_metrics_(std::make_shared<MetricsImpl>()),
      readers_(std::move(readers)),
      current_(0) {}

Result<BatchReader::ReadBatch> ConcatBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           NextBatchWithBitmap());
    PAIMON_ASSIGN_OR_RAISE(
        BatchReader::ReadBatch batch,
        ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool_));
    return batch;
}

void ConcatBatchReader::Close() {
    for (; current_ < readers_.size(); current_++) {
        CloseAndReleaseReader(current_);
    }
}

std::shared_ptr<Metrics> ConcatBatchReader::GetReaderMetrics() const {
    auto metrics = std::make_shared<MetricsImpl>();
    metrics->Merge(finished_reader_metrics_);
    metrics->Merge(MetricsImpl::CollectReadMetrics(readers_));
    return metrics;
}

void ConcatBatchReader::CloseAndReleaseReader(size_t reader_index) {
    std::unique_ptr<BatchReader>& reader = readers_[reader_index];
    if (!reader) {
        return;
    }
    reader->Close();
    finished_reader_metrics_->Merge(reader->GetReaderMetrics());
    reader.reset();
}

Result<BatchReader::ReadBatchWithBitmap> ConcatBatchReader::NextBatchWithBitmap() {
    while (current_ < readers_.size()) {
        auto& current_reader = readers_[current_];
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap result,
                               current_reader->NextBatchWithBitmap());
        if (!BatchReader::IsEofBatch(result)) {
            // current reader not eof, just return
            return result;
        }
        // current meets eof, move to next reader
        CloseAndReleaseReader(current_);
        current_++;
    }
    // read finish
    return BatchReader::MakeEofBatchWithBitmap();
}

}  // namespace paimon
