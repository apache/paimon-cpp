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

#include <algorithm>
#include <utility>

#include "arrow/c/abi.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {
class MemoryPool;

ConcatBatchReader::ConcatBatchReader(std::vector<std::unique_ptr<BatchReader>>&& readers,
                                     const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : arrow_pool_(arrow_pool),
      finished_reader_metrics_(std::make_shared<MetricsImpl>()),
      readers_(std::move(readers)),
      file_readers_(readers_.size(), nullptr),
      current_(0) {
    // Non-file readers keep their null slots so the warmup window stays aligned with readers_.
    for (size_t i = 0; i < readers_.size(); i++) {
        if (auto* file_reader = dynamic_cast<FileBatchReader*>(readers_[i].get())) {
            file_readers_[i] = file_reader;
        }
    }
}

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
    file_readers_[reader_index] = nullptr;
}

void ConcatBatchReader::WarmupRange(size_t idx, size_t count) {
    const size_t end = std::min(idx + count, file_readers_.size());
    for (size_t i = idx; i < end; i++) {
        if (file_readers_[i] != nullptr) {
            file_readers_[i]->Warmup();
        }
    }
}

Result<BatchReader::ReadBatchWithBitmap> ConcatBatchReader::NextBatchWithBitmap() {
    while (current_ < readers_.size()) {
        // Lookahead: when the file system reads asynchronously, the next file's first read is paid
        // while this file is still being consumed, instead of serially after its EOF.
        WarmupRange(current_, 1 + kWarmupLookahead);
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
