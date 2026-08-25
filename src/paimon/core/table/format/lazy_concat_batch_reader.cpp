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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/table/format/lazy_concat_batch_reader.h"

#include <string>
#include <utility>

// `ReadBatch` holds `unique_ptr`s to these, so destroying one needs their definitions.
#include "arrow/c/abi.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"

namespace paimon {

namespace {
/// Says which file a failure came from, keeping the status code it carried.
Status WithFileName(const std::string& name, const Status& status) {
    return Status(status.code(), fmt::format("cannot read {}: {}", name, status.message()));
}
}  // namespace

LazyConcatBatchReader::LazyConcatBatchReader(std::vector<Source>&& sources,
                                             const std::shared_ptr<MemoryPool>& pool)
    : arrow_pool_(GetArrowPool(pool)),
      sources_(std::move(sources)),
      closed_metrics_(std::make_shared<MetricsImpl>()) {}

LazyConcatBatchReader::~LazyConcatBatchReader() {
    DoClose();
}

void LazyConcatBatchReader::CloseCurrent() {
    if (current_reader_ == nullptr) {
        return;
    }
    // Taken before the reader goes, or everything read through it is missing from the totals.
    std::shared_ptr<Metrics> metrics = current_reader_->GetReaderMetrics();
    current_reader_->Close();
    if (metrics != nullptr) {
        closed_metrics_->Merge(metrics);
    }
    // Kept until this reader goes: a batch it handed out is allocated from a pool it owns.
    // Closing already released the file, so little stays behind.
    closed_readers_.push_back(std::move(current_reader_));
    current_name_.clear();
}

Result<BatchReader::ReadBatchWithBitmap> LazyConcatBatchReader::NextBatchWithBitmap() {
    // `BatchReader` promises a failure is terminal: keep answering with it rather than moving
    // on to the next file.
    if (!failure_.ok()) {
        return failure_;
    }
    while (true) {
        if (current_reader_ == nullptr) {
            if (next_source_ >= sources_.size()) {
                return BatchReader::MakeEofBatchWithBitmap();
            }
            Source& source = sources_[next_source_];
            Result<std::unique_ptr<BatchReader>> opened = source.open();
            if (!opened.ok()) {
                failure_ = WithFileName(source.name, opened.status());
                return failure_;
            }
            next_source_++;
            current_reader_ = std::move(opened).value();
            if (current_reader_ == nullptr) {
                continue;
            }
            current_name_ = source.name;
        }
        Result<BatchReader::ReadBatchWithBitmap> result = current_reader_->NextBatchWithBitmap();
        if (!result.ok()) {
            failure_ = WithFileName(current_name_, result.status());
            return failure_;
        }
        if (!BatchReader::IsEofBatch(result.value())) {
            return std::move(result).value();
        }
        CloseCurrent();
    }
}

Result<BatchReader::ReadBatch> LazyConcatBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           NextBatchWithBitmap());
    return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool_.get());
}

void LazyConcatBatchReader::Close() {
    DoClose();
}

void LazyConcatBatchReader::DoClose() {
    CloseCurrent();
    // Whatever was never reached was never opened.
    next_source_ = sources_.size();
}

std::shared_ptr<Metrics> LazyConcatBatchReader::GetReaderMetrics() const {
    auto metrics = std::make_shared<MetricsImpl>();
    metrics->Merge(closed_metrics_);
    if (current_reader_ != nullptr) {
        std::shared_ptr<Metrics> current = current_reader_->GetReaderMetrics();
        if (current != nullptr) {
            metrics->Merge(current);
        }
    }
    return metrics;
}

}  // namespace paimon
