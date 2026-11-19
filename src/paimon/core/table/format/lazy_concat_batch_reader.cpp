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

#include "arrow/c/abi.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"

namespace paimon {

namespace {
Status WithSourceName(const std::string& name, const Status& status) {
    return status.WithMessage(fmt::format("cannot read {}: {}", name, status.message()));
}
}  // namespace

LazyConcatBatchReader::LazyConcatBatchReader(std::vector<Source>&& sources,
                                             const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : arrow_pool_(arrow_pool),
      sources_(std::move(sources)),
      closed_metrics_(std::make_shared<MetricsImpl>()) {}

LazyConcatBatchReader::~LazyConcatBatchReader() {
    DoClose();
}

void LazyConcatBatchReader::CloseCurrent() {
    if (current_reader_ == nullptr) {
        return;
    }
    // Closing drains pending preparation and prefetch work before taking the final metrics.
    current_reader_->Close();
    closed_metrics_->Merge(current_reader_->GetReaderMetrics());
    current_reader_.reset();
    current_name_.clear();
}

Result<BatchReader::ReadBatchWithBitmap> LazyConcatBatchReader::NextBatchWithBitmap() {
    if (!failure_.ok()) {
        return failure_;
    }
    while (true) {
        if (current_reader_ == nullptr) {
            if (next_source_ >= sources_.size()) {
                DoClose();
                return BatchReader::MakeEofBatchWithBitmap();
            }
            Source source = std::exchange(sources_[next_source_++], Source{});
            Result<std::unique_ptr<BatchReader>> opened = source.open();
            if (!opened.ok()) {
                failure_ = WithSourceName(source.name, opened.status());
                DoClose();
                return failure_;
            }
            current_reader_ = std::move(opened).value();
            if (current_reader_ == nullptr) {
                continue;
            }
            current_name_ = std::move(source.name);
        }
        Result<BatchReader::ReadBatchWithBitmap> result = current_reader_->NextBatchWithBitmap();
        if (!result.ok()) {
            failure_ = WithSourceName(current_name_, result.status());
            DoClose();
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
    auto result = ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool_);
    if (!result.ok()) {
        failure_ = WithSourceName(current_name_, result.status());
        DoClose();
        return failure_;
    }
    return result;
}

void LazyConcatBatchReader::Close() {
    DoClose();
}

void LazyConcatBatchReader::DoClose() {
    CloseCurrent();
    std::vector<Source>().swap(sources_);
    next_source_ = 0;
}

std::shared_ptr<Metrics> LazyConcatBatchReader::GetReaderMetrics() const {
    auto metrics = std::make_shared<MetricsImpl>();
    metrics->Merge(closed_metrics_);
    if (current_reader_ != nullptr) {
        metrics->Merge(current_reader_->GetReaderMetrics());
    }
    return metrics;
}

}  // namespace paimon
