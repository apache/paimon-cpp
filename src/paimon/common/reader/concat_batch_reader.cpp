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
#include <chrono>
#include <utility>

#include "arrow/c/abi.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {
class MemoryPool;

ConcatBatchReader::ConcatBatchReader(std::vector<std::unique_ptr<BatchReader>>&& readers,
                                     const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : arrow_pool_(arrow_pool), finished_reader_metrics_(std::make_shared<MetricsImpl>()) {
    for (auto& reader : readers) {
        readers_.emplace_back();
        readers_.back().file_reader = dynamic_cast<FileBatchReader*>(reader.get());
        readers_.back().reader = std::move(reader);
    }
}

ConcatBatchReader::~ConcatBatchReader() {
    ConcatBatchReader::Close();
}

Result<std::unique_ptr<ConcatBatchReader>> ConcatBatchReader::CreateLazy(
    size_t reader_count, ReaderFactory factory,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
    const std::shared_ptr<Executor>& build_executor) {
    if (!factory || !build_executor) {
        return Status::Invalid("lazy reader requires a factory and a build executor");
    }
    auto reader = std::make_unique<ConcatBatchReader>(std::vector<std::unique_ptr<BatchReader>>(),
                                                      arrow_pool);
    reader->preparation_ = std::make_shared<Preparation>(std::move(factory));
    reader->build_executor_ = build_executor;
    reader->reader_count_ = reader_count;
    reader->FillWindow();
    for (auto& state : reader->readers_) {
        PAIMON_RETURN_NOT_OK(reader->ResolveReader(&state, /*wait=*/true));
    }
    return reader;
}

void ConcatBatchReader::FillWindow() {
    while (next_reader_ < reader_count_ && readers_.size() < kPreparationWindow) {
        readers_.emplace_back();
        readers_.back().future =
            Via(build_executor_.get(),
                [preparation = preparation_,
                 index = next_reader_++]() mutable -> Result<std::unique_ptr<BatchReader>> {
                    // Release the task's creator before Via publishes its result, so draining
                    // the future also releases the background owner of the creation context.
                    auto retained = std::move(preparation);
                    if (retained->cancelled.load()) {
                        return std::unique_ptr<BatchReader>();
                    }
                    return retained->factory(index);
                });
    }
}

Status ConcatBatchReader::ResolveReader(ReaderState* state, bool wait) {
    if (state->future.valid() &&
        (wait || state->future.wait_for(std::chrono::seconds(0)) == std::future_status::ready)) {
        auto result = state->future.get();
        if (!result.ok()) {
            state->failure = result.status();
        } else {
            state->reader = std::move(result).value();
            state->file_reader = dynamic_cast<FileBatchReader*>(state->reader.get());
        }
    }
    return state->failure;
}

Result<BatchReader::ReadBatch> ConcatBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           NextBatchWithBitmap());
    auto result = ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool_);
    if (!result.ok()) {
        failure_ = result.status();
        Close();
    }
    return result;
}

void ConcatBatchReader::Close() {
    if (preparation_) {
        preparation_->cancelled.store(true);
    }
    for (auto& state : readers_) {
        try {
            (void)ResolveReader(&state, /*wait=*/true);
        } catch (...) {
            // Construction exceptions captured by Via must not escape destruction or prevent
            // the remaining tasks from being drained.
        }
        CloseAndReleaseReader(&state);
    }
    readers_.clear();
    next_reader_ = reader_count_;
    preparation_.reset();
}

std::shared_ptr<Metrics> ConcatBatchReader::GetReaderMetrics() const {
    auto metrics = std::make_shared<MetricsImpl>();
    metrics->Merge(finished_reader_metrics_);
    for (const auto& state : readers_) {
        if (state.reader) {
            metrics->Merge(state.reader->GetReaderMetrics());
        }
    }
    return metrics;
}

void ConcatBatchReader::CloseAndReleaseReader(ReaderState* state) {
    std::unique_ptr<BatchReader>& reader = state->reader;
    if (!reader) {
        return;
    }
    reader->Close();
    finished_reader_metrics_->Merge(reader->GetReaderMetrics());
    reader.reset();
    state->file_reader = nullptr;
}

void ConcatBatchReader::WarmupRange() {
    const size_t end = std::min(1 + kWarmupLookahead, readers_.size());
    for (size_t i = 0; i < end; i++) {
        // Never stall the current file on lookahead. Keep a failed preparation until its turn.
        (void)ResolveReader(&readers_[i], /*wait=*/false);
        if (readers_[i].file_reader != nullptr) {
            readers_[i].file_reader->Warmup();
        }
    }
}

Result<BatchReader::ReadBatchWithBitmap> ConcatBatchReader::NextBatchWithBitmap() {
    PAIMON_RETURN_NOT_OK(failure_);
    while (!readers_.empty()) {
        failure_ = ResolveReader(&readers_.front(), /*wait=*/true);
        if (!failure_.ok()) {
            Close();
            return failure_;
        }
        // Lookahead: when the file system reads asynchronously, the next file's first read is paid
        // while this file is still being consumed, instead of serially after its EOF.
        WarmupRange();
        auto& current_reader = readers_.front().reader;
        if (current_reader) {
            auto result = current_reader->NextBatchWithBitmap();
            if (!result.ok()) {
                failure_ = result.status();
                Close();
                return failure_;
            }
            if (!BatchReader::IsEofBatch(result.value())) {
                return result;
            }
        }
        CloseAndReleaseReader(&readers_.front());
        readers_.pop_front();
        FillWindow();
    }
    preparation_.reset();
    return BatchReader::MakeEofBatchWithBitmap();
}

}  // namespace paimon
