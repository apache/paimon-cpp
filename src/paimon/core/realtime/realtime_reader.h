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

#pragma once

#include <memory>
#include <utility>

#include "paimon/reader/batch_reader.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/result.h"

namespace paimon {

class RealtimeReader final : public BatchReader {
 public:
    static Result<std::unique_ptr<RealtimeReader>> Create(std::shared_ptr<MemReadView> read_view,
                                                          std::unique_ptr<BatchReader> reader) {
        if (!read_view) {
            return Status::Invalid("real-time reader view is null");
        }
        if (!reader) {
            return Status::Invalid("real-time inner reader is null");
        }
        return std::unique_ptr<RealtimeReader>(
            new RealtimeReader(std::move(read_view), std::move(reader)));
    }

    Result<ReadBatch> NextBatch() override {
        return reader_->NextBatch();
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        return reader_->NextBatchWithBitmap();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
        read_view_.reset();
    }

 private:
    RealtimeReader(std::shared_ptr<MemReadView> read_view, std::unique_ptr<BatchReader> reader)
        : read_view_(std::move(read_view)), reader_(std::move(reader)) {}

    // Keep the view before the delegated reader so reverse member destruction closes the reader
    // before releasing the data it references.
    std::shared_ptr<MemReadView> read_view_;
    std::unique_ptr<BatchReader> reader_;
};

}  // namespace paimon
