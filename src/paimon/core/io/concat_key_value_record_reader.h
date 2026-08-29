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

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/result.h"

namespace paimon {
class Metrics;

/// This reader is to concatenate a list of KeyValueRecordReader and read them sequentially.
/// The input list is already sorted by key and sequence number, and the key intervals do not
/// overlap each other.
class ConcatKeyValueRecordReader : public KeyValueRecordReader {
 public:
    explicit ConcatKeyValueRecordReader(
        std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers)
        : finished_reader_metrics_(std::make_shared<MetricsImpl>()), readers_(std::move(readers)) {}

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override {
        while (current_ < readers_.size()) {
            auto& current_reader = readers_[current_];
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<KeyValueRecordReader::Iterator> iterator,
                                   current_reader->NextBatch());
            if (iterator != nullptr) {
                return iterator;
            }
            // current meets eof, move to next reader
            CloseAndReleaseReader(current_);
            current_++;
        }
        // read finish
        return std::unique_ptr<KeyValueRecordReader::Iterator>();
    }

    void Close() override {
        for (; current_ < readers_.size(); current_++) {
            CloseAndReleaseReader(current_);
        }
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        auto metrics = std::make_shared<MetricsImpl>();
        metrics->Merge(finished_reader_metrics_);
        metrics->Merge(MetricsImpl::CollectReadMetrics(readers_));
        return metrics;
    }

 private:
    void CloseAndReleaseReader(size_t reader_index) {
        std::unique_ptr<KeyValueRecordReader>& reader = readers_[reader_index];
        if (!reader) {
            return;
        }
        reader->Close();
        finished_reader_metrics_->Merge(reader->GetReaderMetrics());
        reader.reset();
    }

    std::shared_ptr<Metrics> finished_reader_metrics_;
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers_;
    size_t current_{0};
};
}  // namespace paimon
