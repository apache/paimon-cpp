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

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/result.h"

namespace paimon {
class Metrics;

namespace concat_reader_internal {
/// How many files AHEAD of the one being consumed get their first read started; 0 warms every
/// remaining file. Read once from PAIMON_CONCAT_WARMUP_DEPTH so a sweep needs no rebuild. Raising
/// it past the shared read executor's thread count only queues the extra reads, and each warm file
/// holds a prefetch queue of its own, so this trades memory for overlap.
inline size_t WarmupLookahead() {
    static const size_t lookahead = [] {
        const char* raw = std::getenv("PAIMON_CONCAT_WARMUP_DEPTH");
        return raw == nullptr ? size_t{1} : static_cast<size_t>(std::strtoul(raw, nullptr, 10));
    }();
    return lookahead;
}
}  // namespace concat_reader_internal

/// This reader is to concatenate a list of KeyValueRecordReader and read them sequentially.
/// The input list is already sorted by key and sequence number, and the key intervals do not
/// overlap each other.
class ConcatKeyValueRecordReader : public KeyValueRecordReader {
 public:
    explicit ConcatKeyValueRecordReader(
        std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers)
        : readers_(std::move(readers)) {}

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override {
        while (current_ < readers_.size()) {
            // Lookahead: the next files' first reads are paid while this file is still being
            // consumed, instead of serially after its EOF. The files here are read strictly one
            // after another, and a filtered read of a keyed table yields about one batch per file,
            // so without this every file costs a full round trip nobody overlaps with.
            PAIMON_RETURN_NOT_OK(WarmupFrom(current_));
            auto& current_reader = readers_[current_];
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<KeyValueRecordReader::Iterator> iterator,
                                   current_reader->NextBatch());
            if (iterator != nullptr) {
                return iterator;
            }
            // current meets eof, move to next reader
            current_reader->Close();
            current_++;
        }
        // read finish
        return std::unique_ptr<KeyValueRecordReader::Iterator>();
    }

    void Close() override {
        for (; current_ < readers_.size(); current_++) {
            readers_[current_]->Close();
        }
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return MetricsImpl::CollectReadMetrics(readers_);
    }

    /// Forwards to the children a read would touch next, so a Concat nested in another one is
    /// warmed through instead of swallowing the call.
    Status Warmup() override {
        return WarmupFrom(current_);
    }

 private:
    /// Warms up the reader at \p idx and the configured number of files after it. Idempotent, so
    /// calling it on every batch costs one virtual call plus one pointer test per already-warm
    /// reader.
    Status WarmupFrom(size_t idx) {
        const size_t lookahead = concat_reader_internal::WarmupLookahead();
        const size_t end =
            lookahead == 0 ? readers_.size() : std::min(idx + 1 + lookahead, readers_.size());
        for (size_t i = idx; i < end; i++) {
            PAIMON_RETURN_NOT_OK(readers_[i]->Warmup());
        }
        return Status::OK();
    }

    // KeyValue rows may outlive the active child and still reference buffers allocated by it.
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers_;
    size_t current_{0};
};
}  // namespace paimon
