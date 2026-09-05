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

#include <arrow/array/array_nested.h>
#include <arrow/c/abi.h>

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/reader/prefetch_file_batch_reader.h"

namespace paimon {

class PredicateFilter;

/// C++-only late materialization metrics. Java Paimon has no corresponding metrics.
class LateMaterializingMetrics {
 public:
    // Probe phase histograms (unit: microseconds)
    static constexpr char PROBE_TOTAL_US[] = "lat-mat.probe.total-us";
    static constexpr char PROBE_IO_US[] = "lat-mat.probe.io-us";
    static constexpr char PROBE_PREDICATE_US[] = "lat-mat.probe.predicate-us";
    static constexpr char PROBE_COMPACT_US[] = "lat-mat.probe.compact-us";
    // Payload phase histograms (unit: microseconds)
    static constexpr char PAYLOAD_TOTAL_US[] = "lat-mat.payload.total-us";
    // Time spent inside the inner reader, i.e. the part an internal payload prefetch could hide.
    static constexpr char PAYLOAD_IO_US[] = "lat-mat.payload.io-us";
    // Time spent intersecting the inner batch bitmap with matched_bitmap_ row by row.
    static constexpr char PAYLOAD_BITMAP_US[] = "lat-mat.payload.bitmap-us";
    // Time spent importing, compacting, normalizing and assembling the output batch.
    static constexpr char PAYLOAD_ASSEMBLE_US[] = "lat-mat.payload.assemble-us";
    // Auxiliary counters
    static constexpr char PROBE_ROWS_SCANNED[] = "lat-mat.probe.rows-scanned";
    static constexpr char PROBE_ROWS_MATCHED[] = "lat-mat.probe.rows-matched";
};

// For convenience, we abbreviate `Later Materializing` as `LatMat`.
// This reader is installed below the prefetch layer (see
// AbstractSplitRead::CreateFileBatchReader) and performs probe/payload two-phase reads when a
// predicate is pushed down through SetReadSchema; without a predicate it is a plain passthrough.
class LateMaterializingFileBatchReader : public PrefetchFileBatchReader {
 public:
    static Result<std::unique_ptr<LateMaterializingFileBatchReader>> Create(
        std::unique_ptr<FileBatchReader> inner,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Result<FileBatchReader::ReadBatch> NextBatch() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        auto inner_metrics = inner_->GetReaderMetrics();
        if (inner_metrics) {
            own_metrics_->Merge(inner_metrics);
        }
        return own_metrics_;
    };

    void Close() override {
        Reset();
        inner_->Close();
    }

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
        return inner_->GetFileSchema();
    }

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;

    Result<uint64_t> GetNumberOfRows() const override {
        return inner_->GetNumberOfRows();
    }

    bool SupportPreciseBitmapSelection() const override {
        // When probe_schema_ or payload_schema_ is null, lat-mat does not take effect.
        // Here we simply pass through the inner reader's support.
        return inner_->SupportPreciseBitmapSelection();
    }

    Status SeekToRow(uint64_t row_number) override;

    Result<uint64_t> GetNextRowToRead() const override {
        PAIMON_ASSIGN_OR_RAISE(PrefetchFileBatchReader * prefetch_reader,
                               GetPrefetchReaderOrRaise("GetNextRowToRead"));
        return prefetch_reader->GetNextRowToRead();
    }

    Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        bool* need_prefetch) const override {
        PAIMON_ASSIGN_OR_RAISE(PrefetchFileBatchReader * prefetch_reader,
                               GetPrefetchReaderOrRaise("GenReadRanges"));
        return prefetch_reader->GenReadRanges(need_prefetch);
    }

    Status SetReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) override;

    Result<std::vector<std::pair<uint64_t, uint64_t>>> PreBufferRange() override {
        // TODO(zhouhongfeng.zhf): PrebufferRange (called by PrefetchFileBatchReader) only read the
        // probe data, consider read the payload data as well.
        if (prefetch_inner_ == nullptr) {
            return std::vector<std::pair<uint64_t, uint64_t>>{};
        }
        return prefetch_inner_->PreBufferRange();
    }

 private:
    LateMaterializingFileBatchReader(std::unique_ptr<FileBatchReader> inner,
                                     PrefetchFileBatchReader* prefetch_inner,
                                     std::shared_ptr<arrow::MemoryPool> arrow_pool)
        : inner_(std::move(inner)),
          prefetch_inner_(prefetch_inner),
          arrow_pool_(std::move(arrow_pool)),
          own_metrics_(std::make_shared<MetricsImpl>()) {}

    /// Reset the state of the late materializing reader, does not close inner reader.
    void Reset();

    enum LatMatState {
        kInit,
        kProbing,   // schema is set, probing is in progress
        kNoLatMat,  // no need to late materialization
        kRunning,   // Lat-mat is enabled and the payload reader is reading data
        kEOF
    };

    /// Read the probe projection once (whole file) and evaluating the predicate batch by batch.
    /// This function updates matched_bitmap_ and probe_data_.
    /// TODO(zhouhongfeng.zhf): Read the probe data batch by batch to save memory.
    Status ReadAndFilterProbeData();

    Result<RoaringBitmap32> FilterProbeBatch(const std::shared_ptr<arrow::Array>& array,
                                             const std::shared_ptr<PredicateFilter>& bound_filter);

    /// Read one payload batch with bitmap (matched rows only)
    Result<FileBatchReader::ReadBatch> ReadPayloadBatch();

    /// Combine the compacted payload columns and the selected probe columns into a single struct
    /// array following full_schema_'s field order.
    Result<FileBatchReader::ReadBatch> AssembleFullBatch(
        const std::shared_ptr<arrow::Array>& payload_array,
        const std::shared_ptr<arrow::Array>& probe_array);

    Status SetInnerReadSchema(const std::shared_ptr<arrow::Schema>& read_schema,
                              const std::shared_ptr<Predicate>& predicate,
                              const std::optional<RoaringBitmap32>& selection);

    /// Returns the inner reader's prefetch interface, or an error when the format reader does not
    /// implement it (avro and blob do not).
    Result<PrefetchFileBatchReader*> GetPrefetchReaderOrRaise(
        std::string_view function_name) const {
        if (prefetch_inner_ == nullptr) {
            return Status::NotImplemented(
                fmt::format("format reader is not a prefetch reader, function {} not supported",
                            function_name));
        }
        return prefetch_inner_;
    }

    /// The probe/payload logic needs nothing beyond FileBatchReader, so the inner reader is held as
    /// the base type: parquet and orc readers implement PrefetchFileBatchReader, while avro and
    /// blob readers only implement FileBatchReader. The prefetch-only methods are rejected for the
    /// latter; AbstractSplitRead::CreateFileBatchReader keeps those formats out of the prefetch
    /// layer so nothing calls them.
    std::unique_ptr<FileBatchReader> inner_;
    /// Non-owning view of inner_ when it implements the prefetch interface, nullptr otherwise.
    /// inner_ is never reassigned, so the cast is resolved once in Create().
    PrefetchFileBatchReader* prefetch_inner_ = nullptr;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    LatMatState state_ = kInit;
    std::shared_ptr<arrow::Schema> full_schema_;
    // projection holding only the predicate fields; nullptr when probing is not applicable
    std::shared_ptr<arrow::Schema> probe_schema_;
    // projection holding the payload (non-probe) fields; nullptr when probing is not applicable
    std::shared_ptr<arrow::Schema> payload_schema_;
    std::shared_ptr<Predicate> predicate_;
    // predicate bound to probe_schema_'s field indices; null when probing is not applicable
    std::shared_ptr<PredicateFilter> probe_filter_;
    std::optional<RoaringBitmap32> selection_;
    // the probe_data_ is sliced and compacted with the matched_bitmap_
    std::shared_ptr<arrow::StructArray> probe_data_;
    RoaringBitmap32 matched_bitmap_;
    // read cursor into probe_data_ for the payload phase
    int64_t probe_cursor_ = 0;
    // to support GetPreviousBatchFileRowId
    std::vector<uint64_t> row_mapping_;
    // own metrics for late materialization timing
    std::shared_ptr<MetricsImpl> own_metrics_;
};

}  // namespace paimon
