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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "paimon/reader/prefetch_file_batch_reader.h"

namespace paimon {

class PredicateFilter;

// For convenience, we abbreviate `Later Materializing` as `LatMat`.
class LateMaterializingFileBatchReader : public PrefetchFileBatchReader {
 public:
    static Result<std::unique_ptr<LateMaterializingFileBatchReader>> Create(
        std::unique_ptr<PrefetchFileBatchReader> inner);

    Result<FileBatchReader::ReadBatch> NextBatch() override;
    Result<FileBatchReader::ReadBatchWithBitmap> NextBatchWithBitmap() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;
    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;
    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;
    Result<uint64_t> GetNumberOfRows() const override;
    bool SupportPreciseBitmapSelection() const override;

    Status SeekToRow(uint64_t row_number) override;
    uint64_t GetNextRowToRead() const override;
    Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        bool* need_prefetch) const override;
    Status SetReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) override;

 private:
    explicit LateMaterializingFileBatchReader(std::unique_ptr<PrefetchFileBatchReader> inner)
        : inner_(std::move(inner)) {}

    enum LatMatState {
        kInit,
        kProbing,   // schema is set
        kNoLatMat,  // no need to late materialization
        kRunning,   // Lat-mat is enable an is reading data
        kEOF
    };
    /// Scan the probe projection once, evaluating the predicate batch by batch: the matched file
    /// row ids go into matched_bitmap_ and the matched probe values into probe_data_.
    Status ReadAndFilterProbeData();

    /// Rebind the predicate to probe_schema_'s field indices, so that it can be evaluated over
    /// the probe batches.
    Result<std::shared_ptr<PredicateFilter>> BindProbeFilter();

    /// Evaluate bound_filter over a single probe batch, adding the matched file row ids (that also
    /// pass selection_) to matched_bitmap_. Returns the batch-local offsets of the matched rows so
    /// the caller can compact the probe batch down to those rows.
    Result<RoaringBitmap32> FilterProbeBatch(const std::shared_ptr<arrow::Array>& array,
                                             const std::shared_ptr<PredicateFilter>& bound_filter);

    /// In kRunning state, read one payload batch, map its matched rows back to the cached probe
    /// rows by file row id, and reassemble the full read schema.
    Result<FileBatchReader::ReadBatchWithBitmap> ReadPayloadBatch();

    /// Combine the compacted payload columns and the selected probe columns into a single struct
    /// array following full_schema_'s field order.
    Result<FileBatchReader::ReadBatch> AssembleFullBatch(
        const std::shared_ptr<arrow::Array>& payload_array,
        const std::shared_ptr<arrow::Array>& probe_array);

    Status SetInnerProbeSchema();
    Status SetInnerPayloadSchema();
    Status SetInnerFullSchema();

    LatMatState state_ = kInit;
    std::unique_ptr<PrefetchFileBatchReader> inner_;
    std::shared_ptr<arrow::Schema> full_schema_;
    // projection holding only the predicate fields; nullptr when probing is not applicable
    std::shared_ptr<arrow::Schema> probe_schema_;
    // projection holding the payload (non-probe) fields; nullptr when probing is not applicable
    std::shared_ptr<arrow::Schema> payload_schema_;
    std::shared_ptr<Predicate> predicate_;
    std::optional<RoaringBitmap32> selection_;
    // matched probe rows only, concatenated in ascending file row order (aligned 1:1 with
    // matched_bitmap_)
    std::shared_ptr<arrow::StructArray> probe_data_;
    // file-level row ids that pass the predicate (and selection_); drives the payload read
    RoaringBitmap32 matched_bitmap_;
    // read cursor into probe_data_ for the payload phase
    int64_t probe_cursor_ = 0;
    // file row id of each row in the batch last emitted in kRunning state
    std::vector<uint64_t> row_mapping_;
};

}  // namespace paimon
