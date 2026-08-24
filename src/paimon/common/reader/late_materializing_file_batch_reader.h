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
// TODO(zhouhongfeng.zhf): add this reader to the split read path.
class LateMaterializingFileBatchReader : public PrefetchFileBatchReader {
 public:
    static Result<std::unique_ptr<LateMaterializingFileBatchReader>> Create(
        std::unique_ptr<PrefetchFileBatchReader> inner, std::shared_ptr<MemoryPool> pool);

    Result<FileBatchReader::ReadBatch> NextBatch() override;

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
    explicit LateMaterializingFileBatchReader(std::unique_ptr<PrefetchFileBatchReader> inner,
                                              std::shared_ptr<arrow::MemoryPool> arrow_pool)
        : inner_(std::move(inner)), arrow_pool_(std::move(arrow_pool)) {}

    enum LatMatState {
        kInit,
        kProbing,   // schema is set
        kNoLatMat,  // no need to late materialization
        kRunning,   // Lat-mat is enable an is reading data
        kEOF
    };

    /// Read the probe projection once (whole file) and evaluating the predicate batch by batch.
    /// This function updates matched_bitmap_ and probe_data_.
    /// TODO(zhouhongfeng.zhf): Read the probe data batch by batch to save memory.
    Status ReadAndFilterProbeData();

    Result<std::shared_ptr<PredicateFilter>> BindProbeFilter();

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

    // Arrow pool for this reader's own allocations (probe/payload compaction). Falls back to the
    // arrow default pool when no pool was provided.
    arrow::MemoryPool* ArrowPool() const;

    std::unique_ptr<PrefetchFileBatchReader> inner_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    LatMatState state_ = kInit;
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges_;
    std::shared_ptr<arrow::Schema> full_schema_;
    // projection holding only the predicate fields; nullptr when probing is not applicable
    std::shared_ptr<arrow::Schema> probe_schema_;
    // projection holding the payload (non-probe) fields; nullptr when probing is not applicable
    std::shared_ptr<arrow::Schema> payload_schema_;
    std::shared_ptr<Predicate> predicate_;
    std::optional<RoaringBitmap32> selection_;
    // the probe_data_ is sliced and compacted with the matched_bitmap_
    std::shared_ptr<arrow::StructArray> probe_data_;
    RoaringBitmap32 matched_bitmap_;
    // read cursor into probe_data_ for the payload phase
    int64_t probe_cursor_ = 0;
    // to support GetPreviousBatchFileRowId
    std::vector<uint64_t> row_mapping_;
};

}  // namespace paimon
