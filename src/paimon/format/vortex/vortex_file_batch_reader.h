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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/format/vortex/vortex_defs.h"
#include "paimon/format/vortex/vortex_ffi.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/format/vortex/vortex_io_callbacks.h"
#include "paimon/format/vortex/vortex_predicate_converter.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class DataType;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace paimon {
class InputStream;
class Metrics;
class PredicateFilter;
}  // namespace paimon

namespace paimon::vortex {

/// Reads a Vortex file via the vortex-ffi scan API.
class VortexFileBatchReader : public FileBatchReader {
 public:
    static Result<std::unique_ptr<VortexFileBatchReader>> Create(
        const std::shared_ptr<InputStream>& input, int32_t batch_size,
        const VortexPredicateOptions& predicate_options,
        const VortexIoCoalesceOptions& coalesce_options,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    ~VortexFileBatchReader() override;

    Result<ReadBatch> NextBatch() override;
    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;
    /// Read fields must be unique and present in the file, including nested projections.
    /// Schema validation failures leave the active scan and row positions unchanged.
    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;
    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;
    Result<uint64_t> GetNumberOfRows() const override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;
    bool SupportPreciseBitmapSelection() const override {
        return true;
    }

 private:
    // Bitmap selection owns the smaller side and maps selected ranks to physical rows.
    class SelectionState {
     public:
        static Result<SelectionState> Create(const std::optional<RoaringBitmap32>& bitmap,
                                             uint64_t total_rows);

        bool IsEmpty() const {
            return has_selection_ && selected_row_count_ == 0;
        }
        uint64_t SelectedRowCount() const {
            return selected_row_count_;
        }
        uint64_t IndexCount() const {
            return indices_.size();
        }
        void ApplyToScan(vx_scan_options* options) const;
        Result<uint64_t> GetPhysicalRowId(uint64_t selected_rank) const;

     private:
        bool has_selection_ = false;
        bool excludes_ = false;
        uint64_t selected_row_count_ = 0;
        std::vector<uint64_t> indices_;
    };

    // Track output positions and retain native row ids when a pushed filter removes rows.
    class RowPositionState {
     public:
        void Reset() {
            *this = RowPositionState{};
        }
        void EnableFilterRowIds() {
            has_filter_ = true;
        }
        void SetEof() {
            previous_batch_row_count_ = 0;
        }
        Status CaptureRowIndices(const std::shared_ptr<arrow::Array>& array);
        void RecordBatch(uint64_t row_count);
        Result<uint64_t> GetPhysicalRowId(uint64_t batch_row_id,
                                          const SelectionState& selection) const;

     private:
        uint64_t rows_emitted_ = 0;
        uint64_t previous_first_row_ = std::numeric_limits<uint64_t>::max();
        uint64_t previous_batch_row_count_ = 0;
        bool has_filter_ = false;
        std::shared_ptr<arrow::Array> previous_batch_row_idx_;
    };

    VortexFileBatchReader(const std::shared_ptr<InputStream>& input, int32_t batch_size,
                          std::shared_ptr<VortexInputContext> input_context, VxSessionPtr session,
                          VxDataSourcePtr data_source, VxScanPtr scan,
                          const std::shared_ptr<arrow::Schema>& file_schema, uint64_t total_rows,
                          const VortexPredicateOptions& predicate_options,
                          const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    /// Pull the next Arrow array from the current partition stream, opening the next partition's
    /// stream when the current one is exhausted. Returns nullptr at end of scan.
    Result<std::shared_ptr<arrow::Array>> ReadNextArray();
    /// Create the single-use scan with the current projection, selection and predicate.
    Status CreateScan();
    /// Open the next partition and fill `current_stream_` with bounded Arrow batches.
    /// Returns false when there are no more partitions.
    Result<bool> OpenNextPartitionStream();
    Status ImportCurrentStreamSchema();
    /// Reshape a scanned batch to `read_schema_`, selecting columns by name and recursing into
    /// nested fields. Returns `array` unchanged when SetReadSchema has not been called.
    Result<std::shared_ptr<arrow::Array>> AlignToReadSchema(
        const std::shared_ptr<arrow::Array>& array) const;
    void ReleaseStream();
    void CloseInternal();

    // Teardown order matters, so these are declared to be destroyed in a safe reverse order and
    // also reset explicitly in CloseInternal(): scan_ borrows data_source_, data_source_ reads
    // through input_context_, and the Arrow stream must be released while session_ is alive.
    std::shared_ptr<InputStream> input_;
    int32_t batch_size_;
    std::shared_ptr<VortexInputContext> input_context_;
    VxSessionPtr session_;
    VxDataSourcePtr data_source_;
    VxScanPtr scan_;
    // Full file schema with standard Arrow types, as reported by GetFileSchema().
    std::shared_ptr<arrow::Schema> file_schema_;
    // The schema NextBatch must output, set by SetReadSchema (the projected read schema). Null
    // until SetReadSchema is called, in which case the full file schema is returned.
    std::shared_ptr<arrow::Schema> read_schema_;
    uint64_t total_rows_ = 0;

    ::ArrowArrayStream current_stream_{};
    // Actual scan output schema, including projection and any hidden physical row index.
    std::shared_ptr<arrow::Schema> current_stream_schema_;
    bool stream_active_ = false;
    std::shared_ptr<arrow::Array> current_batch_;
    int64_t current_batch_offset_ = 0;

    SelectionState selection_;
    RowPositionState row_position_;

    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    VortexPredicateOptions predicate_options_;
    std::shared_ptr<PredicateFilter> predicate_filter_;
    std::shared_ptr<Metrics> metrics_;
    bool closed_ = false;
};

}  // namespace paimon::vortex
