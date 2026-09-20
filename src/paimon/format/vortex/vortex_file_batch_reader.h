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

#include "arrow/type_fwd.h"
#include "paimon/format/vortex/vortex_ffi.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/format/vortex/vortex_io_callbacks.h"
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
class MemoryPool;
class Metrics;
class PredicateFilter;
}  // namespace paimon

namespace paimon::vortex {

/// Reads a Vortex file via the vortex-ffi scan API.
///
/// IO goes through `vx_data_source_new_callback`, the callback-based data source this repository
/// adds to vortex-ffi: Vortex issues positional reads back into `input_context_`, which forwards
/// them to the paimon `InputStream`. Nothing is staged in memory, range reads stay lazy, and any
/// paimon `FileSystem` works. Each batch the scan produces is converted through the Arrow C Data
/// Interface.
class VortexFileBatchReader : public FileBatchReader {
 public:
    static Result<std::unique_ptr<VortexFileBatchReader>> Create(
        const std::shared_ptr<InputStream>& input, int32_t batch_size,
        const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    ~VortexFileBatchReader() override;

    Result<ReadBatch> NextBatch() override;
    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;
    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;
    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override;
    Result<uint64_t> GetNumberOfRows() const override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;
    bool SupportPreciseBitmapSelection() const override {
        return false;
    }

 private:
    VortexFileBatchReader(const std::shared_ptr<InputStream>& input, int32_t batch_size,
                          std::shared_ptr<VortexInputContext> input_context, VxSessionPtr session,
                          VxDataSourcePtr data_source, VxScanPtr scan,
                          const std::shared_ptr<arrow::Schema>& file_schema,
                          const std::shared_ptr<arrow::DataType>& struct_type, uint64_t total_rows,
                          const std::shared_ptr<MemoryPool>& pool,
                          const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    /// Pull the next Arrow array from the current partition stream, opening the next partition's
    /// stream when the current one is exhausted. Returns nullptr at end of scan.
    Result<std::shared_ptr<arrow::Array>> ReadNextArray();
    /// Open the next partition and fill `current_stream_` via `vx_partition_scan_arrow`.
    /// Returns false when there are no more partitions.
    Result<bool> OpenNextPartitionStream();
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
    // The file schema with Vortex's string/binary view types normalized to the standard types; this
    // is what GetFileSchema() reports to paimon.
    std::shared_ptr<arrow::Schema> file_schema_;
    // The RAW view-typed struct that Vortex's Arrow export produces. NextBatch imports each scanned
    // batch as this type (so it must match the C export's buffer layout), then NormalizeViewArray
    // rewrites it to file_schema_'s types. Keeping this view-typed is load-bearing: normalizing it
    // would make ImportArray misread the buffers.
    std::shared_ptr<arrow::DataType> struct_type_;
    // The schema NextBatch must output, set by SetReadSchema (the projected read schema). Null
    // until SetReadSchema is called, in which case the full file schema is returned.
    std::shared_ptr<arrow::Schema> read_schema_;
    uint64_t total_rows_ = 0;

    ::ArrowArrayStream current_stream_{};
    bool stream_active_ = false;
    std::shared_ptr<arrow::Array> current_batch_;
    int64_t current_batch_offset_ = 0;

    uint64_t rows_emitted_ = 0;
    uint64_t previous_first_row_ = std::numeric_limits<uint64_t>::max();
    uint64_t previous_batch_row_count_ = 0;

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<PredicateFilter> predicate_filter_;  // from SetReadSchema; not pushed down
    std::shared_ptr<Metrics> metrics_;
    bool closed_ = false;
};

}  // namespace paimon::vortex
