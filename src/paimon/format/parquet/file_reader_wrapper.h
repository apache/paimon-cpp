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

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/compute/api.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/io/caching.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/parquet/row_ranges.h"
#include "paimon/format/parquet/target_row_group.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "parquet/arrow/reader.h"
#include "parquet/page_index.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::parquet {

// The FileReaderWrapper is a decorator class designed to support seek functionality, as well as the
// methods GetPreviousBatchFirstRowNumber and GetNextRowToRead.
class FileReaderWrapper {
 public:
    ~FileReaderWrapper();

    static Result<std::unique_ptr<FileReaderWrapper>> Create(
        std::unique_ptr<::parquet::arrow::FileReader>&& reader, int64_t batch_size,
        std::shared_ptr<arrow::MemoryPool> pool);

    /// Seek to the specified row number.
    /// @param row_number The row to seek to (must be at a row group boundary).
    /// When the reader is not yet initialized (before the first Next()), the reader
    /// construction is deferred to PrepareForReading to avoid building a batch reader
    /// that would be immediately discarded.
    Status SeekToRow(uint64_t row_number);

    /// Read the next batch of rows.
    /// @return The next RecordBatch, or nullptr if end of data.
    Result<std::shared_ptr<arrow::RecordBatch>> Next();

    /// Get the first row number of the previously returned batch. After Next() reaches EOF,
    /// returns the next unread row number (the end of the readable range).
    Result<uint64_t> GetPreviousBatchFirstRowNumber() const {
        return previous_first_row_;
    }

    /// Get the row number that will be read next.
    uint64_t GetNextRowToRead() const {
        return next_row_to_read_;
    }

    /// Get the total number of rows in the file.
    uint64_t GetNumberOfRows() const {
        return num_rows_;
    }

    /// Get the number of row groups in the file.
    int32_t GetNumberOfRowGroups() const {
        return file_reader_->num_row_groups();
    }

    /// Get the underlying Parquet file reader.
    ::parquet::arrow::FileReader* GetFileReader() {
        return file_reader_.get();
    }

    /// Get the [start, end) ranges for all row groups.
    const std::vector<std::pair<uint64_t, uint64_t>>& GetAllRowGroupRanges() const {
        return all_row_group_ranges_;
    }

    /// Get the Arrow schema of the file.
    Result<std::shared_ptr<arrow::Schema>> GetSchema() const;

    /// Close the batch reader and release resources.
    Status Close();

    /// Get the [start, end) ranges for the specified row groups.
    /// @param row_group_indices The row group indices to get ranges for.
    Result<std::vector<std::pair<uint64_t, uint64_t>>> GetRowGroupRanges(
        const std::set<int32_t>& row_group_indices) const;

    /// Prepare for lazy reading of the specified row groups and columns.
    /// Actual reader initialization is deferred until the first Next() call.
    Status PrepareForReadingLazy(const std::vector<TargetRowGroup>& target_row_groups,
                                 const std::vector<int32_t>& column_indices);

    /// Prepare for immediate reading of the specified row groups and columns.
    /// Initializes the reader and starts pre-buffering I/O.
    ///
    /// Note: when the read schema has nested sub-field projection,
    /// page-level filtering is disabled temporarily due to known offset
    /// calculation issues for nested pages.
    Status PrepareForReading(const std::vector<TargetRowGroup>& target_row_groups,
                             const std::vector<int32_t>& column_indices);

    /// Apply read ranges to the current target_row_groups_, keeping only those
    /// whose row-group range is equal to one of the given read ranges.
    /// Resets reader state so that the next Next() call will re-initialize.
    Status ApplyReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges);

    /// Get the page index reader for the file.
    /// Returns nullptr if page index is not available.
    std::shared_ptr<::parquet::PageIndexReader> GetPageIndexReader();

    /// Calculate filtered row ranges for a row group based on predicate.
    /// @param row_group_index The row group index.
    /// @param predicate The predicate to evaluate.
    /// @param column_name_to_index Map from column name to column index.
    /// @return RowRanges that may contain matching rows.
    Result<RowRanges> CalculateFilteredRowRanges(
        int32_t row_group_index, const std::shared_ptr<Predicate>& predicate,
        const std::map<std::string, int32_t>& column_name_to_index);

    /// Get or create the page index reader for a row group.
    std::shared_ptr<::parquet::RowGroupPageIndexReader> GetRowGroupPageIndexReader(
        int32_t row_group_index);

    /// Compute the (offset, length) byte ranges required by the current target row groups
    /// and columns. Unlike the arrow-internal PreBuffer path, this covers row groups that
    /// are excluded by read-range dispatch as well, because the shared prefetch cache must
    /// serve data consumed by all sub-readers. Relies only on file metadata, so it is safe
    /// to call before the lazy reader initialization.
    Result<std::vector<std::pair<uint64_t, uint64_t>>> GetPreBufferRanges();

 private:
    FileReaderWrapper(std::unique_ptr<::parquet::arrow::FileReader>&& file_reader,
                      const std::vector<std::pair<uint64_t, uint64_t>>& all_row_group_ranges,
                      uint64_t num_rows, int64_t batch_size,
                      std::shared_ptr<::arrow::MemoryPool> pool);

    /// Wait for all pending PreBuffer operations to complete.
    void WaitForPendingPreBuffer();

    /// Advance current_row_group_idx_ to the next row group and update next_row_to_read_.
    void AdvanceToNextRowGroup();

    /// Read next batch from a page-filtered row group. Returns nullptr when the RG is exhausted.
    Result<std::shared_ptr<arrow::RecordBatch>> NextPageFiltered();

    /// Read next batch from the fully-matched batch_reader_. Returns nullptr when exhausted.
    Result<std::shared_ptr<arrow::RecordBatch>> NextFullyMatched();

    /// Collect all byte ranges that need pre-buffering (page-filtered + fully-matched),
    /// skipping row groups excluded by ApplyReadRanges and row groups before start_idx
    /// (already skipped by a deferred seek).
    std::vector<::arrow::io::ReadRange> CollectPreBufferRanges(
        const std::vector<int32_t>& column_indices, uint64_t start_idx);

    /// Core byte-range collection shared by CollectPreBufferRanges and GetPreBufferRanges.
    /// When skip_read_range_excluded is true, row groups excluded by ApplyReadRanges are
    /// skipped (arrow-internal PreBuffer for this reader); when false, they are included
    /// (shared prefetch cache covering all sub-readers). Ranges before start_idx are
    /// never collected.
    std::vector<::arrow::io::ReadRange> DoCollectPreBufferRanges(
        const std::vector<int32_t>& column_indices, bool skip_read_range_excluded,
        uint64_t start_idx);

    /// Dispatch a single PreBufferRanges call with merged ranges.
    void DispatchPreBuffer(std::vector<::arrow::io::ReadRange> ranges);

    std::unique_ptr<::parquet::arrow::FileReader> file_reader_;
    std::unique_ptr<arrow::RecordBatchReader> batch_reader_;

    std::vector<std::pair<uint64_t, uint64_t>> all_row_group_ranges_;
    std::vector<int32_t> target_column_indices_;

    std::shared_ptr<::arrow::MemoryPool> pool_;
    int64_t batch_size_;  // 0 means no limit

    const uint64_t num_rows_;
    uint64_t next_row_to_read_ = std::numeric_limits<uint64_t>::max();
    uint64_t previous_first_row_ = std::numeric_limits<uint64_t>::max();
    uint64_t current_row_group_idx_ = 0;
    bool reader_initialized_ = false;
    // Target index recorded by SeekToRow when the reader was still uninitialized;
    // consumed by PrepareForReading so the deferred initialization starts at the seeked
    // position instead of rebuilding readers twice.
    std::optional<uint64_t> pending_start_idx_;

    // Streaming reader for the currently-active page-filtered row group. Created lazily
    // on the first Next() call into a page-filtered RG, drained batch-by-batch, then reset
    // when ReadNext returns nullptr (end of that RG).
    std::unique_ptr<arrow::RecordBatchReader> current_page_filtered_reader_;
    int64_t filtered_global_offset_ = 0;      // Cumulative filtered-row offset within RG
    RowRanges current_filtered_row_ranges_;   // RowRanges for the active page-filtered RG
    uint64_t current_filtered_rg_start_ = 0;  // Absolute row-group start row number

    // Target row groups with row ranges for none page-level filtering and page-level filtering
    std::vector<TargetRowGroup> target_row_groups_;

    // Track pre-buffered ranges so we can wait on destruction
    std::vector<::arrow::io::ReadRange> prebuffered_ranges_;

    // Arrow caches the file-level PageIndexReader, but RowGroup() creates a new reader each time.
    // Keep one reader per row group so its page-index buffers are shared by all read stages.
    std::map<int32_t, std::shared_ptr<::parquet::RowGroupPageIndexReader>>
        row_group_page_index_readers_;
};

}  // namespace paimon::parquet
