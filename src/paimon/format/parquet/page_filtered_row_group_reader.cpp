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

#include "paimon/format/parquet/page_filtered_row_group_reader.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <unordered_set>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/chunked_array.h"
#include "arrow/io/caching.h"
#include "arrow/io/interfaces.h"
#include "arrow/table.h"
#include "arrow/util/future.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/saturating_cast.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/schema.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"

namespace paimon::parquet {

namespace {

/// Cap each window's value-byte reservation using the file-bounded compressed chunk size.
/// The uncompressed footer size may be forged; builders can grow beyond this estimate as
/// values are decoded.
constexpr int64_t kMaxReservationDecompressionFactor = 8;

struct DataPageLayout {
    int64_t column_chunk_offset;
    int64_t first_data_page_offset;
};

int64_t GetColumnChunkOffset(const ::parquet::ColumnChunkMetaData& column_chunk) {
    int64_t column_chunk_offset = column_chunk.data_page_offset();
    if (column_chunk.has_dictionary_page() && column_chunk.dictionary_page_offset() > 0 &&
        column_chunk.dictionary_page_offset() < column_chunk_offset) {
        column_chunk_offset = column_chunk.dictionary_page_offset();
    }
    return column_chunk_offset;
}

std::optional<DataPageLayout> GetDataPageLayout(
    const ::parquet::ColumnChunkMetaData& column_chunk,
    const std::shared_ptr<::parquet::OffsetIndex>& offset_index, int64_t row_group_row_count) {
    const auto& page_locations = offset_index->page_locations();
    if (page_locations.empty() || row_group_row_count <= 0 ||
        page_locations.front().first_row_index != 0) {
        return std::nullopt;
    }

    const int64_t column_chunk_offset = GetColumnChunkOffset(column_chunk);
    const int64_t first_data_page_offset = page_locations.front().offset;
    const int64_t column_chunk_size = column_chunk.total_compressed_size();
    if (column_chunk_offset < 0 || column_chunk_size <= 0 ||
        column_chunk_offset > std::numeric_limits<int64_t>::max() - column_chunk_size ||
        first_data_page_offset < column_chunk_offset) {
        return std::nullopt;
    }
    const int64_t column_chunk_end = column_chunk_offset + column_chunk_size;

    int64_t previous_page_end = first_data_page_offset;
    int64_t previous_first_row = -1;
    for (const auto& page : page_locations) {
        if (page.offset < previous_page_end || page.compressed_page_size <= 0 ||
            page.offset > std::numeric_limits<int64_t>::max() - page.compressed_page_size ||
            page.offset + page.compressed_page_size > column_chunk_end ||
            page.first_row_index <= previous_first_row || page.first_row_index < 0 ||
            page.first_row_index >= row_group_row_count) {
            return std::nullopt;
        }
        previous_page_end = page.offset + page.compressed_page_size;
        previous_first_row = page.first_row_index;
    }

    return DataPageLayout{column_chunk_offset, first_data_page_offset};
}

/// Aligns a decoding window's Arrow chunks and normalizes array offsets.
class TableRecordBatchReader : public arrow::RecordBatchReader {
 public:
    TableRecordBatchReader(std::shared_ptr<arrow::Table> table, int64_t chunksize,
                           std::shared_ptr<arrow::MemoryPool> pool)
        : table_(std::move(table)), inner_(*table_), pool_(std::move(pool)) {
        inner_.set_chunksize(chunksize);
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        return table_->schema();
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        ARROW_RETURN_NOT_OK(inner_.ReadNext(out));
        if (!*out) {
            return arrow::Status::OK();
        }

        // Page filtering may produce columns with different chunk boundaries. TableBatchReader
        // aligns them by slicing columns, which may leave non-zero child offsets.
        Result<std::shared_ptr<arrow::RecordBatch>> normalized_result =
            ArrowUtils::NormalizeRecordBatchOffsets(*out, pool_.get());
        if (!normalized_result.ok()) {
            return ToArrowStatus(normalized_result.status());
        }
        *out = std::move(normalized_result).value();
        return arrow::Status::OK();
    }

 private:
    std::shared_ptr<arrow::Table> table_;
    arrow::TableBatchReader inner_;
    std::shared_ptr<arrow::MemoryPool> pool_;
};

/// A FileColumnIterator that installs a direct data page read plan on every PageReader
/// it produces. The base class handles row group iteration; this subclass only
/// decorates the PageReader returned by NextChunk().
class PageFilteringColumnIterator : public ::parquet::arrow::FileColumnIterator {
 public:
    PageFilteringColumnIterator(int column_index, ::parquet::ParquetFileReader* reader,
                                std::vector<int> row_groups, bool has_data_page_read_plan,
                                int64_t first_data_page_offset,
                                std::vector<::parquet::DataPageReadPlanEntry> data_pages)
        : FileColumnIterator(column_index, reader, std::move(row_groups)),
          has_data_page_read_plan_(has_data_page_read_plan),
          first_data_page_offset_(first_data_page_offset),
          data_pages_(std::move(data_pages)) {}

    std::unique_ptr<::parquet::PageReader> NextChunk() override {
        std::unique_ptr<::parquet::PageReader> page_reader = FileColumnIterator::NextChunk();
        if (page_reader && has_data_page_read_plan_) {
            page_reader->set_data_page_read_plan(first_data_page_offset_, data_pages_);
        }
        return page_reader;
    }

 private:
    bool has_data_page_read_plan_;
    int64_t first_data_page_offset_;
    std::vector<::parquet::DataPageReadPlanEntry> data_pages_;
};

}  // namespace

std::pair<int64_t, int64_t> PageFilteredRowGroupReader::GetPageRowRange(
    const std::vector<::parquet::PageLocation>& page_locations, int32_t page_idx,
    int64_t row_group_row_count) {
    int64_t first_row = page_locations[page_idx].first_row_index;
    int64_t last_row = (page_idx + 1 < static_cast<int32_t>(page_locations.size()))
                           ? page_locations[page_idx + 1].first_row_index - 1
                           : row_group_row_count - 1;
    return {first_row, last_row};
}

std::optional<PageFilteredRowGroupReader::DataPageReadPlan>
PageFilteredRowGroupReader::MakeDataPageReadPlan(
    const RowRanges& row_ranges, const std::shared_ptr<::parquet::OffsetIndex>& offset_index,
    const ::parquet::ColumnChunkMetaData& column_chunk, int64_t row_group_row_count) {
    std::optional<DataPageLayout> layout =
        GetDataPageLayout(column_chunk, offset_index, row_group_row_count);
    if (!layout) {
        return std::nullopt;
    }

    const auto& page_locations = offset_index->page_locations();
    auto num_pages = static_cast<int32_t>(page_locations.size());
    std::vector<::parquet::DataPageReadPlanEntry> data_pages;
    data_pages.reserve(page_locations.size());

    for (int32_t page_idx = 0; page_idx < num_pages; ++page_idx) {
        auto [first_row, last_row] = GetPageRowRange(page_locations, page_idx, row_group_row_count);
        if (row_ranges.IsOverlapping(first_row, last_row)) {
            const auto& page = page_locations[page_idx];
            data_pages.push_back(
                {page_idx, page.offset - layout->column_chunk_offset, page.compressed_page_size});
        }
    }

    return DataPageReadPlan{layout->first_data_page_offset - layout->column_chunk_offset,
                            std::move(data_pages)};
}

RowRanges PageFilteredRowGroupReader::ComputeCompressedRowRanges(
    const RowRanges& original_ranges, const std::shared_ptr<::parquet::OffsetIndex>& offset_index,
    int64_t row_group_row_count) {
    const auto& page_locations = offset_index->page_locations();
    auto num_pages = static_cast<int32_t>(page_locations.size());
    const auto& ranges = original_ranges.GetRanges();

    RowRanges compressed;
    int64_t compressed_offset = 0;

    for (int32_t page_idx = 0; page_idx < num_pages; ++page_idx) {
        auto [page_from, page_to] = GetPageRowRange(page_locations, page_idx, row_group_row_count);
        int64_t page_size = page_to - page_from + 1;

        if (!original_ranges.IsOverlapping(page_from, page_to)) {
            // Page will be skipped by the direct read plan, not in compressed space
            continue;
        }

        for (const auto& range : ranges) {
            if (range.to < page_from) continue;
            if (range.from > page_to) break;
            int64_t overlap_from = std::max(range.from, page_from);
            int64_t overlap_to = std::min(range.to, page_to);
            compressed.Add(RowRanges::Range(compressed_offset + (overlap_from - page_from),
                                            compressed_offset + (overlap_to - page_from)));
        }

        compressed_offset += page_size;
    }

    return compressed;
}

Status PageFilteredRowGroupReader::WaitForPreBuffer(
    int32_t row_group_index, const std::vector<int32_t>& column_indices,
    const ::arrow::io::CacheOptions& cache_options, bool pre_buffered,
    const std::vector<::arrow::io::ReadRange>& page_ranges,
    std::shared_ptr<::arrow::MemoryPool> pool, ::parquet::ParquetFileReader* parquet_reader) {
    std::vector<int> rg_vec = {row_group_index};
    std::vector<int> col_vec(column_indices.begin(), column_indices.end());
    if (!pre_buffered) {
        ::arrow::io::IOContext io_ctx(pool.get());
        parquet_reader->PreBuffer(rg_vec, col_vec, io_ctx, cache_options);
    }
    if (!page_ranges.empty()) {
        auto status = parquet_reader->WhenBufferedRanges(page_ranges).status();
        if (!status.ok()) {
            ::arrow::io::IOContext io_ctx(pool.get());
            parquet_reader->PreBuffer(rg_vec, col_vec, io_ctx, cache_options);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(parquet_reader->WhenBuffered(rg_vec, col_vec).status());
        }
    } else {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(parquet_reader->WhenBuffered(rg_vec, col_vec).status());
    }
    return Status::OK();
}

/// Retains the decoder and selected-row cursor of every leaf, including leaves whose page
/// plans use different compressed row coordinates. Only the current output window is decoded.
class PageFilteredRowGroupReader::FilteredRecordBatchReader : public arrow::RecordBatchReader {
 public:
    static Result<std::unique_ptr<arrow::RecordBatchReader>> Create(
        const RowRanges& row_ranges, int32_t row_group_index,
        const std::vector<int32_t>& column_indices, int64_t batch_size,
        const std::shared_ptr<::parquet::RowGroupPageIndexReader>& page_index_reader,
        const std::shared_ptr<arrow::MemoryPool>& pool,
        ::parquet::arrow::FileReader* arrow_file_reader) {
        const auto& manifest = arrow_file_reader->manifest();
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::vector<int> field_indices,
                                          manifest.GetFieldIndices(std::vector<int>(
                                              column_indices.begin(), column_indices.end())));
        const int64_t row_group_row_count =
            arrow_file_reader->parquet_reader()->metadata()->RowGroup(row_group_index)->num_rows();
        auto included_columns =
            std::make_shared<std::unordered_set<int>>(column_indices.begin(), column_indices.end());
        std::vector<FieldReader> readers;
        arrow::FieldVector fields;
        readers.reserve(field_indices.size());
        fields.reserve(field_indices.size());
        for (int32_t field_index : field_indices) {
            PAIMON_ASSIGN_OR_RAISE(
                FieldReader field_reader,
                CreateFieldReader(page_index_reader, row_group_index, field_index, included_columns,
                                  row_ranges, row_group_row_count, arrow_file_reader));
            fields.push_back(field_reader.field);
            readers.push_back(std::move(field_reader));
        }
        return std::unique_ptr<arrow::RecordBatchReader>(
            new FilteredRecordBatchReader(std::move(readers), arrow::schema(std::move(fields)),
                                          row_ranges.RowCount(), batch_size, pool));
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        return schema_;
    }

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override {
        *out = nullptr;
        if (!status_.ok()) {
            return status_;
        }
        Result<std::shared_ptr<arrow::RecordBatch>> result = ReadNextBatch();
        if (!result.ok()) {
            status_ = ToArrowStatus(result.status());
            return status_;
        }
        *out = std::move(result).value();
        return arrow::Status::OK();
    }

    arrow::Status Close() override {
        chunk_reader_.reset();
        readers_.clear();
        remaining_rows_ = 0;
        return arrow::Status::OK();
    }

 private:
    struct LeafReadState {
        int32_t column_index;
        RowRanges ranges;
        size_t range_index = 0;
        int64_t next_row = 0;
        double bytes_per_value = 0;
        int64_t max_reservation_bytes = 0;
    };

    struct FieldReader {
        std::shared_ptr<arrow::Field> field;
        std::unique_ptr<::parquet::arrow::ColumnReader> reader;
        std::vector<LeafReadState> leaves;
    };

    static Result<FieldReader> CreateFieldReader(
        const std::shared_ptr<::parquet::RowGroupPageIndexReader>& rg_page_index_reader,
        int32_t row_group_index, int32_t field_index,
        std::shared_ptr<std::unordered_set<int>> column_indices, const RowRanges& row_ranges,
        int64_t row_group_row_count, ::parquet::arrow::FileReader* arrow_file_reader);

    FilteredRecordBatchReader(std::vector<FieldReader>&& readers,
                              std::shared_ptr<arrow::Schema> schema, int64_t remaining_rows,
                              int64_t batch_size, std::shared_ptr<arrow::MemoryPool> pool)
        : readers_(std::move(readers)),
          schema_(std::move(schema)),
          remaining_rows_(remaining_rows),
          batch_size_(batch_size),
          pool_(std::move(pool)) {}

    static Status ReadLeaf(int64_t rows, LeafReadState* leaf,
                           ::parquet::arrow::ColumnReader* reader) {
        const int64_t value_bytes = std::min(
            SaturatingDoubleToInteger<int64_t>(leaf->bytes_per_value * static_cast<double>(rows)),
            leaf->max_reservation_bytes);
        // Reset output buffers without discarding the decoder's unread page levels.
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            reader->ResetLeaf(leaf->column_index, rows, rows, value_bytes));
        int64_t remaining = rows;
        const auto& ranges = leaf->ranges.GetRanges();
        while (remaining > 0) {
            if (leaf->range_index >= ranges.size()) {
                return Status::Invalid("PageFilteredRowGroupReader: selected leaf rows exhausted");
            }
            const auto& range = ranges[leaf->range_index];
            const int64_t skip = std::max<int64_t>(0, range.from - leaf->next_row);
            const int64_t skipped = reader->SkipRecords(leaf->column_index, skip);
            if (skipped != skip) {
                return Status::Invalid(
                    fmt::format("PageFilteredRowGroupReader: leaf {} expected to skip {} records "
                                "but skipped {}",
                                leaf->column_index, skip, skipped));
            }
            leaf->next_row += skipped;
            const int64_t to_read = std::min(remaining, range.to - leaf->next_row + 1);
            const int64_t read = reader->ReadRecords(leaf->column_index, to_read);
            if (read != to_read) {
                return Status::Invalid(fmt::format(
                    "PageFilteredRowGroupReader: leaf {} expected to read {} records but read {}",
                    leaf->column_index, to_read, read));
            }
            leaf->next_row += read;
            remaining -= read;
            if (leaf->next_row > range.to) {
                ++leaf->range_index;
            }
        }
        return Status::OK();
    }

    Result<std::shared_ptr<arrow::RecordBatch>> ReadNextBatch() {
        try {
            while (true) {
                if (chunk_reader_) {
                    std::shared_ptr<arrow::RecordBatch> batch;
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(chunk_reader_->ReadNext(&batch));
                    if (batch) {
                        return batch;
                    }
                    chunk_reader_.reset();
                }
                if (remaining_rows_ == 0) {
                    readers_.clear();
                    return std::shared_ptr<arrow::RecordBatch>();
                }
                const int64_t rows = std::min(remaining_rows_, batch_size_);
                arrow::ChunkedArrayVector columns;
                arrow::ArrayVector arrays;
                columns.reserve(readers_.size());
                arrays.reserve(readers_.size());
                bool single_chunk = true;
                for (auto& field : readers_) {
                    for (auto& leaf : field.leaves) {
                        PAIMON_RETURN_NOT_OK(ReadLeaf(rows, &leaf, field.reader.get()));
                    }
                    std::shared_ptr<arrow::ChunkedArray> column;
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(field.reader->BuildArray(rows, &column));
                    if (column->length() != rows) {
                        return Status::Invalid(fmt::format(
                            "PageFilteredRowGroupReader: field {} produced {} rows but expected {}",
                            field.field->name(), column->length(), rows));
                    }
                    single_chunk = single_chunk && column->num_chunks() == 1;
                    if (column->num_chunks() == 1) {
                        arrays.push_back(column->chunk(0));
                    }
                    columns.push_back(std::move(column));
                }
                remaining_rows_ -= rows;
                if (single_chunk) {
                    auto batch = arrow::RecordBatch::Make(schema_, rows, std::move(arrays));
                    return ArrowUtils::NormalizeRecordBatchOffsets(batch, pool_.get());
                }
                // Binary values can exceed Arrow's array limit within a single output window.
                auto table = arrow::Table::Make(schema_, std::move(columns), rows);
                chunk_reader_ =
                    std::make_unique<TableRecordBatchReader>(std::move(table), batch_size_, pool_);
            }
        }
        PAIMON_PARQUET_CATCH_AND_RETURN_STATUS("PageFilteredRowGroupReader::ReadNext")
    }

    std::vector<FieldReader> readers_;
    std::shared_ptr<arrow::Schema> schema_;
    int64_t remaining_rows_;
    int64_t batch_size_;
    std::shared_ptr<arrow::MemoryPool> pool_;
    std::unique_ptr<arrow::RecordBatchReader> chunk_reader_;
    arrow::Status status_;
};

Result<PageFilteredRowGroupReader::FilteredRecordBatchReader::FieldReader>
PageFilteredRowGroupReader::FilteredRecordBatchReader::CreateFieldReader(
    const std::shared_ptr<::parquet::RowGroupPageIndexReader>& rg_page_index_reader,
    int32_t row_group_index, int32_t field_index,
    std::shared_ptr<std::unordered_set<int>> column_indices, const RowRanges& row_ranges,
    int64_t row_group_row_count, ::parquet::arrow::FileReader* arrow_file_reader) {
    // Factory: set a direct data page read plan on every leaf (per-leaf OffsetIndex).
    // The plan lets Arrow jump over unselected page headers as well as page bodies.
    auto direct_read_columns = std::make_shared<std::unordered_set<int>>();
    auto factory = [row_group_index, rg_page_index_reader, row_ranges, row_group_row_count,
                    direct_read_columns](int col_idx, ::parquet::ParquetFileReader* reader)
        -> ::parquet::arrow::FileColumnIterator* {
        bool has_data_page_read_plan = false;
        int64_t first_data_page_offset = 0;
        std::vector<::parquet::DataPageReadPlanEntry> data_pages;
        if (rg_page_index_reader) {
            auto offset_index = rg_page_index_reader->GetOffsetIndex(col_idx);
            if (offset_index) {
                auto row_group_metadata = reader->metadata()->RowGroup(row_group_index);
                auto column_chunk = row_group_metadata->ColumnChunk(col_idx);
                std::optional<DataPageReadPlan> plan = MakeDataPageReadPlan(
                    row_ranges, offset_index, *column_chunk, row_group_row_count);
                if (plan) {
                    first_data_page_offset = plan->first_data_page_offset;
                    data_pages = std::move(plan->data_pages);
                    has_data_page_read_plan = true;
                    direct_read_columns->insert(col_idx);
                }
            }
        }
        // Empty selections need the projected type, but creating a PageReader would read the
        // column chunk. An iterator without row groups builds the reader tree without I/O.
        std::vector<int> row_groups;
        if (!row_ranges.IsEmpty()) {
            row_groups.push_back(row_group_index);
        }
        return new PageFilteringColumnIterator(col_idx, reader, std::move(row_groups),
                                               has_data_page_read_plan, first_data_page_offset,
                                               std::move(data_pages));
    };

    std::unique_ptr<::parquet::arrow::ColumnReader> column_reader;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow_file_reader->GetColumn(field_index, column_indices, factory, &column_reader));

    if (!column_reader) {
        return Status::Invalid(
            fmt::format("PageFilteredRowGroupReader: field {} has no matching leaf columns "
                        "(row_group={})",
                        field_index, row_group_index));
    }

    FieldReader field;
    const std::vector<int> leaf_indices = column_reader->LeafColumnIndices();
    field.leaves.reserve(leaf_indices.size());
    for (int32_t col_idx : leaf_indices) {
        LeafReadState leaf;
        leaf.column_index = col_idx;
        leaf.ranges = row_ranges;
        if (!row_ranges.IsEmpty()) {
            auto row_group_metadata =
                arrow_file_reader->parquet_reader()->metadata()->RowGroup(row_group_index);
            auto column_chunk = row_group_metadata->ColumnChunk(col_idx);
            if (direct_read_columns->count(col_idx)) {
                auto offset_index = rg_page_index_reader->GetOffsetIndex(col_idx);
                leaf.ranges =
                    ComputeCompressedRowRanges(row_ranges, offset_index, row_group_row_count);
            }
            const int64_t chunk_bytes = column_chunk->total_uncompressed_size();
            const int64_t compressed_bytes = column_chunk->total_compressed_size();
            const int64_t chunk_values = column_chunk->num_values();
            if (chunk_bytes > 0 && compressed_bytes > 0 && chunk_values > 0) {
                leaf.bytes_per_value =
                    static_cast<double>(chunk_bytes) / static_cast<double>(chunk_values);
                leaf.max_reservation_bytes = std::min(
                    chunk_bytes,
                    SaturatingDoubleToInteger<int64_t>(static_cast<double>(compressed_bytes) *
                                                       kMaxReservationDecompressionFactor));
            }
        }
        field.leaves.push_back(std::move(leaf));
    }
    // Arrow's projected field can differ from the manifest (e.g. a pruned struct or restored
    // dictionary). Assemble zero rows to obtain that type without decoding any data pages.
    for (const auto& leaf : field.leaves) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(column_reader->ResetLeaf(leaf.column_index, 0, 0, 0));
    }
    std::shared_ptr<arrow::ChunkedArray> empty;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(column_reader->BuildArray(0, &empty));
    field.field =
        arrow_file_reader->manifest().schema_fields[field_index].field->WithType(empty->type());
    field.reader = std::move(column_reader);
    return field;
}

Result<std::unique_ptr<arrow::RecordBatchReader>> PageFilteredRowGroupReader::ReadFilteredRowGroup(
    const TargetRowGroup& target_row_group, const std::vector<int32_t>& column_indices,
    const ::arrow::io::CacheOptions& cache_options, bool pre_buffered,
    const std::vector<::arrow::io::ReadRange>& page_ranges, int64_t max_chunksize,
    const std::shared_ptr<::parquet::RowGroupPageIndexReader>& row_group_page_index_reader,
    std::shared_ptr<::arrow::MemoryPool> pool, ::parquet::arrow::FileReader* arrow_file_reader) {
    if (max_chunksize <= 0) {
        return Status::Invalid("PageFilteredRowGroupReader: batch size must be positive");
    }
    const auto& row_ranges = target_row_group.GetRowRanges();
    const int32_t row_group_index = target_row_group.GetRowGroupIndex();
    if (!row_ranges.IsEmpty()) {
        PAIMON_RETURN_NOT_OK(WaitForPreBuffer(row_group_index, column_indices, cache_options,
                                              pre_buffered, page_ranges, pool,
                                              arrow_file_reader->parquet_reader()));
    }
    return FilteredRecordBatchReader::Create(row_ranges, row_group_index, column_indices,
                                             max_chunksize, row_group_page_index_reader, pool,
                                             arrow_file_reader);
}

std::vector<::arrow::io::ReadRange> PageFilteredRowGroupReader::ComputePageRanges(
    const TargetRowGroup& target_row_group, const std::vector<int32_t>& column_indices,
    const std::shared_ptr<::parquet::RowGroupPageIndexReader>& row_group_page_index_reader,
    ::parquet::ParquetFileReader* parquet_reader) {
    int32_t row_group_index = target_row_group.GetRowGroupIndex();
    const auto& row_ranges = target_row_group.GetRowRanges();

    std::vector<::arrow::io::ReadRange> ranges;
    if (row_ranges.IsEmpty()) {
        return ranges;
    }

    auto file_metadata = parquet_reader->metadata();
    auto rg_metadata = file_metadata->RowGroup(row_group_index);
    int64_t row_group_row_count = rg_metadata->num_rows();

    for (int32_t col_idx : column_indices) {
        auto col_chunk = rg_metadata->ColumnChunk(col_idx);
        const int64_t column_chunk_offset = GetColumnChunkOffset(*col_chunk);
        const int64_t column_chunk_compressed_size = col_chunk->total_compressed_size();

        // Try to get OffsetIndex for page-level ranges
        std::shared_ptr<::parquet::OffsetIndex> offset_index;
        if (row_group_page_index_reader) {
            offset_index = row_group_page_index_reader->GetOffsetIndex(col_idx);
        }

        if (!offset_index) {
            // No OffsetIndex: fall back to entire column chunk
            ranges.push_back({column_chunk_offset, column_chunk_compressed_size});
            continue;
        }

        std::optional<DataPageLayout> layout =
            GetDataPageLayout(*col_chunk, offset_index, row_group_row_count);
        if (!layout) {
            // Invalid or empty OffsetIndex: keep the original sequential reader path.
            ranges.push_back({column_chunk_offset, column_chunk_compressed_size});
            continue;
        }

        // The full OffsetIndex, rather than data_page_offset in the column metadata, is the
        // authoritative location of the first data page. Older parquet-mr files may omit the
        // dictionary offset or set it equal to data_page_offset even though a dictionary prefix
        // is present (PARQUET-1850/PARQUET-1977).
        if (layout->first_data_page_offset > layout->column_chunk_offset) {
            ranges.push_back({layout->column_chunk_offset,
                              layout->first_data_page_offset - layout->column_chunk_offset});
        }

        const auto& page_locations = offset_index->page_locations();
        auto num_pages = static_cast<int32_t>(page_locations.size());

        for (int32_t page_idx = 0; page_idx < num_pages; ++page_idx) {
            auto [first_row, last_row] =
                GetPageRowRange(page_locations, page_idx, row_group_row_count);

            if (!row_ranges.IsOverlapping(first_row, last_row)) {
                continue;
            }

            const auto& page = page_locations[page_idx];
            ranges.push_back({page.offset, page.compressed_page_size});
        }
    }

    return ranges;
}

}  // namespace paimon::parquet
