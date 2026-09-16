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
#include <memory>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "avro/DataFile.hh"
#include "fmt/format.h"
#include "paimon/format/avro/avro_direct_decoder.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"

namespace paimon::avro {

class AvroFileBatchReader : public FileBatchReader {
 public:
    static Result<std::unique_ptr<AvroFileBatchReader>> Create(
        const std::shared_ptr<InputStream>& input_stream, int32_t batch_size,
        const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    ~AvroFileBatchReader() override;

    Result<BatchReader::ReadBatch> NextBatch() override;

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override;

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override;

    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override {
        if (previous_batch_row_count_ == 0) {
            if (!previous_first_row_) {
                return Status::Invalid("No batch has been read yet.");
            } else {
                return Status::Invalid("Last batch was EOF.");
            }
        }
        if (batch_row_id >= previous_batch_row_count_) {
            return Status::Invalid(
                fmt::format("batch_row_id {} is out of range, last batch row count is {}",
                            batch_row_id, previous_batch_row_count_));
        }
        return selection_ ? previous_row_ids_[batch_row_id]
                          : previous_first_row_.value() + batch_row_id;
    }

    Result<uint64_t> GetNumberOfRows() const override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        DoClose();
    }

    bool SupportPreciseBitmapSelection() const override {
        return true;
    }

 private:
    class SelectionCursor {
     public:
        explicit SelectionCursor(const RoaringBitmap32& bitmap)
            : bitmap_(bitmap), next_(bitmap_.Begin()), end_(bitmap_.End()) {}
        SelectionCursor(const SelectionCursor&) = delete;
        SelectionCursor& operator=(const SelectionCursor&) = delete;

        std::optional<uint64_t> NextRow() const;

        void Advance() {
            ++next_;
        }

     private:
        // Iterators must be destroyed before the bitmap they reference.
        RoaringBitmap32 bitmap_;
        RoaringBitmap32::Iterator next_;
        RoaringBitmap32::Iterator end_;
    };

    class BlockIndex {
     public:
        struct Position {
            uint64_t first_row;
            int64_t file_offset;
        };

        void Observe(uint64_t row, int64_t file_offset);
        void Finish(uint64_t row_count);
        void Reset();
        std::optional<Position> Locate(uint64_t row) const;
        std::optional<uint64_t> RowCount() const;

     private:
        enum class State { kBuilding, kReady, kDisabled };
        static constexpr size_t kMaxBlocks = 64 * 1024;
        State state_ = State::kBuilding;
        std::vector<Position> blocks_;
        uint64_t row_count_ = 0;
    };

    Result<bool> AdvanceToRow(uint64_t row);
    bool PrepareNextRow();
    Status ReadCurrentRow(bool materialize);

    void DoClose();

    static Result<std::unique_ptr<::avro::DataFileReaderBase>> CreateDataFileReader(
        const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool);

    static Result<std::set<size_t>> CalculateReadFieldsProjection(
        const std::shared_ptr<::arrow::Schema>& file_schema, const arrow::FieldVector& read_fields);

    AvroFileBatchReader(const std::shared_ptr<InputStream>& input_stream,
                        const std::shared_ptr<::arrow::DataType>& file_data_type,
                        std::unique_ptr<::avro::DataFileReaderBase>&& reader,
                        std::unique_ptr<arrow::ArrayBuilder>&& array_builder,
                        const std::shared_ptr<arrow::MemoryPool>& arrow_pool, int32_t batch_size,
                        const std::shared_ptr<MemoryPool>& pool);

    static constexpr size_t BUFFER_SIZE = 1024 * 1024;  // 1M

    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<InputStream> input_stream_;
    std::shared_ptr<::arrow::DataType> file_data_type_;
    std::unique_ptr<::avro::DataFileReaderBase> reader_;
    std::unique_ptr<arrow::ArrayBuilder> array_builder_;
    std::optional<std::set<size_t>> read_fields_projection_;
    std::optional<SelectionCursor> selection_;
    // File-level acceleration, independent of the current projection and bitmap.
    BlockIndex block_index_;
    std::vector<uint64_t> previous_row_ids_;
    std::optional<uint64_t> previous_first_row_;
    uint64_t next_row_to_read_ = 0;
    uint64_t previous_batch_row_count_ = 0;
    mutable std::optional<uint64_t> total_rows_ = std::nullopt;
    const int32_t batch_size_;
    bool close_ = false;
    std::shared_ptr<Metrics> metrics_;
    // Decode context for reusing scratch buffers
    AvroDirectDecoder::DecodeContext decode_context_;
};

}  // namespace paimon::avro
