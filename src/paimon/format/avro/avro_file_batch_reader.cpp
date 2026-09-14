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

#include "paimon/format/avro/avro_file_batch_reader.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <utility>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/format/avro/avro_input_stream_impl.h"
#include "paimon/format/avro/avro_schema_converter.h"
#include "paimon/reader/batch_reader.h"

namespace paimon::avro {

AvroFileBatchReader::AvroFileBatchReader(const std::shared_ptr<InputStream>& input_stream,
                                         const std::shared_ptr<::arrow::DataType>& file_data_type,
                                         std::unique_ptr<::avro::DataFileReaderBase>&& reader,
                                         std::unique_ptr<arrow::ArrayBuilder>&& array_builder,
                                         const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                                         int32_t batch_size,
                                         const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      arrow_pool_(arrow_pool),
      input_stream_(input_stream),
      file_data_type_(file_data_type),
      reader_(std::move(reader)),
      array_builder_(std::move(array_builder)),
      batch_size_(batch_size),
      metrics_(std::make_shared<MetricsImpl>()) {}

AvroFileBatchReader::~AvroFileBatchReader() {
    DoClose();
}

void AvroFileBatchReader::DoClose() {
    if (!close_) {
        reader_->close();
        close_ = true;
    }
}

Result<std::unique_ptr<AvroFileBatchReader>> AvroFileBatchReader::Create(
    const std::shared_ptr<InputStream>& input_stream, int32_t batch_size,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (batch_size <= 0) {
        return Status::Invalid(
            fmt::format("invalid batch size {}, must be larger than 0", batch_size));
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::avro::DataFileReaderBase> reader,
                           CreateDataFileReader(input_stream, pool));
    const auto& avro_file_schema = reader->dataSchema();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<::arrow::DataType> file_data_type,
                           AvroSchemaConverter::AvroSchemaToArrowDataType(avro_file_schema));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> array_builder,
                                      arrow::MakeBuilder(file_data_type, arrow_pool.get()));
    return std::unique_ptr<AvroFileBatchReader>(
        new AvroFileBatchReader(input_stream, file_data_type, std::move(reader),
                                std::move(array_builder), arrow_pool, batch_size, pool));
}

Result<std::unique_ptr<::avro::DataFileReaderBase>> AvroFileBatchReader::CreateDataFileReader(
    const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_RETURN_NOT_OK(input_stream->Seek(0, SeekOrigin::FS_SEEK_SET));
    try {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::avro::InputStream> in,
                               AvroInputStreamImpl::Create(input_stream, BUFFER_SIZE, pool));
        auto reader = std::make_unique<::avro::DataFileReaderBase>(std::move(in));
        reader->init();
        return reader;
    } catch (const ::avro::Exception& e) {
        return Status::Invalid(fmt::format("build avro reader failed. {}", e.what()));
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("build avro reader failed. {}", e.what()));
    } catch (...) {
        return Status::Invalid("build avro reader failed. unknown error");
    }
}

Result<BatchReader::ReadBatch> AvroFileBatchReader::NextBatch() {
    previous_first_row_ = next_row_to_read_;
    previous_row_ids_.clear();
    previous_batch_row_count_ = 0;
    try {
        while (array_builder_->length() < batch_size_) {
            const std::optional<uint64_t> target =
                selection_ ? selection_->NextRow() : std::optional<uint64_t>(next_row_to_read_);
            if (!target) {
                break;
            }
            PAIMON_ASSIGN_OR_RAISE(bool available, AdvanceToRow(target.value()));
            if (!available) {
                break;
            }
            PAIMON_RETURN_NOT_OK(ReadCurrentRow(/*materialize=*/true));
            if (selection_) {
                previous_row_ids_.push_back(target.value());
                selection_->Advance();
            }
        }
        if (array_builder_->length() == 0) {
            previous_batch_row_count_ = 0;
            return BatchReader::MakeEofBatch();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          array_builder_->Finish());
#ifndef NDEBUG
        // Keep structural validation in debug builds without adding its recursive cost to reads.
        PAIMON_RETURN_NOT_OK_FROM_ARROW(array->Validate());
#endif
        std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
        PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), arrow_pool_));
        previous_batch_row_count_ = c_array->length;
        return make_pair(std::move(c_array), std::move(c_schema));
    } catch (const ::avro::Exception& e) {
        return Status::Invalid(fmt::format("avro reader next batch failed. {}", e.what()));
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format("avro reader next batch failed. {}", e.what()));
    } catch (...) {
        return Status::Invalid("avro reader next batch failed. unknown error");
    }
}

std::optional<uint64_t> AvroFileBatchReader::SelectionCursor::NextRow() const {
    if (next_ == end_) {
        return std::nullopt;
    }
    return static_cast<uint32_t>(*next_);
}

void AvroFileBatchReader::BlockIndex::Observe(uint64_t row, int64_t file_offset) {
    if (state_ != State::kBuilding ||
        (!blocks_.empty() && blocks_.back().file_offset == file_offset)) {
        return;
    }
    if (blocks_.size() == kMaxBlocks) {
        blocks_.clear();
        state_ = State::kDisabled;
        return;
    }
    blocks_.push_back({row, file_offset});
}

void AvroFileBatchReader::BlockIndex::Finish(uint64_t row_count) {
    if (state_ == State::kBuilding) {
        row_count_ = row_count;
        state_ = State::kReady;
    }
}

void AvroFileBatchReader::BlockIndex::Reset() {
    if (state_ != State::kReady) {
        blocks_.clear();
        state_ = State::kBuilding;
    }
}

std::optional<uint64_t> AvroFileBatchReader::BlockIndex::RowCount() const {
    return state_ == State::kReady ? std::optional<uint64_t>(row_count_) : std::nullopt;
}

std::optional<AvroFileBatchReader::BlockIndex::Position> AvroFileBatchReader::BlockIndex::Locate(
    uint64_t row) const {
    if (state_ != State::kReady || row >= row_count_ || blocks_.empty()) {
        return std::nullopt;
    }
    auto block = std::upper_bound(
        blocks_.begin(), blocks_.end(), row,
        [](uint64_t target, const Position& position) { return target < position.first_row; });
    return *(block - 1);
}

Result<bool> AvroFileBatchReader::AdvanceToRow(uint64_t row) {
    assert(row >= next_row_to_read_);
    const std::optional<uint64_t> row_count = block_index_.RowCount();
    if (row_count && row >= row_count.value()) {
        return false;
    }
    if (row > next_row_to_read_) {
        const auto block = block_index_.Locate(row);
        if (block && block->first_row > next_row_to_read_) {
            reader_->seek(block->file_offset);
            next_row_to_read_ = block->first_row;
        }
    }
    while (next_row_to_read_ < row) {
        if (!PrepareNextRow()) {
            return false;
        }
        PAIMON_RETURN_NOT_OK(ReadCurrentRow(/*materialize=*/false));
    }
    return PrepareNextRow();
}

bool AvroFileBatchReader::PrepareNextRow() {
    if (!reader_->hasMore()) {
        if (!selection_) {
            block_index_.Finish(next_row_to_read_);
        }
        return false;
    }
    // Preserve index construction during full reads; bitmap reads only consume an existing index.
    if (!selection_) {
        block_index_.Observe(next_row_to_read_, reader_->previousSync());
    }
    return true;
}

Status AvroFileBatchReader::ReadCurrentRow(bool materialize) {
    reader_->decr();
    if (materialize) {
        if (array_builder_->length() == 0) {
            PAIMON_RETURN_NOT_OK(
                AvroDirectDecoder::ReserveBuilderCapacity(batch_size_, array_builder_.get()));
        }
        PAIMON_RETURN_NOT_OK(AvroDirectDecoder::DecodeAvroToBuilder(
            reader_->dataSchema().root(), read_fields_projection_, &reader_->decoder(),
            array_builder_.get(), &decode_context_));
    } else {
        PAIMON_RETURN_NOT_OK(
            AvroDirectDecoder::SkipValue(reader_->dataSchema().root(), &reader_->decoder()));
    }
    ++next_row_to_read_;
    return Status::OK();
}

Status AvroFileBatchReader::SetReadSchema(::ArrowSchema* read_schema,
                                          const std::shared_ptr<Predicate>& predicate,
                                          const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!read_schema) {
        return Status::Invalid("SetReadSchema failed: read schema cannot be nullptr");
    }
    // TODO(menglingda.mld): support predicate
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_read_schema,
                                      arrow::ImportSchema(read_schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> file_schema,
                           ArrowUtils::DataTypeToSchema(file_data_type_));
    PAIMON_ASSIGN_OR_RAISE(
        bool has_nested_projection,
        NestedProjectionUtils::HasNestedSubfieldProjection(file_schema, arrow_read_schema));
    if (has_nested_projection) {
        return Status::Invalid(
            "SetReadSchema failed: avro reader does not support nested sub-field projection");
    }
    PAIMON_ASSIGN_OR_RAISE(read_fields_projection_,
                           CalculateReadFieldsProjection(file_schema, arrow_read_schema->fields()));
    std::shared_ptr<::arrow::DataType> read_data_type = arrow::struct_(arrow_read_schema->fields());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> array_builder,
                                      arrow::MakeBuilder(read_data_type, arrow_pool_.get()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::avro::DataFileReaderBase> reader,
                           CreateDataFileReader(input_stream_, pool_));

    if (reader_) {
        reader_->close();
    }
    reader_ = std::move(reader);
    array_builder_ = std::move(array_builder);
    decode_context_.ClearBuilderMetadata();
    selection_.reset();
    if (selection_bitmap) {
        selection_.emplace(selection_bitmap.value());
    }
    block_index_.Reset();
    previous_row_ids_.clear();
    previous_first_row_.reset();
    previous_batch_row_count_ = 0;
    next_row_to_read_ = 0;
    close_ = false;
    return Status::OK();
}

Result<std::set<size_t>> AvroFileBatchReader::CalculateReadFieldsProjection(
    const std::shared_ptr<::arrow::Schema>& file_schema, const arrow::FieldVector& read_fields) {
    std::set<size_t> projection_set;
    PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> projection,
                           ArrowUtils::CreateProjection(file_schema, read_fields));
    int32_t prev_index = -1;
    for (auto& index : projection) {
        if (index <= prev_index) {
            return Status::Invalid(
                "SetReadSchema failed: read schema fields order is different from file schema");
        }
        prev_index = index;
        projection_set.insert(index);
    }
    return projection_set;
}

Result<std::unique_ptr<::ArrowSchema>> AvroFileBatchReader::GetFileSchema() const {
    assert(reader_);
    auto c_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*file_data_type_, c_schema.get()));
    return c_schema;
}

Result<uint64_t> AvroFileBatchReader::GetNumberOfRows() const {
    if (const auto row_count = block_index_.RowCount()) {
        return row_count.value();
    }
    if (!total_rows_) {
        PAIMON_ASSIGN_OR_RAISE(int64_t current_pos, input_stream_->GetPos());
        ScopeGuard stream_guard([this, current_pos]() -> void {
            // reset input stream position to original position
            Status status = input_stream_->Seek(current_pos, SeekOrigin::FS_SEEK_SET);
            (void)status;
        });
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::avro::DataFileReaderBase> reader,
                               CreateDataFileReader(input_stream_, pool_));
        ScopeGuard reader_guard([&reader]() -> void { reader->close(); });
        try {
            while (reader->hasMore()) {
                reader->decr();
                total_rows_ = total_rows_.value_or(0) + 1;
            }
        } catch (const ::avro::Exception& e) {
            return Status::Invalid(fmt::format("avro reader GetNumberOfRows failed. {}", e.what()));
        } catch (const std::exception& e) {
            return Status::Invalid(fmt::format("avro reader GetNumberOfRows failed. {}", e.what()));
        } catch (...) {
            return Status::Invalid("avro reader GetNumberOfRows failed. unknown error");
        }
    }
    return *total_rows_;
}

}  // namespace paimon::avro
