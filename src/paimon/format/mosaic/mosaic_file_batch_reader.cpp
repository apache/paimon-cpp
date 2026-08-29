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

#include "paimon/format/mosaic/mosaic_file_batch_reader.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"
#include "paimon/format/mosaic/mosaic_stats.h"
#include "paimon/fs/file_system.h"

namespace paimon::mosaic {

MosaicFileBatchReader::MosaicFileBatchReader(
    const std::shared_ptr<InputStream>& input, int32_t batch_size,
    std::unique_ptr<MosaicInputContext> input_context, MosaicReaderHandle* reader,
    const std::shared_ptr<arrow::Schema>& file_schema, uint32_t num_row_groups, uint64_t total_rows,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : input_(input),
      batch_size_(batch_size),
      input_context_(std::move(input_context)),
      reader_(reader),
      file_schema_(file_schema),
      num_row_groups_(num_row_groups),
      total_rows_(total_rows),
      pool_(pool),
      arrow_pool_(arrow_pool),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<MosaicFileBatchReader>> MosaicFileBatchReader::Create(
    const std::shared_ptr<InputStream>& input, int32_t batch_size,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (input == nullptr || pool == nullptr || batch_size <= 0) {
        return Status::Invalid(
            "Mosaic reader requires non-null input and memory pool, and positive batch size");
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t signed_length, input->Length());
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(signed_length, "Mosaic input length"));
    auto length = static_cast<uint64_t>(signed_length);
    auto input_context = std::make_unique<MosaicInputContext>(input, length);
    MosaicInputFile input_file = {};
    input_file.ctx = input_context.get();
    input_file.read_at_fn = MosaicInputContext::ReadAt;
    input_file.length_fn = MosaicInputContext::Length;
    std::unique_ptr<MosaicReaderHandle, decltype(&mosaic_reader_free)> reader(
        mosaic_reader_open(input_file), mosaic_reader_free);
    if (reader == nullptr) {
        return MosaicFfiError("open Mosaic reader", input_context->GetCallbackStatus());
    }

    ::ArrowSchema ffi_schema = {};
    if (mosaic_reader_export_schema(reader.get(), &ffi_schema) != 0) {
        return MosaicFfiError("read Mosaic schema", input_context->GetCallbackStatus());
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> file_schema,
                                      arrow::ImportSchema(&ffi_schema));

    uint32_t num_row_groups = 0;
    if (mosaic_reader_num_row_groups(reader.get(), &num_row_groups) != 0) {
        return MosaicFfiError("read Mosaic row group count", input_context->GetCallbackStatus());
    }
    uint64_t total_rows = 0;
    for (uint32_t row_group = 0; row_group < num_row_groups; ++row_group) {
        uint32_t row_count = 0;
        if (mosaic_reader_row_group_num_rows(reader.get(), row_group, &row_count) != 0) {
            return MosaicFfiError("read Mosaic row count", input_context->GetCallbackStatus());
        }
        total_rows += row_count;
    }
    return std::unique_ptr<MosaicFileBatchReader>(
        new MosaicFileBatchReader(input, batch_size, std::move(input_context), reader.release(),
                                  file_schema, num_row_groups, total_rows, pool, arrow_pool));
}

MosaicFileBatchReader::~MosaicFileBatchReader() {
    CloseInternal();
}

Result<std::shared_ptr<arrow::Array>> MosaicFileBatchReader::ReadNextRowGroup() {
    while (next_row_group_ < num_row_groups_) {
        uint32_t row_group = next_row_group_++;
        uint32_t row_count = 0;
        if (mosaic_reader_row_group_num_rows(reader_, row_group, &row_count) != 0) {
            return MosaicFfiError("read Mosaic row count", input_context_->GetCallbackStatus());
        }
        current_row_group_first_row_ = next_row_group_first_row_;
        next_row_group_first_row_ += row_count;
        PAIMON_ASSIGN_OR_RAISE(bool matches, MatchesRowGroup(row_group, row_count));
        if (!matches) {
            continue;
        }

        MosaicRowGroupReaderHandle* row_group_reader =
            mosaic_reader_open_row_group(reader_, row_group);
        if (row_group_reader == nullptr) {
            return MosaicFfiError("open Mosaic row group", input_context_->GetCallbackStatus());
        }
        MosaicRecordBatchHandle* record_batch =
            mosaic_row_group_reader_read_columns(row_group_reader);
        mosaic_row_group_reader_free(row_group_reader);
        if (record_batch == nullptr) {
            return MosaicFfiError("read Mosaic row group", input_context_->GetCallbackStatus());
        }
        ::ArrowArray ffi_array = {};
        ::ArrowSchema ffi_schema = {};
        int32_t export_result = mosaic_record_batch_export(record_batch, &ffi_array, &ffi_schema);
        mosaic_record_batch_free(record_batch);
        if (export_result != 0) {
            return MosaicFfiError("export Mosaic row group", input_context_->GetCallbackStatus());
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> batch,
                                          arrow::ImportArray(&ffi_array, &ffi_schema));
        if (batch->length() != row_count) {
            return Status::Invalid("Mosaic row group row count mismatch");
        }
        if (batch->length() != 0) {
            return batch;
        }
    }
    return std::shared_ptr<arrow::Array>();
}

Result<bool> MosaicFileBatchReader::MatchesRowGroup(uint32_t row_group, uint32_t row_count) {
    if (predicate_filter_ == nullptr) {
        return true;
    }
    PAIMON_ASSIGN_OR_RAISE(
        MosaicStatsUtils::RowGroupStatistics stats,
        MosaicStatsUtils::ReadRowGroupStatistics(row_group, input_context_.get(), reader_));
    // This matches the Java Mosaic reader: a file without row-group statistics is always kept.
    if (stats.empty()) {
        return true;
    }
    std::vector<MosaicStatsUtils::RowGroupStatistics> row_group_stats;
    row_group_stats.push_back(std::move(stats));
    PAIMON_ASSIGN_OR_RAISE(
        ColumnStatsVector column_stats,
        MosaicStatsUtils::ConvertColumnStatistics(file_schema_, row_group_stats,
                                                  /*missing_null_count_is_zero=*/false));
    PAIMON_ASSIGN_OR_RAISE(SimpleStats simple_stats,
                           SimpleStatsConverter::ToBinary(column_stats, pool_.get()));
    return predicate_filter_->Test(file_schema_, row_count, simple_stats.MinValues(),
                                   simple_stats.MaxValues(), simple_stats.NullCounts());
}

Result<BatchReader::ReadBatch> MosaicFileBatchReader::NextBatch() {
    if (closed_) {
        return Status::Invalid("Mosaic reader is closed");
    }
    if (current_batch_ == nullptr || current_batch_offset_ == current_batch_->length()) {
        PAIMON_ASSIGN_OR_RAISE(current_batch_, ReadNextRowGroup());
        current_batch_offset_ = 0;
    }
    if (current_batch_ == nullptr) {
        previous_batch_row_count_ = 0;
        return BatchReader::MakeEofBatch();
    }

    int64_t row_count =
        std::min<int64_t>(batch_size_, current_batch_->length() - current_batch_offset_);
    std::shared_ptr<arrow::Array> sliced_array =
        current_batch_->Slice(current_batch_offset_, row_count);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> normalized_array,
                           ArrowUtils::NormalizeArrayOffsets(sliced_array, arrow_pool_.get()));

    previous_first_row_ = current_row_group_first_row_ + current_batch_offset_;
    previous_batch_row_count_ = row_count;
    current_batch_offset_ += row_count;
    auto ffi_array = std::make_unique<::ArrowArray>();
    auto ffi_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*normalized_array, ffi_array.get(), ffi_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(ffi_array.get(), arrow_pool_));
    return std::make_pair(std::move(ffi_array), std::move(ffi_schema));
}

Result<std::unique_ptr<::ArrowSchema>> MosaicFileBatchReader::GetFileSchema() const {
    auto schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*file_schema_, schema.get()));
    return schema;
}

Status MosaicFileBatchReader::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (read_schema == nullptr) {
        return Status::Invalid("Mosaic read schema is nullptr");
    }
    (void)selection_bitmap;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(read_schema));
    std::vector<std::string> names;
    std::vector<const char*> name_pointers;
    names.reserve(schema->num_fields());
    name_pointers.reserve(schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& field : schema->fields()) {
        names.push_back(field->name());
    }
    for (const std::string& name : names) {
        name_pointers.push_back(name.c_str());
    }
    if (mosaic_reader_set_projection(reader_, name_pointers.data(), name_pointers.size()) != 0) {
        return MosaicFfiError("set Mosaic projection", input_context_->GetCallbackStatus());
    }
    predicate_filter_ = std::dynamic_pointer_cast<PredicateFilter>(predicate);
    next_row_group_ = 0;
    next_row_group_first_row_ = 0;
    current_row_group_first_row_ = 0;
    current_batch_.reset();
    current_batch_offset_ = 0;
    previous_first_row_ = std::numeric_limits<uint64_t>::max();
    previous_batch_row_count_ = 0;
    return Status::OK();
}

Result<uint64_t> MosaicFileBatchReader::GetPreviousBatchFileRowId(uint64_t batch_row_id) const {
    if (previous_batch_row_count_ == 0) {
        return Status::Invalid(previous_first_row_ == std::numeric_limits<uint64_t>::max()
                                   ? "no Mosaic batch has been read yet"
                                   : "last Mosaic batch was EOF");
    }
    if (batch_row_id >= previous_batch_row_count_) {
        return Status::Invalid(fmt::format("batch row id {} is out of range {}", batch_row_id,
                                           previous_batch_row_count_));
    }
    return previous_first_row_ + batch_row_id;
}

Result<uint64_t> MosaicFileBatchReader::GetNumberOfRows() const {
    return total_rows_;
}

std::shared_ptr<Metrics> MosaicFileBatchReader::GetReaderMetrics() const {
    return metrics_;
}

void MosaicFileBatchReader::Close() {
    CloseInternal();
}

void MosaicFileBatchReader::CloseInternal() {
    if (!closed_) {
        if (reader_ != nullptr) {
            mosaic_reader_free(reader_);
            reader_ = nullptr;
        }
        if (input_ != nullptr) {
            (void)input_->Close();
        }
        input_context_.reset();
        input_.reset();
        current_batch_.reset();
        closed_ = true;
    }
}

}  // namespace paimon::mosaic
