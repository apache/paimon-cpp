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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/format/lance/lance_file_batch_reader.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/format/lance/lance_utils.h"
#include "paimon/fs/file_system.h"

namespace paimon::lance {

LanceFileBatchReader::LanceFileBatchReader(const std::shared_ptr<InputStream>& input,
                                           int32_t batch_size, uint32_t batch_readahead,
                                           PaimonLanceReader* reader,
                                           const std::shared_ptr<arrow::Schema>& file_schema,
                                           uint64_t total_rows,
                                           const std::shared_ptr<MemoryPool>& pool,
                                           const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : input_(input),
      batch_size_(batch_size),
      batch_readahead_(batch_readahead),
      reader_(reader),
      file_schema_(file_schema),
      total_rows_(total_rows),
      pool_(pool),
      arrow_pool_(arrow_pool),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<LanceFileBatchReader>> LanceFileBatchReader::Create(
    const std::shared_ptr<InputStream>& input, int32_t batch_size, uint32_t batch_readahead,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (input == nullptr || pool == nullptr || arrow_pool == nullptr || batch_size <= 0 ||
        batch_readahead == 0) {
        return Status::Invalid(
            "Lance reader requires non-null input and memory pools, positive "
            "batch size, and positive batch readahead");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string uri, input->GetUri());
    if (uri.empty()) {
        return Status::Invalid("Lance reader input URI is empty");
    }
    LanceStorageOptions storage_options = GetLanceStorageOptions(options, uri);
    std::vector<const char*> keys;
    std::vector<const char*> values;
    keys.reserve(storage_options.size());
    values.reserve(storage_options.size());
    for (const auto& [key, value] : storage_options) {
        keys.push_back(key.c_str());
        values.push_back(value.c_str());
    }
    PaimonLanceReader* reader = nullptr;
    if (paimon_lance_reader_open(uri.c_str(), keys.data(), values.data(), keys.size(), &reader) !=
        0) {
        return LanceFfiError("open Lance reader");
    }

    ::ArrowSchema ffi_schema = {};
    if (paimon_lance_reader_export_schema(reader, &ffi_schema) != 0) {
        paimon_lance_reader_free(reader);
        return LanceFfiError("read Lance schema");
    }
    arrow::Result<std::shared_ptr<arrow::Schema>> schema_result = arrow::ImportSchema(&ffi_schema);
    if (!schema_result.ok()) {
        paimon_lance_reader_free(reader);
        return ToPaimonStatus(schema_result.status());
    }
    uint64_t total_rows = 0;
    if (paimon_lance_reader_num_rows(reader, &total_rows) != 0) {
        paimon_lance_reader_free(reader);
        return LanceFfiError("read Lance row count");
    }
    return std::unique_ptr<LanceFileBatchReader>(new LanceFileBatchReader(
        input, batch_size, batch_readahead, reader, std::move(schema_result).MoveValueUnsafe(),
        total_rows, pool, arrow_pool));
}

LanceFileBatchReader::~LanceFileBatchReader() {
    CloseInternal();
}

Status LanceFileBatchReader::OpenStream() {
    if (batch_reader_ != nullptr || (has_selection_ && selection_row_ids_.empty())) {
        return Status::OK();
    }
    std::vector<const char*> projection_pointers;
    projection_pointers.reserve(projection_names_.size());
    for (const std::string& name : projection_names_) {
        projection_pointers.push_back(name.c_str());
    }
    if (paimon_lance_reader_open_stream(reader_, static_cast<uint32_t>(batch_size_),
                                        batch_readahead_, projection_pointers.data(),
                                        projection_pointers.size(), has_selection_,
                                        selection_starts_.data(), selection_ends_.data(),
                                        selection_starts_.size(), &batch_reader_) != 0) {
        return LanceFfiError("open Lance batch reader");
    }
    return Status::OK();
}

Result<BatchReader::ReadBatch> LanceFileBatchReader::AlignBatch(ReadBatch batch) const {
    if (read_schema_ == nullptr) {
        return batch;
    }
    auto& [ffi_array, ffi_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(ffi_array.get(), ffi_schema.get()));
    if (array->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("Lance reader returned a non-struct Arrow batch");
    }
    auto struct_array = checked_pointer_cast<arrow::StructArray>(array);
    arrow::ArrayVector fields;
    std::vector<std::string> names;
    fields.reserve(read_schema_->num_fields());
    names.reserve(read_schema_->num_fields());
    for (const std::shared_ptr<arrow::Field>& field : read_schema_->fields()) {
        std::shared_ptr<arrow::Array> child = struct_array->GetFieldByName(field->name());
        if (child == nullptr) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                child,
                arrow::MakeArrayOfNull(field->type(), struct_array->length(), arrow_pool_.get()));
        } else if (!child->type()->Equals(field->type()) &&
                   ArrowUtils::EqualsIgnoreNullable(child->type(), field->type())) {
            std::shared_ptr<arrow::ArrayData> data = child->data()->Copy();
            data->type = field->type();
            child = arrow::MakeArray(std::move(data));
        } else if (!child->type()->Equals(field->type())) {
            PAIMON_ASSIGN_OR_RAISE(child, NestedProjectionUtils::AlignArrayToReadType(
                                              child, field->type(), arrow_pool_.get()));
        }
        fields.push_back(std::move(child));
        names.push_back(field->name());
    }
    std::shared_ptr<arrow::Array> aligned;
    if (fields.empty()) {
        aligned = std::make_shared<arrow::StructArray>(arrow::struct_({}), array->length(), fields);
    } else {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(aligned, arrow::StructArray::Make(fields, names));
    }
    auto out_array = std::make_unique<::ArrowArray>();
    auto out_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*aligned, out_array.get(), out_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(out_array.get(), out_schema.get(), arrow_pool_));
    return std::make_pair(std::move(out_array), std::move(out_schema));
}

Result<BatchReader::ReadBatch> LanceFileBatchReader::NextBatch() {
    if (closed_) {
        return Status::Invalid("Lance reader is closed");
    }
    if (has_selection_ && selection_row_ids_.empty()) {
        previous_batch_row_count_ = 0;
        return BatchReader::MakeEofBatch();
    }
    PAIMON_RETURN_NOT_OK(OpenStream());
    auto ffi_array = std::make_unique<::ArrowArray>();
    auto ffi_schema = std::make_unique<::ArrowSchema>();
    bool eof = false;
    if (paimon_lance_batch_reader_next(batch_reader_, ffi_array.get(), ffi_schema.get(), &eof) !=
        0) {
        return LanceFfiError("read Lance batch");
    }
    if (eof) {
        previous_batch_row_count_ = 0;
        return BatchReader::MakeEofBatch();
    }
    previous_row_offset_ = next_row_offset_;
    previous_batch_row_count_ = static_cast<uint64_t>(ffi_array->length);
    next_row_offset_ += previous_batch_row_count_;
    if (has_selection_ && next_row_offset_ > selection_row_ids_.size()) {
        ArrowArrayRelease(ffi_array.get());
        ArrowSchemaRelease(ffi_schema.get());
        return Status::Invalid("Lance reader returned more selected rows than requested");
    }
    return AlignBatch(std::make_pair(std::move(ffi_array), std::move(ffi_schema)));
}

Result<std::unique_ptr<::ArrowSchema>> LanceFileBatchReader::GetFileSchema() const {
    auto schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*file_schema_, schema.get()));
    return schema;
}

Status LanceFileBatchReader::SetReadSchema(::ArrowSchema* read_schema,
                                           const std::shared_ptr<Predicate>& predicate,
                                           const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (closed_) {
        return Status::Invalid("Lance reader is closed");
    }
    if (read_schema == nullptr) {
        return Status::Invalid("Lance read schema is nullptr");
    }
    (void)predicate;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(read_schema_, arrow::ImportSchema(read_schema));

    projection_names_.clear();
    for (const std::shared_ptr<arrow::Field>& field : read_schema_->fields()) {
        if (file_schema_->GetFieldByName(field->name()) != nullptr) {
            projection_names_.push_back(field->name());
        }
    }
    if (projection_names_.empty() && file_schema_->num_fields() > 0) {
        projection_names_.push_back(file_schema_->field(0)->name());
    }

    has_selection_ = selection_bitmap.has_value();
    selection_row_ids_.clear();
    selection_starts_.clear();
    selection_ends_.clear();
    if (selection_bitmap) {
        for (auto iter = selection_bitmap->Begin(); iter != selection_bitmap->End(); ++iter) {
            uint64_t row_id = *iter;
            if (row_id >= total_rows_) {
                return Status::Invalid(
                    fmt::format("Lance selected row {} is out of range {}", row_id, total_rows_));
            }
            selection_row_ids_.push_back(row_id);
            if (selection_ends_.empty() || row_id != selection_ends_.back()) {
                selection_starts_.push_back(row_id);
                selection_ends_.push_back(row_id + 1);
            } else {
                ++selection_ends_.back();
            }
        }
    }

    if (batch_reader_ != nullptr) {
        paimon_lance_batch_reader_free(batch_reader_);
        batch_reader_ = nullptr;
    }
    next_row_offset_ = 0;
    previous_row_offset_ = std::numeric_limits<uint64_t>::max();
    previous_batch_row_count_ = 0;
    return OpenStream();
}

Result<uint64_t> LanceFileBatchReader::GetPreviousBatchFileRowId(uint64_t batch_row_id) const {
    if (previous_batch_row_count_ == 0) {
        return Status::Invalid(previous_row_offset_ == std::numeric_limits<uint64_t>::max()
                                   ? "no Lance batch has been read yet"
                                   : "last Lance batch was EOF");
    }
    if (batch_row_id >= previous_batch_row_count_) {
        return Status::Invalid(fmt::format("batch row id {} is out of range {}", batch_row_id,
                                           previous_batch_row_count_));
    }
    uint64_t offset = previous_row_offset_ + batch_row_id;
    return has_selection_ ? selection_row_ids_[offset] : offset;
}

Result<uint64_t> LanceFileBatchReader::GetNumberOfRows() const {
    return total_rows_;
}

std::shared_ptr<Metrics> LanceFileBatchReader::GetReaderMetrics() const {
    return metrics_;
}

void LanceFileBatchReader::Close() {
    CloseInternal();
}

void LanceFileBatchReader::CloseInternal() {
    if (closed_) {
        return;
    }
    paimon_lance_batch_reader_free(batch_reader_);
    paimon_lance_reader_free(reader_);
    batch_reader_ = nullptr;
    reader_ = nullptr;
    if (input_ != nullptr) {
        (void)input_->Close();
        input_.reset();
    }
    closed_ = true;
}

}  // namespace paimon::lance
