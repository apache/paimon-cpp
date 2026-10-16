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

#include "paimon/core/io/rolling_vector_file_writer.h"

#include "arrow/array/array_nested.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"

namespace paimon {

RollingVectorFileWriter::RollingVectorFileWriter(
    int64_t target_file_size, int64_t target_file_row_num, int64_t vector_target_file_size,
    const std::shared_ptr<WriterFactory>& main_factory,
    const std::shared_ptr<WriterFactory>& vector_factory,
    const std::shared_ptr<arrow::Schema>& write_schema, int32_t vector_index)
    : RollingFileWriter(target_file_size, target_file_row_num, main_factory),
      vector_target_file_size_(vector_target_file_size),
      vector_factory_(vector_factory),
      write_schema_(write_schema),
      vector_index_(vector_index) {}

Status RollingVectorFileWriter::Write(::ArrowArray* record) {
    if (closed_ || aborted_) {
        return Status::Invalid("Writer has already closed or aborted!");
    }
    ScopeGuard guard([this]() { Abort(); });
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> array,
        arrow::ImportArray(record, arrow::struct_(write_schema_->fields())));
    if (!array || array->type_id() != arrow::Type::STRUCT || array->null_count() != 0) {
        return Status::Invalid("Dedicated VECTOR storage expects non-null struct rows");
    }
    if (array->length() == 0) {
        guard.Release();
        return Status::OK();
    }
    if (!current_writer_) {
        PAIMON_RETURN_NOT_OK(OpenCurrentWriter());
        PAIMON_ASSIGN_OR_RAISE(vector_writer_, vector_factory_->CreateWriter());
    }
    auto rows = checked_pointer_cast<arrow::StructArray>(array);
    arrow::ArrayVector main_arrays;
    arrow::FieldVector main_fields;
    for (int32_t i = 0; i < write_schema_->num_fields(); ++i) {
        if (i != vector_index_) {
            main_arrays.push_back(rows->field(i));
            main_fields.push_back(write_schema_->field(i));
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> main_array,
                                      arrow::StructArray::Make(main_arrays, main_fields));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> vector_array,
        arrow::StructArray::Make({rows->field(vector_index_)},
                                 {write_schema_->field(vector_index_)}));
    ::ArrowArray main_batch = {};
    ::ArrowArray vector_batch = {};
    ScopeGuard arrays_guard([&]() {
        ArrowArrayRelease(&main_batch);
        ArrowArrayRelease(&vector_batch);
    });
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*main_array, &main_batch));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*vector_array, &vector_batch));
    PAIMON_RETURN_NOT_OK(current_writer_->Write(&main_batch));
    PAIMON_RETURN_NOT_OK(vector_writer_->Write(&vector_batch));
    record_count_ += rows->length();
    current_file_record_count_ += rows->length();

    const bool suggested_check = SuggestCheck();
    PAIMON_ASSIGN_OR_RAISE(bool main_full,
                           current_writer_->ReachTargetSize(suggested_check, target_file_size_));
    PAIMON_ASSIGN_OR_RAISE(bool vector_full, vector_writer_->ReachTargetSize(
                                                 suggested_check, vector_target_file_size_));
    if (main_full || vector_full || current_file_record_count_ >= target_file_row_num_) {
        PAIMON_RETURN_NOT_OK(CloseCurrentPair());
    }
    guard.Release();
    return Status::OK();
}

Status RollingVectorFileWriter::CloseCurrentPair() {
    if (!current_writer_) {
        return Status::OK();
    }
    PAIMON_RETURN_NOT_OK(current_writer_->Close());
    PAIMON_ASSIGN_OR_RAISE(Writer::AbortExecutor main_abort, current_writer_->GetAbortExecutor());
    closed_writers_.push_back(main_abort);
    PAIMON_RETURN_NOT_OK(vector_writer_->Close());
    PAIMON_ASSIGN_OR_RAISE(Writer::AbortExecutor vector_abort, vector_writer_->GetAbortExecutor());
    closed_writers_.push_back(vector_abort);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> main_meta, current_writer_->GetResult());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> vector_meta, vector_writer_->GetResult());
    if (main_meta->row_count != vector_meta->row_count) {
        return Status::Invalid("Main and VECTOR files must contain the same number of rows");
    }
    // Row tracking assigns ids in manifest order: the normal anchor must precede its vector file.
    results_.push_back(main_meta);
    results_.push_back(vector_meta);
    metrics_->Merge(current_writer_->GetMetrics());
    metrics_->Merge(vector_writer_->GetMetrics());
    current_writer_.reset();
    vector_writer_.reset();
    current_file_record_count_ = 0;
    return Status::OK();
}

Status RollingVectorFileWriter::Close() {
    if (aborted_) {
        return Status::Invalid("Writer has been aborted!");
    }
    if (closed_) {
        return Status::OK();
    }
    Status status = CloseCurrentPair();
    if (!status.ok()) {
        Abort();
    }
    closed_ = true;
    return status;
}

void RollingVectorFileWriter::Abort() {
    aborted_ = true;
    if (vector_writer_) {
        vector_writer_->Abort();
        vector_writer_.reset();
    }
    RollingFileWriter::Abort();
    current_writer_.reset();
    closed_writers_.clear();
    results_.clear();
}

Result<std::vector<std::shared_ptr<DataFileMeta>>> RollingVectorFileWriter::GetResult() {
    if (aborted_) {
        return Status::Invalid("Writer has been aborted!");
    }
    return RollingFileWriter::GetResult();
}

}  // namespace paimon
