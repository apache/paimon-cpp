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

#include "paimon/common/reader/projection_batch_reader.h"

#include <algorithm>

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"

namespace paimon {

Result<BatchReader::ReadBatch> ProjectionBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch_with_bitmap, NextBatchWithBitmap());
    PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, ReaderUtils::ApplyBitmapToReadBatch(
                                                std::move(batch_with_bitmap), arrow_pool_));
    return batch;
}

Result<BatchReader::ReadBatchWithBitmap> ProjectionBatchReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, reader_->NextBatchWithBitmap());
    PAIMON_RETURN_NOT_OK(Project(&batch.first));
    return batch;
}

Status ProjectionBatchReader::Project(ReadBatch* batch) const {
    if (IsEofBatch(*batch)) {
        return Status::OK();
    }
    ScopeGuard release([batch]() { ReaderUtils::ReleaseReadBatch(std::move(*batch)); });
    auto& [c_array, c_schema] = *batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Field> root,
                                      arrow::ImportField(c_schema.get()));
    if (root->type()->id() != arrow::Type::STRUCT) {
        return Status::Invalid("projection batch reader requires a struct array");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(c_array.get(), root->type()));
    auto struct_array = checked_pointer_cast<arrow::StructArray>(array);
    const auto& type = *struct_array->struct_type();
    arrow::ArrayVector children;
    arrow::FieldVector fields;
    auto append = [&](const std::string& name) -> Status {
        int32_t index = type.GetFieldIndex(name);
        if (index < 0) {
            return Status::Invalid("Output field is missing or ambiguous in batch: ", name);
        }
        fields.push_back(type.field(index));
        children.push_back(struct_array->field(index));
        return Status::OK();
    };
    const std::string& row_kind = SpecialFields::ValueKind().Name();
    if (type.GetFieldIndex(row_kind) >= 0) {
        PAIMON_RETURN_NOT_OK(append(row_kind));
    }
    for (const std::string& name : output_fields_) {
        if (name != row_kind || type.GetFieldIndex(row_kind) < 0) {
            PAIMON_RETURN_NOT_OK(append(name));
        }
    }
    // Root TopK appends its score even when it is not projected. Keep it until the outer TopK
    // reader has ranked the rows and removes the score according to the output schema.
    for (const auto& field : type.fields()) {
        const std::string& name = field->name();
        if (SpecialFields::ParseIndexScoreFieldName(name) &&
            std::find(output_fields_.begin(), output_fields_.end(), name) == output_fields_.end()) {
            PAIMON_RETURN_NOT_OK(append(name));
        }
    }
    // Specify the length so that an empty projection still preserves the row count.
    auto output =
        std::make_shared<arrow::StructArray>(arrow::struct_(fields), array->length(), children,
                                             array->null_bitmap(), array->null_count());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*output, c_array.get()));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportField(*root->WithType(output->type()), c_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), array));
    release.Release();
    return Status::OK();
}
}  // namespace paimon
