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

#include "paimon/common/reader/managed_blob_resolving_batch_reader.h"

#include <iterator>
#include <string_view>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/data/blob.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {

ManagedBlobResolvingBatchReader::ManagedBlobResolvingBatchReader(
    std::unique_ptr<BatchReader>&& reader, std::vector<std::string> managed_blob_fields,
    const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      arrow_pool_(GetArrowPool(pool)),
      reader_(std::move(reader)),
      managed_blob_fields_(std::make_move_iterator(managed_blob_fields.begin()),
                           std::make_move_iterator(managed_blob_fields.end())),
      fs_(fs) {}

Result<BatchReader::ReadBatch> ManagedBlobResolvingBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader_->NextBatch());
    if (BatchReader::IsEofBatch(batch)) {
        return batch;
    }
    if (managed_blob_fields_.empty()) {
        return batch;
    }

    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
    if (struct_array == nullptr) {
        return Status::Invalid(
            "invalid batch, ManagedBlobResolvingBatchReader expects a StructArray batch.");
    }
    const auto struct_type = struct_array->struct_type();

    arrow::ArrayVector new_fields = struct_array->fields();
    for (int32_t field_idx = 0; field_idx < struct_type->num_fields(); ++field_idx) {
        const auto& field = struct_type->field(field_idx);
        if (managed_blob_fields_.find(field->name()) == managed_blob_fields_.end()) {
            continue;
        }
        const auto& column = struct_array->field(field_idx);
        auto descriptor_array = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(column);
        if (descriptor_array == nullptr) {
            return Status::Invalid(fmt::format(
                "ManagedBlobResolvingBatchReader expects managed blob column {} to be a "
                "LargeBinaryArray.",
                field->name()));
        }
        PAIMON_ASSIGN_OR_RAISE(new_fields[field_idx], ResolveBlobColumn(descriptor_array));
    }
    // Rebuild with the original fields so the blob extension metadata and nullability survive
    // the resolution.
    std::shared_ptr<arrow::StructArray> resolved_struct_array;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(resolved_struct_array,
                                      arrow::StructArray::Make(new_fields, struct_type->fields()));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*resolved_struct_array, c_array.get(), c_schema.get()));
    return batch;
}

Result<std::shared_ptr<arrow::Array>> ManagedBlobResolvingBatchReader::ResolveBlobColumn(
    const std::shared_ptr<arrow::LargeBinaryArray>& descriptor_array) {
    arrow::LargeBinaryBuilder builder(arrow_pool_.get());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(descriptor_array->length()));
    for (int64_t row = 0; row < descriptor_array->length(); ++row) {
        if (descriptor_array->IsNull(row)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
            continue;
        }
        std::string_view value = descriptor_array->GetView(row);
        // Every non-null managed value is a descriptor written by the externalizer; anything
        // else means the file was not produced by a managed blob write.
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Blob> blob,
                               Blob::FromDescriptor(value.data(), value.size()));
        PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> payload, blob->ToData(fs_, pool_));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            builder.Append(reinterpret_cast<const uint8_t*>(payload->data()), payload->size()));
    }
    std::shared_ptr<arrow::Array> payload_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&payload_array));
    return payload_array;
}

}  // namespace paimon
