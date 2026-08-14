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

#include "paimon/core/io/managed_blob_reference_collector.h"

#include <optional>
#include <string_view>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/managed_blob_reference_file.h"
#include "paimon/fs/file_system.h"

namespace paimon {

Result<std::unique_ptr<ManagedBlobReferenceCollector>> ManagedBlobReferenceCollector::Create(
    const std::shared_ptr<FileSystem>& fs, const std::string& data_file_path,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::vector<std::string>& managed_field_names) {
    if (managed_field_names.empty()) {
        return Status::Invalid(
            "ManagedBlobReferenceCollector requires at least one managed blob field.");
    }
    std::vector<int32_t> managed_field_indices;
    managed_field_indices.reserve(managed_field_names.size());
    for (const auto& field_name : managed_field_names) {
        int32_t index = write_schema->GetFieldIndex(field_name);
        if (index < 0) {
            return Status::Invalid(
                fmt::format("Managed blob field {} is not part of the write schema.", field_name));
        }
        managed_field_indices.push_back(index);
    }
    int32_t value_kind_index = write_schema->GetFieldIndex(SpecialFields::ValueKind().Name());
    if (value_kind_index < 0) {
        return Status::Invalid(
            "ManagedBlobReferenceCollector requires the value kind column in the write schema.");
    }
    auto batch_type = arrow::struct_(write_schema->fields());
    return std::unique_ptr<ManagedBlobReferenceCollector>(new ManagedBlobReferenceCollector(
        fs, data_file_path, batch_type, std::move(managed_field_indices), value_kind_index));
}

ManagedBlobReferenceCollector::ManagedBlobReferenceCollector(
    const std::shared_ptr<FileSystem>& fs, const std::string& data_file_path,
    const std::shared_ptr<arrow::DataType>& batch_type, std::vector<int32_t> managed_field_indices,
    int32_t value_kind_index)
    : fs_(fs),
      sidecar_path_(ManagedBlobReferenceFile::SidecarPath(data_file_path)),
      batch_type_(batch_type),
      managed_field_indices_(std::move(managed_field_indices)),
      value_kind_index_(value_kind_index),
      logger_(Logger::GetLogger("ManagedBlobReferenceCollector")) {}

Status ManagedBlobReferenceCollector::Collect(KeyValueBatch* batch) {
    if (closed_) {
        return Status::Invalid("ManagedBlobReferenceCollector is already closed.");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(batch->batch.get(), batch_type_));
    // Re-export immediately so the batch stays writable; the imported view shares the buffers.
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*arrow_array, batch->batch.get()));

    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
    if (struct_array == nullptr) {
        return Status::Invalid(
            "ManagedBlobReferenceCollector expects a StructArray key-value batch.");
    }
    auto value_kind_column =
        std::dynamic_pointer_cast<arrow::Int8Array>(struct_array->field(value_kind_index_));
    if (value_kind_column == nullptr) {
        return Status::Invalid("ManagedBlobReferenceCollector expects an int8 value kind column.");
    }
    for (int32_t field_index : managed_field_indices_) {
        auto blob_column =
            std::dynamic_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(field_index));
        if (blob_column == nullptr) {
            return Status::Invalid(
                fmt::format("ManagedBlobReferenceCollector expects managed blob column {} to be a "
                            "LargeBinaryArray.",
                            struct_array->struct_type()->field(field_index)->name()));
        }
        for (int64_t row = 0; row < blob_column->length(); row++) {
            if (blob_column->IsNull(row)) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                                   RowKind::FromByteValue(value_kind_column->Value(row)));
            // A retract row keeps no payload, so it contributes no reference.
            if (row_kind->IsRetract()) {
                continue;
            }
            std::string_view value = blob_column->GetView(row);
            PAIMON_ASSIGN_OR_RAISE(bool is_descriptor,
                                   BlobDescriptor::IsBlobDescriptor(value.data(), value.size()));
            // Only descriptor values reference external payloads; other bytes (for example
            // inline blob data) carry their payload with the row.
            if (!is_descriptor) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlobDescriptor> descriptor,
                                   BlobDescriptor::Deserialize(value.data(), value.size()));
            descriptor_uris_.insert(descriptor->Uri());
        }
    }
    return Status::OK();
}

Status ManagedBlobReferenceCollector::Close() {
    if (closed_) {
        return Status::OK();
    }
    std::vector<ManagedBlobReferenceFile::Reference> references;
    for (const auto& uri : descriptor_uris_) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<ManagedBlobReferenceFile::Reference> reference,
                               ManagedBlobReferenceFile::FromDescriptorUri(uri));
        // URIs of non-managed storage carry their own lifecycle and are not recorded.
        if (reference) {
            references.push_back(std::move(reference.value()));
        }
    }
    descriptor_uris_.clear();
    Status status = ManagedBlobReferenceFile::Write(fs_, sidecar_path_, std::move(references));
    if (!status.ok()) {
        Abort();
        return status;
    }
    closed_ = true;
    return Status::OK();
}

void ManagedBlobReferenceCollector::Abort() {
    Status status = fs_->Delete(sidecar_path_);
    // A missing sidecar is normal here (a failed write already removes its partial file);
    // only a real cleanup failure is worth a warning (Java: deleteQuietly).
    if (!status.ok() && !status.IsNotExist()) {
        PAIMON_LOG_WARN(logger_, "Failed to delete managed blob reference sidecar %s: %s",
                        sidecar_path_.c_str(), status.ToString().c_str());
    }
    closed_ = true;
}

Result<std::string> ManagedBlobReferenceCollector::ResultFileName() const {
    if (!closed_) {
        return Status::Invalid(
            "ManagedBlobReferenceCollector result requires the collector to be closed.");
    }
    return PathUtil::GetName(sidecar_path_);
}

}  // namespace paimon
