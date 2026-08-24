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

#include "paimon/core/io/primary_key_blob_externalizer.h"

#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"

namespace paimon {

namespace {

/// The format managed blob packs are written in. Named once: the writer is built through the
/// format factory, and the same name is what an error has to report.
constexpr const char kPackFormat[] = "blob";

}  // namespace

/// Rolls `.managed.blob` packs for one managed blob field. Each written value appends one blob
/// format record to the current pack; the pack is sealed once it reaches the blob target file
/// size. Descriptors point straight at the payload bytes inside the pack, so reading one back
/// is a single ranged read that needs no pack footer.
class PrimaryKeyBlobExternalizer::ManagedBlobPackWriter {
 public:
    ManagedBlobPackWriter(const CoreOptions& options, const std::shared_ptr<arrow::Field>& field,
                          const std::shared_ptr<DataFilePathFactory>& path_factory,
                          std::vector<std::string>* uncommitted_packs,
                          const std::shared_ptr<MemoryPool>& pool)
        : options_(options),
          field_(field),
          path_factory_(path_factory),
          uncommitted_packs_(uncommitted_packs),
          target_file_size_(options.GetBlobTargetFileSize()),
          pool_(pool) {}

    /// Copies row `row` of `column` into the current pack and returns the serialized
    /// descriptor of the copied payload. The value may itself be a serialized descriptor; the
    /// blob format writer then streams the referenced bytes in, re-materializing them.
    Result<PAIMON_UNIQUE_PTR<Bytes>> Write(const std::shared_ptr<arrow::Array>& column,
                                           int64_t row) {
        if (writer_ == nullptr) {
            PAIMON_RETURN_NOT_OK(OpenCurrent());
        }

        std::shared_ptr<arrow::Array> element = column->Slice(row, 1);
        std::shared_ptr<arrow::Array> pack_row;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(pack_row,
                                          arrow::StructArray::Make({std::move(element)}, {field_}));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*pack_row, &c_array));
        PAIMON_RETURN_NOT_OK(writer_->AddBatch(&c_array));

        // The format writer reports the payload bytes of the record it just stored, so the
        // record layout stays inside the format. Asked through the base interface rather than
        // by downcasting to the blob writer: the blob format lives in a plugin library, so a
        // checked cast would need its type info here, and an unchecked one would be undefined
        // behaviour the moment the factory returned anything else.
        std::optional<std::pair<int64_t, int64_t>> payload_range = writer_->LastPayloadRange();
        if (!payload_range) {
            return Status::Invalid(
                fmt::format("Managed blob pack {} did not produce a payload record. The '{}' "
                            "format must store one addressable payload per record.",
                            current_path_, kPackFormat));
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<BlobDescriptor> descriptor,
            BlobDescriptor::Create(current_path_, payload_range->first, payload_range->second));
        PAIMON_UNIQUE_PTR<Bytes> serialized = descriptor->Serialize(pool_);

        PAIMON_ASSIGN_OR_RAISE(
            bool reach_target_size,
            writer_->ReachTargetSize(/*suggested_check=*/true, target_file_size_));
        if (reach_target_size) {
            PAIMON_RETURN_NOT_OK(CloseCurrent());
        }
        return serialized;
    }

    /// Seals the current pack: writes the blob format footer and closes the stream. The
    /// underlying stream is closed even when writing the footer fails, so a failed seal never
    /// leaks the stream.
    Status CloseCurrent() {
        if (writer_ == nullptr) {
            return Status::OK();
        }
        Status status = writer_->Finish();
        Status close_status = out_->Close();
        if (status.ok()) {
            status = close_status;
        }
        writer_.reset();
        out_.reset();
        current_path_.clear();
        return status;
    }

    /// Quietly drops the current pack writer; the file itself is removed through the
    /// uncommitted pack list.
    void AbortCurrent() {
        if (out_) {
            [[maybe_unused]] Status status = out_->Close();
        }
        writer_.reset();
        out_.reset();
        current_path_.clear();
    }

 private:
    Status OpenCurrent() {
        std::string path = path_factory_->NewManagedBlobPath();
        uncommitted_packs_->push_back(path);
        // The blob format lives in its own plugin library, so the writer is created through
        // the format factory like every other core write path; core must not reference the
        // plugin's out-of-line symbols directly.
        std::map<std::string, std::string> format_options = options_.ToMap();
        // Managed pack writes never convert fetch failures to NULL payloads and never
        // interpret placeholder sentinels: strip the user-facing toggles so the format
        // defaults (false) apply.
        format_options.erase(Options::BLOB_WRITE_NULL_ON_MISSING_FILE);
        format_options.erase(Options::BLOB_WRITE_NULL_ON_FETCH_FAILURE);
        BlobDefs::EraseInternalPlaceholderOptions(&format_options);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileFormat> format,
                               FileFormatFactory::Get(kPackFormat, format_options));
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportSchema(*arrow::schema(arrow::FieldVector{field_}), &c_schema));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriterBuilder> writer_builder,
                               format->CreateWriterBuilder(&c_schema, /*batch_size=*/1));
        writer_builder->WithMemoryPool(pool_);
        if (auto* specific_fs_builder =
                dynamic_cast<SpecificFSWriterBuilder*>(writer_builder.get())) {
            specific_fs_builder->WithFileSystem(options_.GetFileSystem());
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out,
                               options_.GetFileSystem()->Create(path, /*overwrite=*/false));
        out_ = std::move(out);
        PAIMON_ASSIGN_OR_RAISE(writer_, writer_builder->Build(out_, /*compression=*/"none"));
        current_path_ = path;
        return Status::OK();
    }

    CoreOptions options_;
    std::shared_ptr<arrow::Field> field_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::vector<std::string>* uncommitted_packs_;
    int64_t target_file_size_;
    std::shared_ptr<MemoryPool> pool_;

    std::string current_path_;
    std::shared_ptr<OutputStream> out_;
    std::unique_ptr<FormatWriter> writer_;
};

Result<std::unique_ptr<PrimaryKeyBlobExternalizer>> PrimaryKeyBlobExternalizer::Create(
    const CoreOptions& options, const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<MemoryPool>& pool) {
    std::vector<std::string> inline_field_names = options.GetBlobInlineFields();
    std::set<std::string> inline_fields(inline_field_names.begin(), inline_field_names.end());
    std::vector<std::string> managed_field_names =
        BlobUtils::ManagedBlobFieldNames(value_schema, inline_fields);
    if (managed_field_names.empty()) {
        return std::unique_ptr<PrimaryKeyBlobExternalizer>();
    }
    // Guarded by SchemaValidation for tables created through the catalog; checked again here
    // because pack rolling cannot work with a non-positive target size.
    if (options.GetBlobTargetFileSize() <= 0) {
        return Status::Invalid(
            fmt::format("Managed blob target file size must be positive, "
                        "but got {}.",
                        options.GetBlobTargetFileSize()));
    }
    std::vector<int32_t> managed_field_indices;
    managed_field_indices.reserve(managed_field_names.size());
    for (const auto& field_name : managed_field_names) {
        managed_field_indices.push_back(value_schema->GetFieldIndex(field_name));
    }
    auto value_type = arrow::struct_(value_schema->fields());
    return std::unique_ptr<PrimaryKeyBlobExternalizer>(new PrimaryKeyBlobExternalizer(
        options, value_type, std::move(managed_field_indices), path_factory, pool));
}

PrimaryKeyBlobExternalizer::PrimaryKeyBlobExternalizer(
    const CoreOptions& options, const std::shared_ptr<arrow::DataType>& value_type,
    std::vector<int32_t> managed_field_indices,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<MemoryPool>& pool)
    : options_(options),
      value_type_(value_type),
      managed_field_indices_(std::move(managed_field_indices)),
      path_factory_(path_factory),
      pool_(pool),
      arrow_pool_(GetArrowPool(pool)),
      logger_(Logger::GetLogger("PrimaryKeyBlobExternalizer")) {
    auto struct_type = checked_pointer_cast<arrow::StructType>(value_type_);
    pack_writers_.reserve(managed_field_indices_.size());
    for (int32_t field_index : managed_field_indices_) {
        pack_writers_.push_back(std::make_unique<ManagedBlobPackWriter>(
            options_, struct_type->field(field_index), path_factory_, &uncommitted_packs_, pool_));
    }
}

PrimaryKeyBlobExternalizer::~PrimaryKeyBlobExternalizer() {
    Abort();
}

Result<std::unique_ptr<RecordBatch>> PrimaryKeyBlobExternalizer::Externalize(
    std::unique_ptr<RecordBatch>&& moved_batch) {
    std::unique_ptr<RecordBatch> batch = std::move(moved_batch);
    Result<std::unique_ptr<RecordBatch>> result = [&]() -> Result<std::unique_ptr<RecordBatch>> {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                          arrow::ImportArray(batch->GetData(), value_type_));
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
        if (struct_array == nullptr) {
            return Status::Invalid(
                "PrimaryKeyBlobExternalizer expects a StructArray record batch.");
        }
        const std::vector<RecordBatch::RowKind>& row_kinds = batch->GetRowKind();
        if (!row_kinds.empty() &&
            static_cast<int64_t>(row_kinds.size()) != struct_array->length()) {
            return Status::Invalid(
                "PrimaryKeyBlobExternalizer batch row kinds do not match the row count.");
        }
        arrow::ArrayVector new_children = struct_array->fields();
        for (size_t writer_index = 0; writer_index < managed_field_indices_.size();
             writer_index++) {
            int32_t field_index = managed_field_indices_[writer_index];
            const auto& column = struct_array->field(field_index);
            auto blob_column = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(column);
            if (blob_column == nullptr) {
                return Status::Invalid(
                    fmt::format("PrimaryKeyBlobExternalizer expects managed blob column {} to be a "
                                "LargeBinaryArray.",
                                struct_array->struct_type()->field(field_index)->name()));
            }
            // The member pool, not a local one: these buffers enter the write buffer and
            // must stay allocatable-from until the batch is flushed and released.
            arrow::LargeBinaryBuilder builder(arrow_pool_.get());
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(blob_column->length()));
            for (int64_t row = 0; row < blob_column->length(); row++) {
                bool retract =
                    !row_kinds.empty() && (row_kinds[row] == RecordBatch::RowKind::UPDATE_BEFORE ||
                                           row_kinds[row] == RecordBatch::RowKind::DELETE);
                // A retract row never keeps a payload: the managed value is dropped without
                // writing anything to a pack.
                if (retract || blob_column->IsNull(row)) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
                    continue;
                }
                PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> descriptor,
                                       pack_writers_[writer_index]->Write(blob_column, row));
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(
                    reinterpret_cast<const uint8_t*>(descriptor->data()), descriptor->size()));
            }
            std::shared_ptr<arrow::Array> descriptor_column;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&descriptor_column));
            new_children[field_index] = std::move(descriptor_column);
        }

        auto struct_type = checked_pointer_cast<arrow::StructType>(value_type_);
        std::shared_ptr<arrow::StructArray> externalized_array;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            externalized_array, arrow::StructArray::Make(new_children, struct_type->fields()));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*externalized_array, &c_array));
        return std::make_unique<RecordBatch>(batch->GetPartition(), batch->GetBucket(), row_kinds,
                                             &c_array);
    }();
    if (!result.ok()) {
        Abort();
    }
    return result;
}

Status PrimaryKeyBlobExternalizer::CloseCurrentWriters() {
    Status status = Status::OK();
    for (const auto& pack_writer : pack_writers_) {
        Status close_status = pack_writer->CloseCurrent();
        if (status.ok() && !close_status.ok()) {
            status = close_status;
        }
    }
    return status;
}

Result<std::vector<std::string>> PrimaryKeyBlobExternalizer::PrepareCommit() {
    Status status = CloseCurrentWriters();
    if (!status.ok()) {
        Abort();
        return status;
    }
    // The sealed packs are handed over: this externalizer stops tracking them and their paths
    // go to the caller, which is what lets a failed commit delete exactly the packs it created
    // instead of every pack its data files happen to reference.
    std::vector<std::string> handed_over = std::move(uncommitted_packs_);
    uncommitted_packs_.clear();
    return handed_over;
}

void PrimaryKeyBlobExternalizer::Abort() {
    for (const auto& pack_writer : pack_writers_) {
        pack_writer->AbortCurrent();
    }
    const auto& fs = options_.GetFileSystem();
    for (const auto& path : uncommitted_packs_) {
        Status status = fs->Delete(path);
        // A pack that never came into existence (its create failed) is normal on this path;
        // only a real cleanup failure is worth a warning.
        if (!status.ok() && !status.IsNotExist()) {
            PAIMON_LOG_WARN(logger_, "Failed to delete uncommitted managed blob pack %s: %s",
                            path.c_str(), status.ToString().c_str());
        }
    }
    uncommitted_packs_.clear();
}

}  // namespace paimon
