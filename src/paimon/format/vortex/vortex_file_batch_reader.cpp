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

#include "paimon/format/vortex/vortex_file_batch_reader.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/math.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/fs/file_system.h"

namespace paimon::vortex {

namespace {

// Vortex's Arrow export represents strings and binaries with the view types (StringView /
// BinaryView). paimon schemas use the standard string/binary types, so map the view types back,
// recursing through nested types. Types that contain no views are returned unchanged.
std::shared_ptr<arrow::DataType> NormalizeViewType(const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
        case arrow::Type::STRING_VIEW:
            return arrow::utf8();
        case arrow::Type::BINARY_VIEW:
            return arrow::binary();
        case arrow::Type::STRUCT: {
            arrow::FieldVector fields;
            fields.reserve(type->num_fields());
            for (const std::shared_ptr<arrow::Field>& field : type->fields()) {
                fields.push_back(field->WithType(NormalizeViewType(field->type())));
            }
            return arrow::struct_(fields);
        }
        case arrow::Type::LIST:
            return arrow::list(type->field(0)->WithType(NormalizeViewType(type->field(0)->type())));
        case arrow::Type::LARGE_LIST:
            return arrow::large_list(
                type->field(0)->WithType(NormalizeViewType(type->field(0)->type())));
        case arrow::Type::FIXED_SIZE_LIST:
            return arrow::fixed_size_list(
                type->field(0)->WithType(NormalizeViewType(type->field(0)->type())),
                checked_cast<const arrow::FixedSizeListType&>(*type).list_size());
        default:
            return type;
    }
}

std::shared_ptr<arrow::Schema> NormalizeViewSchema(const std::shared_ptr<arrow::Schema>& schema) {
    arrow::FieldVector fields;
    fields.reserve(schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& field : schema->fields()) {
        fields.push_back(field->WithType(NormalizeViewType(field->type())));
    }
    return arrow::schema(fields, schema->metadata());
}

// Rebuilds `array` with every StringView/BinaryView column (which Vortex's Arrow export produces)
// converted to the standard string/binary type. Arrow 17 has no cast kernel between the view and
// standard types, so the leaves are rebuilt with builders and the parents reconstructed, recursing
// through structs and lists. Arrays with no view-typed data are returned unchanged. `array` must be
// offset-normalized (offset 0) so the rebuilt parents and children stay consistent.
Result<std::shared_ptr<arrow::Array>> NormalizeViewArray(const std::shared_ptr<arrow::Array>& array,
                                                         arrow::MemoryPool* pool) {
    switch (array->type_id()) {
        case arrow::Type::STRING_VIEW: {
            const auto& view = checked_cast<const arrow::StringViewArray&>(*array);
            arrow::StringBuilder builder(pool);
            for (int64_t i = 0; i < view.length(); ++i) {
                if (view.IsNull(i)) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
                } else {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(view.GetView(i)));
                }
            }
            std::shared_ptr<arrow::Array> out;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&out));
            return out;
        }
        case arrow::Type::BINARY_VIEW: {
            const auto& view = checked_cast<const arrow::BinaryViewArray&>(*array);
            arrow::BinaryBuilder builder(pool);
            for (int64_t i = 0; i < view.length(); ++i) {
                if (view.IsNull(i)) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
                } else {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(view.GetView(i)));
                }
            }
            std::shared_ptr<arrow::Array> out;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&out));
            return out;
        }
        case arrow::Type::STRUCT: {
            const auto& struct_array = checked_cast<const arrow::StructArray&>(*array);
            arrow::ArrayVector children;
            arrow::FieldVector fields;
            children.reserve(struct_array.num_fields());
            fields.reserve(struct_array.num_fields());
            bool changed = false;
            for (int32_t i = 0; i < struct_array.num_fields(); ++i) {
                const std::shared_ptr<arrow::Array>& child = struct_array.field(i);
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> normalized_child,
                                       NormalizeViewArray(child, pool));
                changed = changed || normalized_child.get() != child.get();
                fields.push_back(array->type()->field(i)->WithType(normalized_child->type()));
                children.push_back(std::move(normalized_child));
            }
            if (!changed) {
                return array;
            }
            return std::make_shared<arrow::StructArray>(
                arrow::struct_(fields), struct_array.length(), children, struct_array.null_bitmap(),
                struct_array.null_count(), struct_array.offset());
        }
        case arrow::Type::LIST: {
            const auto& list = checked_cast<const arrow::ListArray&>(*array);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> values,
                                   NormalizeViewArray(list.values(), pool));
            if (values.get() == list.values().get()) {
                return array;
            }
            return std::make_shared<arrow::ListArray>(
                arrow::list(array->type()->field(0)->WithType(values->type())), list.length(),
                list.value_offsets(), values, list.null_bitmap(), list.null_count(), list.offset());
        }
        case arrow::Type::LARGE_LIST: {
            const auto& list = checked_cast<const arrow::LargeListArray&>(*array);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> values,
                                   NormalizeViewArray(list.values(), pool));
            if (values.get() == list.values().get()) {
                return array;
            }
            return std::make_shared<arrow::LargeListArray>(
                arrow::large_list(array->type()->field(0)->WithType(values->type())), list.length(),
                list.value_offsets(), values, list.null_bitmap(), list.null_count(), list.offset());
        }
        case arrow::Type::FIXED_SIZE_LIST: {
            const auto& list = checked_cast<const arrow::FixedSizeListArray&>(*array);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> values,
                                   NormalizeViewArray(list.values(), pool));
            if (values.get() == list.values().get()) {
                return array;
            }
            const auto& fsl_type = checked_cast<const arrow::FixedSizeListType&>(*array->type());
            return std::make_shared<arrow::FixedSizeListArray>(
                arrow::fixed_size_list(array->type()->field(0)->WithType(values->type()),
                                       fsl_type.list_size()),
                list.length(), values, list.null_bitmap(), list.null_count(), list.offset());
        }
        default:
            return array;
    }
}

// Projects a struct array to the columns named by `read_schema`, selected by field name and ordered
// as in `read_schema`, so NextBatch returns exactly the read schema (the FileBatchReader contract;
// paimon's FieldMappingReader maps fields but does not re-project). Returns `array` unchanged when
// it already matches, or when `read_schema` is null (SetReadSchema not yet called).
Result<std::shared_ptr<arrow::Array>> ProjectToReadSchema(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::Schema>& read_schema) {
    if (read_schema == nullptr) {
        return array;
    }
    const std::shared_ptr<arrow::DataType> target_type = arrow::struct_(read_schema->fields());
    if (array->type()->Equals(target_type)) {
        return array;
    }
    const auto& struct_array = checked_cast<const arrow::StructArray&>(*array);
    const auto& struct_type = checked_cast<const arrow::StructType&>(*array->type());
    arrow::ArrayVector children;
    children.reserve(read_schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& read_field : read_schema->fields()) {
        const int32_t index = struct_type.GetFieldIndex(read_field->name());
        if (index < 0) {
            return Status::Invalid(fmt::format(
                "Vortex read field '{}' is not present in the file schema", read_field->name()));
        }
        children.push_back(struct_array.field(index));
    }
    return std::make_shared<arrow::StructArray>(target_type, struct_array.length(), children,
                                                struct_array.null_bitmap(),
                                                struct_array.null_count(), struct_array.offset());
}

}  // namespace

VortexFileBatchReader::VortexFileBatchReader(
    const std::shared_ptr<InputStream>& input, int32_t batch_size,
    std::shared_ptr<VortexInputContext> input_context, VxSessionPtr session,
    VxDataSourcePtr data_source, VxScanPtr scan, const std::shared_ptr<arrow::Schema>& file_schema,
    const std::shared_ptr<arrow::DataType>& struct_type, uint64_t total_rows,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : input_(input),
      batch_size_(batch_size),
      input_context_(std::move(input_context)),
      session_(std::move(session)),
      data_source_(std::move(data_source)),
      scan_(std::move(scan)),
      file_schema_(NormalizeViewSchema(file_schema)),
      struct_type_(struct_type),
      total_rows_(total_rows),
      pool_(pool),
      arrow_pool_(arrow_pool),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<VortexFileBatchReader>> VortexFileBatchReader::Create(
    const std::shared_ptr<InputStream>& input, int32_t batch_size,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (input == nullptr || pool == nullptr || batch_size <= 0) {
        return Status::Invalid(
            "Vortex reader requires non-null input and memory pool, and positive batch size");
    }
    // Vortex reads the file through positional callbacks into `input`, so nothing is staged here.
    PAIMON_ASSIGN_OR_RAISE(int64_t signed_length, input->Length());
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(signed_length, "Vortex input length"));
    auto length = static_cast<uint64_t>(signed_length);
    auto input_context = std::make_shared<VortexInputContext>(input);

    VxSessionPtr session(vx_session_new(), vx_session_free);
    if (session == nullptr) {
        return Status::IOError("failed to create Vortex session");
    }
    vx_error* error = nullptr;
    VxDataSourcePtr data_source(
        vx_data_source_new_callback(session.get(), VortexInputContext::MakeCallbacks(input_context),
                                    length, &error),
        vx_data_source_free);
    if (data_source == nullptr) {
        return VortexCallbackError("open Vortex callback data source", error,
                                   input_context->GetCallbackStatus());
    }

    // The data source dtype is available without consuming a scan.
    // NOTE(vortex 0.75): vx_data_source_dtype returns a BORROWED pointer (arc_wrapper new_ref:
    // no refcount bump), so it must NOT be freed here; doing so spuriously decrements the data
    // source's DType Arc and causes a use-after-free/segfault later in the scan. It stays valid
    // as long as `data_source` lives, which outlives this schema conversion. (Upstream 0.77
    // changed vx_data_source_dtype to return an owned clone that MUST be freed; if the pin moves
    // to >=0.77, wrap the result with vx_dtype_free again.)
    const vx_dtype* dtype = vx_data_source_dtype(data_source.get());
    if (dtype == nullptr) {
        return Status::IOError("failed to read Vortex data source dtype");
    }
    ::ArrowSchema ffi_schema = {};
    error = nullptr;
    if (vx_dtype_to_arrow_schema(dtype, &ffi_schema, &error) != 0) {
        return VortexFfiError("convert Vortex dtype to Arrow schema", error);
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> file_schema,
                                      arrow::ImportSchema(&ffi_schema));
    // `file_schema` here still carries Vortex's view types. `struct_type` is built from it raw on
    // purpose, because NextBatch imports each scanned batch as this exact type; the constructor
    // normalizes the separate copy it keeps as file_schema_.
    std::shared_ptr<arrow::DataType> struct_type = arrow::struct_(file_schema->fields());

    vx_estimate row_count{};
    vx_data_source_get_row_count(data_source.get(), &row_count);
    if (row_count.type == VX_ESTIMATE_UNKNOWN) {
        return Status::Invalid("Vortex data source did not report a row count");
    }
    const uint64_t total_rows = row_count.estimate;

    // The scan is created lazily on first read (and re-created by SetReadSchema), since a Vortex
    // scan may be consumed only once.
    return std::unique_ptr<VortexFileBatchReader>(new VortexFileBatchReader(
        input, batch_size, std::move(input_context), std::move(session), std::move(data_source),
        VxScanPtr(nullptr, vx_scan_free), file_schema, struct_type, total_rows, pool, arrow_pool));
}

VortexFileBatchReader::~VortexFileBatchReader() {
    CloseInternal();
}

Result<bool> VortexFileBatchReader::OpenNextPartitionStream() {
    if (scan_ == nullptr) {
        vx_error* error = nullptr;
        // NULL options: scan all rows and columns (no projection/predicate pushdown).
        VxScanPtr scan(vx_data_source_scan(data_source_.get(), /*options=*/nullptr,
                                           /*estimate=*/nullptr, &error),
                       vx_scan_free);
        if (scan == nullptr) {
            return VortexCallbackError("create Vortex scan", error,
                                       input_context_->GetCallbackStatus());
        }
        scan_ = std::move(scan);
    }
    vx_error* error = nullptr;
    VxPartitionPtr partition(vx_scan_next_partition(scan_.get(), &error), vx_partition_free);
    if (error != nullptr) {
        return VortexCallbackError("advance Vortex scan partition", error,
                                   input_context_->GetCallbackStatus());
    }
    if (partition == nullptr) {
        return false;  // Scan exhausted.
    }
    // vx_partition_scan_arrow consumes the partition (even on error), so release ownership here.
    ::ArrowArrayStream stream{};
    error = nullptr;
    if (vx_partition_scan_arrow(session_.get(), partition.release(), &stream, &error) != 0) {
        return VortexCallbackError("scan Vortex partition to Arrow", error,
                                   input_context_->GetCallbackStatus());
    }
    current_stream_ = stream;
    stream_active_ = true;
    return true;
}

Result<std::shared_ptr<arrow::Array>> VortexFileBatchReader::ReadNextArray() {
    while (true) {
        if (!stream_active_) {
            PAIMON_ASSIGN_OR_RAISE(bool opened, OpenNextPartitionStream());
            if (!opened) {
                return std::shared_ptr<arrow::Array>();  // End of scan.
            }
        }
        ::ArrowArray ffi_array = {};
        const int32_t rc = current_stream_.get_next(&current_stream_, &ffi_array);
        if (rc != 0) {
            const char* message = current_stream_.get_last_error(&current_stream_);
            // A read failure surfaces here as an opaque stream error, so prefer the status the IO
            // callback stashed.
            Status callback_status = input_context_->GetCallbackStatus();
            ReleaseStream();
            if (!callback_status.ok()) {
                return callback_status.WithMessage("read Vortex batch", ": ",
                                                   callback_status.message());
            }
            return Status::IOError("Vortex Arrow stream error: ",
                                   message == nullptr ? "unknown" : message);
        }
        if (ffi_array.release == nullptr) {
            ReleaseStream();  // Partition exhausted; try the next one.
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::ImportArray(&ffi_array, struct_type_));
        if (array->length() == 0) {
            continue;  // Skip empty batches.
        }
        return array;
    }
}

Result<BatchReader::ReadBatch> VortexFileBatchReader::NextBatch() {
    if (closed_) {
        return Status::Invalid("Vortex reader is closed");
    }
    if (current_batch_ == nullptr || current_batch_offset_ == current_batch_->length()) {
        PAIMON_ASSIGN_OR_RAISE(current_batch_, ReadNextArray());
        current_batch_offset_ = 0;
    }
    if (current_batch_ == nullptr) {
        previous_batch_row_count_ = 0;
        return BatchReader::MakeEofBatch();
    }

    const int64_t row_count =
        std::min<int64_t>(batch_size_, current_batch_->length() - current_batch_offset_);
    std::shared_ptr<arrow::Array> sliced_array =
        current_batch_->Slice(current_batch_offset_, row_count);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> normalized_array,
                           ArrowUtils::NormalizeArrayOffsets(sliced_array, arrow_pool_.get()));
    // Vortex exports strings/binaries as Arrow view types; convert them to the standard types
    // (Arrow 17 has no cast kernel for this) so the batch matches the file schema paimon expects.
    // Done after offset normalization so the array is offset-0.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> output_array,
                           NormalizeViewArray(normalized_array, arrow_pool_.get()));
    // Honor the read schema: NextBatch must return exactly its columns (selected by name), which
    // paimon's read path relies on (FieldMappingReader maps fields but does not re-project).
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> projected_array,
                           ProjectToReadSchema(output_array, read_schema_));

    previous_first_row_ = rows_emitted_;
    previous_batch_row_count_ = static_cast<uint64_t>(row_count);
    rows_emitted_ += static_cast<uint64_t>(row_count);
    current_batch_offset_ += row_count;

    auto ffi_array = std::make_unique<::ArrowArray>();
    auto ffi_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*projected_array, ffi_array.get(), ffi_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(ffi_array.get(), ffi_schema.get(), arrow_pool_));
    return std::make_pair(std::move(ffi_array), std::move(ffi_schema));
}

Result<std::unique_ptr<::ArrowSchema>> VortexFileBatchReader::GetFileSchema() const {
    auto schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*file_schema_, schema.get()));
    return schema;
}

Status VortexFileBatchReader::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (read_schema == nullptr) {
        return Status::Invalid("Vortex read schema is nullptr");
    }
    (void)selection_bitmap;
    // Projection and predicates are not pushed into Vortex: the whole file is scanned and
    // NextBatch projects the result to `read_schema`. Reading restarts from the first row, so drop
    // the single-use scan and any open stream; the next read re-creates the scan.
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> imported_read_schema,
                                      arrow::ImportSchema(read_schema));
    read_schema_ = std::move(imported_read_schema);
    predicate_filter_ = std::dynamic_pointer_cast<PredicateFilter>(predicate);
    ReleaseStream();
    current_batch_.reset();
    current_batch_offset_ = 0;
    scan_.reset();
    rows_emitted_ = 0;
    previous_first_row_ = std::numeric_limits<uint64_t>::max();
    previous_batch_row_count_ = 0;
    return Status::OK();
}

Result<uint64_t> VortexFileBatchReader::GetPreviousBatchFileRowId(uint64_t batch_row_id) const {
    if (previous_batch_row_count_ == 0) {
        return Status::Invalid(previous_first_row_ == std::numeric_limits<uint64_t>::max()
                                   ? "no Vortex batch has been read yet"
                                   : "last Vortex batch was EOF");
    }
    if (batch_row_id >= previous_batch_row_count_) {
        return Status::Invalid(fmt::format("batch row id {} is out of range {}", batch_row_id,
                                           previous_batch_row_count_));
    }
    return previous_first_row_ + batch_row_id;
}

Result<uint64_t> VortexFileBatchReader::GetNumberOfRows() const {
    return total_rows_;
}

std::shared_ptr<Metrics> VortexFileBatchReader::GetReaderMetrics() const {
    return metrics_;
}

void VortexFileBatchReader::Close() {
    CloseInternal();
}

void VortexFileBatchReader::ReleaseStream() {
    if (stream_active_) {
        if (current_stream_.release != nullptr) {
            current_stream_.release(&current_stream_);
        }
        current_stream_ = ::ArrowArrayStream{};
        stream_active_ = false;
    }
}

void VortexFileBatchReader::CloseInternal() {
    if (closed_) {
        return;
    }
    // Release in dependency order: the Arrow stream (needs the session alive) first, then the scan
    // (borrows the data source), the data source (reads through the input context), the session,
    // then our reference to the input context. A read still running on a Vortex thread keeps the
    // context alive through the reference its callbacks hold.
    ReleaseStream();
    current_batch_.reset();
    scan_.reset();
    data_source_.reset();
    session_.reset();
    input_context_.reset();
    if (input_ != nullptr) {
        (void)input_->Close();
    }
    input_.reset();
    closed_ = true;
}

}  // namespace paimon::vortex
