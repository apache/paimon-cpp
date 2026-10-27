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
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/math.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/format/vortex/vortex_predicate_converter.h"
#include "paimon/fs/file_system.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::vortex {

namespace {

// Must match the physical row index column appended by vortex-ffi scan.rs.
constexpr char kRowIdxColumn[] = "_row_idx";

// Vortex reports the file's zone and paimon asks for the reader's; the instants are the same.
std::shared_ptr<arrow::ArrayData> AlignTimestampTimezones(
    const std::shared_ptr<arrow::ArrayData>& data,
    const std::shared_ptr<arrow::DataType>& read_type) {
    const std::shared_ptr<arrow::DataType>& type = data->type;
    if (type->id() == arrow::Type::TIMESTAMP && read_type->id() == arrow::Type::TIMESTAMP) {
        const auto& stored = checked_cast<const arrow::TimestampType&>(*type);
        const auto& wanted = checked_cast<const arrow::TimestampType&>(*read_type);
        if (stored.unit() != wanted.unit() || stored.timezone().empty() ||
            wanted.timezone().empty() || stored.timezone() == wanted.timezone()) {
            return data;
        }
        std::shared_ptr<arrow::ArrayData> retyped = data->Copy();
        retyped->type = arrow::timestamp(wanted.unit(), wanted.timezone());
        return retyped;
    }
    if (type->id() == arrow::Type::STRUCT && read_type->id() == arrow::Type::STRUCT) {
        const auto& wanted_struct = checked_cast<const arrow::StructType&>(*read_type);
        arrow::FieldVector fields = type->fields();
        std::shared_ptr<arrow::ArrayData> retyped;
        for (int32_t i = 0; i < static_cast<int32_t>(data->child_data.size()); ++i) {
            const int32_t index = wanted_struct.GetFieldIndex(fields[i]->name());
            if (index < 0) {
                continue;
            }
            std::shared_ptr<arrow::ArrayData> child =
                AlignTimestampTimezones(data->child_data[i], wanted_struct.field(index)->type());
            if (child == data->child_data[i]) {
                continue;
            }
            if (retyped == nullptr) {
                retyped = data->Copy();
            }
            retyped->child_data[i] = child;
            fields[i] = fields[i]->WithType(child->type);
        }
        if (retyped == nullptr) {
            return data;
        }
        retyped->type = arrow::struct_(fields);
        return retyped;
    }
    if ((type->id() == arrow::Type::LIST || type->id() == arrow::Type::FIXED_SIZE_LIST) &&
        type->id() == read_type->id() && !data->child_data.empty()) {
        std::shared_ptr<arrow::ArrayData> values =
            AlignTimestampTimezones(data->child_data[0], read_type->field(0)->type());
        if (values == data->child_data[0]) {
            return data;
        }
        std::shared_ptr<arrow::ArrayData> retyped = data->Copy();
        retyped->child_data[0] = values;
        const std::shared_ptr<arrow::Field> value_field = type->field(0)->WithType(values->type);
        retyped->type = type->id() == arrow::Type::LIST
                            ? arrow::list(value_field)
                            : arrow::fixed_size_list(
                                  value_field,
                                  checked_cast<const arrow::FixedSizeListType&>(*type).list_size());
        return retyped;
    }
    return data;
}

}  // namespace

VortexFileBatchReader::VortexFileBatchReader(
    const std::shared_ptr<InputStream>& input, int32_t batch_size,
    std::shared_ptr<VortexInputContext> input_context, VxSessionPtr session,
    VxDataSourcePtr data_source, VxScanPtr scan, const std::shared_ptr<arrow::Schema>& file_schema,
    uint64_t total_rows, const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
    const VortexPredicateOptions& predicate_options)
    : input_(input),
      batch_size_(batch_size),
      input_context_(std::move(input_context)),
      session_(std::move(session)),
      data_source_(std::move(data_source)),
      scan_(std::move(scan)),
      file_schema_(file_schema),
      total_rows_(total_rows),
      arrow_pool_(arrow_pool),
      predicate_options_(predicate_options),
      metrics_(std::make_shared<MetricsImpl>()) {}

Result<std::unique_ptr<VortexFileBatchReader>> VortexFileBatchReader::Create(
    const std::shared_ptr<InputStream>& input, int32_t batch_size,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
    const VortexPredicateOptions& predicate_options, VortexIoCoalesceMode coalesce_mode) {
    if (input == nullptr || arrow_pool == nullptr || batch_size <= 0) {
        return Status::Invalid(
            "Vortex reader requires non-null input and Arrow memory pool, and positive batch size");
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t signed_length, input->Length());
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(signed_length, "Vortex input length"));
    auto length = static_cast<uint64_t>(signed_length);
    auto input_context = std::make_shared<VortexInputContext>(input);

    VxSessionPtr session(vx_paimon_read_session_new(), vx_session_free);
    if (session == nullptr) {
        return Status::IOError("failed to create Vortex session");
    }
    vx_error* error = nullptr;
    const bool local_coalesce = coalesce_mode == VortexIoCoalesceMode::LOCAL;
    // Smaller gaps limit read amplification for explicitly selected low-latency sources.
    constexpr uint64_t kLocalMaxGap = 64ULL << 10;
    constexpr uint64_t kLocalMaxSize = 4ULL << 20;
    VxDataSourcePtr data_source(
        local_coalesce
            ? vx_data_source_new_callback_with_coalesce(
                  session.get(), VortexInputContext::MakeCallbacks(input_context), length,
                  kLocalMaxGap, kLocalMaxSize, &error)
            : vx_data_source_new_callback(
                  session.get(), VortexInputContext::MakeCallbacks(input_context), length, &error),
        vx_data_source_free);
    if (data_source == nullptr) {
        return VortexCallbackError("open Vortex callback data source", error,
                                   input_context->GetCallbackStatus());
    }

    // The data source dtype is available without consuming a scan.
    // NOTE(vortex 0.75): vx_data_source_dtype returns a BORROWED pointer (no refcount bump), so it
    // must NOT be freed here: freeing decrements the DType Arc and segfaults later in the scan.
    // Upstream >=0.77 returns an owned clone that MUST be freed with vx_dtype_free.
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
        VxScanPtr(nullptr, vx_scan_free), file_schema, total_rows, arrow_pool, predicate_options));
}

VortexFileBatchReader::~VortexFileBatchReader() {
    CloseInternal();
}

Result<bool> VortexFileBatchReader::OpenNextPartitionStream() {
    if (has_selection_ && selection_row_ids_.empty()) {
        return false;
    }
    if (scan_ == nullptr) {
        vx_error* error = nullptr;
        // Scan in storage order: Vortex defaults to ordered=false and may emit chunks out of order,
        // which would misalign the physical row positions NextBatch assigns.
        vx_scan_options options{};
        options.ordered = true;
        // Missing fields are null-filled by AlignToReadSchema and cannot be selected here.
        // The scan clones the projection, so these expressions need only outlive scan creation.
        VxExpressionPtr root(nullptr, vx_expression_free);
        VxExpressionPtr projection(nullptr, vx_expression_free);
        std::vector<std::string> names;
        std::vector<const char*> name_ptrs;
        if (read_schema_ != nullptr) {
            names.reserve(read_schema_->num_fields());
            // Native merge requires distinct fields; output alignment restores repeated columns.
            std::vector<uint8_t> selected_columns(file_schema_->num_fields(), 0);
            for (const std::shared_ptr<arrow::Field>& field : read_schema_->fields()) {
                const int32_t field_index = file_schema_->GetFieldIndex(field->name());
                if (field_index >= 0 && selected_columns[field_index] == 0) {
                    names.push_back(field->name());
                    selected_columns[field_index] = 1;
                }
            }
            if (!names.empty()) {
                name_ptrs.reserve(names.size());
                for (const std::string& name : names) {
                    name_ptrs.push_back(name.c_str());
                }
                root = VxExpressionPtr(vx_expression_root(), vx_expression_free);
                projection = VxExpressionPtr(
                    vx_expression_select(name_ptrs.data(), name_ptrs.size(), root.get()),
                    vx_expression_free);
                options.projection = projection.get();
            }
        }
        // VX_SELECTION_INCLUDE_RANGE maps to IncludeByIndex, not a contiguous range.
        // The scan copies the individual indices from selection_row_ids_.
        if (has_selection_) {
            options.selection.idx = selection_row_ids_.data();
            options.selection.idx_len = selection_row_ids_.size();
            options.selection.include = VX_SELECTION_INCLUDE_RANGE;
        }
        // Filtering drops rows; include_row_idx preserves their original physical positions.
        // The scan clones the filter, so it can be freed after scan creation.
        VxExpressionPtr filter(nullptr, vx_expression_free);
        // The hidden index must not collide with a file field or a requested missing field.
        const bool has_row_idx_column =
            file_schema_->GetFieldIndex(kRowIdxColumn) >= 0 ||
            (read_schema_ != nullptr && !read_schema_->GetAllFieldIndices(kRowIdxColumn).empty());
        if (!has_row_idx_column) {
            filter = VortexPredicateConverter::Convert(predicate_filter_, file_schema_,
                                                       predicate_options_);
        }
        if (filter != nullptr) {
            options.filter = filter.get();
            options.include_row_idx = true;
            has_filter_ = true;
        }
        VxScanPtr scan(
            vx_data_source_scan(data_source_.get(), &options, /*estimate=*/nullptr, &error),
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
    // Import batches with the scan's actual output schema (projected columns, standard types).
    ::ArrowSchema ffi_schema = {};
    if (current_stream_.get_schema(&current_stream_, &ffi_schema) != 0) {
        Status callback_status = input_context_->GetCallbackStatus();
        ReleaseStream();
        if (!callback_status.ok()) {
            return callback_status.WithMessage("read Vortex stream schema", ": ",
                                               callback_status.message());
        }
        return Status::IOError("Vortex Arrow stream get_schema failed");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(current_stream_schema_, arrow::ImportSchema(&ffi_schema));
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
            // get_last_error returns a pointer owned by the stream; the Arrow C stream contract
            // invalidates it once the stream is released, so copy it before ReleaseStream().
            const char* message = current_stream_.get_last_error(&current_stream_);
            std::string message_copy = message == nullptr ? "unknown" : message;
            // A read failure surfaces here as an opaque stream error, so prefer the status the IO
            // callback stashed.
            Status callback_status = input_context_->GetCallbackStatus();
            ReleaseStream();
            if (!callback_status.ok()) {
                return callback_status.WithMessage("read Vortex batch", ": ",
                                                   callback_status.message());
            }
            return Status::IOError("Vortex Arrow stream error: ", message_copy);
        }
        if (ffi_array.release == nullptr) {
            ReleaseStream();  // Partition exhausted; try the next one.
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ImportArray(&ffi_array, arrow::struct_(current_stream_schema_->fields())));
        if (array->length() == 0) {
            continue;  // Skip empty batches.
        }
        return array;
    }
}

Result<std::shared_ptr<arrow::Array>> VortexFileBatchReader::AlignToReadSchema(
    const std::shared_ptr<arrow::Array>& array) const {
    if (read_schema_ == nullptr) {
        return array;
    }
    std::shared_ptr<arrow::DataType> read_type = arrow::struct_(read_schema_->fields());
    return NestedProjectionUtils::AlignArrayToReadType(
        arrow::MakeArray(AlignTimestampTimezones(array->data(), read_type)), read_type,
        arrow_pool_.get());
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
    if (has_filter_) {
        // Capture physical row indices before AlignToReadSchema removes the hidden column.
        const auto& struct_type = checked_cast<const arrow::StructType&>(*normalized_array->type());
        const int32_t row_idx_pos = struct_type.num_fields() - 1;
        if (row_idx_pos < 0 || struct_type.field(row_idx_pos)->name() != kRowIdxColumn) {
            return Status::Invalid("Vortex scan did not append the _row_idx column");
        }
        previous_batch_row_idx_ =
            checked_cast<const arrow::StructArray&>(*normalized_array).field(row_idx_pos);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> projected_array,
                           AlignToReadSchema(normalized_array));

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
    if (closed_) {
        return Status::Invalid("Vortex reader is closed");
    }
    if (read_schema == nullptr) {
        return Status::Invalid("Vortex read schema is nullptr");
    }
    // Keep selected physical row ids to map output positions after selection pushdown.
    std::vector<uint64_t> selection_row_ids;
    if (selection_bitmap) {
        selection_row_ids.reserve(static_cast<size_t>(selection_bitmap->Cardinality()));
        for (auto iter = selection_bitmap->Begin(); iter != selection_bitmap->End(); ++iter) {
            const auto row_id = static_cast<uint64_t>(*iter);
            if (row_id >= total_rows_) {
                return Status::Invalid(
                    fmt::format("Vortex selected row {} is out of range {}", row_id, total_rows_));
            }
            selection_row_ids.push_back(row_id);
        }
    }
    // Leave the active scan and its row mapping unchanged if validation or import fails.
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> imported_read_schema,
                                      arrow::ImportSchema(read_schema));
    // Recreate the single-use scan on the next read with the new projection and predicates.
    read_schema_ = std::move(imported_read_schema);
    predicate_filter_ = std::dynamic_pointer_cast<PredicateFilter>(predicate);
    has_selection_ = selection_bitmap.has_value();
    selection_row_ids_ = std::move(selection_row_ids);
    ReleaseStream();
    current_batch_.reset();
    current_batch_offset_ = 0;
    scan_.reset();
    rows_emitted_ = 0;
    previous_first_row_ = std::numeric_limits<uint64_t>::max();
    previous_batch_row_count_ = 0;
    has_filter_ = false;
    previous_batch_row_idx_.reset();
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
    // The hidden column accounts for both filtering and selection; emitted offsets do not.
    if (has_filter_) {
        if (previous_batch_row_idx_ == nullptr ||
            batch_row_id >= static_cast<uint64_t>(previous_batch_row_idx_->length())) {
            return Status::Invalid(
                fmt::format("Vortex filter row index unavailable for row {}", batch_row_id));
        }
        return checked_cast<const arrow::UInt64Array&>(*previous_batch_row_idx_)
            .Value(static_cast<int64_t>(batch_row_id));
    }
    const uint64_t emitted_offset = previous_first_row_ + batch_row_id;
    // An ordered selection emits rows in the same order as selection_row_ids_.
    if (has_selection_) {
        if (emitted_offset >= selection_row_ids_.size()) {
            return Status::Invalid(fmt::format("emitted row offset {} exceeds selection size {}",
                                               emitted_offset, selection_row_ids_.size()));
        }
        return selection_row_ids_[emitted_offset];
    }
    return emitted_offset;
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
        ArrowArrayStreamRelease(&current_stream_);
        current_stream_ = ::ArrowArrayStream{};
        current_stream_schema_.reset();
        stream_active_ = false;
    }
}

void VortexFileBatchReader::CloseInternal() {
    if (closed_) {
        return;
    }
    // Release in dependency order: the Arrow stream needs the session alive, and the scan borrows
    // the data source, which reads through the input context.
    ReleaseStream();
    current_batch_.reset();
    scan_.reset();
    data_source_.reset();
    session_.reset();
    input_context_.reset();
    input_.reset();
    closed_ = true;
}

}  // namespace paimon::vortex
