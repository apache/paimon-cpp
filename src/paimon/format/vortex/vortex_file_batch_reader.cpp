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
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/format/vortex/vortex_predicate_converter.h"
#include "paimon/fs/file_system.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::vortex {

namespace {

// Must match the physical row index column appended by vortex-ffi scan.rs.
constexpr char kRowIdxColumn[] = "_row_idx";

// Validate field references without changing read-side types or metadata. Type conversion
// remains the responsibility of AlignToReadSchema.
Status ValidateReadFieldReferences(const std::shared_ptr<arrow::DataType>& file_type,
                                   const std::shared_ptr<arrow::DataType>& read_type) {
    if (file_type->id() != read_type->id()) {
        return Status::OK();
    }
    switch (read_type->id()) {
        case arrow::Type::STRUCT: {
            PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> projection,
                                   ArrowUtils::CreateProjection(arrow::schema(file_type->fields()),
                                                                read_type->fields()));
            for (int32_t i = 0; i < read_type->num_fields(); ++i) {
                PAIMON_RETURN_NOT_OK(ValidateReadFieldReferences(
                    file_type->field(projection[i])->type(), read_type->field(i)->type()));
            }
            return Status::OK();
        }
        case arrow::Type::LIST:
        case arrow::Type::FIXED_SIZE_LIST:
            // Container field names (for example item/element) need not match.
            return ValidateReadFieldReferences(file_type->field(0)->type(),
                                               read_type->field(0)->type());
        case arrow::Type::MAP: {
            const auto& file_map = checked_cast<const arrow::MapType&>(*file_type);
            const auto& read_map = checked_cast<const arrow::MapType&>(*read_type);
            PAIMON_RETURN_NOT_OK(
                ValidateReadFieldReferences(file_map.key_type(), read_map.key_type()));
            return ValidateReadFieldReferences(file_map.item_type(), read_map.item_type());
        }
        default:
            return Status::OK();
    }
}

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

Result<VortexFileBatchReader::SelectionState> VortexFileBatchReader::SelectionState::Create(
    const std::optional<RoaringBitmap32>& bitmap, uint64_t total_rows) {
    SelectionState state;
    state.has_selection_ = bitmap.has_value();
    state.selected_row_count_ = total_rows;
    if (bitmap) {
        const int32_t cardinality = bitmap->Cardinality();
        if (cardinality < 0 || static_cast<uint64_t>(cardinality) > total_rows) {
            return Status::Invalid("Vortex selection cardinality exceeds the file row count");
        }
        state.selected_row_count_ = static_cast<uint64_t>(cardinality);
        state.excludes_ = state.selected_row_count_ > total_rows / 2;
        state.indices_.reserve(static_cast<size_t>(
            state.excludes_ ? total_rows - state.selected_row_count_ : state.selected_row_count_));
        uint64_t next_row = 0;
        const auto end = bitmap->End();
        for (auto iter = bitmap->Begin(); iter != end; ++iter) {
            const auto row_id = static_cast<uint64_t>(*iter);
            if (row_id >= total_rows) {
                return Status::Invalid(
                    fmt::format("Vortex selected row {} is out of range {}", row_id, total_rows));
            }
            if (state.excludes_) {
                // Enumerate gaps directly instead of first expanding all selected rows.
                for (; next_row < row_id; ++next_row) {
                    state.indices_.push_back(next_row);
                }
                next_row = row_id + 1;
            } else {
                state.indices_.push_back(row_id);
            }
        }
        if (state.excludes_) {
            for (; next_row < total_rows; ++next_row) {
                state.indices_.push_back(next_row);
            }
        }
    }
    return state;
}

void VortexFileBatchReader::SelectionState::ApplyToScan(vx_scan_options* options) const {
    // A dense bitmap sends excluded rows; all-selected uses the default INCLUDE_ALL mode.
    if (has_selection_ && !indices_.empty()) {
        options->selection.idx = indices_.data();
        options->selection.idx_len = indices_.size();
        options->selection.include =
            excludes_ ? VX_SELECTION_EXCLUDE_RANGE : VX_SELECTION_INCLUDE_RANGE;
    }
}

Result<uint64_t> VortexFileBatchReader::SelectionState::GetPhysicalRowId(
    uint64_t selected_rank) const {
    // Ordered scans emit selected rows in physical order, regardless of which side is stored.
    if (has_selection_) {
        if (selected_rank >= selected_row_count_) {
            return Status::Invalid(fmt::format("emitted row offset {} exceeds selection size {}",
                                               selected_rank, selected_row_count_));
        }
        if (!excludes_) {
            return indices_[selected_rank];
        }
        // For excluded row i, excluded[i] - i is the number of selected rows before it.
        // Count exclusions at or before this selected rank without expanding a row mapping.
        size_t begin = 0;
        size_t end = indices_.size();
        while (begin < end) {
            const size_t middle = begin + (end - begin) / 2;
            if (indices_[middle] - middle <= selected_rank) {
                begin = middle + 1;
            } else {
                end = middle;
            }
        }
        return selected_rank + begin;
    }
    return selected_rank;
}

Status VortexFileBatchReader::RowPositionState::CaptureRowIndices(
    const std::shared_ptr<arrow::Array>& array) {
    if (has_filter_) {
        // Capture physical row indices before AlignToReadSchema removes the hidden column.
        const auto& struct_type = checked_cast<const arrow::StructType&>(*array->type());
        const int32_t row_idx_pos = struct_type.num_fields() - 1;
        if (row_idx_pos < 0 || struct_type.field(row_idx_pos)->name() != kRowIdxColumn) {
            return Status::Invalid("Vortex scan did not append the _row_idx column");
        }
        previous_batch_row_idx_ =
            checked_cast<const arrow::StructArray&>(*array).field(row_idx_pos);
    }
    return Status::OK();
}

void VortexFileBatchReader::RowPositionState::RecordBatch(uint64_t row_count) {
    previous_first_row_ = rows_emitted_;
    previous_batch_row_count_ = row_count;
    rows_emitted_ += row_count;
}

Result<uint64_t> VortexFileBatchReader::RowPositionState::GetPhysicalRowId(
    uint64_t batch_row_id, const SelectionState& selection) const {
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
    return selection.GetPhysicalRowId(previous_first_row_ + batch_row_id);
}

VortexFileBatchReader::VortexFileBatchReader(
    const std::shared_ptr<InputStream>& input, int32_t batch_size,
    std::shared_ptr<VortexInputContext> input_context, VxSessionPtr session,
    VxDataSourcePtr data_source, VxScanPtr scan, const std::shared_ptr<arrow::Schema>& file_schema,
    uint64_t total_rows, const VortexPredicateOptions& predicate_options,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
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
    const VortexPredicateOptions& predicate_options,
    const VortexIoCoalesceOptions& coalesce_options,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
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
    VxDataSourcePtr data_source(
        vx_data_source_new_callback_with_coalesce(
            session.get(), VortexInputContext::MakeCallbacks(input_context), length,
            coalesce_options.max_gap, coalesce_options.max_size, &error),
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
        VxScanPtr(nullptr, vx_scan_free), file_schema, total_rows, predicate_options, arrow_pool));
}

VortexFileBatchReader::~VortexFileBatchReader() {
    CloseInternal();
}

Status VortexFileBatchReader::CreateScan() {
    vx_error* error = nullptr;
    // Scan in storage order: Vortex defaults to ordered=false and may emit chunks out of order,
    // which would misalign the physical row positions NextBatch assigns.
    vx_scan_options options{};
    options.ordered = true;
    // The scan clones the projection, so these expressions need only outlive scan creation.
    VxExpressionPtr root(nullptr, vx_expression_free);
    VxExpressionPtr projection(nullptr, vx_expression_free);
    std::vector<const char*> name_ptrs;
    if (read_schema_ != nullptr && read_schema_->num_fields() > 0) {
        name_ptrs.reserve(read_schema_->num_fields());
        // The schema owns these names; Vortex copies them when creating the projection.
        for (const std::shared_ptr<arrow::Field>& field : read_schema_->fields()) {
            name_ptrs.push_back(field->name().c_str());
        }
        root = VxExpressionPtr(vx_expression_root(), vx_expression_free);
        projection =
            VxExpressionPtr(vx_expression_select(name_ptrs.data(), name_ptrs.size(), root.get()),
                            vx_expression_free);
        options.projection = projection.get();
    }
    selection_.ApplyToScan(&options);
    // Filtering drops rows; include_row_idx preserves their original physical positions.
    // The scan clones the filter, so it can be freed after scan creation.
    VxExpressionPtr filter(nullptr, vx_expression_free);
    // SetReadSchema validates requested fields against the file schema, so checking the
    // file alone also protects the hidden index from collisions with requested fields.
    const bool has_row_idx_column = !file_schema_->GetAllFieldIndices(kRowIdxColumn).empty();
    if (!has_row_idx_column) {
        PAIMON_ASSIGN_OR_RAISE(filter, VortexPredicateConverter::Convert(
                                           predicate_filter_, file_schema_, predicate_options_));
    }
    if (filter != nullptr) {
        options.filter = filter.get();
        options.include_row_idx = true;
        row_position_.EnableFilterRowIds();
    }
    VxScanPtr scan(vx_data_source_scan(data_source_.get(), &options, /*estimate=*/nullptr, &error),
                   vx_scan_free);
    if (scan == nullptr) {
        return VortexCallbackError("create Vortex scan", error,
                                   input_context_->GetCallbackStatus());
    }
    scan_ = std::move(scan);
    return Status::OK();
}

Result<bool> VortexFileBatchReader::OpenNextPartitionStream() {
    if (selection_.IsEmpty()) {
        return false;
    }
    if (scan_ == nullptr) {
        PAIMON_RETURN_NOT_OK(CreateScan());
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
    // Arrow stream export consumes the partition (even on error), so release ownership here.
    ::ArrowArrayStream stream{};
    error = nullptr;
    if (vx_partition_scan_arrow_with_batch_size(session_.get(), partition.release(),
                                                static_cast<size_t>(batch_size_), &stream,
                                                &error) != 0) {
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
        row_position_.SetEof();
        return BatchReader::MakeEofBatch();
    }

    const int64_t row_count =
        std::min<int64_t>(batch_size_, current_batch_->length() - current_batch_offset_);
    std::shared_ptr<arrow::Array> sliced_array =
        current_batch_->Slice(current_batch_offset_, row_count);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> normalized_array,
                           ArrowUtils::NormalizeArrayOffsets(sliced_array, arrow_pool_.get()));
    PAIMON_RETURN_NOT_OK(row_position_.CaptureRowIndices(normalized_array));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> projected_array,
                           AlignToReadSchema(normalized_array));

    row_position_.RecordBatch(static_cast<uint64_t>(row_count));
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
    PAIMON_ASSIGN_OR_RAISE(SelectionState selection,
                           SelectionState::Create(selection_bitmap, total_rows_));
    // Leave the active scan and its row mapping unchanged if validation or import fails.
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> imported_read_schema,
                                      arrow::ImportSchema(read_schema));
    PAIMON_RETURN_NOT_OK(
        ArrowSchemaValidator::ValidateNoRedundantFields(imported_read_schema->fields()));
    PAIMON_RETURN_NOT_OK(ValidateReadFieldReferences(
        arrow::struct_(file_schema_->fields()), arrow::struct_(imported_read_schema->fields())));
    // Recreate the single-use scan on the next read with the new projection and predicates.
    read_schema_ = std::move(imported_read_schema);
    predicate_filter_ = std::dynamic_pointer_cast<PredicateFilter>(predicate);
    selection_ = std::move(selection);
    metrics_->SetCounter(VortexMetrics::kSelectionRowCount, selection_.SelectedRowCount());
    metrics_->SetCounter(VortexMetrics::kSelectionBufferRowCount, selection_.IndexCount());
    ReleaseStream();
    current_batch_.reset();
    current_batch_offset_ = 0;
    scan_.reset();
    row_position_.Reset();
    return Status::OK();
}

Result<uint64_t> VortexFileBatchReader::GetPreviousBatchFileRowId(uint64_t batch_row_id) const {
    return row_position_.GetPhysicalRowId(batch_row_id, selection_);
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
