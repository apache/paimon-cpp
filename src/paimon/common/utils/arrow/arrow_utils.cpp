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

#include "paimon/common/utils/arrow/arrow_utils.h"

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon {

namespace {

bool HasNonZeroOffset(const std::shared_ptr<arrow::ArrayData>& data) {
    if (data->offset != 0) {
        return true;
    }
    for (const auto& child : data->child_data) {
        if (HasNonZeroOffset(child)) {
            return true;
        }
    }
    return false;
}

Result<std::shared_ptr<arrow::ArrayData>> CopyToZeroOffset(
    const std::shared_ptr<arrow::ArrayData>& data, arrow::MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> copied,
                                      arrow::Concatenate({arrow::MakeArray(data)}, pool));
    return copied->data();
}

Result<std::shared_ptr<arrow::Buffer>> RebaseBitmap(const arrow::ArrayData& data,
                                                    const std::shared_ptr<arrow::Buffer>& bitmap,
                                                    arrow::MemoryPool* pool) {
    if (data.offset % 8 == 0) {
        return arrow::SliceBuffer(bitmap, data.offset / 8,
                                  arrow::bit_util::BytesForBits(data.length));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Buffer> copied,
        arrow::internal::CopyBitmap(pool, bitmap->data(), data.offset, data.length));
    return copied;
}

Result<std::shared_ptr<arrow::Buffer>> RebaseValidityBitmap(const arrow::ArrayData& data,
                                                            arrow::MemoryPool* pool) {
    const std::shared_ptr<arrow::Buffer>& bitmap = data.buffers[0];
    if (bitmap == nullptr || data.null_count.load() == 0) {
        return std::shared_ptr<arrow::Buffer>();
    }
    return RebaseBitmap(data, bitmap, pool);
}

struct RebasedOffsets {
    std::shared_ptr<arrow::Buffer> buffer;
    int64_t first_value = 0;
    int64_t last_value = 0;
};

template <typename OffsetType>
Result<RebasedOffsets> RebaseOffsets(const arrow::ArrayData& data, arrow::MemoryPool* pool) {
    const OffsetType* offsets = data.GetValues<OffsetType>(1);
    const OffsetType base = offsets[0];
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::unique_ptr<arrow::Buffer> buffer,
        arrow::AllocateBuffer((data.length + 1) * static_cast<int64_t>(sizeof(OffsetType)), pool));
    auto* rebased = reinterpret_cast<OffsetType*>(buffer->mutable_data());
    for (int64_t i = 0; i <= data.length; i++) {
        rebased[i] = offsets[i] - base;
    }
    return RebasedOffsets{std::shared_ptr<arrow::Buffer>(std::move(buffer)), base,
                          offsets[data.length]};
}

Result<std::shared_ptr<arrow::ArrayData>> RebaseToZeroOffset(
    const std::shared_ptr<arrow::ArrayData>& data, arrow::MemoryPool* pool);

/// Rebases a boolean array, whose values are a bitmap rather than byte addressable.
Result<std::shared_ptr<arrow::ArrayData>> RebaseBoolean(
    const std::shared_ptr<arrow::ArrayData>& data, arrow::MemoryPool* pool) {
    if (data->buffers.size() != 2 || data->buffers[1] == nullptr) {
        return CopyToZeroOffset(data, pool);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> validity,
                           RebaseValidityBitmap(*data, pool));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> values,
                           RebaseBitmap(*data, data->buffers[1], pool));
    std::shared_ptr<arrow::ArrayData> rebased =
        arrow::ArrayData::Make(data->type, data->length, data->null_count.load(), /*offset=*/0);
    rebased->buffers = {std::move(validity), std::move(values)};
    return rebased;
}

/// Rebases the {validity, offsets, values} layout of binary-like arrays.
template <typename OffsetType>
Result<std::shared_ptr<arrow::ArrayData>> RebaseBinaryLike(
    const std::shared_ptr<arrow::ArrayData>& data, arrow::MemoryPool* pool) {
    if (data->buffers.size() != 3 || data->buffers[1] == nullptr || data->buffers[2] == nullptr) {
        return CopyToZeroOffset(data, pool);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> validity,
                           RebaseValidityBitmap(*data, pool));
    PAIMON_ASSIGN_OR_RAISE(RebasedOffsets offsets, RebaseOffsets<OffsetType>(*data, pool));
    std::shared_ptr<arrow::ArrayData> rebased =
        arrow::ArrayData::Make(data->type, data->length, data->null_count.load(), /*offset=*/0);
    rebased->buffers = {std::move(validity), std::move(offsets.buffer),
                        arrow::SliceBuffer(data->buffers[2], offsets.first_value,
                                           offsets.last_value - offsets.first_value)};
    return rebased;
}

/// Rebases the {validity, offsets} plus single child layout of list, large list and map arrays.
template <typename OffsetType>
Result<std::shared_ptr<arrow::ArrayData>> RebaseListLike(
    const std::shared_ptr<arrow::ArrayData>& data, arrow::MemoryPool* pool) {
    if (data->buffers.size() != 2 || data->buffers[1] == nullptr || data->child_data.size() != 1) {
        return CopyToZeroOffset(data, pool);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> validity,
                           RebaseValidityBitmap(*data, pool));
    PAIMON_ASSIGN_OR_RAISE(RebasedOffsets offsets, RebaseOffsets<OffsetType>(*data, pool));
    // A contiguous slice of the parent always spans a contiguous range of the child.
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::ArrayData> child_slice,
        data->child_data[0]->SliceSafe(offsets.first_value,
                                       offsets.last_value - offsets.first_value));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ArrayData> child,
                           RebaseToZeroOffset(child_slice, pool));
    std::shared_ptr<arrow::ArrayData> rebased =
        arrow::ArrayData::Make(data->type, data->length, data->null_count.load(), /*offset=*/0);
    rebased->buffers = {std::move(validity), std::move(offsets.buffer)};
    rebased->child_data = {std::move(child)};
    return rebased;
}

/// Rebases a struct array, whose slices keep full length children.
Result<std::shared_ptr<arrow::ArrayData>> RebaseStruct(
    const std::shared_ptr<arrow::ArrayData>& data, arrow::MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> validity,
                           RebaseValidityBitmap(*data, pool));
    std::shared_ptr<arrow::ArrayData> rebased =
        arrow::ArrayData::Make(data->type, data->length, data->null_count.load(), /*offset=*/0);
    rebased->buffers = {std::move(validity)};
    rebased->child_data.reserve(data->child_data.size());
    for (const auto& child : data->child_data) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::ArrayData> child_slice,
                                          child->SliceSafe(data->offset, data->length));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ArrayData> rebased_child,
                               RebaseToZeroOffset(child_slice, pool));
        rebased->child_data.push_back(std::move(rebased_child));
    }
    return rebased;
}

/// Slices the single value buffer of a fixed width array. Returns nullptr when the layout is not
/// a plain byte addressable fixed width one.
Result<std::shared_ptr<arrow::ArrayData>> RebaseFixedWidth(
    const std::shared_ptr<arrow::ArrayData>& data, arrow::MemoryPool* pool) {
    // arrow::is_fixed_width() also covers dictionary types, whose dictionary is not in child_data.
    if (!arrow::is_fixed_width(data->type->id()) || data->buffers.size() != 2 ||
        data->buffers[1] == nullptr || !data->child_data.empty() || data->dictionary != nullptr) {
        return std::shared_ptr<arrow::ArrayData>();
    }
    const int32_t bit_width =
        arrow::internal::checked_cast<const arrow::FixedWidthType&>(*data->type).bit_width();
    if (bit_width <= 0 || bit_width % 8 != 0) {
        return std::shared_ptr<arrow::ArrayData>();
    }
    const int64_t byte_width = bit_width / 8;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> validity,
                           RebaseValidityBitmap(*data, pool));
    std::shared_ptr<arrow::ArrayData> rebased =
        arrow::ArrayData::Make(data->type, data->length, data->null_count.load(), /*offset=*/0);
    rebased->buffers = {
        std::move(validity),
        arrow::SliceBuffer(data->buffers[1], data->offset * byte_width, data->length * byte_width)};
    return rebased;
}

/// Returns an ArrayData describing the same rows as `data` with a zero offset at every level.
/// Buffers are sliced rather than copied wherever the layout allows it, so the cost is
/// proportional to the number of rows instead of the number of value bytes.
Result<std::shared_ptr<arrow::ArrayData>> RebaseToZeroOffset(
    const std::shared_ptr<arrow::ArrayData>& data, arrow::MemoryPool* pool) {
    if (!HasNonZeroOffset(data)) {
        return data;
    }
    // An empty array may not carry the buffers the layouts below slice.
    if (data->length == 0 || data->buffers.empty()) {
        return CopyToZeroOffset(data, pool);
    }

    switch (data->type->id()) {
        case arrow::Type::BOOL:
            return RebaseBoolean(data, pool);
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return RebaseBinaryLike<int32_t>(data, pool);
        case arrow::Type::LARGE_STRING:
        case arrow::Type::LARGE_BINARY:
            return RebaseBinaryLike<int64_t>(data, pool);
        case arrow::Type::LIST:
        case arrow::Type::MAP:
            return RebaseListLike<int32_t>(data, pool);
        case arrow::Type::LARGE_LIST:
            return RebaseListLike<int64_t>(data, pool);
        case arrow::Type::STRUCT:
            return RebaseStruct(data, pool);
        case arrow::Type::DICTIONARY:
            return CopyToZeroOffset(data, pool);
        default:
            break;
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ArrayData> rebased, RebaseFixedWidth(data, pool));
    if (rebased != nullptr) {
        return rebased;
    }
    return CopyToZeroOffset(data, pool);
}

}  // namespace

const char* ArrowUtils::kArrowSchemaMetadataKey = "ARROW:schema";

Result<std::shared_ptr<arrow::Schema>> ArrowUtils::DataTypeToSchema(
    const std::shared_ptr<arrow::DataType>& data_type) {
    if (data_type->id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            fmt::format("Expected struct data type, actual data type: {}", data_type->ToString()));
    }
    const auto& struct_type = std::static_pointer_cast<arrow::StructType>(data_type);
    return std::make_shared<arrow::Schema>(struct_type->fields());
}

Result<std::vector<int32_t>> ArrowUtils::CreateProjection(
    const std::shared_ptr<arrow::Schema>& src_schema, const arrow::FieldVector& target_fields) {
    std::vector<int32_t> target_to_src_mapping;
    target_to_src_mapping.reserve(target_fields.size());
    for (const auto& field : target_fields) {
        auto src_field_idx = src_schema->GetFieldIndex(field->name());
        if (src_field_idx < 0) {
            return Status::Invalid(
                fmt::format("Field '{}' not found or duplicate in src schema", field->name()));
        }
        target_to_src_mapping.push_back(src_field_idx);
    }
    return target_to_src_mapping;
}

Status ArrowUtils::CheckNullabilityMatch(const std::shared_ptr<arrow::Schema>& schema,
                                         const std::shared_ptr<arrow::Array>& data) {
    auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(data);
    if (struct_array->num_fields() != schema->num_fields()) {
        return Status::Invalid(fmt::format(
            "CheckNullabilityMatch failed, data field count {} mismatch schema field count {}",
            struct_array->num_fields(), schema->num_fields()));
    }
    for (int32_t i = 0; i < schema->num_fields(); i++) {
        PAIMON_RETURN_NOT_OK(InnerCheckNullabilityMatch(schema->field(i), struct_array->field(i)));
    }
    return Status::OK();
}

void ArrowUtils::TraverseArray(const std::shared_ptr<arrow::Array>& array) {
    arrow::Type::type type = array->type()->id();
    switch (type) {
        case arrow::Type::type::DICTIONARY: {
            auto* dict_array = arrow::internal::checked_cast<arrow::DictionaryArray*>(array.get());
            [[maybe_unused]] auto dict = dict_array->dictionary();
            return;
        }
        case arrow::Type::type::STRUCT: {
            auto* struct_array = arrow::internal::checked_cast<arrow::StructArray*>(array.get());
            for (const auto& field : struct_array->fields()) {
                TraverseArray(field);
            }
            return;
        }
        case arrow::Type::type::MAP: {
            auto* map_array = arrow::internal::checked_cast<arrow::MapArray*>(array.get());
            TraverseArray(map_array->keys());
            TraverseArray(map_array->items());
            return;
        }
        case arrow::Type::type::LIST: {
            auto* list_array = arrow::internal::checked_cast<arrow::ListArray*>(array.get());
            TraverseArray(list_array->values());
            return;
        }
        default:
            return;
    }
}

bool ArrowUtils::EqualsIgnoreNullable(const std::shared_ptr<arrow::DataType>& type,
                                      const std::shared_ptr<arrow::DataType>& other_type) {
    if (type->id() != other_type->id() || type->num_fields() != other_type->num_fields()) {
        return false;
    }
    for (int32_t i = 0; i < type->num_fields(); ++i) {
        const auto& field = type->field(i);
        const auto& other_field = other_type->field(i);
        if (field->name() != other_field->name()) {
            return false;
        }
        if (!EqualsIgnoreNullable(field->type(), other_field->type())) {
            return false;
        }
    }
    return true;
}

Status ArrowUtils::InnerCheckNullabilityMatch(const std::shared_ptr<arrow::Field>& field,
                                              const std::shared_ptr<arrow::Array>& data) {
    if (PAIMON_UNLIKELY(!field->nullable() && data->null_count() != 0)) {
        return Status::Invalid(fmt::format(
            "CheckNullabilityMatch failed, field {} not nullable while data have null value",
            field->name()));
    }
    auto type = field->type();
    if (type->id() == arrow::Type::STRUCT) {
        auto struct_type = arrow::internal::checked_pointer_cast<arrow::StructType>(field->type());
        auto struct_array = arrow::internal::checked_pointer_cast<arrow::StructArray>(data);
        for (int32_t i = 0; i < struct_type->num_fields(); ++i) {
            PAIMON_RETURN_NOT_OK(
                InnerCheckNullabilityMatch(struct_type->field(i), struct_array->field(i)));
        }
    } else if (type->id() == arrow::Type::LIST) {
        auto list_type = arrow::internal::checked_pointer_cast<arrow::ListType>(field->type());
        auto list_array = arrow::internal::checked_pointer_cast<arrow::ListArray>(data);
        PAIMON_RETURN_NOT_OK(
            InnerCheckNullabilityMatch(list_type->value_field(), list_array->values()));
    } else if (type->id() == arrow::Type::MAP) {
        auto map_type = arrow::internal::checked_pointer_cast<arrow::MapType>(field->type());
        auto map_array = arrow::internal::checked_pointer_cast<arrow::MapArray>(data);
        PAIMON_RETURN_NOT_OK(InnerCheckNullabilityMatch(map_type->key_field(), map_array->keys()));
        PAIMON_RETURN_NOT_OK(
            InnerCheckNullabilityMatch(map_type->item_field(), map_array->items()));
    }
    return Status::OK();
}

Result<std::shared_ptr<arrow::StructArray>> ArrowUtils::RemoveFieldFromStructArray(
    const std::shared_ptr<arrow::StructArray>& struct_array, const std::string& field_name) {
    auto struct_type = std::static_pointer_cast<arrow::StructType>(struct_array->type());
    int32_t field_idx = struct_type->GetFieldIndex(field_name);
    if (field_idx == -1) {
        return struct_array;
    }
    std::vector<std::shared_ptr<arrow::Array>> new_arrays;
    std::vector<std::shared_ptr<arrow::Field>> new_fields;
    for (int32_t i = 0; i < struct_type->num_fields(); ++i) {
        if (i != field_idx) {
            new_arrays.emplace_back(struct_array->field(i));
            new_fields.emplace_back(struct_type->field(i));
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> array,
        arrow::StructArray::Make(new_arrays, new_fields, struct_array->null_bitmap(),
                                 struct_array->null_count(), struct_array->offset()));
    return array;
}

Result<std::shared_ptr<arrow::RecordBatch>> ArrowUtils::NormalizeRecordBatchOffsets(
    const std::shared_ptr<arrow::RecordBatch>& record_batch, arrow::MemoryPool* pool) {
    arrow::ArrayVector normalized_columns;
    for (int32_t i = 0; i < record_batch->num_columns(); ++i) {
        const std::shared_ptr<arrow::Array>& column = record_batch->column(i);
        if (!HasNonZeroOffset(column->data())) {
            continue;
        }
        if (normalized_columns.empty()) {
            normalized_columns = record_batch->columns();
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ArrayData> normalized_data,
                               RebaseToZeroOffset(column->data(), pool));
        normalized_columns[i] = arrow::MakeArray(normalized_data);
    }
    if (normalized_columns.empty()) {
        return record_batch;
    }
    return arrow::RecordBatch::Make(record_batch->schema(), record_batch->num_rows(),
                                    std::move(normalized_columns));
}

Result<arrow::Compression::type> ArrowUtils::GetCompressionType(const std::string& compression) {
    std::string normalized = StringUtils::ToLowerCase(compression);
    if (normalized.empty() || normalized == "none") {
        normalized = "uncompressed";
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(arrow::Compression::type compression_type,
                                      arrow::util::Codec::GetCompressionType(normalized));
    return compression_type;
}

}  // namespace paimon
