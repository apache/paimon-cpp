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

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/c/abi.h"
#include "arrow/compute/cast.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/compression.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/arrow/vector_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/casting/casting_utils.h"

namespace paimon {

namespace {

// Whether `type` is a dictionary this can carry across the C data interface unchanged. The index
// width is part of the test because nothing in a layout reveals it; see
// ArrowUtils::IsDictionaryLayoutRecoverableValueType().
bool IsDictionaryLayoutRecoverable(const arrow::DataType& type) {
    if (type.id() != arrow::Type::DICTIONARY) {
        return false;
    }
    const auto& dictionary_type = checked_cast<const arrow::DictionaryType&>(type);
    return dictionary_type.index_type()->id() == arrow::Type::INT32 &&
           ArrowUtils::IsDictionaryLayoutRecoverableValueType(*dictionary_type.value_type());
}

// Whether `type` is or contains a dictionary at any depth.
bool HasDictionary(const arrow::DataType& type) {
    if (type.id() == arrow::Type::DICTIONARY) {
        return true;
    }
    for (const std::shared_ptr<arrow::Field>& field : type.fields()) {
        if (HasDictionary(*field->type())) {
            return true;
        }
    }
    return false;
}

// Whether any descendant of `array` carries a dictionary that `type` does not declare. `array`
// itself is not examined; its caller has already handled the top level.
bool HasUndeclaredDictionaryChild(const std::shared_ptr<arrow::DataType>& type,
                                  const ::ArrowArray* array) {
    if (array == nullptr || array->n_children != type->num_fields()) {
        return false;
    }
    for (int64_t i = 0; i < array->n_children; ++i) {
        const ::ArrowArray* child = array->children[i];
        if (child == nullptr) {
            continue;
        }
        const std::shared_ptr<arrow::DataType>& child_type =
            type->field(static_cast<int>(i))->type();
        if (child->dictionary != nullptr && child_type->id() != arrow::Type::DICTIONARY) {
            return true;
        }
        if (HasUndeclaredDictionaryChild(child_type, child)) {
            return true;
        }
    }
    return false;
}

bool NeedsNormalization(const std::shared_ptr<arrow::ArrayData>& data) {
    if (data->offset != 0) {
        return true;
    }
    if (data->type->id() == arrow::Type::STRUCT) {
        for (const auto& child : data->child_data) {
            // StructArray::Slice(0, length) shortens only the parent ArrayData. Its children may
            // still describe the full unsliced arrays, which cannot be imported as a RecordBatch.
            if (child->length != data->length) {
                return true;
            }
        }
    }
    for (const auto& child : data->child_data) {
        if (NeedsNormalization(child)) {
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
    const auto* offsets = data.GetValues<OffsetType>(1);
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

/// Rebases a fixed size list array, whose child holds `list_size` values per row.
Result<std::shared_ptr<arrow::ArrayData>> RebaseFixedSizeList(
    const std::shared_ptr<arrow::ArrayData>& data, arrow::MemoryPool* pool) {
    if (data->child_data.size() != 1) {
        return CopyToZeroOffset(data, pool);
    }
    const int64_t list_size =
        checked_cast<const arrow::FixedSizeListType&>(*data->type).list_size();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> validity,
                           RebaseValidityBitmap(*data, pool));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::ArrayData> child_slice,
        data->child_data[0]->SliceSafe(data->offset * list_size, data->length * list_size));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ArrayData> child,
                           RebaseToZeroOffset(child_slice, pool));
    std::shared_ptr<arrow::ArrayData> rebased =
        arrow::ArrayData::Make(data->type, data->length, data->null_count.load(), /*offset=*/0);
    rebased->buffers = {std::move(validity)};
    rebased->child_data = {std::move(child)};
    return rebased;
}

/// Rebases a struct array, whose slices keep full length children.
Result<std::shared_ptr<arrow::ArrayData>> RebaseStruct(
    const std::shared_ptr<arrow::ArrayData>& data, arrow::MemoryPool* pool) {
    if (data->buffers.empty()) {
        return CopyToZeroOffset(data, pool);
    }
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
    const int32_t bit_width = checked_cast<const arrow::FixedWidthType&>(*data->type).bit_width();
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
    if (!NeedsNormalization(data)) {
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
        case arrow::Type::FIXED_SIZE_LIST:
            return RebaseFixedSizeList(data, pool);
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

/// The eight bytes one byte of a bitmap expands to: `kExpandedBytes[b].bytes[i]` is bit `i` of `b`.
/// Naming the bytes one at a time rather than building a word keeps the expansion independent of
/// the host's byte order.
struct ExpandedByte {
    char bytes[8];
};

constexpr ExpandedByte ExpandBitmapByte(uint8_t bits) {
    ExpandedByte expanded{};
    for (int i = 0; i < 8; i++) {
        expanded.bytes[i] = static_cast<char>((bits >> i) & uint8_t{1});
    }
    return expanded;
}

constexpr std::array<ExpandedByte, 256> MakeExpandedBytes() {
    std::array<ExpandedByte, 256> table{};
    for (int bits = 0; bits < 256; bits++) {
        table[static_cast<size_t>(bits)] = ExpandBitmapByte(static_cast<uint8_t>(bits));
    }
    return table;
}

constexpr std::array<ExpandedByte, 256> kExpandedBytes = MakeExpandedBytes();

}  // namespace

const char* ArrowUtils::kArrowSchemaMetadataKey = "ARROW:schema";

Result<std::shared_ptr<arrow::Schema>> ArrowUtils::DataTypeToSchema(
    const std::shared_ptr<arrow::DataType>& data_type) {
    if (!data_type || data_type->id() != arrow::Type::STRUCT) {
        return Status::Invalid(fmt::format("Expected struct data type, actual data type: {}",
                                           data_type ? data_type->ToString() : "null"));
    }
    const auto& struct_type = checked_pointer_cast<arrow::StructType>(data_type);
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
    if (!schema || !data || data->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("CheckNullabilityMatch requires a schema and a struct array");
    }
    auto struct_array = checked_pointer_cast<arrow::StructArray>(data);
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
            auto* dict_array = checked_cast<arrow::DictionaryArray*>(array.get());
            [[maybe_unused]] auto dict = dict_array->dictionary();
            return;
        }
        case arrow::Type::type::STRUCT: {
            auto* struct_array = checked_cast<arrow::StructArray*>(array.get());
            for (const auto& field : struct_array->fields()) {
                TraverseArray(field);
            }
            return;
        }
        case arrow::Type::type::MAP: {
            auto* map_array = checked_cast<arrow::MapArray*>(array.get());
            TraverseArray(map_array->keys());
            TraverseArray(map_array->items());
            return;
        }
        case arrow::Type::type::LIST: {
            auto* list_array = checked_cast<arrow::ListArray*>(array.get());
            TraverseArray(list_array->values());
            return;
        }
        case arrow::Type::type::FIXED_SIZE_LIST: {
            auto* vector_array = checked_cast<arrow::FixedSizeListArray*>(array.get());
            TraverseArray(vector_array->values());
            return;
        }
        default:
            return;
    }
}

uint64_t ArrowUtils::GetArrayMemoryUsage(const std::shared_ptr<arrow::ArrayData>& data) {
    uint64_t result = 0;
    for (const std::shared_ptr<arrow::Buffer>& buffer : data->buffers) {
        if (buffer) {
            result += static_cast<uint64_t>(buffer->size());
        }
    }
    for (const std::shared_ptr<arrow::ArrayData>& child : data->child_data) {
        result += GetArrayMemoryUsage(child);
    }
    if (data->dictionary) {
        result += GetArrayMemoryUsage(data->dictionary);
    }
    return result;
}

std::vector<char> ArrowUtils::UnpackBooleansToBytes(const arrow::BooleanArray& array, bool negate) {
    const int64_t length = array.length();
    std::vector<char> is_valid(static_cast<size_t>(length));
    if (length == 0) {
        return is_valid;
    }

    const std::shared_ptr<arrow::ArrayData>& data = array.data();
    // `BooleanArray::Value(i)` and `Array::IsValid(i)` both read their bitmap at bit `i + offset`
    // from the start of the buffer, so one bit index serves the value and the validity alike.
    const int64_t bit_offset = data->offset;
    const uint8_t* values = data->GetValuesSafe<uint8_t>(/*i=*/1, /*absolute_offset=*/0);
    const uint8_t* validity = array.null_bitmap_data();
    // Without a validity bitmap `Array::IsValid` falls back to `null_count != length`, which makes
    // every row valid unless the array is null throughout. One that is has no row left to unpack,
    // and is the only array whose value bitmap is not read.
    if (validity == nullptr && data->null_count == length) {
        return is_valid;
    }

    auto unpack_row = [&](int64_t row) -> char {
        const int64_t bit = bit_offset + row;
        const bool holds_value = validity == nullptr || arrow::bit_util::GetBit(validity, bit);
        return static_cast<char>(holds_value && (arrow::bit_util::GetBit(values, bit) != negate));
    };

    char* out = is_valid.data();
    int64_t row = 0;
    // The rows sharing the leading partial byte, which the byte at a time body cannot take whole.
    // None when the bit offset is already on a byte boundary, as it is for the batch a kernel has
    // just written.
    const int64_t head = std::min<int64_t>(length, (8 - (bit_offset & 7)) & 7);
    for (; row < head; row++) {
        out[row] = unpack_row(row);
    }
    for (; row + 8 <= length; row += 8) {
        const int64_t byte = (bit_offset + row) >> 3;
        // Every row holds a value when there is no validity bitmap, so all eight bits pass.
        const uint8_t valid_byte = validity == nullptr ? uint8_t{0xFF} : validity[byte];
        const uint8_t value_byte = values[byte];
        const uint8_t bits = negate ? static_cast<uint8_t>(~value_byte & valid_byte)
                                    : static_cast<uint8_t>(value_byte & valid_byte);
        std::memcpy(out + row, kExpandedBytes[bits].bytes, sizeof(ExpandedByte));
    }
    for (; row < length; row++) {
        out[row] = unpack_row(row);
    }
    return is_valid;
}

bool ArrowUtils::EqualsIgnoreNullable(const std::shared_ptr<arrow::DataType>& type,
                                      const std::shared_ptr<arrow::DataType>& other_type) {
    if (type->id() != other_type->id() || type->num_fields() != other_type->num_fields()) {
        return false;
    }
    if (type->id() == arrow::Type::FIXED_SIZE_LIST) {
        const auto& vector_type = checked_cast<const arrow::FixedSizeListType&>(*type);
        const auto& other_vector_type = checked_cast<const arrow::FixedSizeListType&>(*other_type);
        if (vector_type.list_size() != other_vector_type.list_size()) {
            return false;
        }
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
        auto struct_type = checked_pointer_cast<arrow::StructType>(field->type());
        auto struct_array = checked_pointer_cast<arrow::StructArray>(data);
        for (int32_t i = 0; i < struct_type->num_fields(); ++i) {
            PAIMON_RETURN_NOT_OK(
                InnerCheckNullabilityMatch(struct_type->field(i), struct_array->field(i)));
        }
    } else if (type->id() == arrow::Type::LIST) {
        auto list_type = checked_pointer_cast<arrow::ListType>(field->type());
        auto list_array = checked_pointer_cast<arrow::ListArray>(data);
        PAIMON_RETURN_NOT_OK(
            InnerCheckNullabilityMatch(list_type->value_field(), list_array->values()));
    } else if (type->id() == arrow::Type::FIXED_SIZE_LIST) {
        Status status = VectorUtils::ValidateVectorElements(*data);
        if (!status.ok()) {
            return Status::Invalid(
                fmt::format("VECTOR field {} is invalid: {}", field->name(), status.message()));
        }
    } else if (type->id() == arrow::Type::MAP) {
        auto map_type = checked_pointer_cast<arrow::MapType>(field->type());
        auto map_array = checked_pointer_cast<arrow::MapArray>(data);
        PAIMON_RETURN_NOT_OK(InnerCheckNullabilityMatch(map_type->key_field(), map_array->keys()));
        PAIMON_RETURN_NOT_OK(
            InnerCheckNullabilityMatch(map_type->item_field(), map_array->items()));
    }
    return Status::OK();
}

Result<std::shared_ptr<arrow::StructArray>> ArrowUtils::RemoveFieldFromStructArray(
    const std::shared_ptr<arrow::StructArray>& struct_array, const std::string& field_name) {
    auto struct_type = checked_pointer_cast<arrow::StructType>(struct_array->type());
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
        if (!NeedsNormalization(column->data())) {
            continue;
        }
        if (normalized_columns.empty()) {
            normalized_columns = record_batch->columns();
        }
        PAIMON_ASSIGN_OR_RAISE(normalized_columns[i], NormalizeArrayOffsets(column, pool));
    }
    if (normalized_columns.empty()) {
        return record_batch;
    }
    return arrow::RecordBatch::Make(record_batch->schema(), record_batch->num_rows(),
                                    std::move(normalized_columns));
}

Result<std::shared_ptr<arrow::Array>> ArrowUtils::NormalizeArrayOffsets(
    const std::shared_ptr<arrow::Array>& array, arrow::MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ArrayData> normalized_data,
                           RebaseToZeroOffset(array->data(), pool));
    return arrow::MakeArray(normalized_data);
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

// `is_binary_like()` is BINARY and STRING and nothing else. `LARGE_STRING` is left out even though
// it is binary-like: the ORC reader widens strings to `dictionary(int64(), large_utf8())` under
// lazy decoding, and a layout reports neither index nor offset width, so reading that back as
// `int32` indices over `int32` offsets would silently reinterpret both buffers instead of failing.
//
// This narrows what may be carried; it cannot verify what was. See
// ResolveDictionaryStructTypeFromLayout() for where the index width becomes a caller contract.
bool ArrowUtils::IsDictionaryLayoutRecoverableValueType(const arrow::DataType& type) {
    return arrow::is_binary_like(type.id());
}

// Why the header calls the `int32` index width a contract rather than a check: the value-type
// check rejects `dictionary(int64(), large_utf8())`, the shape the ORC reader produces, but
// nothing here can tell `dictionary(int32(), utf8())` apart from `dictionary(int64(), utf8())`,
// and the second would be read as the first.
//
// So the contract binds the producer, not the callers: `ParquetFormatWriter::ResolveBatchSchema`
// and `DataFileWriterBase::AddFileIndexBatch` see only the layout.
// `AppendOnlyFileStoreWrite::CompactRewrite` is today's only production path that can hand over a
// batch whose dictionaries the schema does not declare, and it honours the contract by running
// FlattenUnresolvableDictionaries() first. Closing the hole instead of narrowing it needs the real
// `ArrowSchema` to reach the writer, which `FormatWriter::AddBatch(ArrowArray*)` drops.
Result<std::shared_ptr<arrow::DataType>> ArrowUtils::ResolveDictionaryStructTypeFromLayout(
    const std::shared_ptr<arrow::DataType>& logical_type, const ::ArrowArray* batch) {
    if (batch == nullptr || logical_type->id() != arrow::Type::STRUCT ||
        batch->n_children != logical_type->num_fields()) {
        // Leave the mismatch to the import, which reports it with its own diagnostics.
        return logical_type;
    }
    arrow::FieldVector fields;
    bool has_dictionary = false;
    for (int32_t i = 0; i < logical_type->num_fields(); ++i) {
        const std::shared_ptr<arrow::Field>& field = logical_type->field(i);
        const ::ArrowArray* child = batch->children[i];
        if (child == nullptr || child->dictionary == nullptr) {
            if (HasUndeclaredDictionaryChild(field->type(), child)) {
                return Status::NotImplemented(fmt::format(
                    "column '{}' is dictionary-encoded below its top level, which the Arrow "
                    "import cannot describe without the producer's schema",
                    field->name()));
            }
            fields.push_back(field);
            continue;
        }
        if (field->type()->id() == arrow::Type::DICTIONARY) {
            // The caller already declares the column as a dictionary, so its type describes the
            // batch and nothing has to be recovered from the layout.
            fields.push_back(field);
            continue;
        }
        if (!IsDictionaryLayoutRecoverableValueType(*field->type())) {
            return Status::NotImplemented(fmt::format(
                "dictionary-encoded column '{}' of type {} cannot be resolved from the layout of "
                "an ArrowArray, which pins down neither the index nor the offset width",
                field->name(), field->type()->ToString()));
        }
        has_dictionary = true;
        fields.push_back(field->WithType(arrow::dictionary(arrow::int32(), field->type())));
    }
    if (!has_dictionary) {
        return logical_type;
    }
    return arrow::struct_(fields);
}

Result<std::shared_ptr<arrow::StructArray>> ArrowUtils::FlattenUnresolvableDictionaries(
    const std::shared_ptr<arrow::StructArray>& batch,
    const std::shared_ptr<arrow::DataType>& logical_type, arrow::MemoryPool* pool,
    bool preserve_layout_recoverable_dictionaries) {
    const std::shared_ptr<arrow::DataType>& batch_type = batch->type();
    if (logical_type->id() != arrow::Type::STRUCT || !HasDictionary(*batch_type)) {
        return batch;
    }
    const auto& logical_struct_type = checked_cast<const arrow::StructType&>(*logical_type);
    std::shared_ptr<arrow::ArrayData> data;
    arrow::FieldVector fields = batch_type->fields();
    for (int32_t i = 0; i < batch_type->num_fields(); ++i) {
        std::shared_ptr<arrow::Field> field = fields[i];
        if (!HasDictionary(*field->type())) {
            continue;
        }
        // Surviving the export is not enough when the destination imports against the logical
        // type: an undeclared dictionary child of any shape then fails on the buffer count.
        if (preserve_layout_recoverable_dictionaries &&
            IsDictionaryLayoutRecoverable(*field->type())) {
            continue;
        }
        std::shared_ptr<arrow::Field> logical_field =
            logical_struct_type.GetFieldByName(field->name());
        if (logical_field == nullptr) {
            // Nothing says what this column should decode to, so leave it for the import to
            // report against its own schema.
            continue;
        }
        if (data == nullptr) {
            // Copy once, on the first column that has to be decoded: the parent keeps its offset,
            // length and validity, and only the child data is swapped underneath it.
            data = batch->data()->Copy();
        }
        // Decode the whole child rather than the slice the parent exposes, so the replacement
        // lines up with the offset and length the parent still carries.
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Array> decoded,
            CastingUtils::Cast(arrow::MakeArray(data->child_data[i]), logical_field->type(),
                               arrow::compute::CastOptions::Safe(), pool));
        data->child_data[i] = decoded->data();
        fields[i] = field->WithType(logical_field->type());
    }
    if (data == nullptr) {
        return batch;
    }
    data->type = arrow::struct_(fields);
    return checked_pointer_cast<arrow::StructArray>(arrow::MakeArray(data));
}

}  // namespace paimon
