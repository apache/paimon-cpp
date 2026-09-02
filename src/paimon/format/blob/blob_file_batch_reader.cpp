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

#include "paimon/format/blob/blob_file_batch_reader.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <numeric>
#include <set>
#include <string>

#include "arrow/api.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/bridge.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/endian.h"
#include "arrow/util/ubsan.h"
#include "arrow/util/utf8.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/io/offset_input_stream.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/delta_varint_compressor.h"
#include "paimon/common/utils/stream_utils.h"
#include "paimon/data/blob.h"

namespace paimon::blob {
namespace {

constexpr int32_t kMapBlobMagicNumber = 0x4D424342;
constexpr int8_t kMapBlobVersion = 1;
constexpr int32_t kMapBlobHeaderLength = 9;
constexpr int32_t kMapBlobIndexLengthsSize = 8;
constexpr int32_t kMapBlobMinPayloadLength = kMapBlobHeaderLength + kMapBlobIndexLengthsSize;

template <typename T>
T ReadLittleEndian(const uint8_t* data) {
    return arrow::bit_util::FromLittleEndian(arrow::util::SafeLoadAs<T>(data));
}

Result<int32_t> GetMapBlobFixedKeyLength(const std::shared_ptr<arrow::DataType>& key_type) {
    switch (key_type->id()) {
        case arrow::Type::BOOL:
        case arrow::Type::INT8:
            return 1;
        case arrow::Type::INT16:
            return 2;
        case arrow::Type::INT32:
        case arrow::Type::DATE32:
        case arrow::Type::TIME32:
            return 4;
        case arrow::Type::INT64:
            return 8;
        case arrow::Type::DECIMAL128: {
            const auto& decimal_type = static_cast<const arrow::Decimal128Type&>(*key_type);
            return decimal_type.precision() <= 18 ? 8 : -1;
        }
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return -1;
        default:
            return Status::Invalid(
                fmt::format("unsupported MAP<..., BLOB> key type: {}", key_type->ToString()));
    }
}

Status AppendMapBlobKey(const std::shared_ptr<arrow::DataType>& key_type, const uint8_t* data,
                        int32_t length, arrow::ArrayBuilder* builder) {
    switch (key_type->id()) {
        case arrow::Type::BOOL: {
            if (data[0] != 0 && data[0] != 1) {
                return Status::Invalid("invalid MAP<..., BLOB> boolean key");
            }
            return ToPaimonStatus(
                checked_cast<arrow::BooleanBuilder*>(builder)->Append(data[0] == 1));
        }
        case arrow::Type::INT8:
            return ToPaimonStatus(
                checked_cast<arrow::Int8Builder*>(builder)->Append(static_cast<int8_t>(data[0])));
        case arrow::Type::INT16:
            return ToPaimonStatus(checked_cast<arrow::Int16Builder*>(builder)->Append(
                ReadLittleEndian<int16_t>(data)));
        case arrow::Type::INT32:
            return ToPaimonStatus(checked_cast<arrow::Int32Builder*>(builder)->Append(
                ReadLittleEndian<int32_t>(data)));
        case arrow::Type::INT64:
            return ToPaimonStatus(checked_cast<arrow::Int64Builder*>(builder)->Append(
                ReadLittleEndian<int64_t>(data)));
        case arrow::Type::DATE32:
            return ToPaimonStatus(checked_cast<arrow::Date32Builder*>(builder)->Append(
                ReadLittleEndian<int32_t>(data)));
        case arrow::Type::TIME32:
            return ToPaimonStatus(checked_cast<arrow::Time32Builder*>(builder)->Append(
                ReadLittleEndian<int32_t>(data)));
        case arrow::Type::STRING:
            if (!arrow::util::ValidateUTF8(data, length)) {
                return Status::Invalid("invalid UTF-8 in MAP<STRING, BLOB> key");
            }
            return ToPaimonStatus(
                checked_cast<arrow::StringBuilder*>(builder)->Append(data, length));
        case arrow::Type::BINARY:
            return ToPaimonStatus(
                checked_cast<arrow::BinaryBuilder*>(builder)->Append(data, length));
        case arrow::Type::DECIMAL128: {
            const auto& decimal_type = static_cast<const arrow::Decimal128Type&>(*key_type);
            arrow::Decimal128 value;
            if (decimal_type.precision() <= 18) {
                value = arrow::Decimal128(ReadLittleEndian<int64_t>(data));
            } else {
                // Java BigInteger.toByteArray() uses the shortest big-endian two's-complement
                // representation. This makes byte-wise duplicate detection equivalent to
                // decoded decimal equality.
                if (length <= 0 || (length > 1 && ((data[0] == 0x00 && (data[1] & 0x80) == 0) ||
                                                   (data[0] == 0xFF && (data[1] & 0x80) != 0)))) {
                    return Status::Invalid("invalid MAP<..., BLOB> non-canonical decimal key");
                }
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(value,
                                                  arrow::Decimal128::FromBigEndian(data, length));
            }
            if (!value.FitsInPrecision(decimal_type.precision())) {
                return Status::Invalid("MAP<..., BLOB> decimal key exceeds declared precision");
            }
            return ToPaimonStatus(checked_cast<arrow::Decimal128Builder*>(builder)->Append(value));
        }
        default:
            return Status::Invalid(
                fmt::format("unsupported MAP<..., BLOB> key type: {}", key_type->ToString()));
    }
}

}  // namespace

Result<std::unique_ptr<BlobFileBatchReader>> BlobFileBatchReader::Create(
    const std::shared_ptr<InputStream>& input_stream, int32_t batch_size, bool blob_as_descriptor,
    bool emit_placeholder_sentinel, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (input_stream == nullptr) {
        return Status::Invalid("blob file batch reader create failed: input stream is nullptr");
    }
    if (batch_size <= 0) {
        return Status::Invalid(fmt::format(
            "blob file batch reader create failed: read batch size '{}' should be larger than zero",
            batch_size));
    }

    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, input_stream->Length());
    PAIMON_RETURN_NOT_OK(
        input_stream->Seek(file_size - BlobDefs::kBlobFileFooterLength, FS_SEEK_SET));
    int8_t footer[BlobDefs::kBlobFileFooterLength];
    PAIMON_ASSIGN_OR_RAISE(
        int64_t actual_size,
        input_stream->Read(reinterpret_cast<char*>(footer), BlobDefs::kBlobFileFooterLength));
    if (actual_size != BlobDefs::kBlobFileFooterLength) {
        return Status::Invalid(
            fmt::format("actual read size {} not match with expect footer length {}", actual_size,
                        BlobDefs::kBlobFileFooterLength));
    }
    int8_t version = footer[4];
    if (version != BlobDefs::kFileVersion) {
        return Status::Invalid(fmt::format(
            "create blob format reader failed. unsupported blob file version: {}", version));
    }
    int32_t index_length = GetIndexLength(footer, 0);
    PAIMON_RETURN_NOT_OK(input_stream->Seek(
        file_size - BlobDefs::kBlobFileFooterLength - index_length, FS_SEEK_SET));
    std::vector<char> index_bytes(index_length, '\0');
    PAIMON_ASSIGN_OR_RAISE(actual_size, input_stream->Read(index_bytes.data(), index_length));
    if (actual_size != index_length) {
        return Status::Invalid(
            fmt::format("actual read size {} not match with expect index length {}", actual_size,
                        index_length));
    }
    PAIMON_ASSIGN_OR_RAISE(const std::vector<int64_t> blob_lengths,
                           DeltaVarintCompressor::Decompress(index_bytes));

    std::vector<int64_t> blob_offsets;
    blob_offsets.reserve(blob_lengths.size());
    int64_t offset = 0;
    for (const auto& blob_length : blob_lengths) {
        blob_offsets.push_back(offset);
        // null (-1) and placeholder (-2) entries occupy no file space
        if (blob_length >= 0) {
            offset += blob_length;
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::string file_path, input_stream->GetUri());
    auto reader = std::unique_ptr<BlobFileBatchReader>(
        new BlobFileBatchReader(input_stream, file_path, blob_lengths, blob_offsets, batch_size,
                                blob_as_descriptor, emit_placeholder_sentinel, pool, arrow_pool));
    return reader;
}

BlobFileBatchReader::BlobFileBatchReader(
    const std::shared_ptr<InputStream>& input_stream, const std::string& file_path,
    const std::vector<int64_t>& blob_lengths, const std::vector<int64_t>& blob_offsets,
    int32_t batch_size, bool blob_as_descriptor, bool emit_placeholder_sentinel,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : input_stream_(input_stream),
      file_path_(file_path),
      all_blob_lengths_(blob_lengths),
      all_blob_offsets_(blob_offsets),
      target_blob_lengths_(blob_lengths),
      target_blob_offsets_(blob_offsets),
      batch_size_(batch_size),
      blob_as_descriptor_(blob_as_descriptor),
      emit_placeholder_sentinel_(emit_placeholder_sentinel),
      pool_(pool),
      arrow_pool_(arrow_pool),
      metrics_(std::make_shared<MetricsImpl>()) {
    target_blob_row_indexes_.resize(target_blob_lengths_.size());
    std::iota(target_blob_row_indexes_.begin(), target_blob_row_indexes_.end(), 0);
}

Status BlobFileBatchReader::SetReadSchema(::ArrowSchema* read_schema,
                                          const std::shared_ptr<Predicate>& predicate,
                                          const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!read_schema) {
        return Status::Invalid("SetReadSchema failed: read schema cannot be nullptr");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(read_schema));
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid(
            fmt::format("read schema field number {} is not 1", arrow_schema->num_fields()));
    }
    std::shared_ptr<arrow::Field> read_field = arrow_schema->field(0);
    if (!BlobUtils::IsBlobField(read_field) && !BlobUtils::IsMapBlobField(read_field)) {
        return Status::Invalid(fmt::format("field {} is not BLOB", read_field->ToString()));
    }
    if (BlobUtils::IsMapBlobField(read_field)) {
        const auto& map_type = static_cast<const arrow::MapType&>(*read_field->type());
        PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] int32_t fixed_key_length,
                               GetMapBlobFixedKeyLength(map_type.key_type()));
    }
    if (selection_bitmap != std::nullopt) {
        int32_t cardinality = selection_bitmap->Cardinality();
        std::vector<int64_t> new_lengths(cardinality);
        std::vector<int64_t> new_offsets(cardinality);
        std::vector<uint64_t> new_row_indexes(cardinality);

        PAIMON_ASSIGN_OR_RAISE(uint64_t total_rows, GetNumberOfRows());
        RoaringBitmap32::Iterator iterator(*selection_bitmap);
        for (int32_t i = 0; i < cardinality; i++) {
            int32_t row_index = *iterator;
            if (static_cast<size_t>(row_index) >= total_rows) {
                return Status::Invalid(fmt::format(
                    "row index {} is out of bound of total row number {}", row_index, total_rows));
            }
            ++iterator;
            new_lengths[i] = all_blob_lengths_[row_index];
            new_offsets[i] = all_blob_offsets_[row_index];
            new_row_indexes[i] = row_index;
        }
        target_blob_lengths_ = new_lengths;
        target_blob_offsets_ = new_offsets;
        target_blob_row_indexes_ = new_row_indexes;
    }
    target_type_ = arrow::struct_({read_field});
    current_pos_ = 0;
    previous_batch_start_pos_ = std::numeric_limits<size_t>::max();
    previous_batch_row_count_ = 0;
    return Status::OK();
}

Result<std::shared_ptr<arrow::Buffer>> BlobFileBatchReader::NextBlobOffsets(
    int32_t rows_to_read) const {
    arrow::TypedBufferBuilder<int64_t> buffer_builder(arrow_pool_.get());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(buffer_builder.Reserve(rows_to_read + 1));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(buffer_builder.Append(0));
    int64_t data_length = 0;
    for (int32_t k = 0; k < rows_to_read; ++k) {
        data_length += GetTargetOutputLength(current_pos_ + k);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(buffer_builder.Append(data_length));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> offset_buffer,
                                      buffer_builder.Finish());
    return offset_buffer;
}

Result<std::shared_ptr<arrow::Buffer>> BlobFileBatchReader::NextBlobContents(
    int32_t rows_to_read) const {
    int64_t total_length = 0;
    for (int32_t k = 0; k < rows_to_read; ++k) {
        total_length += GetTargetOutputLength(current_pos_ + k);
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> data_buffer,
                                      arrow::AllocateBuffer(total_length, arrow_pool_.get()));
    uint8_t* buffer = data_buffer->mutable_data();
    for (int32_t k = 0; k < rows_to_read; ++k) {
        const size_t i = current_pos_ + k;
        if (IsTargetNull(i)) {
            continue;
        }
        if (IsTargetPlaceholder(i)) {
            // a placeholder entry has no data bytes in the file; emit the sentinel for the
            // data-evolution blob fallback merge to identify it
            memcpy(buffer, BlobDefs::kPlaceholderSentinel, BlobDefs::kPlaceholderSentinelLength);
            buffer += BlobDefs::kPlaceholderSentinelLength;
            continue;
        }
        int64_t offset = GetTargetContentOffset(i);
        int64_t length = GetTargetContentLength(i);
        PAIMON_RETURN_NOT_OK(ReadBlobContentAt(offset, length, buffer));
        buffer += length;
    }
    return data_buffer;
}

Result<std::shared_ptr<arrow::Buffer>> BlobFileBatchReader::BuildNullBitmap(
    int32_t rows_to_read) const {
    bool has_null = false;
    for (int32_t k = 0; k < rows_to_read; ++k) {
        if (IsTargetNull(current_pos_ + k)) {
            has_null = true;
            break;
        }
    }
    if (!has_null) {
        return std::shared_ptr<arrow::Buffer>();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> null_bitmap,
                                      arrow::AllocateBitmap(rows_to_read, arrow_pool_.get()));
    // Initialize all bits to 1 (valid), then clear bits for null rows
    memset(null_bitmap->mutable_data(), 0xFF, null_bitmap->size());
    for (int32_t k = 0; k < rows_to_read; ++k) {
        if (IsTargetNull(current_pos_ + k)) {
            arrow::bit_util::ClearBit(null_bitmap->mutable_data(), k);
        }
    }
    return null_bitmap;
}

Result<std::shared_ptr<arrow::Array>> BlobFileBatchReader::BuildContentArray(
    int32_t rows_to_read) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> value_offsets,
                           NextBlobOffsets(rows_to_read));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> data, NextBlobContents(rows_to_read));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> child_null_bitmap,
                           BuildNullBitmap(rows_to_read));

    auto large_binary_array = std::make_shared<arrow::LargeBinaryArray>(rows_to_read, value_offsets,
                                                                        data, child_null_bitmap);
    std::vector<std::shared_ptr<arrow::ArrayData>> child_data;
    child_data.emplace_back(large_binary_array->data());
    std::shared_ptr<arrow::ArrayData> struct_array_data =
        arrow::ArrayData::Make(target_type_, large_binary_array->length(), {nullptr}, child_data);
    return std::make_shared<arrow::StructArray>(struct_array_data);
}

Result<std::shared_ptr<arrow::Array>> BlobFileBatchReader::BuildMapBlobArray(
    int32_t rows_to_read) const {
    const auto& struct_type = static_cast<const arrow::StructType&>(*target_type_);
    const std::shared_ptr<arrow::Field>& map_field = struct_type.field(0);
    auto map_type = checked_pointer_cast<arrow::MapType>(map_field->type());
    const std::shared_ptr<arrow::DataType>& key_type = map_type->key_type();
    if (key_type->id() == arrow::Type::STRING) {
        arrow::util::InitializeUTF8();
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t fixed_key_length, GetMapBlobFixedKeyLength(key_type));

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> key_builder_unique,
                                      arrow::MakeBuilder(key_type, arrow_pool_.get()));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> item_builder_unique,
                                      arrow::MakeBuilder(map_type->item_type(), arrow_pool_.get()));
    std::shared_ptr<arrow::ArrayBuilder> key_builder(std::move(key_builder_unique));
    std::shared_ptr<arrow::ArrayBuilder> item_builder(std::move(item_builder_unique));
    if (!item_builder || !item_builder->type() ||
        item_builder->type()->id() != arrow::Type::LARGE_BINARY) {
        return Status::Invalid("cast MAP<..., BLOB> item builder to large binary builder failed");
    }
    auto* blob_builder = checked_cast<arrow::LargeBinaryBuilder*>(item_builder.get());
    arrow::MapBuilder map_builder(arrow_pool_.get(), key_builder, item_builder, map_type);

    for (int32_t k = 0; k < rows_to_read; ++k) {
        const size_t row_index = current_pos_ + k;
        if (IsTargetNull(row_index)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder.AppendNull());
            continue;
        }
        if (IsTargetPlaceholder(row_index)) {
            // Duplicate map keys cannot occur in a valid Paimon map, so two empty/default keys
            // with null values form an unambiguous, Arrow-valid internal sentinel.
            PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder.Append());
            PAIMON_RETURN_NOT_OK_FROM_ARROW(key_builder->AppendEmptyValues(2));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(blob_builder->AppendNulls(2));
            continue;
        }
        if (target_blob_lengths_[row_index] < 0) {
            return Status::Invalid(fmt::format("unsupported MAP<..., BLOB> record length: {}",
                                               target_blob_lengths_[row_index]));
        }

        const int64_t payload_offset = GetTargetContentOffset(row_index);
        const int64_t payload_length = GetTargetContentLength(row_index);
        if (payload_length < kMapBlobMinPayloadLength) {
            return Status::Invalid(
                fmt::format("invalid MAP<..., BLOB> payload length: {}", payload_length));
        }

        std::array<uint8_t, kMapBlobHeaderLength> header;
        PAIMON_RETURN_NOT_OK(ReadBlobContentAt(payload_offset, header.size(), header.data()));
        const auto magic_number = ReadLittleEndian<int32_t>(header.data());
        if (magic_number != kMapBlobMagicNumber) {
            return Status::Invalid(
                fmt::format("invalid MAP<..., BLOB> payload magic number: {}", magic_number));
        }
        const auto version = static_cast<int8_t>(header[4]);
        if (version != kMapBlobVersion) {
            return Status::NotImplemented(
                fmt::format("unsupported MAP<..., BLOB> payload version: {}", version));
        }
        const auto entry_count = ReadLittleEndian<int32_t>(header.data() + 5);
        if (entry_count < 0) {
            return Status::Invalid(
                fmt::format("invalid MAP<..., BLOB> entry count: {}", entry_count));
        }

        const int64_t index_lengths_offset =
            payload_offset + payload_length - kMapBlobIndexLengthsSize;
        std::array<uint8_t, kMapBlobIndexLengthsSize> index_lengths;
        PAIMON_RETURN_NOT_OK(
            ReadBlobContentAt(index_lengths_offset, index_lengths.size(), index_lengths.data()));
        const auto key_index_length = ReadLittleEndian<int32_t>(index_lengths.data());
        const auto value_index_length =
            ReadLittleEndian<int32_t>(index_lengths.data() + sizeof(int32_t));
        const int64_t maximum_indexes_length = payload_length - kMapBlobMinPayloadLength;
        if (key_index_length < 0 || key_index_length > maximum_indexes_length) {
            return Status::Invalid(
                fmt::format("invalid MAP<..., BLOB> key index length: {}", key_index_length));
        }
        if (value_index_length < 0 || value_index_length > maximum_indexes_length) {
            return Status::Invalid(
                fmt::format("invalid MAP<..., BLOB> value index length: {}", value_index_length));
        }
        if (static_cast<int64_t>(key_index_length) + value_index_length > maximum_indexes_length) {
            return Status::Invalid("MAP<..., BLOB> indexes exceed the payload length");
        }
        if (entry_count > key_index_length || entry_count > value_index_length) {
            return Status::Invalid("MAP<..., BLOB> entry count exceeds index length");
        }

        const int64_t value_index_offset = index_lengths_offset - value_index_length;
        const int64_t key_index_offset = value_index_offset - key_index_length;
        std::vector<char> key_index_bytes(key_index_length);
        std::vector<char> value_index_bytes(value_index_length);
        PAIMON_RETURN_NOT_OK(ReadBlobContentAt(key_index_offset, key_index_length,
                                               reinterpret_cast<uint8_t*>(key_index_bytes.data())));
        PAIMON_RETURN_NOT_OK(
            ReadBlobContentAt(value_index_offset, value_index_length,
                              reinterpret_cast<uint8_t*>(value_index_bytes.data())));
        PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> key_lengths,
                               DeltaVarintCompressor::Decompress(key_index_bytes));
        PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> value_lengths,
                               DeltaVarintCompressor::Decompress(value_index_bytes));
        if (key_lengths.size() != static_cast<size_t>(entry_count)) {
            return Status::Invalid("MAP<..., BLOB> entry count does not match key index length");
        }
        if (value_lengths.size() != static_cast<size_t>(entry_count)) {
            return Status::Invalid("MAP<..., BLOB> entry count does not match value index length");
        }

        const int64_t data_offset = payload_offset + kMapBlobHeaderLength;
        const int64_t data_length = key_index_offset - data_offset;
        int64_t key_data_length = 0;
        for (int64_t key_length : key_lengths) {
            if (key_length < 0) {
                return Status::Invalid("MAP<..., BLOB> keys cannot be null");
            }
            if (key_length > std::numeric_limits<int32_t>::max()) {
                return Status::Invalid(
                    fmt::format("MAP<..., BLOB> key is too large: {}", key_length));
            }
            if (fixed_key_length >= 0 && key_length != fixed_key_length) {
                return Status::Invalid(
                    fmt::format("invalid MAP<..., BLOB> fixed-width key length: {}", key_length));
            }
            if (key_length > data_length - key_data_length) {
                return Status::Invalid("MAP<..., BLOB> key lengths exceed the payload data length");
            }
            key_data_length += key_length;
        }

        const int64_t maximum_value_data_length = data_length - key_data_length;
        int64_t value_data_length = 0;
        for (int64_t value_length : value_lengths) {
            if (value_length == BlobDefs::kNullBinLength) {
                continue;
            }
            if (value_length < 0) {
                return Status::Invalid(
                    fmt::format("invalid MAP<..., BLOB> value length: {}", value_length));
            }
            if (!blob_as_descriptor_ && value_length > std::numeric_limits<int32_t>::max()) {
                return Status::Invalid(
                    fmt::format("MAP<..., BLOB> inline value is too large: {}", value_length));
            }
            if (value_length > maximum_value_data_length - value_data_length) {
                return Status::Invalid(
                    "MAP<..., BLOB> value lengths exceed the payload data length");
            }
            value_data_length += value_length;
        }
        if (value_data_length != maximum_value_data_length) {
            return Status::Invalid(
                "MAP<..., BLOB> key/value lengths do not match the payload data length");
        }

        PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder.Append());
        int64_t key_offset = data_offset;
        std::set<std::string> serialized_keys;
        for (int32_t entry = 0; entry < entry_count; ++entry) {
            const auto key_length = static_cast<int32_t>(key_lengths[entry]);
            std::vector<uint8_t> key_bytes(key_length);
            PAIMON_RETURN_NOT_OK(ReadBlobContentAt(key_offset, key_length, key_bytes.data()));
            if (!serialized_keys.emplace(key_bytes.begin(), key_bytes.end()).second) {
                return Status::Invalid("invalid MAP<..., BLOB> payload: duplicate key");
            }
            PAIMON_RETURN_NOT_OK(
                AppendMapBlobKey(key_type, key_bytes.data(), key_length, key_builder.get()));
            key_offset += key_length;
        }

        int64_t value_offset = data_offset + key_data_length;
        for (int64_t value_length : value_lengths) {
            if (value_length == BlobDefs::kNullBinLength) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(blob_builder->AppendNull());
                continue;
            }
            if (blob_as_descriptor_) {
                PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Blob> blob,
                                       Blob::FromPath(file_path_, value_offset, value_length));
                PAIMON_UNIQUE_PTR<Bytes> descriptor = blob->ToDescriptor(pool_);
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    blob_builder->Append(descriptor->data(), descriptor->size()));
            } else {
                std::vector<uint8_t> value_bytes(static_cast<size_t>(value_length));
                PAIMON_RETURN_NOT_OK(
                    ReadBlobContentAt(value_offset, value_length, value_bytes.data()));
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    blob_builder->Append(value_bytes.data(), value_length));
            }
            value_offset += value_length;
        }
    }

    std::shared_ptr<arrow::MapArray> built_map_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(map_builder.Finish(&built_map_array));
    auto map_array = std::make_shared<arrow::MapArray>(
        map_type, built_map_array->length(), built_map_array->value_offsets(),
        built_map_array->keys(), built_map_array->items(), built_map_array->null_bitmap(),
        built_map_array->null_count(), built_map_array->offset());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> struct_array,
                                      arrow::StructArray::Make({map_array}, {map_field}));
    return struct_array;
}

Result<std::shared_ptr<arrow::Array>> BlobFileBatchReader::BuildTargetArray(
    int32_t rows_to_read) const {
    const auto& struct_type = static_cast<const arrow::StructType&>(*target_type_);
    if (struct_type.field(0)->type()->id() == arrow::Type::MAP) {
        return BuildMapBlobArray(rows_to_read);
    }
    if (!blob_as_descriptor_) {
        return BuildContentArray(rows_to_read);
    }
    // For descriptor mode, build using StructBuilder to handle nulls properly
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<arrow::ArrayBuilder> array_builder,
                                      arrow::MakeBuilder(target_type_, arrow_pool_.get()));
    if (!array_builder || !array_builder->type() ||
        array_builder->type()->id() != arrow::Type::STRUCT) {
        return Status::Invalid("cast to struct builder failed");
    }
    auto* builder = checked_cast<arrow::StructBuilder*>(array_builder.get());
    auto* field_builder_base = builder->field_builder(0);
    if (!field_builder_base || !field_builder_base->type() ||
        field_builder_base->type()->id() != arrow::Type::LARGE_BINARY) {
        return Status::Invalid("cast to large binary builder failed");
    }
    auto* field_builder = checked_cast<arrow::LargeBinaryBuilder*>(field_builder_base);
    for (int32_t k = 0; k < rows_to_read; ++k) {
        const size_t i = current_pos_ + k;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append());
        if (IsTargetNull(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(field_builder->AppendNull());
        } else if (IsTargetPlaceholder(i)) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(field_builder->Append(
                BlobDefs::kPlaceholderSentinel, BlobDefs::kPlaceholderSentinelLength));
        } else {
            int64_t offset = GetTargetContentOffset(i);
            int64_t length = GetTargetContentLength(i);
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Blob> blob,
                                   Blob::FromPath(file_path_, offset, length));
            auto descriptor = blob->ToDescriptor(pool_);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                field_builder->Append(descriptor->data(), descriptor->size()));
        }
    }
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Finish(&array));
    return array;
}

Result<BatchReader::ReadBatch> BlobFileBatchReader::NextBatch() {
    if (closed_) {
        return Status::Invalid("blob file batch reader is closed");
    }
    if (target_type_ == nullptr) {
        return Status::Invalid("target type is nullptr, call SetReadSchema first");
    }
    if (current_pos_ >= target_blob_lengths_.size()) {
        previous_batch_start_pos_ = target_blob_lengths_.size();
        previous_batch_row_count_ = 0;
        return BatchReader::MakeEofBatch();
    }
    int32_t left_rows = target_blob_lengths_.size() - current_pos_;
    int32_t rows_to_read = std::min(left_rows, batch_size_);
    if (!emit_placeholder_sentinel_) {
        for (int32_t k = 0; k < rows_to_read; ++k) {
            if (IsTargetPlaceholder(current_pos_ + k)) {
                return Status::Invalid(fmt::format(
                    "blob file {} contains a placeholder entry (bin_length {}) written by a "
                    "data-evolution partial update; it can only be resolved by the data-evolution "
                    "blob fallback read path",
                    file_path_, BlobDefs::kPlaceholderBinLength));
            }
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> blob_array,
                           BuildTargetArray(rows_to_read));
    std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*blob_array, c_array.get(), c_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), arrow_pool_));
    previous_batch_start_pos_ = current_pos_;
    current_pos_ += rows_to_read;
    previous_batch_row_count_ = c_array->length;
    return make_pair(std::move(c_array), std::move(c_schema));
}

Status BlobFileBatchReader::ReadBlobContentAt(const int64_t offset, const int64_t length,
                                              uint8_t* content) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OffsetInputStream> offset_input_stream,
                           OffsetInputStream::Create(input_stream_, length, offset));
    return StreamUtils::ReadAsyncFully(std::move(offset_input_stream),
                                       reinterpret_cast<char*>(content));
}

int32_t BlobFileBatchReader::GetIndexLength(const int8_t* bytes, int32_t offset) {
    return static_cast<int32_t>(
        (static_cast<uint32_t>(static_cast<uint8_t>(bytes[offset + 3])) << 24) |
        (static_cast<uint32_t>(static_cast<uint8_t>(bytes[offset + 2])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(bytes[offset + 1])) << 8) |
        static_cast<uint32_t>(static_cast<uint8_t>(bytes[offset])));
}

// Note: blob file has no self-describing schema, use read schema instead.
Result<std::unique_ptr<::ArrowSchema>> BlobFileBatchReader::GetFileSchema() const {
    return Status::NotImplemented("blob file has no self-describing file schema");
}

}  // namespace paimon::blob
