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

#include "paimon/common/file_index/bitmap/bitmap_file_index.h"

#include <utility>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index_meta.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index_meta_v1.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index_meta_v2.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"

namespace paimon {
class MemoryPool;

BitmapFileIndex::BitmapFileIndex(const std::map<std::string, std::string>& options)
    : options_(options) {}

Result<Literal> BitmapFileIndex::ConvertLiteral(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& arrow_type) {
    if (literal.GetType() != FieldType::TIMESTAMP) {
        return literal;
    }
    // convert timestamp literal to long
    if (literal.IsNull()) {
        return Literal(FieldType::BIGINT);
    } else {
        if (!arrow_type || arrow_type->id() != arrow::Type::TIMESTAMP) {
            return Status::Invalid(fmt::format("literal type TIMESTAMP mismatch arrow type {}",
                                               arrow_type ? arrow_type->ToString() : "null"));
        }
        auto ts_type = checked_pointer_cast<arrow::TimestampType>(arrow_type);
        int64_t precision = DateTimeUtils::GetPrecisionFromType(ts_type);
        int64_t value = 0;
        if (precision <= Timestamp::MILLIS_PRECISION) {
            value = literal.GetValue<Timestamp>().GetMillisecond();
        } else {
            value = literal.GetValue<Timestamp>().ToMicrosecond();
        }
        return Literal(value);
    }
}

FieldType BitmapFileIndex::ConvertType(const FieldType& data_type) {
    if (data_type == FieldType::TIMESTAMP) {
        return FieldType::BIGINT;
    }
    return data_type;
}

Result<std::shared_ptr<FileIndexReader>> BitmapFileIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, int32_t start, int32_t length,
    const std::shared_ptr<InputStream>& input_stream,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for BitmapFileIndexReader, supposed to have single "
            "field.");
    }
    auto arrow_type = arrow_schema->field(0)->type();
    PAIMON_ASSIGN_OR_RAISE(FieldType data_type,
                           FieldTypeUtils::ConvertToFieldType(arrow_type->id()));
    return std::make_shared<BitmapFileIndexReader>(arrow_type, data_type, start, length,
                                                   input_stream, pool);
}

Result<std::shared_ptr<FileIndexWriter>> BitmapFileIndex::CreateWriter(
    ::ArrowSchema* c_arrow_schema, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for BitmapFileIndexWriter, supposed to have single "
            "field.");
    }
    return BitmapFileIndexWriter::Create(arrow_schema->field(0), options_, pool);
}

Result<std::shared_ptr<BitmapFileIndexWriter>> BitmapFileIndexWriter::Create(
    const std::shared_ptr<arrow::Field>& field, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(int8_t version,
                           OptionsUtils::GetValueFromMap<int8_t>(options, BitmapFileIndex::VERSION,
                                                                 BitmapFileIndex::VERSION_2));
    std::shared_ptr<arrow::DataType> struct_type = arrow::struct_({field});
    return std::shared_ptr<BitmapFileIndexWriter>(
        new BitmapFileIndexWriter(version, struct_type, field->type(), options, pool));
}

BitmapFileIndexWriter::BitmapFileIndexWriter(int8_t version,
                                             const std::shared_ptr<arrow::DataType>& struct_type,
                                             const std::shared_ptr<arrow::DataType>& arrow_type,
                                             const std::map<std::string, std::string>& options,
                                             const std::shared_ptr<MemoryPool>& pool)
    : version_(version),
      struct_type_(struct_type),
      arrow_type_(arrow_type),
      options_(options),
      pool_(pool) {}

Status BitmapFileIndexWriter::AddBatch(::ArrowArray* batch) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(batch, struct_type_));
    if (!arrow_array || arrow_array->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("invalid batch for BitmapFileIndexWriter, expected a struct array");
    }
    auto struct_array = checked_pointer_cast<arrow::StructArray>(arrow_array);
    if (struct_array->num_fields() != 1) {
        return Status::Invalid(
            "invalid batch for BitmapFileIndexWriter, expected a struct array with exactly one "
            "field");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<Literal> array_values,
        LiteralConverter::ConvertLiteralsFromArray(*(struct_array->field(0)), /*own_data=*/true));
    for (const auto& value : array_values) {
        if (value.IsNull()) {
            null_bitmap_.Add(row_number_);
        } else {
            PAIMON_ASSIGN_OR_RAISE(Literal converted_value,
                                   BitmapFileIndex::ConvertLiteral(value, arrow_type_));
            id_to_bitmap_[converted_value].Add(row_number_);
        }
        row_number_++;
    }
    return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> BitmapFileIndexWriter::SerializedBytes() const {
    PAIMON_ASSIGN_OR_RAISE(FieldType field_type,
                           FieldTypeUtils::ConvertToFieldType(arrow_type_->id()));
    FieldType converted_data_type = BitmapFileIndex::ConvertType(field_type);
    auto data_output_stream = std::make_shared<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    data_output_stream->SetOrder(ByteOrder::PAIMON_BIG_ENDIAN);

    data_output_stream->WriteValue<int8_t>(version_);

    // 1.serialize bitmaps to bytes
    std::shared_ptr<Bytes> null_bitmap_bytes = null_bitmap_.Serialize(pool_.get());
    std::unordered_map<Literal, std::shared_ptr<Bytes>> id_to_bitmap_bytes;
    for (const auto& [literal, bitmap] : id_to_bitmap_) {
        id_to_bitmap_bytes[literal] = bitmap.Serialize(pool_.get());
    }

    // 2.build bitmap file index meta
    std::vector<BitmapFileIndexMeta::Entry> write_entries;
    std::vector<std::shared_ptr<Bytes>> serialize_bitmaps;
    write_entries.reserve(id_to_bitmap_.size());
    serialize_bitmaps.reserve(id_to_bitmap_.size());

    // If null bitmap is not empty, it is placed at the beginning.
    // offset_ref is the offset of first non-null literal.
    int32_t offset_ref =
        (null_bitmap_.IsEmpty() || null_bitmap_.Cardinality() == 1) ? 0 : null_bitmap_bytes->size();
    for (const auto& [literal, bitmap] : id_to_bitmap_) {
        auto bytes = id_to_bitmap_bytes[literal];
        assert(bytes);
        if (bitmap.Cardinality() == 1) {
            // If bitmap has only one element, inline element value to offset.
            write_entries.emplace_back(literal,
                                       /*offset=*/-1 - *(bitmap.Begin()),
                                       /*length=*/-1);
        } else {
            serialize_bitmaps.push_back(bytes);
            write_entries.emplace_back(literal, /*offset=*/offset_ref,
                                       /*length=*/bytes->size());
            offset_ref += bytes->size();
        }
    }

    // If bitmap has only one element, inline element value to offset.
    int32_t null_value_offset =
        null_bitmap_.Cardinality() == 1 ? (-1 - *(null_bitmap_.Begin())) : 0;
    BitmapFileIndexMeta::Entry null_value_entry(Literal(converted_data_type),
                                                /*offset=*/null_value_offset,
                                                /*length=*/null_bitmap_bytes->size());

    // prepare bitmap file index meta
    std::shared_ptr<BitmapFileIndexMeta> bitmap_file_index_meta;
    if (version_ == BitmapFileIndex::VERSION_1) {
        bitmap_file_index_meta = std::make_shared<BitmapFileIndexMetaV1>(
            converted_data_type, row_number_, !null_bitmap_.IsEmpty(), null_value_entry,
            std::move(write_entries), pool_);
    } else if (version_ == BitmapFileIndex::VERSION_2) {
        bitmap_file_index_meta = std::make_shared<BitmapFileIndexMetaV2>(
            converted_data_type, row_number_, !null_bitmap_.IsEmpty(), null_value_entry,
            std::move(write_entries), options_, pool_);
    } else {
        return Status::Invalid(fmt::format("invalid version: {} for bitmap index", version_));
    }

    // 3.serialize meta
    PAIMON_RETURN_NOT_OK(bitmap_file_index_meta->Serialize(data_output_stream));

    // 4.serialize body
    if (null_bitmap_.Cardinality() > 1) {
        data_output_stream->WriteBytes(null_bitmap_bytes);
    }
    for (const auto& bytes : serialize_bitmaps) {
        data_output_stream->WriteBytes(bytes);
    }
    return MemorySegmentUtils::CopyToBytes(data_output_stream->Segments(), /*offset=*/0,
                                           /*num_bytes=*/data_output_stream->CurrentSize(),
                                           pool_.get());
}

BitmapFileIndexReader::BitmapFileIndexReader(const std::shared_ptr<arrow::DataType>& arrow_type,
                                             const FieldType& data_type, int32_t start,
                                             int32_t length,
                                             const std::shared_ptr<InputStream>& input_stream,
                                             const std::shared_ptr<MemoryPool>& pool)
    : head_start_(start),
      length_(length),
      data_type_(data_type),
      arrow_type_(arrow_type),
      pool_(pool),
      input_stream_(input_stream) {}

Result<std::shared_ptr<FileIndexResult>> BitmapFileIndexReader::VisitEqual(const Literal& literal) {
    return VisitIn({literal});
}

Result<std::shared_ptr<FileIndexResult>> BitmapFileIndexReader::VisitNotEqual(
    const Literal& literal) {
    return VisitNotIn({literal});
}

Result<std::shared_ptr<FileIndexResult>> BitmapFileIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    if (literals.empty()) {
        return Status::Invalid("literals cannot be empty in In predicate");
    }
    return std::make_shared<BitmapIndexResult>(
        [literals = literals, reader = shared_from_this()]() -> Result<RoaringBitmap32> {
            PAIMON_RETURN_NOT_OK(reader->ReadInternalMeta());
            return reader->GetInListResultBitmap(literals);
        });
}

Result<std::shared_ptr<FileIndexResult>> BitmapFileIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    if (literals.empty()) {
        return Status::Invalid("literals cannot be empty in In predicate");
    }
    return std::make_shared<BitmapIndexResult>(
        [literals = literals, reader = shared_from_this()]() -> Result<RoaringBitmap32> {
            PAIMON_RETURN_NOT_OK(reader->ReadInternalMeta());
            // not in does not contain null
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 bitmap, reader->GetInListResultBitmap(literals));
            bitmap.Flip(/*min=*/0, /*max=*/reader->bitmap_file_index_meta_->GetRowCount());
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 null,
                                   reader->GetInListResultBitmap({Literal(reader->data_type_)}));
            bitmap -= null;
            return bitmap;
        });
}

Result<std::shared_ptr<FileIndexResult>> BitmapFileIndexReader::VisitIsNull() {
    return VisitIn({Literal(data_type_)});
}

Result<std::shared_ptr<FileIndexResult>> BitmapFileIndexReader::VisitIsNotNull() {
    return VisitNotIn({Literal(data_type_)});
}

Result<RoaringBitmap32> BitmapFileIndexReader::GetInListResultBitmap(
    const std::vector<Literal>& literals) {
    std::vector<const RoaringBitmap32*> result_bitmaps;
    result_bitmaps.reserve(literals.size());
    for (const Literal& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(Literal converted_literal,
                               BitmapFileIndex::ConvertLiteral(literal, arrow_type_));
        auto iter = bitmaps_.find(converted_literal);
        if (iter != bitmaps_.end()) {
            result_bitmaps.emplace_back(&(iter->second));
        } else {
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 bitmap, ReadBitmap(converted_literal));
            auto new_iter = bitmaps_.emplace(converted_literal, std::move(bitmap));
            result_bitmaps.emplace_back(&(new_iter.first->second));
        }
    }
    return RoaringBitmap32::FastUnion(result_bitmaps);
}

Result<RoaringBitmap32> BitmapFileIndexReader::ReadBitmap(const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(const BitmapFileIndexMeta::Entry* entry,
                           bitmap_file_index_meta_->FindEntry(literal));
    if (entry == nullptr) {
        return RoaringBitmap32();
    }
    int32_t offset = entry->offset;
    if (offset < 0) {
        // offset < 0, indicates only one value in bitmap, and the value is (-1 - offset)
        return RoaringBitmap32::From({-1 - offset});
    } else {
        PAIMON_RETURN_NOT_OK(input_stream_->Seek(bitmap_file_index_meta_->GetBodyStart() + offset,
                                                 SeekOrigin::FS_SEEK_SET));
        auto bitmap_bytes = std::make_unique<Bytes>(entry->length, pool_.get());
        DataInputStream input(input_stream_);
        PAIMON_RETURN_NOT_OK(input.ReadBytes(bitmap_bytes.get()));
        RoaringBitmap32 res;
        PAIMON_RETURN_NOT_OK(res.Deserialize(bitmap_bytes->data(), bitmap_bytes->size()));
        return res;
    }
}

Status BitmapFileIndexReader::ReadInternalMeta() {
    if (!bitmap_file_index_meta_) {
        PAIMON_RETURN_NOT_OK(input_stream_->Seek(head_start_, SeekOrigin::FS_SEEK_SET));
        DataInputStream data_input_stream(input_stream_);
        PAIMON_ASSIGN_OR_RAISE(int8_t version, data_input_stream.ReadValue<int8_t>());
        FieldType converted_type = BitmapFileIndex::ConvertType(data_type_);
        if (version == BitmapFileIndex::VERSION_1) {
            bitmap_file_index_meta_ = std::make_shared<BitmapFileIndexMetaV1>(
                converted_type, head_start_, length_, pool_);
        } else if (version == BitmapFileIndex::VERSION_2) {
            bitmap_file_index_meta_ =
                std::make_shared<BitmapFileIndexMetaV2>(converted_type, length_, pool_);
        } else {
            return Status::Invalid(fmt::format("unknown bitmap file index version {}", version));
        }
        PAIMON_RETURN_NOT_OK(bitmap_file_index_meta_->Deserialize(input_stream_));
    }
    return Status::OK();
}

}  // namespace paimon
