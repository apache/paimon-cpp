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

#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"

#include <arrow/c/bridge.h>
#include <arrow/type.h>

#include "paimon/common/file_index/rangebitmap/range_bitmap.h"
#include "paimon/common/file_index/rangebitmap/range_bitmap_type_adapter.h"
#include "paimon/common/io/offset_input_stream.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

RangeBitmapFileIndex::RangeBitmapFileIndex(const std::map<std::string, std::string>& options)
    : options_(options) {}

Result<std::shared_ptr<FileIndexReader>> RangeBitmapFileIndex::CreateReader(
    ::ArrowSchema* arrow_schema, const int32_t start, const int32_t length,
    const std::shared_ptr<InputStream>& input_stream,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema_ptr,
                                      arrow::ImportSchema(arrow_schema));
    if (arrow_schema_ptr->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for RangeBitmapFileIndexReader, supposed to have single field.");
    }
    const auto arrow_type = arrow_schema_ptr->field(0)->type();
    return RangeBitmapFileIndexReader::Create(arrow_type, start, length, input_stream, pool);
}

Result<std::shared_ptr<FileIndexWriter>> RangeBitmapFileIndex::CreateWriter(
    ::ArrowSchema* arrow_schema, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema_ptr,
                                      arrow::ImportSchema(arrow_schema));
    if (arrow_schema_ptr->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for RangeBitmapFileIndexWriter, supposed to have single field.");
    }
    return RangeBitmapFileIndexWriter::Create(arrow_schema_ptr->field(0), options_, pool);
}

Result<std::shared_ptr<RangeBitmapFileIndexWriter>> RangeBitmapFileIndexWriter::Create(
    const std::shared_ptr<arrow::Field>& field, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RangeBitmapTypeAdapter> type_adapter,
                           RangeBitmapTypeAdapter::Create(field->type()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeyFactory> shared_key_factory,
                           KeyFactory::Create(type_adapter->GetStorageType()));
    PAIMON_ASSIGN_OR_RAISE(int64_t parsed_chunk_size,
                           MemorySize::ParseBytes(KeyFactory::kDefaultChunkSize));
    if (const auto chunk_size_it = options.find(RangeBitmapFileIndex::kChunkSize);
        chunk_size_it != options.end()) {
        PAIMON_ASSIGN_OR_RAISE(parsed_chunk_size, MemorySize::ParseBytes(chunk_size_it->second));
    }
    std::shared_ptr<arrow::DataType> struct_type = arrow::struct_({field});
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<RangeBitmap::Appender> appender_ptr,
        RangeBitmap::Appender::Create(shared_key_factory, parsed_chunk_size, pool));
    return std::shared_ptr<RangeBitmapFileIndexWriter>(new RangeBitmapFileIndexWriter(
        struct_type, std::move(type_adapter), std::move(appender_ptr)));
}

RangeBitmapFileIndexWriter::~RangeBitmapFileIndexWriter() = default;

Status RangeBitmapFileIndexWriter::AddBatch(::ArrowArray* batch) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(batch, struct_type_));
    auto struct_array = checked_pointer_cast<arrow::StructArray>(array);
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> array_values,
                           LiteralConverter::ConvertLiteralsFromArray(*(struct_array->field(0)),
                                                                      /*own_data=*/true));
    for (const Literal& literal : array_values) {
        PAIMON_ASSIGN_OR_RAISE(Literal converted_literal, type_adapter_->ToStorageLiteral(literal));
        appender_->Append(converted_literal);
    }
    return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> RangeBitmapFileIndexWriter::SerializedBytes() const {
    return appender_->Serialize();
}

RangeBitmapFileIndexWriter::RangeBitmapFileIndexWriter(
    const std::shared_ptr<arrow::DataType>& struct_type,
    std::unique_ptr<RangeBitmapTypeAdapter> type_adapter,
    std::unique_ptr<RangeBitmap::Appender> appender)
    : struct_type_(struct_type),
      type_adapter_(std::move(type_adapter)),
      appender_(std::move(appender)) {}

Result<std::shared_ptr<RangeBitmapFileIndexReader>> RangeBitmapFileIndexReader::Create(
    const std::shared_ptr<arrow::DataType>& arrow_type, const int32_t start, const int32_t length,
    const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RangeBitmapTypeAdapter> type_adapter,
                           RangeBitmapTypeAdapter::Create(arrow_type));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OffsetInputStream> bounded_stream,
                           OffsetInputStream::Create(input_stream, length, start));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<RangeBitmap> range_bitmap,
        RangeBitmap::Create(bounded_stream, 0, type_adapter->GetStorageType(), pool));
    return std::shared_ptr<RangeBitmapFileIndexReader>(
        new RangeBitmapFileIndexReader(std::move(type_adapter), std::move(range_bitmap)));
}

RangeBitmapFileIndexReader::~RangeBitmapFileIndexReader() = default;

RangeBitmapFileIndexReader::RangeBitmapFileIndexReader(
    std::unique_ptr<RangeBitmapTypeAdapter> type_adapter, std::unique_ptr<RangeBitmap> range_bitmap)
    : type_adapter_(std::move(type_adapter)), range_bitmap_(std::move(range_bitmap)) {}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(Literal converted_literal, type_adapter_->ToStorageLiteral(literal));
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), converted_literal]() -> Result<RoaringBitmap32> {
            return self->range_bitmap_->Eq(converted_literal);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitNotEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(Literal converted_literal, type_adapter_->ToStorageLiteral(literal));
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), converted_literal]() -> Result<RoaringBitmap32> {
            return self->range_bitmap_->Neq(converted_literal);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> converted_literals,
                           type_adapter_->ToStorageLiterals(literals));
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(),
         converted_literals = std::move(converted_literals)]() -> Result<RoaringBitmap32> {
            return self->range_bitmap_->In(converted_literals);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> converted_literals,
                           type_adapter_->ToStorageLiterals(literals));
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(),
         converted_literals = std::move(converted_literals)]() -> Result<RoaringBitmap32> {
            return self->range_bitmap_->NotIn(converted_literals);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitIsNull() {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this()]() -> Result<RoaringBitmap32> {
            return self->range_bitmap_->IsNull();
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitIsNotNull() {
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this()]() -> Result<RoaringBitmap32> {
            return self->range_bitmap_->IsNotNull();
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitGreaterThan(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(Literal converted_literal, type_adapter_->ToStorageLiteral(literal));
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), converted_literal]() -> Result<RoaringBitmap32> {
            return self->range_bitmap_->Gt(converted_literal);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitLessThan(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(Literal converted_literal, type_adapter_->ToStorageLiteral(literal));
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), converted_literal]() -> Result<RoaringBitmap32> {
            return self->range_bitmap_->Lt(converted_literal);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(Literal converted_literal, type_adapter_->ToStorageLiteral(literal));
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), converted_literal]() -> Result<RoaringBitmap32> {
            return self->range_bitmap_->Gte(converted_literal);
        });
}

Result<std::shared_ptr<FileIndexResult>> RangeBitmapFileIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(Literal converted_literal, type_adapter_->ToStorageLiteral(literal));
    return std::make_shared<BitmapIndexResult>(
        [self = shared_from_this(), converted_literal]() -> Result<RoaringBitmap32> {
            return self->range_bitmap_->Lte(converted_literal);
        });
}

}  // namespace paimon
