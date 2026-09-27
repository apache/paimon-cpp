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

#include "paimon/common/global_index/bitmap/bitmap_global_index_writer.h"

#include <limits>
#include <utility>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/global_index/global_index_utils.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/global_index/sorted_index_file_meta.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/preconditions.h"

namespace paimon {
namespace {

constexpr char kBitmapIdentifier[] = "bitmap";

}  // namespace

Result<std::shared_ptr<BitmapGlobalIndexWriter>> BitmapGlobalIndexWriter::Create(
    const std::string& field_name, const std::shared_ptr<arrow::StructType>& arrow_type,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer, int32_t dictionary_block_size,
    const std::shared_ptr<BlockCompressionFactory>& compression_factory,
    const std::shared_ptr<MemoryPool>& pool) {
    if (arrow_type == nullptr || file_writer == nullptr || pool == nullptr) {
        return Status::Invalid(
            "Cannot create BitmapGlobalIndexWriter without schema, file writer, and memory pool.");
    }
    if (dictionary_block_size <= 0) {
        return Status::Invalid("Bitmap dictionary block size must be greater than 0.");
    }
    std::shared_ptr<arrow::Field> key_field = arrow_type->GetFieldByName(field_name);
    PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
        key_field, fmt::format("field {} not in arrow_array when Create BitmapGlobalIndexWriter",
                               field_name)));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> key_serializer,
                           KeySerializer::Create(key_field->type(), pool));
    return std::shared_ptr<BitmapGlobalIndexWriter>(
        new BitmapGlobalIndexWriter(field_name, arrow_type, std::move(key_serializer), file_writer,
                                    dictionary_block_size, compression_factory, pool));
}

Status BitmapGlobalIndexWriter::AddBatch(::ArrowArray* arrow_array,
                                         std::vector<int64_t>&& relative_row_ids) {
    if (finished_) {
        return Status::Invalid("Cannot add a batch to a finished BitmapGlobalIndexWriter.");
    }
    PAIMON_RETURN_NOT_OK(GlobalIndexUtils::CheckRelativeRowIds(
        arrow_array, relative_row_ids, /*expected_next_row_id=*/std::nullopt));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow_type_));
    if (array == nullptr || array->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            "arrow array must be struct array when AddBatch to BitmapGlobalIndexWriter");
    }
    std::shared_ptr<arrow::StructArray> struct_array =
        checked_pointer_cast<arrow::StructArray>(array);
    std::shared_ptr<arrow::Array> value_array = struct_array->GetFieldByName(field_name_);
    PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
        value_array,
        fmt::format("field {} not in arrow_array when AddBatch to BitmapGlobalIndexWriter",
                    field_name_)));
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> literals,
                           LiteralConverter::ConvertLiteralsFromArray(*value_array,
                                                                      /*own_data=*/true));
    for (size_t i = 0; i < literals.size(); ++i) {
        if (row_count_ == std::numeric_limits<int64_t>::max()) {
            return Status::Invalid("Bitmap global index row count exceeds INT64_MAX.");
        }
        ++row_count_;
        int64_t row_id = relative_row_ids[i];
        const Literal& literal = literals[i];
        if (literal.IsNull()) {
            null_rows_.Add(row_id);
            continue;
        }

        non_null_rows_.Add(row_id);
        if (last_key_.has_value()) {
            PAIMON_ASSIGN_OR_RAISE(int32_t comparison, literal.CompareTo(last_key_.value()));
            if (comparison < 0) {
                return Status::Invalid(
                    "Bitmap index keys must be written in monotonically increasing order.");
            }
            if (comparison > 0) {
                PAIMON_RETURN_NOT_OK(FlushCurrentBitmap());
            }
        }
        if (!first_key_.has_value()) {
            first_key_ = literal;
        }
        last_key_ = literal;
        current_bitmap_.Add(row_id);
    }
    return Status::OK();
}

Result<std::vector<GlobalIndexIOMeta>> BitmapGlobalIndexWriter::Finish() {
    if (finished_) {
        return Status::Invalid("BitmapGlobalIndexWriter has already been finished.");
    }
    finished_ = true;
    if (row_count_ == 0) {
        return std::vector<GlobalIndexIOMeta>();
    }

    PAIMON_RETURN_NOT_OK(FlushCurrentBitmap());
    PAIMON_ASSIGN_OR_RAISE(BitmapGlobalIndexFormat::StreamingWriter * streaming_writer,
                           GetOrCreateStreamingWriter());
    PAIMON_RETURN_NOT_OK(streaming_writer->Finish(null_rows_, non_null_rows_));
    PAIMON_RETURN_NOT_OK(output_stream_->Close());

    std::shared_ptr<Bytes> first_key_bytes;
    std::shared_ptr<Bytes> last_key_bytes;
    if (first_key_.has_value()) {
        PAIMON_ASSIGN_OR_RAISE(first_key_bytes, key_serializer_->Serialize(first_key_.value()));
    }
    if (last_key_.has_value()) {
        PAIMON_ASSIGN_OR_RAISE(last_key_bytes, key_serializer_->Serialize(last_key_.value()));
    }
    SortedIndexFileMeta index_meta(first_key_bytes, last_key_bytes, !null_rows_.IsEmpty());
    std::shared_ptr<Bytes> metadata = index_meta.Serialize(pool_.get());
    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file_writer_->GetFileSize(file_name_));
    return std::vector<GlobalIndexIOMeta>{
        GlobalIndexIOMeta(file_writer_->ToPath(file_name_), file_size, std::move(metadata))};
}

Status BitmapGlobalIndexWriter::FlushCurrentBitmap() {
    if (current_bitmap_.IsEmpty()) {
        return Status::OK();
    }
    if (!last_key_.has_value()) {
        return Status::Invalid("BitmapGlobalIndexWriter has a bitmap without a dictionary key.");
    }
    PAIMON_ASSIGN_OR_RAISE(BitmapGlobalIndexFormat::StreamingWriter * streaming_writer,
                           GetOrCreateStreamingWriter());
    PAIMON_ASSIGN_OR_RAISE(
        BitmapGlobalIndexFormat::SerializedKey key,
        BitmapGlobalIndexFormat::SerializedKey::FromLiteral(key_serializer_, last_key_.value()));
    PAIMON_RETURN_NOT_OK(streaming_writer->Write(std::move(key), current_bitmap_));
    current_bitmap_ = RoaringBitmap64();
    return Status::OK();
}

Result<BitmapGlobalIndexFormat::StreamingWriter*>
BitmapGlobalIndexWriter::GetOrCreateStreamingWriter() {
    if (streaming_writer_ != nullptr) {
        return streaming_writer_.get();
    }
    PAIMON_ASSIGN_OR_RAISE(file_name_, file_writer_->NewFileName(kBitmapIdentifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> output_stream,
                           file_writer_->NewOutputStream(file_name_));
    output_stream_ = std::move(output_stream);
    PAIMON_ASSIGN_OR_RAISE(streaming_writer_, BitmapGlobalIndexFormat::StreamingWriter::Create(
                                                  output_stream_, dictionary_block_size_,
                                                  compression_factory_, pool_));
    return streaming_writer_.get();
}

}  // namespace paimon
