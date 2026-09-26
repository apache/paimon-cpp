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

#include "paimon/format/blob/blob_format_writer.h"

#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/delta_varint_compressor.h"
#include "paimon/data/blob.h"
#include "paimon/data/blob_descriptor.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/logging.h"

namespace paimon::blob {

BlobFormatWriter::BlobFormatWriter(const std::shared_ptr<OutputStream>& out, const std::string& uri,
                                   const std::shared_ptr<arrow::DataType>& data_type,
                                   bool write_null_on_missing_file,
                                   bool write_null_on_fetch_failure, bool write_placeholder,
                                   const std::shared_ptr<FileSystem>& fs,
                                   const std::shared_ptr<MemoryPool>& pool)
    : out_(out),
      uri_(uri),
      data_type_(data_type),
      fs_(fs),
      pool_(pool),
      write_null_on_missing_file_(write_null_on_missing_file),
      write_null_on_fetch_failure_(write_null_on_fetch_failure),
      write_placeholder_(write_placeholder) {
    // Create() has already checked that data_type has exactly one BLOB or ARRAY<BLOB> field.
    blob_field_name_ = data_type_->field(0)->name();
    metrics_ = std::make_shared<MetricsImpl>();
    tmp_buffer_ = Bytes::AllocateBytes(kTmpBufferSize, pool_.get());
    magic_number_bytes_ = IntegerToLittleEndian<int32_t>(BlobDefs::kMagicNumber, pool_);
    array_magic_number_bytes_ =
        IntegerToLittleEndian<int32_t>(BlobDefs::kArrayBlobMagicNumber, pool_);
    logger_ = Logger::GetLogger("BlobFormatWriter");
}

Result<std::unique_ptr<BlobFormatWriter>> BlobFormatWriter::Create(
    const std::shared_ptr<OutputStream>& out, const std::shared_ptr<arrow::DataType>& data_type,
    bool write_null_on_missing_file, bool write_null_on_fetch_failure, bool write_placeholder,
    const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<MemoryPool>& pool) {
    if (out == nullptr) {
        return Status::Invalid("blob format writer create failed. out is nullptr");
    }
    if (data_type == nullptr) {
        return Status::Invalid("blob format writer create failed. data_type is nullptr");
    }
    if (pool == nullptr) {
        return Status::Invalid("blob format writer create failed. pool is nullptr");
    }
    if (fs == nullptr) {
        return Status::Invalid("blob format writer create failed. fs is nullptr");
    }
    if (data_type->num_fields() != 1) {
        return Status::Invalid(
            fmt::format("blob data type field number {} is not 1", data_type->num_fields()));
    }
    if (!BlobUtils::IsBlobField(data_type->field(0)) &&
        !BlobUtils::IsArrayBlobField(data_type->field(0))) {
        return Status::Invalid(
            fmt::format("field {} is not BLOB or ARRAY<BLOB>", data_type->field(0)->ToString()));
    }
    PAIMON_ASSIGN_OR_RAISE(std::string uri, out->GetUri());
    return std::unique_ptr<BlobFormatWriter>(
        new BlobFormatWriter(out, uri, data_type, write_null_on_missing_file,
                             write_null_on_fetch_failure, write_placeholder, fs, pool));
}

Status BlobFormatWriter::AddBatch(ArrowArray* batch) {
    if (batch == nullptr) {
        return Status::Invalid("blob format writer add batch failed. batch is nullptr");
    }
    if (batch->length != 1) {
        return Status::Invalid("BlobFormatWriter only supports batch with a row count of 1");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(batch, data_type_));

    assert(arrow_array->num_fields() == 1);
    auto struct_array = checked_pointer_cast<arrow::StructArray>(arrow_array);
    auto child_array = struct_array->field(0);

    // Struct-level null is not supported (caller should not pass null struct rows)
    if (struct_array->IsNull(0)) {
        return Status::Invalid("BlobFormatWriter does not support struct-level null.");
    }
    // Child-level null: record kNullBinLength, skip data writing (aligned with Java)
    if (child_array->IsNull(0)) {
        bin_lengths_.push_back(BlobDefs::kNullBinLength);
        return Status::OK();
    }

    if (child_array->type_id() == arrow::Type::type::LIST) {
        const auto& list_array = checked_cast<const arrow::ListArray&>(*child_array);
        if (write_placeholder_ && BlobUtils::IsArrayBlobPlaceholder(list_array, 0)) {
            bin_lengths_.push_back(BlobDefs::kPlaceholderBinLength);
            return Status::OK();
        }
        PAIMON_RETURN_NOT_OK(WriteArrayBlob(list_array));
        // ARRAY<BLOB> rows are left to Flush() and Finish() to persist; Java flushes one earlier
        // only when its BlobConsumer asks to.
        UpdateMetrics();
        return Status::OK();
    }
    if (child_array->type_id() != arrow::Type::type::LARGE_BINARY) {
        return Status::Invalid("BlobFormatWriter only support large binary type.");
    }

    const auto& blob_array = checked_cast<const arrow::LargeBinaryArray&>(*child_array);
    assert(blob_array.length() == 1);
    std::string_view blob_data = blob_array.GetView(0);
    // Only a data-evolution partial-update write interprets the sentinel, which marks a row
    // the update did not touch; any other write stores the bytes verbatim. See
    // BlobDefs::kPlaceholderSentinel for the accepted collision with a sentinel-equal user
    // value.
    if (write_placeholder_ && BlobDefs::IsPlaceholderSentinel(blob_data.data(), blob_data.size())) {
        bin_lengths_.push_back(BlobDefs::kPlaceholderBinLength);
        return Status::OK();
    }
    PAIMON_RETURN_NOT_OK(WriteBlob(blob_data));
    PAIMON_RETURN_NOT_OK(Flush());
    return Status::OK();
}

void BlobFormatWriter::UpdateMetrics() {
    metrics_->SetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT,
                         null_on_missing_file_count_);
    metrics_->SetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT,
                         null_on_fetch_failure_count_);
}

Status BlobFormatWriter::Flush() {
    UpdateMetrics();
    Status status = out_->Flush();
    if (!status.ok()) {
        return status.WithMessage("failed to flush blob file ", uri_, ": ", status.message());
    }
    return Status::OK();
}

Status BlobFormatWriter::Finish() {
    // index
    const auto& index_bytes = DeltaVarintCompressor::Compress(bin_lengths_);
    PAIMON_ASSIGN_OR_RAISE(int32_t index_length, ToIndexLength(index_bytes.size()));
    PAIMON_RETURN_NOT_OK(WriteBytes(index_bytes.data(), index_bytes.size()));
    // footer: index length and file version
    PAIMON_UNIQUE_PTR<Bytes> index_length_bytes =
        IntegerToLittleEndian<int32_t>(index_length, pool_);
    PAIMON_RETURN_NOT_OK(WriteBytes(index_length_bytes->data(), index_length_bytes->size()));
    PAIMON_RETURN_NOT_OK(WriteBytes(reinterpret_cast<const char*>(&BlobDefs::kFileVersion),
                                    sizeof(BlobDefs::kFileVersion)));

    PAIMON_RETURN_NOT_OK(Flush());

    tmp_buffer_.reset();
    return Status::OK();
}

Status BlobFormatWriter::WriteBlob(std::string_view blob_data) {
    // Open the blob input stream before writing any bytes, so that a failed fetch can be
    // converted to a NULL element without leaving partial data in the output stream.
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> in,
                           OpenBlobInputStream(blob_data, /*element_index=*/std::nullopt));
    // A null stream means a write-null option already converted the failure.
    if (in == nullptr) {
        bin_lengths_.push_back(BlobDefs::kNullBinLength);
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t entry_pos, BeginEntry());
    Result<int64_t> copied = CopyBlobData(in.get());
    if (!copied.ok()) {
        return AddFailureContext(copied.status(), "failed to copy",
                                 /*element_index=*/std::nullopt);
    }
    return FinishEntry(entry_pos);
}

Status BlobFormatWriter::WriteArrayBlob(const arrow::ListArray& list_array) {
    const std::shared_ptr<arrow::Array>& values = list_array.values();
    if (values->type_id() != arrow::Type::type::LARGE_BINARY) {
        return Status::Invalid("BlobFormatWriter only support large binary ARRAY<BLOB> elements.");
    }
    const auto& blob_values = checked_cast<const arrow::LargeBinaryArray&>(*values);
    const int64_t element_offset = list_array.value_offset(0);
    const int32_t element_count = list_array.value_length(0);

    PAIMON_ASSIGN_OR_RAISE(int64_t entry_pos, BeginEntry());
    PAIMON_RETURN_NOT_OK(
        WriteWithCrc32(array_magic_number_bytes_->data(), array_magic_number_bytes_->size()));
    PAIMON_RETURN_NOT_OK(WriteWithCrc32(reinterpret_cast<const char*>(&BlobDefs::kArrayBlobVersion),
                                        sizeof(BlobDefs::kArrayBlobVersion)));
    PAIMON_UNIQUE_PTR<Bytes> element_count_bytes =
        IntegerToLittleEndian<int32_t>(element_count, pool_);
    PAIMON_RETURN_NOT_OK(WriteWithCrc32(element_count_bytes->data(), element_count_bytes->size()));

    // Each element is opened before any of its bytes are written, so an element converted to NULL
    // leaves no partial data. As in Java, any other failure leaves a partial entry and fails the
    // write.
    std::vector<int64_t> element_lengths(element_count, BlobDefs::kNullBinLength);
    for (int32_t i = 0; i < element_count; ++i) {
        const int64_t value_index = element_offset + i;
        if (blob_values.IsNull(value_index)) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> in,
                               OpenBlobInputStream(blob_values.GetView(value_index), i));
        if (in == nullptr) {
            continue;
        }
        Result<int64_t> copied = CopyBlobData(in.get());
        if (!copied.ok()) {
            return AddFailureContext(copied.status(), "failed to copy", i);
        }
        element_lengths[i] = copied.value();
    }

    const std::vector<char> index_bytes = DeltaVarintCompressor::Compress(element_lengths);
    PAIMON_ASSIGN_OR_RAISE(int32_t index_length, ToIndexLength(index_bytes.size()));
    PAIMON_RETURN_NOT_OK(WriteWithCrc32(index_bytes.data(), index_bytes.size()));
    PAIMON_UNIQUE_PTR<Bytes> index_length_bytes =
        IntegerToLittleEndian<int32_t>(index_length, pool_);
    PAIMON_RETURN_NOT_OK(WriteWithCrc32(index_length_bytes->data(), index_length_bytes->size()));
    return FinishEntry(entry_pos);
}

Result<std::unique_ptr<InputStream>> BlobFormatWriter::OpenBlobInputStream(
    std::string_view blob_data, std::optional<int32_t> element_index) {
    // Whether blob_data is a serialized BlobDescriptor is detected by its magic header rather
    // than taken from a blob_as_descriptor option, so each value may hold either form.
    PAIMON_ASSIGN_OR_RAISE(bool is_descriptor,
                           BlobDescriptor::IsBlobDescriptor(blob_data.data(), blob_data.size()));
    if (is_descriptor) {
        return OpenDescriptorInputStream(blob_data, element_index);
    }
    return std::unique_ptr<InputStream>(
        std::make_unique<ByteArrayInputStream>(blob_data.data(), blob_data.size()));
}

Result<int64_t> BlobFormatWriter::CopyBlobData(InputStream* in) {
    PAIMON_ASSIGN_OR_RAISE(int64_t length, in->Length());
    int64_t copied = 0;
    while (copied < length) {
        const int64_t request =
            std::min(length - copied, static_cast<int64_t>(tmp_buffer_->size()));
        PAIMON_ASSIGN_OR_RAISE(int64_t read_len, in->Read(tmp_buffer_->data(), request));
        if (read_len <= 0) {
            return Status::IOError(
                fmt::format("unexpected end of blob data after {} of {} bytes: read returned {}",
                            copied, length, read_len));
        }
        if (read_len > request) {
            return Status::Invalid(fmt::format("read returned {} bytes, more than the {} requested",
                                               read_len, request));
        }
        PAIMON_RETURN_NOT_OK(WriteWithCrc32(tmp_buffer_->data(), read_len));
        copied += read_len;
    }
    return copied;
}

Result<int64_t> BlobFormatWriter::BeginEntry() {
    crc32_ = 0;
    PAIMON_ASSIGN_OR_RAISE(int64_t entry_pos, out_->GetPos());
    PAIMON_RETURN_NOT_OK(WriteWithCrc32(magic_number_bytes_->data(), magic_number_bytes_->size()));
    return entry_pos;
}

Status BlobFormatWriter::FinishEntry(int64_t entry_pos) {
    PAIMON_ASSIGN_OR_RAISE(int64_t current_pos, out_->GetPos());
    /// magic number(4) + blob content(bin length - 16) + bin length(8) + crc32(4)
    /// ↑                                             ↑
    /// entry_pos                                  current_pos
    int64_t bin_length = current_pos - entry_pos + 8 + 4;
    bin_lengths_.push_back(bin_length);
    PAIMON_UNIQUE_PTR<Bytes> bin_length_bytes = IntegerToLittleEndian<int64_t>(bin_length, pool_);
    PAIMON_RETURN_NOT_OK(WriteWithCrc32(bin_length_bytes->data(), bin_length_bytes->size()));

    PAIMON_UNIQUE_PTR<Bytes> crc32_bytes = IntegerToLittleEndian<int32_t>(crc32_, pool_);
    return WriteBytes(crc32_bytes->data(), crc32_bytes->size());
}

Result<std::unique_ptr<InputStream>> BlobFormatWriter::OpenDescriptorInputStream(
    std::string_view blob_data, std::optional<int32_t> element_index) {
    // A descriptor that cannot be deserialized is a fetch failure: the referenced data cannot be
    // reached. Its URI is inside the unreadable bytes, hence the placeholder; the underlying
    // status comes from the byte reader and never mentions blobs, hence the added context.
    Result<std::unique_ptr<Blob>> blob_result =
        Blob::FromDescriptor(blob_data.data(), blob_data.size());
    if (!blob_result.ok()) {
        const Status& status = blob_result.status();
        return HandleFetchFailure(
            "<unknown>", element_index,
            status.WithMessage("invalid blob descriptor: ", status.message()));
    }
    std::unique_ptr<Blob> blob = std::move(blob_result).value();

    // A missing file is identified by FileSystem::Exists rather than by the status of a failed
    // open, since file system implementations disagree on which status a missing file maps to.
    // The check runs only when `write_null_on_missing_file_` needs the classification; otherwise
    // a missing file gets the same treatment as any other failed open.
    if (write_null_on_missing_file_) {
        Result<bool> exists = fs_->Exists(blob->Uri());
        if (exists.ok()) {
            if (!exists.value()) {
                return HandleMissingFile(blob->Uri(), element_index);
            }
        } else if (!write_null_on_fetch_failure_) {
            // The check cannot answer whether the file is there; with no fetch-failure handling
            // to defer to, fail rather than assume either answer.
            return AddFailureContext(
                exists.status(),
                fmt::format("failed to check existence of blob file '{}' for", blob->Uri()),
                element_index);
        }
        // A failed check is otherwise deferred to the open below, which can still succeed.
    }

    Result<std::unique_ptr<InputStream>> opened = blob->NewInputStream(fs_);
    if (!opened.ok()) {
        // The file can be deleted between the check above and this open. Classifying that from
        // `opened.status()` would reintroduce the plugin-specific status codes this writer avoids,
        // so ask FileSystem::Exists once more. This narrows the window rather than closing it; a
        // check that cannot answer falls through to the open failure.
        if (write_null_on_missing_file_) {
            Result<bool> exists = fs_->Exists(blob->Uri());
            if (exists.ok() && !exists.value()) {
                return HandleMissingFile(blob->Uri(), element_index);
            }
        }
        return HandleFetchFailure(blob->Uri(), element_index, opened.status());
    }
    return std::move(opened).value();
}

std::unique_ptr<InputStream> BlobFormatWriter::HandleMissingFile(
    const std::string& blob_uri, std::optional<int32_t> element_index) {
    PAIMON_LOG_WARN(logger_, "Blob file %s does not exist, writing NULL for %s", blob_uri.c_str(),
                    DescribeValue(element_index).c_str());
    ++null_on_missing_file_count_;
    return std::unique_ptr<InputStream>();
}

Result<std::unique_ptr<InputStream>> BlobFormatWriter::HandleFetchFailure(
    const std::string& blob_uri, std::optional<int32_t> element_index, const Status& status) {
    if (!write_null_on_fetch_failure_) {
        return AddFailureContext(status, fmt::format("failed to fetch blob {} for", blob_uri),
                                 element_index);
    }
    PAIMON_LOG_WARN(logger_, "Failed to fetch blob %s, writing NULL for %s: %s", blob_uri.c_str(),
                    DescribeValue(element_index).c_str(), status.ToString().c_str());
    ++null_on_fetch_failure_count_;
    return std::unique_ptr<InputStream>();
}

Status BlobFormatWriter::AddFailureContext(const Status& status, const std::string& action,
                                           std::optional<int32_t> element_index) const {
    return status.WithMessage(action, " ", DescribeValue(element_index), ": ", status.message());
}

std::string BlobFormatWriter::DescribeValue(std::optional<int32_t> element_index) const {
    // The entry of the row being written is appended to bin_lengths_ once the row is done.
    const size_t row = bin_lengths_.size();
    if (!element_index) {
        return fmt::format("BLOB field {} in row {} of blob file {}", blob_field_name_, row, uri_);
    }
    return fmt::format("element {} of ARRAY<BLOB> field {} in row {} of blob file {}",
                       *element_index, blob_field_name_, row, uri_);
}

Result<int32_t> BlobFormatWriter::ToIndexLength(size_t index_size) const {
    if (index_size > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        return Status::Invalid(
            fmt::format("compressed index too large: {} bytes in blob file {}", index_size, uri_));
    }
    return static_cast<int32_t>(index_size);
}

Status BlobFormatWriter::WriteBytes(const char* data, int64_t length) {
    Result<int64_t> actual = out_->Write(data, length);
    if (!actual.ok()) {
        const Status& status = actual.status();
        return status.WithMessage("failed to write blob file ", uri_, ": ", status.message());
    }
    if (actual.value() != length) {
        return Status::Invalid(
            fmt::format("failed to write blob file {}: unexpected actual length {} not match with "
                        "expect {}",
                        uri_, actual.value(), length));
    }
    return Status::OK();
}

Status BlobFormatWriter::WriteWithCrc32(const char* data, int64_t length) {
    crc32_ = arrow::internal::crc32(crc32_, data, length);
    return WriteBytes(data, length);
}

Result<bool> BlobFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    PAIMON_ASSIGN_OR_RAISE(int64_t current_pos, out_->GetPos());
    return current_pos >= target_size;
}

Status BlobFormatWriter::AddMetadata(const std::map<std::string, std::string>& /*metadata*/) {
    return Status::NotImplemented("AddMetadata is not supported by blob format writer.");
}

template <typename T>
PAIMON_UNIQUE_PTR<Bytes> BlobFormatWriter::IntegerToLittleEndian(
    T value, const std::shared_ptr<MemoryPool>& pool) {
    static_assert(std::is_integral_v<T>, "IntegerToLittleEndian() only supports integral types.");
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    out.WriteValue<T>(value);
    return MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
}

}  // namespace paimon::blob
