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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/crc32.h"
#include "paimon/format/format_writer.h"
#include "paimon/logging.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
class ListArray;
}  // namespace arrow
struct ArrowArray;

namespace paimon {
class Blob;
class FileSystem;
class InputStream;
class Metrics;
class OutputStream;
}  // namespace paimon

namespace paimon::blob {

class BlobMetrics {
 public:
    /// Number of BLOB values (rows, or ARRAY<BLOB> elements) written as NULL because their
    /// referenced file did not exist.
    static inline const char WRITE_NULL_ON_MISSING_FILE_COUNT[] =
        "blob.write.null-on-missing-file.count";
    /// Number of BLOB values (rows, or ARRAY<BLOB> elements) written as NULL because their
    /// referenced data could not be reached.
    static inline const char WRITE_NULL_ON_FETCH_FAILURE_COUNT[] =
        "blob.write.null-on-fetch-failure.count";
};

// Blob format:
// https://cwiki.apache.org/confluence/display/PAIMON/PIP-35%3A+Introduce+Blob+to+store+multimodal+data
//
// The single field of a blob file is BLOB or ARRAY<BLOB>. An ARRAY<BLOB> entry stores the nested
// payload of the Paimon BLOB file spec, where a null element has length -1.
class BlobFormatWriter : public FormatWriter {
 public:
    /// `write_null_on_missing_file` converts a descriptor whose referenced file does not exist
    /// (as reported by FileSystem::Exists) to a NULL element, and `write_null_on_fetch_failure`
    /// converts any other failure to access the referenced data; failures during the streaming
    /// copy always fail the write. The existence check runs only when
    /// `write_null_on_missing_file` is enabled; otherwise a missing file follows
    /// `write_null_on_fetch_failure` like any other failed open.
    /// See Options::BLOB_WRITE_NULL_ON_MISSING_FILE / BLOB_WRITE_NULL_ON_FETCH_FAILURE.
    ///
    /// `write_placeholder` (see BlobDefs::kWritePlaceholderKey, false unless the write is a
    /// data-evolution partial update) persists a value exactly equal to
    /// BlobDefs::kPlaceholderSentinel, or an ARRAY<BLOB> whose only element is that sentinel, as
    /// a placeholder entry (bin_length -2, no data bytes); any other value is written as usual.
    /// When disabled, no value is interpreted as a placeholder.
    static Result<std::unique_ptr<BlobFormatWriter>> Create(
        const std::shared_ptr<OutputStream>& out, const std::shared_ptr<arrow::DataType>& data_type,
        bool write_null_on_missing_file, bool write_null_on_fetch_failure, bool write_placeholder,
        const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(ArrowArray* batch) override;

    Status Flush() override;

    Status Finish() override;

    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) const override;

    std::shared_ptr<Metrics> GetWriterMetrics() const override {
        return metrics_;
    }

    Status AddMetadata(const std::map<std::string, std::string>& metadata) override;

 private:
    BlobFormatWriter(const std::shared_ptr<OutputStream>& out, const std::string& uri,
                     const std::shared_ptr<arrow::DataType>& data_type,
                     bool write_null_on_missing_file, bool write_null_on_fetch_failure,
                     bool write_placeholder, const std::shared_ptr<FileSystem>& fs,
                     const std::shared_ptr<MemoryPool>& pool);

    void UpdateMetrics();

    Status WriteBlob(std::string_view blob_data);

    Status WriteArrayBlob(const arrow::ListArray& list_array);

    /// Open an input stream on a blob value, which is either a serialized BlobDescriptor or the
    /// raw blob bytes. Returns a null stream when a failure to reach the referenced data is
    /// converted to a NULL value by `write_null_on_missing_file_` or
    /// `write_null_on_fetch_failure_`. `element_index` identifies an ARRAY<BLOB> element in logs
    /// and errors.
    Result<std::unique_ptr<InputStream>> OpenBlobInputStream(std::string_view blob_data,
                                                             std::optional<int32_t> element_index);

    /// Copy all `in->Length()` bytes of `in` into the current entry, returning the number of bytes
    /// copied. Short reads are continued; a read that fails, or returns no bytes before the copy
    /// is complete, fails the copy.
    Result<int64_t> CopyBlobData(InputStream* in);

    Result<int64_t> BeginEntry();

    Status FinishEntry(int64_t entry_pos);

    /// Deserialize the descriptor and open an input stream on the referenced data.
    /// Returns a null stream when the failure is converted to a NULL element by
    /// `write_null_on_missing_file_` or `write_null_on_fetch_failure_`.
    Result<std::unique_ptr<InputStream>> OpenDescriptorInputStream(
        std::string_view blob_data, std::optional<int32_t> element_index);

    /// Convert a file that FileSystem::Exists reported as absent to a NULL element: count it and
    /// return a null stream. Only reached under `write_null_on_missing_file_`, which callers check.
    std::unique_ptr<InputStream> HandleMissingFile(const std::string& blob_uri,
                                                   std::optional<int32_t> element_index);

    /// Apply `write_null_on_fetch_failure_` to a failure to reach the referenced data: returns
    /// `status` with the value's context when the option is disabled, and a null stream when it
    /// converts the failure to a NULL element. `blob_uri` is only used for logs and errors.
    Result<std::unique_ptr<InputStream>> HandleFetchFailure(const std::string& blob_uri,
                                                            std::optional<int32_t> element_index,
                                                            const Status& status);

    /// Prefix a failure that is not converted to NULL with `action` and the value being written,
    /// keeping its status code.
    Status AddFailureContext(const Status& status, const std::string& action,
                             std::optional<int32_t> element_index) const;

    /// Describe the value being written for logs and errors: the BLOB field or one element of the
    /// ARRAY<BLOB> field, with its row in the blob file.
    std::string DescribeValue(std::optional<int32_t> element_index) const;

    /// Convert the size of a compressed index to the signed 32-bit index length stored after it,
    /// failing when the index is too large for that field.
    Result<int32_t> ToIndexLength(size_t index_size) const;

    Status WriteBytes(const char* data, int64_t length);
    Status WriteWithCrc32(const char* data, int64_t length);

    template <typename T>
    static PAIMON_UNIQUE_PTR<Bytes> IntegerToLittleEndian(T value,
                                                          const std::shared_ptr<MemoryPool>& pool);

 private:
    static constexpr uint32_t kTmpBufferSize = 1024 * 1024;

    uint32_t crc32_ = 0;
    std::vector<int64_t> bin_lengths_;
    std::shared_ptr<OutputStream> out_;
    /// Path of the blob file being written, not of any referenced blob.
    std::string uri_;
    PAIMON_UNIQUE_PTR<Bytes> tmp_buffer_;
    PAIMON_UNIQUE_PTR<Bytes> magic_number_bytes_;
    PAIMON_UNIQUE_PTR<Bytes> array_magic_number_bytes_;
    std::shared_ptr<arrow::DataType> data_type_;
    std::string blob_field_name_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<Metrics> metrics_;
    bool write_null_on_missing_file_ = false;
    bool write_null_on_fetch_failure_ = false;
    bool write_placeholder_ = false;
    uint64_t null_on_missing_file_count_ = 0;
    uint64_t null_on_fetch_failure_count_ = 0;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon::blob
