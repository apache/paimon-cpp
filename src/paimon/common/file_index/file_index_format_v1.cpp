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

#include "paimon/common/file_index/file_index_format_v1.h"

#include <cassert>
#include <utility>

#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/utils/math.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {
namespace {
template <typename T>
Status AddChecked(T value, const char* name, int64_t* total) {
    PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(value, name));
    *total += static_cast<int64_t>(value);
    return ValidateValueInRange<int32_t>(*total, name);
}
}  // namespace

FileIndexFormatV1::Writer::Writer(const std::shared_ptr<OutputStream>& output,
                                  const std::shared_ptr<MemoryPool>& pool)
    : output_stream_(output), pool_(pool) {
    assert(output_stream_);
}

Status FileIndexFormatV1::Writer::WriteIndex(const std::string& column, const std::string& type,
                                             const PayloadWriter& payload) {
    if (finished_ || closed_ || failed_) {
        return Status::Invalid("File index writer is not writable");
    }
    if (buffered_indexes_[column].count(type)) {
        return Status::Invalid("Duplicate file index entry");
    }
    std::shared_ptr<Bytes> bytes;
    if (payload) {
        auto output =
            std::make_shared<ByteArrayOutputStream>(std::make_unique<MemorySegmentOutputStream>(
                MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_));
        failed_ = true;
        PAIMON_ASSIGN_OR_RAISE(bool written, payload(output));
        PAIMON_ASSIGN_OR_RAISE(int64_t length, output->GetPos());
        if (!written && length != 0) {
            return Status::Invalid("Absent file index payload must not write bytes");
        }
        if (written) {
            PAIMON_ASSIGN_OR_RAISE(bytes, output->Finish(pool_.get()));
        }
        failed_ = false;
    }
    buffered_indexes_[column][type] = std::move(bytes);
    return Status::OK();
}

Status FileIndexFormatV1::Writer::Finish() {
    if (finished_ || closed_ || failed_) {
        return Status::Invalid("File index writer is not writable");
    }
    Status status = WriteIndexes(buffered_indexes_);
    failed_ = !status.ok();
    if (status.ok()) {
        buffered_indexes_.clear();
    }
    return status;
}

Status FileIndexFormatV1::Writer::WriteIndexes(const FileIndexFormat::ColumnIndexes& indexes) {
    PAIMON_RETURN_NOT_OK(WriteHead(indexes));
    // Write body.
    DataOutputStream data_output(output_stream_);
    for (const auto& [column_name, column_indexes] : indexes) {
        for (const auto& [index_type, bytes] : column_indexes) {
            if (bytes) {
                PAIMON_RETURN_NOT_OK(data_output.WriteBytes(bytes));
            }
        }
    }
    finished_ = true;
    return Status::OK();
}

Status FileIndexFormatV1::Writer::Close() {
    if (closed_) {
        return Status::OK();
    }
    closed_ = true;
    PAIMON_RETURN_NOT_OK(output_stream_->Flush());
    return output_stream_->Close();
}

Result<int32_t> FileIndexFormatV1::Writer::CalculateHeadLength(
    const FileIndexFormat::ColumnIndexes& indexes) {
    // magic(8), version(4), header length(4), and column count(4).
    int64_t head_length = 8 + 4 + 4 + 4;
    int64_t body_length = 0;
    PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(indexes.size(), "file index column count"));
    for (const auto& [column_name, column_indexes] : indexes) {
        PAIMON_RETURN_NOT_OK(
            ValidateValueInRange<uint16_t>(column_name.size(), "file index column name length"));
        PAIMON_RETURN_NOT_OK(
            ValidateValueInRange<int32_t>(column_indexes.size(), "column index count"));
        // column name(2 + N) + index count(4)
        head_length += 2 + static_cast<int64_t>(column_name.size()) + 4;
        for (const auto& [index_type, bytes] : column_indexes) {
            PAIMON_RETURN_NOT_OK(
                ValidateValueInRange<uint16_t>(index_type.size(), "file index type name length"));
            // index type(2 + N) + body offset(4) + body length(4)
            head_length += 2 + static_cast<int64_t>(index_type.size()) + 4 + 4;
            if (bytes) {
                PAIMON_RETURN_NOT_OK(AddChecked(bytes->size(), "index body", &body_length));
            }
        }
    }

    head_length += 4;  // The trailing redundant-length field(4).
    PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(head_length, "file index header length"));
    int64_t container_length = head_length + body_length;
    PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(container_length, "file index size"));
    return static_cast<int32_t>(head_length);
}

Status FileIndexFormatV1::Writer::WriteHead(const FileIndexFormat::ColumnIndexes& indexes) {
    PAIMON_ASSIGN_OR_RAISE(int32_t head_length, CalculateHeadLength(indexes));
    DataOutputStream data_output(output_stream_);
    // Write magic.
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int64_t>(FileIndexFormat::MAGIC));
    // Write version.
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(FileIndexFormat::V_1));
    // Write head length.
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(head_length));
    // Write column count.
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(static_cast<int32_t>(indexes.size())));

    int64_t body_offset = head_length;
    for (const auto& [column_name, column_indexes] : indexes) {
        // Write column name.
        PAIMON_RETURN_NOT_OK(data_output.WriteString(column_name));
        // Write index count for the column.
        PAIMON_RETURN_NOT_OK(
            data_output.WriteValue<int32_t>(static_cast<int32_t>(column_indexes.size())));
        for (const auto& [index_type, bytes] : column_indexes) {
            // Write index type.
            PAIMON_RETURN_NOT_OK(data_output.WriteString(index_type));
            // Write body offset and length.
            if (bytes) {
                PAIMON_RETURN_NOT_OK(
                    data_output.WriteValue<int32_t>(static_cast<int32_t>(body_offset)));
                PAIMON_RETURN_NOT_OK(
                    data_output.WriteValue<int32_t>(static_cast<int32_t>(bytes->size())));
                body_offset += static_cast<int64_t>(bytes->size());
            } else {
                PAIMON_RETURN_NOT_OK(
                    data_output.WriteValue<int32_t>(FileIndexFormat::EMPTY_INDEX_FLAG));
                PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(0));
            }
        }
    }
    // Write redundant length for future format extensions.
    return data_output.WriteValue<int32_t>(kRedundantLength);
}

Result<FileIndexEntries> FileIndexFormatV1::ReadIndexEntries(
    const std::shared_ptr<InputStream>& input, const std::shared_ptr<MemoryPool>& pool,
    int64_t file_length) {
    DataInputStream data_input(input);
    PAIMON_ASSIGN_OR_RAISE(int32_t head_length, data_input.ReadValue<int32_t>());
    constexpr int64_t kDirectoryStart = 16;
    int64_t directory_length = head_length - kDirectoryStart;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> directory,
                           FileIndexFormatUtils::CreateFileIndexDirectory(
                               input, pool, kDirectoryStart, directory_length, file_length));
    DataInputStream in(directory);
    return FileIndexFormatUtils::ReadFileIndexEntries(in, FileIndexFormat::V_1);
}
}  // namespace paimon
