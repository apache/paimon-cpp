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

#include "paimon/common/file_index/file_index_format_v2.h"

#include <utility>

#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/io/offset_output_stream.h"
#include "paimon/common/utils/math.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {
namespace {
constexpr int64_t kV2PrefixLength = 12;
constexpr int64_t kV2TrailerLength = 12;
constexpr int64_t kV2TailMagic = 0x5041464944583032LL;

}  // namespace

FileIndexFormatV2::Writer::Writer(const std::shared_ptr<OutputStream>& output) : output_(output) {}

Result<std::unique_ptr<FileIndexFormatV2::Writer>> FileIndexFormatV2::Writer::Create(
    const std::shared_ptr<OutputStream>& output) {
    auto writer = std::unique_ptr<Writer>(new Writer(output));
    DataOutputStream data_output(output);
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int64_t>(FileIndexFormat::MAGIC));
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(FileIndexFormat::V_2));
    return writer;
}

Status FileIndexFormatV2::Writer::WriteIndex(const std::string& column, const std::string& type,
                                             const PayloadWriter& payload) {
    if (failed_ || finished_ || closed_) {
        return Status::Invalid("File index writer is not writable");
    }
    PAIMON_RETURN_NOT_OK(ValidateValueInRange<uint16_t>(column.size(), "column name length"));
    PAIMON_RETURN_NOT_OK(ValidateValueInRange<uint16_t>(type.size(), "index type length"));
    auto column_iter = entries_.find(column);
    if (column_iter != entries_.end() && column_iter->second.count(type)) {
        return Status::Invalid("Duplicate file index entry");
    }
    failed_ = true;
    int64_t start = FileIndexFormat::EMPTY_INDEX_FLAG;
    int64_t length = 0;
    if (payload) {
        PAIMON_ASSIGN_OR_RAISE(start, output_->GetPos());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OffsetOutputStream> payload_output,
                               OffsetOutputStream::Create(output_.get()));
        PAIMON_ASSIGN_OR_RAISE(bool written, payload(payload_output.get()));
        PAIMON_ASSIGN_OR_RAISE(int64_t end, output_->GetPos());
        length = end - start;
        if (!written) {
            if (length != 0) {
                return Status::Invalid("Absent file index payload must not write bytes");
            }
            start = FileIndexFormat::EMPTY_INDEX_FLAG;
        }
    }
    entries_[column][type] = {start, length};
    failed_ = false;
    return Status::OK();
}

Status FileIndexFormatV2::Writer::Finish() {
    if (failed_ || finished_ || closed_) {
        return Status::Invalid("File index writer is not writable");
    }
    failed_ = true;
    // Validate the footer size before emitting it; the Java trailer uses int32.
    int64_t footer_length = 4;
    for (const auto& [column, entries] : entries_) {
        footer_length += 2 + static_cast<int64_t>(column.size()) + 4;
        for (const auto& [type, range] : entries) {
            footer_length += 2 + static_cast<int64_t>(type.size()) + 16;
        }
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(footer_length, "footer length"));
    }
    DataOutputStream data_output(output_);
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(static_cast<int32_t>(entries_.size())));
    for (const auto& [column, entries] : entries_) {
        PAIMON_RETURN_NOT_OK(data_output.WriteString(column));
        PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(static_cast<int32_t>(entries.size())));
        for (const auto& [type, range] : entries) {
            PAIMON_RETURN_NOT_OK(data_output.WriteString(type));
            PAIMON_RETURN_NOT_OK(data_output.WriteValue<int64_t>(range.first));
            PAIMON_RETURN_NOT_OK(data_output.WriteValue<int64_t>(range.second));
        }
    }
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(static_cast<int32_t>(footer_length)));
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int64_t>(kV2TailMagic));
    finished_ = true;
    failed_ = false;
    return Status::OK();
}

Status FileIndexFormatV2::Writer::Close() {
    if (closed_) {
        return Status::OK();
    }
    closed_ = true;
    Status flush_status = output_->Flush();
    Status close_status = output_->Close();
    PAIMON_RETURN_NOT_OK(flush_status);
    return close_status;
}

Result<FileIndexFormatUtils::FileIndexEntries> FileIndexFormatV2::ReadIndexEntries(
    const std::shared_ptr<InputStream>& input, const std::shared_ptr<MemoryPool>& pool,
    int64_t file_length) {
    if (file_length < kV2PrefixLength + 4 + kV2TrailerLength) {
        return Status::Invalid("Truncated V2 file index");
    }
    DataInputStream data_input(input);
    PAIMON_RETURN_NOT_OK(data_input.Seek(file_length - kV2TrailerLength));
    PAIMON_ASSIGN_OR_RAISE(int32_t footer_length, data_input.ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t tail_magic, data_input.ReadValue<int64_t>());
    if (tail_magic != kV2TailMagic) {
        return Status::Invalid("Invalid file index tail magic");
    }
    if (footer_length < 4 || footer_length > file_length - kV2TrailerLength - kV2PrefixLength) {
        return Status::Invalid("Invalid file index footer length");
    }
    int64_t footer_start = file_length - kV2TrailerLength - footer_length;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> metadata_input,
                           FileIndexFormatUtils::CreateMetadataInputStream(
                               input, footer_start, footer_length, file_length, pool));
    DataInputStream in(metadata_input);
    return FileIndexFormatUtils::ReadFileIndexEntries(in, FileIndexFormat::V_2);
}
}  // namespace paimon
