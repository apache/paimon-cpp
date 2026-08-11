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

#include "paimon/core/index/pk/primary_key_index_source_meta.h"

#include <algorithm>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/java_modified_utf8.h"
#include "paimon/core/index/index_file_meta.h"

namespace paimon {
namespace {
// Each serialized entry needs at least the two-byte writeUTF length and one int64 row count,
// mirroring the defensive source file count cap of the Java deserializer.
constexpr size_t kMinBytesPerSourceFile = sizeof(uint16_t) + sizeof(int64_t);
constexpr size_t kMaxInitialSourceFileCapacity = 1024;

void AppendBigEndian32(int32_t value, std::string* out) {
    auto bits = static_cast<uint32_t>(value);
    out->push_back(static_cast<char>((bits >> 24) & 0xFF));
    out->push_back(static_cast<char>((bits >> 16) & 0xFF));
    out->push_back(static_cast<char>((bits >> 8) & 0xFF));
    out->push_back(static_cast<char>(bits & 0xFF));
}

void AppendBigEndian64(int64_t value, std::string* out) {
    auto bits = static_cast<uint64_t>(value);
    for (int32_t shift = 56; shift >= 0; shift -= 8) {
        out->push_back(static_cast<char>((bits >> shift) & 0xFF));
    }
}

class BigEndianCursor {
 public:
    BigEndianCursor(const char* data, size_t length) : data_(data), length_(length) {}

    Result<int32_t> ReadInt32() {
        PAIMON_RETURN_NOT_OK(CheckAvailable(sizeof(int32_t)));
        uint32_t bits = 0;
        for (size_t k = 0; k < sizeof(int32_t); k++) {
            bits = (bits << 8) | static_cast<uint8_t>(data_[position_ + k]);
        }
        position_ += sizeof(int32_t);
        return static_cast<int32_t>(bits);
    }

    Result<int64_t> ReadInt64() {
        PAIMON_RETURN_NOT_OK(CheckAvailable(sizeof(int64_t)));
        uint64_t bits = 0;
        for (size_t k = 0; k < sizeof(int64_t); k++) {
            bits = (bits << 8) | static_cast<uint8_t>(data_[position_ + k]);
        }
        position_ += sizeof(int64_t);
        return static_cast<int64_t>(bits);
    }

    Result<uint16_t> ReadUint16() {
        PAIMON_RETURN_NOT_OK(CheckAvailable(sizeof(uint16_t)));
        auto bits = static_cast<uint16_t>((static_cast<uint8_t>(data_[position_]) << 8) |
                                          static_cast<uint8_t>(data_[position_ + 1]));
        position_ += sizeof(uint16_t);
        return bits;
    }

    Result<std::string_view> ReadBytes(size_t length) {
        PAIMON_RETURN_NOT_OK(CheckAvailable(length));
        std::string_view view(data_ + position_, length);
        position_ += length;
        return view;
    }

    size_t Available() const {
        return length_ - position_;
    }

 private:
    Status CheckAvailable(size_t needed) const {
        if (length_ - position_ < needed) {
            return Status::Invalid(fmt::format(
                "Failed to deserialize index source metadata: need {} bytes at offset {} but "
                "only {} remain.",
                needed, position_, length_ - position_));
        }
        return Status::OK();
    }

    const char* data_;
    size_t length_;
    size_t position_ = 0;
};
}  // namespace

Result<PrimaryKeyIndexSourceMeta> PrimaryKeyIndexSourceMeta::Create(
    int32_t data_level, std::vector<PrimaryKeyIndexSourceFile> source_files) {
    if (data_level <= 0) {
        return Status::Invalid("Primary-key index data level must be positive.");
    }
    if (source_files.empty()) {
        return Status::Invalid("An index must reference source files.");
    }
    for (const PrimaryKeyIndexSourceFile& source_file : source_files) {
        if (source_file.row_count < 0) {
            return Status::Invalid(fmt::format("Source file {} has a negative row count {}.",
                                               source_file.file_name, source_file.row_count));
        }
    }
    return PrimaryKeyIndexSourceMeta(data_level, std::move(source_files));
}

Result<PrimaryKeyIndexSourceMeta> PrimaryKeyIndexSourceMeta::FromIndexFile(
    const IndexFileMeta& index_file) {
    const std::optional<GlobalIndexMeta>& global_index_meta = index_file.GetGlobalIndexMeta();
    if (global_index_meta == std::nullopt || global_index_meta.value().source_meta == nullptr) {
        return Status::Invalid(
            fmt::format("Index file {} has no source metadata.", index_file.FileName()));
    }
    const std::shared_ptr<Bytes>& source_meta = global_index_meta.value().source_meta;
    return Deserialize(source_meta->data(), source_meta->size());
}

Result<PrimaryKeyIndexSourceMeta> PrimaryKeyIndexSourceMeta::Deserialize(const char* data,
                                                                         size_t length) {
    BigEndianCursor cursor(data, length);
    PAIMON_ASSIGN_OR_RAISE(int32_t version, cursor.ReadInt32());
    if (version != VERSION) {
        return Status::Invalid(fmt::format("Unsupported index source version: {}.", version));
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t data_level, cursor.ReadInt32());
    PAIMON_ASSIGN_OR_RAISE(int32_t source_file_count, cursor.ReadInt32());
    if (source_file_count <= 0) {
        return Status::Invalid("An index must reference source files.");
    }
    size_t maximum_source_file_count = cursor.Available() / kMinBytesPerSourceFile;
    if (static_cast<size_t>(source_file_count) > maximum_source_file_count) {
        return Status::Invalid(fmt::format(
            "Failed to deserialize index source metadata: source file count {} exceeds the "
            "maximum {} allowed by the remaining bytes.",
            source_file_count, maximum_source_file_count));
    }
    std::vector<PrimaryKeyIndexSourceFile> source_files;
    source_files.reserve(
        std::min(static_cast<size_t>(source_file_count), kMaxInitialSourceFileCapacity));
    for (int32_t i = 0; i < source_file_count; i++) {
        PAIMON_ASSIGN_OR_RAISE(uint16_t name_length, cursor.ReadUint16());
        PAIMON_ASSIGN_OR_RAISE(std::string_view name_bytes, cursor.ReadBytes(name_length));
        PAIMON_ASSIGN_OR_RAISE(std::string file_name, JavaModifiedUtf8::Decode(name_bytes));
        PAIMON_ASSIGN_OR_RAISE(int64_t row_count, cursor.ReadInt64());
        source_files.emplace_back(std::move(file_name), row_count);
    }
    if (cursor.Available() != 0) {
        return Status::Invalid("Unexpected trailing bytes in index source metadata.");
    }
    return Create(data_level, std::move(source_files));
}

Result<std::shared_ptr<Bytes>> PrimaryKeyIndexSourceMeta::Serialize(MemoryPool* pool) const {
    std::string buffer;
    AppendBigEndian32(VERSION, &buffer);
    AppendBigEndian32(data_level_, &buffer);
    AppendBigEndian32(static_cast<int32_t>(source_files_.size()), &buffer);
    for (const PrimaryKeyIndexSourceFile& source_file : source_files_) {
        PAIMON_ASSIGN_OR_RAISE(std::string encoded_name,
                               JavaModifiedUtf8::Encode(source_file.file_name));
        if (encoded_name.size() > std::numeric_limits<uint16_t>::max()) {
            return Status::Invalid(fmt::format(
                "Source file name is too long for writeUTF: {} bytes.", encoded_name.size()));
        }
        auto name_length = static_cast<uint16_t>(encoded_name.size());
        buffer.push_back(static_cast<char>((name_length >> 8) & 0xFF));
        buffer.push_back(static_cast<char>(name_length & 0xFF));
        buffer.append(encoded_name);
        AppendBigEndian64(source_file.row_count, &buffer);
    }
    return std::make_shared<Bytes>(buffer, pool);
}

}  // namespace paimon
