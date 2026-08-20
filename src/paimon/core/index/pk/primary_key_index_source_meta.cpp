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
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"

namespace paimon {
namespace {
// Each serialized entry needs at least one uint16 string length and one int64 row count,
// mirroring the defensive source file count cap of the Java deserializer.
constexpr int64_t kMinBytesPerSourceFile = sizeof(uint16_t) + sizeof(int64_t);
constexpr size_t kMaxInitialSourceFileCapacity = 1024;
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
    if (data == nullptr) {
        return Status::Invalid("Cannot deserialize index source metadata from a null buffer.");
    }
    if (length > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        return Status::Invalid(
            fmt::format("Index source metadata length {} exceeds the supported maximum {}.", length,
                        std::numeric_limits<int64_t>::max()));
    }

    auto input_stream = std::make_shared<ByteArrayInputStream>(data, static_cast<int64_t>(length));
    DataInputStream input(input_stream);
    PAIMON_ASSIGN_OR_RAISE(int32_t version, input.ReadValue<int32_t>());
    if (version != VERSION) {
        return Status::Invalid(fmt::format("Unsupported index source version: {}.", version));
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t data_level, input.ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t source_file_count, input.ReadValue<int32_t>());
    if (source_file_count <= 0) {
        return Status::Invalid("An index must reference source files.");
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t position, input.GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, input.Length());
    int64_t maximum_source_file_count = (stream_length - position) / kMinBytesPerSourceFile;
    if (static_cast<int64_t>(source_file_count) > maximum_source_file_count) {
        return Status::Invalid(fmt::format(
            "Failed to deserialize index source metadata: source file count {} exceeds the "
            "maximum {} allowed by the remaining bytes.",
            source_file_count, maximum_source_file_count));
    }
    std::vector<PrimaryKeyIndexSourceFile> source_files;
    source_files.reserve(
        std::min(static_cast<size_t>(source_file_count), kMaxInitialSourceFileCapacity));
    for (int32_t i = 0; i < source_file_count; i++) {
        PAIMON_ASSIGN_OR_RAISE(std::string file_name, input.ReadString());
        PAIMON_ASSIGN_OR_RAISE(int64_t row_count, input.ReadValue<int64_t>());
        source_files.emplace_back(std::move(file_name), row_count);
    }
    PAIMON_ASSIGN_OR_RAISE(position, input.GetPos());
    if (position != stream_length) {
        return Status::Invalid("Unexpected trailing bytes in index source metadata.");
    }
    return Create(data_level, std::move(source_files));
}

Result<std::shared_ptr<Bytes>> PrimaryKeyIndexSourceMeta::Serialize(
    const std::shared_ptr<MemoryPool>& pool) const {
    if (pool == nullptr) {
        return Status::Invalid("Cannot serialize index source metadata with a null memory pool.");
    }
    if (source_files_.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        return Status::Invalid(
            fmt::format("Index source file count {} exceeds the supported maximum {}.",
                        source_files_.size(), std::numeric_limits<int32_t>::max()));
    }

    int64_t serialized_size = 3 * static_cast<int64_t>(sizeof(int32_t));
    for (const PrimaryKeyIndexSourceFile& source_file : source_files_) {
        if (source_file.file_name.size() > std::numeric_limits<uint16_t>::max()) {
            return Status::Invalid(
                fmt::format("Source file name is too long for a 16-bit length: {} bytes.",
                            source_file.file_name.size()));
        }
        int64_t entry_size =
            kMinBytesPerSourceFile + static_cast<int64_t>(source_file.file_name.size());
        if (serialized_size > std::numeric_limits<int32_t>::max() - entry_size) {
            return Status::Invalid(fmt::format(
                "Serialized index source metadata exceeds the supported maximum {} bytes.",
                std::numeric_limits<int32_t>::max()));
        }
        serialized_size += entry_size;
    }

    MemorySegmentOutputStream output(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    output.WriteValue<int32_t>(VERSION);
    output.WriteValue<int32_t>(data_level_);
    output.WriteValue<int32_t>(static_cast<int32_t>(source_files_.size()));
    for (const PrimaryKeyIndexSourceFile& source_file : source_files_) {
        auto name_length = static_cast<uint16_t>(source_file.file_name.size());
        output.WriteValue<uint16_t>(name_length);
        output.Write(source_file.file_name.data(), name_length);
        output.WriteValue<int64_t>(source_file.row_count);
    }
    PAIMON_UNIQUE_PTR<Bytes> bytes = MemorySegmentUtils::CopyToBytes(
        output.Segments(), /*offset=*/0, static_cast<int32_t>(serialized_size), pool.get());
    return std::shared_ptr<Bytes>(std::move(bytes));
}

}  // namespace paimon
