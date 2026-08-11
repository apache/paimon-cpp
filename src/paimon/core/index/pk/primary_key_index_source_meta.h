/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/index/pk/primary_key_index_source_file.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon {
class IndexFileMeta;

/// Ordered source data files covered by a source-backed primary-key index payload.
///
/// Wire format (version 1), byte-compatible with Java `PrimaryKeyIndexSourceMeta`:
/// big-endian int32 version, big-endian int32 data level (> 0), big-endian int32 source
/// file count (> 0), then per source file a Java `writeUTF` file name (uint16 big-endian
/// byte length + modified UTF-8 bytes) and a big-endian int64 row count. Trailing bytes
/// are rejected.
class PrimaryKeyIndexSourceMeta {
 public:
    static constexpr int32_t VERSION = 1;

    static Result<PrimaryKeyIndexSourceMeta> Create(
        int32_t data_level, std::vector<PrimaryKeyIndexSourceFile> source_files);

    /// Extracts and deserializes the source metadata carried by an index file.
    static Result<PrimaryKeyIndexSourceMeta> FromIndexFile(const IndexFileMeta& index_file);

    static Result<PrimaryKeyIndexSourceMeta> Deserialize(const char* data, size_t length);

    Result<std::shared_ptr<Bytes>> Serialize(MemoryPool* pool) const;

    int32_t DataLevel() const {
        return data_level_;
    }

    const std::vector<PrimaryKeyIndexSourceFile>& SourceFiles() const {
        return source_files_;
    }

 private:
    PrimaryKeyIndexSourceMeta(int32_t data_level,
                              std::vector<PrimaryKeyIndexSourceFile> source_files)
        : data_level_(data_level), source_files_(std::move(source_files)) {}

    int32_t data_level_;
    std::vector<PrimaryKeyIndexSourceFile> source_files_;
};

}  // namespace paimon
