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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type_fwd.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

class DataFilePathFactory;
class FileSystem;
class KeySerializer;
class MemoryPool;
struct DataFileMeta;

/// Immutable user-defined deduplicate key to global realtime-offset lookup.
class KeyOffsetLookup {
 public:
    virtual ~KeyOffsetLookup() = default;

    virtual Result<RoaringBitmap64> LookupOffsets(
        const std::shared_ptr<arrow::StructArray>& keys) const = 0;
};

/// Copy-on-write in-memory implementation used by one building segment. Each key retains only its
/// latest live offset; overwrites replace the value and deletes erase the key.
class MutableKeyOffsetIndex final : public KeyOffsetLookup {
 public:
    static Result<std::shared_ptr<MutableKeyOffsetIndex>> Create(
        const std::shared_ptr<arrow::Field>& key_field,
        const std::shared_ptr<MemoryPool>& memory_pool);

    Status Add(const std::shared_ptr<arrow::StructArray>& keys,
               const std::shared_ptr<arrow::Int64Array>& offsets);

    Status Erase(const std::shared_ptr<arrow::StructArray>& keys);

    Result<RoaringBitmap64> LookupOffsets(
        const std::shared_ptr<arrow::StructArray>& keys) const override;

    std::shared_ptr<const KeyOffsetLookup> Snapshot() const;

 private:
    using OffsetMap = std::map<std::string, int64_t>;

    MutableKeyOffsetIndex(std::shared_ptr<arrow::Field> key_field,
                          std::shared_ptr<KeySerializer> key_serializer,
                          std::shared_ptr<OffsetMap> offsets);

    Result<std::vector<std::string>> EncodeKeys(
        const std::shared_ptr<arrow::StructArray>& keys) const;

    std::shared_ptr<arrow::Field> key_field_;
    std::shared_ptr<KeySerializer> key_serializer_;
    std::shared_ptr<OffsetMap> offsets_;
};

/// Unions a fixed set of immutable lookups. The set itself is pinned by a writer lookup view.
class CompositeKeyOffsetLookup final : public KeyOffsetLookup {
 public:
    explicit CompositeKeyOffsetLookup(std::vector<std::shared_ptr<const KeyOffsetLookup>>&& lookups)
        : lookups_(std::move(lookups)) {}

    Result<RoaringBitmap64> LookupOffsets(
        const std::shared_ptr<arrow::StructArray>& keys) const override;

 private:
    std::vector<std::shared_ptr<const KeyOffsetLookup>> lookups_;
};

/// Buffers one data file's surviving key/offset pairs and writes a temporary `.offset` sidecar.
/// Segment-level deduplication guarantees at most one offset per key in the resulting data file.
/// TODO(xinyu.lxy): This writer and its `.offset` sidecar format are experimental. The format will
/// be revised before production use.
class DataFileKeyOffsetIndexWriter {
 public:
    static Result<std::unique_ptr<DataFileKeyOffsetIndexWriter>> Create(
        const std::shared_ptr<arrow::Field>& key_field,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<FileSystem>& file_system,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::map<std::string, std::string>& options);

    Status AddBatch(const std::shared_ptr<arrow::StructArray>& keys,
                    const std::shared_ptr<arrow::Int64Array>& offsets);

    Result<std::string> Finish(const std::shared_ptr<DataFileMeta>& data_file);

    void Abort();

 private:
    DataFileKeyOffsetIndexWriter(std::shared_ptr<arrow::Field> key_field,
                                 std::shared_ptr<DataFilePathFactory> path_factory,
                                 std::shared_ptr<FileSystem> file_system,
                                 std::shared_ptr<KeySerializer> key_serializer);

    std::shared_ptr<arrow::Field> key_field_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<KeySerializer> key_serializer_;
    std::map<std::string, int64_t> offsets_;
    std::string output_path_;
};

/// Reads one temporary `.offset` sidecar attached to a data file.
class DataFileKeyOffsetIndexReader final : public KeyOffsetLookup {
 public:
    static constexpr const char* kFileSuffix = ".offset";

    static Result<std::shared_ptr<DataFileKeyOffsetIndexReader>> Create(
        const std::shared_ptr<arrow::Field>& key_field,
        const std::shared_ptr<DataFileMeta>& data_file,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<FileSystem>& file_system,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::map<std::string, std::string>& options);

    Result<RoaringBitmap64> LookupOffsets(
        const std::shared_ptr<arrow::StructArray>& keys) const override;

 private:
    DataFileKeyOffsetIndexReader(std::shared_ptr<arrow::Field> key_field,
                                 std::shared_ptr<KeySerializer> key_serializer,
                                 std::shared_ptr<const std::map<std::string, int64_t>> offsets)
        : key_field_(std::move(key_field)),
          key_serializer_(std::move(key_serializer)),
          offsets_(std::move(offsets)) {}

    std::shared_ptr<arrow::Field> key_field_;
    std::shared_ptr<KeySerializer> key_serializer_;
    std::shared_ptr<const std::map<std::string, int64_t>> offsets_;
};

}  // namespace paimon
