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

#include "paimon/core/realtime/realtime_key_offset_index.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "arrow/api.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/io/offset_input_stream.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/realtime/key_offset_index_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/macros.h"

namespace paimon {
Result<std::shared_ptr<MutableKeyOffsetIndex>> MutableKeyOffsetIndex::Create(
    const std::shared_ptr<arrow::Field>& key_field,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!key_field || !memory_pool) {
        return Status::Invalid("mutable key-offset index requires a key field and memory pool");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> serializer,
                           KeySerializer::Create(key_field->type(), memory_pool));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OffsetMap> offsets,
                           OffsetMap::Create(/*initial_capacity=*/1024, memory_pool));
    return std::shared_ptr<MutableKeyOffsetIndex>(new MutableKeyOffsetIndex(
        key_field, std::move(serializer), std::move(offsets), memory_pool));
}

MutableKeyOffsetIndex::MutableKeyOffsetIndex(std::shared_ptr<arrow::Field> key_field,
                                             std::shared_ptr<KeySerializer> key_serializer,
                                             std::shared_ptr<OffsetMap> offsets,
                                             std::shared_ptr<MemoryPool> memory_pool)
    : key_field_(std::move(key_field)),
      key_serializer_(std::move(key_serializer)),
      offsets_(std::move(offsets)),
      memory_pool_(std::move(memory_pool)) {}

Result<std::vector<uint64_t>> MutableKeyOffsetIndex::HashKeys(
    const std::shared_ptr<arrow::StructArray>& keys) const {
    return KeyOffsetIndexUtils::HashKeys(keys, key_field_, key_serializer_);
}

Status MutableKeyOffsetIndex::EnsureMutable() {
    if (offsets_.use_count() == 1) {
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<OffsetMap> copy,
        OffsetMap::Create(std::max<uint64_t>(1024, offsets_->Size() * 2), memory_pool_));
    Status status = Status::OK();
    offsets_->ForEach([&](uint64_t hash, int64_t offset) {
        if (status.ok() && offset >= 0) {
            status = copy->FindAndInsert(hash, offset);
        }
    });
    PAIMON_RETURN_NOT_OK(status);
    offsets_ = std::move(copy);
    return Status::OK();
}

Status MutableKeyOffsetIndex::Add(const std::shared_ptr<arrow::StructArray>& keys,
                                  const std::shared_ptr<arrow::Int64Array>& offsets) {
    if (!offsets || offsets->null_count() != 0 || !keys || keys->length() != offsets->length()) {
        return Status::Invalid("key-offset index input columns are not aligned");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<uint64_t> hashes, HashKeys(keys));
    for (int64_t row = 0; row < offsets->length(); ++row) {
        if (offsets->Value(row) < 0) {
            return Status::Invalid("realtime offset must not be negative");
        }
    }
    PAIMON_RETURN_NOT_OK(EnsureMutable());
    for (int64_t row = 0; row < offsets->length(); ++row) {
        PAIMON_RETURN_NOT_OK(offsets_->FindAndInsert(hashes[row], offsets->Value(row)));
    }
    return Status::OK();
}

Status MutableKeyOffsetIndex::Erase(const std::shared_ptr<arrow::StructArray>& keys) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<uint64_t> hashes, HashKeys(keys));
    PAIMON_RETURN_NOT_OK(EnsureMutable());
    for (uint64_t hash : hashes) {
        if (offsets_->Find(hash)) {
            PAIMON_RETURN_NOT_OK(offsets_->FindAndInsert(hash, /*value=*/-1));
        }
    }
    return Status::OK();
}

Result<RoaringBitmap64> MutableKeyOffsetIndex::LookupOffsets(
    const std::shared_ptr<arrow::StructArray>& keys) const {
    PAIMON_ASSIGN_OR_RAISE(std::vector<uint64_t> hashes, HashKeys(keys));
    RoaringBitmap64 result;
    for (uint64_t hash : hashes) {
        const std::optional<int64_t> offset = offsets_->Find(hash);
        if (offset && offset.value() >= 0) {
            result.Add(offset.value());
        }
    }
    return result;
}

std::shared_ptr<const MutableKeyOffsetIndex> MutableKeyOffsetIndex::Snapshot() const {
    return std::shared_ptr<const MutableKeyOffsetIndex>(
        new MutableKeyOffsetIndex(key_field_, key_serializer_, offsets_, memory_pool_));
}

uint64_t MutableKeyOffsetIndex::GetMemoryUsage() const {
    return offsets_->MemoryUsage();
}

Result<RoaringBitmap64> CompositeKeyOffsetLookup::LookupOffsets(
    const std::shared_ptr<arrow::StructArray>& keys) const {
    RoaringBitmap64 result;
    for (const std::shared_ptr<const KeyOffsetLookup>& lookup : lookups_) {
        if (!lookup) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 offsets, lookup->LookupOffsets(keys));
        result |= offsets;
    }
    return result;
}

Result<std::shared_ptr<DataFileKeyOffsetIndexReader>> DataFileKeyOffsetIndexReader::Create(
    uint32_t page_size, const std::shared_ptr<CacheManager>& cache_manager,
    const std::shared_ptr<arrow::Field>& key_field, const std::shared_ptr<DataFileMeta>& data_file,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!key_field || !data_file || !path_factory || !file_system || !memory_pool ||
        !cache_manager) {
        return Status::Invalid("key-offset reader is missing a required dependency");
    }
    std::optional<std::string> file_name;
    for (const std::optional<std::string>& extra : data_file->extra_files) {
        if (extra && StringUtils::EndsWith(extra.value(), KeyOffsetIndexUtils::kFileSuffix)) {
            if (file_name) {
                return Status::Invalid("data file contains multiple .offset sidecars");
            }
            file_name = extra.value();
        }
    }
    if (!file_name) {
        return Status::Invalid("data file does not contain the required .offset sidecar: ",
                               data_file->file_name);
    }

    const std::string file_path = path_factory->ToAlignedPath(file_name.value(), data_file);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system->Open(file_path));
    DataInputStream data_input(input);
    PAIMON_ASSIGN_OR_RAISE(int32_t version, data_input.ReadValue<int32_t>());
    if (version != KeyOffsetIndexUtils::kCurrentVersion) {
        return Status::Invalid("unsupported realtime .offset index version: ", version);
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t length, input->Length());
    constexpr int64_t kVersionSize = sizeof(int32_t);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<OffsetInputStream> hash_table_input,
        OffsetInputStream::Create(input, length - kVersionSize, kVersionSize, length));
    using Reader = DenseHashTableFileReader<uint64_t, int64_t>;
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<Reader> offsets,
        Reader::Create(page_size, cache_manager, file_path, hash_table_input, memory_pool));
    if (data_file->row_count < 0 ||
        offsets->Size() != static_cast<uint64_t>(data_file->row_count)) {
        return Status::Invalid("realtime .offset index entry count does not match data file");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> serializer,
                           KeySerializer::Create(key_field->type(), memory_pool));
    return std::shared_ptr<DataFileKeyOffsetIndexReader>(
        new DataFileKeyOffsetIndexReader(key_field, std::move(serializer), std::move(offsets)));
}

Result<RoaringBitmap64> DataFileKeyOffsetIndexReader::LookupOffsets(
    const std::shared_ptr<arrow::StructArray>& keys) const {
    PAIMON_ASSIGN_OR_RAISE(std::vector<uint64_t> hashes,
                           KeyOffsetIndexUtils::HashKeys(keys, key_field_, key_serializer_));
    RoaringBitmap64 result;
    for (uint64_t hash : hashes) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> offset, offsets_->Find(hash));
        if (offset) {
            if (offset.value() < 0) {
                return Status::Invalid("negative offset in realtime .offset index");
            }
            result.Add(offset.value());
        }
    }
    return result;
}

}  // namespace paimon
