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

#include <optional>
#include <utility>

#include "arrow/api.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/macros.h"
#include "paimon/memory/bytes.h"

namespace paimon {
namespace {

constexpr int64_t kOffsetIndexMagic = 0x5041494D4F464653LL;
constexpr int32_t kOffsetIndexVersion = 1;

Result<std::vector<std::string>> EncodeKeyBatch(
    const std::shared_ptr<arrow::StructArray>& keys, const std::shared_ptr<arrow::Field>& key_field,
    const std::shared_ptr<KeySerializer>& key_serializer) {
    if (!keys || keys->num_fields() != 1 || !keys->field(0)->type()->Equals(key_field->type())) {
        return Status::Invalid(
            "user-defined key lookup must contain exactly the configured key field");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> literals,
                           LiteralConverter::ConvertLiteralsFromArray(*keys->field(0),
                                                                      /*own_data=*/true));
    std::vector<std::string> encoded;
    encoded.reserve(literals.size());
    for (const Literal& literal : literals) {
        if (literal.IsNull()) {
            encoded.emplace_back(1, '\0');
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, key_serializer->Serialize(literal));
        std::string key(1, '\1');
        key.append(bytes->data(), bytes->size());
        encoded.push_back(std::move(key));
    }
    return encoded;
}

Status WriteRaw(const std::shared_ptr<OutputStream>& output, const char* data, int64_t size) {
    PAIMON_ASSIGN_OR_RAISE(int64_t written, output->Write(data, size));
    if (written != size) {
        return Status::IOError("short write for realtime .offset index");
    }
    return Status::OK();
}

}  // namespace

Result<std::shared_ptr<MutableKeyOffsetIndex>> MutableKeyOffsetIndex::Create(
    const std::shared_ptr<arrow::Field>& key_field,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!key_field || !memory_pool) {
        return Status::Invalid("mutable key-offset index requires a key field and memory pool");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> serializer,
                           KeySerializer::Create(key_field->type(), memory_pool));
    return std::shared_ptr<MutableKeyOffsetIndex>(
        new MutableKeyOffsetIndex(key_field, std::move(serializer), std::make_shared<OffsetMap>()));
}

MutableKeyOffsetIndex::MutableKeyOffsetIndex(std::shared_ptr<arrow::Field> key_field,
                                             std::shared_ptr<KeySerializer> key_serializer,
                                             std::shared_ptr<OffsetMap> offsets)
    : key_field_(std::move(key_field)),
      key_serializer_(std::move(key_serializer)),
      offsets_(std::move(offsets)) {}

Result<std::vector<std::string>> MutableKeyOffsetIndex::EncodeKeys(
    const std::shared_ptr<arrow::StructArray>& keys) const {
    return EncodeKeyBatch(keys, key_field_, key_serializer_);
}

Status MutableKeyOffsetIndex::Add(const std::shared_ptr<arrow::StructArray>& keys,
                                  const std::shared_ptr<arrow::Int64Array>& offsets) {
    if (!offsets || offsets->null_count() != 0 || !keys || keys->length() != offsets->length()) {
        return Status::Invalid("key-offset index input columns are not aligned");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> encoded, EncodeKeys(keys));
    if (!offsets_.unique()) {
        offsets_ = std::make_shared<OffsetMap>(*offsets_);
    }
    for (int64_t row = 0; row < offsets->length(); ++row) {
        const int64_t offset = offsets->Value(row);
        if (offset < 0) {
            return Status::Invalid("realtime offset must not be negative");
        }
        (*offsets_)[encoded[row]] = offset;
    }
    return Status::OK();
}

Status MutableKeyOffsetIndex::Erase(const std::shared_ptr<arrow::StructArray>& keys) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> encoded, EncodeKeys(keys));
    if (!offsets_.unique()) {
        offsets_ = std::make_shared<OffsetMap>(*offsets_);
    }
    for (const std::string& key : encoded) {
        offsets_->erase(key);
    }
    return Status::OK();
}

Result<RoaringBitmap64> MutableKeyOffsetIndex::LookupOffsets(
    const std::shared_ptr<arrow::StructArray>& keys) const {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> encoded, EncodeKeys(keys));
    RoaringBitmap64 result;
    for (const std::string& key : encoded) {
        auto iter = offsets_->find(key);
        if (iter != offsets_->end()) {
            result.Add(iter->second);
        }
    }
    return result;
}

std::shared_ptr<const KeyOffsetLookup> MutableKeyOffsetIndex::Snapshot() const {
    return std::shared_ptr<const KeyOffsetLookup>(
        new MutableKeyOffsetIndex(key_field_, key_serializer_, offsets_));
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

Result<std::unique_ptr<DataFileKeyOffsetIndexWriter>> DataFileKeyOffsetIndexWriter::Create(
    const std::shared_ptr<arrow::Field>& key_field,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& memory_pool,
    const std::map<std::string, std::string>& options) {
    (void)options;
    if (!key_field || !path_factory || !file_system || !memory_pool) {
        return Status::Invalid("key-offset writer is missing a required dependency");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> serializer,
                           KeySerializer::Create(key_field->type(), memory_pool));
    return std::unique_ptr<DataFileKeyOffsetIndexWriter>(new DataFileKeyOffsetIndexWriter(
        key_field, path_factory, file_system, std::move(serializer)));
}

DataFileKeyOffsetIndexWriter::DataFileKeyOffsetIndexWriter(
    std::shared_ptr<arrow::Field> key_field, std::shared_ptr<DataFilePathFactory> path_factory,
    std::shared_ptr<FileSystem> file_system, std::shared_ptr<KeySerializer> key_serializer)
    : key_field_(std::move(key_field)),
      path_factory_(std::move(path_factory)),
      file_system_(std::move(file_system)),
      key_serializer_(std::move(key_serializer)) {}

Status DataFileKeyOffsetIndexWriter::AddBatch(const std::shared_ptr<arrow::StructArray>& keys,
                                              const std::shared_ptr<arrow::Int64Array>& offsets) {
    if (!keys || !offsets || keys->length() != offsets->length() || offsets->null_count() != 0) {
        return Status::Invalid("key-offset writer input columns are not aligned");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> encoded,
                           EncodeKeyBatch(keys, key_field_, key_serializer_));
    for (int64_t row = 0; row < offsets->length(); ++row) {
        const int64_t offset = offsets->Value(row);
        if (offset < 0) {
            return Status::Invalid("realtime offset must not be negative");
        }
        if (!offsets_.emplace(encoded[row], offset).second) {
            return Status::Invalid(
                "deduplicate data file contains more than one offset for a user-defined key");
        }
    }
    return Status::OK();
}

Result<std::string> DataFileKeyOffsetIndexWriter::Finish(
    const std::shared_ptr<DataFileMeta>& data_file) {
    if (!data_file || offsets_.empty()) {
        return Status::Invalid("cannot finish an empty key-offset index");
    }
    const std::string file_name = data_file->file_name + DataFileKeyOffsetIndexReader::kFileSuffix;
    output_path_ = path_factory_->ToAlignedPath(file_name, data_file);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> unique_output,
                           file_system_->Create(output_path_, /*overwrite=*/false));
    std::shared_ptr<OutputStream> output(std::move(unique_output));
    DataOutputStream data_output(output);
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int64_t>(kOffsetIndexMagic));
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(kOffsetIndexVersion));
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int64_t>(offsets_.size()));
    for (const auto& [key, offset] : offsets_) {
        PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(static_cast<int32_t>(key.size())));
        PAIMON_RETURN_NOT_OK(WriteRaw(output, key.data(), key.size()));
        PAIMON_RETURN_NOT_OK(data_output.WriteValue<int64_t>(offset));
    }
    PAIMON_RETURN_NOT_OK(output->Flush());
    PAIMON_RETURN_NOT_OK(output->Close());
    return file_name;
}

void DataFileKeyOffsetIndexWriter::Abort() {
    if (!output_path_.empty()) {
        [[maybe_unused]] Status status = file_system_->Delete(output_path_);
        output_path_.clear();
    }
}

Result<std::shared_ptr<DataFileKeyOffsetIndexReader>> DataFileKeyOffsetIndexReader::Create(
    const std::shared_ptr<arrow::Field>& key_field, const std::shared_ptr<DataFileMeta>& data_file,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& memory_pool,
    const std::map<std::string, std::string>& options) {
    (void)options;
    if (!key_field || !data_file || !path_factory || !file_system || !memory_pool) {
        return Status::Invalid("key-offset reader is missing a required dependency");
    }
    std::optional<std::string> file_name;
    for (const std::optional<std::string>& extra : data_file->extra_files) {
        if (extra && StringUtils::EndsWith(extra.value(), kFileSuffix)) {
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
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> unique_input, file_system->Open(file_path));
    std::shared_ptr<InputStream> input(std::move(unique_input));
    DataInputStream data_input(input);
    PAIMON_ASSIGN_OR_RAISE(int64_t magic, data_input.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t version, data_input.ReadValue<int32_t>());
    if (magic != kOffsetIndexMagic || version != kOffsetIndexVersion) {
        return Status::Invalid("unsupported realtime .offset index format: ", file_name.value());
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t entry_count, data_input.ReadValue<int64_t>());
    if (entry_count < 0) {
        return Status::Invalid("negative entry count in realtime .offset index");
    }
    auto offsets = std::make_shared<std::map<std::string, int64_t>>();
    for (int64_t entry = 0; entry < entry_count; ++entry) {
        PAIMON_ASSIGN_OR_RAISE(int32_t key_size, data_input.ReadValue<int32_t>());
        if (key_size <= 0) {
            return Status::Invalid("invalid key size in realtime .offset index");
        }
        std::string key(key_size, '\0');
        PAIMON_RETURN_NOT_OK(data_input.Read(key.data(), key_size));
        PAIMON_ASSIGN_OR_RAISE(int64_t offset, data_input.ReadValue<int64_t>());
        if (offset < 0) {
            return Status::Invalid("negative offset in realtime .offset index");
        }
        if (!offsets->emplace(std::move(key), offset).second) {
            return Status::Invalid("duplicate key in realtime .offset index");
        }
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t position, data_input.GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t length, data_input.Length());
    if (position != length) {
        return Status::Invalid("trailing bytes in realtime .offset index");
    }
    PAIMON_RETURN_NOT_OK(input->Close());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> serializer,
                           KeySerializer::Create(key_field->type(), memory_pool));
    return std::shared_ptr<DataFileKeyOffsetIndexReader>(
        new DataFileKeyOffsetIndexReader(key_field, std::move(serializer), std::move(offsets)));
}

Result<RoaringBitmap64> DataFileKeyOffsetIndexReader::LookupOffsets(
    const std::shared_ptr<arrow::StructArray>& keys) const {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> encoded,
                           EncodeKeyBatch(keys, key_field_, key_serializer_));
    RoaringBitmap64 result;
    for (const std::string& key : encoded) {
        auto iter = offsets_->find(key);
        if (iter != offsets_->end()) {
            result.Add(iter->second);
        }
    }
    return result;
}

}  // namespace paimon
