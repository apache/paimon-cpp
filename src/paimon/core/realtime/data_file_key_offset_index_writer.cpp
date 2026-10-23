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

#include "paimon/core/realtime/data_file_key_offset_index_writer.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/compute/cast.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/dense_hash_table.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/core/realtime/key_offset_index_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"

namespace paimon {
Result<std::unique_ptr<DataFileKeyOffsetIndexWriter>> DataFileKeyOffsetIndexWriter::Create(
    const std::shared_ptr<arrow::Field>& key_field, const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!key_field || !file_system || !memory_pool) {
        return Status::Invalid("key-offset writer is missing a required dependency");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> serializer,
                           KeySerializer::Create(key_field->type(), memory_pool));
    using OffsetMap = HashMap<uint64_t, int64_t>;
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OffsetMap> offsets,
                           OffsetMap::Create(/*initial_capacity=*/1024, memory_pool));
    return std::unique_ptr<DataFileKeyOffsetIndexWriter>(new DataFileKeyOffsetIndexWriter(
        key_field, file_system, serializer, std::move(offsets), memory_pool));
}

DataFileKeyOffsetIndexWriter::DataFileKeyOffsetIndexWriter(
    const std::shared_ptr<arrow::Field>& key_field, const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<KeySerializer>& key_serializer,
    std::unique_ptr<HashMap<uint64_t, int64_t>> offsets,
    const std::shared_ptr<MemoryPool>& memory_pool)
    : key_field_(key_field),
      file_system_(file_system),
      key_serializer_(key_serializer),
      offsets_(std::move(offsets)),
      memory_pool_(memory_pool) {}

Status DataFileKeyOffsetIndexWriter::AddBatch(
    const std::shared_ptr<arrow::StructArray>& logical_batch) {
    if (finished_) {
        return Status::Invalid("key-offset writer has already finished");
    }
    if (!logical_batch || logical_batch->null_count() != 0) {
        return Status::Invalid("key-offset writer requires non-null logical rows");
    }
    // The adaptor must outlive any decoded arrays allocated through it.
    std::shared_ptr<arrow::MemoryPool> arrow_pool;
    std::shared_ptr<arrow::Array> key = logical_batch->GetFieldByName(key_field_->name());
    std::shared_ptr<arrow::Array> offsets =
        logical_batch->GetFieldByName(SpecialFields::RealtimeOffset().Name());
    if (!key || !offsets) {
        return Status::Invalid("key-offset writer requires the key and realtime offset columns");
    }
    // Compaction may forward dictionary-encoded columns. Materialize only the index inputs.
    if (key->type_id() == arrow::Type::DICTIONARY ||
        offsets->type_id() == arrow::Type::DICTIONARY) {
        arrow_pool = GetArrowPool(memory_pool_);
    }
    if (key->type_id() == arrow::Type::DICTIONARY) {
        PAIMON_ASSIGN_OR_RAISE(
            key, CastingUtils::Cast(key, key_field_->type(), arrow::compute::CastOptions::Safe(),
                                    arrow_pool.get()));
    }
    if (offsets->type_id() == arrow::Type::DICTIONARY) {
        PAIMON_ASSIGN_OR_RAISE(
            offsets, CastingUtils::Cast(offsets, arrow::int64(),
                                        arrow::compute::CastOptions::Safe(), arrow_pool.get()));
    }
    if (offsets->type_id() != arrow::Type::INT64) {
        return Status::Invalid("realtime offset must have int64 type");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> keys,
                                      arrow::StructArray::Make({key}, {key_field_}));
    const std::shared_ptr<arrow::Int64Array> typed_offsets =
        checked_pointer_cast<arrow::Int64Array>(offsets);
    if (keys->length() != typed_offsets->length() || typed_offsets->null_count() != 0) {
        return Status::Invalid("key-offset writer input columns are not aligned");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<uint64_t> hashes,
                           KeyOffsetIndexUtils::HashKeys(keys, key_field_, key_serializer_));
    for (int64_t row = 0; row < typed_offsets->length(); ++row) {
        const int64_t offset = typed_offsets->Value(row);
        if (offset < 0) {
            return Status::Invalid("realtime offset must not be negative");
        }
        if (offsets_->Find(hashes[row])) {
            return Status::Invalid(
                "deduplicate data file contains more than one offset for a deduplicate key");
        }
        PAIMON_RETURN_NOT_OK(offsets_->FindAndInsert(hashes[row], offset));
    }
    return Status::OK();
}

Result<std::string> DataFileKeyOffsetIndexWriter::Finish(const std::string& data_file_path,
                                                         int64_t row_count) {
    if (finished_) {
        return Status::Invalid("key-offset writer has already finished");
    }
    if (data_file_path.empty() || offsets_->Size() == 0) {
        return Status::Invalid("cannot finish an empty key-offset index");
    }
    if (row_count < 0 || offsets_->Size() != static_cast<uint64_t>(row_count)) {
        return Status::Invalid("realtime .offset index entry count does not match data file");
    }
    finished_ = true;
    using HashTable = DenseHashTable<uint64_t, int64_t>;
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<HashTable> hash_table,
                           HashTable::Create(offsets_->Size(), /*occupancy_pct=*/80, memory_pool_));
    Status status = Status::OK();
    offsets_->ForEach([&](uint64_t hash, int64_t offset) {
        if (status.ok()) {
            status = hash_table->Insert(hash, offset);
        }
    });
    PAIMON_RETURN_NOT_OK(status);
    const std::string output_path = data_file_path + KeyOffsetIndexUtils::kFileSuffix;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> output,
                           file_system_->Create(output_path, /*overwrite=*/false));
    output_path_ = output_path;
    ScopeGuard guard([this, &output]() {
        [[maybe_unused]] Status status = output->Close();
        Abort();
    });
    DataOutputStream data_output(output);
    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(KeyOffsetIndexUtils::kCurrentVersion));
    PAIMON_RETURN_NOT_OK(hash_table->WriteTo(output.get()));
    PAIMON_RETURN_NOT_OK(output->Flush());
    PAIMON_RETURN_NOT_OK(output->Close());
    guard.Release();
    return PathUtil::GetName(output_path_);
}

void DataFileKeyOffsetIndexWriter::Abort() {
    if (!output_path_.empty()) {
        [[maybe_unused]] Status status = file_system_->Delete(output_path_);
        output_path_.clear();
    }
}

}  // namespace paimon
