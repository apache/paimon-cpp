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

#include "paimon/core/io/data_file_meta_serializer.h"

#include <cassert>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/internal_row_utils.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/status.h"

namespace paimon {

class Bytes;
class InternalArray;
class MemoryPool;

Result<BinaryRow> DataFileMetaSerializer::ToRow(const std::shared_ptr<DataFileMeta>& meta) const {
    BinaryRow row(20);
    BinaryRowWriter writer(&row, 32 * 1024, pool_.get());
    writer.WriteString(0, BinaryString::FromString(meta->file_name, pool_.get()));
    writer.WriteLong(1, meta->file_size);
    writer.WriteLong(2, meta->row_count);
    auto min_key_bytes = SerializationUtils::SerializeBinaryRow(meta->min_key, pool_.get());
    writer.WriteBinary(3, *min_key_bytes);
    auto max_key_bytes = SerializationUtils::SerializeBinaryRow(meta->max_key, pool_.get());
    writer.WriteBinary(4, *max_key_bytes);
    writer.WriteRow(5, meta->key_stats.ToRow());
    writer.WriteRow(6, meta->value_stats.ToRow());
    writer.WriteLong(7, meta->min_sequence_number);
    writer.WriteLong(8, meta->max_sequence_number);
    writer.WriteLong(9, meta->schema_id);
    writer.WriteInt(10, meta->level);
    writer.WriteArray(11, InternalRowUtils::ToStringArrayData(meta->extra_files, pool_));
    writer.WriteTimestamp(12, meta->creation_time, 3);
    if (meta->delete_row_count == std::nullopt) {
        writer.SetNullAt(13);
    } else {
        writer.WriteLong(13, meta->delete_row_count.value());
    }
    if (meta->embedded_index == nullptr) {
        writer.SetNullAt(14);
    } else {
        writer.WriteBinary(14, *meta->embedded_index);
    }
    if (meta->file_source == std::nullopt) {
        writer.SetNullAt(15);
    } else {
        writer.WriteByte(15, meta->file_source.value().ToByteValue());
    }
    if (meta->value_stats_cols == std::nullopt) {
        writer.SetNullAt(16);
    } else {
        writer.WriteArray(
            16, InternalRowUtils::ToNotNullStringArrayData(meta->value_stats_cols.value(), pool_));
    }
    if (meta->external_path == std::nullopt) {
        writer.SetNullAt(17);
    } else {
        writer.WriteString(17, BinaryString::FromString(meta->external_path.value(), pool_.get()));
    }
    if (meta->first_row_id == std::nullopt) {
        writer.SetNullAt(18);
    } else {
        writer.WriteLong(18, meta->first_row_id.value());
    }
    if (meta->write_cols == std::nullopt) {
        writer.SetNullAt(19);
    } else {
        writer.WriteArray(
            19, InternalRowUtils::ToNotNullStringArrayData(meta->write_cols.value(), pool_));
    }
    writer.Complete();
    return row;
}

Result<std::shared_ptr<DataFileMeta>> DataFileMetaSerializer::FromRow(
    const InternalRow& row) const {
    auto file_name = row.GetString(0);
    auto file_size = row.GetLong(1);
    auto row_count = row.GetLong(2);
    auto min_key = row.GetBinary(3);
    auto max_key = row.GetBinary(4);
    auto key_stats_row = row.GetRow(5, 3);
    auto value_stats_row = row.GetRow(6, 3);
    auto min_sequence_number = row.GetLong(7);
    auto max_sequence_number = row.GetLong(8);
    auto schema_id = row.GetLong(9);
    auto level = row.GetInt(10);
    std::shared_ptr<InternalArray> extra_files = row.GetArray(11);
    auto creation_time = row.GetTimestamp(12, 3);

    assert(min_key && max_key && key_stats_row && value_stats_row);
    if (extra_files == nullptr) {
        return Status::Invalid("extra files is empty");
    }

    std::optional<int64_t> delete_row_count;
    if (!row.IsNullAt(13)) {
        delete_row_count = row.GetLong(13);
    }
    std::shared_ptr<Bytes> embedded_file_index;
    if (!row.IsNullAt(14)) {
        embedded_file_index = row.GetBinary(14);
    }

    std::optional<FileSource> file_source;
    if (!row.IsNullAt(15)) {
        PAIMON_ASSIGN_OR_RAISE(file_source, FileSource::FromByteValue(row.GetByte(15)));
    }

    std::optional<std::vector<std::string>> value_stats_cols;
    if (!row.IsNullAt(16)) {
        std::shared_ptr<InternalArray> array = row.GetArray(16);
        if (array == nullptr) {
            return Status::Invalid("invalid value stats cols");
        }
        value_stats_cols = InternalRowUtils::FromNotNullStringArrayData(array.get());
    }

    std::optional<std::string> external_path;
    if (!row.IsNullAt(17)) {
        external_path = row.GetString(17).ToString();
    }
    std::optional<int64_t> first_row_id;
    if (!row.IsNullAt(18)) {
        first_row_id = row.GetLong(18);
    }

    std::optional<std::vector<std::string>> write_cols;
    if (!row.IsNullAt(19)) {
        std::shared_ptr<InternalArray> array = row.GetArray(19);
        if (array == nullptr) {
            return Status::Invalid("invalid write cols");
        }
        write_cols = InternalRowUtils::FromNotNullStringArrayData(array.get());
    }
    PAIMON_ASSIGN_OR_RAISE(BinaryRow min_values, SerializationUtils::DeserializeBinaryRow(min_key));
    PAIMON_ASSIGN_OR_RAISE(BinaryRow max_values, SerializationUtils::DeserializeBinaryRow(max_key));
    PAIMON_ASSIGN_OR_RAISE(SimpleStats key_stats,
                           SimpleStats::FromRow(key_stats_row.get(), pool_.get()));
    PAIMON_ASSIGN_OR_RAISE(SimpleStats value_stats,
                           SimpleStats::FromRow(value_stats_row.get(), pool_.get()));
    return std::make_shared<DataFileMeta>(
        file_name.ToString(), file_size, row_count, min_values, max_values, key_stats, value_stats,
        min_sequence_number, max_sequence_number, schema_id, level,
        InternalRowUtils::FromStringArrayData(extra_files.get()), creation_time, delete_row_count,
        embedded_file_index, file_source, std::optional<std::vector<std::string>>(value_stats_cols),
        external_path, first_row_id, write_cols);
}

}  // namespace paimon
