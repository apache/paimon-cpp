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

#include "paimon/core/io/data_file_meta_first_row_id_legacy_serializer.h"

#include <cassert>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/internal_row_utils.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/status.h"

namespace paimon {
class Bytes;
class InternalArray;

const std::shared_ptr<arrow::DataType>& DataFileMetaFirstRowIdLegacySerializer::DataType() {
    static std::shared_ptr<arrow::DataType> schema = arrow::struct_(
        {arrow::field("_FILE_NAME", arrow::utf8(), /*nullable=*/false),
         arrow::field("_FILE_SIZE", arrow::int64(), /*nullable=*/false),
         arrow::field("_ROW_COUNT", arrow::int64(), /*nullable=*/false),
         arrow::field("_MIN_KEY", arrow::binary(), /*nullable=*/false),
         arrow::field("_MAX_KEY", arrow::binary(), /*nullable=*/false),
         arrow::field("_KEY_STATS", SimpleStats::DataType(), /*nullable=*/false),
         arrow::field("_VALUE_STATS", SimpleStats::DataType(), /*nullable=*/false),
         arrow::field("_MIN_SEQUENCE_NUMBER", arrow::int64(), /*nullable=*/false),
         arrow::field("_MAX_SEQUENCE_NUMBER", arrow::int64(), /*nullable=*/false),
         arrow::field("_SCHEMA_ID", arrow::int64(), /*nullable=*/false),
         arrow::field("_LEVEL", arrow::int32(), /*nullable=*/false),
         arrow::field("_EXTRA_FILES",
                      arrow::list(arrow::field("item", arrow::utf8(), /*nullable=*/false)),
                      /*nullable=*/false),
         arrow::field("_CREATION_TIME", arrow::timestamp(arrow::TimeUnit::NANO), /*nullable=*/true),
         arrow::field("_DELETE_ROW_COUNT", arrow::int64(), /*nullable=*/true),
         arrow::field("_EMBEDDED_FILE_INDEX", arrow::binary(), /*nullable=*/true),
         arrow::field("_FILE_SOURCE", arrow::int8(), /*nullable=*/true),
         arrow::field("_VALUE_STATS_COLS",
                      arrow::list(arrow::field("item", arrow::utf8(), /*nullable=*/false)),
                      /*nullable=*/true),
         arrow::field("_EXTERNAL_PATH", arrow::utf8(), /*nullable=*/true),
         arrow::field("_FIRST_ROW_ID", arrow::int64(), /*nullable=*/true)});
    return schema;
}

Result<BinaryRow> DataFileMetaFirstRowIdLegacySerializer::ToRow(
    const std::shared_ptr<DataFileMeta>& meta) const {
    assert(false);
    return Status::Invalid("to row for data file meta first row id legacy serializer is invalid");
}

Result<std::shared_ptr<DataFileMeta>> DataFileMetaFirstRowIdLegacySerializer::FromRow(
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
        external_path, first_row_id, /*write_cols=*/std::nullopt);
}

}  // namespace paimon
