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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/format/lance/lance_stats_extractor.h"

#include <optional>
#include <utility>

#include "arrow/api.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/defs.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/lance/lance_file_batch_reader.h"
#include "paimon/fs/file_system.h"

namespace paimon::lance {

Result<ColumnStatsVector> LanceStatsExtractor::Extract(
    const std::shared_ptr<FileSystem>& file_system, const std::string& path,
    const std::shared_ptr<MemoryPool>& pool) {
    using StatsWithFileInfo = std::pair<ColumnStatsVector, FileInfo>;
    PAIMON_ASSIGN_OR_RAISE(StatsWithFileInfo result, ExtractWithFileInfo(file_system, path, pool));
    return std::move(result.first);
}

Result<std::pair<ColumnStatsVector, FormatStatsExtractor::FileInfo>>
LanceStatsExtractor::ExtractWithFileInfo(const std::shared_ptr<FileSystem>& file_system,
                                         const std::string& path,
                                         const std::shared_ptr<MemoryPool>& pool) {
    if (file_system == nullptr || pool == nullptr) {
        return Status::Invalid("Lance stats extractor requires file system and memory pool");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system->Open(path));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<LanceFileBatchReader> reader,
        LanceFileBatchReader::Create(input, /*batch_size=*/1, /*batch_readahead=*/1, options_, pool,
                                     GetArrowPool(pool)));
    ColumnStatsVector stats;
    stats.reserve(schema_->num_fields());
    for (const std::shared_ptr<arrow::Field>& field : schema_->fields()) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ColumnStats> column_stats,
                               CreateEmptyStats(field->type()));
        stats.push_back(std::move(column_stats));
    }
    PAIMON_ASSIGN_OR_RAISE(uint64_t row_count, reader->GetNumberOfRows());
    return std::make_pair(std::move(stats), FileInfo(row_count));
}

Result<std::unique_ptr<ColumnStats>> LanceStatsExtractor::CreateEmptyStats(
    const std::shared_ptr<arrow::DataType>& type) const {
    switch (type->id()) {
        case arrow::Type::BOOL:
            return ColumnStats::CreateBooleanColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::INT8:
            return ColumnStats::CreateTinyIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::INT16:
            return ColumnStats::CreateSmallIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::INT32:
        case arrow::Type::TIME32:
            return ColumnStats::CreateIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::INT64:
            return ColumnStats::CreateBigIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::FLOAT:
            return ColumnStats::CreateFloatColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::DOUBLE:
            return ColumnStats::CreateDoubleColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            return ColumnStats::CreateStringColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::DATE32:
            return ColumnStats::CreateDateColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::TIMESTAMP: {
            std::shared_ptr<arrow::TimestampType> timestamp_type =
                checked_pointer_cast<arrow::TimestampType>(type);
            return ColumnStats::CreateTimestampColumnStats(
                std::nullopt, std::nullopt, std::nullopt,
                DateTimeUtils::GetPrecisionFromType(timestamp_type));
        }
        case arrow::Type::DECIMAL128: {
            const auto& decimal_type = checked_cast<const arrow::Decimal128Type&>(*type);
            return ColumnStats::CreateDecimalColumnStats(std::nullopt, std::nullopt, std::nullopt,
                                                         decimal_type.precision(),
                                                         decimal_type.scale());
        }
        case arrow::Type::LIST:
            return ColumnStats::CreateNestedColumnStats(FieldType::ARRAY, std::nullopt);
        case arrow::Type::FIXED_SIZE_LIST:
            return ColumnStats::CreateNestedColumnStats(FieldType::VECTOR, std::nullopt);
        case arrow::Type::STRUCT:
            return ColumnStats::CreateNestedColumnStats(FieldType::STRUCT, std::nullopt);
        default:
            return Status::Invalid("cannot create empty Lance statistics for type ",
                                   type->ToString());
    }
}

}  // namespace paimon::lance
