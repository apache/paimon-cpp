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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/format/vortex/vortex_stats_extractor.h"

#include <optional>
#include <utility>

#include "arrow/api.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/defs.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/vortex/vortex_ffi.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/format/vortex/vortex_io_callbacks.h"
#include "paimon/fs/file_system.h"

namespace paimon::vortex {

Result<std::pair<ColumnStatsVector, FormatStatsExtractor::FileInfo>>
VortexStatsExtractor::ExtractWithFileInfo(const std::shared_ptr<FileSystem>& file_system,
                                          const std::string& path,
                                          const std::shared_ptr<MemoryPool>& pool) {
    if (file_system == nullptr) {
        return Status::Invalid("Vortex stats extractor requires a file system");
    }
    if (schema_ == nullptr) {
        return Status::Invalid("Vortex stats extractor has no schema");
    }
    (void)pool;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system->Open(path));
    // Only the row count is needed; Vortex keeps it in the file footer.
    PAIMON_ASSIGN_OR_RAISE(int64_t signed_length, input->Length());
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(signed_length, "Vortex input length"));
    auto length = static_cast<uint64_t>(signed_length);
    auto input_context = std::make_shared<VortexInputContext>(input);

    VxSessionPtr session(vx_session_new(), vx_session_free);
    if (session == nullptr) {
        return Status::IOError("failed to create Vortex session");
    }
    vx_error* error = nullptr;
    VxDataSourcePtr data_source(
        vx_data_source_new_callback(session.get(), VortexInputContext::MakeCallbacks(input_context),
                                    length, &error),
        vx_data_source_free);
    if (data_source == nullptr) {
        return VortexCallbackError("open Vortex file for stats", error,
                                   input_context->GetCallbackStatus());
    }
    vx_estimate row_count{};
    vx_data_source_get_row_count(data_source.get(), &row_count);
    if (row_count.type == VX_ESTIMATE_UNKNOWN) {
        return Status::Invalid("Vortex file did not report a row count");
    }
    const auto rows = static_cast<int64_t>(row_count.estimate);
    // Vortex keeps its statistics internal, so every field reports an unknown entry; consumers
    // index this vector by field position and require one entry per write-schema field.
    ColumnStatsVector stats;
    stats.reserve(schema_->num_fields());
    for (const std::shared_ptr<arrow::Field>& field : schema_->fields()) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ColumnStats> column_stats,
                               CreateUnknownStats(field->type()));
        stats.push_back(std::move(column_stats));
    }
    return std::make_pair(std::move(stats), FileInfo(rows));
}

Result<std::unique_ptr<ColumnStats>> VortexStatsExtractor::CreateUnknownStats(
    const std::shared_ptr<arrow::DataType>& type) const {
    switch (type->id()) {
        case arrow::Type::BOOL:
            return ColumnStats::CreateBooleanColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::INT8:
            return ColumnStats::CreateTinyIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::INT16:
            return ColumnStats::CreateSmallIntColumnStats(std::nullopt, std::nullopt, std::nullopt);
        case arrow::Type::INT32:
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
            auto timestamp_type = checked_pointer_cast<arrow::TimestampType>(type);
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
            return Status::Invalid("cannot create unknown Vortex statistics for type ",
                                   type->ToString());
    }
}

}  // namespace paimon::vortex
