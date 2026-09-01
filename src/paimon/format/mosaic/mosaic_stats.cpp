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

#include "paimon/format/mosaic/mosaic_stats.h"

#include <cstring>
#include <functional>
#include <limits>
#include <utility>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/math.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/mosaic/mosaic_stream.h"
#include "paimon/io/byte_order.h"

namespace paimon::mosaic {

namespace {

template <typename T>
using OptionalMinMax = std::pair<std::optional<T>, std::optional<T>>;

template <typename T>
Result<T> DecodeBigEndian(const std::vector<uint8_t>& bytes) {
    if (bytes.size() != sizeof(T)) {
        return Status::Invalid(
            fmt::format("invalid Mosaic statistic size {}, expected {}", bytes.size(), sizeof(T)));
    }
    T value;
    std::memcpy(&value, bytes.data(), sizeof(T));
    if constexpr (SystemByteOrder() == ByteOrder::PAIMON_LITTLE_ENDIAN) {
        value = EndianSwapValue(value);
    }
    return value;
}

template <typename T, typename Decoder, typename Less = std::less<T>>
Result<std::pair<std::optional<T>, std::optional<T>>> CollectMinMax(
    const std::vector<const MosaicStatsUtils::ColumnStatistics*>& stats, Decoder decoder,
    Less less = Less()) {
    std::optional<T> min;
    std::optional<T> max;
    for (const MosaicStatsUtils::ColumnStatistics* stat : stats) {
        if (stat->min.has_value() != stat->max.has_value()) {
            return Status::Invalid("Mosaic statistics contain incomplete min/max values");
        }
        if (!stat->min.has_value()) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(T row_group_min, decoder(stat->min.value()));
        PAIMON_ASSIGN_OR_RAISE(T row_group_max, decoder(stat->max.value()));
        if (!min.has_value() || less(row_group_min, min.value())) {
            min = std::move(row_group_min);
        }
        if (!max.has_value() || less(max.value(), row_group_max)) {
            max = std::move(row_group_max);
        }
    }
    return std::make_pair(std::move(min), std::move(max));
}

Result<std::optional<int64_t>> CollectNullCount(
    const std::vector<const MosaicStatsUtils::ColumnStatistics*>& stats,
    bool missing_null_count_is_zero) {
    if (stats.empty()) {
        return missing_null_count_is_zero ? std::optional<int64_t>(0) : std::nullopt;
    }
    uint64_t result = 0;
    for (const MosaicStatsUtils::ColumnStatistics* stat : stats) {
        if (stat->null_count >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - result) {
            return Status::Invalid("Mosaic null count exceeds int64 range");
        }
        result += stat->null_count;
    }
    return std::optional<int64_t>(static_cast<int64_t>(result));
}

Result<Timestamp> DecodeTimestamp(const std::vector<uint8_t>& bytes,
                                  const std::shared_ptr<arrow::TimestampType>& type) {
    if (type->unit() == arrow::TimeUnit::NANO) {
        if (bytes.size() != 12) {
            return Status::Invalid(
                fmt::format("invalid Mosaic nanosecond timestamp statistic size {}", bytes.size()));
        }
        std::vector<uint8_t> millis_bytes(bytes.begin(), bytes.begin() + 8);
        std::vector<uint8_t> nanos_bytes(bytes.begin() + 8, bytes.end());
        PAIMON_ASSIGN_OR_RAISE(int64_t millis, DecodeBigEndian<int64_t>(millis_bytes));
        PAIMON_ASSIGN_OR_RAISE(int32_t nanos, DecodeBigEndian<int32_t>(nanos_bytes));
        if (nanos < 0 || nanos > 999999) {
            return Status::Invalid("invalid Mosaic nanosecond timestamp statistic");
        }
        return Timestamp(millis, nanos);
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t value, DecodeBigEndian<int64_t>(bytes));
    auto [millis, nanos] = DateTimeUtils::TimestampConverter(
        value, DateTimeUtils::GetTimeTypeFromArrowType(type), DateTimeUtils::TimeType::MILLISECOND,
        DateTimeUtils::TimeType::NANOSECOND);
    return Timestamp(millis, static_cast<int32_t>(nanos));
}

Result<std::unique_ptr<ColumnStats>> ConvertFieldStatistics(
    const std::shared_ptr<arrow::DataType>& type,
    const std::vector<const MosaicStatsUtils::ColumnStatistics*>& stats,
    bool missing_null_count_is_zero) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> null_count,
                           CollectNullCount(stats, missing_null_count_is_zero));
    switch (type->id()) {
        case arrow::Type::BOOL: {
            auto decoder = [](const std::vector<uint8_t>& bytes) -> Result<bool> {
                if (bytes.size() != 1 || bytes[0] > 1) {
                    return Status::Invalid("invalid Mosaic boolean statistic");
                }
                return bytes[0] != 0;
            };
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<bool> min_max,
                                   CollectMinMax<bool>(stats, decoder));
            return ColumnStats::CreateBooleanColumnStats(min_max.first, min_max.second, null_count);
        }
        case arrow::Type::INT8: {
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<int8_t> min_max,
                                   CollectMinMax<int8_t>(stats, DecodeBigEndian<int8_t>));
            return ColumnStats::CreateTinyIntColumnStats(min_max.first, min_max.second, null_count);
        }
        case arrow::Type::INT16: {
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<int16_t> min_max,
                                   CollectMinMax<int16_t>(stats, DecodeBigEndian<int16_t>));
            return ColumnStats::CreateSmallIntColumnStats(min_max.first, min_max.second,
                                                          null_count);
        }
        case arrow::Type::INT32: {
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<int32_t> min_max,
                                   CollectMinMax<int32_t>(stats, DecodeBigEndian<int32_t>));
            return ColumnStats::CreateIntColumnStats(min_max.first, min_max.second, null_count);
        }
        case arrow::Type::INT64: {
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<int64_t> min_max,
                                   CollectMinMax<int64_t>(stats, DecodeBigEndian<int64_t>));
            return ColumnStats::CreateBigIntColumnStats(min_max.first, min_max.second, null_count);
        }
        case arrow::Type::FLOAT: {
            auto less = [](float lhs, float rhs) {
                return FieldsComparator::CompareFloatingPoint(lhs, rhs) < 0;
            };
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<float> min_max,
                                   CollectMinMax<float>(stats, DecodeBigEndian<float>, less));
            return ColumnStats::CreateFloatColumnStats(min_max.first, min_max.second, null_count);
        }
        case arrow::Type::DOUBLE: {
            auto less = [](double lhs, double rhs) {
                return FieldsComparator::CompareFloatingPoint(lhs, rhs) < 0;
            };
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<double> min_max,
                                   CollectMinMax<double>(stats, DecodeBigEndian<double>, less));
            return ColumnStats::CreateDoubleColumnStats(min_max.first, min_max.second, null_count);
        }
        case arrow::Type::STRING: {
            auto decoder = [](const std::vector<uint8_t>& bytes) -> Result<std::string> {
                if (bytes.empty()) {
                    return std::string();
                }
                return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            };
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<std::string> min_max,
                                   CollectMinMax<std::string>(stats, decoder));
            return ColumnStats::CreateStringColumnStats(min_max.first, min_max.second, null_count);
        }
        case arrow::Type::BINARY:
            return ColumnStats::CreateStringColumnStats(std::nullopt, std::nullopt, null_count);
        case arrow::Type::DATE32: {
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<int32_t> min_max,
                                   CollectMinMax<int32_t>(stats, DecodeBigEndian<int32_t>));
            return ColumnStats::CreateDateColumnStats(min_max.first, min_max.second, null_count);
        }
        case arrow::Type::TIMESTAMP: {
            auto timestamp_type = checked_pointer_cast<arrow::TimestampType>(type);
            auto decoder = [&timestamp_type](const std::vector<uint8_t>& bytes) {
                return DecodeTimestamp(bytes, timestamp_type);
            };
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<Timestamp> min_max,
                                   CollectMinMax<Timestamp>(stats, decoder));
            return ColumnStats::CreateTimestampColumnStats(
                min_max.first, min_max.second, null_count,
                DateTimeUtils::GetPrecisionFromType(timestamp_type));
        }
        case arrow::Type::DECIMAL128: {
            auto decimal_type = checked_pointer_cast<arrow::Decimal128Type>(type);
            if (decimal_type->precision() > 18) {
                return ColumnStats::CreateDecimalColumnStats(std::nullopt, std::nullopt, null_count,
                                                             decimal_type->precision(),
                                                             decimal_type->scale());
            }
            auto decoder = [&decimal_type](const std::vector<uint8_t>& bytes) -> Result<Decimal> {
                PAIMON_ASSIGN_OR_RAISE(int64_t value, DecodeBigEndian<int64_t>(bytes));
                return Decimal::FromUnscaledLong(value, decimal_type->precision(),
                                                 decimal_type->scale());
            };
            PAIMON_ASSIGN_OR_RAISE(OptionalMinMax<Decimal> min_max,
                                   CollectMinMax<Decimal>(stats, decoder));
            return ColumnStats::CreateDecimalColumnStats(min_max.first, min_max.second, null_count,
                                                         decimal_type->precision(),
                                                         decimal_type->scale());
        }
        case arrow::Type::LIST:
            return ColumnStats::CreateNestedColumnStats(FieldType::ARRAY, null_count);
        case arrow::Type::MAP:
            return ColumnStats::CreateNestedColumnStats(FieldType::MAP, null_count);
        case arrow::Type::STRUCT:
            return ColumnStats::CreateNestedColumnStats(FieldType::STRUCT, null_count);
        default:
            return Status::Invalid(
                fmt::format("cannot fetch Mosaic statistics for type {}", type->ToString()));
    }
}

}  // namespace

Result<MosaicStatsUtils::RowGroupStatistics> MosaicStatsUtils::ReadRowGroupStatistics(
    uint32_t row_group, const MosaicInputContext* input_context, MosaicReaderHandle* reader) {
    uint32_t num_stats = 0;
    if (mosaic_reader_row_group_num_stats(reader, row_group, &num_stats) != 0) {
        return MosaicFfiError("read Mosaic row group statistic count",
                              input_context->GetCallbackStatus());
    }
    if (num_stats == 0) {
        return RowGroupStatistics();
    }
    std::vector<const char*> names(num_stats);
    std::vector<uint64_t> null_counts(num_stats);
    std::vector<const uint8_t*> min_ptrs(num_stats);
    std::vector<uintptr_t> min_lens(num_stats);
    std::vector<const uint8_t*> max_ptrs(num_stats);
    std::vector<uintptr_t> max_lens(num_stats);
    if (mosaic_reader_row_group_stats(reader, row_group, names.data(), null_counts.data(),
                                      min_ptrs.data(), min_lens.data(), max_ptrs.data(),
                                      max_lens.data()) != 0) {
        return MosaicFfiError("read Mosaic row group statistics",
                              input_context->GetCallbackStatus());
    }
    RowGroupStatistics result;
    result.reserve(num_stats);
    for (uint32_t i = 0; i < num_stats; ++i) {
        if (names[i] == nullptr || (min_ptrs[i] == nullptr) != (max_ptrs[i] == nullptr) ||
            (min_ptrs[i] == nullptr && (min_lens[i] != 0 || max_lens[i] != 0))) {
            return Status::Invalid("invalid Mosaic row group statistics");
        }
        ColumnStatistics stats = {null_counts[i], std::nullopt, std::nullopt};
        if (min_ptrs[i] != nullptr) {
            stats.min = min_lens[i] == 0
                            ? std::vector<uint8_t>()
                            : std::vector<uint8_t>(min_ptrs[i], min_ptrs[i] + min_lens[i]);
            stats.max = max_lens[i] == 0
                            ? std::vector<uint8_t>()
                            : std::vector<uint8_t>(max_ptrs[i], max_ptrs[i] + max_lens[i]);
        }
        auto [iter, inserted] = result.emplace(names[i], std::move(stats));
        if (!inserted) {
            return Status::Invalid(
                fmt::format("duplicate Mosaic statistics for column {}", iter->first));
        }
    }
    return result;
}

Result<ColumnStatsVector> MosaicStatsUtils::ConvertColumnStatistics(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<RowGroupStatistics>& row_group_stats, bool missing_null_count_is_zero) {
    ColumnStatsVector result;
    result.reserve(schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& field : schema->fields()) {
        std::vector<const ColumnStatistics*> field_stats;
        field_stats.reserve(row_group_stats.size());
        for (const RowGroupStatistics& stats : row_group_stats) {
            auto iter = stats.find(field->name());
            if (iter != stats.end()) {
                field_stats.push_back(&iter->second);
            }
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<ColumnStats> column_stats,
            ConvertFieldStatistics(field->type(), field_stats, missing_null_count_is_zero));
        result.push_back(std::move(column_stats));
    }
    return result;
}

}  // namespace paimon::mosaic
