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

#include "paimon/core/stats/simple_stats_converter.h"

#include <cstdint>
#include <optional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/column_stats.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;

Result<SimpleStats> SimpleStatsConverter::ToBinary(
    const std::vector<std::shared_ptr<ColumnStats>>& stats_vec, MemoryPool* pool) {
    int32_t row_field_count = stats_vec.size();
    BinaryRow min_values(row_field_count);
    BinaryRowWriter min_writer(&min_values, 1024, pool);
    BinaryRow max_values(row_field_count);
    BinaryRowWriter max_writer(&max_values, 1024, pool);
    BinaryArray null_counts;
    BinaryArrayWriter null_writer(&null_counts, row_field_count, sizeof(int64_t), pool);
    for (int32_t i = 0; i < row_field_count; i++) {
        const auto& stats = stats_vec[i];
        auto type = stats->GetFieldType();
        if (stats->NullCount()) {
            null_writer.WriteLong(i, stats->NullCount().value());
        } else {
            null_writer.SetNullAt(i);
        }
        if (auto null_stats = std::dynamic_pointer_cast<NestedColumnStats>(stats)) {
            // nested type, e.g., List, Map, Struct
            min_writer.SetNullAt(i);
            max_writer.SetNullAt(i);
            continue;
        }
        switch (type) {
            case FieldType::BOOLEAN: {
                auto typed_stats = std::dynamic_pointer_cast<BooleanColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast BooleanColumnStats failed");
                }
                if (typed_stats->Min() == std::nullopt) {
                    min_writer.SetNullAt(i);
                } else {
                    min_writer.WriteBoolean(i, typed_stats->Min().value());
                }
                if (typed_stats->Max() == std::nullopt) {
                    max_writer.SetNullAt(i);
                } else {
                    max_writer.WriteBoolean(i, typed_stats->Max().value());
                }
                break;
            }
            case FieldType::TINYINT: {
                auto typed_stats = std::dynamic_pointer_cast<TinyIntColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast TinyIntColumnStats failed");
                }
                if (typed_stats->Min() == std::nullopt) {
                    min_writer.SetNullAt(i);
                } else {
                    min_writer.WriteByte(i, typed_stats->Min().value());
                }
                if (typed_stats->Max() == std::nullopt) {
                    max_writer.SetNullAt(i);
                } else {
                    max_writer.WriteByte(i, typed_stats->Max().value());
                }
                break;
            }
            case FieldType::SMALLINT: {
                auto typed_stats = std::dynamic_pointer_cast<SmallIntColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast SmallIntColumnStats failed");
                }
                if (typed_stats->Min() == std::nullopt) {
                    min_writer.SetNullAt(i);
                } else {
                    min_writer.WriteShort(i, typed_stats->Min().value());
                }
                if (typed_stats->Max() == std::nullopt) {
                    max_writer.SetNullAt(i);
                } else {
                    max_writer.WriteShort(i, typed_stats->Max().value());
                }
                break;
            }
            case FieldType::INT: {
                auto typed_stats = std::dynamic_pointer_cast<IntColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast IntColumnStats failed");
                }
                if (typed_stats->Min() == std::nullopt) {
                    min_writer.SetNullAt(i);
                } else {
                    min_writer.WriteInt(i, typed_stats->Min().value());
                }
                if (typed_stats->Max() == std::nullopt) {
                    max_writer.SetNullAt(i);
                } else {
                    max_writer.WriteInt(i, typed_stats->Max().value());
                }
                break;
            }
            case FieldType::BIGINT: {
                auto typed_stats = std::dynamic_pointer_cast<BigIntColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast LongColumnStats failed");
                }
                if (typed_stats->Min() == std::nullopt) {
                    min_writer.SetNullAt(i);
                } else {
                    min_writer.WriteLong(i, typed_stats->Min().value());
                }
                if (typed_stats->Max() == std::nullopt) {
                    max_writer.SetNullAt(i);
                } else {
                    max_writer.WriteLong(i, typed_stats->Max().value());
                }
                break;
            }
            case FieldType::FLOAT: {
                auto typed_stats = std::dynamic_pointer_cast<FloatColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast FloatColumnStats failed");
                }
                if (typed_stats->Min() == std::nullopt) {
                    min_writer.SetNullAt(i);
                } else {
                    min_writer.WriteFloat(i, typed_stats->Min().value());
                }
                if (typed_stats->Max() == std::nullopt) {
                    max_writer.SetNullAt(i);
                } else {
                    max_writer.WriteFloat(i, typed_stats->Max().value());
                }
                break;
            }
            case FieldType::DOUBLE: {
                auto typed_stats = std::dynamic_pointer_cast<DoubleColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast DoubleColumnStats failed");
                }
                if (typed_stats->Min() == std::nullopt) {
                    min_writer.SetNullAt(i);
                } else {
                    min_writer.WriteDouble(i, typed_stats->Min().value());
                }
                if (typed_stats->Max() == std::nullopt) {
                    max_writer.SetNullAt(i);
                } else {
                    max_writer.WriteDouble(i, typed_stats->Max().value());
                }
                break;
            }
            case FieldType::STRING: {
                auto typed_stats = std::dynamic_pointer_cast<StringColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast StringColumnStats failed");
                }
                if (typed_stats->Min() == std::nullopt) {
                    min_writer.SetNullAt(i);
                } else {
                    min_writer.WriteString(
                        i, BinaryString::FromString(typed_stats->Min().value(), pool));
                }
                if (typed_stats->Max() == std::nullopt) {
                    max_writer.SetNullAt(i);
                } else {
                    max_writer.WriteString(
                        i, BinaryString::FromString(typed_stats->Max().value(), pool));
                }
                break;
            }
            case FieldType::DATE: {
                auto typed_stats = std::dynamic_pointer_cast<DateColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast DateColumnStats failed");
                }
                if (typed_stats->Min() == std::nullopt) {
                    min_writer.SetNullAt(i);
                } else {
                    min_writer.WriteInt(i, typed_stats->Min().value());
                }
                if (typed_stats->Max() == std::nullopt) {
                    max_writer.SetNullAt(i);
                } else {
                    max_writer.WriteInt(i, typed_stats->Max().value());
                }
                break;
            }
            case FieldType::TIMESTAMP: {
                auto typed_stats = std::dynamic_pointer_cast<TimestampColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast TimestampColumnStats failed");
                }
                int32_t precision = typed_stats->GetPrecision();
                if (typed_stats->Min() == std::nullopt) {
                    if (!Timestamp::IsCompact(precision)) {
                        min_writer.WriteTimestamp(i, std::nullopt, precision);
                    } else {
                        min_writer.SetNullAt(i);
                    }
                } else {
                    min_writer.WriteTimestamp(i, typed_stats->Min().value(), precision);
                }
                if (typed_stats->Max() == std::nullopt) {
                    if (!Timestamp::IsCompact(precision)) {
                        max_writer.WriteTimestamp(i, std::nullopt, precision);
                    } else {
                        max_writer.SetNullAt(i);
                    }
                } else {
                    max_writer.WriteTimestamp(i, typed_stats->Max().value(), precision);
                }
                break;
            }
            case FieldType::DECIMAL: {
                auto typed_stats = std::dynamic_pointer_cast<DecimalColumnStats>(stats);
                if (typed_stats == nullptr) {
                    return Status::Invalid("cast DecimalColumnStats failed");
                }
                auto precision = typed_stats->GetPrecision();
                auto scale = typed_stats->GetScale();
                if (typed_stats->Min() == std::nullopt) {
                    if (!Decimal::IsCompact(precision)) {
                        min_writer.WriteDecimal(i, std::nullopt, precision);
                    } else {
                        min_writer.SetNullAt(i);
                    }
                } else {
                    auto min_decimal = typed_stats->Min().value();
                    if (min_decimal.Scale() != scale) {
                        return Status::Invalid(
                            fmt::format("in SimpleStatsConverter decimal scale mismatch: min "
                                        "decimal scale {}, target type scale {}",
                                        min_decimal.Scale(), scale));
                    }
                    min_writer.WriteDecimal(i, min_decimal, precision);
                }
                if (typed_stats->Max() == std::nullopt) {
                    if (!Decimal::IsCompact(precision)) {
                        max_writer.WriteDecimal(i, std::nullopt, precision);
                    } else {
                        max_writer.SetNullAt(i);
                    }
                } else {
                    auto max_decimal = typed_stats->Max().value();
                    if (max_decimal.Scale() != scale) {
                        return Status::Invalid(
                            fmt::format("in SimpleStatsConverter decimal scale mismatch: max "
                                        "decimal scale {}, target type scale {}",
                                        max_decimal.Scale(), scale));
                    }
                    max_writer.WriteDecimal(i, max_decimal, precision);
                }
                break;
            }
            default:
                return Status::Invalid(fmt::format("invalid type {} for SimpleStatsConverter",
                                                   static_cast<int32_t>(type)));
        }
    }
    min_writer.Complete();
    max_writer.Complete();
    null_writer.Complete();
    return SimpleStats(min_values, max_values, null_counts);
}

}  // namespace paimon
