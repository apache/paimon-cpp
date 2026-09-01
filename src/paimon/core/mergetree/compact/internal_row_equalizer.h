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

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/data_getters.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// Creates equality functions for internal rows, including nested values.
/// Java's RecordEqualiser also compares the RowKind embedded in InternalRow. This comparator
/// currently compares field values only. This does not affect the current lookup changelog results
/// because changelog kinds are tracked separately by KeyValue::value_kind.
class InternalRowEqualizer {
 public:
    static Result<FieldsComparator::FieldComparatorFunc> Create(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::string>& ignore_fields) {
        std::set<std::string> ignored(ignore_fields.begin(), ignore_fields.end());
        std::vector<std::pair<int32_t, ValueEqualizer>> equalizers;
        for (int32_t i = 0; i < schema->num_fields(); ++i) {
            if (ignored.find(schema->field(i)->name()) != ignored.end()) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(ValueEqualizer equalizer,
                                   CreateValueEqualizer(schema->field(i)->type()));
            equalizers.emplace_back(i, std::move(equalizer));
        }
        return FieldsComparator::FieldComparatorFunc(
            [equalizers = std::move(equalizers)](const InternalRow& lhs, const InternalRow& rhs) {
                for (const auto& [field_idx, equalizer] : equalizers) {
                    if (!EqualAt(lhs, field_idx, rhs, field_idx, equalizer)) {
                        return 1;
                    }
                }
                return 0;
            });
    }

 private:
    using ValueEqualizer =
        std::function<bool(const DataGetters&, int32_t, const DataGetters&, int32_t)>;

    static bool EqualAt(const DataGetters& lhs, int32_t lhs_pos, const DataGetters& rhs,
                        int32_t rhs_pos, const ValueEqualizer& equalizer) {
        bool lhs_null = lhs.IsNullAt(lhs_pos);
        bool rhs_null = rhs.IsNullAt(rhs_pos);
        if (lhs_null || rhs_null) {
            return lhs_null == rhs_null;
        }
        return equalizer(lhs, lhs_pos, rhs, rhs_pos);
    }

    static Result<ValueEqualizer> CreateValueEqualizer(
        const std::shared_ptr<arrow::DataType>& type) {
        switch (type->id()) {
            case arrow::Type::BOOL:
                return PrimitiveEqualizer<bool>(
                    [](const DataGetters& row, int32_t pos) { return row.GetBoolean(pos); });
            case arrow::Type::INT8:
                return PrimitiveEqualizer<char>(
                    [](const DataGetters& row, int32_t pos) { return row.GetByte(pos); });
            case arrow::Type::INT16:
                return PrimitiveEqualizer<int16_t>(
                    [](const DataGetters& row, int32_t pos) { return row.GetShort(pos); });
            case arrow::Type::INT32:
                return PrimitiveEqualizer<int32_t>(
                    [](const DataGetters& row, int32_t pos) { return row.GetInt(pos); });
            case arrow::Type::DATE32:
                return PrimitiveEqualizer<int32_t>(
                    [](const DataGetters& row, int32_t pos) { return row.GetDate(pos); });
            case arrow::Type::INT64:
                return PrimitiveEqualizer<int64_t>(
                    [](const DataGetters& row, int32_t pos) { return row.GetLong(pos); });
            case arrow::Type::FLOAT:
                return ValueEqualizer([](const DataGetters& lhs, int32_t lhs_pos,
                                         const DataGetters& rhs, int32_t rhs_pos) {
                    return FieldsComparator::CompareFloatingPoint(lhs.GetFloat(lhs_pos),
                                                                  rhs.GetFloat(rhs_pos)) == 0;
                });
            case arrow::Type::DOUBLE:
                return ValueEqualizer([](const DataGetters& lhs, int32_t lhs_pos,
                                         const DataGetters& rhs, int32_t rhs_pos) {
                    return FieldsComparator::CompareFloatingPoint(lhs.GetDouble(lhs_pos),
                                                                  rhs.GetDouble(rhs_pos)) == 0;
                });
            case arrow::Type::STRING:
            case arrow::Type::BINARY:
                return ValueEqualizer([](const DataGetters& lhs, int32_t lhs_pos,
                                         const DataGetters& rhs, int32_t rhs_pos) {
                    return lhs.GetStringView(lhs_pos) == rhs.GetStringView(rhs_pos);
                });
            case arrow::Type::TIMESTAMP: {
                std::shared_ptr<arrow::TimestampType> timestamp_type =
                    checked_pointer_cast<arrow::TimestampType>(type);
                int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
                return ValueEqualizer([precision](const DataGetters& lhs, int32_t lhs_pos,
                                                  const DataGetters& rhs, int32_t rhs_pos) {
                    return lhs.GetTimestamp(lhs_pos, precision) ==
                           rhs.GetTimestamp(rhs_pos, precision);
                });
            }
            case arrow::Type::DECIMAL128: {
                std::shared_ptr<arrow::Decimal128Type> decimal_type =
                    checked_pointer_cast<arrow::Decimal128Type>(type);
                int32_t precision = decimal_type->precision();
                int32_t scale = decimal_type->scale();
                return ValueEqualizer([precision, scale](const DataGetters& lhs, int32_t lhs_pos,
                                                         const DataGetters& rhs, int32_t rhs_pos) {
                    return lhs.GetDecimal(lhs_pos, precision, scale)
                               .CompareTo(rhs.GetDecimal(rhs_pos, precision, scale)) == 0;
                });
            }
            case arrow::Type::LIST: {
                std::shared_ptr<arrow::ListType> list_type =
                    checked_pointer_cast<arrow::ListType>(type);
                PAIMON_ASSIGN_OR_RAISE(ValueEqualizer element_equalizer,
                                       CreateValueEqualizer(list_type->value_type()));
                return ValueEqualizer([element_equalizer = std::move(element_equalizer)](
                                          const DataGetters& lhs, int32_t lhs_pos,
                                          const DataGetters& rhs, int32_t rhs_pos) {
                    std::shared_ptr<InternalArray> lhs_array = lhs.GetArray(lhs_pos);
                    std::shared_ptr<InternalArray> rhs_array = rhs.GetArray(rhs_pos);
                    if (lhs_array->Size() != rhs_array->Size()) {
                        return false;
                    }
                    for (int32_t i = 0; i < lhs_array->Size(); ++i) {
                        if (!EqualAt(*lhs_array, i, *rhs_array, i, element_equalizer)) {
                            return false;
                        }
                    }
                    return true;
                });
            }
            case arrow::Type::MAP: {
                std::shared_ptr<arrow::MapType> map_type =
                    checked_pointer_cast<arrow::MapType>(type);
                PAIMON_ASSIGN_OR_RAISE(ValueEqualizer key_equalizer,
                                       CreateValueEqualizer(map_type->key_type()));
                PAIMON_ASSIGN_OR_RAISE(ValueEqualizer item_equalizer,
                                       CreateValueEqualizer(map_type->item_type()));
                return ValueEqualizer([key_equalizer = std::move(key_equalizer),
                                       item_equalizer = std::move(item_equalizer)](
                                          const DataGetters& lhs, int32_t lhs_pos,
                                          const DataGetters& rhs, int32_t rhs_pos) {
                    std::shared_ptr<InternalMap> lhs_map = lhs.GetMap(lhs_pos);
                    std::shared_ptr<InternalMap> rhs_map = rhs.GetMap(rhs_pos);
                    if (lhs_map->Size() != rhs_map->Size()) {
                        return false;
                    }
                    std::shared_ptr<InternalArray> lhs_keys = lhs_map->KeyArray();
                    std::shared_ptr<InternalArray> rhs_keys = rhs_map->KeyArray();
                    std::shared_ptr<InternalArray> lhs_values = lhs_map->ValueArray();
                    std::shared_ptr<InternalArray> rhs_values = rhs_map->ValueArray();
                    std::vector<bool> matched(rhs_map->Size(), false);
                    for (int32_t lhs_index = 0; lhs_index < lhs_map->Size(); ++lhs_index) {
                        bool found = false;
                        for (int32_t rhs_index = 0; rhs_index < rhs_map->Size(); ++rhs_index) {
                            if (matched[rhs_index]) {
                                continue;
                            }
                            if (EqualAt(*lhs_keys, lhs_index, *rhs_keys, rhs_index,
                                        key_equalizer) &&
                                EqualAt(*lhs_values, lhs_index, *rhs_values, rhs_index,
                                        item_equalizer)) {
                                matched[rhs_index] = true;
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            return false;
                        }
                    }
                    return true;
                });
            }
            case arrow::Type::STRUCT: {
                std::shared_ptr<arrow::StructType> struct_type =
                    checked_pointer_cast<arrow::StructType>(type);
                std::vector<ValueEqualizer> field_equalizers;
                field_equalizers.reserve(struct_type->num_fields());
                for (const auto& field : struct_type->fields()) {
                    PAIMON_ASSIGN_OR_RAISE(ValueEqualizer field_equalizer,
                                           CreateValueEqualizer(field->type()));
                    field_equalizers.emplace_back(std::move(field_equalizer));
                }
                int32_t field_count = struct_type->num_fields();
                return ValueEqualizer([field_equalizers = std::move(field_equalizers), field_count](
                                          const DataGetters& lhs, int32_t lhs_pos,
                                          const DataGetters& rhs, int32_t rhs_pos) {
                    std::shared_ptr<InternalRow> lhs_row = lhs.GetRow(lhs_pos, field_count);
                    std::shared_ptr<InternalRow> rhs_row = rhs.GetRow(rhs_pos, field_count);
                    for (int32_t i = 0; i < field_count; ++i) {
                        if (!EqualAt(*lhs_row, i, *rhs_row, i, field_equalizers[i])) {
                            return false;
                        }
                    }
                    return true;
                });
            }
            default:
                return Status::NotImplemented(
                    fmt::format("Do not support equality for type {}", type->ToString()));
        }
    }

    template <typename T, typename Getter>
    static ValueEqualizer PrimitiveEqualizer(Getter getter) {
        return [getter = std::move(getter)](const DataGetters& lhs, int32_t lhs_pos,
                                            const DataGetters& rhs, int32_t rhs_pos) {
            return static_cast<T>(getter(lhs, lhs_pos)) == static_cast<T>(getter(rhs, rhs_pos));
        };
    }
};

}  // namespace paimon
