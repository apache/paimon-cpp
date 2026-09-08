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

#include "paimon/common/file_index/rangebitmap/range_bitmap_type_adapter.h"

#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/status.h"

namespace paimon {

Result<std::unique_ptr<RangeBitmapTypeAdapter>> RangeBitmapTypeAdapter::Create(
    const std::shared_ptr<arrow::DataType>& arrow_type) {
    PAIMON_ASSIGN_OR_RAISE(FieldType field_type,
                           FieldTypeUtils::ConvertToFieldType(arrow_type->id()));
    if (field_type == FieldType::DECIMAL) {
        const auto decimal_type = checked_pointer_cast<arrow::Decimal128Type>(arrow_type);
        if (decimal_type->precision() > 18) {
            return Status::Invalid(fmt::format(
                "range-bitmap index only supports DECIMAL with precision in [1, 18], got {}",
                decimal_type->precision()));
        }
        return std::unique_ptr<RangeBitmapTypeAdapter>(
            new RangeBitmapTypeAdapter(field_type, FieldType::BIGINT, std::nullopt));
    }
    if (field_type == FieldType::TIMESTAMP) {
        const auto timestamp_type = checked_pointer_cast<arrow::TimestampType>(arrow_type);
        const int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
        if (precision > 6) {
            return Status::Invalid(fmt::format(
                "range-bitmap index only supports TIMESTAMP with precision in [0, 6], got {}",
                precision));
        }
        return std::unique_ptr<RangeBitmapTypeAdapter>(
            new RangeBitmapTypeAdapter(field_type, FieldType::BIGINT, precision));
    }
    return std::unique_ptr<RangeBitmapTypeAdapter>(
        new RangeBitmapTypeAdapter(field_type, field_type, std::nullopt));
}

FieldType RangeBitmapTypeAdapter::GetStorageType() const {
    return storage_type_;
}

Result<Literal> RangeBitmapTypeAdapter::ToStorageLiteral(const Literal& literal) const {
    if (literal.IsNull()) {
        return Literal(storage_type_);
    }
    if (logical_type_ == FieldType::DECIMAL) {
        if (literal.GetType() != FieldType::DECIMAL) {
            return Status::Invalid("range-bitmap DECIMAL field requires a DECIMAL literal");
        }
        return Literal(literal.GetValue<Decimal>().ToUnscaledLong());
    }
    if (logical_type_ == FieldType::TIMESTAMP) {
        if (literal.GetType() != FieldType::TIMESTAMP) {
            return Status::Invalid("range-bitmap TIMESTAMP field requires a TIMESTAMP literal");
        }
        if (!timestamp_precision_.has_value()) {
            return Status::Invalid("range-bitmap TIMESTAMP adapter is missing precision");
        }
        const auto value = literal.GetValue<Timestamp>();
        return Literal(*timestamp_precision_ <= Timestamp::MILLIS_PRECISION
                           ? value.GetMillisecond()
                           : value.ToMicrosecond());
    }
    if (literal.GetType() != storage_type_) {
        return Status::Invalid(
            fmt::format("range-bitmap literal type {} does not match field type {}",
                        FieldTypeUtils::FieldTypeToString(literal.GetType()),
                        FieldTypeUtils::FieldTypeToString(storage_type_)));
    }
    return literal;
}

Result<std::vector<Literal>> RangeBitmapTypeAdapter::ToStorageLiterals(
    const std::vector<Literal>& literals) const {
    std::vector<Literal> converted_literals;
    converted_literals.reserve(literals.size());
    for (const Literal& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(Literal converted_literal, ToStorageLiteral(literal));
        converted_literals.emplace_back(std::move(converted_literal));
    }
    return converted_literals;
}

RangeBitmapTypeAdapter::RangeBitmapTypeAdapter(FieldType logical_type, FieldType storage_type,
                                               std::optional<int32_t> timestamp_precision)
    : logical_type_(logical_type),
      storage_type_(storage_type),
      timestamp_precision_(timestamp_precision) {}

}  // namespace paimon
