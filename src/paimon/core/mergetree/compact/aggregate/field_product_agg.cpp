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

#include "paimon/core/mergetree/compact/aggregate/field_product_agg.h"

#include <array>
#include <cassert>
#include <cstdint>
#include <limits>

#include "arrow/type.h"
#include "arrow/util/basic_decimal.h"
#include "fmt/format.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/status.h"

namespace paimon {
namespace {
arrow::BasicDecimal256 ToDecimal256(const Decimal& value) {
    return arrow::BasicDecimal256(
        arrow::BasicDecimal128(static_cast<int64_t>(value.HighBits()), value.LowBits()));
}

// @warning Only call this for values already known to fit into a decimal of precision 38 or less,
// the higher 128 bits are dropped.
Decimal::int128_t ToInt128(const arrow::BasicDecimal256& value) {
    std::array<uint64_t, 4> words = value.little_endian_array();
    return static_cast<Decimal::int128_t>((static_cast<Decimal::uint128_t>(words[1]) << 64) |
                                          words[0]);
}

template <typename T>
Result<T> MultiplyExact(T left, T right, const char* type_name) {
    T product = 0;
    if (__builtin_mul_overflow(left, right, &product)) {
        return Status::Invalid(fmt::format("{} overflow: {} * {}", type_name,
                                           static_cast<int64_t>(left),
                                           static_cast<int64_t>(right)));
    }
    return product;
}

// Rejects the two divisions Java rejects: a zero divisor, and the one quotient the type cannot
// represent, its minimum divided by -1. The quotient itself still truncates towards zero, the way
// integer division does in Java.
template <typename T>
Result<T> DivideExact(T left, T right, const char* type_name) {
    if (right == 0) {
        return Status::Invalid(fmt::format("{} division by zero: {} / {}", type_name,
                                           static_cast<int64_t>(left),
                                           static_cast<int64_t>(right)));
    }
    if (left == std::numeric_limits<T>::min() && right == -1) {
        return Status::Invalid(fmt::format("{} overflow: {} / {}", type_name,
                                           static_cast<int64_t>(left),
                                           static_cast<int64_t>(right)));
    }
    return static_cast<T>(left / right);
}

// Negating through the unsigned type rather than the signed one keeps the type's minimum, whose
// magnitude a signed negation cannot represent, well defined.
Decimal::uint128_t Magnitude(Decimal::int128_t value) {
    auto magnitude = static_cast<Decimal::uint128_t>(value);
    return value < 0 ? -magnitude : magnitude;
}

// Whether the exact quotient of two decimals sharing a scale has a finite decimal expansion, which
// is what Java's BigDecimal.divide() requires before it hands the quotient to fromBigDecimal().
//
// The scales cancel out, so the exact quotient is the ratio of the unscaled values. Reduced to
// lowest terms it terminates exactly when its denominator has no prime factor besides 2 and 5,
// which holds when the divisor stripped of those two factors divides the dividend.
//
// @warning The divisor must be non-zero; stripping the factors of a zero never terminates.
bool QuotientTerminates(Decimal::int128_t dividend, Decimal::int128_t divisor) {
    Decimal::uint128_t coprime_divisor = Magnitude(divisor);
    while (coprime_divisor % 2 == 0) {
        coprime_divisor /= 2;
    }
    while (coprime_divisor % 5 == 0) {
        coprime_divisor /= 5;
    }
    return Magnitude(dividend) % coprime_divisor == 0;
}

Result<VariantType> MultiplyDecimal(const Decimal& accumulator, const Decimal& input_field) {
    arrow::BasicDecimal256 product = ToDecimal256(accumulator);
    product *= ToDecimal256(input_field);
    // Multiplying two values sharing the field scale doubles that scale, so drop the extra digits
    // again, rounding half up like Java's BigDecimal.setScale(scale, HALF_UP) does.
    product = product.ReduceScaleBy(accumulator.Scale(), /*round=*/true);
    if (!product.FitsInPrecision(accumulator.Precision())) {
        // Java's Decimal.fromBigDecimal() returns null for a result the field precision cannot
        // hold, which leaves the aggregated field null.
        return VariantType(NullType());
    }
    return VariantType(Decimal(accumulator.Precision(), accumulator.Scale(), ToInt128(product)));
}

Result<VariantType> DivideDecimal(const Decimal& accumulator, const Decimal& input_field) {
    arrow::BasicDecimal256 divisor = ToDecimal256(input_field);
    // Dividing two values sharing the field scale cancels that scale out, so scale the dividend up
    // first to keep the field scale in the quotient.
    arrow::BasicDecimal256 dividend =
        ToDecimal256(accumulator).IncreaseScaleBy(accumulator.Scale());
    arrow::BasicDecimal256 quotient;
    arrow::BasicDecimal256 remainder;
    if (dividend.Divide(divisor, &quotient, &remainder) != arrow::DecimalStatus::kSuccess) {
        return Status::Invalid(fmt::format("decimal division by zero: {} / {}",
                                           accumulator.ToString(), input_field.ToString()));
    }
    if (!QuotientTerminates(accumulator.Value(), input_field.Value())) {
        return Status::Invalid(fmt::format("decimal {} / {} has no finite decimal expansion",
                                           accumulator.ToString(), input_field.ToString()));
    }
    // Divide() drops the fractional part towards zero, round it half up instead. A quotient that
    // terminates can still carry more digits than the field scale, so this is still needed.
    arrow::BasicDecimal256 abs_remainder = arrow::BasicDecimal256::Abs(remainder);
    arrow::BasicDecimal256 twice_remainder = abs_remainder;
    twice_remainder += abs_remainder;
    if (twice_remainder >= arrow::BasicDecimal256::Abs(divisor)) {
        quotient += arrow::BasicDecimal256(
            dividend.IsNegative() == divisor.IsNegative() ? int64_t{1} : int64_t{-1});
    }
    if (!quotient.FitsInPrecision(accumulator.Precision())) {
        // Same as the multiplication, Java's fromBigDecimal() nulls out a quotient the field
        // precision cannot hold.
        return VariantType(NullType());
    }
    return VariantType(Decimal(accumulator.Precision(), accumulator.Scale(), ToInt128(quotient)));
}
}  // namespace

Result<std::unique_ptr<FieldProductAgg>> FieldProductAgg::Create(
    const std::shared_ptr<arrow::DataType>& field_type, const std::shared_ptr<MemoryPool>& pool) {
    if (field_type->id() == arrow::Type::type::DECIMAL128) {
        // Both directions rescale by the field scale, so reject up front the decimal types the
        // rescaling helpers cannot handle.
        PAIMON_RETURN_NOT_OK(DecimalUtils::CheckDecimalType(*field_type));
        const auto* decimal_type = checked_cast<const arrow::Decimal128Type*>(field_type.get());
        if (decimal_type->scale() < 0) {
            return Status::Invalid(
                fmt::format("Invalid decimal type {}, scale must >= 0", field_type->ToString()));
        }
    }
    PAIMON_ASSIGN_OR_RAISE(FieldArithmeticFunc multiply_func, CreateMultiplyFunc(field_type));
    PAIMON_ASSIGN_OR_RAISE(FieldArithmeticFunc divide_func, CreateDivideFunc(field_type));
    return std::unique_ptr<FieldProductAgg>(
        new FieldProductAgg(field_type, multiply_func, divide_func, pool));
}

Result<FieldProductAgg::FieldArithmeticFunc> FieldProductAgg::CreateMultiplyFunc(
    const std::shared_ptr<arrow::DataType>& field_type) {
    arrow::Type::type type = field_type->id();
    switch (type) {
        case arrow::Type::type::INT8:
            // The variant holds TINYINT as a plain char, whose signedness follows the ABI, so
            // multiply the signed value it stands for.
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                auto accumulator_value =
                    static_cast<int8_t>(DataDefine::GetVariantValue<char>(accumulator));
                auto input_value =
                    static_cast<int8_t>(DataDefine::GetVariantValue<char>(input_field));
                PAIMON_ASSIGN_OR_RAISE(int8_t product,
                                       MultiplyExact(accumulator_value, input_value, "int8"));
                return VariantType(static_cast<char>(product));
            });
        case arrow::Type::type::INT16:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(
                    int16_t product,
                    MultiplyExact(DataDefine::GetVariantValue<int16_t>(accumulator),
                                  DataDefine::GetVariantValue<int16_t>(input_field), "int16"));
                return VariantType(product);
            });
        case arrow::Type::type::INT32:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t product,
                    MultiplyExact(DataDefine::GetVariantValue<int32_t>(accumulator),
                                  DataDefine::GetVariantValue<int32_t>(input_field), "int32"));
                return VariantType(product);
            });
        case arrow::Type::type::INT64:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(
                    int64_t product,
                    MultiplyExact(DataDefine::GetVariantValue<int64_t>(accumulator),
                                  DataDefine::GetVariantValue<int64_t>(input_field), "int64"));
                return VariantType(product);
            });
        case arrow::Type::type::FLOAT:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                float product = DataDefine::GetVariantValue<float>(accumulator) *
                                DataDefine::GetVariantValue<float>(input_field);
                return VariantType(product);
            });
        case arrow::Type::type::DOUBLE:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                double product = DataDefine::GetVariantValue<double>(accumulator) *
                                 DataDefine::GetVariantValue<double>(input_field);
                return VariantType(product);
            });
        case arrow::Type::type::DECIMAL128:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                auto accumulator_value = DataDefine::GetVariantValue<Decimal>(accumulator);
                auto input_value = DataDefine::GetVariantValue<Decimal>(input_field);
                assert(accumulator_value.Precision() == input_value.Precision() &&
                       accumulator_value.Scale() == input_value.Scale());
                return MultiplyDecimal(accumulator_value, input_value);
            });
        default:
            return Status::Invalid(
                fmt::format("type {} not support in FieldProductAgg", field_type->ToString()));
    }
}

Result<FieldProductAgg::FieldArithmeticFunc> FieldProductAgg::CreateDivideFunc(
    const std::shared_ptr<arrow::DataType>& field_type) {
    arrow::Type::type type = field_type->id();
    switch (type) {
        case arrow::Type::type::INT8:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                auto accumulator_value =
                    static_cast<int8_t>(DataDefine::GetVariantValue<char>(accumulator));
                auto input_value =
                    static_cast<int8_t>(DataDefine::GetVariantValue<char>(input_field));
                PAIMON_ASSIGN_OR_RAISE(int8_t quotient,
                                       DivideExact(accumulator_value, input_value, "int8"));
                return VariantType(static_cast<char>(quotient));
            });
        case arrow::Type::type::INT16:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(
                    int16_t quotient,
                    DivideExact(DataDefine::GetVariantValue<int16_t>(accumulator),
                                DataDefine::GetVariantValue<int16_t>(input_field), "int16"));
                return VariantType(quotient);
            });
        case arrow::Type::type::INT32:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t quotient,
                    DivideExact(DataDefine::GetVariantValue<int32_t>(accumulator),
                                DataDefine::GetVariantValue<int32_t>(input_field), "int32"));
                return VariantType(quotient);
            });
        case arrow::Type::type::INT64:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                PAIMON_ASSIGN_OR_RAISE(
                    int64_t quotient,
                    DivideExact(DataDefine::GetVariantValue<int64_t>(accumulator),
                                DataDefine::GetVariantValue<int64_t>(input_field), "int64"));
                return VariantType(quotient);
            });
        case arrow::Type::type::FLOAT:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                float quotient = DataDefine::GetVariantValue<float>(accumulator) /
                                 DataDefine::GetVariantValue<float>(input_field);
                return VariantType(quotient);
            });
        case arrow::Type::type::DOUBLE:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                double quotient = DataDefine::GetVariantValue<double>(accumulator) /
                                  DataDefine::GetVariantValue<double>(input_field);
                return VariantType(quotient);
            });
        case arrow::Type::type::DECIMAL128:
            return FieldArithmeticFunc([](const VariantType& accumulator,
                                          const VariantType& input_field) -> Result<VariantType> {
                auto accumulator_value = DataDefine::GetVariantValue<Decimal>(accumulator);
                auto input_value = DataDefine::GetVariantValue<Decimal>(input_field);
                assert(accumulator_value.Precision() == input_value.Precision() &&
                       accumulator_value.Scale() == input_value.Scale());
                return DivideDecimal(accumulator_value, input_value);
            });
        default:
            return Status::Invalid(
                fmt::format("type {} not support in FieldProductAgg", field_type->ToString()));
    }
}
}  // namespace paimon
