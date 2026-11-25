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

#include "paimon/format/vortex/vortex_predicate_converter.h"

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "arrow/util/decimal.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/casting/decimal_to_decimal_cast_executor.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"

namespace paimon::vortex {
namespace {

// Reject precision loss; timestamp values are assumed to fit int64 in the target unit.
bool GetTimestampValue(const Literal& literal, const std::shared_ptr<arrow::DataType>& arrow_type,
                       int64_t* result) {
    if (arrow_type->id() != arrow::Type::TIMESTAMP) {
        return false;
    }
    const auto timestamp_type = checked_pointer_cast<arrow::TimestampType>(arrow_type);
    if (!timestamp_type->timezone().empty() && timestamp_type->timezone() != "UTC") {
        return false;
    }
    const auto timestamp = literal.GetValue<Timestamp>();
    const DateTimeUtils::TimeType time_type =
        DateTimeUtils::GetTimeTypeFromArrowType(timestamp_type);
    const int64_t value = DateTimeUtils::TimestampToInteger(timestamp, time_type);
    const auto [millis, nanos] = DateTimeUtils::TimestampConverter(
        value, time_type, DateTimeUtils::MILLISECOND, DateTimeUtils::NANOSECOND);
    if (Timestamp::FromEpochMillis(millis, static_cast<int32_t>(nanos)) != timestamp) {
        return false;
    }
    *result = value;
    return true;
}

// Normalize constants only; shared CAST may round, so accept it only if the value is unchanged.
Result<std::optional<Decimal>> NormalizeDecimalLiteral(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& arrow_type) {
    const auto decimal = literal.GetValue<Decimal>();
    const auto& target_type = checked_cast<const arrow::Decimal128Type&>(*arrow_type);
    // Keep literals within Paimon's decimal range and the shared scale table.
    if (decimal.Precision() < Decimal::MIN_PRECISION ||
        decimal.Precision() > Decimal::MAX_PRECISION || decimal.Scale() < 0 ||
        decimal.Scale() > decimal.Precision() || target_type.precision() < Decimal::MIN_PRECISION ||
        target_type.precision() > Decimal::MAX_PRECISION || target_type.scale() < 0 ||
        target_type.scale() > target_type.precision()) {
        return std::optional<Decimal>();
    }
    if (!arrow::Decimal128(decimal.HighBits(), decimal.LowBits())
             .FitsInPrecision(decimal.Precision())) {
        return std::optional<Decimal>();
    }
    if (decimal.Precision() == target_type.precision() && decimal.Scale() == target_type.scale()) {
        return std::make_optional(decimal);
    }
    const DecimalToDecimalCastExecutor cast_executor;
    PAIMON_ASSIGN_OR_RAISE(Literal converted, cast_executor.Cast(literal, arrow_type));
    if (converted.IsNull()) {
        return std::optional<Decimal>();
    }
    const auto normalized = converted.GetValue<Decimal>();
    if (decimal.CompareTo(normalized) != 0) {
        return std::optional<Decimal>();
    }
    return std::make_optional(normalized);
}

// Import the actual Arrow type so Vortex owns the extension metadata encoding. Cache the
// non-nullable literal dtype across an IN list; only the storage scalar varies per value.
Result<VxScalarPtr> MakeTemporalScalar(const vx_scalar* storage,
                                       const std::shared_ptr<arrow::DataType>& arrow_type,
                                       VxDtypePtr* temporal_dtype) {
    vx_error* error = nullptr;
    if (*temporal_dtype == nullptr) {
        VxSessionPtr session(vx_paimon_read_session_new());
        const auto schema = arrow::schema({arrow::field("value", arrow_type, false)});
        ::ArrowSchema ffi_schema{};
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
        // Vortex consumes ffi_schema, including on error.
        VxDtypePtr struct_dtype(vx_dtype_from_arrow_schema(session.get(), &ffi_schema, &error));
        PAIMON_RETURN_NOT_OK(VortexFfiError("import Vortex temporal dtype", error));
        VxUniquePtr<const vx_struct_fields, vx_struct_fields_free> fields(
            vx_dtype_struct_dtype(struct_dtype.get()));
        temporal_dtype->reset(vx_struct_fields_field_dtype(fields.get(), 0));
    }
    VxScalarPtr scalar(vx_scalar_new_extension(temporal_dtype->get(), storage, &error));
    PAIMON_RETURN_NOT_OK(VortexFfiError("create Vortex temporal scalar", error));
    return scalar;
}

Result<VxScalarPtr> MakeLiteralScalar(const Literal& literal,
                                      const std::shared_ptr<arrow::DataType>& arrow_type,
                                      VxDtypePtr* temporal_dtype) {
    VxScalarPtr scalar;
    if (literal.IsNull()) {
        return scalar;
    }
    vx_error* error = nullptr;
    switch (literal.GetType()) {
        case FieldType::BOOLEAN:
            scalar.reset(vx_scalar_new_bool(literal.GetValue<bool>(), false));
            break;
        case FieldType::TINYINT:
            scalar.reset(vx_scalar_new_i8(literal.GetValue<int8_t>(), false));
            break;
        case FieldType::SMALLINT:
            scalar.reset(vx_scalar_new_i16(literal.GetValue<int16_t>(), false));
            break;
        case FieldType::INT:
            scalar.reset(vx_scalar_new_i32(literal.GetValue<int32_t>(), false));
            break;
        // Vortex stores a date column as a date[days] extension and rejects comparing it against a
        // plain int scalar, so build a date-typed scalar.
        case FieldType::DATE: {
            const auto days = literal.GetValue<int32_t>();
            VxScalarPtr storage(vx_scalar_new_i32(days, false));
            return MakeTemporalScalar(storage.get(), arrow_type, temporal_dtype);
        }
        case FieldType::BIGINT:
            scalar.reset(vx_scalar_new_i64(literal.GetValue<int64_t>(), false));
            break;
        case FieldType::FLOAT:
        case FieldType::DOUBLE:
            // Native totalOrder distinguishes NaN signs and payloads; Paimon treats all NaNs alike.
            return scalar;
        case FieldType::STRING: {
            const auto value = literal.GetValue<std::string>();
            scalar.reset(vx_scalar_new_utf8(vx_view{value.data(), value.size()}, false, &error));
            break;
        }
        case FieldType::DECIMAL: {
            PAIMON_ASSIGN_OR_RAISE(std::optional<Decimal> normalized,
                                   NormalizeDecimalLiteral(literal, arrow_type));
            if (!normalized.has_value()) {
                return scalar;
            }
            // Cast validated the full source value before selecting a narrower representation.
            const Decimal& decimal = normalized.value();
            const auto precision = static_cast<uint8_t>(decimal.Precision());
            const auto scale = static_cast<int8_t>(decimal.Scale());
            if (decimal.IsCompact()) {
                scalar.reset(vx_scalar_new_decimal_i64(decimal.ToUnscaledLong(), precision, scale,
                                                       false, &error));
            } else {
                // Little-endian 16-byte two's complement, low word first.
                uint8_t bytes16[16];
                const uint64_t low = decimal.LowBits();
                const uint64_t high = decimal.HighBits();
                std::memcpy(bytes16, &low, sizeof(low));
                std::memcpy(bytes16 + sizeof(low), &high, sizeof(high));
                scalar.reset(
                    vx_scalar_new_decimal_i128_le(bytes16, precision, scale, false, &error));
            }
            break;
        }
        case FieldType::TIMESTAMP: {
            int64_t value = 0;
            if (!GetTimestampValue(literal, arrow_type, &value)) {
                return scalar;
            }
            VxScalarPtr storage(vx_scalar_new_i64(value, false));
            return MakeTemporalScalar(storage.get(), arrow_type, temporal_dtype);
        }
        default:
            return scalar;
    }
    PAIMON_RETURN_NOT_OK(VortexFfiError("create Vortex predicate scalar", error));
    return scalar;
}

Result<VxExpressionPtr> MakeLiteralExpr(const Literal& literal,
                                        const std::shared_ptr<arrow::DataType>& arrow_type) {
    VxDtypePtr temporal_dtype;
    PAIMON_ASSIGN_OR_RAISE(VxScalarPtr scalar,
                           MakeLiteralScalar(literal, arrow_type, &temporal_dtype));
    if (scalar == nullptr) {
        return VxExpressionPtr();
    }
    vx_error* error = nullptr;
    VxExpressionPtr expression(vx_expression_literal(scalar.get(), &error));
    PAIMON_RETURN_NOT_OK(VortexFfiError("create Vortex literal expression", error));
    return expression;
}

// Literal types are validated by the caller. Check the actual file type once per leaf;
// scalar construction still checks value safety against its parameters.
Result<bool> CheckFieldTypePushdownEligibility(const std::shared_ptr<LeafPredicate>& leaf,
                                               const std::shared_ptr<arrow::DataType>& arrow_type) {
    if (arrow_type == nullptr) {
        return false;
    }
    const FieldType field_type = leaf->GetFieldType();
    const Result<FieldType> file_type = FieldTypeUtils::ConvertToFieldType(arrow_type->id());
    if (!file_type.ok()) {
        // Arrow types outside the Paimon type mapping cannot be pushed.
        return false;
    }
    if (file_type.value() != field_type) {
        return Status::Invalid("Predicate type mismatch for field '", leaf->FieldName(),
                               "': expected ", FieldTypeUtils::FieldTypeToString(file_type.value()),
                               " from file type ", arrow_type->ToString(), ", actual ",
                               FieldTypeUtils::FieldTypeToString(field_type));
    }
    switch (field_type) {
        case FieldType::BOOLEAN:
        case FieldType::TINYINT:
        case FieldType::SMALLINT:
        case FieldType::INT:
        case FieldType::BIGINT:
        case FieldType::STRING:
        case FieldType::DATE:
        case FieldType::DECIMAL:
        case FieldType::TIMESTAMP:
            return true;
        default:
            // NaN and signed-zero equivalence needs separate validation; keep the residual path.
            return false;
    }
}

Result<VxExpressionPtr> ConvertInList(const std::vector<Literal>& literals,
                                      const std::shared_ptr<arrow::DataType>& arrow_type,
                                      const vx_expression* column) {
    std::vector<VxScalarPtr> scalars;
    std::vector<const vx_scalar*> raw;
    VxDtypePtr temporal_dtype;
    scalars.reserve(literals.size());
    raw.reserve(literals.size());
    for (const Literal& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(VxScalarPtr scalar,
                               MakeLiteralScalar(literal, arrow_type, &temporal_dtype));
        if (scalar == nullptr) {
            return VxExpressionPtr();
        }
        raw.push_back(scalar.get());
        scalars.push_back(std::move(scalar));
    }
    vx_error* error = nullptr;
    VxDtypePtr element_dtype(vx_scalar_dtype(raw[0]));
    VxScalarPtr list(
        vx_scalar_new_list(element_dtype.get(), raw.data(), raw.size(), false, &error));
    PAIMON_RETURN_NOT_OK(VortexFfiError("create Vortex list scalar", error));
    VxExpressionPtr literal_expr(vx_expression_literal(list.get(), &error));
    PAIMON_RETURN_NOT_OK(VortexFfiError("create Vortex literal expression", error));
    VxExpressionPtr expression(vx_expression_list_contains(literal_expr.get(), column));
    return expression;
}

Result<VxExpressionPtr> ConvertMembership(const std::shared_ptr<LeafPredicate>& leaf,
                                          const std::shared_ptr<arrow::DataType>& arrow_type,
                                          const vx_expression* column, bool negate,
                                          const VortexPredicateOptions& options) {
    const std::vector<Literal>& literals = leaf->Literals();
    // The query engine owns deduplication and integer-range rewrites. Apply value conversions
    // and create native scalars only within the original-list limit.
    if (literals.empty() || literals.size() > options.max_in_values) {
        return VxExpressionPtr();
    }
    PAIMON_ASSIGN_OR_RAISE(bool type_eligible, CheckFieldTypePushdownEligibility(leaf, arrow_type));
    if (!type_eligible) {
        return VxExpressionPtr();
    }
    PAIMON_ASSIGN_OR_RAISE(VxExpressionPtr expression, ConvertInList(literals, arrow_type, column));
    if (expression == nullptr || !negate) {
        return expression;
    }
    expression.reset(vx_expression_not(expression.get()));
    VxExpressionPtr is_null(vx_expression_is_null(column));
    VxExpressionPtr is_not_null(vx_expression_not(is_null.get()));
    // Neither IN nor NOT IN may retain null rows. Validation also rejects null/empty lists.
    const vx_expression* children[] = {is_not_null.get(), expression.get()};
    VxExpressionPtr combined(vx_expression_and(children, 2));
    return combined;
}

bool ComparisonOperator(Function::Type type, vx_binary_operator* op) {
    switch (type) {
        case Function::Type::EQUAL:
            *op = VX_OPERATOR_EQ;
            return true;
        case Function::Type::NOT_EQUAL:
            *op = VX_OPERATOR_NOT_EQ;
            return true;
        case Function::Type::GREATER_THAN:
            *op = VX_OPERATOR_GT;
            return true;
        case Function::Type::GREATER_OR_EQUAL:
            *op = VX_OPERATOR_GTE;
            return true;
        case Function::Type::LESS_THAN:
            *op = VX_OPERATOR_LT;
            return true;
        case Function::Type::LESS_OR_EQUAL:
            *op = VX_OPERATOR_LTE;
            return true;
        default:
            return false;
    }
}

Result<VxExpressionPtr> ConvertLeaf(const std::shared_ptr<LeafPredicate>& leaf,
                                    const std::shared_ptr<arrow::Schema>& file_schema,
                                    const vx_expression* root,
                                    const VortexPredicateOptions& options) {
    const Function::Type type = leaf->GetFunction().GetType();
    vx_binary_operator op = VX_OPERATOR_EQ;
    const bool is_comparison = ComparisonOperator(type, &op);
    if (is_comparison && leaf->Literals().size() != 1) {
        return Status::Invalid("Comparison predicate for field '", leaf->FieldName(),
                               "' requires exactly one literal");
    }
    const int32_t field_index =
        file_schema != nullptr ? file_schema->GetFieldIndex(leaf->FieldName()) : -1;
    if (field_index < 0) {
        // Missing fields are null-filled by the reader, but cannot bind to a native file scan.
        return VxExpressionPtr();
    }
    const std::shared_ptr<arrow::DataType>& arrow_type = file_schema->field(field_index)->type();
    VxExpressionPtr column(
        vx_expression_get_item(vx_view{leaf->FieldName().data(), leaf->FieldName().size()}, root));
    if (type == Function::Type::IS_NULL || type == Function::Type::IS_NOT_NULL) {
        VxExpressionPtr is_null(vx_expression_is_null(column.get()));
        if (type == Function::Type::IS_NULL) {
            return is_null;
        }
        return VxExpressionPtr(vx_expression_not(is_null.get()));
    }
    if (type == Function::Type::IN) {
        return ConvertMembership(leaf, arrow_type, column.get(), false, options);
    }
    if (type == Function::Type::NOT_IN) {
        return ConvertMembership(leaf, arrow_type, column.get(), true, options);
    }
    if (!is_comparison) {
        return VxExpressionPtr();
    }
    PAIMON_ASSIGN_OR_RAISE(bool type_eligible, CheckFieldTypePushdownEligibility(leaf, arrow_type));
    if (!type_eligible) {
        return VxExpressionPtr();
    }
    PAIMON_ASSIGN_OR_RAISE(VxExpressionPtr literal_expr,
                           MakeLiteralExpr(leaf->Literals()[0], arrow_type));
    if (literal_expr == nullptr) {
        return VxExpressionPtr();
    }
    VxExpressionPtr expression(vx_expression_binary(op, column.get(), literal_expr.get()));
    return expression;
}

Result<VxExpressionPtr> ConvertImpl(const std::shared_ptr<Predicate>& predicate,
                                    const std::shared_ptr<arrow::Schema>& file_schema,
                                    const vx_expression* root,
                                    const VortexPredicateOptions& options) {
    const Function::Type type = predicate->GetFunction().GetType();
    if (type == Function::Type::AND || type == Function::Type::OR) {
        const auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
        if (compound == nullptr) {
            return Status::Invalid("AND/OR predicate must be a CompoundPredicate");
        }
        if (compound->Children().empty()) {
            return Status::Invalid("AND/OR predicate must have at least one child");
        }
        std::vector<VxExpressionPtr> children;
        children.reserve(compound->Children().size());
        for (const std::shared_ptr<Predicate>& child : compound->Children()) {
            if (child == nullptr) {
                return Status::Invalid("AND/OR predicate must not contain a null child");
            }
            PAIMON_ASSIGN_OR_RAISE(VxExpressionPtr converted,
                                   ConvertImpl(child, file_schema, root, options));
            if (converted != nullptr) {
                children.push_back(std::move(converted));
            }
            // Visit all branches so an unsupported OR child cannot hide a later error.
        }
        // AND can retain supported children; dropping any OR branch could exclude matching rows.
        if (children.empty() ||
            (type == Function::Type::OR && children.size() != compound->Children().size())) {
            return VxExpressionPtr();
        }
        std::vector<const vx_expression*> raw;
        raw.reserve(children.size());
        for (const VxExpressionPtr& child : children) {
            raw.push_back(child.get());
        }
        VxExpressionPtr combined(type == Function::Type::AND
                                     ? vx_expression_and(raw.data(), raw.size())
                                     : vx_expression_or(raw.data(), raw.size()));
        return combined;
    }
    const auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    if (leaf == nullptr) {
        vx_binary_operator op = VX_OPERATOR_EQ;
        if (ComparisonOperator(type, &op) || type == Function::Type::IS_NULL ||
            type == Function::Type::IS_NOT_NULL || type == Function::Type::IN ||
            type == Function::Type::NOT_IN) {
            return Status::Invalid("Leaf predicate must be a LeafPredicate");
        }
        return VxExpressionPtr();
    }
    return ConvertLeaf(leaf, file_schema, root, options);
}

}  // namespace

Result<VxExpressionPtr> VortexPredicateConverter::Convert(
    const std::shared_ptr<Predicate>& predicate, const std::shared_ptr<arrow::Schema>& file_schema,
    const VortexPredicateOptions& options) {
    if (predicate == nullptr) {
        return VxExpressionPtr();
    }
    VxExpressionPtr root(vx_expression_root());
    return ConvertImpl(predicate, file_schema, root.get(), options);
}

}  // namespace paimon::vortex
