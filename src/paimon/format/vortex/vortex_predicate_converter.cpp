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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/utils/checked_cast.h"
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

VxExpressionPtr NullExpr() {
    return VxExpressionPtr(nullptr, vx_expression_free);
}

void ClearError(vx_error* error) {
    if (error != nullptr) {
        vx_error_free(error);
    }
}

// vx_expression_not returns an owned handle typed const; reclaim it as an owned non-const pointer.
VxExpressionPtr MakeNotExpr(const vx_expression* child) {
    return VxExpressionPtr(const_cast<vx_expression*>(vx_expression_not(child)),
                           vx_expression_free);
}

// Reject precision loss and overflow before constructing a native timestamp scalar.
bool GetTimestampValue(const Literal& literal, const std::shared_ptr<arrow::DataType>& arrow_type,
                       int64_t* result, vx_time_unit* result_unit) {
    if (arrow_type == nullptr || arrow_type->id() != arrow::Type::TIMESTAMP) {
        return false;
    }
    const auto& timestamp_type = checked_cast<const arrow::TimestampType&>(*arrow_type);
    if (!timestamp_type.timezone().empty() && timestamp_type.timezone() != "UTC") {
        return false;
    }
    const auto timestamp = literal.GetValue<Timestamp>();
    int64_t value = timestamp.GetMillisecond();
    const int32_t nanos = timestamp.GetNanoOfMillisecond();
    vx_time_unit unit = VX_TIME_UNIT_MILLISECOND;
    int64_t factor = 1;
    int64_t fraction = 0;
    switch (timestamp_type.unit()) {
        case arrow::TimeUnit::SECOND:
            if (value % 1000 != 0 || nanos != 0) {
                return false;
            }
            value /= 1000;
            unit = VX_TIME_UNIT_SECOND;
            break;
        case arrow::TimeUnit::MILLI:
            if (nanos != 0) {
                return false;
            }
            break;
        case arrow::TimeUnit::MICRO:
            if (nanos % 1000 != 0) {
                return false;
            }
            factor = 1000;
            fraction = nanos / 1000;
            unit = VX_TIME_UNIT_MICROSECOND;
            break;
        case arrow::TimeUnit::NANO:
            factor = 1000000;
            fraction = nanos;
            unit = VX_TIME_UNIT_NANOSECOND;
            break;
    }
    if (value < std::numeric_limits<int64_t>::min() / factor ||
        value > std::numeric_limits<int64_t>::max() / factor) {
        return false;
    }
    value *= factor;
    if (value > std::numeric_limits<int64_t>::max() - fraction) {
        return false;
    }
    *result = value + fraction;
    *result_unit = unit;
    return true;
}

VxScalarPtr MakeLiteralScalar(const Literal& literal,
                              const std::shared_ptr<arrow::DataType>& arrow_type) {
    VxScalarPtr scalar(nullptr, vx_scalar_free);
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
        case FieldType::DATE:
            scalar.reset(vx_scalar_new_date(literal.GetValue<int32_t>(), false, &error));
            break;
        case FieldType::BIGINT:
            scalar.reset(vx_scalar_new_i64(literal.GetValue<int64_t>(), false));
            break;
        case FieldType::FLOAT:
        case FieldType::DOUBLE:
            // Native totalOrder distinguishes NaN signs and payloads; Paimon treats all NaNs alike.
            return scalar;
        case FieldType::STRING: {
            const auto value = literal.GetValue<std::string>();
            scalar.reset(vx_scalar_new_utf8(value.data(), value.size(), false, &error));
            break;
        }
        case FieldType::DECIMAL: {
            const auto decimal = literal.GetValue<Decimal>();
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
            vx_time_unit unit = VX_TIME_UNIT_MILLISECOND;
            if (!GetTimestampValue(literal, arrow_type, &value, &unit)) {
                return scalar;
            }
            const auto& timestamp_type = checked_cast<const arrow::TimestampType&>(*arrow_type);
            const char* zone = timestamp_type.timezone().empty() ? nullptr : "UTC";
            scalar.reset(vx_scalar_new_timestamp(value, unit, zone, false, &error));
            break;
        }
        default:
            return scalar;
    }
    ClearError(error);
    return scalar;
}

VxExpressionPtr MakeLiteralExpr(const Literal& literal,
                                const std::shared_ptr<arrow::DataType>& arrow_type) {
    VxScalarPtr scalar = MakeLiteralScalar(literal, arrow_type);
    if (scalar == nullptr) {
        return NullExpr();
    }
    vx_error* literal_error = nullptr;
    vx_expression* expr = vx_expression_literal(scalar.get(), &literal_error);
    ClearError(literal_error);
    return VxExpressionPtr(expr, vx_expression_free);
}

// Reject implicit coercions that could narrow values before the schema-evolution/residual path.
bool LiteralMatchesColumn(const Literal& literal, FieldType field_type,
                          const std::shared_ptr<arrow::DataType>& arrow_type) {
    if (literal.IsNull() || literal.GetType() != field_type || arrow_type == nullptr) {
        return false;
    }
    switch (field_type) {
        case FieldType::BOOLEAN:
            return arrow_type->id() == arrow::Type::BOOL;
        case FieldType::TINYINT:
            return arrow_type->id() == arrow::Type::INT8;
        case FieldType::SMALLINT:
            return arrow_type->id() == arrow::Type::INT16;
        case FieldType::INT:
            return arrow_type->id() == arrow::Type::INT32;
        case FieldType::BIGINT:
            return arrow_type->id() == arrow::Type::INT64;
        case FieldType::STRING:
            return arrow_type->id() == arrow::Type::STRING;
        case FieldType::DATE:
            return arrow_type->id() == arrow::Type::DATE32;
        case FieldType::DECIMAL: {
            if (arrow_type->id() != arrow::Type::DECIMAL128) {
                return false;
            }
            const auto& decimal_type = checked_cast<const arrow::Decimal128Type&>(*arrow_type);
            const auto decimal = literal.GetValue<Decimal>();
            return decimal.Precision() == decimal_type.precision() &&
                   decimal.Scale() == decimal_type.scale();
        }
        case FieldType::TIMESTAMP: {
            if (arrow_type->id() != arrow::Type::TIMESTAMP) {
                return false;
            }
            const auto& timestamp_type = checked_cast<const arrow::TimestampType&>(*arrow_type);
            return timestamp_type.timezone().empty() || timestamp_type.timezone() == "UTC";
        }
        default:
            // NaN and signed-zero equivalence needs separate validation; keep the residual path.
            return false;
    }
}

struct LiteralPointerHash {
    size_t operator()(const Literal* literal) const {
        return literal->HashCode();
    }
};

struct LiteralPointerEqual {
    bool operator()(const Literal* lhs, const Literal* rhs) const {
        return *lhs == *rhs;
    }
};

// All literals must be validated before their values are inspected or used as hash keys. Decimal
// scale/precision is identical to the column, so Literal's same-scale hash equality is safe here.
bool ValidateAndDeduplicate(const std::shared_ptr<LeafPredicate>& leaf,
                            const std::shared_ptr<arrow::DataType>& arrow_type,
                            std::vector<const Literal*>* literals) {
    if (leaf->Literals().empty()) {
        return false;
    }
    for (const Literal& literal : leaf->Literals()) {
        if (!LiteralMatchesColumn(literal, leaf->GetFieldType(), arrow_type)) {
            return false;
        }
        if (leaf->GetFieldType() == FieldType::TIMESTAMP) {
            int64_t value = 0;
            vx_time_unit unit = VX_TIME_UNIT_MILLISECOND;
            if (!GetTimestampValue(literal, arrow_type, &value, &unit)) {
                return false;
            }
        }
    }
    std::unordered_set<const Literal*, LiteralPointerHash, LiteralPointerEqual> unique;
    unique.reserve(leaf->Literals().size());
    literals->reserve(leaf->Literals().size());
    for (const Literal& literal : leaf->Literals()) {
        if (unique.insert(&literal).second) {
            literals->push_back(&literal);
        }
    }
    return true;
}

VxExpressionPtr ConvertInList(const std::vector<const Literal*>& literals,
                              const std::shared_ptr<arrow::DataType>& arrow_type,
                              const vx_expression* column) {
    std::vector<VxScalarPtr> scalars;
    std::vector<const vx_scalar*> raw;
    scalars.reserve(literals.size());
    raw.reserve(literals.size());
    for (const Literal* literal : literals) {
        VxScalarPtr scalar = MakeLiteralScalar(*literal, arrow_type);
        if (scalar == nullptr) {
            return NullExpr();
        }
        raw.push_back(scalar.get());
        scalars.push_back(std::move(scalar));
    }
    vx_error* error = nullptr;
    VxScalarPtr list(
        vx_scalar_new_list(vx_scalar_dtype(raw[0]), raw.data(), raw.size(), false, &error),
        vx_scalar_free);
    ClearError(error);
    if (list == nullptr) {
        return NullExpr();
    }
    error = nullptr;
    VxExpressionPtr literal_expr(vx_expression_literal(list.get(), &error), vx_expression_free);
    ClearError(error);
    if (literal_expr == nullptr) {
        return NullExpr();
    }
    return VxExpressionPtr(vx_expression_list_contains(literal_expr.get(), column),
                           vx_expression_free);
}

bool IsSignedInteger(FieldType field_type) {
    return field_type == FieldType::TINYINT || field_type == FieldType::SMALLINT ||
           field_type == FieldType::INT || field_type == FieldType::BIGINT;
}

// Only called for non-null signed integers whose type has already been validated.
int64_t SignedIntegerValue(const Literal& literal) {
    switch (literal.GetType()) {
        case FieldType::TINYINT:
            return literal.GetValue<int8_t>();
        case FieldType::SMALLINT:
            return literal.GetValue<int16_t>();
        case FieldType::INT:
            return literal.GetValue<int32_t>();
        case FieldType::BIGINT:
            return literal.GetValue<int64_t>();
        default:
            assert(false);
            return 0;
    }
}

using IntegerRange = std::pair<const Literal*, const Literal*>;

std::vector<IntegerRange> PlanIntegerRanges(std::vector<const Literal*> literals) {
    std::sort(literals.begin(), literals.end(), [](const Literal* lhs, const Literal* rhs) {
        return SignedIntegerValue(*lhs) < SignedIntegerValue(*rhs);
    });
    std::vector<IntegerRange> ranges;
    for (const Literal* literal : literals) {
        const int64_t value = SignedIntegerValue(*literal);
        const int64_t previous = ranges.empty() ? 0 : SignedIntegerValue(*ranges.back().second);
        // Guard before addition, including when the stored type is BIGINT at INT64_MAX.
        if (!ranges.empty() && previous != std::numeric_limits<int64_t>::max() &&
            value == previous + 1) {
            ranges.back().second = literal;
        } else {
            ranges.emplace_back(literal, literal);
        }
    }
    return ranges;
}

// Exclude input/literal nodes. Each range costs two comparisons and one connective; R ranges
// need R-1 additional connectives.
uint64_t IntegerRangeNodes(const std::vector<IntegerRange>& ranges) {
    uint64_t nodes = ranges.size() - 1;
    for (const auto& [lower, upper] : ranges) {
        nodes += lower == upper ? 1 : 3;
    }
    return nodes;
}

VxExpressionPtr ConvertIntegerRanges(const std::vector<IntegerRange>& ranges,
                                     const std::shared_ptr<arrow::DataType>& arrow_type,
                                     const vx_expression* column, bool negate) {
    std::vector<VxExpressionPtr> expressions;
    expressions.reserve(ranges.size());
    for (const auto& [lower, upper] : ranges) {
        VxExpressionPtr lower_literal = MakeLiteralExpr(*lower, arrow_type);
        if (lower_literal == nullptr) {
            return NullExpr();
        }
        if (lower == upper) {
            VxExpressionPtr equal(vx_expression_binary(negate ? VX_OPERATOR_NOT_EQ : VX_OPERATOR_EQ,
                                                       column, lower_literal.get()),
                                  vx_expression_free);
            if (equal == nullptr) {
                return NullExpr();
            }
            expressions.push_back(std::move(equal));
            continue;
        }
        VxExpressionPtr upper_literal = MakeLiteralExpr(*upper, arrow_type);
        if (upper_literal == nullptr) {
            return NullExpr();
        }
        VxExpressionPtr lower_bound(vx_expression_binary(negate ? VX_OPERATOR_LT : VX_OPERATOR_GTE,
                                                         column, lower_literal.get()),
                                    vx_expression_free);
        VxExpressionPtr upper_bound(vx_expression_binary(negate ? VX_OPERATOR_GT : VX_OPERATOR_LTE,
                                                         column, upper_literal.get()),
                                    vx_expression_free);
        if (lower_bound == nullptr || upper_bound == nullptr) {
            return NullExpr();
        }
        const vx_expression* bounds[] = {lower_bound.get(), upper_bound.get()};
        VxExpressionPtr expression(
            negate ? vx_expression_or(bounds, 2) : vx_expression_and(bounds, 2),
            vx_expression_free);
        if (expression == nullptr) {
            return NullExpr();
        }
        expressions.push_back(std::move(expression));
    }
    if (expressions.size() == 1) {
        return std::move(expressions.front());
    }
    std::vector<const vx_expression*> raw;
    raw.reserve(expressions.size());
    for (const VxExpressionPtr& expression : expressions) {
        raw.push_back(expression.get());
    }
    // IN is the union of closed ranges; NOT IN intersects their open complements.
    return VxExpressionPtr(negate ? vx_expression_and(raw.data(), raw.size())
                                  : vx_expression_or(raw.data(), raw.size()),
                           vx_expression_free);
}

VxExpressionPtr ConvertMembership(const std::shared_ptr<LeafPredicate>& leaf,
                                  const std::shared_ptr<arrow::DataType>& arrow_type,
                                  const vx_expression* column, bool negate,
                                  const VortexPredicateOptions& options) {
    if (options.membership_pushdown == MembershipPushdownMode::OFF) {
        return NullExpr();
    }
    std::vector<const Literal*> literals;
    if (!ValidateAndDeduplicate(leaf, arrow_type, &literals)) {
        return NullExpr();
    }
    std::vector<IntegerRange> ranges;
    if (IsSignedInteger(leaf->GetFieldType())) {
        ranges = PlanIntegerRanges(literals);
    }
    // ListContains lowers N values to N equality nodes and N-1 OR nodes. NOT IN adds NOT plus
    // the IS NOT NULL guard (IS NULL, NOT, AND); a complement-of-ranges needs only the guard.
    const uint64_t list_nodes = 2 * static_cast<uint64_t>(literals.size()) - 1 + (negate ? 4 : 0);
    const uint64_t range_nodes =
        ranges.empty() ? list_nodes : IntegerRangeNodes(ranges) + (negate ? 3 : 0);
    const bool use_ranges = !ranges.empty() && range_nodes < list_nodes;
    const uint64_t nodes = use_ranges ? range_nodes : list_nodes;
    if (options.membership_pushdown == MembershipPushdownMode::AUTO &&
        nodes > options.max_membership_nodes) {
        return NullExpr();
    }
    // Allocate native scalars only after the plan passes the budget; ranges need only endpoints.
    VxExpressionPtr expression = use_ranges
                                     ? ConvertIntegerRanges(ranges, arrow_type, column, negate)
                                     : ConvertInList(literals, arrow_type, column);
    if (expression == nullptr || !negate) {
        return expression;
    }
    if (!use_ranges) {
        expression = MakeNotExpr(expression.get());
    }
    VxExpressionPtr is_null(vx_expression_is_null(column), vx_expression_free);
    if (expression == nullptr || is_null == nullptr) {
        return NullExpr();
    }
    VxExpressionPtr is_not_null = MakeNotExpr(is_null.get());
    if (is_not_null == nullptr) {
        return NullExpr();
    }
    // Neither IN nor NOT IN may retain null rows. Validation also rejects null/empty lists.
    const vx_expression* children[] = {is_not_null.get(), expression.get()};
    return VxExpressionPtr(vx_expression_and(children, 2), vx_expression_free);
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

VxExpressionPtr ConvertLeaf(const std::shared_ptr<LeafPredicate>& leaf,
                            const std::shared_ptr<arrow::Schema>& file_schema,
                            const vx_expression* root, const VortexPredicateOptions& options) {
    const int32_t field_index =
        file_schema != nullptr ? file_schema->GetFieldIndex(leaf->FieldName()) : -1;
    if (field_index < 0) {
        // Missing fields are null-filled by the reader, but cannot bind to a native file scan.
        return NullExpr();
    }
    const std::shared_ptr<arrow::DataType>& arrow_type = file_schema->field(field_index)->type();
    const Function::Type type = leaf->GetFunction().GetType();
    VxExpressionPtr column(vx_expression_get_item(leaf->FieldName().c_str(), root),
                           vx_expression_free);
    if (column == nullptr) {
        return NullExpr();
    }
    if (type == Function::Type::IS_NULL) {
        return VxExpressionPtr(vx_expression_is_null(column.get()), vx_expression_free);
    }
    if (type == Function::Type::IS_NOT_NULL) {
        VxExpressionPtr is_null(vx_expression_is_null(column.get()), vx_expression_free);
        if (is_null == nullptr) {
            return NullExpr();
        }
        return MakeNotExpr(is_null.get());
    }
    if (type == Function::Type::IN) {
        return ConvertMembership(leaf, arrow_type, column.get(), false, options);
    }
    if (type == Function::Type::NOT_IN) {
        return ConvertMembership(leaf, arrow_type, column.get(), true, options);
    }
    vx_binary_operator op = VX_OPERATOR_EQ;
    if (!ComparisonOperator(type, &op)) {
        return NullExpr();
    }
    if (leaf->Literals().empty() ||
        !LiteralMatchesColumn(leaf->Literals()[0], leaf->GetFieldType(), arrow_type)) {
        return NullExpr();
    }
    VxExpressionPtr literal_expr = MakeLiteralExpr(leaf->Literals()[0], arrow_type);
    if (literal_expr == nullptr) {
        return NullExpr();
    }
    return VxExpressionPtr(vx_expression_binary(op, column.get(), literal_expr.get()),
                           vx_expression_free);
}

VxExpressionPtr ConvertImpl(const std::shared_ptr<Predicate>& predicate,
                            const std::shared_ptr<arrow::Schema>& file_schema,
                            const vx_expression* root, const VortexPredicateOptions& options) {
    if (predicate == nullptr) {
        return NullExpr();
    }
    const Function::Type type = predicate->GetFunction().GetType();
    if (type == Function::Type::AND || type == Function::Type::OR) {
        const auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
        if (compound == nullptr) {
            return NullExpr();
        }
        std::vector<VxExpressionPtr> children;
        children.reserve(compound->Children().size());
        for (const std::shared_ptr<Predicate>& child : compound->Children()) {
            VxExpressionPtr converted = ConvertImpl(child, file_schema, root, options);
            if (converted != nullptr) {
                children.push_back(std::move(converted));
            } else if (type == Function::Type::OR) {
                // Dropping an OR branch could exclude matching rows.
                return NullExpr();
            }
            // AND: an unpushable child is dropped; the residual filter still applies it.
        }
        if (children.empty()) {
            return NullExpr();
        }
        std::vector<const vx_expression*> raw;
        raw.reserve(children.size());
        for (const VxExpressionPtr& child : children) {
            raw.push_back(child.get());
        }
        vx_expression* combined = type == Function::Type::AND
                                      ? vx_expression_and(raw.data(), raw.size())
                                      : vx_expression_or(raw.data(), raw.size());
        return VxExpressionPtr(combined, vx_expression_free);
    }
    const auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    if (leaf == nullptr) {
        return NullExpr();
    }
    return ConvertLeaf(leaf, file_schema, root, options);
}

}  // namespace

VxExpressionPtr VortexPredicateConverter::Convert(const std::shared_ptr<Predicate>& predicate,
                                                  const std::shared_ptr<arrow::Schema>& file_schema,
                                                  const VortexPredicateOptions& options) {
    if (predicate == nullptr) {
        return NullExpr();
    }
    VxExpressionPtr root(vx_expression_root(), vx_expression_free);
    if (root == nullptr) {
        return NullExpr();
    }
    return ConvertImpl(predicate, file_schema, root.get(), options);
}

}  // namespace paimon::vortex
