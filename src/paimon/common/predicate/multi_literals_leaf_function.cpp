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

#include "paimon/common/predicate/multi_literals_leaf_function.h"

#include <cmath>
#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/datum.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"

namespace paimon {
namespace {
/// The field types `arrow::compute::is_in` can stand in for `Literal::CompareTo`. These are also
/// the field types `LiteralConverter::ConvertLiteralsToArray` writes today, and the check keeps one
/// it starts writing from reaching `is_in` on its own.
///
/// `DECIMAL` and `TIMESTAMP` are left out because their arrow types are parameterized, by precision
/// and scale and by time unit, which a `FieldType` alone does not pin down. `is_in` does compare a
/// decimal value set against a column of another scale correctly, but it gets there by casting the
/// whole column, which overflows into an error where `Literal::CompareTo` merely finds no match.
/// `FLOAT` and `DOUBLE` agree on every value but NaN, which `MakeInValueSet` keeps off this path
/// on its own.
bool CanProbeWithIsIn(FieldType field_type) {
    switch (field_type) {
        case FieldType::BOOLEAN:
        case FieldType::TINYINT:
        case FieldType::SMALLINT:
        case FieldType::INT:
        case FieldType::BIGINT:
        case FieldType::FLOAT:
        case FieldType::DOUBLE:
        case FieldType::DATE:
        case FieldType::STRING:
        case FieldType::BINARY:
            return true;
        default:
            return false;
    }
}

/// Whether `literal` holds a floating point NaN.
bool IsNanLiteral(const Literal& literal) {
    switch (literal.GetType()) {
        case FieldType::FLOAT:
            return std::isnan(literal.GetValue<float>());
        case FieldType::DOUBLE:
            return std::isnan(literal.GetValue<double>());
        default:
            return false;
    }
}

/// Builds the value set that `arrow::compute::is_in` takes from the literals of an `IN` / `NOT IN`
/// predicate. The arrow type comes from the literals themselves, which all share one `FieldType`.
///
/// @param negate `false` for `IN`, `true` for `NOT IN`.
/// @return `nullptr` when the literals cannot be probed by `is_in`, which covers the field types
///         `CanProbeWithIsIn` rejects, a NaN literal, and `NOT IN` holding a null literal, which
///         `NotIn::InnerTest` makes false for every row. This never fails.
std::shared_ptr<arrow::Array> MakeInValueSet(const std::vector<Literal>& literals, bool negate) {
    if (literals.empty()) {
        return nullptr;
    }
    // The literals of one predicate share a type, so take it from the first non-null one. When
    // every literal is null the value set is all nulls, which `is_in` ignores, but it still needs a
    // type to compare against the column, and a null `Literal` carries its type too.
    FieldType field_type = literals.front().GetType();
    for (const auto& literal : literals) {
        if (!literal.IsNull()) {
            field_type = literal.GetType();
            break;
        }
    }
    if (!CanProbeWithIsIn(field_type)) {
        return nullptr;
    }
    for (const auto& literal : literals) {
        // A literal typed differently makes `Literal::CompareTo` fail, keep that on the row by row
        // path.
        if (!literal.IsNull() && literal.GetType() != field_type) {
            return nullptr;
        }
        // `is_in` hashes the raw bits of a float, so a NaN literal would only match the column NaNs
        // carrying the very same bit pattern, while `FieldsComparator::CompareFloatingPoint` makes
        // every NaN equal. Keep a NaN literal on the row by row path. With none in the value set
        // the two agree, because a column NaN then matches no literal either way.
        if (!literal.IsNull() && IsNanLiteral(literal)) {
            return nullptr;
        }
        // `NotIn::InnerTest` returns false as soon as it meets a null literal, so no row can match
        // and there is nothing worth building.
        if (negate && literal.IsNull()) {
            return nullptr;
        }
    }
    Result<std::shared_ptr<arrow::Array>> value_set =
        LiteralConverter::ConvertLiteralsToArray(field_type, literals);
    // A failure only says the value set is not there, and the row by row path still is. The field
    // type is one it writes, so this is an arrow failure.
    if (!value_set.ok()) {
        return nullptr;
    }
    return std::move(value_set).value();
}

/// Probes every non-null row of `array` against `value_set`.
///
/// @param negate `false` for `IN` semantics, `true` for `NOT IN` semantics.
/// @return One entry per row, with the null rows left at 0 because `IN` and `NOT IN` are both false
///         on null.
///
/// `is_in` resolves the comparison itself: it decodes a dictionary column, and promotes both sides
/// to their common type when the column is read as a wider or narrower arrow type than the one
/// `value_set` was built with. It fails when the two types have no common type at all, which only
/// happens when the field type disagrees with the column the predicate is evaluated against.
Result<std::vector<char>> ProbeInValueSet(const arrow::Array& array, const arrow::Array& value_set,
                                          bool negate) {
    // `EMIT_NULL` ignores the nulls of the value set and turns a null input into a null output, so
    // the validity of `matches` marks exactly the rows that `In` / `NotIn` consider null. That also
    // covers a dictionary column, whose null rows come either from the indices or from a null
    // dictionary value once `is_in` decodes it.
    arrow::compute::SetLookupOptions options(value_set.data(),
                                             arrow::compute::SetLookupOptions::EMIT_NULL);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(arrow::Datum matches,
                                      arrow::compute::IsIn(arrow::Datum(array), options));
    // `make_array` hands out a new `shared_ptr` that owns the array, so it has to be held for as
    // long as the array is read. Binding a reference straight to what it points at would drop the
    // last owner at the end of the statement and leave that reference dangling.
    std::shared_ptr<arrow::Array> matches_array = matches.make_array();
    const auto& matched = checked_cast<const arrow::BooleanArray&>(*matches_array);
    std::vector<char> is_valid(matched.length(), 0);
    for (int64_t i = 0; i < matched.length(); i++) {
        if (matched.IsNull(i)) {
            // `IN` and `NOT IN` are both false on a null value, leave the row at 0.
            continue;
        }
        is_valid[i] = static_cast<char>(matched.Value(i) != negate);
    }
    return is_valid;
}
}  // namespace

Result<std::vector<char>> MultiLiteralsLeafFunction::Test(
    const arrow::Array& array, const std::vector<Literal>& literals) const {
    const Function::Type type = GetType();
    // `In` and `NotIn` are the only subclasses today. The check keeps a future one off this path,
    // because `MakeInValueSet` only looks at the literals and would silently give it `IN`
    // semantics.
    if (type == Function::Type::IN || type == Function::Type::NOT_IN) {
        const bool negate = type == Function::Type::NOT_IN;
        std::shared_ptr<arrow::Array> value_set = MakeInValueSet(literals, negate);
        if (value_set != nullptr) {
            return ProbeInValueSet(array, *value_set, negate);
        }
    }

    // Materializing the column into `Literal` objects costs one heap allocation per row and then
    // every row scans the literals linearly, so this only runs when `is_in` cannot probe them.
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> array_values,
                           LiteralConverter::ConvertLiteralsFromArray(array, /*own_data=*/false));
    std::vector<char> is_valid(array.length(), false);
    for (int64_t i = 0; i < array.length(); i++) {
        if (!array.IsNull(i)) {
            PAIMON_ASSIGN_OR_RAISE(is_valid[i], Test(array_values[i], literals));
        }
    }
    return is_valid;
}

}  // namespace paimon
