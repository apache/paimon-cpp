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

#include "paimon/common/predicate/null_false_leaf_binary_function.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace paimon {
namespace {
/// The `arrow::compute` kernel that compares a column against a scalar the way `Test(field,
/// literal)` compares two `Literal`s, or `nullptr` for a function that is not a comparison.
///
/// `STARTS_WITH`, `ENDS_WITH`, `CONTAINS` and `LIKE` are `NullFalseLeafBinaryFunction`s too, but
/// they match a pattern instead of ordering two values, so they are not here and keep the row by
/// row path.
const char* ComparisonKernel(Function::Type type) {
    switch (type) {
        case Function::Type::EQUAL:
            return "equal";
        case Function::Type::NOT_EQUAL:
            return "not_equal";
        case Function::Type::LESS_THAN:
            return "less";
        case Function::Type::LESS_OR_EQUAL:
            return "less_equal";
        case Function::Type::GREATER_THAN:
            return "greater";
        case Function::Type::GREATER_OR_EQUAL:
            return "greater_equal";
        default:
            return nullptr;
    }
}

/// The field types one comparison kernel can stand in for `Literal::CompareTo`.
///
/// `FLOAT` and `DOUBLE` are not here: `FieldsComparator::CompareFloatingPoint` orders
/// `-0.0 < +0.0` and makes every NaN equal to every NaN, where a kernel follows IEEE 754 and says
/// `-0.0 == +0.0` and that no NaN compares to anything. Unlike a NaN literal, which `IN` keeps off
/// its own fast path by looking at the literals, that divergence is a property of the column, so no
/// literal can be checked for it and a float column keeps the row by row path.
bool CanCompareWithKernel(FieldType field_type) {
    switch (field_type) {
        case FieldType::BOOLEAN:
        case FieldType::TINYINT:
        case FieldType::SMALLINT:
        case FieldType::INT:
        case FieldType::BIGINT:
        case FieldType::DATE:
        case FieldType::STRING:
        case FieldType::BINARY:
        case FieldType::DECIMAL:
        case FieldType::TIMESTAMP:
            return true;
        default:
            return false;
    }
}

/// The `FieldType` the row by row path reads the values of `array` as, which is the one
/// `LiteralConverter::ConvertLiteralsFromArray` gives them, or `std::nullopt` for a layout that
/// conversion rejects and this therefore has to leave to it to report.
std::optional<FieldType> ArrayFieldType(const arrow::Array& array) {
    const std::shared_ptr<arrow::DataType>& type = array.type();
    if (type->id() == arrow::Type::DICTIONARY) {
        const auto& dict_type = checked_cast<const arrow::DictionaryType&>(*type);
        const bool is_string = dict_type.value_type()->id() == arrow::Type::STRING &&
                               dict_type.index_type()->id() == arrow::Type::INT32;
        const bool is_large_string = dict_type.value_type()->id() == arrow::Type::LARGE_STRING &&
                                     dict_type.index_type()->id() == arrow::Type::INT64;
        if (is_string || is_large_string) {
            return FieldType::STRING;
        }
        return std::nullopt;
    }
    Result<FieldType> field_type = FieldTypeUtils::ConvertToFieldType(type->id());
    if (!field_type.ok()) {
        return std::nullopt;
    }
    return field_type.value();
}

/// Whether the column and the scalar the literal was written to can be compared without arrow
/// casting one side to a type that `Literal::CompareTo` does not compare by.
///
/// A kernel resolves a type difference by casting, and a cast fails on a value the target type does
/// not keep, where `Decimal::CompareTo` rescales two decimals and compares them by value and
/// `Literal::CompareTo` compares two timestamps by the instant they name and merely finds the
/// answer. A decimal column of another scale than the literal and a timestamp column of another
/// unit or with a time zone therefore keep the row by row path.
///
/// Every other difference is one arrow resolves losslessly: a kernel decodes a dictionary column to
/// its values, a string column of another width compares byte by byte either way, and a decimal
/// column of another precision carrying the scale of the literal is widened without losing a digit.
/// The two `FieldType`s are already guarded to be the same one, so no numeric column reaches a
/// kernel of another width.
bool AgreesWithScalar(const arrow::DataType& column_type, const arrow::DataType& scalar_type) {
    switch (column_type.id()) {
        case arrow::Type::DECIMAL128:
            return scalar_type.id() == arrow::Type::DECIMAL128 &&
                   checked_cast<const arrow::Decimal128Type&>(column_type).scale() ==
                       checked_cast<const arrow::Decimal128Type&>(scalar_type).scale();
        case arrow::Type::TIMESTAMP:
            return scalar_type.id() == arrow::Type::TIMESTAMP &&
                   checked_cast<const arrow::TimestampType&>(column_type).unit() ==
                       checked_cast<const arrow::TimestampType&>(scalar_type).unit() &&
                   checked_cast<const arrow::TimestampType&>(column_type).timezone().empty();
        default:
            return true;
    }
}

/// Writes `literal` to the one element scalar a comparison kernel compares the column against.
///
/// @param field_type The field type of the literal, which picks the arrow type it is written with.
/// @param column_type The arrow type of the column the predicate is evaluated against, which
///                     settles whether a decimal or a timestamp scalar can be compared against it.
/// @param pool The pool the scalar is allocated from.
/// @return `nullptr` when the literal cannot be compared by a kernel, which covers a decimal column
///         of another scale and a timestamp column of another unit or with a time zone. This never
///         fails.
std::shared_ptr<arrow::Scalar> MakeComparisonScalar(const Literal& literal, FieldType field_type,
                                                    const arrow::DataType& column_type,
                                                    arrow::MemoryPool* pool) {
    // A `NullFalseLeafBinaryFunction` compares one literal at a time, so there is exactly one to
    // write. `ConvertLiteralsToArray` is what `IN` writes its value set with, and it settles the
    // arrow type of the scalar the same way for both.
    Result<std::shared_ptr<arrow::Array>> one =
        LiteralConverter::ConvertLiteralsToArray(field_type, {literal}, pool);
    // A failure only says the scalar is not there, and the row by row path still is.
    if (!one.ok()) {
        return nullptr;
    }
    arrow::Result<std::shared_ptr<arrow::Scalar>> scalar = one.value()->GetScalar(0);
    if (!scalar.ok()) {
        return nullptr;
    }
    const std::shared_ptr<arrow::Scalar>& value = scalar.ValueOrDie();
    if (!AgreesWithScalar(column_type, *value->type)) {
        return nullptr;
    }
    return value;
}

/// Compares every row of `array` against `literal` with the kernel `kernel_name`.
///
/// @param pool The pool the boolean bitmap the kernel writes is allocated from.
/// @return One entry per row, with the null rows left at 0 because a comparison is false on null.
///
/// The kernel resolves the comparison itself: it decodes a dictionary column and emits a null for a
/// null row. It fails when the two types have no common type at all, which only happens when the
/// field type disagrees with the column the predicate is evaluated against.
Result<std::vector<char>> ProbeComparison(const arrow::Array& array, const char* kernel_name,
                                          const std::shared_ptr<arrow::Scalar>& literal,
                                          arrow::MemoryPool* pool) {
    arrow::compute::ExecContext exec_context(pool);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow::Datum matches,
        arrow::compute::CallFunction(kernel_name, {arrow::Datum(array), arrow::Datum(literal)},
                                     /*options=*/nullptr, &exec_context));
    // `make_array` hands out a new `shared_ptr` that owns the array, so it has to be held for as
    // long as the array is read. Binding a reference straight to what it points at would drop the
    // last owner at the end of the statement and leave that reference dangling.
    std::shared_ptr<arrow::Array> matches_array = matches.make_array();
    const auto& matched = checked_cast<const arrow::BooleanArray&>(*matches_array);
    // A kernel emits a null for a null row, and every `NullFalseLeafBinaryFunction` is false on
    // one, so the null rows unpack to 0 rather than to a value they do not hold.
    return ArrowUtils::UnpackBooleansToBytes(matched, /*negate=*/false);
}
}  // namespace

Result<std::vector<char>> NullFalseLeafBinaryFunction::Test(const arrow::Array& array,
                                                            const std::vector<Literal>& literals,
                                                            arrow::MemoryPool* pool) const {
    if (literals.size() < LITERAL_LIMIT) {
        return Status::Invalid("NullFalseLeafBinaryFunction needs single literal for field");
    }
    std::vector<char> is_valid(array.length(), false);
    if (literals[0].IsNull()) {
        return is_valid;
    }

    const char* kernel_name = ComparisonKernel(GetType());
    if (kernel_name != nullptr) {
        const std::optional<FieldType> field_type = ArrayFieldType(array);
        // A literal typed differently from the column makes `Literal::CompareTo` fail, so it keeps
        // the row by row path that reports it instead of reaching a kernel that would cast one side
        // to the other and compare by chance.
        if (field_type != std::nullopt && *field_type == literals[0].GetType() &&
            CanCompareWithKernel(*field_type)) {
            std::shared_ptr<arrow::Scalar> scalar =
                MakeComparisonScalar(literals[0], *field_type, *array.type(), pool);
            if (scalar != nullptr) {
                return ProbeComparison(array, kernel_name, scalar, pool);
            }
        }
    }

    // Materializing the column into `Literal` objects costs one heap allocation and one hash of the
    // value per row, so this only runs when no kernel can compare the column against the literal.
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> array_values,
                           LiteralConverter::ConvertLiteralsFromArray(array, /*own_data=*/false));
    for (int64_t i = 0; i < array.length(); i++) {
        if (!array.IsNull(i)) {
            PAIMON_ASSIGN_OR_RAISE(is_valid[i], Test(array_values[i], literals[0]));
        }
    }
    return is_valid;
}

}  // namespace paimon
