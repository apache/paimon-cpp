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

#include "paimon/common/predicate/equal.h"

#include <string_view>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "paimon/common/predicate/not_equal.h"
#include "paimon/common/utils/checked_cast.h"

namespace paimon {
class LeafFunction;

namespace {
template <typename Matches>
std::vector<char> TestEqualValues(const arrow::Array& array, Matches matches) {
    std::vector<char> result(array.length(), false);
    if (array.null_count() == 0) {
        for (int64_t i = 0; i < array.length(); ++i) {
            result[i] = matches(i);
        }
    } else {
        for (int64_t i = 0; i < array.length(); ++i) {
            result[i] = !array.IsNull(i) && matches(i);
        }
    }
    return result;
}

template <typename ArrayType, typename ValueType>
std::vector<char> TestEqualPrimitive(const arrow::Array& array, const Literal& literal) {
    const auto& values = checked_cast<const ArrayType&>(array);
    const auto expected = literal.GetValue<ValueType>();
    return TestEqualValues(array, [&](int64_t i) { return values.Value(i) == expected; });
}

template <typename ArrayType>
std::vector<char> TestEqualBinary(const arrow::Array& array, const Literal& literal) {
    const auto& values = checked_cast<const ArrayType&>(array);
    const auto expected = literal.GetValue<std::string>();
    return TestEqualValues(array, [&](int64_t i) {
        const auto value = values.GetView(i);
        return std::string_view(value.data(), value.size()) == expected;
    });
}
}  // namespace

Result<std::vector<char>> Equal::Test(const arrow::Array& array,
                                      const std::vector<Literal>& literals,
                                      arrow::MemoryPool* pool) const {
    if (!literals.empty() && !literals[0].IsNull()) {
        const Literal& literal = literals[0];
        // Only bypass Literal conversion for types with identical equality semantics. Keep the
        // existing conversion and validation for all other types, including mismatched literals.
        switch (array.type_id()) {
            case arrow::Type::BOOL:
                if (literal.GetType() == FieldType::BOOLEAN) {
                    return TestEqualPrimitive<arrow::BooleanArray, bool>(array, literal);
                }
                break;
            case arrow::Type::INT8:
                if (literal.GetType() == FieldType::TINYINT) {
                    return TestEqualPrimitive<arrow::Int8Array, int8_t>(array, literal);
                }
                break;
            case arrow::Type::INT16:
                if (literal.GetType() == FieldType::SMALLINT) {
                    return TestEqualPrimitive<arrow::Int16Array, int16_t>(array, literal);
                }
                break;
            case arrow::Type::INT32:
                if (literal.GetType() == FieldType::INT) {
                    return TestEqualPrimitive<arrow::Int32Array, int32_t>(array, literal);
                }
                break;
            case arrow::Type::INT64:
                if (literal.GetType() == FieldType::BIGINT) {
                    return TestEqualPrimitive<arrow::Int64Array, int64_t>(array, literal);
                }
                break;
            case arrow::Type::DATE32:
                if (literal.GetType() == FieldType::DATE) {
                    return TestEqualPrimitive<arrow::Date32Array, int32_t>(array, literal);
                }
                break;
            case arrow::Type::STRING:
                if (literal.GetType() == FieldType::STRING) {
                    return TestEqualBinary<arrow::StringArray>(array, literal);
                }
                break;
            case arrow::Type::BINARY:
                if (literal.GetType() == FieldType::BINARY) {
                    return TestEqualBinary<arrow::BinaryArray>(array, literal);
                }
                break;
            default:
                break;
        }
    }
    return NullFalseLeafBinaryFunction::Test(array, literals, pool);
}

const LeafFunction* Equal::Negate() const {
    return &NotEqual::Instance();
}

}  // namespace paimon
