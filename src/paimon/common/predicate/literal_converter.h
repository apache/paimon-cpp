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

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_dict.h"
#include "arrow/type_traits.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/visibility.h"
namespace arrow {
class Array;
class Schema;
class StringArray;
}  // namespace arrow

namespace paimon {
class InternalRow;
enum class FieldType;
/// Utils for convert `Literal`.
class PAIMON_EXPORT LiteralConverter {
 public:
    LiteralConverter() = delete;
    ~LiteralConverter() = delete;

    static Result<std::vector<Literal>> ConvertLiteralsFromArray(const arrow::Array& array,
                                                                 bool own_data);

    /// Collects the literals into an arrow array, the reverse of `ConvertLiteralsFromArray`.
    ///
    /// @param field_type The field type shared by every literal, it picks the arrow type.
    /// @param literals The literals to convert, a null literal is written as a null, so the result
    ///                 has one entry per literal.
    /// @return `Status::Invalid` for a field type this does not write, which is every one outside
    ///         `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DATE`,
    ///         `STRING`, `BINARY`, `DECIMAL` and `TIMESTAMP`. `DECIMAL` is written with the
    ///         precision and the scale its literals carry, so at least one literal has to be non
    ///         null and every non null one has to carry the same pair. `TIMESTAMP` is written with
    ///         the finest time unit that keeps every value, so at least one literal has to be non
    ///         null to settle the unit.
    static Result<std::shared_ptr<arrow::Array>> ConvertLiteralsToArray(
        const FieldType& field_type, const std::vector<Literal>& literals);

    static Result<Literal> ConvertLiteralsFromString(const FieldType& type,
                                                     const std::string& value_str);

    static Result<Literal> ConvertLiteralsFromRow(const std::shared_ptr<arrow::Schema>& schema,
                                                  const InternalRow& row, int32_t field_idx,
                                                  const FieldType& type);

 private:
    template <class DataType>
    static std::vector<Literal> GetLiteralFromGenericArray(const arrow::Array& array,
                                                           const FieldType& literal_type) {
        using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
        using ValueType = typename arrow::TypeTraits<DataType>::CType;
        const ArrayType& array_(checked_cast<const ArrayType&>(array));
        std::vector<Literal> literals;
        literals.reserve(array_.length());
        for (int64_t i = 0; i < array_.length(); i++) {
            if (array_.IsNull(i)) {
                literals.emplace_back(literal_type);
            } else {
                literals.emplace_back(static_cast<ValueType>(array_.Value(i)));
            }
        }
        return literals;
    }

    template <class DataType>
    static std::vector<Literal> GetLiteralFromStringArray(const arrow::Array& array,
                                                          const FieldType& literal_type,
                                                          bool own_data) {
        using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
        using OffsetType = typename ArrayType::offset_type;
        const ArrayType& array_(checked_cast<const ArrayType&>(array));
        std::vector<Literal> literals;
        literals.reserve(array_.length());
        for (int64_t i = 0; i < array_.length(); i++) {
            if (array_.IsNull(i)) {
                literals.emplace_back(literal_type);
            } else {
                OffsetType length = 0;
                const uint8_t* value = array_.GetValue(i, &length);
                literals.emplace_back(literal_type, reinterpret_cast<const char*>(value), length,
                                      own_data);
            }
        }
        return literals;
    }

    template <typename DictArrayType, typename IndicesArrayType>
    static std::vector<Literal> GetLiteralFromDictionaryArray(
        const arrow::DictionaryArray& dict_array, const FieldType& literal_type, bool own_data) {
        auto* dictionary = checked_cast<DictArrayType*>(dict_array.dictionary().get());
        auto* indices = checked_cast<IndicesArrayType*>(dict_array.indices().get());
        std::vector<Literal> literals;
        literals.reserve(dict_array.length());
        for (int64_t i = 0; i < dict_array.length(); ++i) {
            if (dict_array.IsNull(i)) {
                literals.emplace_back(literal_type);
            } else {
                int64_t dict_index = indices->Value(i);
                if constexpr (std::is_same_v<DictArrayType, arrow::StringArray>) {
                    int32_t length = 0;
                    const uint8_t* value = dictionary->GetValue(dict_index, &length);
                    literals.emplace_back(literal_type, reinterpret_cast<const char*>(value),
                                          length, own_data);
                } else {
                    int64_t length = 0;
                    const uint8_t* value = dictionary->GetValue(dict_index, &length);
                    literals.emplace_back(literal_type, reinterpret_cast<const char*>(value),
                                          length, own_data);
                }
            }
        }
        return literals;
    }

    static std::vector<Literal> GetLiteralFromDecimalArray(const arrow::Array& array);

    static std::vector<Literal> GetLiteralFromDateArray(const arrow::Array& array);

    static std::vector<Literal> GetLiteralFromTimestampArray(const arrow::Array& array);
};
}  // namespace paimon
