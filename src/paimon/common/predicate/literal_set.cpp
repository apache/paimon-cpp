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

#include "paimon/common/predicate/literal_set.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <utility>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_primitive.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/field_type_utils.h"

namespace paimon {
namespace {
// A dense bitmap is only worth it when the value range stays close to the literal count and small
// enough to keep the bitmap in cache friendly territory.
constexpr uint64_t DENSE_SPAN_FACTOR = 4;
constexpr uint64_t MAX_DENSE_SPAN = 1ULL << 20;

// Maps an arrow array to the `FieldType` that `LiteralConverter::ConvertLiteralsFromArray` would
// produce for it. Types outside `LiteralSet`'s support are intentionally left unmapped:
// `field_type_` can never be one of them, so `MatchesArrowType` returns false and the caller falls
// back.
std::optional<FieldType> MapArrowType(const arrow::Array& array) {
    switch (array.type_id()) {
        case arrow::Type::type::BOOL:
            return FieldType::BOOLEAN;
        case arrow::Type::type::INT8:
            return FieldType::TINYINT;
        case arrow::Type::type::INT16:
            return FieldType::SMALLINT;
        case arrow::Type::type::INT32:
            return FieldType::INT;
        case arrow::Type::type::INT64:
            return FieldType::BIGINT;
        case arrow::Type::type::STRING:
            return FieldType::STRING;
        case arrow::Type::type::BINARY:
            return FieldType::BINARY;
        case arrow::Type::type::DATE32:
            return FieldType::DATE;
        case arrow::Type::type::DICTIONARY: {
            const auto& dict_array = checked_cast<const arrow::DictionaryArray&>(array);
            const auto* dict_type =
                checked_cast<const arrow::DictionaryType*>(dict_array.type().get());
            auto value_type_id = dict_type->value_type()->id();
            auto index_type_id = dict_type->index_type()->id();
            if ((value_type_id == arrow::Type::type::STRING &&
                 index_type_id == arrow::Type::type::INT32) ||
                (value_type_id == arrow::Type::type::LARGE_STRING &&
                 index_type_id == arrow::Type::type::INT64)) {
                return FieldType::STRING;
            }
            return std::nullopt;
        }
        default:
            return std::nullopt;
    }
}
}  // namespace

std::shared_ptr<const LiteralSet> LiteralSet::CreateOrNull(FieldType field_type,
                                                           const std::vector<Literal>& literals) {
    if (literals.empty()) {
        return nullptr;
    }
    // A literal typed differently makes `Literal::CompareTo` fail, keep that on the fallback path.
    for (const auto& literal : literals) {
        if (!literal.IsNull() && literal.GetType() != field_type) {
            return nullptr;
        }
    }

    std::shared_ptr<LiteralSet> literal_set(new LiteralSet(field_type));
    bool built = false;
    switch (field_type) {
        case FieldType::BOOLEAN:
            built = literal_set->BuildBooleans(literals);
            break;
        case FieldType::TINYINT:
        case FieldType::SMALLINT:
        case FieldType::INT:
        case FieldType::BIGINT:
        case FieldType::DATE:
            built = literal_set->BuildIntegers(literals);
            break;
        case FieldType::STRING:
        case FieldType::BINARY:
            built = literal_set->BuildBinaries(literals);
            break;
        default:
            // FLOAT / DOUBLE carry NaN semantics, TIMESTAMP needs unit conversion and DECIMAL
            // compares across scales, all of them keep the generic comparison path.
            built = false;
            break;
    }
    if (!built) {
        return nullptr;
    }
    return literal_set;
}

bool LiteralSet::BuildBooleans(const std::vector<Literal>& literals) {
    for (const auto& literal : literals) {
        if (literal.IsNull()) {
            has_null_literal_ = true;
        } else if (literal.GetValue<bool>()) {
            has_true_ = true;
        } else {
            has_false_ = true;
        }
    }
    return true;
}

bool LiteralSet::BuildIntegers(const std::vector<Literal>& literals) {
    std::vector<int64_t> values;
    values.reserve(literals.size());
    for (const auto& literal : literals) {
        if (literal.IsNull()) {
            has_null_literal_ = true;
            continue;
        }
        switch (field_type_) {
            case FieldType::TINYINT:
                values.push_back(literal.GetValue<int8_t>());
                break;
            case FieldType::SMALLINT:
                values.push_back(literal.GetValue<int16_t>());
                break;
            case FieldType::INT:
            case FieldType::DATE:
                values.push_back(literal.GetValue<int32_t>());
                break;
            default:
                values.push_back(literal.GetValue<int64_t>());
                break;
        }
    }
    if (values.empty()) {
        // Only null literals: `IN` never matches and `NOT IN` is always false, both handled without
        // any lookup structure.
        min_ = std::numeric_limits<int64_t>::max();
        max_ = std::numeric_limits<int64_t>::min();
        return true;
    }

    const auto [min_it, max_it] = std::minmax_element(values.begin(), values.end());
    min_ = *min_it;
    max_ = *max_it;
    const uint64_t span = static_cast<uint64_t>(max_) - static_cast<uint64_t>(min_) + 1;
    if (span <= MAX_DENSE_SPAN && span <= DENSE_SPAN_FACTOR * values.size()) {
        dense_.assign(static_cast<size_t>(span), false);
        for (int64_t value : values) {
            dense_[static_cast<size_t>(value - min_)] = true;
        }
    } else {
        sparse_.reserve(values.size());
        for (int64_t value : values) {
            sparse_.insert(value);
        }
    }
    return true;
}

bool LiteralSet::BuildBinaries(const std::vector<Literal>& literals) {
    min_length_ = std::numeric_limits<size_t>::max();
    max_length_ = 0;
    for (const auto& literal : literals) {
        if (literal.IsNull()) {
            has_null_literal_ = true;
            continue;
        }
        binary_storage_.push_back(literal.GetValue<std::string>());
        const std::string& value = binary_storage_.back();
        min_length_ = std::min(min_length_, value.size());
        max_length_ = std::max(max_length_, value.size());
        if (value.empty()) {
            has_empty_binary_ = true;
        } else {
            const auto first_byte = static_cast<uint8_t>(value[0]);
            first_byte_bitmap_[first_byte >> 6] |= 1ULL << (first_byte & 63);
        }
        binary_set_.insert(std::string_view(value));
    }
    if (binary_storage_.empty()) {
        // Only null literals, see `BuildIntegers`. Keeps `ContainsBinary` rejecting everything.
        min_length_ = 1;
        max_length_ = 0;
    }
    return true;
}

bool LiteralSet::MatchesArrowType(const arrow::Array& array) const {
    std::optional<FieldType> mapped = MapArrowType(array);
    return mapped.has_value() && mapped.value() == field_type_;
}

void LiteralSet::TestBooleanArray(const arrow::Array& array, bool negate,
                                  std::vector<char>* out) const {
    const auto& typed = checked_cast<const arrow::BooleanArray&>(array);
    for (int64_t i = 0; i < typed.length(); i++) {
        if (typed.IsNull(i)) {
            continue;
        }
        const bool contains = typed.Value(i) ? has_true_ : has_false_;
        (*out)[i] = static_cast<char>(contains != negate);
    }
}

template <typename ArrayType>
void LiteralSet::TestIntegerArray(const arrow::Array& array, bool negate,
                                  std::vector<char>* out) const {
    const auto& typed = checked_cast<const ArrayType&>(array);
    const auto* values = typed.raw_values();
    const int64_t length = typed.length();
    if (typed.null_count() == 0) {
        for (int64_t i = 0; i < length; i++) {
            (*out)[i] =
                static_cast<char>(ContainsInteger(static_cast<int64_t>(values[i])) != negate);
        }
        return;
    }
    for (int64_t i = 0; i < length; i++) {
        if (typed.IsNull(i)) {
            continue;
        }
        (*out)[i] = static_cast<char>(ContainsInteger(static_cast<int64_t>(values[i])) != negate);
    }
}

template <typename ArrayType>
void LiteralSet::TestBinaryArray(const arrow::Array& array, bool negate,
                                 std::vector<char>* out) const {
    const auto& typed = checked_cast<const ArrayType&>(array);
    const int64_t length = typed.length();
    for (int64_t i = 0; i < length; i++) {
        if (typed.IsNull(i)) {
            continue;
        }
        (*out)[i] = static_cast<char>(ContainsBinary(typed.GetView(i)) != negate);
    }
}

template <typename DictArrayType, typename IndicesArrayType>
void LiteralSet::TestDictionaryArray(const arrow::DictionaryArray& dict_array, bool negate,
                                     std::vector<char>* out) const {
    const auto& dictionary = checked_cast<const DictArrayType&>(*dict_array.dictionary());
    const auto& indices = checked_cast<const IndicesArrayType&>(*dict_array.indices());
    const int64_t dict_length = dictionary.length();
    // Probe the dictionary once and then only follow indices, O(dict_size + rows).
    std::vector<char> dict_hits(dict_length, 0);
    for (int64_t i = 0; i < dict_length; i++) {
        if (!dictionary.IsNull(i)) {
            dict_hits[i] = static_cast<char>(ContainsBinary(dictionary.GetView(i)));
        }
    }
    for (int64_t i = 0; i < dict_array.length(); i++) {
        if (dict_array.IsNull(i)) {
            continue;
        }
        const int64_t dict_index = indices.Value(i);
        const bool contains =
            dict_index >= 0 && dict_index < dict_length && dict_hits[dict_index] != 0;
        (*out)[i] = static_cast<char>(contains != negate);
    }
}

Status LiteralSet::TestArray(const arrow::Array& array, bool negate, std::vector<char>* out) const {
    if (out == nullptr || static_cast<int64_t>(out->size()) != array.length()) {
        return Status::Invalid("output buffer size must match the array length");
    }
    if (!MatchesArrowType(array)) {
        return Status::Invalid(fmt::format("literal set of type {} cannot probe arrow {} type",
                                           FieldTypeUtils::FieldTypeToString(field_type_),
                                           array.type()->ToString()));
    }
    if (negate && has_null_literal_) {
        // `NotIn::InnerTest` returns false as soon as it meets a null literal, so no row matches.
        return Status::OK();
    }
    switch (array.type_id()) {
        case arrow::Type::type::BOOL:
            TestBooleanArray(array, negate, out);
            break;
        case arrow::Type::type::INT8:
            TestIntegerArray<arrow::Int8Array>(array, negate, out);
            break;
        case arrow::Type::type::INT16:
            TestIntegerArray<arrow::Int16Array>(array, negate, out);
            break;
        case arrow::Type::type::INT32:
            TestIntegerArray<arrow::Int32Array>(array, negate, out);
            break;
        case arrow::Type::type::INT64:
            TestIntegerArray<arrow::Int64Array>(array, negate, out);
            break;
        case arrow::Type::type::DATE32:
            TestIntegerArray<arrow::Date32Array>(array, negate, out);
            break;
        case arrow::Type::type::STRING:
            TestBinaryArray<arrow::StringArray>(array, negate, out);
            break;
        case arrow::Type::type::BINARY:
            TestBinaryArray<arrow::BinaryArray>(array, negate, out);
            break;
        case arrow::Type::type::DICTIONARY: {
            const auto& dict_array = checked_cast<const arrow::DictionaryArray&>(array);
            const auto* dict_type =
                checked_cast<const arrow::DictionaryType*>(dict_array.type().get());
            if (dict_type->value_type()->id() == arrow::Type::type::STRING) {
                TestDictionaryArray<arrow::StringArray, arrow::Int32Array>(dict_array, negate, out);
            } else {
                TestDictionaryArray<arrow::LargeStringArray, arrow::Int64Array>(dict_array, negate,
                                                                                out);
            }
            break;
        }
        default:
            return Status::Invalid(
                fmt::format("Not support literal set on arrow {} type", array.type()->ToString()));
    }
    return Status::OK();
}

Result<bool> LiteralSet::TestValue(const Literal& value, bool negate) const {
    if (value.IsNull()) {
        return false;
    }
    if (value.GetType() != field_type_) {
        return Status::Invalid(
            fmt::format("cannot probe literal set of type {} with value {} of type {}",
                        FieldTypeUtils::FieldTypeToString(field_type_), value.ToString(),
                        FieldTypeUtils::FieldTypeToString(value.GetType())));
    }
    if (negate && has_null_literal_) {
        return false;
    }
    bool contains = false;
    switch (field_type_) {
        case FieldType::BOOLEAN:
            contains = value.GetValue<bool>() ? has_true_ : has_false_;
            break;
        case FieldType::TINYINT:
            contains = ContainsInteger(value.GetValue<int8_t>());
            break;
        case FieldType::SMALLINT:
            contains = ContainsInteger(value.GetValue<int16_t>());
            break;
        case FieldType::INT:
        case FieldType::DATE:
            contains = ContainsInteger(value.GetValue<int32_t>());
            break;
        case FieldType::BIGINT:
            contains = ContainsInteger(value.GetValue<int64_t>());
            break;
        default:
            contains = ContainsBinary(value.GetValue<std::string>());
            break;
    }
    return contains != negate;
}
}  // namespace paimon
