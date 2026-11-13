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

#include "paimon/core/catalog/table_query_auth_result.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>

#include "arrow/api.h"
#include "arrow/util/decimal.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_type_json_parser.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/predicate_utils.h"
#include "rapidjson/document.h"

namespace paimon {
namespace {

using ReferencedTypes = std::map<std::string, std::shared_ptr<arrow::DataType>>;

/// Fails when two rules read one column as different types.
Status RecordReferencedType(const std::string& name, const std::shared_ptr<arrow::DataType>& type,
                            ReferencedTypes* types) {
    auto [iter, inserted] = types->emplace(name, type);
    if (!inserted && !iter->second->Equals(*type)) {
        return Status::Invalid(fmt::format("Query auth rules read column '{}' as both {} and {}.",
                                           name, iter->second->ToString(), type->ToString()));
    }
    return Status::OK();
}

Result<std::string> GetString(const rapidjson::Value& object, const char* key,
                              const std::string& what) {
    auto iter = object.FindMember(key);
    if (iter == object.MemberEnd() || !iter->value.IsString()) {
        return Status::Invalid(fmt::format("{} needs a string member '{}'.", what, key));
    }
    return std::string(iter->value.GetString(), iter->value.GetStringLength());
}

/// A Java `FieldRef`: `{"index": 0, "name": "f0", "type": "INT"}`.
struct FieldRef {
    int32_t index = 0;
    std::string name;
    std::shared_ptr<arrow::DataType> type;
    /// `UNKNOWN` for a type a predicate cannot compare, which a mask still copies.
    FieldType field_type = FieldType::UNKNOWN;
};

Result<FieldRef> ParseFieldRef(const rapidjson::Value& value, ReferencedTypes* types) {
    if (!value.IsObject()) {
        return Status::Invalid("A field reference of a query auth rule must be a JSON object.");
    }
    FieldRef ref;
    PAIMON_ASSIGN_OR_RAISE(ref.name, GetString(value, "name", "A field reference"));
    if (ref.name.empty()) {
        return Status::Invalid("A field reference of a query auth rule cannot have an empty name.");
    }
    auto index_iter = value.FindMember("index");
    if (index_iter != value.MemberEnd() && index_iter->value.IsInt()) {
        ref.index = index_iter->value.GetInt();
    }
    auto type_iter = value.FindMember("type");
    if (type_iter == value.MemberEnd() || type_iter->value.IsNull()) {
        return Status::Invalid(
            fmt::format("The field reference to '{}' of a query auth rule has no type.", ref.name));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> field,
                           DataTypeJsonParser::ParseType(ref.name, type_iter->value));
    ref.type = field->type();
    Result<FieldType> field_type = FieldTypeUtils::ConvertToFieldType(field);
    if (field_type.ok()) {
        ref.field_type = field_type.value();
    }
    PAIMON_RETURN_NOT_OK(RecordReferencedType(ref.name, ref.type, types));
    return ref;
}

bool IsDigit(char c) {
    return c >= '0' && c <= '9';
}

/// Reads exactly `count` digits of `text` at `*pos`.
bool ReadDigits(std::string_view text, size_t* pos, size_t count, int64_t* value) {
    if (text.size() - *pos < count) {
        return false;
    }
    int64_t result = 0;
    for (size_t i = 0; i < count; ++i) {
        if (!IsDigit(text[*pos + i])) {
            return false;
        }
        result = result * 10 + (text[*pos + i] - '0');
    }
    *pos += count;
    *value = result;
    return true;
}

bool ReadChar(std::string_view text, size_t* pos, char expected) {
    if (*pos >= text.size() || text[*pos] != expected) {
        return false;
    }
    ++*pos;
    return true;
}

/// Days from 1970-01-01 to a date of the proleptic Gregorian calendar, as Java
/// `LocalDate.toEpochDay()`.
int64_t EpochDay(int64_t year, int64_t month, int64_t day) {
    const int64_t y = month <= 2 ? year - 1 : year;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const int64_t year_of_era = y - era * 400;
    const int64_t day_of_year = (153 * (month > 2 ? month - 3 : month + 9) + 2) / 5 + day - 1;
    const int64_t day_of_era =
        year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    return era * 146097 + day_of_era - 719468;
}

/// Reads a date as Java `DateTimeFormatter.ISO_LOCAL_DATE` parses it: a year of four digits, of
/// more only after a `+`, or negative after a `-`, then the `-MM-dd` of a real date.
bool ReadIsoDate(std::string_view text, size_t* pos, int64_t* epoch_day) {
    const bool positive = *pos < text.size() && text[*pos] == '+';
    const bool negative = *pos < text.size() && text[*pos] == '-';
    if (positive || negative) {
        ++*pos;
    }
    const size_t start = *pos;
    int64_t year = 0;
    while (*pos < text.size() && *pos - start < 10 && IsDigit(text[*pos])) {
        year = year * 10 + (text[(*pos)++] - '0');
    }
    const size_t digits = *pos - start;
    if (digits < 4 || (positive && digits == 4) || (!positive && !negative && digits > 4) ||
        (negative && year == 0) || year > 999999999) {
        return false;
    }
    if (negative) {
        year = -year;
    }
    int64_t month = 0;
    int64_t day = 0;
    if (!ReadChar(text, pos, '-') || !ReadDigits(text, pos, 2, &month) ||
        !ReadChar(text, pos, '-') || !ReadDigits(text, pos, 2, &day)) {
        return false;
    }
    static constexpr int64_t kDaysInMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    if (month < 1 || month > 12 || day < 1 ||
        day > kDaysInMonth[month - 1] + (month == 2 && leap ? 1 : 0)) {
        return false;
    }
    *epoch_day = EpochDay(year, month, day);
    return true;
}

/// Reads a time as Java `DateTimeFormatter.ISO_LOCAL_TIME` parses it: `HH:mm`, optionally followed
/// by `:ss` and then by a fraction of one to nine digits. `seconds_required` makes only `:ss`
/// mandatory; the fraction stays optional.
bool ReadIsoTime(std::string_view text, size_t* pos, bool seconds_required, int64_t* second_of_day,
                 int64_t* nanos) {
    int64_t hour = 0;
    int64_t minute = 0;
    int64_t second = 0;
    *nanos = 0;
    if (!ReadDigits(text, pos, 2, &hour) || !ReadChar(text, pos, ':') ||
        !ReadDigits(text, pos, 2, &minute) || hour > 23 || minute > 59) {
        return false;
    }
    if (ReadChar(text, pos, ':')) {
        if (!ReadDigits(text, pos, 2, &second) || second > 59) {
            return false;
        }
        if (ReadChar(text, pos, '.')) {
            size_t digits = 0;
            // Accumulate at most ten digits, so an overlong fraction cannot overflow.
            while (*pos < text.size() && IsDigit(text[*pos]) && digits < 10) {
                *nanos = *nanos * 10 + (text[(*pos)++] - '0');
                ++digits;
            }
            if (digits == 0 || digits > 9) {
                return false;
            }
            for (; digits < 9; ++digits) {
                *nanos *= 10;
            }
        }
    } else if (seconds_required) {
        return false;
    }
    *second_of_day = hour * 3600 + minute * 60 + second;
    return true;
}

/// Parses the text Java `LocalDate.parse()` reads.
Result<int32_t> ParseDateLiteral(std::string_view text, const FieldRef& ref) {
    size_t pos = 0;
    int64_t epoch_day = 0;
    if (!ReadIsoDate(text, &pos, &epoch_day) || pos != text.size() ||
        epoch_day < std::numeric_limits<int32_t>::min() ||
        epoch_day > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid(
            fmt::format("Cannot parse '{}' as a date literal of column '{}'.", text, ref.name));
    }
    return static_cast<int32_t>(epoch_day);
}

Result<int32_t> ParseTimeLiteral(std::string_view text, const FieldRef& ref) {
    size_t pos = 0;
    int64_t second_of_day = 0;
    int64_t nanos = 0;
    if (!ReadIsoTime(text, &pos, /*seconds_required=*/false, &second_of_day, &nanos) ||
        pos != text.size()) {
        return Status::Invalid(
            fmt::format("Cannot parse '{}' as a time literal of column '{}'.", text, ref.name));
    }
    return static_cast<int32_t>(second_of_day * 1000 + nanos / 1000000);
}

/// Parses the ISO text Java `LocalDateTime.toString()` writes for a TIMESTAMP column, or the
/// text `Instant.toString()` writes with a `Z` for a TIMESTAMP_LTZ one. Expanded years are
/// supported; the instant must lie within the range of epoch milliseconds.
Result<Literal> ParseTimestampLiteral(std::string_view text, const FieldRef& ref) {
    const bool instant = !checked_cast<const arrow::TimestampType&>(*ref.type).timezone().empty();
    size_t pos = 0;
    int64_t epoch_day = 0;
    int64_t second_of_day = 0;
    int64_t nanos = 0;
    if (!ReadIsoDate(text, &pos, &epoch_day) || !ReadChar(text, &pos, 'T') ||
        !ReadIsoTime(text, &pos, /*seconds_required=*/instant, &second_of_day, &nanos) ||
        (instant && !ReadChar(text, &pos, 'Z')) || pos != text.size()) {
        return Status::Invalid(fmt::format(
            "Cannot parse '{}' as a timestamp literal of column '{}'.", text, ref.name));
    }
    const int64_t seconds = epoch_day * 86400 + second_of_day;
    // Check the complete value: the fractional milliseconds can bring the first second into
    // range, and only part of the last second fits. Widen before multiplying to avoid overflow.
    const Decimal::int128_t millis =
        static_cast<Decimal::int128_t>(seconds) * 1000 + nanos / 1000000;
    if (millis > std::numeric_limits<int64_t>::max() ||
        millis < std::numeric_limits<int64_t>::min()) {
        return Status::Invalid(fmt::format(
            "Timestamp literal '{}' of column '{}' is outside the range of epoch milliseconds.",
            text, ref.name));
    }
    return Literal(Timestamp::FromEpochMillis(static_cast<int64_t>(millis),
                                              static_cast<int32_t>(nanos % 1000000)));
}

Result<Literal> ParseDecimalLiteral(const std::string& text, const FieldRef& ref) {
    const auto& decimal_type = checked_cast<const arrow::DecimalType&>(*ref.type);
    arrow::Decimal128 value;
    int32_t precision = 0;
    int32_t scale = 0;
    if (!arrow::Decimal128::FromString(text, &value, &precision, &scale).ok()) {
        return Status::Invalid(
            fmt::format("Cannot parse '{}' as a decimal literal of column '{}'.", text, ref.name));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(arrow::Decimal128 rescaled,
                                      value.Rescale(scale, decimal_type.scale()));
    if (!rescaled.FitsInPrecision(decimal_type.precision())) {
        return Status::Invalid(
            fmt::format("Decimal literal '{}' does not fit column '{}' of type {}.", text, ref.name,
                        ref.type->ToString()));
    }
    return Literal(Decimal(
        decimal_type.precision(), decimal_type.scale(),
        static_cast<Decimal::int128_t>(
            static_cast<Decimal::uint128_t>(static_cast<uint64_t>(rescaled.high_bits())) << 64 |
            rescaled.low_bits())));
}

/// Parses one literal as Java `LeafPredicate` writes it for a column of the type of `ref`.
Result<Literal> ParseLiteral(const rapidjson::Value& value, const rapidjson::Value& number_text,
                             const FieldRef& ref) {
    if (value.IsNull()) {
        return Literal(ref.field_type);
    }
    switch (ref.field_type) {
        case FieldType::BOOLEAN:
            if (value.IsBool()) {
                return Literal(value.GetBool());
            }
            break;
        case FieldType::TINYINT:
            if (value.IsInt() && value.GetInt() >= std::numeric_limits<int8_t>::min() &&
                value.GetInt() <= std::numeric_limits<int8_t>::max()) {
                return Literal(static_cast<int8_t>(value.GetInt()));
            }
            break;
        case FieldType::SMALLINT:
            if (value.IsInt() && value.GetInt() >= std::numeric_limits<int16_t>::min() &&
                value.GetInt() <= std::numeric_limits<int16_t>::max()) {
                return Literal(static_cast<int16_t>(value.GetInt()));
            }
            break;
        case FieldType::INT:
            if (value.IsInt()) {
                return Literal(static_cast<int32_t>(value.GetInt()));
            }
            break;
        case FieldType::BIGINT:
            if (value.IsInt64()) {
                return Literal(static_cast<int64_t>(value.GetInt64()));
            }
            break;
        case FieldType::FLOAT:
            // Preserve integer-to-float rounding as Java Number.floatValue() does; converting
            // through double can lose the bit that decides which adjacent float to choose.
            if (value.IsInt64()) {
                return Literal(static_cast<float>(value.GetInt64()));
            }
            if (value.IsUint64()) {
                return Literal(static_cast<float>(value.GetUint64()));
            }
            if (value.IsNumber()) {
                // Java reads integers outside the long range as BigInteger. Preserve their
                // spelling: rounding through double first can select the other adjacent float.
                const std::string text(number_text.GetString(), number_text.GetStringLength());
                if (text.find_first_of(".eE") == std::string::npos) {
                    const std::optional<float> number = StringUtils::StringToValue<float>(text);
                    if (number && std::isfinite(number.value())) {
                        return Literal(number.value());
                    }
                    break;
                }
                const double number = value.GetDouble();
                const double maximum = std::numeric_limits<float>::max();
                const double overflow_threshold =
                    maximum + std::ldexp(1.0, std::numeric_limits<float>::max_exponent -
                                                  std::numeric_limits<float>::digits - 1);
                // Java writes Float.MAX_VALUE as 3.4028235E38, slightly above its exact value.
                // Clamp values rounding back to that bound before the C++ narrowing conversion.
                if (number > -overflow_threshold && number < overflow_threshold) {
                    return Literal(static_cast<float>(std::clamp(number, -maximum, maximum)));
                }
            }
            break;
        case FieldType::DOUBLE:
            if (value.IsNumber()) {
                return Literal(value.GetDouble());
            }
            break;
        case FieldType::STRING:
            if (value.IsString()) {
                return Literal(FieldType::STRING, value.GetString(), value.GetStringLength());
            }
            break;
        case FieldType::DATE:
            // Not a number: Java `PredicateBuilder.convertJavaObject()` refuses one for a DATE.
            if (value.IsString()) {
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t days,
                    ParseDateLiteral(std::string_view(value.GetString(), value.GetStringLength()),
                                     ref));
                return Literal(FieldType::DATE, days);
            }
            break;
        case FieldType::TIME:
            if (value.IsString()) {
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t millis,
                    ParseTimeLiteral(std::string_view(value.GetString(), value.GetStringLength()),
                                     ref));
                return Literal(FieldType::TIME, millis);
            }
            break;
        case FieldType::TIMESTAMP:
            if (value.IsString()) {
                return ParseTimestampLiteral(
                    std::string_view(value.GetString(), value.GetStringLength()), ref);
            }
            break;
        case FieldType::DECIMAL:
            if (value.IsString()) {
                return ParseDecimalLiteral(std::string(value.GetString(), value.GetStringLength()),
                                           ref);
            }
            break;
        default:
            return Status::NotImplemented(
                fmt::format("Query auth row filters on column '{}' of type {} are not supported.",
                            ref.name, ref.type->ToString()));
    }
    return Status::Invalid(
        fmt::format("A literal of a query auth row filter does not fit column '{}' of type {}.",
                    ref.name, ref.type->ToString()));
}

/// A predicate no row satisfies, kept on a field so it binds like any other leaf.
Result<std::shared_ptr<Predicate>> AlwaysFalse(const FieldRef& ref) {
    return PredicateBuilder::And(
        {PredicateBuilder::IsNull(ref.index, ref.name, ref.field_type),
         PredicateBuilder::IsNotNull(ref.index, ref.name, ref.field_type)});
}

/// Maps a Java `LeafFunction` name to a predicate; a null literal never matches, as in Java.
Result<std::shared_ptr<Predicate>> MakeLeafPredicate(const std::string& function,
                                                     const FieldRef& ref,
                                                     const std::vector<Literal>& literals) {
    auto expect_literals = [&](size_t count) -> Status {
        if (literals.size() != count) {
            return Status::Invalid(
                fmt::format("Query auth row filter {} on column '{}' needs {} literals but has {}.",
                            function, ref.name, count, literals.size()));
        }
        return Status::OK();
    };
    auto any_null = [&]() {
        return std::any_of(literals.begin(), literals.end(),
                           [](const Literal& literal) { return literal.IsNull(); });
    };
    const int32_t index = ref.index;
    const std::string& name = ref.name;
    const FieldType type = ref.field_type;
    if (function == "IS_NULL") {
        return PredicateBuilder::IsNull(index, name, type);
    }
    if (function == "IS_NOT_NULL") {
        return PredicateBuilder::IsNotNull(index, name, type);
    }
    if (function == "IN") {
        // `x IN (1, NULL)` holds exactly when `x IN (1)` does.
        std::vector<Literal> non_null;
        for (const Literal& literal : literals) {
            if (!literal.IsNull()) {
                non_null.push_back(literal);
            }
        }
        if (non_null.empty()) {
            return AlwaysFalse(ref);
        }
        return PredicateBuilder::In(index, name, type, non_null);
    }
    if (function == "NOT_IN") {
        // `x NOT IN (1, NULL)` never holds.
        if (any_null()) {
            return AlwaysFalse(ref);
        }
        if (literals.empty()) {
            return PredicateBuilder::IsNotNull(index, name, type);
        }
        return PredicateBuilder::NotIn(index, name, type, literals);
    }
    if (function == "BETWEEN" || function == "NOT_BETWEEN") {
        PAIMON_RETURN_NOT_OK(expect_literals(2));
        if (any_null()) {
            return AlwaysFalse(ref);
        }
        if (function == "BETWEEN") {
            return PredicateBuilder::Between(index, name, type, literals[0], literals[1]);
        }
        return PredicateBuilder::Or(
            {PredicateBuilder::LessThan(index, name, type, literals[0]),
             PredicateBuilder::GreaterThan(index, name, type, literals[1])});
    }
    static const std::set<std::string> kBinaryFunctions = {"EQUAL",        "NOT_EQUAL",
                                                           "LESS_THAN",    "LESS_OR_EQUAL",
                                                           "GREATER_THAN", "GREATER_OR_EQUAL",
                                                           "STARTS_WITH",  "ENDS_WITH",
                                                           "CONTAINS",     "LIKE"};
    if (kBinaryFunctions.count(function) == 0) {
        return Status::NotImplemented(
            fmt::format("Query auth row filter function '{}' is not supported.", function));
    }
    PAIMON_RETURN_NOT_OK(expect_literals(1));
    if (literals[0].IsNull()) {
        return AlwaysFalse(ref);
    }
    const Literal& literal = literals[0];
    if (function == "EQUAL") {
        return PredicateBuilder::Equal(index, name, type, literal);
    }
    if (function == "NOT_EQUAL") {
        return PredicateBuilder::NotEqual(index, name, type, literal);
    }
    if (function == "LESS_THAN") {
        return PredicateBuilder::LessThan(index, name, type, literal);
    }
    if (function == "LESS_OR_EQUAL") {
        return PredicateBuilder::LessOrEqual(index, name, type, literal);
    }
    if (function == "GREATER_THAN") {
        return PredicateBuilder::GreaterThan(index, name, type, literal);
    }
    if (function == "GREATER_OR_EQUAL") {
        return PredicateBuilder::GreaterOrEqual(index, name, type, literal);
    }
    if (function == "STARTS_WITH") {
        return PredicateBuilder::StartsWith(index, name, type, literal);
    }
    if (function == "ENDS_WITH") {
        return PredicateBuilder::EndsWith(index, name, type, literal);
    }
    if (function == "CONTAINS") {
        return PredicateBuilder::Contains(index, name, type, literal);
    }
    return PredicateBuilder::Like(index, name, type, literal);
}

/// A parsed row filter: `predicate`, or, when it reads no column, the constant `value`.
struct ParsedFilter {
    std::shared_ptr<Predicate> predicate;
    bool value = true;
};

/// Combines `children` with AND, or with OR when `is_and` is false, folding away their constants
/// as Java `PredicateBuilder.and()` and `or()` do.
Result<ParsedFilter> Combine(const std::vector<ParsedFilter>& children, bool is_and) {
    std::vector<std::shared_ptr<Predicate>> predicates;
    for (const ParsedFilter& child : children) {
        if (child.predicate != nullptr) {
            predicates.push_back(child.predicate);
        } else if (child.value != is_and) {
            // FALSE decides an AND, TRUE an OR.
            return ParsedFilter{nullptr, child.value};
        }
    }
    if (predicates.empty()) {
        return ParsedFilter{nullptr, is_and};
    }
    if (predicates.size() == 1) {
        return ParsedFilter{predicates[0], true};
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<Predicate> combined,
        is_and ? PredicateBuilder::And(predicates) : PredicateBuilder::Or(predicates));
    return ParsedFilter{std::move(combined), true};
}

/// Parses a Java `Predicate`: `{"kind": "LEAF", "transform": ..., "function": ..., "literals":
/// [...]}` or `{"kind": "COMPOUND", "function": "AND", "children": [...]}`.
Result<ParsedFilter> ParsePredicate(const rapidjson::Value& value,
                                    const rapidjson::Value& number_texts, ReferencedTypes* types) {
    if (!value.IsObject()) {
        return Status::Invalid("A query auth row filter must be a JSON object.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string kind, GetString(value, "kind", "A row filter"));
    PAIMON_ASSIGN_OR_RAISE(std::string function, GetString(value, "function", "A row filter"));
    if (kind == "COMPOUND") {
        if (function != "AND" && function != "OR") {
            return Status::NotImplemented(
                fmt::format("Query auth row filter function '{}' is not supported.", function));
        }
        auto children_iter = value.FindMember("children");
        if (children_iter == value.MemberEnd() || !children_iter->value.IsArray() ||
            children_iter->value.Empty()) {
            return Status::Invalid("A compound row filter needs a non-empty 'children' array.");
        }
        // Every child is parsed before the constants fold, so one this library cannot apply fails
        // and the columns each names stay recorded for the schema checks.
        std::vector<ParsedFilter> children;
        for (rapidjson::SizeType i = 0; i < children_iter->value.Size(); ++i) {
            PAIMON_ASSIGN_OR_RAISE(
                ParsedFilter parsed,
                ParsePredicate(children_iter->value[i], number_texts["children"][i], types));
            children.push_back(std::move(parsed));
        }
        return Combine(children, /*is_and=*/function == "AND");
    }
    if (kind != "LEAF") {
        return Status::NotImplemented(
            fmt::format("Query auth row filter kind '{}' is not supported.", kind));
    }
    auto transform_iter = value.FindMember("transform");
    if (transform_iter == value.MemberEnd() || !transform_iter->value.IsObject()) {
        return Status::Invalid("A leaf row filter needs a 'transform' object.");
    }
    const rapidjson::Value& transform = transform_iter->value;
    PAIMON_ASSIGN_OR_RAISE(std::string transform_name,
                           GetString(transform, "name", "A row filter transform"));
    // Checked before the constants, which ignore the literals: Java reads them as a list.
    auto literals_iter = value.FindMember("literals");
    const bool has_literals = literals_iter != value.MemberEnd() && !literals_iter->value.IsNull();
    if (has_literals && !literals_iter->value.IsArray()) {
        return Status::Invalid("The 'literals' of a row filter must be a JSON array.");
    }
    // A missing NOT_IN list must not become an empty set accepting every non-null value.
    if (!has_literals && (function == "IN" || function == "NOT_IN")) {
        return Status::Invalid(fmt::format(
            "Query auth row filter {} requires a non-null 'literals' array.", function));
    }
    const bool constant = function == "TRUE" || function == "FALSE";
    // Java `PredicateBuilder.alwaysTrue()` and `alwaysFalse()` put their constant on a NULL
    // transform; the constant ignores whatever its transform computes.
    if (constant && transform_name == "NULL") {
        return ParsedFilter{nullptr, function == "TRUE"};
    }
    if (transform_name != "FIELD_REF") {
        return Status::NotImplemented(fmt::format(
            "Query auth row filters on a '{}' transform are not supported.", transform_name));
    }
    auto field_ref_iter = transform.FindMember("fieldRef");
    if (field_ref_iter == transform.MemberEnd()) {
        return Status::Invalid("A FIELD_REF transform needs a 'fieldRef' member.");
    }
    PAIMON_ASSIGN_OR_RAISE(FieldRef ref, ParseFieldRef(field_ref_iter->value, types));
    if (constant) {
        return ParsedFilter{nullptr, function == "TRUE"};
    }
    if (ref.field_type == FieldType::UNKNOWN) {
        return Status::NotImplemented(
            fmt::format("Query auth row filter {} on column '{}' of type {} is not supported.",
                        function, ref.name, ref.type->ToString()));
    }
    std::vector<Literal> literals;
    if (has_literals) {
        for (rapidjson::SizeType i = 0; i < literals_iter->value.Size(); ++i) {
            PAIMON_ASSIGN_OR_RAISE(Literal parsed, ParseLiteral(literals_iter->value[i],
                                                                number_texts["literals"][i], ref));
            literals.push_back(std::move(parsed));
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> predicate,
                           MakeLeafPredicate(function, ref, literals));
    return ParsedFilter{std::move(predicate), true};
}

Result<rapidjson::Document> ParseJson(const std::string& json, const std::string& what) {
    // An unescaped NUL terminates RapidJSON's input before it validates the remaining bytes.
    if (json.find('\0') != std::string::npos) {
        return Status::Invalid(fmt::format("{} contains an unescaped NUL byte.", what));
    }
    rapidjson::Document document;
    // Approximate double parsing can shift a comparison boundary by a few ULPs and authorize
    // different rows than Java. Preserve the correctly rounded value of each numeric literal.
    document.Parse<rapidjson::kParseValidateEncodingFlag | rapidjson::kParseFullPrecisionFlag>(
        json.c_str(), json.size());
    if (document.HasParseError()) {
        return Status::Invalid(
            fmt::format("{} is not valid JSON at offset {}.", what, document.GetErrorOffset()));
    }
    Status unique = RapidJsonUtil::CheckNoDuplicateMembers(document);
    if (!unique.ok()) {
        return Status::Invalid(fmt::format("{} is ambiguous: {}.", what, unique.message()));
    }
    return document;
}

Result<std::shared_ptr<arrow::Array>> FindColumn(const arrow::StructArray& batch,
                                                 const std::string& name) {
    std::shared_ptr<arrow::Array> column = batch.GetFieldByName(name);
    if (column == nullptr) {
        return Status::Invalid(
            fmt::format("Column '{}', which a column mask reads, is not in the read batch.", name));
    }
    return column;
}

Status CheckStringTarget(const std::string& transform, const DataField& target) {
    if (target.Type()->id() != arrow::Type::STRING) {
        return Status::Invalid(
            fmt::format("Column masking {} produces a string, but column '{}' is {}.", transform,
                        target.Name(), target.Type()->ToString()));
    }
    return Status::OK();
}

class NullMask : public MaskTransform {
 public:
    const std::string& Name() const override {
        static const std::string kName = "NULL";
        return kName;
    }
    std::vector<std::string> InputFields() const override {
        return {};
    }
    Status ValidateTarget(const DataField& target) const override {
        return Status::OK();
    }
    Result<std::shared_ptr<arrow::Array>> Evaluate(
        const arrow::StructArray& batch, const std::shared_ptr<arrow::DataType>& target_type,
        arrow::MemoryPool* pool) const override {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> nulls,
            arrow::MakeArrayOfNull(target_type, batch.length(), pool));
        return nulls;
    }
};

class FieldRefMask : public MaskTransform {
 public:
    explicit FieldRefMask(FieldRef source) : source_(std::move(source)) {}

    const std::string& Name() const override {
        static const std::string kName = "FIELD_REF";
        return kName;
    }
    std::vector<std::string> InputFields() const override {
        return {source_.name};
    }
    Status ValidateTarget(const DataField& target) const override {
        if (!target.Type()->Equals(*source_.type)) {
            return Status::Invalid(fmt::format(
                "Column masking on '{}' copies column '{}' of type {} into a column of type {}.",
                target.Name(), source_.name, source_.type->ToString(), target.Type()->ToString()));
        }
        return Status::OK();
    }
    Result<std::shared_ptr<arrow::Array>> Evaluate(
        const arrow::StructArray& batch, const std::shared_ptr<arrow::DataType>& target_type,
        arrow::MemoryPool* pool) const override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> column,
                               FindColumn(batch, source_.name));
        if (!column->type()->Equals(*target_type)) {
            return Status::Invalid(fmt::format(
                "Column masking copies column '{}' of type {} into a column of type {}.",
                source_.name, column->type()->ToString(), target_type->ToString()));
        }
        return column;
    }

 private:
    FieldRef source_;
};

struct StringOperand {
    enum class Kind { kNull, kLiteral, kField };
    Kind kind = Kind::kNull;
    std::string literal;
    std::string field;
};

Result<StringOperand> ParseStringOperand(const rapidjson::Value& value, ReferencedTypes* types) {
    StringOperand operand;
    if (value.IsNull()) {
        return operand;
    }
    if (value.IsString()) {
        operand.kind = StringOperand::Kind::kLiteral;
        operand.literal.assign(value.GetString(), value.GetStringLength());
        return operand;
    }
    if (value.IsObject()) {
        PAIMON_ASSIGN_OR_RAISE(FieldRef ref, ParseFieldRef(value, types));
        if (ref.type->id() != arrow::Type::STRING) {
            return Status::Invalid(fmt::format(
                "A string transform of a column mask reads column '{}' of type {}, not a string.",
                ref.name, ref.type->ToString()));
        }
        operand.kind = StringOperand::Kind::kField;
        operand.field = ref.name;
        return operand;
    }
    return Status::Invalid("A string transform input must be a string, a field reference or null.");
}

class StringOperandReader {
 public:
    static Result<StringOperandReader> Create(const StringOperand& operand,
                                              const arrow::StructArray& batch) {
        StringOperandReader reader(operand);
        if (operand.kind == StringOperand::Kind::kField) {
            PAIMON_ASSIGN_OR_RAISE(reader.column_, FindColumn(batch, operand.field));
            if (reader.column_->type_id() != arrow::Type::STRING) {
                return Status::Invalid(fmt::format("Column '{}' a string mask reads is {}.",
                                                   operand.field,
                                                   reader.column_->type()->ToString()));
            }
        }
        return reader;
    }

    std::optional<std::string_view> Get(int64_t row) const {
        switch (operand_.kind) {
            case StringOperand::Kind::kNull:
                return std::nullopt;
            case StringOperand::Kind::kLiteral:
                return std::string_view(operand_.literal);
            case StringOperand::Kind::kField: {
                const auto& strings = checked_cast<const arrow::StringArray&>(*column_);
                if (strings.IsNull(row)) {
                    return std::nullopt;
                }
                return strings.GetView(row);
            }
        }
        return std::nullopt;
    }

 private:
    explicit StringOperandReader(const StringOperand& operand) : operand_(operand) {}

    const StringOperand& operand_;
    std::shared_ptr<arrow::Array> column_;
};

/// `CONCAT` is null when any input is; `CONCAT_WS` is null when its separator is and skips the
/// other null inputs, as Java `BinaryString` does.
class ConcatMask : public MaskTransform {
 public:
    ConcatMask(std::vector<StringOperand> inputs, bool with_separator)
        : inputs_(std::move(inputs)), with_separator_(with_separator) {}

    const std::string& Name() const override {
        static const std::string kConcat = "CONCAT";
        static const std::string kConcatWs = "CONCAT_WS";
        return with_separator_ ? kConcatWs : kConcat;
    }
    std::vector<std::string> InputFields() const override {
        std::vector<std::string> fields;
        for (const StringOperand& input : inputs_) {
            if (input.kind == StringOperand::Kind::kField) {
                fields.push_back(input.field);
            }
        }
        return fields;
    }
    Status ValidateTarget(const DataField& target) const override {
        return CheckStringTarget(Name(), target);
    }
    Result<std::shared_ptr<arrow::Array>> Evaluate(
        const arrow::StructArray& batch, const std::shared_ptr<arrow::DataType>& target_type,
        arrow::MemoryPool* pool) const override {
        std::vector<StringOperandReader> readers;
        readers.reserve(inputs_.size());
        for (const StringOperand& input : inputs_) {
            PAIMON_ASSIGN_OR_RAISE(StringOperandReader reader,
                                   StringOperandReader::Create(input, batch));
            readers.push_back(std::move(reader));
        }
        arrow::StringBuilder builder(pool);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(batch.length()));
        std::string value;
        for (int64_t row = 0; row < batch.length(); ++row) {
            value.clear();
            bool is_null = false;
            if (with_separator_) {
                std::optional<std::string_view> separator = readers[0].Get(row);
                is_null = !separator.has_value();
                bool first = true;
                for (size_t i = 1; !is_null && i < readers.size(); ++i) {
                    std::optional<std::string_view> input = readers[i].Get(row);
                    if (!input) {
                        continue;
                    }
                    if (!first) {
                        value.append(separator->data(), separator->size());
                    }
                    value.append(input->data(), input->size());
                    first = false;
                }
            } else {
                for (const StringOperandReader& reader : readers) {
                    std::optional<std::string_view> input = reader.Get(row);
                    if (!input) {
                        is_null = true;
                        break;
                    }
                    value.append(input->data(), input->size());
                }
            }
            if (is_null) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
            } else {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(value));
            }
        }
        std::shared_ptr<arrow::Array> result;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&result));
        return result;
    }

 private:
    std::vector<StringOperand> inputs_;
    bool with_separator_;
};

struct PositionOperand {
    enum class Kind { kNull, kLiteral, kField };
    Kind kind = Kind::kNull;
    int32_t literal = 0;
    std::string field;
};

Result<PositionOperand> ParsePositionOperand(const rapidjson::Value& value,
                                             ReferencedTypes* types) {
    PositionOperand operand;
    if (value.IsNull()) {
        return operand;
    }
    if (value.IsInt()) {
        operand.kind = PositionOperand::Kind::kLiteral;
        operand.literal = value.GetInt();
        return operand;
    }
    if (value.IsObject()) {
        PAIMON_ASSIGN_OR_RAISE(FieldRef ref, ParseFieldRef(value, types));
        if (!FieldTypeUtils::IsIntegerNumeric(ref.field_type)) {
            return Status::Invalid(fmt::format(
                "SUBSTRING of a column mask takes a position from column '{}' of type {}, not an "
                "integer.",
                ref.name, ref.type->ToString()));
        }
        operand.kind = PositionOperand::Kind::kField;
        operand.field = ref.name;
        return operand;
    }
    return Status::Invalid("A SUBSTRING position must be an integer, a field reference or null.");
}

/// Java `BinaryString.substringSQL`: `pos` counts characters from 1, or from the end when negative.
std::string_view SubstringSql(std::string_view value, int32_t pos, int32_t length) {
    int64_t num_chars = 0;
    for (size_t i = 0; i < value.size(); i += BinaryString::NumBytesForFirstByte(value[i])) {
        ++num_chars;
    }
    int64_t start = 0;
    if (pos > 0) {
        start = static_cast<int64_t>(pos) - 1;
    } else if (pos < 0) {
        start = num_chars + pos;
    }
    const int64_t end = std::clamp<int64_t>(start + length, std::numeric_limits<int32_t>::min(),
                                            std::numeric_limits<int32_t>::max());
    // Java `BinaryString.substring`, comparing a character index with the size in bytes as it does.
    if (end <= start || start >= static_cast<int64_t>(value.size())) {
        return {};
    }
    size_t i = 0;
    int64_t c = 0;
    while (i < value.size() && c < start) {
        i += BinaryString::NumBytesForFirstByte(value[i]);
        ++c;
    }
    const size_t begin = std::min(i, value.size());
    while (i < value.size() && c < end) {
        i += BinaryString::NumBytesForFirstByte(value[i]);
        ++c;
    }
    i = std::min(i, value.size());
    return i > begin ? value.substr(begin, i - begin) : std::string_view();
}

/// `SUBSTRING(source, pos[, length])`, null when any input is.
class SubstringMask : public MaskTransform {
 public:
    /// @param positions `pos`, then the optional `length`.
    SubstringMask(StringOperand source, std::vector<PositionOperand> positions)
        : source_(std::move(source)), positions_(std::move(positions)) {}

    const std::string& Name() const override {
        static const std::string kName = "SUBSTRING";
        return kName;
    }
    std::vector<std::string> InputFields() const override {
        std::vector<std::string> fields;
        if (source_.kind == StringOperand::Kind::kField) {
            fields.push_back(source_.field);
        }
        for (const PositionOperand& position : positions_) {
            if (position.kind == PositionOperand::Kind::kField) {
                fields.push_back(position.field);
            }
        }
        return fields;
    }
    Status ValidateTarget(const DataField& target) const override {
        return CheckStringTarget(Name(), target);
    }
    Result<std::shared_ptr<arrow::Array>> Evaluate(
        const arrow::StructArray& batch, const std::shared_ptr<arrow::DataType>& target_type,
        arrow::MemoryPool* pool) const override {
        PAIMON_ASSIGN_OR_RAISE(StringOperandReader source,
                               StringOperandReader::Create(source_, batch));
        std::vector<std::shared_ptr<arrow::Array>> position_columns(positions_.size());
        for (size_t i = 0; i < positions_.size(); ++i) {
            if (positions_[i].kind == PositionOperand::Kind::kField) {
                PAIMON_ASSIGN_OR_RAISE(position_columns[i], FindColumn(batch, positions_[i].field));
            }
        }
        arrow::StringBuilder builder(pool);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(batch.length()));
        for (int64_t row = 0; row < batch.length(); ++row) {
            std::optional<std::string_view> value = source.Get(row);
            bool null_position = false;
            for (size_t i = 0; i < positions_.size(); ++i) {
                if (positions_[i].kind == PositionOperand::Kind::kNull ||
                    (positions_[i].kind == PositionOperand::Kind::kField &&
                     position_columns[i]->IsNull(row))) {
                    null_position = true;
                    break;
                }
            }
            // Java checks every input for NULL before converting either position to an int.
            if (!value || null_position) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
                continue;
            }
            std::optional<int32_t> pos;
            std::optional<int32_t> length = std::numeric_limits<int32_t>::max();
            PAIMON_ASSIGN_OR_RAISE(pos, ReadPosition(positions_[0], position_columns[0], row));
            if (positions_.size() == 2) {
                PAIMON_ASSIGN_OR_RAISE(length,
                                       ReadPosition(positions_[1], position_columns[1], row));
            }
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                builder.Append(SubstringSql(value.value(), pos.value(), length.value())));
        }
        std::shared_ptr<arrow::Array> result;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&result));
        return result;
    }

 private:
    static Result<std::optional<int32_t>> ReadPosition(const PositionOperand& position,
                                                       const std::shared_ptr<arrow::Array>& column,
                                                       int64_t row) {
        if (position.kind == PositionOperand::Kind::kNull) {
            return std::optional<int32_t>();
        }
        if (position.kind == PositionOperand::Kind::kLiteral) {
            return std::optional<int32_t>(position.literal);
        }
        if (column->IsNull(row)) {
            return std::optional<int32_t>();
        }
        int64_t value = 0;
        switch (column->type_id()) {
            case arrow::Type::INT8:
                value = checked_cast<const arrow::Int8Array&>(*column).Value(row);
                break;
            case arrow::Type::INT16:
                value = checked_cast<const arrow::Int16Array&>(*column).Value(row);
                break;
            case arrow::Type::INT32:
                value = checked_cast<const arrow::Int32Array&>(*column).Value(row);
                break;
            case arrow::Type::INT64:
                value = checked_cast<const arrow::Int64Array&>(*column).Value(row);
                break;
            default:
                return Status::Invalid(fmt::format("Column '{}' a SUBSTRING mask reads is {}.",
                                                   position.field, column->type()->ToString()));
        }
        if (value < std::numeric_limits<int32_t>::min() ||
            value > std::numeric_limits<int32_t>::max()) {
            return Status::Invalid(fmt::format(
                "SUBSTRING position of column '{}' does not fit an int.", position.field));
        }
        return std::optional<int32_t>(static_cast<int32_t>(value));
    }

    StringOperand source_;
    std::vector<PositionOperand> positions_;
};

/// Parses a Java `Transform` used as a column mask: `{"name": "CONCAT", "inputs": [...]}`.
Result<std::shared_ptr<MaskTransform>> ParseMaskTransform(const rapidjson::Value& value,
                                                          ReferencedTypes* types) {
    if (value.IsNull()) {
        return Status::Invalid("Column mask transform cannot be JSON null.");
    }
    if (!value.IsObject()) {
        return Status::Invalid("A column mask transform must be a JSON object.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string name, GetString(value, "name", "A column mask transform"));
    if (name == "NULL") {
        return std::make_shared<NullMask>();
    }
    if (name == "FIELD_REF") {
        auto field_ref_iter = value.FindMember("fieldRef");
        if (field_ref_iter == value.MemberEnd()) {
            return Status::Invalid("A FIELD_REF transform needs a 'fieldRef' member.");
        }
        PAIMON_ASSIGN_OR_RAISE(FieldRef ref, ParseFieldRef(field_ref_iter->value, types));
        return std::make_shared<FieldRefMask>(std::move(ref));
    }
    if (name != "CONCAT" && name != "CONCAT_WS" && name != "SUBSTRING") {
        return Status::NotImplemented(
            fmt::format("Column mask transform '{}' is not supported.", name));
    }
    auto inputs_iter = value.FindMember("inputs");
    if (inputs_iter == value.MemberEnd() || !inputs_iter->value.IsArray()) {
        return Status::Invalid(fmt::format("A {} transform needs an 'inputs' array.", name));
    }
    const auto& inputs = inputs_iter->value;
    if (name == "SUBSTRING") {
        if (inputs.Size() != 2 && inputs.Size() != 3) {
            return Status::Invalid("A SUBSTRING transform takes 2 or 3 inputs.");
        }
        PAIMON_ASSIGN_OR_RAISE(StringOperand source, ParseStringOperand(*inputs.Begin(), types));
        std::vector<PositionOperand> positions;
        for (rapidjson::SizeType i = 1; i < inputs.Size(); ++i) {
            PAIMON_ASSIGN_OR_RAISE(PositionOperand position,
                                   ParsePositionOperand(inputs[i], types));
            positions.push_back(std::move(position));
        }
        return std::make_shared<SubstringMask>(std::move(source), std::move(positions));
    }
    if (name == "CONCAT_WS" && inputs.Size() < 2) {
        return Status::Invalid("A CONCAT_WS transform needs a separator and at least one input.");
    }
    std::vector<StringOperand> operands;
    for (const rapidjson::Value& input : inputs.GetArray()) {
        PAIMON_ASSIGN_OR_RAISE(StringOperand operand, ParseStringOperand(input, types));
        operands.push_back(std::move(operand));
    }
    return std::make_shared<ConcatMask>(std::move(operands), name == "CONCAT_WS");
}

/// Java `SpecialFields.SYSTEM_FIELD_NAMES`, none of which a query-auth read projects.
bool IsSystemColumn(const std::string& name) {
    return name == SpecialFields::SequenceNumber().Name() ||
           name == SpecialFields::ValueKind().Name() || name == "_LEVEL" ||
           name == SpecialFields::RowKind().Name() || name == SpecialFields::RowId().Name();
}

const DataField* FindField(const std::vector<DataField>& fields, const std::string& name) {
    for (const DataField& field : fields) {
        if (field.Name() == name) {
            return &field;
        }
    }
    return nullptr;
}

const DataField* FindFieldById(const std::vector<DataField>& fields, int32_t id) {
    for (const DataField& field : fields) {
        if (field.Id() == id) {
            return &field;
        }
    }
    return nullptr;
}

std::vector<std::string> FieldNames(const std::vector<DataField>& fields) {
    std::vector<std::string> names;
    names.reserve(fields.size());
    for (const DataField& field : fields) {
        names.push_back(field.Name());
    }
    return names;
}

}  // namespace

Result<std::shared_ptr<TableQueryAuthResult>> TableQueryAuthResult::Create(
    const std::vector<std::string>& filter,
    const std::map<std::string, std::string>& column_masking) {
    std::shared_ptr<TableQueryAuthResult> result(new TableQueryAuthResult(filter, column_masking));
    std::vector<ParsedFilter> filters;
    for (const std::string& json : filter) {
        if (json.empty()) {
            return Status::Invalid("Row filter cannot be empty.");
        }
        PAIMON_ASSIGN_OR_RAISE(rapidjson::Document document, ParseJson(json, "Row filter"));
        if (document.IsNull()) {
            return Status::Invalid("Row filter cannot be JSON null.");
        }
        // A second view retains numeric token spellings without treating quoted literals as
        // numbers. FLOAT conversion needs these for integers outside the 64-bit range.
        rapidjson::Document number_texts;
        number_texts.Parse<rapidjson::kParseNumbersAsStringsFlag>(json.c_str(), json.size());
        if (number_texts.HasParseError()) {
            return Status::Invalid("Row filter numeric literals could not be parsed.");
        }
        PAIMON_ASSIGN_OR_RAISE(ParsedFilter parsed,
                               ParsePredicate(document, number_texts, &result->referenced_types_));
        filters.push_back(std::move(parsed));
    }
    PAIMON_ASSIGN_OR_RAISE(ParsedFilter row_filter, Combine(filters, /*is_and=*/true));
    result->rejects_all_rows_ = row_filter.predicate == nullptr && !row_filter.value;
    result->row_filter_ = std::move(row_filter.predicate);
    if (result->row_filter_ != nullptr) {
        PAIMON_RETURN_NOT_OK(
            PredicateUtils::GetAllNames(result->row_filter_, &result->row_filter_fields_));
    }
    for (const auto& [column, json] : column_masking) {
        if (column.empty()) {
            return Status::Invalid("Column mask target cannot be empty.");
        }
        if (json.empty()) {
            return Status::Invalid("Column mask transform cannot be empty.");
        }
        PAIMON_ASSIGN_OR_RAISE(rapidjson::Document document,
                               ParseJson(json, "Column mask transform"));
        if (IsSystemColumn(column)) {
            // As in Java for a system column the query does not project, the mask never reaches
            // the output: it is parsed, but neither validated nor applied.
            ReferencedTypes inert_types;
            PAIMON_RETURN_NOT_OK(ParseMaskTransform(document, &inert_types).status());
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<MaskTransform> transform,
                               ParseMaskTransform(document, &result->referenced_types_));
        result->masks_.emplace(column, std::move(transform));
    }
    return result;
}

std::set<std::string> TableQueryAuthResult::MaskedFields() const {
    std::set<std::string> fields;
    for (const auto& [column, transform] : masks_) {
        fields.insert(column);
    }
    return fields;
}

bool TableQueryAuthResult::SameFilter(const TableQueryAuthResult& other) const {
    if (rejects_all_rows_ != other.rejects_all_rows_) {
        return false;
    }
    // As Java, compares the parsed conjuncts, so neither the JSON text nor their order matters.
    std::vector<std::shared_ptr<Predicate>> conjuncts = PredicateUtils::SplitAnd(row_filter_);
    std::vector<std::shared_ptr<Predicate>> other_conjuncts =
        PredicateUtils::SplitAnd(other.row_filter_);
    auto contains_all = [](const std::vector<std::shared_ptr<Predicate>>& left,
                           const std::vector<std::shared_ptr<Predicate>>& right) {
        return std::all_of(right.begin(), right.end(), [&](const std::shared_ptr<Predicate>& r) {
            return std::any_of(left.begin(), left.end(),
                               [&](const std::shared_ptr<Predicate>& l) { return *l == *r; });
        });
    };
    return contains_all(conjuncts, other_conjuncts) && contains_all(other_conjuncts, conjuncts);
}

Status TableQueryAuthResult::ValidateAgainstSchema(
    const std::vector<DataField>& latest_fields) const {
    for (const auto& [target, json] : column_masking_) {
        if (IsSystemColumn(target) && FindField(latest_fields, target) != nullptr) {
            return Status::Invalid(fmt::format(
                "Column mask target '{}' names a table column reserved as a system column; "
                "refusing to ignore its mask.",
                target));
        }
    }
    auto check_exists = [&](const std::string& rule, const std::string& field) -> Status {
        if (FindField(latest_fields, field) == nullptr) {
            return Status::Invalid(fmt::format(
                "{} references column '{}' which does not exist in table schema [{}]. The rule "
                "may be stale after a column rename or drop; refusing to read.",
                rule, field, fmt::join(FieldNames(latest_fields), ", ")));
        }
        return Status::OK();
    };
    for (const std::string& field : row_filter_fields_) {
        PAIMON_RETURN_NOT_OK(check_exists("Row filter", field));
    }
    for (const auto& [target, transform] : masks_) {
        PAIMON_RETURN_NOT_OK(check_exists("Column masking", target));
        PAIMON_RETURN_NOT_OK(transform->ValidateTarget(*FindField(latest_fields, target)));
        for (const std::string& input : transform->InputFields()) {
            PAIMON_RETURN_NOT_OK(check_exists("Column masking", input));
            if (input != target && masks_.count(input) > 0) {
                return Status::Invalid(fmt::format(
                    "Column masking on '{}' reads column '{}', which is masked too. The mask "
                    "would be computed from the raw value of '{}' and expose it through '{}'.",
                    target, input, input, target));
            }
        }
    }
    // Also every column of a branch a constant folded away, as Java validates the parsed rules.
    for (const auto& [name, type] : referenced_types_) {
        PAIMON_RETURN_NOT_OK(check_exists("A query auth rule", name));
        const DataField* field = FindField(latest_fields, name);
        if (!field->Type()->Equals(*type)) {
            return Status::Invalid(fmt::format(
                "Query auth rules read column '{}' as {}, but the table holds it as {}; refusing "
                "to read.",
                name, type->ToString(), field->Type()->ToString()));
        }
    }
    return Status::OK();
}

std::set<std::string> TableQueryAuthResult::ReferencedFields() const {
    std::set<std::string> referenced = row_filter_fields_;
    for (const auto& [name, type] : referenced_types_) {
        referenced.insert(name);
    }
    for (const auto& [target, transform] : masks_) {
        referenced.insert(target);
        for (const std::string& input : transform->InputFields()) {
            referenced.insert(input);
        }
    }
    return referenced;
}

std::map<std::string, int32_t> TableQueryAuthResult::BindFieldIds(
    const std::vector<DataField>& fields) const {
    std::map<std::string, int32_t> field_ids;
    for (const std::string& name : ReferencedFields()) {
        const DataField* field = FindField(fields, name);
        if (field != nullptr) {
            field_ids.emplace(name, field->Id());
        }
    }
    return field_ids;
}

Status TableQueryAuthResult::ValidateFieldIds(const std::map<std::string, int32_t>& field_ids,
                                              const std::vector<DataField>& fields) const {
    // A column left unbound would be matched by name alone.
    for (const std::string& name : ReferencedFields()) {
        if (field_ids.count(name) == 0) {
            return Status::Invalid(fmt::format(
                "Query auth rules reference column '{}', which the schema the scan planned with "
                "did not hold. Create the scan again.",
                name));
        }
    }
    for (const auto& [name, id] : field_ids) {
        const DataField* field = FindField(fields, name);
        if (field != nullptr) {
            if (field->Id() != id) {
                return Status::Invalid(fmt::format(
                    "Query auth rules reference column '{}' which the schema being read exposes "
                    "as a different column of the same name (field {}, while the rules were bound "
                    "to field {}); refusing to read to avoid applying the rule to unrelated data.",
                    name, field->Id(), id));
            }
            continue;
        }
        const DataField* renamed = FindFieldById(fields, id);
        if (renamed != nullptr) {
            return Status::Invalid(fmt::format(
                "Query auth rules reference column '{}' which the schema being read exposes as "
                "'{}' (renamed since), field {}; refusing to read to avoid applying the rule by a "
                "stale name.",
                name, renamed->Name(), id));
        }
    }
    return Status::OK();
}

std::set<std::string> TableQueryAuthResult::RequiredFields(
    const std::vector<std::string>& visible_fields) const {
    std::set<std::string> required = row_filter_fields_;
    std::set<std::string> readable(visible_fields.begin(), visible_fields.end());
    std::deque<std::string> newly_readable(visible_fields.begin(), visible_fields.end());
    for (const std::string& field : row_filter_fields_) {
        if (readable.insert(field).second) {
            newly_readable.push_back(field);
        }
    }
    while (!newly_readable.empty()) {
        std::string field = std::move(newly_readable.front());
        newly_readable.pop_front();
        auto iter = masks_.find(field);
        if (iter == masks_.end()) {
            continue;
        }
        for (const std::string& input : iter->second->InputFields()) {
            required.insert(input);
            if (readable.insert(input).second) {
                newly_readable.push_back(input);
            }
        }
    }
    return required;
}

}  // namespace paimon
