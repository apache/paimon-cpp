/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/common/utils/string_utils.h"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <iterator>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>

#include "fmt/format.h"
#include "paimon/status.h"

namespace paimon {
namespace {

bool IsTrimCharacter(unsigned char c) {
    // Match the characters removed by Java String::trim for the ASCII strings handled here.
    return c <= 0x20;
}

bool IsJavaWhitespace(uint32_t code_point) {
    return (code_point >= 0x0009 && code_point <= 0x000d) ||
           (code_point >= 0x001c && code_point <= 0x0020) || code_point == 0x1680 ||
           (code_point >= 0x2000 && code_point <= 0x2006) ||
           (code_point >= 0x2008 && code_point <= 0x200a) || code_point == 0x2028 ||
           code_point == 0x2029 || code_point == 0x205f || code_point == 0x3000;
}

char ToAsciiLower(unsigned char c) {
    return c >= 'A' && c <= 'Z' ? static_cast<char>(c + ('a' - 'A')) : static_cast<char>(c);
}

char ToAsciiUpper(unsigned char c) {
    return c >= 'a' && c <= 'z' ? static_cast<char>(c - ('a' - 'A')) : static_cast<char>(c);
}

}  // namespace

std::string StringUtils::Replace(const std::string& text, const std::string& search_string,
                                 const std::string& replacement, int32_t max) {
    if (text.empty() || search_string.empty() || max == 0) {
        return text;
    }
    std::string str = text;
    size_t pos = str.find(search_string);
    int32_t count = 0;
    while (pos != std::string::npos && (count < max || max == -1)) {
        str.replace(pos, search_string.size(), replacement);
        pos = str.find(search_string, pos + replacement.size());
        count++;
    }
    return str;
}

std::string StringUtils::ReplaceLast(const std::string& text, const std::string& old_str,
                                     const std::string& new_str) {
    if (text.empty() || old_str.empty()) {
        return text;
    }
    std::string str = text;
    size_t pos = str.rfind(old_str);
    if (pos != std::string::npos) {
        str.replace(pos, old_str.size(), new_str);
    }
    return str;
}

bool StringUtils::StartsWith(const std::string& str, const std::string& prefix, size_t start_pos) {
    return start_pos <= str.size() && prefix.size() <= str.size() - start_pos &&
           str.compare(start_pos, prefix.size(), prefix) == 0;
}
bool StringUtils::EndsWith(const std::string& str, const std::string& suffix) {
    size_t s1 = str.size();
    size_t s2 = suffix.size();
    return (s1 >= s2) && (str.compare(s1 - s2, s2, suffix) == 0);
}

bool StringUtils::IsBlank(std::string_view str) {
    size_t offset = 0;
    while (offset < str.size()) {
        const auto first = static_cast<uint8_t>(str[offset]);
        uint32_t code_point = 0;
        size_t length = 0;
        if (first <= 0x7f) {
            code_point = first;
            length = 1;
        } else if (first >= 0xc2 && first <= 0xdf) {
            code_point = first & 0x1f;
            length = 2;
        } else if (first >= 0xe0 && first <= 0xef) {
            code_point = first & 0x0f;
            length = 3;
        } else if (first >= 0xf0 && first <= 0xf4) {
            code_point = first & 0x07;
            length = 4;
        } else {
            return false;
        }
        if (offset + length > str.size()) {
            return false;
        }
        for (size_t i = 1; i < length; ++i) {
            const auto continuation = static_cast<uint8_t>(str[offset + i]);
            if ((continuation & 0xc0) != 0x80) {
                return false;
            }
            code_point = (code_point << 6) | (continuation & 0x3f);
        }
        if ((length == 3 && code_point < 0x800) || (length == 4 && code_point < 0x10000) ||
            (code_point >= 0xd800 && code_point <= 0xdfff) || code_point > 0x10ffff) {
            return false;
        }
        if (!IsJavaWhitespace(code_point)) {
            return false;
        }
        offset += length;
    }
    return true;
}

bool StringUtils::IsNullOrWhitespaceOnly(std::string_view str) {
    return IsBlank(str);
}

bool StringUtils::IsEmptyAfterTrim(std::string_view str) {
    return std::all_of(str.begin(), str.end(), [](unsigned char c) { return IsTrimCharacter(c); });
}

void StringUtils::Trim(std::string* str) {
    auto first = std::find_if_not(str->begin(), str->end(),
                                  [](unsigned char c) { return IsTrimCharacter(c); });
    auto last = std::find_if_not(str->rbegin(), str->rend(), [](unsigned char c) {
                    return IsTrimCharacter(c);
                }).base();
    if (first >= last) {
        str->clear();
        return;
    }
    *str = std::string(first, last);
}

std::string StringUtils::ToLowerCase(const std::string& str) {
    std::string result;
    result.reserve(str.length());
    std::transform(str.begin(), str.end(), std::back_inserter(result), ToAsciiLower);
    return result;
}

std::string StringUtils::ToUpperCase(const std::string& str) {
    std::string result;
    result.reserve(str.length());
    std::transform(str.begin(), str.end(), std::back_inserter(result), ToAsciiUpper);
    return result;
}

bool StringUtils::EqualsIgnoreCase(const std::string& left, const std::string& right) {
    if (left.size() != right.size()) {
        return false;
    }
    for (size_t i = 0; i < left.size(); ++i) {
        if (ToAsciiLower(static_cast<unsigned char>(left[i])) !=
            ToAsciiLower(static_cast<unsigned char>(right[i]))) {
            return false;
        }
    }
    return true;
}

std::vector<std::string> StringUtils::Split(const std::string& text, const std::string& sep_str,
                                            bool ignore_empty) {
    std::vector<std::string> vec;
    if (sep_str.empty()) {
        // invalid case, do not split.
        vec.emplace_back(text);
        return vec;
    }
    size_t n = 0, old = 0;
    while (n != std::string::npos) {
        n = text.find(sep_str, n);
        if (n != std::string::npos) {
            if (!ignore_empty || n != old) {
                vec.emplace_back(text.substr(old, n - old));
            }
            n += sep_str.length();
            old = n;
        }
    }

    if (!ignore_empty || old < text.length()) {
        vec.emplace_back(text.substr(old, text.length() - old));
    }
    return vec;
}

std::vector<std::vector<std::string>> StringUtils::Split(const std::string& text,
                                                         const std::string& delim1,
                                                         const std::string& delim2) {
    std::vector<std::vector<std::string>> result;
    std::vector<std::string> split_parts = Split(text, delim1);
    result.reserve(split_parts.size());
    for (auto& part : split_parts) {
        result.emplace_back(Split(part, delim2));
    }
    return result;
}

Result<int32_t> StringUtils::StringToDate(const std::string& str) {
    auto int_value = StringToValue<int32_t>(str);
    if (int_value) {
        return int_value.value();
    }
    std::tm timeinfo = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, nullptr};
    std::istringstream ss(str);
    ss >> std::get_time(&timeinfo, "%Y-%m-%d");
    if (ss.fail()) {
        return Status::Invalid(fmt::format("failed to convert string '{}' to date", str));
    }
    int32_t orig_mon = timeinfo.tm_mon;
    int32_t orig_mday = timeinfo.tm_mday;
    std::time_t time = timegm(&timeinfo);
    if (time == -1 || timeinfo.tm_mon != orig_mon || timeinfo.tm_mday != orig_mday) {
        return Status::Invalid(fmt::format("failed to convert string '{}' to date", str));
    }
    static const int64_t SECONDS_PER_DAY = 86400l;  // = 24 * 60 * 60
    return time / SECONDS_PER_DAY;
}

namespace {
constexpr int32_t kMillisPerSecond = 1000;
constexpr int32_t kMillisPerMinute = 60 * kMillisPerSecond;
constexpr int32_t kMillisPerHour = 60 * kMillisPerMinute;
constexpr int32_t kMillisPerDay = 24 * kMillisPerHour;

// Removes the characters Java `String#trim` removes from both ends.
std::string_view TrimView(std::string_view str) {
    while (!str.empty() && IsTrimCharacter(static_cast<unsigned char>(str.front()))) {
        str.remove_prefix(1);
    }
    while (!str.empty() && IsTrimCharacter(static_cast<unsigned char>(str.back()))) {
        str.remove_suffix(1);
    }
    return str;
}

bool IsDigit(char c) {
    return c >= '0' && c <= '9';
}

// Parses a non-negative decimal component of a TIME string, surrounding whitespace allowed, like
// Java `DateTimeUtils#isInteger` followed by `Integer#parseInt`.
std::optional<int32_t> ParseTimeComponent(std::string_view component) {
    component = TrimView(component);
    if (component.empty()) {
        return std::nullopt;
    }
    int64_t value = 0;
    for (char c : component) {
        if (!IsDigit(c)) {
            return std::nullopt;
        }
        value = value * 10 + (c - '0');
        if (value > std::numeric_limits<int32_t>::max()) {
            return std::nullopt;
        }
    }
    return static_cast<int32_t>(value);
}

// Rounds the fractional digits of a second half up to milliseconds, surrounding whitespace
// allowed, like Java `DateTimeUtils#parseFraction`. An empty fraction is 0.
std::optional<int32_t> ParseTimeFraction(std::string_view fraction) {
    fraction = TrimView(fraction);
    if (!std::all_of(fraction.begin(), fraction.end(), IsDigit)) {
        return std::nullopt;
    }
    int32_t result = 0;
    int32_t multiplier = 100;
    for (size_t i = 0; i < fraction.size() && multiplier > 0; ++i, multiplier /= 10) {
        result += multiplier * (fraction[i] - '0');
    }
    if (fraction.size() > 3 && fraction[3] >= '5') {
        ++result;
    }
    return result;
}

// Parses the `hh[:mm]` of a TIME zone offset into milliseconds, like the offset part of Java
// `DateTimeUtils#parseTime`, but requiring hh <= 23 and mm <= 59.
std::optional<int32_t> ParseTimeOffset(std::string_view offset) {
    size_t colon = offset.find(':');
    std::optional<int32_t> hour;
    std::optional<int32_t> minute = 0;
    if (colon == std::string_view::npos) {
        hour = ParseTimeComponent(offset);
    } else {
        hour = ParseTimeComponent(offset.substr(0, colon));
        minute = ParseTimeComponent(offset.substr(colon + 1));
    }
    if (!hour || !minute || hour.value() > 23 || minute.value() > 59) {
        return std::nullopt;
    }
    return hour.value() * kMillisPerHour + minute.value() * kMillisPerMinute;
}
}  // namespace

Result<int32_t> StringUtils::StringToTime(const std::string& str) {
    auto invalid = [&str]() {
        return Status::Invalid(
            fmt::format("failed to convert string '{}' to time, expected milliseconds of the day "
                        "or H[H][:m[m][:s[s][.f...]]] with an optional [+|-]hh[:mm] offset, "
                        "within [00:00:00, 24:00:00)",
                        str));
    };
    std::optional<int32_t> millis = StringToValue<int32_t>(str);
    if (!millis) {
        std::string_view view(str);
        // Like Java `DateTimeUtils#parseTime`, a '-' offset is searched before a '+' one.
        int32_t offset_sign = -1;
        size_t sign_pos = view.find('-');
        if (sign_pos == std::string_view::npos) {
            offset_sign = 1;
            sign_pos = view.find('+');
        }
        std::optional<int32_t> offset = 0;
        if (sign_pos != std::string_view::npos) {
            offset = ParseTimeOffset(view.substr(sign_pos + 1));
            view = view.substr(0, sign_pos);
        }
        size_t colon1 = view.find(':');
        std::optional<int32_t> hour;
        std::optional<int32_t> minute = 0;
        std::optional<int32_t> second = 0;
        std::optional<int32_t> milli = 0;
        if (colon1 == std::string_view::npos) {
            hour = ParseTimeComponent(view);
        } else {
            hour = ParseTimeComponent(view.substr(0, colon1));
            size_t colon2 = view.find(':', colon1 + 1);
            if (colon2 == std::string_view::npos) {
                minute = ParseTimeComponent(view.substr(colon1 + 1));
            } else {
                minute = ParseTimeComponent(view.substr(colon1 + 1, colon2 - colon1 - 1));
                size_t dot = view.find('.', colon2 + 1);
                if (dot == std::string_view::npos) {
                    second = ParseTimeComponent(view.substr(colon2 + 1));
                } else {
                    second = ParseTimeComponent(view.substr(colon2 + 1, dot - colon2 - 1));
                    milli = ParseTimeFraction(view.substr(dot + 1));
                }
            }
        }
        if (!offset || !hour || !minute || !second || !milli || hour.value() > 23 ||
            minute.value() > 59 || second.value() > 59) {
            return invalid();
        }
        millis = hour.value() * kMillisPerHour + minute.value() * kMillisPerMinute +
                 second.value() * kMillisPerSecond + milli.value() + offset_sign * offset.value();
    }
    if (millis.value() < 0 || millis.value() >= kMillisPerDay) {
        return invalid();
    }
    return millis.value();
}

/// Parses a timestamp string into unix milliseconds.
/// Supported formats: "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS".
/// Uses the default local time zone, consistent with Java Paimon behavior.
Result<int64_t> StringUtils::StringToTimestampMillis(const std::string& str) {
    std::tm timeinfo{};
    timeinfo.tm_isdst = -1;

    // Try "yyyy-MM-dd HH:mm:ss" first (also matches "yyyy-MM-dd HH:mm:ss.SSS")
    std::istringstream ss(str);
    ss >> std::get_time(&timeinfo, "%Y-%m-%d %H:%M:%S");
    int32_t millis_part = 0;

    if (!ss.fail()) {
        // Check for optional fractional seconds ".SSS"
        if (ss.peek() == '.') {
            ss.get();
            std::string frac;
            while (frac.size() < 3 && ss.peek() != std::char_traits<char>::eof() &&
                   std::isdigit(static_cast<unsigned char>(ss.peek()))) {
                frac += static_cast<char>(ss.get());
            }
            if (frac.empty()) {
                return Status::Invalid(
                    fmt::format("failed to convert string '{}' to timestamp, "
                                "expected digits after '.'",
                                str));
            }
            // Pad to 3 digits: "1" -> 100, "12" -> 120, "123" -> 123
            while (frac.size() < 3) {
                frac += '0';
            }
            auto parsed = StringToValue<int32_t>(frac);
            if (parsed) {
                millis_part = parsed.value();
            }
        }
    } else {
        // Fall back to "yyyy-MM-dd" (date only, time defaults to 00:00:00)
        ss.clear();
        ss.str(str);
        timeinfo = std::tm{};
        timeinfo.tm_isdst = -1;
        ss >> std::get_time(&timeinfo, "%Y-%m-%d");
        if (ss.fail()) {
            return Status::Invalid(
                fmt::format("failed to convert string '{}' to timestamp, "
                            "supported formats: yyyy-MM-dd, yyyy-MM-dd HH:mm:ss, "
                            "yyyy-MM-dd HH:mm:ss.SSS",
                            str));
        }
    }

    if (ss.peek() != std::char_traits<char>::eof()) {
        return Status::Invalid(
            fmt::format("failed to convert string '{}' to timestamp, "
                        "unexpected trailing characters",
                        str));
    }

    int32_t orig_mon = timeinfo.tm_mon;
    int32_t orig_mday = timeinfo.tm_mday;
    std::time_t time = mktime(&timeinfo);
    if (time == -1 || timeinfo.tm_mon != orig_mon || timeinfo.tm_mday != orig_mday) {
        return Status::Invalid(fmt::format("failed to convert string '{}' to timestamp", str));
    }
    return static_cast<int64_t>(time) * 1000 + millis_part;
}

}  // namespace paimon
