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
        const uint8_t first = static_cast<uint8_t>(str[offset]);
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
            const uint8_t continuation = static_cast<uint8_t>(str[offset + i]);
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

bool StringUtils::IsNullOrWhitespaceOnly(const std::string& str) {
    return IsBlank(str);
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
