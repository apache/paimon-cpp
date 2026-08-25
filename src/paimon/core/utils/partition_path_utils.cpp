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

#include "paimon/core/utils/partition_path_utils.h"

#include <array>
#include <cstdint>
#include <optional>
#include <utility>

#include "paimon/status.h"

namespace paimon {

const std::bitset<128>& PartitionPathUtils::CharToEscape() {
    constexpr auto char_to_escape = []() {
        std::bitset<128> bitset;
        for (char c = 0; c < ' '; c++) {
            bitset.set(static_cast<unsigned char>(c));
        }
        std::array<char, 48> clist = {
            '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', '\u0008',
            '\u0009', '\n',     '\u000B', '\u000C', '\r',     '\u000E', '\u000F', '\u0010',
            '\u0011', '\u0012', '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018',
            '\u0019', '\u001A', '\u001B', '\u001C', '\u001D', '\u001E', '\u001F', '"',
            '#',      '%',      '\'',     '*',      '/',      ':',      '=',      '?',
            '\\',     '\u007F', '{',      '}',      '[',      ']',      '^'};
        for (char c : clist) {
            bitset.set(static_cast<unsigned char>(c));
        }
        return bitset;
    };
    static std::bitset<128> bitset = char_to_escape();
    return bitset;
}

Status PartitionPathUtils::ValidatePartitionValueForPath(const std::string& value,
                                                         bool only_value) {
    if (value.empty() || (only_value && (value == "." || value == ".."))) {
        return Status::Invalid("Partition value '" + value +
                               "' cannot be used as a partition path component.");
    }
    return Status::OK();
}

Result<std::string> PartitionPathUtils::GeneratePartitionPath(
    const std::vector<std::pair<std::string, std::string>>& partition_spec, bool only_value) {
    if (partition_spec.empty()) {
        return std::string();
    }
    std::stringstream ss;
    int32_t i = 0;
    for (const auto& [key, value] : partition_spec) {
        if (i > 0) {
            ss << PATH_SEPARATOR;
        }
        if (!only_value) {
            PAIMON_ASSIGN_OR_RAISE(std::string key_esc, EscapePathName(key));
            ss << key_esc << "=";
        }
        PAIMON_RETURN_NOT_OK(ValidatePartitionValueForPath(value, only_value));
        PAIMON_ASSIGN_OR_RAISE(std::string value_esc, EscapePathName(value));
        ss << value_esc;
        i++;
    }
    ss << PATH_SEPARATOR;
    return ss.str();
}

Result<std::string> PartitionPathUtils::EscapePathName(const std::string& path) {
    if (path.empty()) {
        return Status::Invalid("path should not be empty");
    }

    std::optional<std::stringstream> ss;
    for (size_t i = 0; i < path.size(); i++) {
        char c = path[i];
        if (NeedsEscaping(c)) {
            if (ss == std::nullopt) {
                ss = std::stringstream();
                for (size_t j = 0; j < i; j++) {
                    ss.value() << path[j];
                }
            }
            EscapeChar(c, &ss.value());
        } else if (ss != std::nullopt) {
            ss.value() << c;
        }
    }
    if (ss == std::nullopt) {
        return path;
    }
    return ss.value().str();
}

namespace {
/// Value of one hexadecimal digit, or -1 when `c` is not one.
///
/// Written out rather than handed to `strtol`, which also accepts a leading sign or whitespace and
/// so would read `"% 1"` and `"%+1"` as escape sequences that `EscapePathName` never produces.
int32_t HexDigit(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}
}  // namespace

std::string PartitionPathUtils::UnescapePathName(const std::string& path) {
    std::string result;
    result.reserve(path.size());
    for (size_t i = 0; i < path.size(); i++) {
        // Not an off-by-one: a `%` among the last two characters starts no sequence and is left
        // as written.
        if (path[i] == '%' && i + 2 < path.size()) {
            const int32_t high = HexDigit(path[i + 1]);
            const int32_t low = HexDigit(path[i + 2]);
            if (high >= 0 && low >= 0) {
                result.push_back(static_cast<char>(high * 16 + low));
                i += 2;
                continue;
            }
        }
        result.push_back(path[i]);
    }
    return result;
}

std::optional<std::pair<std::string, std::string>> PartitionPathUtils::ExtractPartitionKeyValue(
    const std::string& directory_name) {
    size_t separator = directory_name.find('=');
    // Both halves must be non-empty: `=v` names no key and `k=` no value, and neither is a
    // partition directory this table wrote.
    if (separator == std::string::npos || separator == 0 ||
        separator + 1 == directory_name.size()) {
        return std::nullopt;
    }
    // A second `=` cannot appear unescaped, so the name belongs to something else.
    if (directory_name.find('=', separator + 1) != std::string::npos) {
        return std::nullopt;
    }
    return std::make_pair(UnescapePathName(directory_name.substr(0, separator)),
                          UnescapePathName(directory_name.substr(separator + 1)));
}

void PartitionPathUtils::EscapeChar(char c, std::stringstream* ss_ptr) {
    auto& ss = *ss_ptr;
    ss << '%';
    auto uc = static_cast<unsigned char>(c);
    if (uc < 16) {
        ss << '0';
    }
    std::stringstream hex_ss;
    hex_ss << std::hex << std::uppercase << static_cast<int32_t>(uc);
    ss << hex_ss.str();
}

Result<std::vector<std::string>> PartitionPathUtils::GenerateHierarchicalPartitionPaths(
    const std::vector<std::pair<std::string, std::string>>& partition_spec) {
    std::vector<std::string> paths;
    if (partition_spec.empty()) {
        return paths;
    }
    std::string suffix_buf;
    for (const auto& [key, value] : partition_spec) {
        PAIMON_ASSIGN_OR_RAISE(std::string escaped_key, EscapePathName(key));
        PAIMON_ASSIGN_OR_RAISE(std::string escaped_value, EscapePathName(value));
        suffix_buf.append(escaped_key);
        suffix_buf.append("=");
        suffix_buf.append(escaped_value);
        suffix_buf.append(PATH_SEPARATOR);
        paths.push_back(suffix_buf);
    }
    return paths;
}

}  // namespace paimon
