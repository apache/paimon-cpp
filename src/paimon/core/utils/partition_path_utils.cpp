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

Result<std::string> PartitionPathUtils::GeneratePartitionPath(
    const std::vector<std::pair<std::string, std::string>>& partition_spec) {
    if (partition_spec.empty()) {
        return std::string();
    }
    std::stringstream ss;
    int32_t i = 0;
    for (const auto& [key, value] : partition_spec) {
        if (i > 0) {
            ss << PATH_SEPARATOR;
        }
        PAIMON_ASSIGN_OR_RAISE(std::string key_esc, EscapePathName(key));
        PAIMON_ASSIGN_OR_RAISE(std::string value_esc, EscapePathName(value));
        ss << key_esc << "=" << value_esc;
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
