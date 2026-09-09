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

#pragma once

#include <array>
#include <bitset>
#include <cstddef>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

// Utils for file system.
class PartitionPathUtils {
 public:
    static constexpr char PATH_SEPARATOR[] = "/";

    PartitionPathUtils() = delete;
    ~PartitionPathUtils() = delete;
    /// Make partition path from partition spec.
    ///
    /// @param partition_spec The partition spec.
    /// @param only_value Name each level by its escaped value alone (`2025/01/`) instead of
    ///        `key=value` (`year=2025/month=01/`). That is the layout a format table takes when
    ///        `format-table.partition-path-only-value` is on.
    /// @return An escaped, valid partition name.
    static Result<std::string> GeneratePartitionPath(
        const std::vector<std::pair<std::string, std::string>>& partition_spec,
        bool only_value = false);

    /// Escapes a path name.
    ///
    /// @param path The path to escape.
    /// @return An escaped path name.
    static Result<std::string> EscapePathName(const std::string& path);

    /// Reverses `EscapePathName`, turning every `%XX` sequence back into the character it stands
    /// for. A `%` that does not start a valid sequence is kept as written.
    static std::string UnescapePathName(const std::string& path);

    /// Splits a `key=value` partition directory name into its unescaped key and value.
    ///
    /// Returns nullopt when the name is not of that shape, which is how a directory that is not a
    /// partition of this table is told apart from one that is.
    ///
    /// A name carrying a second unescaped `=` is not of that shape: `EscapePathName` escapes
    /// `=`, so no directory paimon wrote looks like that, and one written by something else is
    /// skipped rather than bound to a key it does not name.
    static std::optional<std::pair<std::string, std::string>> ExtractPartitionKeyValue(
        const std::string& directory_name);

    /// Whether a path component is hidden by the `_` / `.` convention every engine writing a
    /// Hive-style directory uses to mark output that is not committed table data.
    static bool IsHiddenName(const std::string& name) {
        return !name.empty() && (name[0] == '_' || name[0] == '.');
    }

    /// Fails when `value` cannot name a partition directory.
    ///
    /// A value-only directory is the bare value, so "." and ".." would name the directory itself
    /// and its parent instead of a partition; under `key=value` the "=" already keeps them apart
    /// from a relative path.
    static Status ValidatePartitionValueForPath(const std::string& value, bool only_value);

    /// Generate all hierarchical paths from partition spec.
    ///
    /// For example, if the partition spec is (pt1: '0601', pt2: '12', pt3: '30'), this method
    /// will return a list (start from index 0):
    ///
    /// <ul>
    /// <li>pt1=0601
    /// <li>pt1=0601/pt2=12
    /// <li>pt1=0601/pt2=12/pt3=30
    /// </ul>
    static Result<std::vector<std::string>> GenerateHierarchicalPartitionPaths(
        const std::vector<std::pair<std::string, std::string>>& partition_spec);

 private:
    static const std::bitset<128>& CharToEscape();
    static bool NeedsEscaping(char c) {
        return static_cast<size_t>(c) < CharToEscape().size() && CharToEscape().test(c);
    }

    static void EscapeChar(char c, std::stringstream* ss_ptr);
};

}  // namespace paimon
