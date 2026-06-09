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
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "paimon/result.h"

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
    /// @return An escaped, valid partition name.
    static Result<std::string> GeneratePartitionPath(
        const std::vector<std::pair<std::string, std::string>>& partition_spec);

    /// Escapes a path name.
    ///
    /// @param path The path to escape.
    /// @return An escaped path name.
    static Result<std::string> EscapePathName(const std::string& path);

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
