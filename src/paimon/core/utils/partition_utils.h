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

#include <map>
#include <string>

#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/result.h"

namespace paimon {

class PartitionUtils {
 public:
    PartitionUtils() = delete;
    ~PartitionUtils() = delete;

    static Result<bool> MatchPartitionSpec(const std::map<std::string, std::string>& partition,
                                           const std::map<std::string, std::string>& partition_spec,
                                           const BinaryRowPartitionComputer& partition_computer) {
        for (const auto& entry : partition_spec) {
            if (partition.find(entry.first) == partition.end()) {
                return false;
            }
        }
        // Dynamic overwrite already supplies canonical partition values. Avoid trying to parse
        // legacy DATE names such as "19723" as user-facing DATE literals again.
        if (MatchNormalizedPartitionSpec(partition, partition_spec)) {
            return true;
        }
        std::map<std::string, std::string> normalized_partition_spec;
        PAIMON_ASSIGN_OR_RAISE(normalized_partition_spec,
                               partition_computer.NormalizePartitionSpec(partition_spec));
        return MatchNormalizedPartitionSpec(partition, normalized_partition_spec);
    }

    static bool MatchNormalizedPartitionSpec(
        const std::map<std::string, std::string>& partition,
        const std::map<std::string, std::string>& normalized_partition_spec) {
        for (const auto& [key, value] : normalized_partition_spec) {
            auto iter = partition.find(key);
            if (iter == partition.end() || iter->second != value) {
                return false;
            }
        }
        return true;
    }
};

}  // namespace paimon
