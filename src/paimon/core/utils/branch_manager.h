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
#include <string>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"

namespace paimon {
class BranchManager {
 public:
    BranchManager() = delete;
    ~BranchManager() = delete;

    static constexpr char DEFAULT_MAIN_BRANCH[] = "main";
    static constexpr char BRANCH_PREFIX[] = "branch-";

    static std::string NormalizeBranch(const std::string& branch) {
        return StringUtils::IsNullOrWhitespaceOnly(branch) ? DEFAULT_MAIN_BRANCH : branch;
    }

    static std::string BranchPath(const std::string& table_root, const std::string& branch) {
        return IsMainBranch(branch)
                   ? table_root
                   : PathUtil::JoinPath(table_root,
                                        "/branch/" + std::string(BRANCH_PREFIX) + branch);
    }

    static bool IsMainBranch(const std::string& branch) {
        return branch == DEFAULT_MAIN_BRANCH;
    }
};
}  // namespace paimon
