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

#include "paimon/core/utils/branch_manager.h"

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/catalog/identifier.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"

namespace paimon {

Result<std::string> BranchManager::ResolveBranch(const std::optional<Identifier>& identifier,
                                                 const std::map<std::string, std::string>& options,
                                                 const std::optional<std::string>& explicit_branch,
                                                 const std::string& operation) {
    std::optional<std::string> identifier_branch;
    if (identifier) {
        PAIMON_ASSIGN_OR_RAISE(identifier_branch, identifier.value().GetBranchName());
    }
    auto option_iter = options.find(Options::BRANCH);
    std::optional<std::string> option_branch =
        option_iter == options.end() ? std::nullopt
                                     : std::optional<std::string>(option_iter->second);

    std::optional<std::string> resolved;
    for (const std::optional<std::string>& named_branch :
         {identifier_branch, option_branch, explicit_branch}) {
        if (!named_branch) {
            continue;
        }
        std::string branch = NormalizeBranch(named_branch.value());
        // Checked before the comparison below, so that a name naming no directory of the table
        // is reported as such rather than as one of two branches an operation was aimed at.
        PAIMON_RETURN_NOT_OK(CheckValidBranch(branch));
        if (resolved && branch != resolved.value()) {
            return Status::Invalid(
                fmt::format("a {} is aimed at one branch, but both '{}' and '{}' were named",
                            operation, resolved.value(), branch));
        }
        resolved = std::move(branch);
    }
    return resolved.value_or(DEFAULT_MAIN_BRANCH);
}

Status BranchManager::CheckCatalogAddressableBranch(const std::string& branch) {
    const std::string normalized = NormalizeBranch(branch);
    if (!IsMainBranch(normalized) &&
        StringUtils::EqualsIgnoreCase(normalized, DEFAULT_MAIN_BRANCH)) {
        return Status::Invalid(fmt::format(
            "a catalog names branch '{}' as it names the main branch, while the snapshots of the "
            "two go to directories of their own, so a commit to it through a catalog would publish "
            "them on the main branch",
            normalized));
    }
    if (normalized.find(Identifier::kSystemTableSplitter) != std::string::npos) {
        return Status::Invalid(fmt::format(
            "a catalog names branch '{}' by the object name '<table>{}{}{}', which reads back as "
            "another branch or as a system table of one, so a branch a catalog addresses cannot "
            "contain '{}'",
            normalized, Identifier::kSystemTableSplitter, Identifier::kSystemBranchPrefix,
            normalized, Identifier::kSystemTableSplitter));
    }
    return Status::OK();
}

Result<std::vector<std::string>> BranchManager::ListBranches(const std::shared_ptr<FileSystem>& fs,
                                                             const std::string& table_root) {
    std::vector<std::string> branches = {DEFAULT_MAIN_BRANCH};
    std::string branch_dir = PathUtil::JoinPath(table_root, "branch");
    PAIMON_ASSIGN_OR_RAISE(bool is_exist, fs->Exists(branch_dir));
    if (!is_exist) {
        return branches;
    }

    std::vector<BasicFileStatus> file_status_list;
    PAIMON_RETURN_NOT_OK(fs->ListDir(branch_dir, &file_status_list));
    std::string branch_prefix = BRANCH_PREFIX;
    for (const auto& file_status : file_status_list) {
        if (!file_status.IsDir()) {
            continue;
        }
        std::string dir_name = PathUtil::GetName(file_status.GetPath());
        if (StringUtils::StartsWith(dir_name, branch_prefix, /*start_pos=*/0)) {
            branches.push_back(dir_name.substr(branch_prefix.length()));
        }
    }
    std::sort(branches.begin(), branches.end());
    return branches;
}

}  // namespace paimon
