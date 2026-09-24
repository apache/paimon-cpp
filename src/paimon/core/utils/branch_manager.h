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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class FileSystem;
}  // namespace paimon

namespace paimon {
/// Utility methods for table branch paths and branch discovery.
class BranchManager {
 public:
    BranchManager() = delete;
    ~BranchManager() = delete;

    static constexpr char DEFAULT_MAIN_BRANCH[] = "main";
    static constexpr char BRANCH_PREFIX[] = "branch-";

    /// Normalizes an empty or blank branch name to `main`.
    static std::string NormalizeBranch(const std::string& branch) {
        return StringUtils::IsNullOrWhitespaceOnly(branch) ? DEFAULT_MAIN_BRANCH : branch;
    }

    /// Fails when `branch` cannot be used as a single path component, which is required to keep
    /// the branch path under the table root. A branch that `NormalizeBranch` maps to `main`
    /// names no directory of its own and is therefore accepted.
    static Status CheckValidBranch(const std::string& branch) {
        if (StringUtils::IsNullOrWhitespaceOnly(branch)) {
            return Status::OK();
        }
        return PathUtil::CheckSinglePathComponent("branch", branch);
    }

    /// Returns the table root path for the selected branch. A branch that `NormalizeBranch` maps
    /// to `main` resolves to the table root, so that a caller passing a raw option value cannot
    /// end up with a directory of its own.
    static std::string BranchPath(const std::string& table_root, const std::string& branch) {
        const std::string normalized = NormalizeBranch(branch);
        if (IsMainBranch(normalized)) {
            return table_root;
        }
        return PathUtil::JoinPath(table_root, "/branch/" + std::string(BRANCH_PREFIX) + normalized);
    }

    /// Returns whether the branch is the default main branch.
    static bool IsMainBranch(const std::string& branch) {
        return branch == DEFAULT_MAIN_BRANCH;
    }

    /// Returns the branch an operation is aimed at, which the table identifier, the `branch`
    /// option and `explicit_branch` may each name.
    ///
    /// An absent name and a name which `NormalizeBranch` maps to `main` both mean the main
    /// branch. Names which disagree are refused rather than silently chosen between, with
    /// `operation` naming the operation in that error. Every name is put through
    /// `CheckValidBranch` before it is compared, so neither the branch this returns nor a
    /// branch it names in an error is one that would leave the table root.
    static Result<std::string> ResolveBranch(const std::optional<Identifier>& identifier,
                                             const std::map<std::string, std::string>& options,
                                             const std::optional<std::string>& explicit_branch,
                                             const std::string& operation);

    /// Fails when the branch object name `Identifier` builds, as `tbl$branch_dev`, does not read
    /// back as `branch`, which is how a catalog is told which branch a commit is aimed at.
    ///
    /// That object name reads as the bare table for every spelling of `main`, while `BranchPath`
    /// keeps a directory of its own for every spelling but `main`; and a branch holding a `$`
    /// reads back as another branch, or as a system table of one. A branch this refuses can still
    /// be committed to by writing the snapshot to its directory, which names no table.
    static Status CheckCatalogAddressableBranch(const std::string& branch);

    /// Lists all branches for a table, including `main`.
    static Result<std::vector<std::string>> ListBranches(const std::shared_ptr<FileSystem>& fs,
                                                         const std::string& table_root);
};
}  // namespace paimon
