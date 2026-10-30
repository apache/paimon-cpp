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

#include "paimon/commit_context.h"

#include <optional>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

CommitContext::CommitContext(const std::string& root_path, const std::string& commit_user,
                             bool ignore_empty_commit, bool use_rest_catalog_commit,
                             bool append_commit_check_conflict,
                             const std::shared_ptr<MemoryPool>& memory_pool,
                             const std::shared_ptr<Executor>& executor,
                             const std::shared_ptr<FileSystem>& specific_file_system,
                             const std::map<std::string, std::string>& options)
    : CommitContext(root_path, commit_user, ignore_empty_commit, use_rest_catalog_commit,
                    /*catalog=*/nullptr, /*identifier=*/std::nullopt, /*table_id=*/std::nullopt,
                    append_commit_check_conflict, memory_pool, executor, specific_file_system,
                    options,
                    // Without an identifier the option is the only name a branch can have.
                    BranchManager::NormalizeBranch(
                        options.count(Options::BRANCH) ? options.at(Options::BRANCH) : ""),
                    /*table_schema=*/std::nullopt, /*fs_scheme_to_identifier_map=*/{}) {}

CommitContext::CommitContext(
    const std::string& root_path, const std::string& commit_user, bool ignore_empty_commit,
    bool use_rest_catalog_commit, const std::shared_ptr<Catalog>& catalog,
    const std::optional<Identifier>& identifier, const std::optional<std::string>& table_id,
    bool append_commit_check_conflict, const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<FileSystem>& specific_file_system,
    const std::map<std::string, std::string>& options, const std::string& branch,
    const std::optional<std::string>& table_schema,
    const std::map<std::string, std::string>& fs_scheme_to_identifier_map)
    : root_path_(root_path),
      branch_(branch),
      commit_user_(commit_user),
      ignore_empty_commit_(ignore_empty_commit),
      use_rest_catalog_commit_(use_rest_catalog_commit),
      catalog_(catalog),
      identifier_(identifier),
      table_id_(table_id),
      append_commit_check_conflict_(append_commit_check_conflict),
      memory_pool_(memory_pool),
      executor_(executor),
      specific_file_system_(specific_file_system),
      fs_scheme_to_identifier_map_(fs_scheme_to_identifier_map),
      options_(options),
      table_schema_(table_schema) {}

CommitContext::~CommitContext() = default;

class CommitContextBuilder::Impl {
 public:
    friend class CommitContextBuilder;

    void Reset() {
        ignore_empty_commit_ = true;
        use_rest_catalog_commit_ = false;
        catalog_.reset();
        identifier_.reset();
        table_id_.reset();
        append_commit_check_conflict_ = false;
        memory_pool_ = GetDefaultPool();
        executor_ = CreateDefaultExecutor();
        specific_file_system_.reset();
        fs_scheme_to_identifier_map_.clear();
        table_schema_ = std::nullopt;
        options_.clear();
    }

 private:
    std::string root_path_;
    std::string commit_user_;
    bool ignore_empty_commit_ = true;
    bool use_rest_catalog_commit_ = false;
    std::shared_ptr<Catalog> catalog_;
    std::optional<Identifier> identifier_;
    std::optional<std::string> table_id_;
    bool append_commit_check_conflict_ = false;
    std::shared_ptr<MemoryPool> memory_pool_ = GetDefaultPool();
    std::shared_ptr<Executor> executor_ = CreateDefaultExecutor();
    std::shared_ptr<FileSystem> specific_file_system_;
    std::map<std::string, std::string> fs_scheme_to_identifier_map_;
    std::optional<std::string> table_schema_;
    std::map<std::string, std::string> options_;
};

CommitContextBuilder::CommitContextBuilder(const std::string& root_path,
                                           const std::string& commit_user)
    : impl_(std::make_unique<Impl>()) {
    impl_->root_path_ = root_path;
    impl_->commit_user_ = commit_user;
}

CommitContextBuilder::~CommitContextBuilder() = default;

CommitContextBuilder& CommitContextBuilder::AddOption(const std::string& key,
                                                      const std::string& value) {
    impl_->options_[key] = value;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::SetOptions(
    const std::map<std::string, std::string>& opts) {
    impl_->options_ = opts;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::IgnoreEmptyCommit(bool ignore_empty_commit) {
    impl_->ignore_empty_commit_ = ignore_empty_commit;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::UseRESTCatalogCommit(bool use_rest_catalog_commit) {
    impl_->use_rest_catalog_commit_ = use_rest_catalog_commit;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::WithCatalog(const std::shared_ptr<Catalog>& catalog,
                                                        const Identifier& identifier) {
    impl_->catalog_ = catalog;
    impl_->identifier_.emplace(identifier);
    return *this;
}

CommitContextBuilder& CommitContextBuilder::WithTableId(const std::string& table_id) {
    impl_->table_id_ = table_id;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::AppendCommitCheckConflict(
    bool append_commit_check_conflict) {
    impl_->append_commit_check_conflict_ = append_commit_check_conflict;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::WithMemoryPool(
    const std::shared_ptr<MemoryPool>& memory_pool) {
    impl_->memory_pool_ = memory_pool;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::WithExecutor(
    const std::shared_ptr<Executor>& executor) {
    impl_->executor_ = executor;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::WithFileSystem(
    const std::shared_ptr<FileSystem>& file_system) {
    impl_->specific_file_system_ = file_system;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::WithFileSystemSchemeToIdentifierMap(
    const std::map<std::string, std::string>& fs_scheme_to_identifier_map) {
    impl_->fs_scheme_to_identifier_map_ = fs_scheme_to_identifier_map;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::SetTableSchema(const std::string& table_schema) {
    impl_->table_schema_ = table_schema;
    return *this;
}

Result<std::unique_ptr<CommitContext>> CommitContextBuilder::Finish() {
    if (impl_->catalog_ == nullptr && impl_->identifier_) {
        return Status::Invalid("cannot commit through a null catalog");
    }
    if (impl_->specific_file_system_ && !impl_->fs_scheme_to_identifier_map_.empty()) {
        return Status::Invalid(
            "WithFileSystem() and WithFileSystemSchemeToIdentifierMap() cannot be used together");
    }
    if (impl_->identifier_) {
        // Before the branch is read out of the identifier: the branch of `tbl$branch_dev$options`
        // reads back as `dev`, so a commit built for such a name would be aimed at another branch.
        PAIMON_RETURN_NOT_OK(
            CatalogUtils::CheckNotSystemTable(impl_->identifier_.value(), "commit"));
    }
    PAIMON_ASSIGN_OR_RAISE(std::string branch, BranchManager::ResolveBranch(
                                                   impl_->identifier_, impl_->options_,
                                                   /*explicit_branch=*/std::nullopt, "commit"));
    if (!BranchManager::IsMainBranch(branch)) {
        // A commit which goes to a catalog, or is handed back as a request for one, names its
        // branch by an object name, so the branch has to be one that reads back as itself.
        // Refused here rather than at the commit, which writes metadata before publishing it.
        if (impl_->catalog_ != nullptr || impl_->use_rest_catalog_commit_) {
            PAIMON_RETURN_NOT_OK(BranchManager::CheckCatalogAddressableBranch(branch));
        }
        if (impl_->catalog_ != nullptr) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> identifier_branch,
                                   impl_->identifier_.value().GetBranchName());
            if (!identifier_branch) {
                PAIMON_ASSIGN_OR_RAISE(std::string table_name,
                                       impl_->identifier_.value().GetDataTableName());
                return Status::Invalid(fmt::format(
                    "a commit through a catalog addresses a branch by the table identifier, so "
                    "name branch '{}' there as '{}$branch_{}' rather than only in the '{}' option",
                    branch, table_name, branch, Options::BRANCH));
            }
        }
    }
    if (impl_->catalog_ != nullptr && impl_->use_rest_catalog_commit_) {
        return Status::Invalid(
            "a commit either goes to the catalog of WithCatalog() or is handed back as a request "
            "for the caller to send, which is what UseRESTCatalogCommit() asks for");
    }
    if (impl_->catalog_ != nullptr && impl_->specific_file_system_ == nullptr &&
        impl_->fs_scheme_to_identifier_map_.empty()) {
        // A catalog issuing per-table temporary credentials only hands them out through
        // GetTableFileSystem, so the commit uses the credentials of this table. A scheme map
        // routes by scheme instead, so the catalog is not asked when one is set.
        PAIMON_ASSIGN_OR_RAISE(impl_->specific_file_system_,
                               impl_->catalog_->GetTableFileSystem(impl_->identifier_.value()));
    }
    PAIMON_ASSIGN_OR_RAISE(impl_->root_path_, PathUtil::NormalizePath(impl_->root_path_));
    if (impl_->root_path_.empty()) {
        return Status::Invalid("root path is empty");
    }
    auto ctx = std::make_unique<CommitContext>(
        impl_->root_path_, impl_->commit_user_, impl_->ignore_empty_commit_,
        impl_->use_rest_catalog_commit_, impl_->catalog_, impl_->identifier_, impl_->table_id_,
        impl_->append_commit_check_conflict_, impl_->memory_pool_, impl_->executor_,
        impl_->specific_file_system_, impl_->options_, branch, impl_->table_schema_,
        impl_->fs_scheme_to_identifier_map_);
    impl_->Reset();
    return ctx;
}

}  // namespace paimon
