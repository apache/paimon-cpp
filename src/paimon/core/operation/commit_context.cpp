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

#include <string>
#include <utility>

#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

CommitContext::CommitContext(const std::string& commit_user, bool ignore_empty_commit,
                             bool use_rest_catalog_commit, bool append_commit_check_conflict,
                             const std::shared_ptr<MemoryPool>& memory_pool,
                             const std::shared_ptr<Executor>& executor)
    : commit_user_(commit_user),
      ignore_empty_commit_(ignore_empty_commit),
      use_rest_catalog_commit_(use_rest_catalog_commit),
      append_commit_check_conflict_(append_commit_check_conflict),
      memory_pool_(memory_pool),
      executor_(executor) {}

CommitContext::~CommitContext() = default;

class CommitContextBuilder::Impl {
 public:
    friend class CommitContextBuilder;

    void Reset() {
        ignore_empty_commit_ = true;
        use_rest_catalog_commit_ = false;
        append_commit_check_conflict_ = false;
        memory_pool_ = GetDefaultPool();
        executor_ = CreateDefaultExecutor();
    }

 private:
    std::string commit_user_;
    bool ignore_empty_commit_ = true;
    bool use_rest_catalog_commit_ = false;
    bool append_commit_check_conflict_ = false;
    std::shared_ptr<MemoryPool> memory_pool_ = GetDefaultPool();
    std::shared_ptr<Executor> executor_ = CreateDefaultExecutor();
};

CommitContextBuilder::CommitContextBuilder(const std::string& commit_user)
    : impl_(std::make_unique<Impl>()) {
    impl_->commit_user_ = commit_user;
}

CommitContextBuilder::~CommitContextBuilder() = default;

CommitContextBuilder& CommitContextBuilder::IgnoreEmptyCommit(bool ignore_empty_commit) {
    impl_->ignore_empty_commit_ = ignore_empty_commit;
    return *this;
}

CommitContextBuilder& CommitContextBuilder::UseRESTCatalogCommit(bool use_rest_catalog_commit) {
    impl_->use_rest_catalog_commit_ = use_rest_catalog_commit;
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

Result<std::unique_ptr<CommitContext>> CommitContextBuilder::Finish() {
    auto ctx = std::make_unique<CommitContext>(
        impl_->commit_user_, impl_->ignore_empty_commit_, impl_->use_rest_catalog_commit_,
        impl_->append_commit_check_conflict_, impl_->memory_pool_, impl_->executor_);
    impl_->Reset();
    return ctx;
}

}  // namespace paimon
