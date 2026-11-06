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

#include <memory>
#include <string>

#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class MemoryPool;

/// `CommitContext` is some configuration for commit operations.
///
/// Please do not use this class directly, use `CommitContextBuilder` to build a `CommitContext`
/// which has input validation.
/// @see CommitContextBuilder
class PAIMON_EXPORT CommitContext {
 public:
    CommitContext(const std::string& commit_user, bool ignore_empty_commit,
                  bool use_rest_catalog_commit, bool append_commit_check_conflict,
                  const std::shared_ptr<MemoryPool>& memory_pool,
                  const std::shared_ptr<Executor>& executor);
    ~CommitContext();

    const std::string& GetCommitUser() const {
        return commit_user_;
    }

    bool IgnoreEmptyCommit() const {
        return ignore_empty_commit_;
    }

    bool UseRESTCatalogCommit() const {
        return use_rest_catalog_commit_;
    }

    bool AppendCommitCheckConflict() const {
        return append_commit_check_conflict_;
    }

    std::shared_ptr<MemoryPool> GetMemoryPool() const {
        return memory_pool_;
    }

    std::shared_ptr<Executor> GetExecutor() const {
        return executor_;
    }

 private:
    std::string commit_user_;
    bool ignore_empty_commit_;
    bool use_rest_catalog_commit_;
    bool append_commit_check_conflict_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
};

/// `CommitContextBuilder` used to build a `CommitContext`, has input validation.
class PAIMON_EXPORT CommitContextBuilder {
 public:
    explicit CommitContextBuilder(const std::string& commit_user);

    ~CommitContextBuilder();

    /// Sets whether to ignore empty commits (default is true).
    /// When set to true, commits that don't contain any actual data changes will be ignored.
    /// @param ignore_empty_commit True to ignore empty commits, false otherwise.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& IgnoreEmptyCommit(bool ignore_empty_commit);

    /// Builds a REST commit request for the caller to send (default is false).
    /// A table loaded through a catalog commits through that catalog; the two modes are exclusive.
    /// @note Temporary interface, will be removed in the future.
    /// @param use_rest_catalog_commit True to build the request without sending it.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& UseRESTCatalogCommit(bool use_rest_catalog_commit);

    /// Sets whether append commits should perform conflict checking (default is false).
    /// @param append_commit_check_conflict True to enable append conflict checks.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& AppendCommitCheckConflict(bool append_commit_check_conflict);

    /// Sets the memory pool to be used for memory allocation during commit operations.
    /// @param memory_pool Shared pointer to the memory pool instance.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& WithMemoryPool(const std::shared_ptr<MemoryPool>& memory_pool);

    /// Sets the executor to be used for asynchronous operations during commit.
    /// @param executor Shared pointer to the executor instance.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& WithExecutor(const std::shared_ptr<Executor>& executor);

    /// Build and return a `CommitContext` instance with input validation.
    /// @return Result containing the constructed `CommitContext` or an error status.
    Result<std::unique_ptr<CommitContext>> Finish();

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
