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
#include <memory>
#include <optional>
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Catalog;
class Executor;
class FormatTable;
class MemoryPool;

/// `CommitContext` is some configuration for commit operations.
///
/// Please do not use this class directly, use `CommitContextBuilder` to build a `CommitContext`
/// which has input validation.
/// @see CommitContextBuilder
class PAIMON_EXPORT CommitContext {
 public:
    /// Retained for source compatibility. Prefer `CommitContextBuilder` for input validation.
    CommitContext(const std::string& root_path, const std::string& commit_user,
                  bool ignore_empty_commit, bool use_rest_catalog_commit,
                  bool append_commit_check_conflict, const std::shared_ptr<MemoryPool>& memory_pool,
                  const std::shared_ptr<Executor>& executor,
                  const std::shared_ptr<FileSystem>& specific_file_system,
                  const std::map<std::string, std::string>& options,
                  const std::shared_ptr<FormatTable>& format_table);

    CommitContext(const std::string& root_path, const std::string& commit_user,
                  bool ignore_empty_commit, bool use_rest_catalog_commit,
                  const std::shared_ptr<Catalog>& catalog,
                  const std::optional<Identifier>& identifier,
                  const std::optional<std::string>& table_id, bool append_commit_check_conflict,
                  const std::shared_ptr<MemoryPool>& memory_pool,
                  const std::shared_ptr<Executor>& executor,
                  const std::shared_ptr<FileSystem>& specific_file_system,
                  const std::map<std::string, std::string>& options,
                  const std::shared_ptr<FormatTable>& format_table);
    ~CommitContext();

    const std::string& GetRootPath() const {
        return root_path_;
    }

    const std::string& GetCommitUser() const {
        return commit_user_;
    }

    bool IgnoreEmptyCommit() const {
        return ignore_empty_commit_;
    }

    bool UseRESTCatalogCommit() const {
        return use_rest_catalog_commit_;
    }

    /// Returns the configured catalog, or null if unset.
    const std::shared_ptr<Catalog>& GetCatalog() const {
        return catalog_;
    }

    /// Returns the table identifier supplied with the catalog.
    const std::optional<Identifier>& GetIdentifier() const {
        return identifier_;
    }

    /// Returns the catalog table UUID sent as `tableId` in commit requests.
    const std::optional<std::string>& GetTableId() const {
        return table_id_;
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

    std::shared_ptr<FileSystem> GetSpecificFileSystem() const {
        return specific_file_system_;
    }

    const std::map<std::string, std::string>& GetOptions() const {
        return options_;
    }

    /// The format table this context was built from, or null when it names a table path and the
    /// schema under that path says what kind of table it is.
    const std::shared_ptr<FormatTable>& GetFormatTable() const {
        return format_table_;
    }

 private:
    std::string root_path_;
    std::string commit_user_;
    bool ignore_empty_commit_;
    bool use_rest_catalog_commit_;
    std::shared_ptr<Catalog> catalog_;
    std::optional<Identifier> identifier_;
    std::optional<std::string> table_id_;
    bool append_commit_check_conflict_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<FileSystem> specific_file_system_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<FormatTable> format_table_;
};

/// `CommitContextBuilder` used to build a `CommitContext`, has input validation.
class PAIMON_EXPORT CommitContextBuilder {
 public:
    /// Constructs a `CommitContextBuilder` with required parameters.
    /// @param root_path The root path of the Paimon table.
    /// @param commit_user The user identifier for the commit operation.
    CommitContextBuilder(const std::string& root_path, const std::string& commit_user);

    /// Constructs a `CommitContextBuilder` for a format table that is already loaded: the only way
    /// to commit to one whose schema lives in a metastore rather than under its location, such as
    /// a table a REST catalog serves. The table carries what such a location does not say, so
    /// `WithFileSystem()` is refused here rather than ignored.
    ///
    /// There is no commit user: a format table keeps no snapshot to record one in.
    ///
    /// @param table The format table to commit to, as `Catalog::GetFormatTable()` hands it back.
    explicit CommitContextBuilder(const std::shared_ptr<FormatTable>& table);

    ~CommitContextBuilder();

    /// Set a configuration options map to set some option entries which are not defined in the
    /// table schema or whose values you want to overwrite.
    /// @note The options map will clear the options added by `AddOption()` before.
    /// @param options The configuration options map.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& SetOptions(const std::map<std::string, std::string>& options);

    /// Add a single configuration option which is not defined in the table schema or whose value
    /// you want to overwrite.
    ///
    /// If you want to add multiple options, call `AddOption()` multiple times or use `SetOptions()`
    /// instead.
    /// @param key The option key.
    /// @param value The option value.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& AddOption(const std::string& key, const std::string& value);

    /// Sets whether to ignore empty commits (default is true).
    /// When set to true, commits that don't contain any actual data changes will be ignored.
    /// @param ignore_empty_commit True to ignore empty commits, false otherwise.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& IgnoreEmptyCommit(bool ignore_empty_commit);

    /// Builds a REST commit request for the caller to send (default is false).
    /// Use `WithCatalog()` for catalog-managed commits; the two modes are mutually exclusive.
    /// @note Temporary interface, will be removed in the future.
    /// @param use_rest_catalog_commit True to build the request without sending it.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& UseRESTCatalogCommit(bool use_rest_catalog_commit);

    /// Loads the current schema from `catalog`. Catalogs with version management also load and
    /// commit snapshots, rebasing on conflicts. Other catalogs use file-system snapshot commits.
    /// Mutually exclusive with `UseRESTCatalogCommit()`.
    ///
    /// Data and manifests remain on the file system. Recovery, rollback and row-id checks require
    /// historical snapshots there; row-id checks also require the inspected data files' schemas.
    /// `Expire()` manages file-system snapshots after confirming that the catalog's current
    /// snapshot and retained history are published there. Configure writers with the same catalog
    /// via `WriteContextBuilder::WithCatalog()`.
    /// A branch is addressed by the identifier, as `tbl$branch_dev`, so that the catalog answers
    /// for that branch. Its schema is read from the branch directory rather than from the catalog,
    /// just as a read of that branch reads it. Configure the writer with the same identifier.
    /// @param catalog Non-null catalog, kept alive by this context.
    /// @param identifier Table to commit to, naming a branch of it as `tbl$branch_dev`.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& WithCatalog(const std::shared_ptr<Catalog>& catalog,
                                      const Identifier& identifier);

    /// Sets the catalog table UUID sent as `tableId` to detect table recreation (default is unset).
    ///
    /// Capture `Table::CatalogUuid()` before preparing changes. `Table::Uuid()` may fall back to
    /// the table name. An unset ID is sent as null and validated by the server.
    /// Requires `WithCatalog()` or `UseRESTCatalogCommit()`.
    /// @param table_id Catalog UUID of the table to commit to.
    /// @return Reference to this builder for method chaining.
    CommitContextBuilder& WithTableId(const std::string& table_id);

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

    /// Sets a custom file system instance to be used for all file operations in this commit
    /// context.
    /// This bypasses the global file system registry and uses the provided implementation directly.
    ///
    /// @param file_system The file system to use.
    /// @return Reference to this builder for method chaining.
    /// @note If not set, use default file system (configured in `Options::FILE_SYSTEM`)
    CommitContextBuilder& WithFileSystem(const std::shared_ptr<FileSystem>& file_system);

    /// Build and return a `CommitContext` instance with input validation.
    /// @return Result containing the constructed `CommitContext` or an error status.
    Result<std::unique_ptr<CommitContext>> Finish();

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
