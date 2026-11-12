/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/catalog/table.h"
#include "paimon/core/operation/commit/commit_scanner.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace paimon {

class InternalReadContext;
class IOManager;
class ManifestFile;
class ManifestList;
class RealtimeContext;
class SplitGenerator;

/// Managed table backed by snapshots and manifests.
/// Shares operator setup across append-only and primary-key tables, selected from the schema.
class FileStoreTable : public Table {
 public:
    /// Creates an append-only or primary-key table according to the schema's primary keys.
    /// @param specified_file_system Explicit file system, overriding option-based resolution.
    /// @param schema Loaded table schema; must not be null.
    /// @param branch Branch used by all operators and metadata managers.
    /// @param fs_scheme_map URI scheme routing for option-based file system resolution.
    static Result<std::shared_ptr<FileStoreTable>> Create(
        const std::shared_ptr<FileSystem>& specified_file_system, const std::string& root_path,
        const std::shared_ptr<TableSchema>& schema, const Identifier& identifier,
        const std::string& uuid, const std::string& branch,
        const std::map<std::string, std::string>& fs_scheme_map = {});

    /// Binds a resolved store without replacing the loaded schema's options.
    static Result<std::shared_ptr<FileStoreTable>> Create(
        std::unique_ptr<FileStore> store, const std::shared_ptr<TableSchema>& schema,
        const Identifier& identifier, const std::string& uuid,
        const std::map<std::string, std::string>& fs_scheme_map = {});

    std::string CurrentBranch() const {
        auto branch = options_.find(Options::BRANCH);
        return branch == options_.end() ? BranchManager::DEFAULT_MAIN_BRANCH
                                        : BranchManager::NormalizeBranch(branch->second);
    }

    std::shared_ptr<SchemaManager> GetSchemaManager() const {
        return std::make_shared<SchemaManager>(file_system_, root_path_, CurrentBranch());
    }

    std::shared_ptr<SnapshotManager> GetSnapshotManager() const {
        return std::make_shared<SnapshotManager>(file_system_, root_path_, CurrentBranch());
    }

    std::string Name() const override {
        return identifier_.GetObjectName();
    }

    std::string FullName() const override {
        return identifier_.GetFullName();
    }

    Result<std::unique_ptr<TableScan>> NewScan(std::unique_ptr<ScanContext> context) const override;

    Result<std::unique_ptr<TableRead>> NewRead(std::unique_ptr<ReadContext> context) const override;

    Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const override;

    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const override;

 private:
    Result<std::unique_ptr<TableScan>> NewScan(const std::shared_ptr<ScanContext>& context,
                                               bool query_auth_applied) const;

    Result<std::unique_ptr<TableRead>> NewRead(const std::shared_ptr<ReadContext>& context,
                                               bool query_auth_applied) const;

    static Result<std::shared_ptr<FileStoreTable>> CreateWithOptions(
        const std::shared_ptr<FileSystem>& specified_file_system, const std::string& root_path,
        const std::shared_ptr<TableSchema>& schema, const Identifier& identifier,
        const std::string& uuid, const std::map<std::string, std::string>& options,
        const std::map<std::string, std::string>& fs_scheme_map);

    Result<CoreOptions> ResolveOptions(const std::map<std::string, std::string>& options) const;
    /// Rejects paths, identifiers or UUIDs that contradict this table's identity.
    /// Missing context fields are allowed.
    /// @param kind Operation name used in errors.
    /// @param context_table_id Catalog commit UUID; nullopt for writes.
    Status CheckContextNamesThisTable(const std::string& kind, const std::string& context_root_path,
                                      const std::optional<Identifier>& context_identifier,
                                      const std::optional<std::string>& context_table_id) const;

    /// Creates a reader for the internal context's schema and primary keys.
    Result<std::unique_ptr<TableRead>> CreateTableRead(
        const std::shared_ptr<InternalReadContext>& internal_context,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::shared_ptr<Executor>& executor) const;

 protected:
    FileStoreTable(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
                   const std::shared_ptr<TableSchema>& schema, const Identifier& identifier,
                   const std::string& uuid, const std::map<std::string, std::string>& options);

    /// Restores real-time write progress from the latest snapshot.
    static Status RestoreRealtimeCommittedProgress(
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::shared_ptr<SnapshotManager>& snapshot_manager, const CoreOptions& options);

    static std::unique_ptr<FileStoreScan> WithTablePath(std::unique_ptr<FileStoreScan>&& scan,
                                                        const std::string& root_path);

    /// Creates the table-specific writer.
    /// @param io_manager Optional spill manager.
    /// @param ignore_previous_files Whether to start over instead of appending.
    virtual Result<std::unique_ptr<FileStoreWrite>> CreateWrite(
        const WriteContext& ctx, const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& options,
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<IOManager>& io_manager, bool ignore_previous_files) const = 0;

    virtual Result<std::unique_ptr<FileStoreScan>> NewFileStoreScan(
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& core_options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& memory_pool,
        const ScanContext* context) const = 0;

    virtual Result<std::unique_ptr<SplitGenerator>> NewSplitGenerator(
        const CoreOptions& core_options) const = 0;

    virtual CommitScanner::ScanSupplier NewCommitScanSupplier(
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
        const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool) const = 0;

    const std::map<std::string, std::string> options_;

    /// URI routing supplied when this table was loaded.
    std::map<std::string, std::string> fs_scheme_map_;
};

}  // namespace paimon
