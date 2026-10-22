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

#include "paimon/catalog/identifier.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace paimon {

class CoreOptions;
class FileStoreScan;
class IOManager;
class SplitGenerator;
class TableSchema;

/// The managed table a schema with primary keys describes, Java Paimon's
/// `PrimaryKeyFileStoreTable`. Its write merges by key, and its scan, read and commit read
/// through the merge. The operator flows are the ones `FileStoreTable` carries; the pieces
/// those flows hand down are built here.
class PrimaryKeyFileStoreTable : public FileStoreTable {
 public:
    /// The constructor `FileStoreTable::Create` builds a primary key table through; the
    /// parameters are the ones it takes, and the base class carries them.
    PrimaryKeyFileStoreTable(const std::shared_ptr<FileSystem>& file_system,
                             const std::string& root_path, const std::shared_ptr<Schema>& schema,
                             const Identifier& identifier, const std::string& uuid = "",
                             const std::string& branch = BranchManager::DEFAULT_MAIN_BRANCH,
                             const std::shared_ptr<SchemaManager>& schema_manager = nullptr,
                             const std::shared_ptr<SnapshotManager>& snapshot_manager = nullptr);

 private:
    Result<std::unique_ptr<FileStoreWrite>> CreateWrite(
        const WriteContext& ctx, const std::shared_ptr<TableSchema>& schema,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& options,
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<IOManager>& io_manager, bool ignore_previous_files) const override;

    Result<std::unique_ptr<FileStoreScan>> NewFileStoreScan(
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<TableSchema>& table_schema, const CoreOptions& core_options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& memory_pool,
        const ScanContext* context) const override;

    Result<std::unique_ptr<SplitGenerator>> NewSplitGenerator(
        const std::shared_ptr<TableSchema>& table_schema,
        const CoreOptions& core_options) const override;

    Result<std::unique_ptr<TableRead>> NewTableRead(
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<InternalReadContext>& internal_context,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::shared_ptr<Executor>& executor) const override;

    CommitScanner::ScanSupplier NewCommitScanSupplier(
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
        const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool) const override;
};

}  // namespace paimon
