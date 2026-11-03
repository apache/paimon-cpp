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
#include <optional>
#include <string>
#include <vector>

#include "paimon/catalog/identifier.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class BinaryRowPartitionComputer;
class Catalog;
class IndexFileHandler;
class IndexManifestFile;
class ManifestFile;
class ManifestList;

/// Storage state and factories for managed-table operators.
/// Operator factories create fresh instances so catalog snapshot loaders are not shared
/// between scans, writes and commits.
class FileStore {
 public:
    /// Builds storage from already resolved CoreOptions.
    /// @param root_path Table root path.
    /// @param core_options Effective options, including the file system and branch.
    static std::shared_ptr<FileStore> Create(const std::string& root_path,
                                             CoreOptions core_options);

    const std::string& CurrentBranch() const {
        return branch_;
    }

    const std::shared_ptr<SchemaManager>& GetSchemaManager() const {
        return schema_manager_;
    }

    const std::shared_ptr<SnapshotManager>& GetSnapshotManager() const {
        return snapshot_manager_;
    }

    const std::shared_ptr<FileSystem>& GetFileSystem() const {
        return file_system_;
    }

    const std::string& GetRootPath() const {
        return root_path_;
    }

    const CoreOptions& GetOptions() const {
        return core_options_;
    }

    /// Uses the caller's partition keys and format identifier, which may differ for fallback
    /// reads or per-level writes.
    Result<std::shared_ptr<FileStorePathFactory>> NewPathFactory(
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::vector<std::string>& partition_keys, const CoreOptions& options,
        const std::string& file_format_identifier,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// Creates an operator-local snapshot manager with an optional catalog loader.
    /// Version-managed catalogs require catalog_identifier.
    Result<std::shared_ptr<SnapshotManager>> NewSnapshotManager(
        const CoreOptions& options, const std::shared_ptr<Catalog>& catalog,
        const std::optional<Identifier>& catalog_identifier) const;

    Result<std::shared_ptr<ManifestList>> NewManifestList(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    Result<std::shared_ptr<ManifestFile>> NewManifestFile(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<arrow::Schema>& partition_schema,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    Result<std::unique_ptr<IndexManifestFile>> NewIndexManifestFile(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    Result<std::unique_ptr<IndexFileHandler>> NewIndexFileHandler(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    Result<std::unique_ptr<BinaryRowPartitionComputer>> NewPartitionComputer(
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::vector<std::string>& partition_keys, const CoreOptions& options,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

 private:
    FileStore(const std::string& root_path, CoreOptions core_options);

    std::shared_ptr<FileSystem> file_system_;
    std::string root_path_;
    std::string branch_;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    CoreOptions core_options_;
};

}  // namespace paimon
