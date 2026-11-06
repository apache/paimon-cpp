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
#include "paimon/core/table/bucket_mode.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class BinaryRowPartitionComputer;
class Catalog;
class FileStorePathFactory;
class IndexFileHandler;
class IndexManifestFile;
class ManifestFile;
class ManifestList;
class SnapshotManager;
class SchemaManager;

/// Managed-table storage configuration and component factories using resolved options.
class FileStore {
 public:
    FileStore(std::string root_path, CoreOptions options);

    const CoreOptions& GetOptions() const {
        return options_;
    }
    const std::string& GetRootPath() const {
        return root_path_;
    }
    std::string CurrentBranch() const;
    std::shared_ptr<FileSystem> GetFileSystem() const {
        return options_.GetFileSystem();
    }
    std::shared_ptr<SchemaManager> GetSchemaManager() const;
    std::shared_ptr<SnapshotManager> GetSnapshotManager() const;

    Result<std::shared_ptr<FileStorePathFactory>> NewPathFactory(
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::vector<std::string>& partition_keys, const std::string& file_format_identifier,
        const std::shared_ptr<MemoryPool>& memory_pool) const;
    Result<std::shared_ptr<SnapshotManager>> NewSnapshotManager(
        const std::shared_ptr<Catalog>& catalog,
        const std::optional<Identifier>& catalog_identifier) const;
    Result<std::shared_ptr<ManifestList>> NewManifestList(
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;
    Result<std::shared_ptr<ManifestFile>> NewManifestFile(
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<arrow::Schema>& partition_schema,
        const std::shared_ptr<MemoryPool>& memory_pool) const;
    Result<std::unique_ptr<IndexManifestFile>> NewIndexManifestFile(
        BucketMode bucket_mode, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;
    Result<std::unique_ptr<IndexFileHandler>> NewIndexFileHandler(
        BucketMode bucket_mode, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;
    Result<std::unique_ptr<BinaryRowPartitionComputer>> NewPartitionComputer(
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::vector<std::string>& partition_keys,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

 private:
    std::string root_path_;
    CoreOptions options_;
};
}  // namespace paimon
