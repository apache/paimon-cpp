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

#include "paimon/core/table/file_store.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/catalog/catalog.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

std::shared_ptr<FileStore> FileStore::Create(const std::string& root_path,
                                             CoreOptions core_options) {
    return std::shared_ptr<FileStore>(new FileStore(root_path, std::move(core_options)));
}

FileStore::FileStore(const std::string& root_path, CoreOptions core_options)
    : file_system_(core_options.GetFileSystem()),
      root_path_(root_path),
      branch_(BranchManager::NormalizeBranch(core_options.GetBranch())),
      schema_manager_(std::make_shared<SchemaManager>(file_system_, root_path_, branch_)),
      snapshot_manager_(std::make_shared<SnapshotManager>(file_system_, root_path_, branch_)),
      core_options_(std::move(core_options)) {}

Result<std::shared_ptr<FileStorePathFactory>> FileStore::NewPathFactory(
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::vector<std::string>& partition_keys, const CoreOptions& options,
    const std::string& file_format_identifier,
    const std::shared_ptr<MemoryPool>& memory_pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());
    return FileStorePathFactory::Create(
        root_path_, arrow_schema, partition_keys, options.GetPartitionDefaultName(),
        file_format_identifier, options.DataFilePrefix(), options.LegacyPartitionNameEnabled(),
        external_paths, global_index_external_path, options.IndexFileInDataFileDir(), memory_pool);
}

Result<std::shared_ptr<SnapshotManager>> FileStore::NewSnapshotManager(
    const CoreOptions& options, const std::shared_ptr<Catalog>& catalog,
    const std::optional<Identifier>& catalog_identifier) const {
    auto snapshot_manager =
        std::make_shared<SnapshotManager>(options.GetFileSystem(), root_path_, branch_);
    if (VersionManagedCatalog* versioned = AsVersionManaged(catalog)) {
        if (!catalog_identifier) {
            return Status::Invalid("a version-managed catalog requires a table identifier");
        }
        Identifier identifier = catalog_identifier.value();
        snapshot_manager->SetSnapshotLoader(
            [catalog, versioned, identifier]() -> Result<std::optional<Snapshot>> {
                return versioned->LoadSnapshot(identifier);
            });
    }
    return snapshot_manager;
}

Result<std::shared_ptr<ManifestList>> FileStore::NewManifestList(
    const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<MemoryPool>& memory_pool) const {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(options.GetFileSystem(), options.GetManifestFormat(),
                             options.GetManifestCompression(), path_factory, options.GetCache(),
                             memory_pool));
    return manifest_list;
}

Result<std::shared_ptr<ManifestFile>> FileStore::NewManifestFile(
    const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<arrow::Schema>& partition_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) const {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(options.GetFileSystem(), options.GetManifestFormat(),
                             options.GetManifestCompression(), path_factory,
                             options.GetManifestTargetFileSize(), memory_pool, options,
                             partition_schema));
    return manifest_file;
}

Result<std::unique_ptr<IndexManifestFile>> FileStore::NewIndexManifestFile(
    const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<MemoryPool>& memory_pool) const {
    return IndexManifestFile::Create(options.GetFileSystem(), options.GetManifestFormat(),
                                     options.GetManifestCompression(), path_factory,
                                     options.GetBucket(), memory_pool, options);
}

Result<std::unique_ptr<IndexFileHandler>> FileStore::NewIndexFileHandler(
    const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<MemoryPool>& memory_pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexManifestFile> index_manifest_file,
                           NewIndexManifestFile(options, path_factory, memory_pool));
    return std::make_unique<IndexFileHandler>(
        options.GetFileSystem(), std::move(index_manifest_file),
        std::make_shared<IndexFilePathFactories>(path_factory), options.DeletionVectorsBitmap64(),
        memory_pool);
}

Result<std::unique_ptr<BinaryRowPartitionComputer>> FileStore::NewPartitionComputer(
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::vector<std::string>& partition_keys, const CoreOptions& options,
    const std::shared_ptr<MemoryPool>& memory_pool) const {
    return BinaryRowPartitionComputer::Create(partition_keys, arrow_schema,
                                              options.GetPartitionDefaultName(),
                                              options.LegacyPartitionNameEnabled(), memory_pool);
}

}  // namespace paimon
