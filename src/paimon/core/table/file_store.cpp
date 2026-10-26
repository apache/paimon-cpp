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

#include <map>
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
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

namespace {

/// Whether an option key takes part in file-system resolution: the backend `file-system` picks,
/// an `fs.` access key carries, or a native S3 key names - `s3.` and its `s3a.` spelling, the
/// prefixes the S3 file system consumes alongside `fs.s3.`/`fs.s3a.`. The scheme-map keys a
/// context carries are the same set.
bool IsFileSystemResolutionKey(const std::string& key) {
    return key == Options::FILE_SYSTEM || key.rfind("fs.", 0) == 0 || key.rfind("s3.", 0) == 0 ||
           key.rfind("s3a.", 0) == 0;
}

}  // namespace

Result<std::shared_ptr<FileStore>> FileStore::Create(
    const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
    const std::shared_ptr<TableSchema>& schema, const std::string& branch,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::map<std::string, std::string>& fs_scheme_map) {
    auto store = std::shared_ptr<FileStore>(new FileStore(
        file_system, root_path, schema, branch, schema_manager, snapshot_manager, fs_scheme_map));
    if (schema != nullptr) {
        // The options the schema was loaded with, on the branch this store serves: what reads
        // through them - whose cache keys are scoped by the branch - reads the same one the
        // managers point at. The scheme map is the one the schema was read through, so a scheme
        // it mapped resolves here the way it resolved there.
        std::map<std::string, std::string> options = schema->Options();
        if (BranchManager::IsMainBranch(store->branch_)) {
            options.erase(Options::BRANCH);
        } else {
            options[Options::BRANCH] = store->branch_;
        }
        PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                               CoreOptions::FromMap(options, file_system, fs_scheme_map));
        store->core_options_ = std::move(core_options);
    }
    // The file system the table is served through: the one the options resolve - a schema
    // carrying access configuration names its own, which the file system the caller opened the
    // table through wins over - and without a schema, the one handed over, whose absence leaves
    // the default local one.
    if (store->core_options_.has_value()) {
        store->file_system_ = store->core_options_->GetFileSystem();
    } else if (file_system != nullptr) {
        store->file_system_ = file_system;
    } else {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions default_options, CoreOptions::FromMap({}));
        store->file_system_ = default_options.GetFileSystem();
    }
    // The managers read through the same file system: the metadata belongs to the data, and
    // both were written under the options the schema carries. A manager handed over already
    // points at `root_path` on `branch`; a missing one is built here, so the operators always
    // read through the managers the store carries.
    if (store->schema_manager_ == nullptr) {
        store->schema_manager_ =
            std::make_shared<SchemaManager>(store->file_system_, root_path, store->branch_);
    }
    if (store->snapshot_manager_ == nullptr) {
        store->snapshot_manager_ =
            std::make_shared<SnapshotManager>(store->file_system_, root_path, store->branch_);
    }
    return store;
}

bool FileStore::FileSystemResolutionMayChange(
    const TableSchema& schema, const std::map<std::string, std::string>& dynamic_options) {
    const std::map<std::string, std::string>& schema_options = schema.Options();
    for (const auto& [key, value] : dynamic_options) {
        if (!IsFileSystemResolutionKey(key)) {
            continue;
        }
        auto schema_option = schema_options.find(key);
        if (schema_option == schema_options.end() || schema_option->second != value) {
            return true;
        }
    }
    return false;
}

bool FileStore::FileSystemResolutionAgrees(const std::map<std::string, std::string>& left,
                                           const std::map<std::string, std::string>& right) {
    for (const auto& [key, value] : left) {
        if (IsFileSystemResolutionKey(key)) {
            auto right_option = right.find(key);
            if (right_option == right.end() || right_option->second != value) {
                return false;
            }
        }
    }
    for (const auto& [key, value] : right) {
        if (IsFileSystemResolutionKey(key)) {
            auto left_option = left.find(key);
            if (left_option == left.end() || left_option->second != value) {
                return false;
            }
        }
    }
    return true;
}

FileStore::FileStore(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
                     const std::shared_ptr<TableSchema>& schema, const std::string& branch,
                     const std::shared_ptr<SchemaManager>& schema_manager,
                     const std::shared_ptr<SnapshotManager>& snapshot_manager,
                     const std::map<std::string, std::string>& fs_scheme_map)
    : file_system_(file_system),
      specified_file_system_(file_system),
      fs_scheme_map_(fs_scheme_map),
      root_path_(root_path),
      schema_(schema),
      branch_(BranchManager::NormalizeBranch(branch)),
      schema_manager_(schema_manager),
      snapshot_manager_(snapshot_manager) {}

Result<std::shared_ptr<FileStorePathFactory>> FileStore::NewPathFactory(
    const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
    const std::string& file_format_identifier,
    const std::shared_ptr<MemoryPool>& memory_pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());
    return FileStorePathFactory::Create(
        root_path_, arrow_schema, schema_->PartitionKeys(), options.GetPartitionDefaultName(),
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
    const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
    const std::shared_ptr<MemoryPool>& memory_pool) const {
    return BinaryRowPartitionComputer::Create(schema_->PartitionKeys(), arrow_schema,
                                              options.GetPartitionDefaultName(),
                                              options.LegacyPartitionNameEnabled(), memory_pool);
}

}  // namespace paimon
