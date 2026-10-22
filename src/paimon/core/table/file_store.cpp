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
#include <vector>

#include "paimon/catalog/catalog.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

FileStore::FileStore(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
                     const std::shared_ptr<TableSchema>& schema, const std::string& branch,
                     const std::shared_ptr<SchemaManager>& schema_manager,
                     const std::shared_ptr<SnapshotManager>& snapshot_manager)
    : file_system_(file_system),
      root_path_(root_path),
      schema_(schema),
      branch_(BranchManager::NormalizeBranch(branch)),
      schema_manager_(schema_manager),
      snapshot_manager_(snapshot_manager) {
    // A manager handed over points at `root_path` on `branch` already; a missing one is built
    // here, so the operators always read through the managers the store carries.
    if (schema_manager_ == nullptr) {
        schema_manager_ = std::make_shared<SchemaManager>(file_system_, root_path_, branch_);
    }
    if (snapshot_manager_ == nullptr) {
        snapshot_manager_ = std::make_shared<SnapshotManager>(file_system_, root_path_, branch_);
    }
}

Result<std::shared_ptr<FileStorePathFactory>> FileStore::NewPathFactory(
    const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
    const std::string& file_format_identifier,
    const std::shared_ptr<MemoryPool>& memory_pool) const {
    return MakePathFactory(root_path_, arrow_schema, schema_->PartitionKeys(), options,
                           file_format_identifier, memory_pool);
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

Result<std::shared_ptr<FileStorePathFactory>> FileStore::MakePathFactory(
    const std::string& root_path, const std::shared_ptr<arrow::Schema>& arrow_schema,
    const std::vector<std::string>& partition_keys, const CoreOptions& options,
    const std::string& file_format_identifier, const std::shared_ptr<MemoryPool>& memory_pool) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());
    return FileStorePathFactory::Create(
        root_path, arrow_schema, partition_keys, options.GetPartitionDefaultName(),
        file_format_identifier, options.DataFilePrefix(), options.LegacyPartitionNameEnabled(),
        external_paths, global_index_external_path, options.IndexFileInDataFileDir(), memory_pool);
}

}  // namespace paimon
