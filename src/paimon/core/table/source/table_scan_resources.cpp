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

#include "paimon/table/source/table_scan_resources.h"

#include <utility>

#include "paimon/common/utils/path_util.h"
#include "paimon/core/table/source/table_scan_resources_impl.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/field_mapping.h"

namespace paimon {

Result<std::shared_ptr<TableScanResources>> TableScanResources::Create(
    const std::string& table_path, const std::shared_ptr<FileSystem>& file_system,
    const std::string& branch, const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!file_system || !memory_pool) {
        return Status::Invalid("table scan resources require a file system and memory pool");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string path, PathUtil::NormalizePath(table_path));
    if (path.empty()) {
        return Status::Invalid("table scan resources require a non-empty table path");
    }
    PAIMON_RETURN_NOT_OK(BranchManager::CheckValidBranch(branch));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_path,
                           SystemTableLoader::TryParsePath(path));
    if (system_path) {
        return Status::Invalid("table scan resources require a physical table path");
    }
    return std::shared_ptr<TableScanResources>(new TableScanResources(std::make_unique<Impl>(
        path, BranchManager::NormalizeBranch(branch), file_system, memory_pool)));
}

TableScanResources::TableScanResources(std::unique_ptr<Impl>&& impl) : impl_(std::move(impl)) {}

TableScanResources::~TableScanResources() = default;

TableScanResources::Impl::Impl(const std::string& path, const std::string& branch,
                               const std::shared_ptr<FileSystem>& file_system,
                               const std::shared_ptr<MemoryPool>& memory_pool)
    : path_(path),
      branch_(branch),
      file_system_(file_system),
      memory_pool_(memory_pool),
      schema_manager_(std::make_shared<SchemaManager>(file_system, path, branch)),
      snapshot_manager_(std::make_shared<SnapshotManager>(file_system, path, branch)) {}

Status TableScanResources::Impl::Validate(
    const std::string& scan_path, const std::string& scan_branch,
    const std::shared_ptr<FileSystem>& specific_file_system) const {
    PAIMON_ASSIGN_OR_RAISE(std::string physical_path, PathUtil::NormalizePath(scan_path));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_path,
                           SystemTableLoader::TryParsePath(physical_path));
    if (system_path) {
        if (system_path->is_global) {
            return Status::Invalid(
                "table scan resources cannot be used with a global system table");
        }
        PAIMON_ASSIGN_OR_RAISE(physical_path, PathUtil::NormalizePath(system_path->table_path));
        if (system_path->branch &&
            BranchManager::NormalizeBranch(*system_path->branch) != branch_) {
            return Status::Invalid("table scan resources branch does not match system table path");
        }
    }
    if (physical_path != path_) {
        return Status::Invalid("table scan resources path does not match scan path");
    }
    if (BranchManager::NormalizeBranch(scan_branch) != branch_) {
        return Status::Invalid("table scan resources branch does not match scan branch");
    }
    if (specific_file_system && specific_file_system != file_system_) {
        return Status::Invalid("table scan resources file system does not match scan file system");
    }
    return Status::OK();
}

Result<std::shared_ptr<const ScanSchemaResources>> TableScanResources::Impl::GetSchemaResources(
    const std::shared_ptr<TableSchema>& table_schema) {
    // Only schema-derived construction is serialized here; metadata I/O and scans run outside
    // this lock. External schemas supplied through SetTableSchema() never enter this cache.
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = schemas_.find(table_schema->Id());
    if (iter != schemas_.end()) {
        return iter->second;
    }
    auto resources = std::make_shared<ScanSchemaResources>();
    resources->arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        resources->partition_schema,
        FieldMapping::GetPartitionSchema(resources->arrow_schema, table_schema->PartitionKeys()));
    if (!table_schema->PrimaryKeys().empty()) {
        PAIMON_ASSIGN_OR_RAISE(resources->primary_key_fields,
                               table_schema->TrimmedPrimaryKeyFields());
    }
    resources->stats_evolutions =
        std::make_shared<SimpleStatsEvolutions>(table_schema, memory_pool_);
    schemas_.emplace(table_schema->Id(), resources);
    return std::shared_ptr<const ScanSchemaResources>(std::move(resources));
}
}  // namespace paimon
