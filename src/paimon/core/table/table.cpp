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

#include "paimon/catalog/table.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/write_context.h"

namespace paimon {

Table::Table(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
             const std::shared_ptr<Schema>& schema, const Identifier& identifier,
             const std::string& uuid, const std::shared_ptr<SchemaManager>& schema_manager)
    : file_system_(file_system),
      root_path_(root_path),
      schema_(schema),
      identifier_(identifier),
      uuid_(uuid),
      schema_manager_(schema_manager) {
    // The managers are built on the table's own branch, which a malformed object name
    // cannot name: such a table is handed out with none rather than fail here, since a
    // constructor has no error to return. A null file system, which a mock in a test
    // opens a table with, leaves none for the same reason.
    Result<std::string> branch = identifier_.GetBranchNameOrDefault();
    if (file_system_ == nullptr || !branch.ok()) {
        schema_manager_ = nullptr;
        return;
    }
    if (schema_manager_ == nullptr) {
        schema_manager_ = std::make_shared<SchemaManager>(file_system_, root_path_, branch.value());
    }
    snapshot_manager_ = std::make_shared<SnapshotManager>(file_system_, root_path_, branch.value());
}

Result<std::shared_ptr<Table>> Table::Create(const std::shared_ptr<FileSystem>& file_system,
                                             const std::string& table_path,
                                             const Identifier& identifier) {
    PAIMON_ASSIGN_OR_RAISE(bool exist, file_system->Exists(table_path));
    if (!exist) {
        return Status::NotExist(fmt::format("{} not exist", identifier.ToString()));
    }

    // The schema is read through the manager the returned table keeps, so the file read
    // here is the one read through the table as well. Built on the table's own branch:
    // `root/branch/branch-<name>` below the table path, which `main` resolves to itself.
    // `GetBranchNameOrDefault` folds any spelling of `main`, so the managers the
    // constructor keeps and `CurrentBranch()` agree with this one.
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckValidTableName(identifier));
    PAIMON_ASSIGN_OR_RAISE(std::string branch, identifier.GetBranchNameOrDefault());
    auto schema_manager = std::make_shared<SchemaManager>(file_system, table_path, branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           schema_manager->Latest());
    if (!latest_schema) {
        return Status::NotExist(
            fmt::format("load table schema for {} failed", identifier.ToString()));
    }

    // Only a managed table is a `Table`; describing another type as one would hand back snapshots
    // and manifests it never had. Checked here because this is the one place every `Table` built
    // from a table directory passes through, catalogs included.
    PAIMON_RETURN_NOT_OK(
        CatalogUtils::CheckManagedTableType(identifier, *latest_schema, "Table::Create"));

    auto schema = checked_pointer_cast<Schema>(*latest_schema);
    return std::make_shared<Table>(file_system, table_path, schema, identifier, /*uuid=*/"",
                                   schema_manager);
}

Result<std::unique_ptr<TableScan>> Table::NewScan(std::unique_ptr<ScanContext> context) const {
    return TableScan::Create(std::move(context));
}

Result<std::unique_ptr<TableRead>> Table::NewRead(std::unique_ptr<ReadContext> context) const {
    return TableRead::Create(std::move(context));
}

Result<std::unique_ptr<FileStoreWrite>> Table::NewWrite(
    std::unique_ptr<WriteContext> context) const {
    return FileStoreWrite::Create(std::move(context));
}

Result<std::unique_ptr<FileStoreCommit>> Table::NewCommit(
    std::unique_ptr<CommitContext> context) const {
    return FileStoreCommit::Create(std::move(context));
}

}  // namespace paimon
