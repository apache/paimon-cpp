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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/catalog/table.h"

#include <optional>

#include "fmt/format.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/write_context.h"

namespace paimon {

namespace {

/// The table's shared owner, which a `NewScan()`, `NewRead()`, `NewWrite()` or `NewCommit()` call
/// hands to the operator it creates. A table no `shared_ptr` holds has no owner to hand over, and
/// `shared_from_this()` would throw rather than say so, so the failure is stood up as a status.
Result<std::shared_ptr<const Table>> SharedOwnerOf(const Table* table) {
    if (std::shared_ptr<const Table> owner = table->weak_from_this().lock()) {
        return owner;
    }
    return Status::Invalid("table is not held by a shared_ptr");
}

}  // namespace

Result<std::shared_ptr<Table>> Table::Create(const std::shared_ptr<FileSystem>& file_system,
                                             const std::string& table_path,
                                             const Identifier& identifier) {
    PAIMON_ASSIGN_OR_RAISE(bool exist, file_system->Exists(table_path));
    if (!exist) {
        return Status::NotExist(fmt::format("{} not exist", identifier.ToString()));
    }

    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckValidTableName(identifier));
    PAIMON_ASSIGN_OR_RAISE(std::string branch, identifier.GetBranchNameOrDefault());
    if (StringUtils::EqualsIgnoreCase(branch, Identifier::kDefaultMainBranch)) {
        branch = Identifier::kDefaultMainBranch;
    }
    SchemaManager schema_manager(file_system, table_path, branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           schema_manager.Latest());
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
    return std::make_shared<Table>(schema, identifier);
}

Result<std::unique_ptr<TableScan>> Table::NewScan(std::unique_ptr<ScanContext> context) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const Table> table, SharedOwnerOf(this));
    return TableScan::Create(std::move(context), table);
}

Result<std::unique_ptr<TableRead>> Table::NewRead(std::unique_ptr<ReadContext> context) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const Table> table, SharedOwnerOf(this));
    return TableRead::Create(std::move(context), table);
}

Result<std::unique_ptr<FileStoreWrite>> Table::NewWrite(
    std::unique_ptr<WriteContext> context) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const Table> table, SharedOwnerOf(this));
    return FileStoreWrite::Create(std::move(context), table);
}

Result<std::unique_ptr<FileStoreCommit>> Table::NewCommit(
    std::unique_ptr<CommitContext> context) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const Table> table, SharedOwnerOf(this));
    return FileStoreCommit::Create(std::move(context), table);
}

}  // namespace paimon
