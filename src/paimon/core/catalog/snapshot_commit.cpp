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

#include "paimon/core/catalog/snapshot_commit.h"

#include <memory>
#include <optional>

#include "paimon/core/catalog/catalog_snapshot_commit.h"
#include "paimon/core/catalog/renaming_snapshot_commit.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

Result<std::shared_ptr<SnapshotCommit>> SnapshotCommit::Create(
    const std::shared_ptr<Catalog>& catalog, const std::optional<Identifier>& catalog_identifier,
    const std::optional<std::string>& table_id, bool use_rest_catalog_commit,
    const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<SnapshotManager>& snapshot_manager) {
    if (AsVersionManaged(catalog) != nullptr) {
        if (!catalog_identifier) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        return std::shared_ptr<SnapshotCommit>(
            std::make_shared<CatalogSnapshotCommit>(catalog, catalog_identifier.value(), table_id));
    }
    if (use_rest_catalog_commit) {
        return std::shared_ptr<SnapshotCommit>(std::make_shared<CatalogSnapshotCommit>(table_id));
    }
    if (table_id && catalog == nullptr) {
        return Status::Invalid(
            "a commit which publishes the snapshot by writing it to the table directory sends no "
            "commit table request, so there is nowhere to put a table id; commit through a "
            "catalog with WithCatalog() or ask for the request with UseRESTCatalogCommit()");
    }
    return std::shared_ptr<SnapshotCommit>(
        std::make_shared<RenamingSnapshotCommit>(file_system, snapshot_manager));
}

}  // namespace paimon
