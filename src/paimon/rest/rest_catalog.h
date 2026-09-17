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

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/catalog/catalog.h"
#include "paimon/common/utils/generic_lru_cache.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/logging.h"
#include "paimon/rest/rest_api.h"
#include "paimon/rest/rest_token_file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"

struct ArrowSchema;

namespace paimon {
class FileSystem;
class TableSchema;

/// A catalog backed by a REST catalog server. Metadata operations are delegated to
/// `RestApi`; table data is accessed through the file system configured by the
/// server-merged options.
class RestCatalog : public Catalog, public VersionManagedCatalog {
 public:
    /// Creates the catalog: fetches and merges "/v1/config" from the server configured
    /// by `CatalogOptions::URI`, then builds the file system from the merged options.
    ///
    /// @param warehouse The warehouse identifier sent to the server; may be empty.
    static Result<std::unique_ptr<RestCatalog>> Create(
        const std::string& warehouse, const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system,
        const RestHttpClient::Config& http_config = RestHttpClient::Config());

    Status CreateDatabase(const std::string& name,
                          const std::map<std::string, std::string>& options,
                          bool ignore_if_exists) override;
    Status CreateTable(const Identifier& identifier, ArrowSchema* c_schema,
                       const std::vector<std::string>& partition_keys,
                       const std::vector<std::string>& primary_keys,
                       const std::map<std::string, std::string>& options,
                       bool ignore_if_exists) override;
    Status DropDatabase(const std::string& name, bool ignore_if_not_exists, bool cascade) override;
    Status DropTable(const Identifier& identifier, bool ignore_if_not_exists) override;
    Status RenameTable(const Identifier& from_table, const Identifier& to_table,
                       bool ignore_if_not_exists) override;
    Result<std::vector<std::string>> ListDatabases() const override;
    Result<std::vector<std::string>> ListTables(const std::string& db_name) const override;
    Result<bool> DatabaseExists(const std::string& db_name) const override;
    Result<bool> TableExists(const Identifier& identifier) const override;
    Result<std::string> GetDatabaseLocation(const std::string& db_name) const override;
    Result<std::string> GetTableLocation(const Identifier& identifier) const override;
    Result<std::shared_ptr<Schema>> LoadTableSchema(const Identifier& identifier) const override;
    std::string GetRootPath() const override;
    std::shared_ptr<FileSystem> GetFileSystem() const override;
    /// Returns a file system that refreshes the temporary credentials the server issues
    /// for the table when `CatalogOptions::DATA_TOKEN_ENABLED` is set, and the
    /// catalog-level file system otherwise. Every call returns an instance bound to the
    /// table it was asked for, whose credentials are loaded on the first access; the file
    /// systems built from them are shared through a bounded cache, so the tables the
    /// server issues the same credentials for share one file system. `fs_options` override
    /// the catalog options the file system is built from, the credentials still win over
    /// them; a call that overrides anything builds a file system of its own rather than
    /// reading the shared cache, so its override is never dropped for a cache hit.
    Result<std::shared_ptr<FileSystem>> GetTableFileSystem(
        const Identifier& identifier,
        const std::map<std::string, std::string>& fs_options) const override;
    Result<std::shared_ptr<Table>> GetTable(const Identifier& identifier) const override;
    Result<std::vector<SnapshotInfo>> ListSnapshots(const Identifier& identifier,
                                                    const std::string& branch) const override;

    bool SupportsVersionManagement() const override {
        return true;
    }

    Result<std::optional<Snapshot>> LoadSnapshot(const Identifier& identifier) const override;

    Result<bool> CommitSnapshot(const Identifier& identifier,
                                const std::optional<std::string>& table_uuid,
                                const std::optional<std::string>& base_snapshot_uuid,
                                const Snapshot& snapshot,
                                const std::vector<PartitionStatistics>& statistics) override;

    /// Options merged with the server side config.
    const std::map<std::string, std::string>& GetOptions() const override;

 protected:
    /// Loads the location and the schema from one `GetTable` response, so the table cannot be
    /// built from a location and a schema that two requests disagreed on.
    Result<std::shared_ptr<FormatTable>> LoadFormatTable(
        const Identifier& identifier) const override;

 private:
    RestCatalog(std::shared_ptr<RestApi> api, const std::shared_ptr<FileSystem>& fs,
                const std::string& warehouse, bool data_token_enabled);

    /// Loads the schema and catalog table ID from the same response.
    Result<std::shared_ptr<Schema>> LoadTableSchema(const Identifier& identifier,
                                                    std::string* table_id) const;

    /// Loads the schema with table path, audit and branch options.
    /// Fills `table_path` and `table_id` when supplied.
    Result<std::shared_ptr<TableSchema>> LoadDataTableSchema(
        const Identifier& data_identifier, const std::optional<std::string>& branch,
        std::string* table_path, std::string* table_id) const;

    static Result<std::unique_ptr<TableSchema>> ToTableSchema(
        const GetTableResponse& response, const std::optional<std::string>& branch);

    std::shared_ptr<RestApi> api_;
    std::shared_ptr<FileSystem> fs_;
    std::string warehouse_;
    /// Whether table data is accessed with the credentials the server issues per table.
    bool data_token_enabled_ = false;
    /// The "table-default." options of the merged config, applied to `CreateTable`
    /// options when absent.
    std::map<std::string, std::string> table_default_options_;
    /// The file systems of the data tokens, keyed by the credentials they were built from
    /// and shared by the data token file systems this catalog hands out. Only created when
    /// `data_token_enabled_` is set.
    std::shared_ptr<RestTokenFileSystemCache> token_fs_cache_;
    std::shared_ptr<Logger> logger_;
};

}  // namespace paimon
