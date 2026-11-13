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

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>

#include "arrow/api.h"
#include "paimon/catalog/identifier.h"
#include "paimon/catalog/table.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"

namespace paimon {
class FileSystem;
class FileStoreTable;
class ReadContext;
class ScanContext;
class TableSchema;
class SystemTableSchema;
struct GlobalSystemTableContext;

/// Parsed information for a table-scoped system table path.
struct SystemTablePath {
    /// Base data table path without the system table suffix.
    std::string table_path;
    /// Optional branch parsed from identifiers such as `T$branch_dev$options`.
    std::optional<std::string> branch;
    /// System table name, for example `options` or `snapshots`.
    std::string system_table_name;
    /// Whether this is a global system table under the `sys` database.
    bool is_global = false;
};

/// Read-only table-scoped or global system view.
/// Table-scoped names are <data table>$<kind>; global names are <kind>.
/// Catalog-loaded tables retain the database in FullName().
class SystemTable : public Table, public std::enable_shared_from_this<SystemTable> {
 public:
    std::string Name() const override {
        return name_;
    }

    std::string FullName() const override {
        return full_name_.empty() ? name_ : full_name_;
    }

    /// Returns this view's schema, rather than its data table's schema.
    virtual Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const = 0;

    Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const override;

    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const override;

 protected:
    /// @param fs Data table file system.
    /// @param table_path Data table root path.
    /// @param name System table name.
    SystemTable(const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
                std::string name);

    /// Composes <data_table_name>$<kind>, preserving any branch suffix.
    static std::string ComposeName(const std::string& data_table_name, const std::string& kind);

    /// Rejects TopK and full-text search requests, which system-table readers cannot evaluate.
    static Status CheckNoSearch(const std::shared_ptr<ReadContext>& context);

    /// Initializes the system schema after construction, when ArrowSchema() can be called.
    Status AdoptOwnSchema();

    /// Applies options without changing the view's table type or branch.
    static Result<std::map<std::string, std::string>> CopyOptions(
        const std::map<std::string, std::string>& bound_options,
        const std::map<std::string, std::string>& overrides);

    /// Builds the data table using this view's data schema and options.
    Result<std::shared_ptr<FileStoreTable>> CreateDataTable(
        const std::shared_ptr<TableSchema>& table_schema,
        const std::map<std::string, std::string>& options) const;

    friend class SystemTableLoader;
    friend class GlobalSystemTableLoader;

 private:
    std::string name_;
    std::string full_name_;
    std::map<std::string, std::string> fs_scheme_map_;
};

/// Loads system table implementations from parsed table identifiers or table paths.
///
/// The loader owns the registry that maps a system table name to its factory.
class SystemTableLoader {
 public:
    static bool IsSupported(const std::string& system_table_name);

    /// Loads a catalog view while retaining its full logical identity.
    static Result<std::shared_ptr<SystemTable>> Load(
        const Identifier& identifier, const std::shared_ptr<FileSystem>& fs,
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        const std::map<std::string, std::string>& dynamic_options,
        const std::map<std::string, std::string>& fs_scheme_map);

    static Result<std::shared_ptr<SystemTable>> Load(
        const std::string& system_table_name, const std::shared_ptr<FileSystem>& fs,
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        const std::map<std::string, std::string>& dynamic_options,
        const std::string& data_table_name = "",
        const std::map<std::string, std::string>& fs_scheme_map = {});

    static Result<std::optional<SystemTablePath>> TryParsePath(const std::string& path);

    static Result<std::shared_ptr<SystemTable>> LoadFromPath(
        const std::shared_ptr<FileSystem>& fs, const std::string& path,
        const std::map<std::string, std::string>& dynamic_options,
        const std::map<std::string, std::string>& fs_scheme_map = {});

    static Result<std::shared_ptr<SystemTable>> LoadFromPath(
        const std::shared_ptr<FileSystem>& fs, const std::string& path,
        const std::map<std::string, std::string>& dynamic_options,
        const std::map<std::string, std::string>& fs_scheme_map,
        const std::optional<std::string>& specific_schema);
};

}  // namespace paimon
