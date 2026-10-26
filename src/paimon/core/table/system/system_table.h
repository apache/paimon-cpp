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
class ReadContext;
class ScanContext;
class TableSchema;
class SystemTableSchema;

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

/// Base class for table-scoped system tables such as `T$options` and `T$snapshots`: the read-only
/// view a `T$system` identifier names, which exposes a schema of its own and serves scans and
/// reads from the data table it belongs to. `Name()` - which each kind answers for itself - is
/// its full name as well: the identifier it is opened by belongs to the data table underneath.
class SystemTable : public Table, public std::enable_shared_from_this<SystemTable> {
 public:
    std::string FullName() const override {
        return Name();
    }

    /// The read-only schema this system table exposes, which is not the data table's schema.
    virtual Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const = 0;

    /// A system table is read-only: copying it, writing it and committing for it are refused
    /// rather than routed to the data table underneath, which its own `Table` serves.
    Result<std::shared_ptr<Table>> Copy(
        const std::map<std::string, std::string>& dynamic_options) const override;

    Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const override;

    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const override;

 protected:
    /// @param fs File system the data table underneath is read through.
    /// @param table_path Root path of the data table this system table belongs to.
    SystemTable(const std::shared_ptr<FileSystem>& fs, const std::string& table_path);

    /// Refuses a read whose context asks for a search: each system table builds a context of its
    /// own for the data table underneath, which carries no search, so answering one would return
    /// plain rows and quietly drop the search and its top K.
    static Status CheckNoSearch(const std::shared_ptr<ReadContext>& context);

    /// Loaders call this once the object exists, because `ArrowSchema()` is virtual and cannot run
    /// from a constructor: it fills in this table's own read-only `SystemTableSchema`.
    Status AdoptOwnSchema();

    friend class SystemTableLoader;
    friend class GlobalSystemTableLoader;
};

/// Loads system table implementations from parsed table identifiers or table paths.
///
/// The loader owns the registry that maps a system table name to its factory.
class SystemTableLoader {
 public:
    static bool IsSupported(const std::string& system_table_name);

    static Result<std::shared_ptr<SystemTable>> Load(
        const std::string& system_table_name, const std::shared_ptr<FileSystem>& fs,
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        const std::map<std::string, std::string>& dynamic_options);

    static Result<std::optional<SystemTablePath>> TryParsePath(const std::string& path);

    static Result<std::shared_ptr<SystemTable>> LoadFromPath(
        const std::shared_ptr<FileSystem>& fs, const std::string& path,
        const std::map<std::string, std::string>& dynamic_options);
};

}  // namespace paimon
