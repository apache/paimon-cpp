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

/// Base class for the table-scoped system tables such as `T$options` and `T$snapshots`. A
/// `T$system` identifier names a read-only view that exposes a schema of its own and serves scans
/// and reads from the data table it belongs to.
///
/// A system table of a data table answers `<data table name>$<kind>` for `Name()`. One of the `sys`
/// database answers the bare kind name. `FullName()` returns the same as `Name()` in both cases,
/// which is what Java Paimon's `Table.fullName()` defaults to.
class SystemTable : public Table, public std::enable_shared_from_this<SystemTable> {
 public:
    std::string Name() const override {
        return name_;
    }

    std::string FullName() const override {
        return name_;
    }

    /// Returns the read-only schema this system table exposes. It is not the data table's schema.
    virtual Result<std::shared_ptr<arrow::Schema>> ArrowSchema() const = 0;

    /// Copies this system table with `dynamic_options` on top of the ones it was loaded with,
    /// rebuilding it through the loader that created it - the way Java Paimon's system tables
    /// re-wrap `dataTable.copy(dynamicOptions)`. A global system table of the `sys` database
    /// copies over the options it already carries and ignores the dynamic ones, as Java's do.
    /// Writing and committing stay refused: this view is read-only, and the data table
    /// underneath serves them through its own `Table`.
    Result<std::shared_ptr<Table>> Copy(
        const std::map<std::string, std::string>& dynamic_options) const override;

    Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const override;

    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const override;

 protected:
    /// @param fs File system the data table underneath is read through.
    /// @param table_path Root path of the data table this system table belongs to.
    /// @param name The name this table answers with: `ComposeName(data_table_name, kind)` for a
    ///        table-scoped system table, the bare kind name for one of the `sys` database.
    SystemTable(const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
                std::string name);

    /// Composes the name of a table-scoped system table out of the logical name of the data table
    /// underneath - the object name a catalog knows it by, branch included - and the kind. Java
    /// Paimon's `OptionsTable.name()` composes it the same way, from `location.getName()`; the
    /// metadata tables answer with the directory name, while the views that serve the data table
    /// answer with its logical name, the way Java's `ReadOptimizedTable` answers with the name of
    /// the table it wraps.
    static std::string ComposeName(const std::string& data_table_name, const std::string& kind);

    /// Refuses a read whose context asks for a search: a root TopK request or a `FullTextMatch`
    /// predicate. Neither is evaluated on the rows a system table serves, so answering one
    /// would return plain rows and quietly drop the search and its top K.
    static Status CheckNoSearch(const std::shared_ptr<ReadContext>& context);

    /// Fills in this table's own read-only `SystemTableSchema`. A loader calls it once the object
    /// exists, because `ArrowSchema()` is virtual and cannot run from a constructor.
    Status AdoptOwnSchema();

    friend class SystemTableLoader;
    friend class GlobalSystemTableLoader;

 private:
    /// The inputs the loader that created this table used, recorded so `Copy` rebuilds through
    /// the same loader. A table-scoped system table records the `SystemTableLoader::Load`
    /// arguments; a global one records the `GlobalSystemTableLoader::Load` ones instead.
    struct Origin {
        /// System table kind, lower-case, the way the registry knows it.
        std::string kind;
        /// Data table schema the view was loaded with. Null for a global system table.
        std::shared_ptr<TableSchema> table_schema;
        /// Dynamic options the table was loaded with, on top of the schema's own.
        std::map<std::string, std::string> dynamic_options;
        /// Logical name of the data table underneath, branch included. May be empty for a
        /// table-scoped table, whose loader then falls back to the directory name.
        std::string data_table_name;
        /// Context a global system table was loaded with. Null for a table-scoped table.
        std::shared_ptr<const GlobalSystemTableContext> context;
    };

    std::string name_;
    std::optional<Origin> origin_;
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
        const std::map<std::string, std::string>& dynamic_options,
        const std::string& data_table_name);

    static Result<std::optional<SystemTablePath>> TryParsePath(const std::string& path);

    static Result<std::shared_ptr<SystemTable>> LoadFromPath(
        const std::shared_ptr<FileSystem>& fs, const std::string& path,
        const std::map<std::string, std::string>& dynamic_options);
};

}  // namespace paimon
