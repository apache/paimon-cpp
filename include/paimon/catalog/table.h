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

#include "paimon/catalog/identifier.h"
#include "paimon/result.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {

class CommitContext;
class FileStoreCommit;
class FileStoreWrite;
class ReadContext;
class ScanContext;
class TableRead;
class TableScan;
class WriteContext;

/// Common interface for managed, format and system tables.
/// Create operators with NewScan/NewRead/NewWrite/NewCommit; unsupported operations return errors.
/// Operators use this table's type, path, schema and file system, with context execution settings.
class PAIMON_EXPORT Table {
 public:
    virtual ~Table() = default;

    /// Returns the table file system, or null if none was supplied.
    std::shared_ptr<FileSystem> GetFileSystem() const {
        return file_system_;
    }

    /// Returns the table root path, or an empty string if unavailable.
    const std::string& GetRootPath() const {
        return root_path_;
    }

    /// Returns the object name for data tables, or the system table name.
    virtual std::string Name() const = 0;

    /// Returns database.tableName when the database is known, otherwise Name().
    virtual std::string FullName() const = 0;

    /// Returns the metastore UUID, falling back to `FullName()`.
    /// @note Use `CatalogUuid()` for commit requests to avoid sending a table name as an ID.
    std::string Uuid() const {
        return uuid_.empty() ? FullName() : uuid_;
    }

    /// Returns the metastore UUID, or null if unavailable, for
    /// `CommitContextBuilder::WithTableId()`.
    std::optional<std::string> CatalogUuid() const {
        return uuid_.empty() ? std::nullopt : std::optional<std::string>(uuid_);
    }

    /// Returns the schema bound to this table instance without metadata I/O.
    /// Reload through the catalog to obtain the latest schema; this accessor does not refresh it.
    std::shared_ptr<Schema> GetSchema() const {
        return schema_;
    }

    /// Creates a scan using this table's schema and options.
    virtual Result<std::unique_ptr<TableScan>> NewScan(
        std::unique_ptr<ScanContext> context) const = 0;

    /// Creates a reader using this table's schema and options.
    virtual Result<std::unique_ptr<TableRead>> NewRead(
        std::unique_ptr<ReadContext> context) const = 0;

    /// Creates a writer using this table's schema and options.
    virtual Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const = 0;

    /// Creates a commit using this table's schema and options.
    virtual Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const = 0;

 protected:
    /// @param file_system File system the table's data is accessed through.
    /// @param root_path Root path of the table in the file system.
    /// @param uuid Metastore UUID, or empty if unavailable.
    Table(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
          const std::shared_ptr<Schema>& schema, const Identifier& identifier,
          const std::string& uuid);

    std::shared_ptr<FileSystem> file_system_;
    std::string root_path_;
    std::shared_ptr<Schema> schema_;
    Identifier identifier_;

 private:
    std::string uuid_;
};

}  // namespace paimon
