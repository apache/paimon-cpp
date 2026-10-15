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

class TableScan;
class TableRead;

/// A table provides basic abstraction for table type.
class PAIMON_EXPORT Table : public std::enable_shared_from_this<Table> {
 public:
    /// Creates a table by loading the latest schema from the specified table path.
    ///
    /// @param file_system File system used to access the table directory and schema files.
    /// @param table_path Root path of the table in the file system.
    /// @param identifier Logical table identifier used for naming and error messages.
    /// @return A table initialized with the latest available schema.
    static Result<std::shared_ptr<Table>> Create(const std::shared_ptr<FileSystem>& file_system,
                                                 const std::string& table_path,
                                                 const Identifier& identifier);

    /// @param uuid Metastore UUID, or empty if unavailable.
    Table(const std::shared_ptr<Schema>& schema, const Identifier& identifier,
          const std::string& uuid = "")
        : schema_(schema), identifier_(identifier), uuid_(uuid) {}

    ~Table() = default;

    /// A name to identify this table.
    std::string Name() const {
        return identifier_.GetObjectName();
    }

    /// Full name of the table, default is database.tableName.
    std::string FullName() const {
        return identifier_.GetFullName();
    }

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

    /// Loads the latest schema of table.
    std::shared_ptr<Schema> LatestSchema() const {
        return schema_;
    }

    /// Creates a scan of this table from a finished scan context, standing in for
    /// `TableScan::Create`. The scan is handed the table's shared owner, so it shares the
    /// caller's hold on the table.
    /// @param context The finished scan context.
    /// @return The scan, or the error `TableScan::Create` reports. A table no `shared_ptr` holds
    ///         cannot hand itself over and reports that.
    Result<std::unique_ptr<TableScan>> NewScan(std::unique_ptr<ScanContext> context) const;

    /// Creates a read of this table from a finished read context, standing in for
    /// `TableRead::Create`. The read is handed the table's shared owner, so it shares the
    /// caller's hold on the table.
    /// @param context The finished read context.
    /// @return The read, or the error `TableRead::Create` reports. A table no `shared_ptr` holds
    ///         cannot hand itself over and reports that.
    Result<std::unique_ptr<TableRead>> NewRead(std::unique_ptr<ReadContext> context) const;

    /// Creates a write of this table from a finished write context, standing in for
    /// `FileStoreWrite::Create`. The write is handed the table's shared owner, so it shares the
    /// caller's hold on the table.
    /// @param context The finished write context.
    /// @return The write, or the error `FileStoreWrite::Create` reports. A table no `shared_ptr`
    ///         holds cannot hand itself over and reports that.
    Result<std::unique_ptr<FileStoreWrite>> NewWrite(std::unique_ptr<WriteContext> context) const;

    /// Creates a commit of this table from a finished commit context, standing in for
    /// `FileStoreCommit::Create`. The commit is handed the table's shared owner, so it shares
    /// the caller's hold on the table.
    /// @param context The finished commit context.
    /// @return The commit, or the error `FileStoreCommit::Create` reports. A table no
    ///         `shared_ptr` holds cannot hand itself over and reports that.
    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const;

 private:
    std::shared_ptr<Schema> schema_;
    Identifier identifier_;
    std::string uuid_;
};

}  // namespace paimon
