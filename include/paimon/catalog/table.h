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

/// A table provides basic abstraction for table type.
///
/// The state a table of every kind carries - file system, root path, schema and identifier - lives
/// here, and each kind of table fills the operations in: the operators below create this table's
/// scan, read, write or commit, from a context the caller finishes through its builder. What the
/// context asks for that this table cannot honour is refused by name by the implementation rather
/// than silently dropped.
class PAIMON_EXPORT Table {
 public:
    virtual ~Table() = default;

    /// Returns the file system the table was opened with, which serves the table's
    /// files. A catalog that hands out per-table file systems opens a table with the
    /// one for that table; it is null when the opener had none.
    std::shared_ptr<FileSystem> GetFileSystem() const {
        return file_system_;
    }

    /// Returns the root path of the table in its file system. It is empty when the
    /// opener had no location for the table, which a mock in a test is one case of.
    const std::string& GetRootPath() const {
        return root_path_;
    }

    /// A name to identify this table, which each kind of table answers for itself: a managed or
    /// format table is named by its identifier, a system table by its own kind.
    virtual std::string Name() const = 0;

    /// Full name of the table, which each kind of table answers for itself. A managed or format
    /// table answers database.tableName, a system table the same name as `Name()`.
    virtual std::string FullName() const = 0;

    /// Returns the branch the table was opened for, which its identifier carries; `main`
    /// when it names none or a spelling of it, which its managers are bound to. A
    /// malformed object name that hides the branch is reported.
    Result<std::string> CurrentBranch() const {
        return identifier_.GetBranchNameOrDefault();
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

    /// Creates a scan of this table from a finished scan context, the table-first entry point
    /// that `TableScan::Create()` loads the table the context names and hands the context to.
    /// @param context The finished scan context.
    /// @return The scan, or the error building it reports.
    virtual Result<std::unique_ptr<TableScan>> NewScan(
        std::unique_ptr<ScanContext> context) const = 0;

    /// Creates a read of this table from a finished read context, the table-first entry point
    /// that `TableRead::Create()` loads the table the context names and hands the context to.
    /// @param context The finished read context.
    /// @return The read, or the error building it reports.
    virtual Result<std::unique_ptr<TableRead>> NewRead(
        std::unique_ptr<ReadContext> context) const = 0;

    /// Creates a write of this table from a finished write context, the table-first entry point
    /// that `FileStoreWrite::Create()` loads the table the context names and hands the context to.
    /// @param context The finished write context.
    /// @return The write, or the error building it reports.
    virtual Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const = 0;

    /// Creates a commit of this table from a finished commit context, the table-first entry point
    /// that `FileStoreCommit::Create()` loads the table the context names and hands the context
    /// to.
    /// @param context The finished commit context.
    /// @return The commit, or the error building it reports.
    virtual Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const = 0;

 protected:
    /// @param file_system File system the table's data is accessed through.
    /// @param root_path Root path of the table in the file system.
    /// @param uuid Metastore UUID, or empty if unavailable.
    Table(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
          const std::shared_ptr<Schema>& schema, const Identifier& identifier,
          const std::string& uuid = "");

    std::shared_ptr<FileSystem> file_system_;
    std::string root_path_;
    std::shared_ptr<Schema> schema_;
    Identifier identifier_;

 private:
    std::string uuid_;
};

}  // namespace paimon
