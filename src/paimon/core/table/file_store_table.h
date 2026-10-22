/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/catalog/table.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace paimon {

class Schema;

/// The managed table a catalog hands out: one with snapshots and manifests, which the schema under
/// its root path describes. A format table - a directory of plain data files - is a `FormatTable`,
/// the other kind of `Table`.
///
/// Its `NewScan`/`NewRead`/`NewWrite`/`NewCommit` build the operators here, from the context plus
/// the schema, file system, root path, branch and managers the table was constructed with, so
/// they serve the table as it was loaded. The
/// context-only entry points - `TableScan::Create()`, `TableRead::Create()`,
/// `FileStoreWrite::Create()` and `FileStoreCommit::Create()` - load the table the context names
/// and hand the context to that table's `NewXxx`, which is how a context built without a table
/// reaches the same operators.
class FileStoreTable : public Table {
 public:
    /// The constructor a catalog builds a managed table through, so a `shared_ptr` can hold one;
    /// the base constructor is protected, which an inherited constructor would stay.
    /// @param file_system File system the table's data is accessed through.
    /// @param root_path Root path of the table in the file system.
    /// @param uuid Metastore UUID, or empty if unavailable.
    /// @param branch The branch the table serves, which the managers below read and write on.
    /// @param schema_manager A schema manager to carry instead of building one - the one a
    ///        context dispatch read the schema through, whose cache the operators then serve.
    /// @param snapshot_manager A snapshot manager to carry instead of building one, the way
    ///        `Copy` hands its table's over so both share one.
    /// A null manager is built over `root_path` on `branch`, so the operators always read
    /// through the managers the table carries.
    FileStoreTable(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
                   const std::shared_ptr<Schema>& schema, const Identifier& identifier,
                   const std::string& uuid = "",
                   const std::string& branch = BranchManager::DEFAULT_MAIN_BRANCH,
                   const std::shared_ptr<SchemaManager>& schema_manager = nullptr,
                   const std::shared_ptr<SnapshotManager>& snapshot_manager = nullptr);

    /// The branch this table serves, as resolved when the table was built: the schema the table
    /// was loaded with came from it, and its managers point at it.
    const std::string& CurrentBranch() const {
        return store_->branch();
    }

    /// The schema manager the operators read through. It carries the schema cache of `root_path`
    /// on `CurrentBranch()`, which every operator serves rather than each reading the schema
    /// files again.
    const std::shared_ptr<SchemaManager>& GetSchemaManager() const {
        return store_->schema_manager();
    }

    /// The snapshot manager a scan reads through. A write or a commit builds its own: it arms
    /// the manager with a catalog snapshot loader, which a shared one would keep serving to
    /// scans afterwards.
    const std::shared_ptr<SnapshotManager>& GetSnapshotManager() const {
        return store_->snapshot_manager();
    }

    std::string Name() const override {
        return identifier_.GetObjectName();
    }

    std::string FullName() const override {
        return identifier_.GetFullName();
    }

    /// Copies the table with `dynamic_options` on top of the ones its schema stored, the
    /// precedence `Copy` promises. The file system, root path and identifier stay as they are:
    /// they name this table, which an option does not rename.
    Result<std::shared_ptr<Table>> Copy(
        const std::map<std::string, std::string>& dynamic_options) const override;

    /// Returns the table of `branch`, as Java Paimon's `switchToBranch` does. The branch switched
    /// to is served as it is: its schema is read from its own directory, so the dynamic options
    /// of this table are not carried over. Switching to the branch already served hands back a
    /// copy of this table, schema and managers shared, and a branch with no schema does not
    /// exist.
    Result<std::shared_ptr<Table>> SwitchToBranch(const std::string& branch) const;

    Result<std::unique_ptr<TableScan>> NewScan(std::unique_ptr<ScanContext> context) const override;

    Result<std::unique_ptr<TableRead>> NewRead(std::unique_ptr<ReadContext> context) const override;

    Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const override;

    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const override;

 private:
    /// The storage this table serves: the file system, root path, schema, branch and managers it
    /// was loaded with, and the factories that build the pieces every operator opens with.
    std::shared_ptr<FileStore> store_;
};

}  // namespace paimon
