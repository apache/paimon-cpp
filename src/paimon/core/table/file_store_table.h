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
#include "paimon/core/operation/commit/commit_scanner.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace paimon {

class InternalReadContext;
class IOManager;
class ManifestFile;
class ManifestList;
class RealtimeContext;
class Schema;
class SplitGenerator;

/// The managed table a catalog hands out: one with snapshots and manifests, described by the
/// schema under its root path. A format table - a directory of plain data files - is a
/// `FormatTable`, the other kind of `Table`.
///
/// The four operators share one flow, the one Java Paimon's `AbstractFileStoreTable` carries. What
/// the table was loaded with - schema, file system, root path, branch and managers - decides what
/// every operator serves, so it is read once here and handed down. The schema's primary keys decide
/// the kind of managed table it is, `AppendOnlyFileStoreTable` or `PrimaryKeyFileStoreTable`, which
/// the `Create` factory below picks. That kind then builds the pieces the flow hands down: the
/// write itself, the scans, the read and the split generator.
class FileStoreTable : public Table {
 public:
    /// Builds the managed table the schema describes, as the kind its primary keys name: an
    /// `AppendOnlyFileStoreTable` when there are none, a `PrimaryKeyFileStoreTable` when there are.
    /// Java Paimon's `FileStoreTableFactory` picks the kind the same way. A schema change can
    /// neither add nor drop primary keys, so every schema the root path ever held names one kind.
    /// @param schema The schema the table was loaded with. Its primary keys name the kind and its
    ///        options are what the operators serve. Null when the caller had none, in which case
    ///        the table is built as the append-only kind and its operators report that none was
    ///        found.
    /// @param branch The branch the table serves, which the managers below read and write on.
    /// @param schema_manager A schema manager to carry instead of building one. See
    ///        `FileStore::Create`.
    /// @param snapshot_manager A snapshot manager to carry instead of building one. `Copy` hands
    ///        its table's over so both share one. See `FileStore::Create`.
    /// @param fs_scheme_map The scheme-to-identifier map the caller's contexts resolve custom
    ///        schemes through, which the store's options resolve through as well. `Copy` hands its
    ///        table's map over.
    /// @param dynamic_options The options a copy was given over the ones its schema stored, which
    ///        `Copy` accumulates. See the member of the same name.
    static Result<std::shared_ptr<FileStoreTable>> Create(
        const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
        const std::shared_ptr<Schema>& schema, const Identifier& identifier,
        const std::string& uuid, const std::string& branch,
        const std::shared_ptr<SchemaManager>& schema_manager,
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::map<std::string, std::string>& fs_scheme_map,
        const std::map<std::string, std::string>& dynamic_options);

    const std::string& CurrentBranch() const {
        return store_->CurrentBranch();
    }

    const std::shared_ptr<SchemaManager>& GetSchemaManager() const {
        return store_->GetSchemaManager();
    }

    const std::shared_ptr<SnapshotManager>& GetSnapshotManager() const {
        return store_->GetSnapshotManager();
    }

    std::string Name() const override {
        return identifier_.GetObjectName();
    }

    std::string FullName() const override {
        return identifier_.GetFullName();
    }

    /// Returns a copy that serves `dynamic_options`. The file system, root path and identifier
    /// stay as they are: they name this table, and an option does not rename it.
    Result<std::shared_ptr<Table>> Copy(
        const std::map<std::string, std::string>& dynamic_options) const override;

    /// Returns the table of `branch`, as Java Paimon's `switchToBranch` does. The branch switched
    /// to is served as it is: its schema is read from its own directory, so this table's dynamic
    /// options are not carried over. Switching to the branch this table already serves hands back a
    /// copy of it with the schema and the managers shared. A branch with no schema does not exist.
    Result<std::shared_ptr<Table>> SwitchToBranch(const std::string& branch) const;

    Result<std::unique_ptr<TableScan>> NewScan(std::unique_ptr<ScanContext> context) const override;

    Result<std::unique_ptr<TableRead>> NewRead(std::unique_ptr<ReadContext> context) const override;

    Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const override;

    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const override;

 private:
    /// Checks the options a context carries against the ones the table was loaded with. A table
    /// serves the options it was loaded with, so an option the context contradicts is refused: a
    /// caller that means to change them copies the table. A table a load hands back carries the
    /// context's own options merged in - `FileStoreTableFactory` merges them - so it agrees by
    /// construction. The refusal belongs to a table the caller already held, whose load is over. A
    /// key the table was not loaded with is no contradiction: it names a runtime setting, a scan
    /// mode or a batch size, which is none of the table's business and is served as given.
    static Status CheckOptionsAgreeWithTable(
        const TableSchema& table_schema, const std::map<std::string, std::string>& context_options);

    /// Checks the identity a context carries - the path it names, the identifier it gives a
    /// catalog, the table id it records - against this table's own. The store below serves this
    /// table's files, so a context naming another table would write this table's metadata and
    /// publish it as the other's; a field that contradicts the table is refused by name, the way
    /// `CheckOptionsAgreeWithTable` refuses a contradicting option. A field the context does not
    /// carry is no contradiction: a write or a commit from a bare path names no catalog object.
    /// An identifier that names a branch this table does not serve names a branch of another
    /// table, as the operator serves the branch the table was loaded with alone.
    /// @param kind The operator the context was handed to, which the error names.
    /// @param context_table_id The id `CommitContext` records for a catalog commit; a write
    ///        context carries none, so `NewWrite` passes null.
    Status CheckContextNamesThisTable(const std::string& kind, const std::string& context_root_path,
                                      const std::optional<Identifier>& context_identifier,
                                      const std::optional<std::string>& context_table_id) const;

    /// Builds the read `NewRead` hands back, from the internal context it read the schema into. The
    /// path factory comes from the store. The reader is picked by the schema the read serves - the
    /// primary keys it carries - not by the table's own kind.
    Result<std::unique_ptr<TableRead>> CreateTableRead(
        const std::shared_ptr<InternalReadContext>& internal_context,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::shared_ptr<Executor>& executor) const;

 protected:
    /// The base constructor the kinds of managed table build through. The store carries every state
    /// they hand down; see `Create` for what each piece of it is.
    FileStoreTable(const std::shared_ptr<FileStore>& store, const Identifier& identifier,
                   const std::string& uuid);

    /// Restores the committed progress of a real-time write from the latest snapshot the snapshot
    /// manager reads. It tells the write where to pick up, and both kinds ask for it the same way.
    static Status RestoreRealtimeCommittedProgress(
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::shared_ptr<SnapshotManager>& snapshot_manager, const CoreOptions& options);

    /// Returns the scan restricted to the table's root path, which the kinds of file store scan
    /// read from.
    static std::unique_ptr<FileStoreScan> WithTablePath(std::unique_ptr<FileStoreScan>&& scan,
                                                        const std::string& root_path);

    /// Builds the write of this kind of table. `NewWrite` hands the pieces of the shared flow to
    /// it. The schema and the branch the write serves are the table's own.
    /// @param io_manager The spill manager the context's temp directory names, or null when the
    ///        context named none.
    /// @param ignore_previous_files Whether the write starts over rather than appends, which the
    ///        flow resolved with this kind's own rules.
    virtual Result<std::unique_ptr<FileStoreWrite>> CreateWrite(
        const WriteContext& ctx, const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& options,
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<IOManager>& io_manager, bool ignore_previous_files) const = 0;

    /// Builds the scan of this kind of table. `NewScan` hands it the manifest list and the manifest
    /// file it built, over the table's own schema and managers.
    virtual Result<std::unique_ptr<FileStoreScan>> NewFileStoreScan(
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& core_options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& memory_pool,
        const ScanContext* context) const = 0;

    /// Builds the split generator of this kind of table, Java Paimon's `splitGenerator()`.
    virtual Result<std::unique_ptr<SplitGenerator>> NewSplitGenerator(
        const CoreOptions& core_options) const = 0;

    /// Builds the manifest entry scan supplier of this kind of table. `NewCommit` hands it the
    /// manifest list and the manifest file it built.
    virtual CommitScanner::ScanSupplier NewCommitScanSupplier(
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
        const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool) const = 0;

    /// The storage this table reads and writes through. See `FileStore`.
    std::shared_ptr<FileStore> store_;

    /// The dynamic options the table was loaded with, over the ones its schema stored. `Copy`
    /// accumulates them into the copy it derives. They stay separate from the schema they were
    /// merged into because a fallback reader needs them on their own: it serves another branch's
    /// schema and merges the same options over it again.
    std::map<std::string, std::string> dynamic_options_;
};

}  // namespace paimon
