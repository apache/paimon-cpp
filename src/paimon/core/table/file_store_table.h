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

/// The managed table a catalog hands out: one with snapshots and manifests, which the schema under
/// its root path describes. A format table - a directory of plain data files - is a `FormatTable`,
/// the other kind of `Table`.
///
/// This is the flow the four operators share, Java Paimon's `AbstractFileStoreTable`: what the
/// table was constructed with - the schema, file system, root path, branch and managers - decides
/// what every operator serves, so it is read here once and handed down. The schema's primary keys
/// decide the kind of managed table it is - `AppendOnlyFileStoreTable` or
/// `PrimaryKeyFileStoreTable`, the `Create` factory below - and that kind builds the pieces the
/// flows hand down: the write itself, the scans, the read and the split generator.
///
/// Its `NewScan`/`NewRead`/`NewWrite`/`NewCommit` build the operators here, from the context plus
/// the state the table was constructed with, so they serve the table as it was loaded. The
/// context-only entry points - `TableScan::Create()`, `TableRead::Create()`,
/// `FileStoreWrite::Create()` and `FileStoreCommit::Create()` - load the table the context names
/// and hand the context to that table's `NewXxx`, which is how a context built without a table
/// reaches the same operators.
class FileStoreTable : public Table {
 public:
    /// The managed table the schema describes, built as the kind its primary keys name: an
    /// `AppendOnlyFileStoreTable` when there are none, a `PrimaryKeyFileStoreTable` when there
    /// are, the way Java Paimon's `FileStoreTableFactory` does. Primary keys cannot be added or
    /// dropped by a schema change, so the kind the table was built as is the kind every schema
    /// its root path ever held describes.
    /// @param schema The schema the table was loaded with, whose primary keys name the kind and
    ///        whose options the operators serve. It is null when the caller had no schema, and
    ///        the table is built as the append-only kind - its operators then report that no
    ///        schema was found.
    /// @param branch The branch the table serves, which the managers below read and write on.
    /// @param schema_manager A schema manager to carry instead of building one - the one a
    ///        context dispatch read the schema through, whose cache the operators then serve.
    /// @param snapshot_manager A snapshot manager to carry instead of building one, the way
    ///        `Copy` hands its table's over so both share one.
    /// A null manager is built over `root_path` on `branch`, so the operators always read
    /// through the managers the table carries.
    /// @param fs_scheme_map The scheme-to-identifier map the caller's contexts resolve custom
    ///        schemes through, which the store's options resolve through as well - see
    ///        `FileStore::Create`. `Copy` hands its table's map over.
    static Result<std::shared_ptr<FileStoreTable>> Create(
        const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
        const std::shared_ptr<Schema>& schema, const Identifier& identifier,
        const std::string& uuid = "",
        const std::string& branch = BranchManager::DEFAULT_MAIN_BRANCH,
        const std::shared_ptr<SchemaManager>& schema_manager = nullptr,
        const std::shared_ptr<SnapshotManager>& snapshot_manager = nullptr,
        const std::map<std::string, std::string>& fs_scheme_map = {});

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
    /// The read `NewRead` hands back, from the internal context it read the schema into: the
    /// path factory comes from the store - a fallback branch's schema carries the table's
    /// partition keys, which no schema change can alter - and the read itself is the kind's,
    /// `NewTableRead`.
    Result<std::unique_ptr<TableRead>> CreateTableRead(
        const std::shared_ptr<InternalReadContext>& internal_context,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::shared_ptr<Executor>& executor) const;

 protected:
    /// The base constructor the kinds of managed table build through, `Create` above: the store
    /// the table was loaded with carries every state the kinds hand down - see `Create` for what
    /// each piece of it is.
    FileStoreTable(const std::shared_ptr<FileStore>& store, const Identifier& identifier,
                   const std::string& uuid);

    /// The committed progress a real-time write restores, from the latest snapshot the snapshot
    /// manager reads: where the write picks up after, which both kinds of real-time write ask
    /// for the same way.
    static Status RestoreRealtimeCommittedProgress(
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::shared_ptr<SnapshotManager>& snapshot_manager, const CoreOptions& options);

    /// The scan the table's root path names, which the kinds of file store scan read from: the
    /// kinds hand their scans back through it.
    static std::unique_ptr<FileStoreScan> WithTablePath(std::unique_ptr<FileStoreScan>&& scan,
                                                        const std::string& root_path);

    /// The write of this kind of table, which `NewWrite` hands the shared flow's pieces to: the
    /// kind's writer, built over the path factory, snapshot manager and options that flow
    /// resolved. The schema is the store's own, the one the table was loaded with, and the
    /// branch the write serves is the table's, `CurrentBranch()`.
    /// @param io_manager The spill manager the context's temp directory names, or null when the
    ///        context named none.
    /// @param ignore_previous_files Whether the write starts over rather than appending, which
    ///        the flow resolved with this kind's own rules.
    virtual Result<std::unique_ptr<FileStoreWrite>> CreateWrite(
        const WriteContext& ctx, const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& options,
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<IOManager>& io_manager, bool ignore_previous_files) const = 0;

    /// The scan of this kind of table, which `NewScan` hands the manifest list and file it built
    /// to. The schema is the store's own and the snapshot and schema managers are the table's
    /// own, which the scan serves.
    virtual Result<std::unique_ptr<FileStoreScan>> NewFileStoreScan(
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& core_options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& memory_pool,
        const ScanContext* context) const = 0;

    /// The split generator of this kind of table, Java Paimon's `splitGenerator()`. The schema
    /// it reads its kind's keys from is the store's own.
    virtual Result<std::unique_ptr<SplitGenerator>> NewSplitGenerator(
        const CoreOptions& core_options) const = 0;

    /// The read of this kind of table, which `NewRead` hands the internal context it read the
    /// schema into and the path factory it built to.
    virtual Result<std::unique_ptr<TableRead>> NewTableRead(
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<InternalReadContext>& internal_context,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::shared_ptr<Executor>& executor) const = 0;

    /// The manifest entry scan supplier of this kind of table, which `NewCommit` hands the
    /// manifest list and file it built to. The schema is the store's own, and the schema manager
    /// the scans it hands out read through is the table's own.
    virtual CommitScanner::ScanSupplier NewCommitScanSupplier(
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
        const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool) const = 0;

    /// The storage this table serves: the file system, root path, schema, branch and managers it
    /// was loaded with, and the factories that build the pieces every operator opens with.
    std::shared_ptr<FileStore> store_;
};

}  // namespace paimon
