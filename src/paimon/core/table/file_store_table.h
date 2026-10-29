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
#include "paimon/core/schema/table_schema.h"
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
    /// The final options are resolved here once, before the store is built: the schema's own
    /// options as `branch` restyles them - the main branch carries no `branch` option, another
    /// branch names itself - are parsed, and a parse failure is this call's error. Every operator
    /// and both managers are built over that one resolution.
    /// @param specified_file_system The file system the caller opened the table through. It
    ///        wins over the access configuration in the schema's options. Null leaves the
    ///        resolution to that configuration, and to the default local file system when the
    ///        options name none. Kept so `Copy` and `SwitchToBranch` resolve through it again.
    /// @param schema The schema the table was loaded with, a `TableSchema` as every managed
    ///        table's is. Its primary keys name the kind and its options are what the operators
    ///        serve. Null is refused: a path a load found no schema under hands back no table,
    ///        and the failure belongs to the call that opens one.
    /// @param branch The branch the table serves, which the managers below read and write on.
    /// @param fs_scheme_map The scheme-to-identifier map the caller's contexts resolve custom
    ///        schemes through, which the options resolve through as well. Kept so `Copy` and
    ///        `SwitchToBranch` resolve through it again.
    /// @param dynamic_options The options a copy was given over the ones its schema stored, which
    ///        `Copy` accumulates. See the member of the same name.
    static Result<std::shared_ptr<FileStoreTable>> Create(
        const std::shared_ptr<FileSystem>& specified_file_system, const std::string& root_path,
        const std::shared_ptr<TableSchema>& schema, const Identifier& identifier,
        const std::string& uuid, const std::string& branch,
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

    /// Returns a copy that serves `dynamic_options`. The root path stays as it is: it names this
    /// table, and an option does not rename it. The `type` option is immutable the way Java
    /// Paimon's is - a value naming the type the schema's primary keys already name, whatever its
    /// case, is dropped, and any other value is refused - and the `branch` option names the branch
    /// the copy serves: one on the branch this table serves keeps this table's schema, while one
    /// on another branch reads that branch's own latest schema, carries the identifier of that
    /// branch the way a load's routing does, and refuses a branch holding no schema. The copy
    /// resolves the file system the caller opened this table through, if any, together with its
    /// own merged options, and builds its managers over that resolution rather than taking this
    /// table's over.
    Result<std::shared_ptr<Table>> Copy(
        const std::map<std::string, std::string>& dynamic_options) const override;

    /// Returns the table of `branch`, as Java Paimon's `switchToBranch` does. The branch switched
    /// to is served as it is: its schema is read from its own directory and its managers are built
    /// over the file system that schema resolves, so neither this table's dynamic options nor its
    /// managers are carried over. Switching to the branch this table already serves hands back a
    /// copy of it with the schema shared and the managers built over the same resolution again. A
    /// branch with no schema does not exist.
    Result<std::shared_ptr<Table>> SwitchToBranch(const std::string& branch) const;

    Result<std::unique_ptr<TableScan>> NewScan(std::unique_ptr<ScanContext> context) const override;

    Result<std::unique_ptr<TableRead>> NewRead(std::unique_ptr<ReadContext> context) const override;

    Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const override;

    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const override;

 private:
    /// Checks the options a context carries against the configuration the table serves: its
    /// schema's options with its dynamic options on top. A table serves that configuration and
    /// nothing else - the operators read it and add no context option to it - so an option the
    /// context contradicts is refused, and so is one the table was not loaded with: serving it
    /// would drop the context value in silence behind the table's own configuration. A caller
    /// that means to change the configuration copies the table. A table a load hands back carries
    /// the context's own options merged in - `FileStoreTableFactory` merges them, whatever the
    /// key, as it merges a `header.` key the transport reads - so it agrees by construction; the
    /// refusal belongs to a table the caller already held, whose load is over. The keys a load
    /// routes with (`branch`, `type`) are folded into the table there - by the load or by `Copy` -
    /// and checked against the identifier and the schema instead.
    Status CheckOptionsAgreeWithTable(
        const TableSchema& table_schema,
        const std::map<std::string, std::string>& context_options) const;

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
    /// The base constructor the kinds of managed table build through. The store carries the
    /// storage they hand down; `schema` is the schema the table was loaded with, which the `Table`
    /// base holds and the operators read. See `Create` for what each piece is.
    FileStoreTable(const std::shared_ptr<FileStore>& store,
                   const std::shared_ptr<TableSchema>& schema, const Identifier& identifier,
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

    /// The file system the caller opened this table through, as the options were resolved with -
    /// null when the load had none. A copy and a branch switch resolve their own options, and the
    /// resolution that built this table's store is one-way: neither the file system it produced
    /// nor the options hand the inputs back, so they are kept here for the next resolution.
    std::shared_ptr<FileSystem> specified_file_system_;

    /// The scheme-to-identifier map the options were resolved with, kept for the same next
    /// resolution `specified_file_system_` is kept for.
    std::map<std::string, std::string> fs_scheme_map_;
};

}  // namespace paimon
