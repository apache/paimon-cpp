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
class FileStoreTable : public Table {
 public:
    /// The managed table the schema describes, built as the kind its primary keys name: an
    /// `AppendOnlyFileStoreTable` when there are none, a `PrimaryKeyFileStoreTable` when there
    /// are, the way Java Paimon's `FileStoreTableFactory` does. Primary keys cannot be added or
    /// dropped by a schema change, so the kind is the same for every schema the root path held.
    /// @param schema The schema the table was loaded with, whose primary keys name the kind and
    ///        whose options the operators serve. Null when the caller had none, in which case the
    ///        table is built as the append-only kind and its operators report that none was found.
    /// @param branch The branch the table serves, which the managers below read and write on.
    /// @param schema_manager A schema manager to carry instead of building one - see
    ///        `FileStore::Create`.
    /// @param snapshot_manager A snapshot manager to carry instead of building one, the way `Copy`
    ///        hands its table's over so both share one - see `FileStore::Create`.
    /// @param fs_scheme_map The scheme-to-identifier map the caller's contexts resolve custom
    ///        schemes through, which the store's options resolve through as well. `Copy` hands its
    ///        table's map over.
    /// @param dynamic_options The options a copy was given over the ones its schema stored, which
    ///        `Copy` accumulates - see the member of the same name.
    static Result<std::shared_ptr<FileStoreTable>> Create(
        const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
        const std::shared_ptr<Schema>& schema, const Identifier& identifier,
        const std::string& uuid = "",
        const std::string& branch = BranchManager::DEFAULT_MAIN_BRANCH,
        const std::shared_ptr<SchemaManager>& schema_manager = nullptr,
        const std::shared_ptr<SnapshotManager>& snapshot_manager = nullptr,
        const std::map<std::string, std::string>& fs_scheme_map = {},
        const std::map<std::string, std::string>& dynamic_options = {});

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

    /// The file system, root path and identifier stay as they are: they name this table, which an
    /// option does not rename.
    Result<std::shared_ptr<Table>> Copy(
        const std::map<std::string, std::string>& dynamic_options) const override;

    /// Returns the table of `branch`, as Java Paimon's `switchToBranch` does. The branch switched
    /// to is served as it is: its schema is read from its own directory, so this table's dynamic
    /// options are not carried over. Switching to the branch already served hands back a copy of
    /// this table, schema and managers shared, and a branch with no schema does not exist.
    Result<std::shared_ptr<Table>> SwitchToBranch(const std::string& branch) const;

    Result<std::unique_ptr<TableScan>> NewScan(std::unique_ptr<ScanContext> context) const override;

    Result<std::unique_ptr<TableRead>> NewRead(std::unique_ptr<ReadContext> context) const override;

    Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const override;

    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const override;

 private:
    /// The options a context carries against the ones the table was loaded with. A table serves
    /// the options it was loaded with, so an option the context contradicts is refused rather than
    /// honoured - a caller that means to change them copies the table. A table a load hands back
    /// carries the context's own options merged in - `FileStoreTableFactory` does the merging - so
    /// it agrees by construction; the refusal belongs to a table the caller held already, whose
    /// load is over. A key the table was not loaded with is no contradiction: it names a runtime
    /// setting - a scan mode, a batch size - which is none of the table's business, and is served
    /// as given.
    static Status CheckOptionsAgreeWithTable(
        const TableSchema& table_schema, const std::map<std::string, std::string>& context_options);

    /// The read `NewRead` hands back, from the internal context it read the schema into: the path
    /// factory comes from the store, and the reader is selected by the schema the read serves - the
    /// primary keys it carries - rather than by the table's own kind.
    Result<std::unique_ptr<TableRead>> CreateTableRead(
        const std::shared_ptr<InternalReadContext>& internal_context,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::shared_ptr<Executor>& executor) const;

 protected:
    /// The base constructor the kinds of managed table build through: the store carries every state
    /// the kinds hand down - see `Create` for what each piece of it is.
    FileStoreTable(const std::shared_ptr<FileStore>& store, const Identifier& identifier,
                   const std::string& uuid);

    /// The committed progress a real-time write restores, from the latest snapshot the snapshot
    /// manager reads: where the write picks up after, which both kinds ask for the same way.
    static Status RestoreRealtimeCommittedProgress(
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::shared_ptr<SnapshotManager>& snapshot_manager, const CoreOptions& options);

    /// The scan the table's root path names, which the kinds of file store scan read from.
    static std::unique_ptr<FileStoreScan> WithTablePath(std::unique_ptr<FileStoreScan>&& scan,
                                                        const std::string& root_path);

    /// The write of this kind of table, which `NewWrite` hands the shared flow's pieces to. The
    /// schema and the branch the write serves are the table's own.
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
    /// to, over the table's own schema and managers.
    virtual Result<std::unique_ptr<FileStoreScan>> NewFileStoreScan(
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& core_options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& memory_pool,
        const ScanContext* context) const = 0;

    /// The split generator of this kind of table, Java Paimon's `splitGenerator()`.
    virtual Result<std::unique_ptr<SplitGenerator>> NewSplitGenerator(
        const CoreOptions& core_options) const = 0;

    /// The manifest entry scan supplier of this kind of table, which `NewCommit` hands the manifest
    /// list and file it built to.
    virtual CommitScanner::ScanSupplier NewCommitScanSupplier(
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::shared_ptr<ManifestList>& manifest_list,
        const std::shared_ptr<ManifestFile>& manifest_file,
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
        const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool) const = 0;

    /// The storage this table serves - see `FileStore`.
    std::shared_ptr<FileStore> store_;

    /// The dynamic options the table was loaded with over the ones its schema stored, which `Copy`
    /// accumulates into the copy it derives. They stay separate from the schema they were merged
    /// into because the fallback reader of a read needs them alone: it serves another branch's
    /// schema, over which the same options are merged again.
    std::map<std::string, std::string> dynamic_options_;
};

}  // namespace paimon
