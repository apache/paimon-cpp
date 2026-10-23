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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <memory>
#include <optional>
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class BinaryRowPartitionComputer;
class Catalog;
class IndexFileHandler;
class IndexManifestFile;
class ManifestFile;
class ManifestList;
class TableSchema;

/// The storage a managed table is served through: the file system, root path, schema, branch,
/// options and the schema and snapshot managers it was loaded with - the state Java Paimon's
/// `AbstractFileStore` carries, parsed once here rather than per operator. `FileStoreTable` hands
/// the state it was loaded with to one of these and asks it back for the pieces every operator
/// opens with.
///
/// The options are the ones the table was loaded with on this branch - the schema's own, or
/// the ones a load merged over them - the base every operator serves: `NewXxx` takes no
/// options of its own beyond them. A cache is not part of them - `WithCache()` arms a copy an
/// operator makes - so the factories below take the options of the call, not the store's own.
///
/// The factory methods build a fresh instance on every call, never a cached one: a path factory
/// names each file it builds, and a snapshot manager a write or a commit arms with a catalog
/// loader is not one a scan should be served through afterwards.
class FileStore {
 public:
    /// @param file_system The file system the caller opened the table through, which wins over
    ///        the access configuration the schema's options may carry when it is not null. A
    ///        null one hands the resolution to that configuration: the table is then served by
    ///        the file system its options name - the way a load that only had a path and a
    ///        schema to read names it - and by the default local one when they name none.
    /// @param root_path Root path of the table in the file system.
    /// @param schema The schema the table was loaded with, whose partition keys name the
    ///        partitions the paths below are built for and whose options `options()` parses. It
    ///        is null when the caller had no schema; the operators then report that none was
    ///        found.
    /// @param branch The branch the store serves, normalized here.
    /// @param schema_manager A schema manager to carry instead of building one - the one a
    ///        context dispatch read the schema through, whose cache the operators then serve.
    /// @param snapshot_manager A snapshot manager to carry instead of building one.
    /// A null manager is built over `root_path` on `branch`, so the operators always read
    /// through the managers the store carries.
    ///
    /// Building the store parses the schema's options, so it fails when they do not parse: the
    /// error is the caller's to see, which is why a store is built through this factory rather
    /// than the constructor.
    static Result<std::shared_ptr<FileStore>> Create(
        const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
        const std::shared_ptr<TableSchema>& schema, const std::string& branch,
        const std::shared_ptr<SchemaManager>& schema_manager = nullptr,
        const std::shared_ptr<SnapshotManager>& snapshot_manager = nullptr);

    /// The branch this store serves, as resolved when it was built.
    const std::string& branch() const {
        return branch_;
    }

    /// The schema manager the operators read through. It carries the schema cache of `root_path`
    /// on `branch()`.
    const std::shared_ptr<SchemaManager>& schema_manager() const {
        return schema_manager_;
    }

    /// The snapshot manager a scan reads through. A write or a commit builds its own: see
    /// `NewSnapshotManager`.
    const std::shared_ptr<SnapshotManager>& snapshot_manager() const {
        return snapshot_manager_;
    }

    /// The schema the table was loaded with, which every operator serves rather than reading the
    /// schema again. It is null when the caller had no schema.
    const std::shared_ptr<TableSchema>& schema() const {
        return schema_;
    }

    /// The file system the table's data is accessed through, as the table was opened with it:
    /// the one the options resolve - a schema carrying access configuration names its own,
    /// which the file system the caller opened the table through wins over - rather than the
    /// one that call handed over, which `specified_file_system()` hands back untouched.
    const std::shared_ptr<FileSystem>& file_system() const {
        return file_system_;
    }

    /// The file system the caller opened the table through, as it was handed to `Create`: null
    /// when the call had none, so a copy of the table resolves from its own options again and
    /// the runtime ones it merges in - a file-system option among them - can speak.
    const std::shared_ptr<FileSystem>& specified_file_system() const {
        return specified_file_system_;
    }

    /// The root path of the table in its file system, as the table was opened with it.
    const std::string& root_path() const {
        return root_path_;
    }

    /// The options the schema was loaded with on `branch()`, which every operator serves. They
    /// exist only over a schema: an operator reads them after reporting that no schema was
    /// found when there is none.
    const CoreOptions& options() const {
        assert(core_options_.has_value());
        return *core_options_;
    }

    /// The path factory an operator builds its metadata paths through: over this store's root
    /// path and the partition keys of its schema - which no schema change can alter, so a read
    /// serving a fallback branch's schema still names the same partitions. The file format
    /// identifier is the caller's to name: a write names the format its level-0 files are
    /// written in, which the options may set per level, while a scan, a read and a commit name
    /// the table's format.
    Result<std::shared_ptr<FileStorePathFactory>> NewPathFactory(
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
        const std::string& file_format_identifier,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// The snapshot manager a write or a commit reads snapshots through, over this store's root
    /// path and branch. The file system is the caller's to name, from the options its operator
    /// serves: a table opened without one has its options build the resolving file system. A
    /// catalog that version-manages the table hands its snapshots out itself, so its loader is
    /// attached, the catalog captured to keep the versioned interface - and the loader with it
    /// - alive. Such a catalog names the table by an identifier, which the context must have
    /// supplied.
    Result<std::shared_ptr<SnapshotManager>> NewSnapshotManager(
        const CoreOptions& options, const std::shared_ptr<Catalog>& catalog,
        const std::optional<Identifier>& catalog_identifier) const;

    /// The manifest list a scan or a commit reads the table's manifest entries through, over
    /// the caller's path factory: an operator's manifests sit in the paths its path factory
    /// names.
    Result<std::shared_ptr<ManifestList>> NewManifestList(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// The manifest file a scan or a commit reads a single manifest's entries through, over
    /// the caller's path factory and the partition schema its entries are pruned by.
    Result<std::shared_ptr<ManifestFile>> NewManifestFile(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<arrow::Schema>& partition_schema,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// The index manifest file a write, a scan or a commit reads the table's index manifest
    /// through, over the caller's path factory.
    Result<std::unique_ptr<IndexManifestFile>> NewIndexManifestFile(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// The index file handler a write's deletion vectors and indexes are maintained through, or
    /// a scan reads them through: the index manifest file above and the path factories over the
    /// caller's path factory, in one handler.
    Result<std::unique_ptr<IndexFileHandler>> NewIndexFileHandler(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// The partition computer a commit names the partitions it writes with, over the partition
    /// keys of this store's schema - which no schema change can alter, so a commit serving a
    /// schema read after the table was loaded still names the same partitions - and the
    /// caller's arrow schema and options.
    Result<std::unique_ptr<BinaryRowPartitionComputer>> NewPartitionComputer(
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

 private:
    FileStore(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
              const std::shared_ptr<TableSchema>& schema, const std::string& branch,
              const std::shared_ptr<SchemaManager>& schema_manager,
              const std::shared_ptr<SnapshotManager>& snapshot_manager);

    std::shared_ptr<FileSystem> file_system_;
    /// The file system the caller opened the table through, as handed to `Create` - null when
    /// the call had none - kept for a copy to hand over again: what `file_system_` holds is
    /// resolved, and would pin the copy to this table's resolution.
    std::shared_ptr<FileSystem> specified_file_system_;
    std::string root_path_;
    std::shared_ptr<TableSchema> schema_;
    std::string branch_;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    /// The options of `schema_` on `branch_`, or unset when there is no schema. `Create` parses
    /// them: the constructor cannot report a parse failure.
    std::optional<CoreOptions> core_options_;
};

}  // namespace paimon
