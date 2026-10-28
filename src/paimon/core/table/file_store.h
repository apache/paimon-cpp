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

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

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

/// Holds the storage a managed table reads and writes through: the file system, root path,
/// schema, branch, options and managers. Java Paimon carries the same state in `AbstractFileStore`.
/// `FileStoreTable` builds one when it loads and asks it for the pieces each operator opens with.
///
/// The factories below take the options of the call, so an operator can serve a cache of its own
/// over the options the table was loaded with. Each call builds a fresh instance and never a
/// cached one: a snapshot manager a write armed with a catalog loader must not serve a scan.
class FileStore {
 public:
    /// Builds the store. It parses the schema's options and fails when they do not parse, so the
    /// constructor is private and callers come here.
    /// @param file_system The file system the caller opened the table through. It wins over the
    ///        access configuration in the schema's options. Null leaves the resolution to that
    ///        configuration, and to the default local file system when the options name none.
    /// @param root_path Root path of the table in the file system.
    /// @param schema The schema the table was loaded with. Its partition keys name the partitions
    ///        the paths below are built for, and `GetOptions()` parses its options. Null when the
    ///        caller had none, which the operators then report.
    /// @param branch The branch the store serves, normalized here.
    /// @param schema_manager A schema manager to carry instead of building one: the one a context
    ///        dispatch read the schema through, whose cache the operators then serve.
    /// @param snapshot_manager A snapshot manager to carry instead of building one. Null builds one
    ///        over `root_path` on `branch`.
    /// @param fs_scheme_map The scheme-to-identifier map the caller's contexts resolve custom
    ///        schemes through, kept so a copy resolves as this store does.
    static Result<std::shared_ptr<FileStore>> Create(
        const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
        const std::shared_ptr<TableSchema>& schema, const std::string& branch,
        const std::shared_ptr<SchemaManager>& schema_manager,
        const std::shared_ptr<SnapshotManager>& snapshot_manager,
        const std::map<std::string, std::string>& fs_scheme_map);

    /// Returns whether a copy's dynamic options change the file system its store resolves to. When
    /// they do, the copy must not inherit the managers of the table it was copied from: they were
    /// built over that table's resolution and would keep reading metadata through the backend the
    /// copy left.
    static bool FileSystemResolutionMayChange(
        const TableSchema& schema, const std::map<std::string, std::string>& dynamic_options);

    /// Returns whether two options maps resolve to the same file system. Every resolution key has
    /// to agree, including a key missing on both sides.
    static bool FileSystemResolutionAgrees(const std::map<std::string, std::string>& left,
                                           const std::map<std::string, std::string>& right);

    const std::string& CurrentBranch() const {
        return branch_;
    }

    /// The schema manager the operators read through. It carries the schema cache of `root_path`
    /// on `CurrentBranch()`.
    const std::shared_ptr<SchemaManager>& GetSchemaManager() const {
        return schema_manager_;
    }

    /// The snapshot manager a scan reads through. A write or a commit builds its own: see
    /// `NewSnapshotManager`.
    const std::shared_ptr<SnapshotManager>& GetSnapshotManager() const {
        return snapshot_manager_;
    }

    /// The schema the table was loaded with, null when the caller had none.
    const std::shared_ptr<TableSchema>& GetSchema() const {
        return schema_;
    }

    /// The file system the table's data is accessed through: the one the options resolve, unless
    /// the caller opened the table through one of its own.
    const std::shared_ptr<FileSystem>& GetFileSystem() const {
        return file_system_;
    }

    /// The file system the caller opened the table through, as handed to `Create`. Null when the
    /// call had none, so a copy resolves from its own options again.
    const std::shared_ptr<FileSystem>& GetSpecifiedFileSystem() const {
        return specified_file_system_;
    }

    /// The scheme-to-identifier map the options were resolved with, kept so a copy can resolve
    /// through it again.
    const std::map<std::string, std::string>& GetFsSchemeMap() const {
        return fs_scheme_map_;
    }

    const std::string& GetRootPath() const {
        return root_path_;
    }

    /// The options of the schema loaded on `CurrentBranch()`. They exist only over a schema, so an
    /// operator reports that no schema was found before it ever reads them.
    const CoreOptions& GetOptions() const {
        assert(core_options_.has_value());
        return *core_options_;
    }

    /// Builds the path factory an operator names its metadata paths through, over this store's
    /// root path. The caller names the partition keys of the schema the arrow schema was converted
    /// from: an operator serving the table's own schema names this store's partition keys, while
    /// a read of a fallback branch names the keys of that branch's schema, which is not this
    /// store's. The caller names the file format identifier: a write names the format of its
    /// level-0 files, which the options may set per level, while a scan, a read and a commit name
    /// the table's format.
    Result<std::shared_ptr<FileStorePathFactory>> NewPathFactory(
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::vector<std::string>& partition_keys, const CoreOptions& options,
        const std::string& file_format_identifier,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// Builds the snapshot manager a write or a commit reads snapshots through, over this store's
    /// root path and branch. The caller names the file system, from the options its operator
    /// serves. A catalog that version-manages the table hands out its own snapshots, so its loader
    /// is attached and the catalog is captured to keep it alive. Such a catalog names the table by
    /// an identifier, which the context must have supplied.
    Result<std::shared_ptr<SnapshotManager>> NewSnapshotManager(
        const CoreOptions& options, const std::shared_ptr<Catalog>& catalog,
        const std::optional<Identifier>& catalog_identifier) const;

    /// Builds the manifest list a scan or a commit reads the table's manifest entries through.
    Result<std::shared_ptr<ManifestList>> NewManifestList(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// Builds the manifest file a scan or a commit reads a single manifest's entries through,
    /// pruned by `partition_schema`.
    Result<std::shared_ptr<ManifestFile>> NewManifestFile(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<arrow::Schema>& partition_schema,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// Builds the index manifest file a write, a scan or a commit reads the table's index
    /// manifest through.
    Result<std::unique_ptr<IndexManifestFile>> NewIndexManifestFile(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// Builds the index file handler a write maintains its deletion vectors and indexes through,
    /// or a scan reads them through.
    Result<std::unique_ptr<IndexFileHandler>> NewIndexFileHandler(
        const CoreOptions& options, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

    /// Builds the partition computer a commit names the partitions it writes with, over the
    /// partition keys of this store's schema and the caller's arrow schema.
    Result<std::unique_ptr<BinaryRowPartitionComputer>> NewPartitionComputer(
        const std::shared_ptr<arrow::Schema>& arrow_schema, const CoreOptions& options,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

 private:
    FileStore(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
              const std::shared_ptr<TableSchema>& schema, const std::string& branch,
              const std::shared_ptr<SchemaManager>& schema_manager,
              const std::shared_ptr<SnapshotManager>& snapshot_manager,
              const std::map<std::string, std::string>& fs_scheme_map);

    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<FileSystem> specified_file_system_;
    std::map<std::string, std::string> fs_scheme_map_;
    std::string root_path_;
    std::shared_ptr<TableSchema> schema_;
    std::string branch_;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    /// The options of `schema_` on `branch_`, unset when there is no schema.
    std::optional<CoreOptions> core_options_;
};

}  // namespace paimon
