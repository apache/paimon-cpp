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

/// Holds the storage a managed table reads and writes through: the file system, root path,
/// branch, options and managers. Java Paimon carries the same state in `AbstractFileStore`.
/// `FileStoreTable` builds one when it loads and asks it for the pieces each operator opens with.
///
/// The factories below take the options of the call, so an operator can serve a cache of its own
/// over the options the table was loaded with. Each call builds a fresh instance and never a
/// cached one: a snapshot manager a write armed with a catalog loader must not serve a scan.
class FileStore {
 public:
    /// Builds the store over options resolved before the call - `FileStoreTable::Create` resolves
    /// them once - so the store consumes the resolution and parses nothing itself.
    /// @param root_path Root path of the table in the file system.
    /// @param core_options The final options the table serves. The store reads and writes through
    ///        their file system and serves their branch, normalized here; both managers are built
    ///        over the two, so no manager is ever left reading through a backend its table moved
    ///        off.
    static std::shared_ptr<FileStore> Create(const std::string& root_path,
                                             CoreOptions core_options);

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

    /// The file system the table's data is accessed through: the one the options resolved, which
    /// is the one the caller opened the table through when it opened it through one of its own.
    const std::shared_ptr<FileSystem>& GetFileSystem() const {
        return file_system_;
    }

    const std::string& GetRootPath() const {
        return root_path_;
    }

    /// The final options the table serves, as `Create` took them. Every operator reads them; a
    /// copy or a branch switch resolves its own, from the inputs the table keeps.
    const CoreOptions& GetOptions() const {
        return core_options_;
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
    /// caller's partition keys and arrow schema.
    Result<std::unique_ptr<BinaryRowPartitionComputer>> NewPartitionComputer(
        const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::vector<std::string>& partition_keys, const CoreOptions& options,
        const std::shared_ptr<MemoryPool>& memory_pool) const;

 private:
    FileStore(const std::string& root_path, CoreOptions core_options);

    std::shared_ptr<FileSystem> file_system_;
    std::string root_path_;
    std::string branch_;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    /// The final options the table serves, as `Create` took them.
    CoreOptions core_options_;
};

}  // namespace paimon
