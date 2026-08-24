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

#include <memory>
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/logging.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
class Field;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace paimon {

class DataFilePathFactory;
class MemoryPool;
class OutputStream;

/// Externalizes managed blob values of a primary-key table before they enter the merge-tree
/// write buffer.
///
/// Only top-level scalar BLOB columns are managed. `ARRAY<BLOB>` and `MAP<K, BLOB>` fields are
/// expressible in Paimon C++'s type system but not handled by the schema validation and the
/// write/read paths yet, and descriptors are always read and copied through the table's own
/// file system (`blob-descriptor.source-table` is rejected instead).
///
/// Each managed blob field owns a rolling pack writer producing `.managed.blob` files in the
/// blob file format. Every non-null value of an insert row is copied into the current pack,
/// even when the input already is a serialized descriptor (the payload is re-materialized into
/// a pack this table owns), and the buffered column value becomes the serialized
/// `BlobDescriptor` pointing at the copied payload. Retract rows never write a payload, their
/// blob value is nulled out.
///
/// Packs created since the last `PrepareCommit` are uncommitted: `Abort` (and `Close` without
/// a commit) deletes them, while `PrepareCommit` seals them and hands them over, returning
/// their paths. The committed data files' `.blobref` sidecars record which packs are
/// referenced.
///
/// Those returned paths, and only those, are what a failed commit rolls back: they travel with
/// the commit message and `UncommittedFileCleaner`, which documents why ownership may not be
/// derived from a sidecar, deletes them. Nothing else collects a pack — orphan file cleaning
/// skips `.managed.blob` because several data files may share one — so the hand-over has to
/// stay paired with the rollback.
class PrimaryKeyBlobExternalizer {
 public:
    /// Creates an externalizer for `value_schema`, or nullptr when the schema holds no managed
    /// blob field.
    static Result<std::unique_ptr<PrimaryKeyBlobExternalizer>> Create(
        const CoreOptions& options, const std::shared_ptr<arrow::Schema>& value_schema,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& pool);

    ~PrimaryKeyBlobExternalizer();

    /// Rewrites the managed blob columns of `batch`: payloads move to pack files and the
    /// columns hold serialized descriptors afterwards. On failure all uncommitted packs are
    /// deleted and the error is returned.
    Result<std::unique_ptr<RecordBatch>> Externalize(std::unique_ptr<RecordBatch>&& batch);

    /// Seals the currently open packs and hands over every pack written since the last call, so
    /// a later `Abort` or `Close` no longer deletes them. Call after the write buffer has been
    /// flushed, right before the commit messages are handed out.
    ///
    /// @return The paths of the handed-over packs, in creation order. The caller owns them from
    ///     now on and has to delete them if its commit never lands.
    Result<std::vector<std::string>> PrepareCommit();

    /// Closes the open pack writers and deletes every uncommitted pack.
    void Abort();

 private:
    /// A rolling writer for the packs of one managed blob field.
    class ManagedBlobPackWriter;

    PrimaryKeyBlobExternalizer(const CoreOptions& options,
                               const std::shared_ptr<arrow::DataType>& value_type,
                               std::vector<int32_t> managed_field_indices,
                               const std::shared_ptr<DataFilePathFactory>& path_factory,
                               const std::shared_ptr<MemoryPool>& pool);

    Status CloseCurrentWriters();

 private:
    CoreOptions options_;
    std::shared_ptr<arrow::DataType> value_type_;
    std::vector<int32_t> managed_field_indices_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<MemoryPool> pool_;
    /// Allocates the descriptor columns of externalized batches. Those buffers flow into the
    /// merge-tree write buffer, so the pool must live as long as the externalizer — the
    /// owning writer keeps the externalizer alive until its buffered batches are released.
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;

    std::unique_ptr<Logger> logger_;

    /// One pack writer per managed field, aligned with managed_field_indices_.
    std::vector<std::unique_ptr<ManagedBlobPackWriter>> pack_writers_;
    /// Paths of packs not yet handed over by PrepareCommit; deleted on Abort.
    std::vector<std::string> uncommitted_packs_;
};

}  // namespace paimon
