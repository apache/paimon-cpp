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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/result.h"

namespace paimon {

class CommitMessage;
class FileSystem;
class MemoryPool;

/// Compact coordinator for append-only unaware-bucket tables.
///
/// This coordinator scans the latest snapshot for small files, groups them by partition,
/// and generates compaction tasks using a bin-packing algorithm. It then synchronously
/// executes all tasks and returns the resulting commit messages.
///
/// For a data-evolution table (`data-evolution.enabled` = true) the coordinator instead plans
/// with `DataEvolutionCompactPlanner`: files are grouped by row id range into evolved
/// field groups, each task merges the field groups of one contiguous row id run into a single
/// normal file holding every non-dedicated column, and row ids and file-level sequence number
/// ranges are preserved.
///
/// A data-evolution table may also enable `deletion-vectors.enabled`, with either vector
/// kind. The rewrite keeps every input row and preserves row ids, so the deletions of the
/// replaced files are re-keyed onto the rewritten ones and committed in the same snapshot.
/// Deletion vectors on a plain append table are still unsupported.
///
/// @note This implementation does not support streaming mode. It only scans the current
///       latest snapshot (batch mode).
class PAIMON_EXPORT AppendCompactCoordinator {
 public:
    AppendCompactCoordinator() = delete;
    ~AppendCompactCoordinator() = delete;
    /// Run the compaction coordinator.
    ///
    /// Scans the latest snapshot across the specified partitions — small files only for a
    /// plain append table, every live file for a data-evolution table — generates compact
    /// tasks, executes them synchronously, and returns the resulting commit messages.
    ///
    /// @param table_path The root path of the table.
    /// @param options User-defined options, merged over the schema options. Options that
    ///                decide the compaction path or the physical layout (row tracking, data
    ///                evolution, deletion vectors, bucket and the blob layout fields) must
    ///                not change the persisted values; such an override is rejected.
    /// @param partitions Partition filters; each element is a partition spec as key-value pairs.
    ///                   Empty vector means all partitions.
    /// @param file_system The file system to use. If nullptr, will be created from options.
    /// @param pool The memory pool to use. If nullptr, will use default pool.
    /// @return Result containing a vector of commit messages from compaction tasks. On a
    ///         data-evolution table with deletion vectors the vector also holds index-only
    ///         messages that re-key the deletions of the replaced files. The caller must
    ///         commit the whole vector in one commit: committing only the data messages
    ///         publishes the rewritten files without their deletions and brings deleted rows
    ///         back. Use `RunAndCommit` to have the commit handled here instead.
    static Result<std::vector<std::shared_ptr<CommitMessage>>> Run(
        const std::string& table_path, const std::map<std::string, std::string>& options,
        const std::vector<std::map<std::string, std::string>>& partitions,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool);

    /// Default target for the number of files one `RunAndCommit` round plans over.
    ///
    /// A soft target, not a bound: a round can only be cut where one data manifest's row id
    /// coverage ends before the next one's begins, so a single large manifest — or manifests
    /// whose row id ranges overlap — keeps its files in one round however many they are.
    static constexpr int64_t kDefaultCandidateFilesPerRound = 100000;

    /// Run the compaction coordinator and commit its results.
    ///
    /// Unlike `Run`, which plans the whole snapshot at once and hands the messages back, this
    /// splits a data-evolution table's row id space into rounds targeting
    /// `candidate_files_per_round` files and commits each round on its own, so one round's file
    /// metadata is held at a time rather than the whole table's. The target is soft, as
    /// `kDefaultCandidateFilesPerRound` documents, and a snapshot whose manifests carry no
    /// usable row id statistics falls back to a single round. Rounds are independent: if one
    /// fails, the rounds already committed stay committed and a later call re-plans whatever
    /// is left.
    ///
    /// A plain append table has no row id space to split on, so it runs as a single round and
    /// only gains the commit.
    ///
    /// @param table_path The root path of the table.
    /// @param options User-defined options, merged over the schema options, with the same
    ///                immutable-option rules as `Run`.
    /// @param partitions Partition filters; each element is a partition spec as key-value pairs.
    ///                   Empty vector means all partitions.
    /// @param commit_user The user recorded on every snapshot this call commits.
    /// @param file_system The file system to use. If nullptr, will be created from options.
    /// @param pool The memory pool to use. If nullptr, will use default pool.
    /// @param candidate_files_per_round Target number of files one round plans over, honored
    ///                                  only where the manifests allow a cut. Must be positive.
    /// @return The number of rounds that produced a commit.
    static Result<int32_t> RunAndCommit(
        const std::string& table_path, const std::map<std::string, std::string>& options,
        const std::vector<std::map<std::string, std::string>>& partitions,
        const std::string& commit_user, const std::shared_ptr<FileSystem>& file_system = nullptr,
        const std::shared_ptr<MemoryPool>& pool = nullptr,
        int64_t candidate_files_per_round = kDefaultCandidateFilesPerRound);

    /// Physically applies the deletion vectors of a data-evolution table and commits.
    ///
    /// This is the heavy alternative to what `Run` and `RunAndCommit` do. They keep every row
    /// and leave the deletions logical, so row ids survive; this drops the deleted rows for
    /// real, which means the surviving rows get **new** row ids assigned by the commit. Three
    /// consequences follow, and they are why it is never done automatically:
    ///
    /// - Every column of a rewritten range is rewritten together.
    /// - `_ROW_ID` values change, so anything holding a reference to one has to look it up
    ///   again.
    /// - Global indexes of the touched partitions are invalidated and dropped in the same
    ///   commit; they have to be rebuilt afterwards.
    ///
    /// Only the row ranges that actually carry deletions are rewritten. A table with no
    /// deletion vectors is a no-op.
    ///
    /// The work is split into rounds over the row id space the same way `RunAndCommit` splits
    /// compaction, and with the same soft target, so one round's file metadata is held at a
    /// time and each round commits on its own. Rounds are independent: if one fails, the
    /// rounds already committed stay committed and a later call re-plans whatever is left.
    ///
    /// @param table_path The root path of the table.
    /// @param options User-defined options, merged over the schema options, with the same
    ///                immutable-option rules as `Run`.
    /// @param partitions Partition filters; each element is a partition spec as key-value pairs.
    ///                   Empty vector means all partitions.
    /// @param commit_user The user recorded on every snapshot this call commits.
    /// @param file_system The file system to use. If nullptr, will be created from options.
    /// @param pool The memory pool to use. If nullptr, will use default pool.
    /// @return The number of rounds that produced a commit, 0 when there was nothing to do.
    /// @note Not supported for a row range covered by a dedicated blob or vector-store file:
    ///       Paimon C++ does not rewrite those, so their row ids cannot be reassigned in step
    ///       with the rows they belong to, and such a range is rejected rather than corrupted.
    static Result<int32_t> MaterializeDeletionVectors(
        const std::string& table_path, const std::map<std::string, std::string>& options,
        const std::vector<std::map<std::string, std::string>>& partitions,
        const std::string& commit_user, const std::shared_ptr<FileSystem>& file_system = nullptr,
        const std::shared_ptr<MemoryPool>& pool = nullptr);
};

}  // namespace paimon
