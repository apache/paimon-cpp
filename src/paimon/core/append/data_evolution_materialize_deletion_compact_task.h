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
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/append/data_evolution_normal_compact_task.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/result.h"

namespace paimon {

class CommitMessage;

/// Physically applies the deletion vectors of one contiguous row id range while rewriting it.
///
/// This is the heavy counterpart of `DataEvolutionNormalCompactTask`. That one keeps every
/// input row and leaves the deletions logical, so row ids survive and the vectors are merely
/// re-keyed. Materializing instead drops the deleted rows for real, which means the output
/// holds fewer rows than its inputs and cannot keep their row ids: the files are written
/// without any, and the commit assigns fresh ones the way it does for an append. So every
/// column of the range has to be rewritten together, and the global indexes over it are
/// invalidated and dropped by `DataEvolutionCompactGlobalIndexDropper` in the same commit -
/// the consequences `AppendCompactCoordinator::MaterializeDeletionVectors` documents for the
/// caller.
///
/// The vectors themselves are not moved anywhere:
/// `DataEvolutionCompactDeletionVectorRewriter` recognises a materialized task by its output
/// carrying no row id and removes the old vectors instead of re-keying them.
///
/// Scope: a range covered by a dedicated blob or vector-store file is rejected. Those files are
/// never rewritten by Paimon C++, so reassigning the row ids of the normal file alone would
/// break the alignment they are read through.
class DataEvolutionMaterializeDeletionCompactTask {
 public:
    /// Creates a task over `files`, which must cover one contiguous row id range, together with
    /// the deletion file of each — `std::nullopt` for a file that has none. At least one file
    /// has to carry deletions, or there is nothing to materialize.
    static Result<DataEvolutionMaterializeDeletionCompactTask> Create(
        const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& files,
        const std::vector<std::optional<DeletionFile>>& deletion_files);

    ~DataEvolutionMaterializeDeletionCompactTask() = default;

    const BinaryRow& Partition() const {
        return partition_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& CompactBefore() const {
        return compact_before_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& CompactAfter() const {
        return compact_after_;
    }

    const std::vector<std::optional<DeletionFile>>& DeletionFiles() const {
        return deletion_files_;
    }

    /// Rewrites the range with its deletions applied. Unlike the normal task this may produce
    /// several files, because the surviving rows are written through the table's ordinary
    /// rolling rules rather than into one file pinned to the input range.
    Result<std::shared_ptr<CommitMessage>> DoCompact(const DataEvolutionCompactContext& context);

    std::string ToString() const;

 private:
    DataEvolutionMaterializeDeletionCompactTask(
        const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& files,
        const std::vector<std::optional<DeletionFile>>& deletion_files, const Range& row_range);

    BinaryRow partition_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_before_;
    std::vector<std::optional<DeletionFile>> deletion_files_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_after_;
    Range row_range_;
};

}  // namespace paimon
