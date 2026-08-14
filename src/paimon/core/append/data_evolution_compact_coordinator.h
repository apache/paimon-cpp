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
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/append/data_evolution_normal_compact_task.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/result.h"

namespace paimon {

class CoreOptions;

/// Plans compaction for data-evolution tables, mirroring the planning part of Java's
/// `DataEvolutionCompactCoordinator`.
///
/// Files of a data-evolution table are grouped by row id range: files sharing the exact same
/// range form one evolved field group (different versions or subsets of the table columns for
/// the same rows, produced by partial-column updates). The planner packs field groups of one
/// contiguous row id run into bins and emits a `DataEvolutionNormalCompactTask` per bin, so
/// one task merges its field groups back into a single normal file holding every
/// non-dedicated column, without changing any row id.
///
/// Planning rules (the normal-file bin packing follows Java):
/// - Blob files are dedicated storage and never enter a task. Tables holding vector-store
///   files are rejected until a VECTOR schema type allows excluding their columns from the
///   rewrite.
/// - A file group's weight is `sum(max(file_size, open_file_cost))`. A bin is emitted once its
///   weight exceeds `target-file-size` (strictly greater).
/// - A single group heavier than the target cuts the current bin and is emitted on its own —
///   provided it holds at least `compaction.min.file-num` files — so bins never span a large
///   file.
/// - A row id gap always cuts the current bin: tasks stay contiguous.
/// - A bin becomes a task only when it holds at least `compaction.min.file-num` files.
class DataEvolutionCompactCoordinator {
 public:
    DataEvolutionCompactCoordinator() = delete;
    ~DataEvolutionCompactCoordinator() = delete;

    /// Plans compact tasks from the ADD file entries of one snapshot, grouped by partition.
    /// `partition_files` must contain every live file of the scanned partitions, small or
    /// large: whether a file takes part in compaction is decided here, not by a size
    /// pre-filter.
    static Result<std::vector<DataEvolutionNormalCompactTask>> PlanCompactTasks(
        const LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>>& partition_files,
        const CoreOptions& options);
};

}  // namespace paimon
