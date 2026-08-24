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
#include "paimon/utils/range.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class CoreOptions;
class Logger;
class ManifestList;
class PredicateFilter;
class Snapshot;

/// Plans compaction for data-evolution tables.
///
/// Files of a data-evolution table are grouped by row id range: files sharing the exact same
/// range form one evolved field group (different versions or subsets of the table columns for
/// the same rows, produced by partial-column updates). The planner packs field groups of one
/// contiguous row id run into bins and emits a `DataEvolutionNormalCompactTask` per bin, so
/// one task merges its field groups back into a single normal file holding every
/// non-dedicated column, without changing any row id.
///
/// Planning rules:
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
///
/// The row id windows a batched run compacts one at a time are planned here too, from the
/// manifest metadata rather than from the files themselves.
class DataEvolutionCompactPlanner {
 public:
    DataEvolutionCompactPlanner() = delete;
    ~DataEvolutionCompactPlanner() = delete;

    /// Plans compact tasks from the ADD file entries of one snapshot, grouped by partition.
    /// `partition_files` must contain every live file of the scanned partitions, small or
    /// large: whether a file takes part in compaction is decided here, not by a size
    /// pre-filter.
    static Result<std::vector<DataEvolutionNormalCompactTask>> PlanCompactTasks(
        const LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>>& partition_files,
        const CoreOptions& options);

    /// Plans the row id windows a batched data-evolution run compacts one at a time.
    ///
    /// The windows come from the snapshot's data manifests, which carry the row id range and the
    /// file count of their entries, so the split is decided without reading a single manifest
    /// entry. Manifests the partition filter rules out are skipped, so compacting one partition
    /// does not produce rounds that only cover other partitions' rows.
    ///
    /// Manifests are walked in row id order and the current window is closed once the files it
    /// claims reach `candidate_files_per_round` — but only where the next manifest starts beyond
    /// everything the window covers. Cutting only at such a gap keeps every file inside exactly
    /// one window, so no file is planned, rewritten and committed twice, and the row-range
    /// pruning of the per-round scan then reads each *candidate-bearing* manifest file for exactly
    /// one round. The exception is a delete-only manifest, which carries no candidate at all:
    /// it is left out of the split, and if it also lacks row id statistics every round reads it.
    ///
    /// That gap rule is the only thing keeping the rounds disjoint: `AppendOnlyFileStoreScan`
    /// applies a row id window at manifest granularity only, so a cut placed anywhere but a
    /// coverage gap would hand the same file to two rounds and have it committed twice.
    ///
    /// The windows are disjoint and the last one is open ended, so together they cover every row
    /// id the table can hold. Returns an empty vector when the snapshot carries no usable row id
    /// statistics, which asks the caller to fall back to a single unbounded round.
    static Result<std::vector<Range>> PlanRowIdWindows(
        const std::shared_ptr<ManifestList>& manifest_list, const Snapshot& snapshot,
        const std::shared_ptr<PredicateFilter>& partition_filter,
        const std::shared_ptr<arrow::Schema>& partition_schema, int64_t candidate_files_per_round,
        Logger* logger);
};

}  // namespace paimon
