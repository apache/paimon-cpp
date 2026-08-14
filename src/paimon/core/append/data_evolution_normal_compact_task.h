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

#include "paimon/common/data/binary_row.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/result.h"
#include "paimon/utils/range.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class CommitMessage;
class Executor;
class FileStorePathFactory;
class MemoryPool;
class TableSchema;

/// Everything a data-evolution compact task needs to rewrite its files, shared by all tasks of
/// one coordinator run.
struct DataEvolutionCompactContext {
    std::string table_path;
    std::shared_ptr<TableSchema> table_schema;
    std::shared_ptr<arrow::Schema> arrow_schema;
    CoreOptions core_options;
    std::shared_ptr<FileStorePathFactory> path_factory;
    std::shared_ptr<Executor> executor;
    std::shared_ptr<MemoryPool> pool;
};

/// Compaction task for data-evolution tables.
///
/// The files to compact cover one contiguous row id range and may span several evolved field
/// groups: files sharing the exact same row range whose columns are different versions or
/// different subsets of the table fields, produced by partial-column updates. The task reads the
/// files through `DataEvolutionSplitRead`, which merges the field groups so the newest version
/// of every column wins, and rewrites the merged rows into a single new file.
///
/// Row ids are never changed: the output file keeps the input group's first row id and its
/// merged `[min, max]` sequence number range, so `_ROW_ID` values derived from manifest
/// metadata and the "newest field group wins" ordering stay intact. Preserving row ids is
/// also a prerequisite for row-id-keyed deletion vectors, whose commit-time migration is not
/// ported yet: the compaction entry point still rejects deletion-vector tables. Blob files
/// are dedicated storage and are not part of a task; their row ranges remain covered by the
/// rewritten data file.
class DataEvolutionNormalCompactTask {
 public:
    /// Creates a task after validating that `files` cover one contiguous row id range.
    static Result<DataEvolutionNormalCompactTask> Create(
        const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& files);

    ~DataEvolutionNormalCompactTask() = default;

    const BinaryRow& Partition() const {
        return partition_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& CompactBefore() const {
        return compact_before_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& CompactAfter() const {
        return compact_after_;
    }

    /// The contiguous row id range covered by the task's input files.
    const Range& RowRange() const {
        return row_range_;
    }

    Result<std::shared_ptr<CommitMessage>> DoCompact(const DataEvolutionCompactContext& context);

    std::string ToString() const;

 private:
    DataEvolutionNormalCompactTask(const BinaryRow& partition,
                                   const std::vector<std::shared_ptr<DataFileMeta>>& files,
                                   const Range& row_range);

    /// Checks that the row id ranges of `files` merge into one contiguous range and returns it.
    static Result<Range> CheckContiguousRowRange(
        const std::vector<std::shared_ptr<DataFileMeta>>& files);

 private:
    BinaryRow partition_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_before_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_after_;
    Range row_range_;
};

}  // namespace paimon
