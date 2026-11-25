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
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class Bytes;
class Executor;
class FileSystem;
class MemoryPool;

/// Payload byte range relative to the index container; (-1, 0) denotes an empty index.
struct PAIMON_EXPORT FileIndexRange {
    int64_t offset;
    int64_t length;
};

/// File index metadata with exactly one container source.
struct PAIMON_EXPORT FileIndexMeta {
    /// External container path, absent for embedded indexes.
    std::optional<std::string> index_file_path;
    /// Embedded container bytes, retaining their memory pool.
    std::shared_ptr<const Bytes> embedded_index;
    /// Original field name -> index type -> range, including empty and unregistered indexes.
    std::map<std::string, std::map<std::string, FileIndexRange>> indexes;
};

/// Maps [source_start, source_start + length) to [target_start, target_start + length).
/// Row ids are zero-based physical positions within each file.
struct PAIMON_EXPORT RowIdMappingRange {
    int64_t source_start;
    int64_t target_start;
    int64_t length;
};

/// Index metadata and surviving row positions of one input file.
struct PAIMON_EXPORT FileCompactionInfo {
    std::string file_name;
    int64_t schema_id;
    int64_t source_row_count;
    int64_t live_row_count;
    /// Absent if the file has no index container.
    std::optional<FileIndexMeta> index_meta;
    /// Sorted surviving source ranges mapped into the single output file.
    std::vector<RowIdMappingRange> row_id_mapping;

    /// Looks up a source row in the planned output.
    /// @param source_row_id Zero-based physical position in the original file.
    /// @return Output row id, nullopt if deleted, or Invalid if out of range.
    Result<std::optional<int64_t>> MapRowId(int64_t source_row_id) const;
};

/// Metadata for concatenating surviving input rows into one output file.
struct PAIMON_EXPORT AppendFileCompactionPlan {
    int64_t snapshot_id;
    int64_t output_row_count;
    /// File name order (std::set iteration order), including fully deleted files.
    std::vector<FileCompactionInfo> files;
};

/// Reads index metadata and row mappings for ordered append-file compaction.
class PAIMON_EXPORT AppendFileCompactionPlanner {
 public:
    AppendFileCompactionPlanner() = delete;
    ~AppendFileCompactionPlanner() = delete;

    /// Reads indexes and applies snapshot deletion vectors in the selected partition/bucket.
    /// Supports fixed-bucket append tables without real-time mode or data evolution.
    /// If any input has indexes, all source row counts and the output row count must fit int32.
    /// @note IMPORTANT: Returned index metadata uses each file's schema.
    /// Callers must handle schema changes.
    ///
    /// Uses the same file name and physical row order as AppendFileCompaction::Execute.
    /// Callers must retain source files and use the same snapshot deletion state for execution.
    ///
    /// @param table_path Table root path.
    /// @param branch Branch name; empty selects main.
    /// @param partition Complete partition values; empty for an unpartitioned table.
    /// @param bucket Bucket containing the input files.
    /// @param file_names Nonempty set of unambiguous live file basenames.
    /// @param snapshot_id Snapshot to scan; nullopt fixes the latest snapshot once per call.
    /// @param options Overrides for the latest schema options.
    /// @param file_system File system; nullptr selects one from options.
    /// @param pool Memory pool; nullptr uses the default.
    /// @param executor Manifest scan executor; nullptr uses the default.
    /// @return Index metadata and row mappings, or an error.
    static Result<AppendFileCompactionPlan> CreatePlan(
        const std::string& table_path, const std::string& branch,
        const std::map<std::string, std::string>& partition, int32_t bucket,
        const std::set<std::string>& file_names, const std::optional<int64_t>& snapshot_id,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool,
        const std::shared_ptr<Executor>& executor);
};

}  // namespace paimon
