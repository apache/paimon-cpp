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

#include "paimon/global_index/global_index_result.h"
#include "paimon/result.h"
#include "paimon/table/source/full_text_read.h"
#include "paimon/table/source/full_text_scan.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;

/// Builds a table-level full-text search of a data-evolution table, aligned with Java
/// `org.apache.paimon.table.source.FullTextSearchBuilder`. It searches the global full-text
/// indexes of one column and returns the highest-scoring rows:
///
/// @code
/// FullTextSearchBuilder builder(table_path);
/// builder.SetQuery("content", R"({"match":{"query":"paimon lake"}})").SetLimit(10);
/// PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> result,
///                        builder.ExecuteLocal());
/// ScanContextBuilder scan_builder(table_path);
/// scan_builder.SetGlobalIndexResult(result);
/// @endcode
///
/// `ExecuteLocal()` plans and searches in this process. To run them in different processes, plan
/// with `NewFullTextScan()`, serialize the splits with `FullTextSearchSplit::Serialize()`, and
/// search them with the `NewFullTextRead()` of a builder for the same table, column and query.
///
/// The row ids of the result refer to the snapshot the search was planned from, which
/// `FullTextScan::Plan::SnapshotId()` returns. If the table may change in between, pin the data
/// scan to it with `scan.mode` = `from-snapshot` and `scan.snapshot-id`, or with the same
/// `scan.tag-name` for a search of a tag, as the tag may outlive the snapshot.
class PAIMON_EXPORT FullTextSearchBuilder {
 public:
    /// @param table_path The root path of the table.
    explicit FullTextSearchBuilder(const std::string& table_path);

    ~FullTextSearchBuilder();

    /// Restricts the search to the given partitions. Each map in the vector is one partition
    /// spec whose entries are ANDed, and the specs are ORed, as in
    /// `ScanContextBuilder::SetPartitionFilter()`. If not set, every partition is searched.
    FullTextSearchBuilder& SetPartitionFilter(
        const std::vector<std::map<std::string, std::string>>& partition_filters);

    /// Sets the number of highest-scoring rows to return. It must be positive to create a read.
    FullTextSearchBuilder& SetLimit(int32_t limit);

    /// Sets the column to search and the JSON query of `FullTextSearch::query`, which the index
    /// receives unchanged.
    FullTextSearchBuilder& SetQuery(const std::string& field_name, const std::string& query);

    /// Adds an option that overrides the one of the same key in the table schema. Use one of
    /// `scan.snapshot-id`, `scan.tag-name`, `scan.timestamp` and `scan.timestamp-millis` to search
    /// an earlier snapshot; it must agree with `scan.mode`, as `FullTextScan::CreatePlan()`
    /// describes.
    FullTextSearchBuilder& AddOption(const std::string& key, const std::string& value);

    /// Sets the options that override the options in the table schema, replacing the options
    /// added before.
    FullTextSearchBuilder& SetOptions(const std::map<std::string, std::string>& options);

    /// Sets the memory pool. If not set, the default pool is used.
    FullTextSearchBuilder& WithMemoryPool(const std::shared_ptr<MemoryPool>& memory_pool);

    /// Sets the executor that searches splits in parallel and reads manifests. If not set, each
    /// read creates an executor with `global-index.thread-num` threads, which defaults to the
    /// number of CPUs.
    FullTextSearchBuilder& WithExecutor(const std::shared_ptr<Executor>& executor);

    /// Sets the file system to access the table with. If not set, the file system is created from
    /// the `file-system` option.
    FullTextSearchBuilder& WithFileSystem(const std::shared_ptr<FileSystem>& file_system);

    /// Creates a scan that plans the search on the current settings. The query must be set, and
    /// its column must exist in the latest schema of the table.
    /// @return The scan, or an error if the settings are invalid or the table cannot be loaded;
    ///         `NotImplemented` for a primary-key full-text index (`pk-full-text.index.columns`) or
    ///         a search mode other than `fast` (`full-text-index.search-mode`, or else
    ///         `global-index.search-mode`).
    Result<std::unique_ptr<FullTextScan>> NewFullTextScan() const;

    /// Creates a read that searches the splits of a scan on the current settings. In addition to
    /// the requirements of `NewFullTextScan()`, the limit must be positive.
    /// @return The read, or an error if the settings are invalid or the table cannot be loaded.
    Result<std::unique_ptr<FullTextRead>> NewFullTextRead() const;

    /// Plans and searches in this process: `NewFullTextRead()->Read(plan)` with the plan of
    /// `NewFullTextScan()`.
    Result<std::shared_ptr<ScoredGlobalIndexResult>> ExecuteLocal() const;

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
