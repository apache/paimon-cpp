/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class FileSystem;
class MemoryPool;
class ScanFilter;

/// Lists the physical files required by a table snapshot.
class PAIMON_EXPORT SnapshotFileScan {
 public:
    SnapshotFileScan() = delete;
    ~SnapshotFileScan() = delete;

    /// List the physical files required to materialize a snapshot.
    ///
    /// The result includes the snapshot and schema files, manifest lists and manifest files,
    /// live data and changelog files, external file indexes, the index manifest and its live index
    /// files, statistics, and real-time offset files. Mutable snapshot hints such as `LATEST` and
    /// `EARLIEST` are not included.
    ///
    /// Partition and bucket filters apply only to files which belong to a partition or bucket.
    /// Snapshot-level shared metadata is always returned. A manifest file is returned when it
    /// cannot be pruned by manifest-level partition and bucket statistics, even if entry-level
    /// filtering later removes all of its entries. Predicate filters are not supported.
    ///
    /// @param table_path Root path of the table.
    /// @param branch Branch to list. An empty value selects the main branch. This parameter takes
    ///               precedence over a branch configured in options.
    /// @param snapshot_id Snapshot to list, or `std::nullopt` to use the latest snapshot.
    /// @param scan_filter Optional partition and bucket filters. Partition maps use AND semantics,
    ///                    while the vector uses OR semantics. Predicate filters are rejected.
    /// @param options User options overriding options stored in the latest table schema.
    /// @param file_system Optional file system implementation. If null, it is resolved from
    /// options.
    /// @param executor Optional executor used to read manifest files in parallel.
    /// @param memory_pool Optional memory pool. If null, the default pool is used.
    /// @return Physical file paths required by the selected snapshot and filters.
    static Result<std::set<std::string>> ListFiles(
        const std::string& table_path, const std::string& branch,
        const std::optional<int64_t>& snapshot_id, const std::shared_ptr<ScanFilter>& scan_filter,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& memory_pool);
};

}  // namespace paimon
