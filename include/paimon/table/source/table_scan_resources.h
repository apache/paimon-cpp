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

#include <memory>
#include <string>

#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class FileSystem;
class TableScanResourcesAccess;

/// Metadata resources shared by scans of one managed table and branch.
///
/// Share this object through ScanContextBuilder::WithTableResources(). Each scan still discovers
/// the latest schema ID and chooses its own snapshot. Schema versions, schema-derived resources
/// and snapshots use separate LRU caches with at most 64 entries each. Each derived resource caches
/// up to 64 statistics evolutions, each retaining up to 64 dense-field mappings. Eviction releases
/// cache references; active scans retain the metadata they use. These are entry limits, not byte
/// limits. Latest snapshot discovery and existence checks still query the file system.
/// A cached snapshot does not pin its metadata or data files. The caller must recreate this object
/// after fast-forward, dropping and recreating a table or branch, or changing the file system's
/// access configuration.
/// Fast-forward can replace schema and snapshot contents under existing IDs; they are not refreshed
/// automatically. Use the new resources for subsequent scans.
///
/// Concurrent scans may share these resources. The supplied file system and metadata memory pool
/// must support concurrent use. Filters, executors, scan progress and scan metrics are not shared.
class PAIMON_EXPORT TableScanResources {
 public:
    /// Creates resources without loading table metadata.
    /// @param table_path Physical table root, without a system table suffix.
    /// @param file_system Non-null file system used by scans sharing these resources.
    /// @param branch Branch to scan; an empty branch is normalized to main.
    /// @param memory_pool Non-null pool retained for shared metadata, independent of scan pools.
    static Result<std::shared_ptr<TableScanResources>> Create(
        const std::string& table_path, const std::shared_ptr<FileSystem>& file_system,
        const std::string& branch, const std::shared_ptr<MemoryPool>& memory_pool);

    ~TableScanResources();

 private:
    friend class TableScanResourcesAccess;
    class Impl;
    explicit TableScanResources(std::unique_ptr<Impl>&& impl);

    std::unique_ptr<Impl> impl_;
};
}  // namespace paimon
