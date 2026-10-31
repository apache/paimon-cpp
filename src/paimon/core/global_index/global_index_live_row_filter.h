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
#include <optional>
#include <string>

#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
/// Live-row filtering shared by global index search readers, aligned with Java
/// `GlobalIndexLiveRowFilter`. Deletion vectors are applied only when rows are read, so a search
/// that ranks deleted rows can fill its top-k slots with rows the read then drops.
class GlobalIndexLiveRowFilter {
 public:
    GlobalIndexLiveRowFilter() = delete;
    ~GlobalIndexLiveRowFilter() = delete;

    /// Returns the global row ids of the rows that are live in `snapshot`: the row id ranges of
    /// the data files in the partitions accepted by `partition_filter`, minus the rows deleted by
    /// their deletion vectors. Returns nullopt when deletion vectors or row tracking are disabled,
    /// so every row counts as live.
    /// @param root_path        Root directory of the table.
    /// @param table_schema     Schema of the table.
    /// @param snapshot         The snapshot to read; pin it to the one the global index is
    ///                         read from, so row ids and live rows agree.
    /// @param partition_filter Predicate over the partition fields, or null for all partitions.
    /// @param options          Options of the table.
    /// @param executor         Executor for reading manifests.
    /// @param pool             Memory pool for reading manifests and deletion vectors.
    static Result<std::optional<RoaringBitmap64>> LiveRows(
        const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
        const Snapshot& snapshot, const std::shared_ptr<PredicateFilter>& partition_filter,
        const CoreOptions& options, const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
