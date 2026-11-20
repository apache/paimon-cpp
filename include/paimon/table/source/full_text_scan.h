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
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "paimon/result.h"
#include "paimon/table/source/full_text_search_split.h"
#include "paimon/visibility.h"

namespace paimon {
/// Plans a table-level full-text search into splits, aligned with Java
/// `org.apache.paimon.table.source.FullTextScan`. Create one with
/// `FullTextSearchBuilder::NewFullTextScan()`.
class PAIMON_EXPORT FullTextScan {
 public:
    /// The splits of a full-text search and the snapshot they were planned from.
    class Plan {
     public:
        Plan(const std::optional<int64_t>& snapshot_id,
             std::vector<std::shared_ptr<FullTextSearchSplit>> splits)
            : snapshot_id_(snapshot_id), splits_(std::move(splits)) {}

        /// @return The splits to search. They can be read in this process or serialized and read
        ///         in others.
        const std::vector<std::shared_ptr<FullTextSearchSplit>>& Splits() const {
            return splits_;
        }

        /// @return The id of the snapshot the plan was built from, or nullopt if the table has no
        ///         snapshot.
        const std::optional<int64_t>& SnapshotId() const {
            return snapshot_id_;
        }

     private:
        std::optional<int64_t> snapshot_id_;
        std::vector<std::shared_ptr<FullTextSearchSplit>> splits_;
    };

    virtual ~FullTextScan() = default;

    /// Plans the search on the latest snapshot or the one selected by `scan.snapshot-id`,
    /// `scan.tag-name`, `scan.timestamp` or `scan.timestamp-millis`. A timestamp selects the
    /// latest snapshot at or before that time. Set at most one selector, consistent with
    /// `scan.mode`. Each plan resolves its tag or timestamp anew.
    ///
    /// Each full-text shard assigned rows in the selected partitions becomes one split. For
    /// overlapping shards, the planner prefers an index on the searched column over one that
    /// carries it as an extra field. Ties are broken by index type, field IDs and shard range.
    /// A stored split carries single-field scalar indexes for its row filter only where those
    /// indexes intersect its assigned search ranges.
    ///
    /// When a selected partition has a full-text index for the column, `full` and `detail` assign
    /// gaps in single-field index coverage to a `RawFullTextSearchSplit`. `full` checks all row
    /// ids below the snapshot's next row id. `detail` checks data files in the selected partitions.
    /// Raw and stored splits rank disjoint rows and can be read independently.
    /// @return The plan, or an error for conflicting scan options, a missing snapshot or tag, or
    ///         unreadable manifests. A missing `full-text` or `lucene-fts` backend returns
    ///         `NotImplemented`.
    virtual Result<std::shared_ptr<Plan>> CreatePlan() = 0;
};

}  // namespace paimon
