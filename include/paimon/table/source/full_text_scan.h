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

    /// Plans the search on the snapshot that the options select, as Java
    /// `TimeTravelUtil#tryTravelOrLatest` does, whatever `scan.mode` is: snapshot
    /// `scan.snapshot-id`, the snapshot of tag `scan.tag-name`, the latest snapshot at or before
    /// `scan.timestamp` or `scan.timestamp-millis`, or else the latest snapshot.
    ///
    /// Each index shard of the searched column becomes one split. The scan considers only the
    /// index files that are in the partitions of the partition filter, that index the column as
    /// their indexed field or as one of their extra fields, and whose indexer supports full-text
    /// search (`GlobalIndexer::SupportsFullTextSearch()`). The plan fails if Paimon C++ is built
    /// without the backend of a `full-text` or `lucene-fts` index it finds. When the row ranges of
    /// index shards overlap, each row is ranked by one shard only: an index whose indexed field is
    /// the column comes before one that carries it as an extra field, and further ties are broken
    /// by index type and fields, then by the start and end of the shard.
    /// @return The plan, or an error if more than one time-travel option is set, the selected
    ///         snapshot or tag does not exist, or reading the index manifests fails;
    ///         `NotImplemented` for an index whose backend is not built.
    virtual Result<std::shared_ptr<Plan>> CreatePlan() = 0;
};

}  // namespace paimon
