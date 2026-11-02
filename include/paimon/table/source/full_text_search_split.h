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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/utils/range.h"
#include "paimon/visibility.h"

namespace paimon {
/// A unit of work of a table-level full-text search, planned by `FullTextScan` and searched by
/// `FullTextRead`, aligned with Java `org.apache.paimon.table.source.FullTextSearchSplit`.
///
/// Splits can be serialized, so that a search can be planned in one process and run in others.
/// The encoding is specific to Paimon C++: Java Paimon serializes its splits with Java
/// serialization, so there is no cross-language format to match.
class PAIMON_EXPORT FullTextSearchSplit {
 public:
    virtual ~FullTextSearchSplit() = default;

    /// Serializes `split` to a binary string.
    /// @param split The split to serialize; it must come from `FullTextScan` or `Deserialize()`.
    /// @param pool  Memory pool for temporary allocations.
    static Result<std::string> Serialize(const std::shared_ptr<FullTextSearchSplit>& split,
                                         const std::shared_ptr<MemoryPool>& pool);

    /// Deserializes a split written by `Serialize()`.
    /// @param buffer Pointer to the serialized bytes.
    /// @param length Number of serialized bytes.
    /// @param pool   Memory pool for the objects of the split.
    /// @return The split, or an error if `buffer` is null, or the bytes are corrupt or not a split
    ///         of a supported version.
    static Result<std::shared_ptr<FullTextSearchSplit>> Deserialize(
        const char* buffer, size_t length, const std::shared_ptr<MemoryPool>& pool);
};

/// A split that searches the full-text index files of one index shard, aligned with Java
/// `org.apache.paimon.table.source.IndexFullTextSearchSplit`.
///
/// When index shards overlap, the scan gives each row to one shard, so a split may rank only some
/// rows of its shard: `SearchRowRanges()` lie within [`RowRangeStart()`, `RowRangeEnd()`], the
/// physical range of the shard, which maps the shard-local row ids of the index to global ones.
class PAIMON_EXPORT IndexFullTextSearchSplit : public FullTextSearchSplit {
 public:
    /// @return The id of the snapshot the split was planned from. The read ranks only the rows
    ///         live in this snapshot.
    virtual int64_t SnapshotId() const = 0;

    /// @return The name of the searched table column.
    virtual const std::string& ColumnName() const = 0;

    /// @return The first global row id of the index shard.
    virtual int64_t RowRangeStart() const = 0;

    /// @return The last global row id of the index shard, inclusive.
    virtual int64_t RowRangeEnd() const = 0;

    /// @return The sorted, disjoint global row id ranges that this split ranks.
    virtual const std::vector<Range>& SearchRowRanges() const = 0;
};

}  // namespace paimon
