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
#include <string>
#include <utility>
#include <vector>

#include "paimon/reader/batch_reader.h"
#include "paimon/realtime/offset_range.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class MemoryPool;
class Predicate;

/// Statistics collected by a `RealtimeStore` for query predicate pushdown.
enum class StatisticsMode {
    /// Do not collect statistics.
    NONE,
    /// Collect statistics for all supported fields.
    FULL,
};

/// A table record batch and its framework-assigned contiguous offset range.
///
/// The batch contains only table write fields. Row `i` is associated with
/// `offset_range.begin + i`; the offset is progress metadata and is not a table field.
struct PAIMON_EXPORT RealtimeWriteBatch {
    /// Input batch whose ownership is transferred to `RealtimeStore::Write`.
    std::unique_ptr<RecordBatch> batch;
    /// Left-closed, right-open offset range covered by `batch`.
    OffsetRange offset_range;
};

/// Opaque handle to an immutable segment returned by `RealtimeStore::SealForCommit`.
///
/// A plugin may store the segment in memory or in spill files. Callers use this handle only to
/// request commit readers and inspect its offset range.
class PAIMON_EXPORT RealtimeSegmentHandle {
 public:
    virtual ~RealtimeSegmentHandle() = default;

    /// Returns the left-closed, right-open offset range covered by this segment.
    virtual OffsetRange GetOffsetRange() const = 0;
};

/// Opaque immutable view of the rows visible from one `RealtimeStore`.
///
/// A view pins all referenced resources until the readers created from it are closed. Later
/// writes, seals, and committed-offset reclamation do not change the contents of an existing view.
class PAIMON_EXPORT RealtimeReadView {
 public:
    virtual ~RealtimeReadView() = default;

    /// Returns the left-closed, right-open offset range visible in this view, or no range when it
    /// is empty.
    virtual std::optional<OffsetRange> GetOffsetRange() const = 0;
};

/// Parameters used by a `RealtimeStore` to create readers for a query.
struct PAIMON_EXPORT RealtimeQueryContext {
    /// Requested output fields before the mandatory leading `_VALUE_KIND` field is added.
    ::ArrowSchema* read_schema;
    /// Predicate using field indexes from `read_schema`.
    std::shared_ptr<Predicate> predicate;
    /// Whether the plugin may use `predicate` to prune candidate rows.
    ///
    /// Keep this disabled for primary-key merge-on-read. Pruning memory before PK merge may remove
    /// the newest row and incorrectly expose an older disk row. Exact predicate filtering, when
    /// requested, is applied by the Paimon read framework after plugin reader creation.
    bool enable_predicate_pushdown;
};

/// Customizable plugin interface for storing and querying real-time rows before Paimon data-file
/// generation.
///
/// Paimon serializes calls to `Write` and `SealForCommit` for the same store. After sealing,
/// `CreateCommitReaders` may read the immutable sealed segment while later `Write` calls append to
/// a new building segment. Paimon retains control of file format, rolling, indexes, and
/// commit-message generation. A store may choose its own in-memory representation, indexes, and
/// spill strategy. It may outlive an individual writer because the shared real-time context and
/// active read views retain it.
class PAIMON_EXPORT RealtimeStore {
 public:
    virtual ~RealtimeStore() = default;

    /// Adds a batch to the current building segment.
    ///
    /// The row count matches the framework-assigned `offset_range`.
    virtual Status Write(RealtimeWriteBatch&& batch) = 0;

    /// Seals the current building data and opens a new building segment.
    ///
    /// Returns an immutable segment handle, or `std::nullopt` when there is no data to seal.
    virtual Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() = 0;

    /// Creates readers that expose all rows in a sealed segment for Paimon file writing.
    ///
    /// Concatenating the returned readers must produce every sealed row exactly once and in write
    /// order. Each output batch contains `_VALUE_KIND` followed by all fields from the factory's
    /// `write_schema`.
    virtual Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) = 0;

    /// Acquires an immutable view containing the current sealed and building rows.
    ///
    /// This method may be called concurrently with query-reader creation and reclamation. It must
    /// also provide a consistent snapshot when a write or seal is in progress.
    virtual Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() = 0;

    /// Creates readers over rows in `view` whose offsets are greater than or equal to
    /// `offset_begin`.
    ///
    /// Each output batch contains `_VALUE_KIND` first, followed by the fields requested by
    /// `context.read_schema` except a duplicate `_VALUE_KIND`. Concatenating all returned readers
    /// must produce every matching row once. Paimon retains `view` for the lifetime of the
    /// resulting framework reader.
    virtual Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view, int64_t offset_begin,
        const RealtimeQueryContext& context) = 0;

    /// Notifies the store that its partition-bucket committed end offset has advanced.
    ///
    /// Calls are monotonic and may repeat the same offset after a previous call reports an error,
    /// so implementations must apply this notification idempotently.
    ///
    /// An implementation may reclaim covered segments immediately, defer destruction, spill them,
    /// or retain them. Existing read views continue to keep referenced resources alive.
    virtual Status AdvanceCommittedOffset(int64_t committed_end_offset) = 0;

    /// Returns the number of bytes currently retained by building and sealed segments.
    virtual uint64_t GetMemoryUsage() const = 0;
};

/// Factory for application-provided `RealtimeStore` implementations.
class PAIMON_EXPORT RealtimeStoreFactory {
 public:
    virtual ~RealtimeStoreFactory() = default;

    /// Creates a store configured with the supplied schema, statistics, options, and memory pool.
    /// @param write_schema Complete table write schema whose ownership is transferred to the
    /// factory. The factory may consume it or retain it in the created store.
    /// @param statistics_mode Framework-parsed statistics collection mode.
    /// @param options Effective table options available to the store.
    /// @param memory_pool Memory pool provided by the write context.
    virtual Result<std::shared_ptr<RealtimeStore>> Create(
        std::unique_ptr<::ArrowSchema> write_schema, StatisticsMode statistics_mode,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool) = 0;
};

}  // namespace paimon
