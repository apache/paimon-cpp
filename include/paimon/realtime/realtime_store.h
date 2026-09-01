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

#include "arrow/c/abi.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/realtime/offset_range.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/statistics_mode.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class MemoryPool;
class Predicate;

enum class PAIMON_EXPORT RealtimeStoreMode {
    APPEND_ONLY,
    PRIMARY_KEY,
};

/// Parameters used by a `RealtimeStoreFactory` to create a store.
struct PAIMON_EXPORT RealtimeStoreCreateRequest {
    /// Schema whose ownership is transferred to the factory. Append mode receives the complete
    /// append transport schema: [_REALTIME_OFFSET, table write fields]. Primary-key mode receives
    /// the realtime primary-key transport schema:
    /// [_VALUE_KIND, _SEQUENCE_NUMBER, _REALTIME_OFFSET, table write fields].
    std::unique_ptr<::ArrowSchema> write_schema;
    /// Table options available to the store implementation.
    std::map<std::string, std::string> options;
    /// Memory pool for allocations retained by the store.
    std::shared_ptr<MemoryPool> memory_pool;
    /// Table mode implemented by the store.
    RealtimeStoreMode mode = RealtimeStoreMode::APPEND_ONLY;
    /// Statistics collected by append-only stores.
    StatisticsMode statistics_mode = StatisticsMode::NONE;
};

/// A record batch and its application-assigned offset bounds.
///
/// Append-mode batches use the append transport schema [_REALTIME_OFFSET, table write fields], and
/// offsets are strictly increasing before the batch enters the store. Primary-key batches use the
/// realtime primary-key transport schema, are sorted by full primary key then sequence number, and
/// retain the original offset in `_REALTIME_OFFSET`. `offset_range` is the left-closed, right-open
/// envelope from the first application offset through one past the last; offsets may have gaps, so
/// its count is not the batch row count.
struct PAIMON_EXPORT RealtimeWriteBatch {
    /// Input batch whose ownership is transferred to `RealtimeStore::Write`.
    std::unique_ptr<RecordBatch> batch;
    /// Left-closed, right-open offset envelope covered by `batch`.
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

    /// Returns the number of rows represented by this segment.
    virtual int64_t GetRowCount() const = 0;
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

    /// Returns the exact number of rows whose offsets fall in `visible_offsets`.
    ///
    /// Offset ranges may contain gaps, so callers cannot derive this count from the range width.
    virtual Result<int64_t> GetRowCount(const OffsetRange& visible_offsets) const = 0;
};

/// Parameters used by a `RealtimeStore` to create readers for a query.
struct PAIMON_EXPORT RealtimeQueryContext {
    /// Physical source schema the store must materialize. Query readers must include the mandatory
    /// `_VALUE_KIND` field in returned batches. Paimon may subsequently convert physical fields
    /// into the query's logical output schema, for example for selected-key MAP or VARIANT access.
    /// This schema is borrowed and remains valid only during `CreateQueryReaders`; plugins must
    /// import or copy it synchronously.
    ::ArrowSchema* read_schema;
    /// Optional predicate using field indexes from `read_schema`. A non-null predicate allows the
    /// plugin to prune candidate rows. Exact filtering is applied by the Paimon read framework.
    std::shared_ptr<Predicate> predicate;
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
    /// The offset envelope may contain gaps and does not imply the batch row count.
    virtual Status Write(RealtimeWriteBatch&& batch) = 0;

    /// Seals the current building data and opens a new building segment.
    ///
    /// Returns an immutable segment handle, or `std::nullopt` when there is no data to seal.
    virtual Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() = 0;

    /// Creates readers that expose all rows in a sealed segment for Paimon file writing.
    ///
    /// The returned readers collectively expose every sealed row exactly once. Append-mode readers
    /// preserve write order and contain `_VALUE_KIND`, `_REALTIME_OFFSET`, and table write fields.
    /// Primary-key readers contain the realtime primary-key transport fields; each reader's
    /// complete stream is sorted by full primary key then sequence number.
    virtual Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) = 0;

    /// Acquires an immutable view containing the current sealed and building rows.
    ///
    /// This method may be called concurrently with query-reader creation and reclamation. It must
    /// also provide a consistent snapshot when a write or seal is in progress.
    virtual Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() = 0;

    /// Creates readers over rows in `view`. The readers collectively expose every candidate row
    /// exactly once. Primary-key reader streams are sorted by full primary key then sequence
    /// number. Paimon retains `view` for the lifetime of the resulting framework reader.
    virtual Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view, const RealtimeQueryContext& context) = 0;

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
    /// Creates a store for the requested table mode.
    /// The factory consumes `request`, including ownership of `request.write_schema`.
    virtual Result<std::shared_ptr<RealtimeStore>> Create(RealtimeStoreCreateRequest&& request) = 0;
};

}  // namespace paimon
