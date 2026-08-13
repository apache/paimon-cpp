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
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/utils/range.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class MemoryPool;
class Predicate;

/// A table record batch and its framework-assigned contiguous offset range.
///
/// The batch contains only table write fields. Row `i` is associated with
/// `offset_range.from + i`; the offset is progress metadata and is not a table field.
struct PAIMON_EXPORT RealtimeWriteBatch {
    /// Input batch whose ownership is transferred to `MemIndexer::Write`.
    std::unique_ptr<RecordBatch> batch;
    /// Inclusive `[from, to]` offset range covered by `batch`.
    Range offset_range;
};

/// Opaque handle to an immutable segment returned by `MemIndexer::SealForCommit`.
///
/// A plugin may store the segment in memory or in spill files. Callers use this handle only to
/// request commit readers and inspect its offset range.
class PAIMON_EXPORT RealtimeSegmentHandle {
 public:
    virtual ~RealtimeSegmentHandle() = default;

    /// Returns the inclusive offset range covered by this segment.
    virtual Range GetOffsetRange() const = 0;
};

/// Opaque immutable view of the rows visible from one `MemIndexer`.
///
/// A view pins all referenced resources until the readers created from it are closed. Later
/// writes, seals, and committed-offset reclamation do not change the contents of an existing view.
class PAIMON_EXPORT MemReadView {
 public:
    virtual ~MemReadView() = default;

    /// Returns the inclusive offset range visible in this view, or no range when it is empty.
    virtual std::optional<Range> GetOffsetRange() const = 0;
};

/// Outermost query reader that pins a `MemReadView`.
class PAIMON_EXPORT RealtimeReader final : public BatchReader {
 public:
    /// Creates a reader that delegates reads to `reader` and pins `read_view` for its lifetime.
    static Result<std::unique_ptr<RealtimeReader>> Create(std::shared_ptr<MemReadView> read_view,
                                                          std::unique_ptr<BatchReader> reader) {
        if (!read_view) {
            return Status::Invalid("real-time reader view is null");
        }
        if (!reader) {
            return Status::Invalid("real-time inner reader is null");
        }
        return std::unique_ptr<RealtimeReader>(
            new RealtimeReader(std::move(read_view), std::move(reader)));
    }

    /// Delegates batch reads to the wrapped plugin reader.
    Result<ReadBatch> NextBatch() override {
        return reader_->NextBatch();
    }

    /// Delegates bitmap batch reads to the wrapped plugin reader.
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        return reader_->NextBatchWithBitmap();
    }

    /// Returns metrics from the wrapped plugin reader.
    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    /// Closes the delegated reader and releases the pinned read view.
    void Close() override {
        reader_->Close();
        read_view_.reset();
    }

 private:
    RealtimeReader(std::shared_ptr<MemReadView> read_view, std::unique_ptr<BatchReader> reader)
        : read_view_(std::move(read_view)), reader_(std::move(reader)) {}

    // Keep the view before the delegated reader so reverse member destruction closes the reader
    // before releasing the data it references.
    std::shared_ptr<MemReadView> read_view_;
    std::unique_ptr<BatchReader> reader_;
};

/// Parameters used by a `MemIndexer` to create readers for a query.
struct PAIMON_EXPORT MemQueryContext {
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

/// Plugin interface for buffering real-time writes before Paimon data-file generation.
///
/// Paimon serializes calls to `Write` and `SealForCommit` for the same indexer. After sealing,
/// `CreateCommitReaders` may read the immutable sealed segment while later `Write` calls append to
/// a new building segment. Paimon retains control of file format, rolling, indexes, and
/// commit-message generation. An indexer may outlive an individual writer because the shared
/// real-time context and active read views retain it.
class PAIMON_EXPORT MemIndexer {
 public:
    virtual ~MemIndexer() = default;

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
    virtual Result<std::shared_ptr<MemReadView>> AcquireReadView() = 0;

    /// Creates readers over rows in `view` whose offsets are greater than
    /// `offset_lower_exclusive`.
    ///
    /// Each output batch contains `_VALUE_KIND` first, followed by the fields requested by
    /// `context.read_schema` except a duplicate `_VALUE_KIND`. Concatenating all returned readers
    /// must produce every matching row once. Paimon retains `view` for the lifetime of the
    /// resulting framework reader.
    virtual Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<MemReadView>& view, int64_t offset_lower_exclusive,
        const MemQueryContext& context) = 0;

    /// Notifies the indexer that its partition-bucket committed offset has advanced.
    ///
    /// Calls are monotonic and may repeat the same offset after a previous call reports an error,
    /// so implementations must apply this notification idempotently.
    ///
    /// An implementation may reclaim covered segments immediately, defer destruction, spill them,
    /// or retain them. Existing read views continue to keep referenced resources alive.
    virtual Status AdvanceCommittedOffset(int64_t committed_offset) = 0;

    /// Returns the number of bytes currently retained by building and sealed segments.
    virtual uint64_t GetMemoryUsage() const = 0;
};

/// Factory for application-provided `MemIndexer` implementations.
class PAIMON_EXPORT MemIndexerFactory {
 public:
    virtual ~MemIndexerFactory() = default;

    /// Creates an indexer configured with the supplied schema, options, and memory pool.
    /// @param write_schema Complete table write schema whose ownership is transferred to the
    /// factory. The factory may consume it or retain it in the created indexer.
    /// @param options Effective table options available to the indexer.
    /// @param memory_pool Memory pool provided by the write context.
    virtual Result<std::shared_ptr<MemIndexer>> Create(
        std::unique_ptr<::ArrowSchema> write_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool) = 0;
};

}  // namespace paimon
