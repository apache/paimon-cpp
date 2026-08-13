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
#include <string>
#include <utility>
#include <vector>

#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class MemIndexer;
class MemIndexerFactory;
class MemReadView;
class MemoryPool;

/// Identifies one partition-bucket by its logical partition values.
struct PAIMON_EXPORT RealtimePartitionBucket {
    /// Creates a key from logical partition values and a fixed bucket id.
    RealtimePartitionBucket(std::map<std::string, std::string> partition, int32_t bucket)
        : partition(std::move(partition)), bucket(bucket) {}

    /// Orders keys by partition values and then bucket id.
    bool operator<(const RealtimePartitionBucket& other) const {
        if (partition != other.partition) {
            return partition < other.partition;
        }
        return bucket < other.bucket;
    }

    /// Returns whether both partition values and bucket id are equal.
    bool operator==(const RealtimePartitionBucket& other) const {
        return partition == other.partition && bucket == other.bucket;
    }

    /// Logical partition values, before partition-path escaping.
    std::map<std::string, std::string> partition;
    /// Fixed bucket id.
    int32_t bucket = -1;
};

/// Largest committed offset for each partition-bucket.
using RealtimeOffsetMap = std::map<RealtimePartitionBucket, int64_t>;

/// Memory indexer and its initial offset resolved from committed and retained memory progress.
struct PAIMON_EXPORT RealtimeMemIndexerState {
    /// Plugin instance associated with the requested partition-bucket.
    std::shared_ptr<MemIndexer> indexer;
    /// First offset after both committed rows and rows currently retained by the indexer.
    int64_t initial_offset;
};

/// One partition-bucket and the immutable plugin view captured for a table scan.
struct PAIMON_EXPORT RealtimePartitionBucketView {
    /// Partition-bucket associated with this view.
    RealtimePartitionBucket partition_bucket;
    /// Plugin instance that creates readers from `read_view`.
    std::shared_ptr<MemIndexer> indexer;
    /// Immutable rows pinned for one query plan.
    std::shared_ptr<MemReadView> read_view;
};

/// Shared context that owns the `MemIndexer` instances used by a real-time writer.
///
/// Applications share one context between `WriteContext`, `ScanContext`, and `ReadContext`. The
/// context uses either the default Arrow implementation or an application-provided factory and
/// keeps each created indexer available across writes, prepare-commit operations, and
/// process-local reads.
class PAIMON_EXPORT RealtimeContext {
 public:
    /// Default lifetime of a pinned read view that has not been explicitly released.
    static constexpr int64_t kDefaultReadViewTtlMillis = 5 * 60 * 1000;

    /// Creates a context backed by Paimon's default Arrow `MemIndexer`.
    static Result<std::shared_ptr<RealtimeContext>> Create();

    /// Creates a context backed by an application-provided indexer factory.
    ///
    /// @param factory Non-null factory used to create indexers on demand.
    static Result<std::shared_ptr<RealtimeContext>> Create(
        const std::shared_ptr<MemIndexerFactory>& factory);

    /// Returns the stable indexer and next writable offset associated with a partition-bucket.
    ///
    /// The offset follows both committed progress and any building or sealed rows retained by a
    /// reused indexer.
    Result<RealtimeMemIndexerState> GetOrCreateMemIndexer(
        const std::map<std::string, std::string>& partition, int32_t bucket,
        std::unique_ptr<::ArrowSchema> write_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& memory_pool);

    /// Captures an immutable read view from every currently registered indexer.
    ///
    /// The indexer registry is fixed during this call and each returned plugin view is stable. New
    /// partition-buckets registered after this call are not visible in that query.
    Result<std::vector<RealtimePartitionBucketView>> AcquireReadViews();

    /// Registers a captured read view during scan planning and returns an opaque process-local
    /// lookup ticket for the resulting split.
    ///
    /// The registry retains shared ownership of the view until the ticket is explicitly released
    /// or its TTL expires.
    Result<std::string> PinReadView(const RealtimePartitionBucketView& view, int64_t ttl_millis);

    /// Looks up a ticket while creating a reader and returns another shared reference to the
    /// immutable view captured during scan planning.
    ///
    /// This operation does not remove the ticket. After a `RealtimeReader` retains the returned
    /// view, the caller releases the ticket separately.
    Result<RealtimePartitionBucketView> ResolveReadView(const std::string& opaque_ticket);

    /// Removes a pinned read-view ticket from the registry.
    ///
    /// The registry-held reference is handed to the background release queue. This operation is
    /// idempotent, and each `RealtimeReader` retains its own shared reference until it is closed.
    Status ReleaseReadView(const std::string& opaque_ticket);

    /// Advances the committed progress visible to the registered memory indexers.
    ///
    /// Snapshot ids must advance monotonically. After validation, the committed snapshot progress
    /// is adopted atomically before indexers are notified outside the context's registry lock. If
    /// an indexer notification fails, retrying the same snapshot only notifies indexers whose
    /// reclamation progress is behind. Existing read views continue to pin referenced resources
    /// until their readers are closed.
    Status AdvanceCommittedProgress(int64_t snapshot_id,
                                    const RealtimeOffsetMap& committed_offsets);

    ~RealtimeContext();

 private:
    class Impl;

    explicit RealtimeContext(std::unique_ptr<Impl>&& impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
