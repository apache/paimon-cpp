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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/commit_message.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/realtime/realtime_commit_progress.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class RecordBatch;
class WriteContext;

/// Interface for write operations in a file store.
class PAIMON_EXPORT FileStoreWrite {
 public:
    /// Create an instance of `FileStoreWrite`.
    ///
    /// @param context A unique pointer to the `WriteContext` used for write operations.
    ///
    /// @return A Result containing a unique pointer to the `FileStoreWrite` instance.
    static Result<std::unique_ptr<FileStoreWrite>> Create(std::unique_ptr<WriteContext> context);

    virtual ~FileStoreWrite() = default;

    /// Support write an input `RecordBatch` to internal buffer or file.
    /// @note Real-time writers require a non-nullable int64 `_REALTIME_OFFSET` field before the
    ///       table write fields. Its values must be strictly increasing within each batch and
    ///       monotonically increasing for each partition-bucket across batches; gaps are allowed.
    ///       The field is used for snapshot progress and is not written to data files.
    /// @note If a field in table schema is marked as non-nullable (`nullable = false`),
    ///       the corresponding array in `batch` must have zero null entries.
    virtual Status Write(std::unique_ptr<RecordBatch>&& batch) = 0;

    /// Slices the current in-memory real-time data into a sealed segment so it can be reclaimed
    /// independently. A future implementation will support spilling sealed segments to a
    /// temporary directory; currently this method only creates the in-memory segment boundary.
    /// Calling this method on a non-real-time writer returns an error.
    /// If sealing fails, the caller must recreate both the `RealtimeContext` and writer. The
    /// upstream must then recover input from the durable recovery offset persisted in the
    /// snapshot. Reusing the failed writer is unsupported.
    virtual Status Seal();

    /// Compact data stored in given partition and bucket. Note that compaction process is only
    /// submitted and may not be completed when the method returns.
    ///
    /// @param partition the partition to compact
    /// @param bucket the bucket to compact
    /// @param full_compaction whether to trigger full compaction or just normal compaction
    ///
    /// @return status for compacting the records
    virtual Status Compact(const std::map<std::string, std::string>& partition, int32_t bucket,
                           bool full_compaction) = 0;

    /// Generate a list of commit messages with the latest generated data file meta
    /// information of the current snapshot.
    ///
    /// When we need commit, call PrepareCommit to get the current {@link CommitMessage}s with the
    /// latest generated data file meta information of the current snapshot.
    ///
    /// This function is designed to be called when a commit is required. Depending on the writing
    /// scenario, the behavior will differ:
    ///
    /// - For batch write, simply call `PrepareCommit()` without any parameters.
    /// - For streaming write, you need to provide both parameters:
    ///     `PrepareCommit(bool wait_compaction, int64_t commit_identifier)`.
    ///
    /// @param wait_compaction Indicates whether to wait for any ongoing compaction process to
    ///                        complete.
    /// @param commit_identifier A unique identifier for the commit operation. This parameter is
    ///                          only relevant in streaming write scenarios.
    ///
    /// @return A Result containing `std::vector<std::shared_ptr<CommitMessage>>` objects,
    ///         representing the generated commit messages.
    /// @note Real-time writers must use `PrepareCommitWithProgress()` so offset ranges are not
    ///       discarded.
    virtual Result<std::vector<std::shared_ptr<CommitMessage>>> PrepareCommit(
        bool wait_compaction = true, int64_t commit_identifier = BATCH_WRITE_COMMIT_IDENTIFIER) = 0;

    /// Generates commit messages together with partition-bucket real-time offset ranges.
    ///
    /// Each range is returned atomically with the commit message generated from the same sealed
    /// segments. Repeated calls return incremental progress. The upstream coordinator must retain
    /// every result until it is committed and include all earlier prepared-but-uncommitted
    /// progress when a later checkpoint subsumes it.
    ///
    /// @param commit_identifier Identifier of this prepare-commit operation in streaming mode.
    /// @return Real-time commit messages with their partition-bucket offset ranges.
    /// @note Calling this method on a non-real-time writer or in batch mode returns an error.
    /// @note If preparation fails, the caller must recreate both the `RealtimeContext` and writer.
    ///       The upstream must then recover input from the durable recovery offset persisted in the
    ///       snapshot. The failed writer may contain partially prepared bucket state and must not
    ///       be reused.
    virtual Result<std::vector<RealtimeCommitProgress>> PrepareCommitWithProgress(
        int64_t commit_identifier);

    /// Refreshes a real-time writer after `snapshot_id` has committed successfully.
    ///
    /// The writer loads the snapshot's partition-bucket offsets and releases sealed memory that is
    /// fully covered by disk. Calling this method on a non-real-time writer returns an error.
    /// If the snapshot overwrites table contents or moves committed progress backwards, such as
    /// after a partition drop, overwrite, or rollback, this method returns an error and the caller
    /// must recreate the `RealtimeContext` and writer. These operations are not fenced against an
    /// active writer and do not clear its process-local state automatically. The caller must
    /// coordinate them with active writers; skipping the resetting snapshot and continuing to use
    /// an old context is unsupported.
    virtual Status RefreshCommittedSnapshot(int64_t snapshot_id);

    virtual std::shared_ptr<Metrics> GetMetrics() const = 0;

    /// Releases resources owned by this writer.
    ///
    /// Closing a real-time writer with data not covered by a successful
    /// `PrepareCommitWithProgress()` returns an error and invalidates its `RealtimeContext`.
    /// The caller must rebuild both objects and recover input from the durable snapshot offset.
    virtual Status Close() = 0;
};

}  // namespace paimon
