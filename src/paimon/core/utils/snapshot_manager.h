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
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "paimon/core/snapshot.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"

namespace paimon {

class FileSystem;

/// Manager for `Snapshot`, providing utility methods related to paths and snapshot hints.
class SnapshotManager {
 public:
    static constexpr char SNAPSHOT_PREFIX[] = "snapshot-";
    static constexpr char EARLIEST[] = "EARLIEST";
    static constexpr char LATEST[] = "LATEST";

    /// Loads the catalog's latest snapshot. Null means no snapshot; only `NotImplemented`
    /// permits file-system fallback.
    using SnapshotLoader = std::function<Result<std::optional<Snapshot>>()>;

    SnapshotManager(const std::shared_ptr<FileSystem>& fs, const std::string& root_path);
    SnapshotManager(const std::shared_ptr<FileSystem>& fs, const std::string& root_path,
                    const std::string& branch);
    ~SnapshotManager();

    /// Sets the loader for `LatestSnapshot()` and `LatestSnapshotId()`.
    /// Historical snapshots and manifests remain on the file system.
    void SetSnapshotLoader(SnapshotLoader loader);

    /// Returns whether a catalog loader is configured, even if it falls back to the file system.
    bool HasSnapshotLoader() const {
        return static_cast<bool>(snapshot_loader_);
    }

    /// Latest snapshot and its source. `from_catalog` is false after file-system fallback.
    struct LatestSnapshotResult {
        std::optional<Snapshot> snapshot;
        bool from_catalog = false;
    };

    const std::shared_ptr<FileSystem>& Fs() const;
    const std::string& RootPath() const;
    const std::string& Branch() const;
    Result<std::optional<Snapshot>> LatestSnapshot() const;
    /// Returns the latest snapshot and source used to determine the history boundary.
    Result<LatestSnapshotResult> LatestSnapshotWithSource() const;
    std::string SnapshotDirectory() const;
    std::string SnapshotPath(int64_t snapshot_id) const;
    /// Finds the user's newest snapshot, reading older snapshots from the table directory.
    /// Expired history ends the search. Missing catalog history at or above EARLIEST is an error
    /// to prevent duplicate commits during recovery. Returns null if no matching snapshot is found.
    Result<std::optional<Snapshot>> LatestSnapshotOfUser(const std::string& user);

    /// Searches from the supplied snapshot; null `latest` returns null.
    /// Pass the source flag returned with `latest` to select the correct history boundary.
    Result<std::optional<Snapshot>> LatestSnapshotOfUserAtOrBefore(
        const std::string& user, const std::optional<Snapshot>& latest,
        bool latest_from_catalog) const;
    Status CommitLatestHint(int64_t snapshot_id);
    Status CommitEarliestHint(int64_t snapshot_id);
    Result<Snapshot> LoadSnapshot(int64_t snapshot_id) const;
    Result<std::optional<int64_t>> EarliestSnapshotId() const;
    Result<std::optional<int64_t>> LatestSnapshotId() const;
    /// Finds the latest snapshot published to the file system, without consulting the loader.
    Result<std::optional<int64_t>> LatestSnapshotIdFromFileSystem() const;
    Result<bool> SnapshotExists(int64_t snapshot_id) const;
    Result<std::set<std::string>> TryGetNonSnapshotFiles(int64_t older_than_ms) const;
    Result<std::vector<Snapshot>> GetAllSnapshots() const;
    Result<std::optional<Snapshot>> EarlierOrEqualTimeMillis(int64_t timestamp_millis) const;
    Result<std::optional<Snapshot>> EarlierThanTimeMillis(int64_t timestamp_millis) const;

 private:
    static constexpr int32_t READ_HINT_RETRY_NUM = 3;
    static constexpr int32_t READ_HINT_RETRY_INTERVAL = 1;

    /// Rechecks EARLIEST a bounded number of times to confirm expiration.
    /// False leaves expiration unconfirmed, so the caller must report the missing snapshot.
    bool ExpiredSinceBoundaryWasRead(int64_t id) const;

    std::string BranchPath() const;

    Result<std::optional<int64_t>> FindEarliest(
        const std::string& dir, const std::string& prefix,
        const std::function<std::string(int64_t)>& path_func) const;
    Result<std::optional<int64_t>> FindLatest(
        const std::string& dir, const std::string& prefix,
        const std::function<std::string(int64_t)>& path_func) const;
    Result<std::optional<int64_t>> FindByListFiles(
        const std::function<int64_t(int64_t, int64_t)> reducer_func, const std::string& dir,
        const std::string& prefix) const;
    std::optional<int64_t> ReadHint(const std::string& file_name, const std::string& dir) const;
    Status CommitHint(int64_t snapshot_id, const std::string& file_name, const std::string& dir);
    Result<std::optional<Snapshot>> FindSnapshotBeforeTimestamp(
        int64_t timestamp_millis, const std::function<bool(int64_t, int64_t)>& compare) const;

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string root_path_;
    std::string branch_;
    SnapshotLoader snapshot_loader_;
};

}  // namespace paimon
