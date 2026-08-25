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
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/table/format/format_commit_message.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class FormatTable;
class Metrics;

/// Commits a format table through the `FileStoreCommit` interface, so that a caller holding a
/// table path commits it the way it commits any other table. Java Paimon does the same through
/// `FormatTable.newBatchWriteBuilder()`.
///
/// Most of `FileStoreCommit` is about snapshots and manifests, which a format table has none of:
/// expiring them, rolling back to one, and filtering by a commit identifier recorded in one all
/// refer to state this table does not keep. Each is refused rather than quietly doing nothing, so
/// a caller moving between table types finds out at the call rather than from a table that did
/// not change. `RowIdCheckConflict()` is the one exception, since it returns a reference and has
/// no way to report a refusal.
///
/// What is left is what a directory of files can do: `Commit()`, `Overwrite()` and `Abort()`.
class FormatTableFileStoreCommit : public FileStoreCommit {
 public:
    static Result<std::unique_ptr<FormatTableFileStoreCommit>> Create(
        const std::shared_ptr<FormatTable>& table);

    ~FormatTableFileStoreCommit() override;

    Status Commit(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                  int64_t commit_identifier = BATCH_WRITE_COMMIT_IDENTIFIER,
                  std::optional<int64_t> watermark = std::nullopt) override;

    Status Overwrite(const std::map<std::string, std::string>& partition,
                     const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                     int64_t commit_identifier,
                     std::optional<int64_t> watermark = std::nullopt) override;

    Status Abort(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages) override;

    Result<int64_t> CommitWithProgress(const std::vector<RealtimeCommitProgress>& realtime_commits,
                                       int64_t commit_identifier,
                                       std::optional<int64_t> watermark) override;

    Result<int32_t> FilterAndCommit(
        const std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>>&
            commit_identifier_and_messages,
        std::optional<int64_t> watermark = std::nullopt) override;

    Result<int32_t> FilterAndOverwrite(
        const std::map<std::string, std::string>& partition,
        const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
        int64_t commit_identifier, std::optional<int64_t> watermark = std::nullopt) override;

    Result<std::string> GetLastCommitTableRequest() override;

    Result<int32_t> Expire() override;

    Status DropPartition(const std::vector<std::map<std::string, std::string>>& partitions,
                         int64_t commit_identifier) override;

    Status TruncateTable(int64_t commit_identifier) override;

    Result<bool> RollbackToAsLatest(int64_t target_snapshot_id) override;

    FileStoreCommit& RowIdCheckConflict(std::optional<int64_t> row_id_check_from_snapshot) override;

    std::shared_ptr<Metrics> GetCommitMetrics() const override;

 private:
    explicit FormatTableFileStoreCommit(const std::shared_ptr<FormatTable>& table);

    /// The messages a format table commit takes, or a refusal when one of them belongs to another
    /// table type. Nothing is published until every message has been recognised.
    static Result<std::vector<FormatCommitMessage>> ToFormatMessages(
        const std::vector<std::shared_ptr<CommitMessage>>& commit_messages);

    std::shared_ptr<FormatTable> table_;
};

}  // namespace paimon
