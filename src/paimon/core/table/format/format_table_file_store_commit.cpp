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

#include "paimon/core/table/format/format_table_file_store_commit.h"

#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/table/format/format_commit_message.h"
#include "paimon/core/table/format/format_table_commit.h"
#include "paimon/table/format/format_table.h"

namespace paimon {

namespace {

/// The refusal the snapshot half of `FileStoreCommit` returns, so a caller reading one knows to
/// look at the table type rather than at its own arguments.
Status UnsupportedFormatTableOperation(const char* what) {
    return Status::NotImplemented(
        fmt::format("a format table has no snapshots or manifests, so {}", what));
}

}  // namespace

FormatTableFileStoreCommit::FormatTableFileStoreCommit(const std::shared_ptr<FormatTable>& table)
    : table_(table) {}

FormatTableFileStoreCommit::~FormatTableFileStoreCommit() = default;

Result<std::unique_ptr<FormatTableFileStoreCommit>> FormatTableFileStoreCommit::Create(
    const std::shared_ptr<FormatTable>& table) {
    if (table == nullptr) {
        return Status::Invalid("format table commit requires a table");
    }
    return std::unique_ptr<FormatTableFileStoreCommit>(new FormatTableFileStoreCommit(table));
}

Result<std::vector<FormatCommitMessage>> FormatTableFileStoreCommit::ToFormatMessages(
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages) {
    std::vector<FormatCommitMessage> messages;
    messages.reserve(commit_messages.size());
    for (const std::shared_ptr<CommitMessage>& commit_message : commit_messages) {
        auto message = std::dynamic_pointer_cast<FormatCommitMessage>(commit_message);
        if (message == nullptr) {
            return Status::Invalid(
                "a format table commit takes the messages a format table write produced; this one "
                "describes files to record in a manifest");
        }
        messages.push_back(*message);
    }
    return messages;
}

Status FormatTableFileStoreCommit::Commit(
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages, int64_t commit_identifier,
    std::optional<int64_t> watermark) {
    if (commit_identifier != BATCH_WRITE_COMMIT_IDENTIFIER) {
        return UnsupportedFormatTableOperation("a commit identifier has nowhere to be recorded");
    }
    if (watermark) {
        return UnsupportedFormatTableOperation("a watermark has nowhere to be recorded");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<FormatCommitMessage> messages,
                           ToFormatMessages(commit_messages));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table_, /*overwrite=*/false, /*static_partition=*/{}));
    return commit->Commit(messages);
}

Status FormatTableFileStoreCommit::Overwrite(
    const std::map<std::string, std::string>& partition,
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages, int64_t commit_identifier,
    std::optional<int64_t> watermark) {
    if (commit_identifier != BATCH_WRITE_COMMIT_IDENTIFIER) {
        return UnsupportedFormatTableOperation("a commit identifier has nowhere to be recorded");
    }
    if (watermark) {
        return UnsupportedFormatTableOperation("a watermark has nowhere to be recorded");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<FormatCommitMessage> messages,
                           ToFormatMessages(commit_messages));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableCommit> commit,
                           FormatTableCommit::Create(table_, /*overwrite=*/true, partition));
    return commit->Commit(messages);
}

Status FormatTableFileStoreCommit::Abort(
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<FormatCommitMessage> messages,
                           ToFormatMessages(commit_messages));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table_, /*overwrite=*/false, /*static_partition=*/{}));
    return commit->Abort(messages);
}

Result<int64_t> FormatTableFileStoreCommit::CommitWithProgress(
    const std::vector<RealtimeCommitProgress>& realtime_commits, int64_t commit_identifier,
    std::optional<int64_t> watermark) {
    return UnsupportedFormatTableOperation("it has no real-time offsets to publish with a commit");
}

Result<int32_t> FormatTableFileStoreCommit::FilterAndCommit(
    const std::map<int64_t, std::vector<std::shared_ptr<CommitMessage>>>&
        commit_identifier_and_messages,
    std::optional<int64_t> watermark) {
    return UnsupportedFormatTableOperation(
        "nothing records which commit identifiers have already been committed");
}

Result<int32_t> FormatTableFileStoreCommit::FilterAndOverwrite(
    const std::map<std::string, std::string>& partition,
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages, int64_t commit_identifier,
    std::optional<int64_t> watermark) {
    return UnsupportedFormatTableOperation(
        "nothing records which commit identifiers have already been committed");
}

Result<std::string> FormatTableFileStoreCommit::GetLastCommitTableRequest() {
    return UnsupportedFormatTableOperation(
        "it commits by renaming files rather than through a rest catalog");
}

Result<int32_t> FormatTableFileStoreCommit::Expire() {
    return UnsupportedFormatTableOperation("there are no snapshots to expire");
}

Status FormatTableFileStoreCommit::DropPartition(
    const std::vector<std::map<std::string, std::string>>& partitions, int64_t commit_identifier) {
    // Not refused for lack of snapshots but simply missing: dropping a partition means removing
    // its directory, which nothing here does yet. An overwrite of that partition with no messages
    // empties it, which is as far as this goes today.
    return Status::NotImplemented(
        "dropping a partition of a format table is not implemented yet; an overwrite of it with "
        "no commit messages empties it instead");
}

Status FormatTableFileStoreCommit::TruncateTable(int64_t commit_identifier) {
    return Status::NotImplemented("emptying a format table is not implemented yet");
}

Result<bool> FormatTableFileStoreCommit::RollbackToAsLatest(int64_t target_snapshot_id) {
    return UnsupportedFormatTableOperation("there is no snapshot to roll back to");
}

FileStoreCommit& FormatTableFileStoreCommit::RowIdCheckConflict(
    std::optional<int64_t> row_id_check_from_snapshot) {
    // The interface returns a reference and cannot report a refusal, so this is the one call that
    // has to be a no-op. A format table records no row ids, so there is no conflict to check.
    return *this;
}

std::shared_ptr<Metrics> FormatTableFileStoreCommit::GetCommitMetrics() const {
    // Empty rather than null, as on the write side's `GetMetrics()`.
    return std::make_shared<MetricsImpl>();
}

}  // namespace paimon
