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

#include "paimon/core/table/format/format_table_file_store_write.h"

#include <utility>

#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/table/format/format_commit_message.h"
#include "paimon/core/table/format/format_table_write.h"
#include "paimon/table/format/format_table.h"

namespace paimon {

FormatTableFileStoreWrite::FormatTableFileStoreWrite(std::unique_ptr<FormatTableWrite>&& write)
    : write_(std::move(write)) {}

FormatTableFileStoreWrite::~FormatTableFileStoreWrite() = default;

Result<std::unique_ptr<FormatTableFileStoreWrite>> FormatTableFileStoreWrite::Create(
    const std::shared_ptr<FormatTable>& table, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableWrite> write,
                           FormatTableWrite::Create(table, pool));
    return std::unique_ptr<FormatTableFileStoreWrite>(
        new FormatTableFileStoreWrite(std::move(write)));
}

Status FormatTableFileStoreWrite::Write(std::unique_ptr<RecordBatch>&& batch) {
    if (write_ == nullptr) {
        return Status::Invalid("format table write has been closed");
    }
    return write_->Write(std::move(batch));
}

Status FormatTableFileStoreWrite::Compact(const std::map<std::string, std::string>& partition,
                                          int32_t bucket, bool full_compaction) {
    return Status::NotImplemented(
        "a format table cannot be compacted: it has no manifests to rewrite and no buckets to "
        "compact within");
}

Result<std::vector<std::shared_ptr<CommitMessage>>> FormatTableFileStoreWrite::PrepareCommit(
    bool wait_compaction, int64_t commit_identifier) {
    if (write_ == nullptr) {
        return Status::Invalid("format table write has been closed");
    }
    // `wait_compaction` asks to wait rather than to do anything, and there is no compaction here
    // to wait for, so it is honoured by returning at once rather than refused.
    //
    // A commit identifier is how a streaming write tells its attempts apart in a snapshot; a
    // format table has no snapshots to record one in, so only a batch write fits here.
    if (commit_identifier != BATCH_WRITE_COMMIT_IDENTIFIER) {
        return Status::NotImplemented(
            "a format table takes batch writes only: it has no snapshot to record a commit "
            "identifier in");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<FormatCommitMessage> messages, write_->PrepareCommit());
    std::vector<std::shared_ptr<CommitMessage>> result;
    result.reserve(messages.size());
    for (const FormatCommitMessage& message : messages) {
        result.push_back(std::make_shared<FormatCommitMessage>(message));
    }
    return result;
}

std::shared_ptr<Metrics> FormatTableFileStoreWrite::GetMetrics() const {
    // Empty rather than null, so a caller merging metrics from several writers need not tell a
    // table type that keeps none apart from one that does. What this write produced is on the
    // commit messages it hands out.
    return std::make_shared<MetricsImpl>();
}

Status FormatTableFileStoreWrite::Close() {
    // Closing drops the write rather than aborting it: a write that never prepared a commit clears
    // what it staged from its own destructor, while one that has prepared has handed those files
    // to a commit and must leave them alone.
    write_.reset();
    return Status::OK();
}

}  // namespace paimon
