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

#include <memory>
#include <vector>

#include "paimon/core/table/format/format_commit_message.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/format/format_table.h"

namespace paimon {

/// Writes new rows into a format table.
///
/// Only inserts are supported: a directory of plain data files has nowhere to record that a row
/// replaced or removed an earlier one, so a batch carrying any other row kind is rejected.
///
/// Writing is two-phase, because a directory has no metadata to switch atomically: `Write()` fills
/// files in a `_temporary` directory beside where they will end up, which a scan skips, and only
/// `FormatTableCommit` renames them into place. That is the layout Java Paimon stages under too.
///
/// A write dropped before `PrepareCommit()` clears what it staged from its destructor. One that
/// has prepared has handed those files to a commit, so its destructor leaves them alone; if that
/// commit never happens, `Abort()` removes them.
///
/// The partition a batch belongs to comes from `RecordBatch::GetPartition()`, and the partition
/// columns are not written into the file: a Hive-style layout keeps those values in the directory
/// names. One batch therefore carries one partition, and every row is checked against it.
///
/// Not thread-safe: one write holds an open file per partition and a counter naming them, so a
/// single thread must drive it. Separate writes may fill one table at once, since each stages its
/// files under a uuid of its own.
class FormatTableWrite {
 public:
    /// @param table Table to write to.
    /// @param pool Memory pool the writers allocate from.
    static Result<std::unique_ptr<FormatTableWrite>> Create(
        const std::shared_ptr<FormatTable>& table, const std::shared_ptr<MemoryPool>& pool);

    ~FormatTableWrite();

    /// Writes a batch of rows. The batch's data must match the table schema, including its
    /// partition columns, whose values must also be given by `RecordBatch::SetPartition()`.
    Status Write(std::unique_ptr<RecordBatch>&& batch);

    /// Closes the written files and reports them for committing. The write cannot be used
    /// afterwards.
    Result<std::vector<FormatCommitMessage>> PrepareCommit();

    /// Removes every file this write has staged, on a best-effort basis.
    ///
    /// This is the one call still allowed after `PrepareCommit()`, and after itself, so a commit
    /// that is prepared and then abandoned can still be cleaned up. A file already published by
    /// `FormatTableCommit` is no longer staged and is not touched.
    Status Abort();

    class Impl;

 private:
    explicit FormatTableWrite(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
