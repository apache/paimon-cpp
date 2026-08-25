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

#include "paimon/core/table/format/format_commit_message.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/format/format_table.h"

namespace paimon {

/// Publishes the files a `FormatTableWrite` produced, by renaming each out of the `_temporary`
/// directory it was staged in.
///
/// A directory has no metadata to switch, so a commit is not atomic across files: it renames them
/// one at a time, and a reader scanning midway sees the ones renamed so far. Each rename does
/// guarantee that a file becomes visible whole, never half-written.
///
/// A commit that fails partway tries to remove the files it had already renamed, on a best-effort
/// basis: a file that cannot be removed is reported in the log and stays. An overwriting commit is
/// further limited - the data it replaces is deleted before the new files are published and cannot
/// be brought back, so a failure there leaves the table without the replaced data.
///
/// Only the messages this job's own writers produced may be passed in. The checks here can tell
/// that a message's path belongs to this table, sits in the partition it declares, and is staged
/// rather than already published - not whose staged file it is.
///
/// Not thread-safe. Separate commits may add to one table at once, each publishing only the files
/// its own messages name; two overwriting commits over the same directory race, since an overwrite
/// clears everything committed there before publishing anything.
class FormatTableCommit {
 public:
    /// @param table Table to commit to.
    /// @param overwrite Whether the commit replaces the data already in the directories it writes
    ///        to, instead of adding to it. Without a static partition that means every partition
    ///        the commit touches; with one it means the partitions that spec covers, whether or
    ///        not this commit wrote to them.
    /// @param static_partition Partition the commit writes to, keyed by partition field name. It
    ///        may name only the leading partition keys, in which case it stands for every
    ///        partition below that prefix. Empty means the partitions are whatever the written
    ///        files say they are.
    static Result<std::unique_ptr<FormatTableCommit>> Create(
        const std::shared_ptr<FormatTable>& table, bool overwrite,
        const std::map<std::string, std::string>& static_partition);

    ~FormatTableCommit();

    /// Renames every written file into place, first clearing what it replaces when the commit
    /// overwrites.
    Status Commit(const std::vector<FormatCommitMessage>& commit_messages);

    /// Removes the staged files of `commit_messages` instead of publishing them, on a
    /// best-effort basis.
    ///
    /// It undoes a commit that never happened, not one that did: a file already renamed into
    /// place is no longer staged and stays where it is.
    ///
    /// It never fails. A message that is refused, or a file that cannot be removed, does not stop
    /// the remaining messages from being cleaned up, so the log is the only signal that a cleanup
    /// did not fully succeed.
    Status Abort(const std::vector<FormatCommitMessage>& commit_messages);

 private:
    FormatTableCommit(const std::shared_ptr<FormatTable>& table, bool overwrite,
                      const std::map<std::string, std::string>& static_partition);

    /// The body of `Commit()`, so that every failure in it is followed by the same cleanup.
    Status CommitImpl(const std::vector<FormatCommitMessage>& commit_messages);

    /// Deletes the committed data files under `directory`, leaving what another writer has staged
    /// there untouched.
    ///
    /// @param partition_levels How many directory levels below `directory` still hold partition
    ///        directories, which a static partition naming only the leading keys leaves behind.
    Status DeletePreviousDataFiles(const std::string& directory, int32_t partition_levels) const;

    std::shared_ptr<FormatTable> table_;
    bool overwrite_ = false;
    std::map<std::string, std::string> static_partition_;
};

}  // namespace paimon
