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
/// A directory has no metadata to switch, so a commit is not atomic across files: a reader
/// scanning midway sees the ones renamed so far, though each file becomes visible whole. A commit
/// that fails partway takes back every target it tried to publish, best effort; an overwrite
/// cannot, having deleted what it replaces first.
///
/// Only this job's own messages may be passed in: the checks here can tell that a message
/// describes this table, not whose staged file it names. Not thread-safe, and two overwriting
/// commits over the same directory race.
class FormatTableCommit {
 public:
    /// @param overwrite Whether the commit replaces what is already there instead of adding to
    ///        it. What it replaces: the static partition when one is given, otherwise the
    ///        partitions this commit writes when `dynamic-partition-overwrite` is on and the table
    ///        is partitioned, and the whole table otherwise. Replacing the table empties it even
    ///        when no file is published.
    /// @param static_partition Partition the commit writes to, keyed by partition field name. It
    ///        may name only the leading keys, standing for every partition below that prefix.
    ///        Empty leaves the partitions to the written files.
    static Result<std::unique_ptr<FormatTableCommit>> Create(
        const std::shared_ptr<FormatTable>& table, bool overwrite,
        const std::map<std::string, std::string>& static_partition);

    ~FormatTableCommit();

    /// Renames every written file into place, first clearing what it replaces when the commit
    /// overwrites.
    Status Commit(const std::vector<FormatCommitMessage>& commit_messages);

    /// Removes the staged files of `commit_messages` instead of publishing them.
    ///
    /// It undoes a commit that never happened, not one that did: a file already renamed into
    /// place is no longer staged and stays. It never fails, so the log is the only signal that a
    /// cleanup fell short.
    Status Abort(const std::vector<FormatCommitMessage>& commit_messages);

 private:
    FormatTableCommit(const std::shared_ptr<FormatTable>& table, bool overwrite,
                      const std::map<std::string, std::string>& static_partition,
                      bool dynamic_partition_overwrite);

    /// Whether an overwrite naming no partition replaces only the partitions this commit wrote.
    /// An unpartitioned table has no partitions to select, so it is always replaced whole.
    bool ReplacesOnlyWrittenPartitions() const;

    /// The body of `Commit()`, so that every failure in it is followed by the same cleanup.
    Status CommitImpl(const std::vector<FormatCommitMessage>& commit_messages);

    /// Deletes the committed data files under `directory`, leaving another writer's staged files
    /// alone.
    ///
    /// @param partition_levels Directory levels below `directory` that still hold partition
    ///        directories, which a static partition naming only the leading keys leaves behind.
    Status DeletePreviousDataFiles(const std::string& directory, int32_t partition_levels) const;

    std::shared_ptr<FormatTable> table_;
    bool overwrite_ = false;
    std::map<std::string, std::string> static_partition_;
    /// Read once at `Create()`, so an overwrite cannot see a different value than the table was
    /// opened with.
    bool dynamic_partition_overwrite_ = true;
};

}  // namespace paimon
