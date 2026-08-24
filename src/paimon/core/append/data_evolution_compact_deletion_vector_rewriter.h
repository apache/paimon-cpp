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

#include "paimon/result.h"

namespace paimon {

class CommitMessage;
class CoreOptions;
class IndexFileHandler;
class Snapshot;

/// Moves the deletion vectors of a data-evolution compaction onto its rewritten files.
///
/// Data-evolution compaction rewrites files without applying their deletion vectors: every
/// input row survives into the output file, and row ids are preserved. The deletions
/// themselves are keyed by the *anchor file* of each row range group (see
/// `DataEvolutionUtils::RetrieveAnchorFile`), so once a task replaces a group's files with a
/// single new file, the vectors have to be re-keyed onto that file before the compaction is
/// committed — otherwise the deleted rows come back.
///
/// The rewriter consumes the compact commit messages of one round and returns extra
/// index-only commit messages that add the rewritten deletion-vector index files and delete
/// the ones they replace. The caller commits both sets together, so the data files and their
/// deletions move in a single atomic snapshot.
///
/// The rewrite is scoped to what it actually changes:
///
/// - Ownership of a vector is taken from the index metadata, which already records which data
///   files each index file stores a vector for and where. Building it deserializes nothing.
/// - An index file is opened only if a move takes one of its vectors away. An untouched index
///   file is never opened, however many vectors the partition holds.
/// - Such a touched index file is replaced whole, because its remaining vectors have to move
///   into the file that takes over: they are read through a single opened stream, by their
///   recorded positions, and rewritten. One left with no vector at all is simply deleted.
/// - A replaced file's vector is taken away whether or not it deletes anything, so no entry is
///   ever left keying a vector by a data file this commit removes. That keeps this rule and
///   the commit-side migration check, which can only see the recorded cardinality, deciding on
///   the same thing.
/// - New index files are rolled at `deletion-vector.index-file.target-size` rather than
///   written as one file, which also keeps a large rewrite inside the int32 offsets an index
///   file addresses its vectors with.
///
/// Both vector kinds are supported. A moved vector is rebuilt as the kind the table stores,
/// because the two serialize differently and refuse to merge into one another.
class DataEvolutionCompactDeletionVectorRewriter {
 public:
    DataEvolutionCompactDeletionVectorRewriter() = delete;
    ~DataEvolutionCompactDeletionVectorRewriter() = delete;

    /// Rewrites the deletion vectors touched by `compact_messages`.
    ///
    /// @param compact_messages The compact commit messages of one coordinator round. Messages
    ///                         carrying no normal data file are ignored.
    /// @param snapshot The snapshot the compaction planned against; its deletion-vector index
    ///                 is the one being moved.
    /// @param core_options The table's options. A table without deletion vectors returns no
    ///                     messages, so a caller that does not pre-check stays correct.
    /// @param index_file_handler Reads the snapshot's index files and writes the new ones.
    /// @return Index-only commit messages, one per touched partition, or an empty vector when
    ///         nothing had to move.
    static Result<std::vector<std::shared_ptr<CommitMessage>>> RewriteDeletionVectors(
        const std::vector<std::shared_ptr<CommitMessage>>& compact_messages,
        const Snapshot& snapshot, const CoreOptions& core_options,
        const std::shared_ptr<IndexFileHandler>& index_file_handler);
};

}  // namespace paimon
