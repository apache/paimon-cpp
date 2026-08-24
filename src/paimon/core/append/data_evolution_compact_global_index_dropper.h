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
class IndexFileHandler;
class Snapshot;

/// Drops the global indexes a materialized data-evolution compaction invalidates.
///
/// A global index maps a value to the row id holding it. Materializing deletion vectors gives
/// the surviving rows new row ids, so every global index of a touched partition now points at
/// rows that moved or no longer exist. There is no way to fix such an index up from the
/// compaction's own metadata, so it is dropped and has to be rebuilt.
///
/// Only a *materialized* task invalidates anything: a normal data-evolution compaction keeps
/// row ids, which is exactly what lets its indexes stand. The two are told apart the same way
/// the deletion vector rewriter tells them apart — by whether the rewritten files carry row ids.
class DataEvolutionCompactGlobalIndexDropper {
 public:
    DataEvolutionCompactGlobalIndexDropper() = delete;
    ~DataEvolutionCompactGlobalIndexDropper() = delete;

    /// Returns index-only commit messages deleting the global indexes of every partition
    /// `compact_messages` materialized, or an empty vector when none did.
    ///
    /// @param latest_snapshot The snapshot to scan, which should be the newest one rather than
    ///                        the one the compaction planned against: an index built
    ///                        concurrently still refers to pre-materialization row ids and has
    ///                        to be dropped as well.
    static Result<std::vector<std::shared_ptr<CommitMessage>>> DropGlobalIndexes(
        const std::vector<std::shared_ptr<CommitMessage>>& compact_messages,
        const Snapshot& latest_snapshot,
        const std::shared_ptr<IndexFileHandler>& index_file_handler);
};

}  // namespace paimon
