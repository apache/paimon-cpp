/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <memory>
#include <vector>

#include "paimon/result.h"

namespace paimon {

class CommitMessage;
class FileStorePathFactory;
class FileSystem;
class Logger;

/// Deletes the files a set of commit messages describes, for messages that will never be
/// committed.
///
/// A writer hands its files over when it produces a commit message: `PrepareCommit` drains
/// them, so from then on nothing the writer does removes them. Two callers have to finish that
/// job when the commit does not happen — `FileStoreCommit::Abort`, for a commit that failed or
/// was given up on, and the write path itself, when `PrepareCommit` fails after some of its
/// messages were already produced. Both delete the same set, hence one implementation.
///
/// Removed per message: the new and rewritten data files with their companion files (a managed
/// blob reference sidecar, for instance), the new index files, and the managed blob packs the
/// message *owns*. Owning a pack and referencing one are different things — a compacted file's
/// sidecar lists the packs of the files it merged, which live snapshots still read — so only
/// `CommitMessageImpl::OwnedManagedBlobPacks` is consulted, never a sidecar.
class UncommittedFileCleaner {
 public:
    UncommittedFileCleaner() = delete;
    ~UncommittedFileCleaner() = delete;

    /// Best effort throughout: an individual delete failure is ignored, or logged when nothing
    /// would collect the file later, and a message that cannot be handled at all — one this
    /// cleaner cannot interpret, or whose paths cannot be resolved — is skipped rather than
    /// ending the pass, so it never strands the messages behind it. The first such failure is
    /// returned once every message has been visited, because files were left behind.
    static Status Delete(const std::shared_ptr<FileStorePathFactory>& path_factory,
                         const std::shared_ptr<FileSystem>& fs,
                         const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                         Logger* logger);
};

}  // namespace paimon
