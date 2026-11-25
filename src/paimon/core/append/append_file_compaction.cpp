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

#include "paimon/append/append_file_compaction.h"

#include <utility>

#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/operation/append_only_file_store_write.h"
#include "paimon/file_store_write.h"
#include "paimon/write_context.h"

namespace paimon {

Result<std::vector<std::shared_ptr<CommitMessage>>> AppendFileCompaction::Execute(
    const std::string& table_path, const std::string& branch,
    const std::map<std::string, std::string>& partition, int32_t bucket,
    const std::set<std::string>& file_names, const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor) {
    WriteContextBuilder builder(table_path, "append-file-compaction");
    builder.WithBranch(branch)
        .SetOptions(options)
        .WithStreamingMode(false)
        .WithIgnorePreviousFiles(false)
        .WithIgnoreNumBucketCheck(false)
        .WithFileSystem(file_system);
    if (pool) {
        builder.WithMemoryPool(pool);
    }
    if (executor) {
        builder.WithExecutor(executor);
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> context, builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> write,
                           FileStoreWrite::Create(std::move(context)));
    ScopeGuard close_guard([&]() { (void)write->Close(); });
    auto* append_write = dynamic_cast<AppendOnlyFileStoreWrite*>(write.get());
    if (!append_write) {
        return Status::NotImplemented("custom compaction only supports fixed-bucket append tables");
    }
    PAIMON_RETURN_NOT_OK(append_write->CompactFiles(partition, bucket, file_names));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> messages,
                           write->PrepareCommit());
    // PrepareCommit drains the increments; Close leaves their files available for commit or abort.
    close_guard.Release();
    PAIMON_RETURN_NOT_OK(write->Close());
    return messages;
}

}  // namespace paimon
