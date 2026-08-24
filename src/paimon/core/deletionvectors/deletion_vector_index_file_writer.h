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

#include <map>
#include <memory>
#include <vector>

#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {

/// Writer for deletion vector index file.
class DeletionVectorIndexFileWriter {
 public:
    DeletionVectorIndexFileWriter(const std::shared_ptr<FileSystem>& fs,
                                  const std::shared_ptr<IndexPathFactory>& path_factory,
                                  const std::shared_ptr<MemoryPool>& pool)
        : index_path_factory_(path_factory), fs_(fs), pool_(pool) {}

    /// The deletion file of the bucketed table is updated according to the bucket. If a compaction
    /// occurs and there is no longer a deletion file, an empty deletion file needs to be generated
    /// to overwrite the old file.
    /// TODO(yonghao.fyh): We can consider sending a message to delete the deletion file in the
    /// future.
    Result<std::shared_ptr<IndexFileMeta>> WriteSingleFile(
        const std::map<std::string, std::shared_ptr<DeletionVector>>& input);

    /// Writes `input` into as many index files as `target_size` allows, rolling to a new file
    /// once the current one reaches it, and returns them in write order.
    ///
    /// A single index file addresses its vectors with int32 offsets and lengths, so a large
    /// enough set of vectors cannot be written into one file at all. Rolling also keeps a
    /// rewrite from replacing many small index files with one oversized file.
    Result<std::vector<std::shared_ptr<IndexFileMeta>>> WriteWithRolling(
        const std::map<std::string, std::shared_ptr<DeletionVector>>& input, int64_t target_size);

 private:
    std::shared_ptr<IndexPathFactory> index_path_factory_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
