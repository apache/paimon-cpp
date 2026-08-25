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

#include "paimon/defs.h"
#include "paimon/file_store_write.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class FormatTable;
class FormatTableWrite;
class MemoryPool;
class Metrics;

/// Writes a format table through the `FileStoreWrite` interface, so that a caller holding a table
/// path writes it the way it writes any other table. Java Paimon does the same through
/// `FormatTable.newBatchWriteBuilder()`.
///
/// A format table has no manifests and no snapshots, so the parts of `FileStoreWrite` that are
/// about them (compaction, and the commit identifier a streaming write carries) have nothing here
/// to act on and are refused rather than quietly ignored.
class FormatTableFileStoreWrite : public FileStoreWrite {
 public:
    static Result<std::unique_ptr<FormatTableFileStoreWrite>> Create(
        const std::shared_ptr<FormatTable>& table, const std::shared_ptr<MemoryPool>& pool);

    ~FormatTableFileStoreWrite() override;

    Status Write(std::unique_ptr<RecordBatch>&& batch) override;

    Status Compact(const std::map<std::string, std::string>& partition, int32_t bucket,
                   bool full_compaction) override;

    Result<std::vector<std::shared_ptr<CommitMessage>>> PrepareCommit(
        bool wait_compaction, int64_t commit_identifier) override;

    std::shared_ptr<Metrics> GetMetrics() const override;

    Status Close() override;

 private:
    explicit FormatTableFileStoreWrite(std::unique_ptr<FormatTableWrite>&& write);

    std::unique_ptr<FormatTableWrite> write_;
};

}  // namespace paimon
