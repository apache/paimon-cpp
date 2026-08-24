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
#include <set>
#include <string>
#include <vector>

#include "arrow/memory_pool.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class LargeBinaryArray;
}  // namespace arrow

namespace paimon {

/// Materializes the managed blob columns of a primary-key read: every non-null value is a
/// serialized `BlobDescriptor` pointing into a `.managed.blob` pack, and this reader replaces
/// it with the payload bytes through one ranged read per value. Wraps the fully merged batch
/// stream, so only values of surviving rows are ever fetched. Not used when the read keeps
/// descriptors (`blob-as-descriptor`).
class ManagedBlobResolvingBatchReader : public BatchReader {
 public:
    ManagedBlobResolvingBatchReader(std::unique_ptr<BatchReader>&& reader,
                                    std::vector<std::string> managed_blob_fields,
                                    const std::shared_ptr<FileSystem>& fs,
                                    const std::shared_ptr<MemoryPool>& pool);

    Result<ReadBatch> NextBatch() override;

    void Close() override {
        reader_->Close();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

 private:
    Result<std::shared_ptr<arrow::Array>> ResolveBlobColumn(
        const std::shared_ptr<arrow::LargeBinaryArray>& descriptor_array);

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<BatchReader> reader_;
    std::set<std::string> managed_blob_fields_;
    std::shared_ptr<FileSystem> fs_;
};

}  // namespace paimon
