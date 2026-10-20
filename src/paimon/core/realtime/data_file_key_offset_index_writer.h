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

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/type_fwd.h"
#include "paimon/common/utils/hash_map.h"
#include "paimon/result.h"

namespace paimon {
class FileSystem;
class KeySerializer;
class MemoryPool;

/// Builds one data file's hash -> global realtime offset index from its surviving logical rows.
/// Layout: [big-endian int32 version][native DenseHashTable<uint64_t, int64_t> layout].
class DataFileKeyOffsetIndexWriter {
 public:
    static Result<std::unique_ptr<DataFileKeyOffsetIndexWriter>> Create(
        const std::shared_ptr<arrow::Field>& key_field,
        const std::shared_ptr<FileSystem>& file_system,
        const std::shared_ptr<MemoryPool>& memory_pool);

    Status AddBatch(const std::shared_ptr<arrow::StructArray>& logical_batch);

    Result<std::string> Finish(const std::string& data_file_path, int64_t row_count);
    void Abort();

    const std::string& OutputPath() const {
        return output_path_;
    }

 private:
    DataFileKeyOffsetIndexWriter(const std::shared_ptr<arrow::Field>& key_field,
                                 const std::shared_ptr<FileSystem>& file_system,
                                 const std::shared_ptr<KeySerializer>& key_serializer,
                                 std::unique_ptr<HashMap<uint64_t, int64_t>> offsets,
                                 const std::shared_ptr<MemoryPool>& memory_pool);

    std::shared_ptr<arrow::Field> key_field_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<KeySerializer> key_serializer_;
    std::unique_ptr<HashMap<uint64_t, int64_t>> offsets_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::string output_path_;
    bool finished_ = false;
};
}  // namespace paimon
