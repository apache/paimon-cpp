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

#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace arrow {
class Field;
class Schema;
}  // namespace arrow

namespace paimon {

class CoreOptions;
class DataFilePathFactory;
class FileSystem;
class KeyOffsetLookup;
class MemoryPool;
struct DataFileMeta;

/// Uses the configured bitmap file index on `_REALTIME_OFFSET` to translate a pinned offset DV to
/// physical row positions. Instances are immutable and therefore safe to pin for lock-free I/O.
class RealtimeOffsetFileIndexLookup {
 public:
    static Result<std::shared_ptr<RealtimeOffsetFileIndexLookup>> Create(
        const std::shared_ptr<arrow::Schema>& data_schema, int64_t data_schema_id,
        const std::shared_ptr<arrow::Field>& deduplicate_key_field,
        const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<FileSystem>& file_system,
        const std::shared_ptr<MemoryPool>& memory_pool, const CoreOptions& options);

    Result<std::map<std::string, RoaringBitmap32>> LookupFilePositions(
        const RoaringBitmap64& offsets) const;

    Result<std::shared_ptr<const KeyOffsetLookup>> CreateKeyOffsetLookup() const;

    Result<std::shared_ptr<RealtimeOffsetFileIndexLookup>> WithDataFiles(
        const std::vector<std::shared_ptr<DataFileMeta>>& data_files) const;

    const std::vector<std::shared_ptr<DataFileMeta>>& DataFiles() const {
        return data_files_;
    }

 private:
    RealtimeOffsetFileIndexLookup(std::shared_ptr<arrow::Schema> data_schema,
                                  int64_t data_schema_id,
                                  std::shared_ptr<arrow::Field> deduplicate_key_field,
                                  std::vector<std::shared_ptr<DataFileMeta>> data_files,
                                  std::shared_ptr<DataFilePathFactory> path_factory,
                                  std::shared_ptr<FileSystem> file_system,
                                  std::shared_ptr<MemoryPool> memory_pool,
                                  std::map<std::string, std::string> options);

    std::shared_ptr<arrow::Schema> data_schema_;
    int64_t data_schema_id_;
    std::shared_ptr<arrow::Field> deduplicate_key_field_;
    std::vector<std::shared_ptr<DataFileMeta>> data_files_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon
