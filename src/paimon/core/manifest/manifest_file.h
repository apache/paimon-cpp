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
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/stats/simple_stats_collector.h"
#include "paimon/core/utils/objects_file.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class FileSystem;
class FileFormat;
class FileStorePathFactory;
class ReaderBuilder;
class WriterBuilder;
class PathFactory;
class ManifestFileMeta;
class ManifestEntry;
class MemoryPool;

/// This file includes several `ManifestEntry`s, representing the additional changes since last
/// snapshot.
class ManifestFile : public ObjectsFile<ManifestEntry> {
 public:
    static Result<std::unique_ptr<ManifestFile>> Create(
        const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<FileFormat>& file_format,
        const std::string& compression, const std::shared_ptr<FileStorePathFactory>& path_factory,
        int64_t target_file_size, const std::shared_ptr<MemoryPool>& pool,
        const CoreOptions& options, const std::shared_ptr<arrow::Schema>& partition_type);

    /// Write several `ManifestEntry`s into manifest files.
    ///
    /// @note This method is atomic.
    Result<std::vector<ManifestFileMeta>> Write(const std::vector<ManifestEntry>& entries);

 private:
    ManifestFile(const std::shared_ptr<FileSystem>& file_system,
                 const std::shared_ptr<ReaderBuilder>& reader_builder,
                 const std::shared_ptr<WriterBuilder>& writer_builder,
                 const std::string& compression, const std::shared_ptr<PathFactory>& path_factory,
                 int64_t target_file_size, const std::shared_ptr<MemoryPool>& pool,
                 const CoreOptions& options, const std::shared_ptr<arrow::Schema>& partition_type);

 private:
    int64_t target_file_size_;
    CoreOptions options_;
    std::shared_ptr<arrow::Schema> partition_type_;
};

}  // namespace paimon
