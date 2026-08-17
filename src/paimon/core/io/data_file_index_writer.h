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
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/io/file_index_options.h"
#include "paimon/result.h"

namespace arrow {
class Field;
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {

class Bytes;
class DataFilePathFactory;
class FileIndexWriter;
class FileSystem;
class MemoryPool;

struct FileIndexWriteResult {
    std::shared_ptr<Bytes> embedded_index;
    std::vector<std::optional<std::string>> extra_files;
};

/// Builds every configured column index for one data file.
class DataFileIndexWriter {
 public:
    static Result<std::unique_ptr<DataFileIndexWriter>> Create(
        const std::shared_ptr<arrow::Schema>& logical_schema, const FileIndexOptions& options,
        const std::shared_ptr<FileSystem>& file_system,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(const std::shared_ptr<arrow::StructArray>& logical_batch);

    Result<FileIndexWriteResult> Finish(const std::string& data_file_path);

    void Abort();

    const std::optional<std::string>& ExternalIndexPath() const {
        return external_index_path_;
    }

 private:
    struct IndexWriterEntry {
        std::string column_name;
        std::string index_type;
        int32_t field_index;
        std::shared_ptr<arrow::Field> field;
        std::shared_ptr<FileIndexWriter> writer;
    };

    DataFileIndexWriter(std::vector<IndexWriterEntry>&& writers, int64_t in_manifest_threshold,
                        const std::shared_ptr<FileSystem>& file_system,
                        const std::shared_ptr<DataFilePathFactory>& path_factory,
                        const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<Bytes>> SerializeContainer();
    Status WriteExternal(const std::string& path, const std::shared_ptr<Bytes>& bytes);

    std::vector<IndexWriterEntry> writers_;
    int64_t in_manifest_threshold_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<MemoryPool> pool_;
    std::optional<std::string> external_index_path_;
};

}  // namespace paimon
