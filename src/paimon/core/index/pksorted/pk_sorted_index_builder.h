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
#include <utility>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/pk/primary_key_index_definition.h"
#include "paimon/result.h"

namespace paimon {
class Executor;
class FileSystem;
class FileStorePathFactory;
class GlobalIndexFileManager;
class IOManager;
class IndexPathFactory;
class MemoryPool;
class PkSortedDataFileReader;
class TableSchema;
struct DataFileMeta;

/// Builds one Java-compatible source-backed BTree payload over a complete compacted data level.
class PkSortedIndexBuilder {
 public:
    static Result<std::unique_ptr<PkSortedIndexBuilder>> Create(
        const std::string& root_path, const std::string& branch, const BinaryRow& partition,
        int32_t bucket, const std::shared_ptr<TableSchema>& table_schema,
        const PrimaryKeyIndexDefinition& definition,
        const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& options,
        const std::shared_ptr<IOManager>& io_manager, bool enable_multi_thread_spill,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<IndexFileMeta>> Build(
        const std::vector<std::shared_ptr<DataFileMeta>>& source_files) const;

    Status DeletePayload(const std::shared_ptr<IndexFileMeta>& payload) const;

 private:
    PkSortedIndexBuilder(const BinaryRow& partition, int32_t bucket, DataField field,
                         PrimaryKeyIndexDefinition definition,
                         const std::shared_ptr<PkSortedDataFileReader>& data_file_reader,
                         const std::shared_ptr<FileSystem>& fs,
                         const std::shared_ptr<IndexPathFactory>& index_path_factory,
                         const CoreOptions& options, const std::shared_ptr<IOManager>& io_manager,
                         bool enable_multi_thread_spill, const std::shared_ptr<MemoryPool>& pool)
        : partition_(partition),
          bucket_(bucket),
          field_(std::move(field)),
          definition_(std::move(definition)),
          data_file_reader_(data_file_reader),
          fs_(fs),
          index_path_factory_(index_path_factory),
          options_(options),
          io_manager_(io_manager),
          enable_multi_thread_spill_(enable_multi_thread_spill),
          pool_(pool) {}

    BinaryRow partition_;
    int32_t bucket_;
    DataField field_;
    PrimaryKeyIndexDefinition definition_;
    std::shared_ptr<PkSortedDataFileReader> data_file_reader_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<IndexPathFactory> index_path_factory_;
    CoreOptions options_;
    std::shared_ptr<IOManager> io_manager_;
    bool enable_multi_thread_spill_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
