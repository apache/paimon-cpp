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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/pk/primary_key_index_definition.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/result.h"

namespace paimon {
class CommitIncrement;
class Executor;
class FileStorePathFactory;
class IOManager;
class IndexFileHandler;
class MemoryPool;
class PkSortedIndexBuilder;
class TableSchema;

/// Maintains Java-compatible source-backed primary-key indexes for one partition and bucket.
///
/// This initial implementation builds synchronously during prepare-commit. Synchronous execution
/// deliberately omits Java's scheduling and retry policy while preserving the same source-level
/// reconciliation, payload format, rollback, and atomic commit semantics.
class BucketedPrimaryKeyIndexMaintainer {
 public:
    class Factory {
     public:
        static Result<std::shared_ptr<Factory>> Create(
            const std::string& root_path, const std::string& branch,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::vector<PrimaryKeyIndexDefinition>& definitions,
            const std::shared_ptr<FileStorePathFactory>& path_factory,
            const std::shared_ptr<IndexFileHandler>& index_file_handler, const CoreOptions& options,
            const std::shared_ptr<IOManager>& io_manager, bool enable_multi_thread_spill,
            const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool);

        Result<std::shared_ptr<BucketedPrimaryKeyIndexMaintainer>> CreateMaintainer(
            const BinaryRow& partition, int32_t bucket,
            const std::vector<std::shared_ptr<DataFileMeta>>& restored_data_files,
            const std::vector<std::shared_ptr<IndexFileMeta>>& restored_payloads) const;

        bool Enabled() const {
            return !definitions_.empty();
        }

        std::shared_ptr<IndexFileHandler> GetIndexFileHandler() const {
            return index_file_handler_;
        }

     private:
        Factory(std::string root_path, std::string branch,
                const std::shared_ptr<TableSchema>& table_schema,
                std::vector<PrimaryKeyIndexDefinition> definitions,
                const std::shared_ptr<FileStorePathFactory>& path_factory,
                const std::shared_ptr<IndexFileHandler>& index_file_handler,
                const CoreOptions& options, const std::shared_ptr<IOManager>& io_manager,
                bool enable_multi_thread_spill, const std::shared_ptr<Executor>& executor,
                const std::shared_ptr<MemoryPool>& pool)
            : root_path_(std::move(root_path)),
              branch_(std::move(branch)),
              table_schema_(table_schema),
              definitions_(std::move(definitions)),
              path_factory_(path_factory),
              index_file_handler_(index_file_handler),
              options_(options),
              io_manager_(io_manager),
              enable_multi_thread_spill_(enable_multi_thread_spill),
              executor_(executor),
              pool_(pool) {}

        std::string root_path_;
        std::string branch_;
        std::shared_ptr<TableSchema> table_schema_;
        std::vector<PrimaryKeyIndexDefinition> definitions_;
        std::shared_ptr<FileStorePathFactory> path_factory_;
        std::shared_ptr<IndexFileHandler> index_file_handler_;
        CoreOptions options_;
        std::shared_ptr<IOManager> io_manager_;
        bool enable_multi_thread_spill_;
        std::shared_ptr<Executor> executor_;
        std::shared_ptr<MemoryPool> pool_;
    };

    Status PrepareCommit(CommitIncrement* increment);

 private:
    struct FieldMaintainer {
        PrimaryKeyIndexDefinition definition;
        std::shared_ptr<PkSortedIndexBuilder> builder;
    };

    BucketedPrimaryKeyIndexMaintainer(
        std::vector<FieldMaintainer> fields,
        std::map<std::string, std::shared_ptr<DataFileMeta>> active_data_files,
        std::vector<std::shared_ptr<IndexFileMeta>> active_payloads)
        : fields_(std::move(fields)),
          active_data_files_(std::move(active_data_files)),
          active_payloads_(std::move(active_payloads)) {}

    std::vector<FieldMaintainer> fields_;
    std::map<std::string, std::shared_ptr<DataFileMeta>> active_data_files_;
    std::vector<std::shared_ptr<IndexFileMeta>> active_payloads_;
};

}  // namespace paimon
