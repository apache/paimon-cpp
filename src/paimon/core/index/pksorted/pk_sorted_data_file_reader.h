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
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "arrow/type_fwd.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/result.h"

namespace arrow {
class StructArray;
}  // namespace arrow

namespace paimon {
class BinaryRow;
class Executor;
class DataFilePathFactory;
class FileBatchReader;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;
class Predicate;
class TableSchema;
struct DataFileMeta;

/// Reads one indexed column from primary-key data files without applying deletion vectors.
///
/// The reader validates that projected batches contain every physical row in file order before
/// passing them to the callback, so group row ids remain identical to the Java source-backed index
/// contract.
class PkSortedDataFileReader : public RawFileSplitRead {
 public:
    using BatchConsumer = std::function<Status(const std::shared_ptr<arrow::StructArray>&)>;

    static Result<std::unique_ptr<PkSortedDataFileReader>> Create(
        const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
        int32_t field_id, const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::string& branch, const CoreOptions& options,
        const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool);

    Status ReadFile(const BinaryRow& partition, int32_t bucket,
                    const std::shared_ptr<DataFileMeta>& file, const BatchConsumer& consumer) const;

 protected:
    Result<std::unique_ptr<FileBatchReader>> ApplyIndexAndDvReaderIfNeeded(
        std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
        const std::shared_ptr<arrow::Schema>& data_schema,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate, DeletionVector::Factory dv_factory,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const override;

 private:
    PkSortedDataFileReader(const std::shared_ptr<FileStorePathFactory>& path_factory,
                           const std::shared_ptr<InternalReadContext>& context,
                           const std::shared_ptr<MemoryPool>& pool,
                           const std::shared_ptr<Executor>& executor);
};

}  // namespace paimon
