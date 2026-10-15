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
#include <string>
#include <vector>

#include "arrow/type.h"
#include "paimon/global_index/full_text/full_text_ffi_utils.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/logging.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::full_text {

/// Writes one full-text index archive per shard through the native `paimon-full-text-index`
/// engine, aligned with Java `NativeFullTextGlobalIndexWriter`.
///
/// Each non-null value is added as a document keyed by its relative row id. Null values advance
/// the row count but are not indexed. A writer that received no rows writes no file.
class FullTextGlobalIndexWriter : public GlobalIndexWriter {
 public:
    /// @param options Full-text options with the `full-text.` prefix already stripped. They are
    ///                passed to the native engine and stored as flat JSON in the index metadata.
    static Result<std::shared_ptr<FullTextGlobalIndexWriter>> Create(
        const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(::ArrowArray* arrow_array, std::vector<int64_t>&& relative_row_ids) override;

    Result<std::vector<GlobalIndexIOMeta>> Finish() override;

 private:
    FullTextGlobalIndexWriter(const std::string& field_name,
                              const std::shared_ptr<arrow::DataType>& arrow_type,
                              FtindexWriterPtr writer,
                              const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
                              const std::map<std::string, std::string>& options,
                              const std::shared_ptr<MemoryPool>& pool);

    Result<GlobalIndexIOMeta> WriteIndex(PaimonFtindexWriterHandle* writer) const;

    std::shared_ptr<MemoryPool> pool_;
    std::string field_name_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    FtindexWriterPtr writer_;
    std::shared_ptr<GlobalIndexFileWriter> file_writer_;
    std::map<std::string, std::string> options_;
    /// Number of rows received, including null values.
    int64_t row_count_ = 0;
    bool finished_ = false;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon::full_text
