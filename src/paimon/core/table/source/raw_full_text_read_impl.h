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
#include <vector>

#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/executor.h"
#include "paimon/global_index/global_index_result.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "paimon/utils/row_range_index.h"

namespace paimon {
/// Files become visible to readers when their output streams close.
class InMemoryGlobalIndexFiles : public GlobalIndexFileWriter, public GlobalIndexFileReader {
 public:
    explicit InMemoryGlobalIndexFiles(const std::shared_ptr<MemoryPool>& pool);

    Result<std::string> NewPath(const std::string& prefix) const override;

    Result<std::unique_ptr<OutputStream>> NewOutputStream(
        const std::string& file_path) const override;

    Result<int64_t> GetFileSize(const std::string& file_path) const override;

    Result<std::unique_ptr<InputStream>> GetInputStream(
        const std::string& file_path) const override;

 private:
    struct Storage;
    class IndexOutputStream;

    std::shared_ptr<Storage> storage_;
};

class RawFullTextReadImpl {
 public:
    RawFullTextReadImpl(const std::string& root_path,
                        const std::shared_ptr<TableSchema>& table_schema,
                        const CoreOptions& options,
                        const std::shared_ptr<PredicateFilter>& partition_filter,
                        const std::shared_ptr<Predicate>& filter, const DataField& text_field,
                        const std::string& query, int32_t limit,
                        const std::shared_ptr<Executor>& executor,
                        const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<ScoredGlobalIndexResult>> Search(int64_t snapshot_id,
                                                            const std::string& index_type,
                                                            const RowRangeIndex& raw_rows) const;

 private:
    std::string root_path_;
    std::shared_ptr<TableSchema> table_schema_;
    CoreOptions options_;
    std::shared_ptr<PredicateFilter> partition_filter_;
    std::shared_ptr<Predicate> filter_;
    DataField text_field_;
    std::string query_;
    int32_t limit_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
