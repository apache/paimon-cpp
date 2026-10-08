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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "lumina/api/Options.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/indexer/lumina/lumina_index_accumulator.h"
#include "paimon/indexer/lumina/lumina_index_options.h"
#include "paimon/indexer/lumina/lumina_index_searcher.h"
#include "paimon/indexer/lumina/lumina_memory_pool.h"
#include "paimon/indexer/lumina/lumina_tag_utils.h"

namespace paimon::lumina {

class LuminaFileIndexer final : public FileIndexer {
 public:
    explicit LuminaFileIndexer(const std::map<std::string, std::string>& options)
        : options_(options) {}

    Result<std::optional<std::vector<std::string>>> GetExtraFieldNames() const override;

    Result<std::shared_ptr<FileIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, int32_t start, int32_t length,
        const std::shared_ptr<InputStream>& input_stream,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<FileIndexWriter>> CreateWriter(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    std::map<std::string, std::string> options_;
};

class LuminaFileIndexWriter final : public FileIndexWriter {
 public:
    LuminaFileIndexWriter(std::string field_name, std::shared_ptr<arrow::DataType> arrow_type,
                          const LuminaIndexInfo& index_info,
                          ::lumina::api::BuilderOptions&& builder_options,
                          std::vector<LuminaTagField>&& tag_fields,
                          std::shared_ptr<LuminaMemoryPool> pool);

    Status AddBatch(::ArrowArray* batch) override;

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const override;

 private:
    std::string field_name_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    LuminaIndexInfo index_info_;
    ::lumina::api::BuilderOptions builder_options_;
    std::vector<LuminaTagField> tag_fields_;
    std::shared_ptr<LuminaMemoryPool> pool_;
    int64_t row_count_ = 0;
    mutable LuminaIndexAccumulator accumulator_;
    mutable bool serialized_ = false;
};

class LuminaFileIndexReader final : public FileIndexReader {
 public:
    explicit LuminaFileIndexReader(std::unique_ptr<LuminaIndexSearcher>&& searcher);

    Result<std::shared_ptr<ScoredFileIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override;

 private:
    std::unique_ptr<LuminaIndexSearcher> searcher_;
};

}  // namespace paimon::lumina
