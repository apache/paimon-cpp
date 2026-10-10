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
#include <string>
#include <utility>
#include <vector>

#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/result.h"

namespace paimon::test {

enum class SearchIndexType { VECTOR, FULL_TEXT };

/// File Index test backend with configured matches in search order. It applies pre-filters
/// before limits and returns vector scores aligned with ascending file-local row positions.
class MockSearchFileIndex {
 public:
    MockSearchFileIndex() = delete;
    ~MockSearchFileIndex() = delete;

    static const char VECTOR_IDENTIFIER[];
    static const char FULL_TEXT_IDENTIFIER[];

    using ScoredRows = std::vector<std::pair<int32_t, float>>;

    /// Serialize configured matches into a mock backend payload, without a FileIndexFormat
    /// container. Use Serialize() to wrap it before reading through FileIndexFormat.
    /// @param rows File-local row positions and scores in search order. Full-text search ignores
    /// the scores; the mock reader applies pre-filters and limits in the supplied order.
    /// @param pool Memory pool for the serialized payload.
    /// @return Mock payload bytes, or an error status.
    static Result<std::shared_ptr<Bytes>> MakePayload(const ScoredRows& rows,
                                                      const std::shared_ptr<MemoryPool>& pool);

    /// Wrap per-column index payloads in a complete FileIndexFormat container.
    /// @param indexes Payloads keyed by field name and index identifier. A nullptr payload
    /// represents an empty index.
    /// @param pool Memory pool for the serialized container.
    /// @return Container bytes usable as an embedded index or external index file, or an error
    /// status.
    static Result<std::shared_ptr<Bytes>> Serialize(const FileIndexFormat::ColumnIndexes& indexes,
                                                    const std::shared_ptr<MemoryPool>& pool);

    /// Build a complete single-column mock index by combining MakePayload() and Serialize().
    /// @param field_name Name of the indexed field.
    /// @param index_type VECTOR_IDENTIFIER or FULL_TEXT_IDENTIFIER.
    /// @param rows Configured matches with the same semantics as MakePayload().
    /// @param pool Memory pool for the serialized index.
    /// @return Complete FileIndexFormat container bytes, or an error status.
    static Result<std::shared_ptr<Bytes>> MakeIndex(const std::string& field_name,
                                                    const std::string& index_type,
                                                    const ScoredRows& rows,
                                                    const std::shared_ptr<MemoryPool>& pool);
};

class MockSearchFileIndexReader final : public FileIndexReader {
 public:
    MockSearchFileIndexReader(std::string field_name, SearchIndexType index_type,
                              MockSearchFileIndex::ScoredRows rows);

    Result<std::shared_ptr<ScoredFileIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& search) override;

    Result<std::shared_ptr<FileIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& search) override;

 private:
    std::string field_name_;
    SearchIndexType index_type_;
    MockSearchFileIndex::ScoredRows rows_;
};

class MockSearchFileIndexer final : public FileIndexer {
 public:
    explicit MockSearchFileIndexer(SearchIndexType index_type);

    Result<std::shared_ptr<FileIndexReader>> CreateReader(
        ::ArrowSchema* c_schema, int32_t start, int32_t length,
        const std::shared_ptr<InputStream>& input,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<FileIndexWriter>> CreateWriter(
        ::ArrowSchema* c_schema, const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    SearchIndexType index_type_;
};

class MockVectorSearchFileIndexFactory final : public FileIndexerFactory {
 public:
    const char* Identifier() const override;

    Result<std::unique_ptr<FileIndexer>> Create(
        const std::map<std::string, std::string>& options) const override;
};

class MockFullTextSearchFileIndexFactory final : public FileIndexerFactory {
 public:
    const char* Identifier() const override;

    Result<std::unique_ptr<FileIndexer>> Create(
        const std::map<std::string, std::string>& options) const override;
};

}  // namespace paimon::test
