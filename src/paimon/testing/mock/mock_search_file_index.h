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
/// before limits and returns search scores aligned with ascending file-local row positions.
class MockSearchFileIndex {
 public:
    MockSearchFileIndex() = delete;
    ~MockSearchFileIndex() = delete;

    static const char VECTOR_IDENTIFIER[];
    static const char FULL_TEXT_IDENTIFIER[];

    using ScoredRows = std::vector<std::pair<int32_t, float>>;

    struct QueryResult {
        ScoredRows rows;
        std::optional<double> selectivity = std::nullopt;
    };
    using QueryMatches = std::map<std::string, QueryResult>;

    struct Execution {
        std::string field_name;
        std::string query;
        bool needs_scores;
    };

    struct Statistics {
        std::vector<Execution> executions;
    };

    static Result<std::shared_ptr<Bytes>> MakeQueryPayload(const QueryMatches& queries,
                                                           const std::shared_ptr<MemoryPool>& pool);

    static Result<std::shared_ptr<Bytes>> MakeQueryIndex(const std::string& field_name,
                                                         const std::string& index_type,
                                                         const QueryMatches& queries,
                                                         const std::shared_ptr<MemoryPool>& pool);

    /// Serialize configured matches into a mock backend payload, without a FileIndexFormat
    /// container. Use Serialize() to wrap it before reading through FileIndexFormat.
    /// @param rows File-local row positions and scores in search order. Full-text search ignores
    /// the scores; the mock reader applies pre-filters and limits in the supplied order.
    /// @param pool Memory pool for the serialized payload.
    /// @return Mock payload bytes, or an error status.
    static Result<std::shared_ptr<Bytes>> MakePayload(const ScoredRows& rows,
                                                      const std::shared_ptr<MemoryPool>& pool);

    /// Read all raw payloads from a V1 or V2 container for merging test indexes. Null input
    /// returns an empty map; empty index entries retain their null payloads.
    static Result<FileIndexFormat::ColumnIndexes> ReadIndexes(
        const std::shared_ptr<InputStream>& input, const std::shared_ptr<MemoryPool>& pool);

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
                              MockSearchFileIndex::ScoredRows rows,
                              std::shared_ptr<MockSearchFileIndex::Statistics> statistics);

    MockSearchFileIndexReader(std::string field_name, SearchIndexType index_type,
                              MockSearchFileIndex::QueryMatches queries,
                              std::shared_ptr<MockSearchFileIndex::Statistics> statistics);

    bool SupportsSearch(SearchType search_type) const override;

    std::optional<double> EstimateSelectivity(
        const std::shared_ptr<LeafPredicate>& predicate) const override;

    Result<std::shared_ptr<FileIndexResult>> VisitFullTextMatch(
        const std::shared_ptr<FullTextMatch>& match, bool needs_scores = false) override;

    Result<std::shared_ptr<ScoredFileIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& search) override;

    Result<std::shared_ptr<ScoredFileIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& search) override;

 private:
    const MockSearchFileIndex::ScoredRows& GetRows(const std::string& query) const;

    Result<std::shared_ptr<FileIndexResult>> MakeResult(const std::map<int32_t, float>& selected,
                                                        bool needs_scores) const;

    Result<std::shared_ptr<ScoredFileIndexResult>> MakeScoredResult(
        const std::map<int32_t, float>& selected) const;

    std::string field_name_;
    SearchIndexType index_type_;
    MockSearchFileIndex::ScoredRows rows_;
    MockSearchFileIndex::QueryMatches queries_;
    std::shared_ptr<MockSearchFileIndex::Statistics> statistics_;
};

class MockSearchFileIndexer final : public FileIndexer {
 public:
    MockSearchFileIndexer(SearchIndexType index_type,
                          std::shared_ptr<MockSearchFileIndex::Statistics> statistics);

    std::shared_ptr<FileIndexReader> CreateEmptyReader() const override;

    Result<std::shared_ptr<FileIndexReader>> CreateReader(
        ::ArrowSchema* c_schema, int64_t start, int64_t length,
        const std::shared_ptr<InputStream>& input,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<FileIndexWriter>> CreateWriter(
        ::ArrowSchema* c_schema, const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    SearchIndexType index_type_;
    std::shared_ptr<MockSearchFileIndex::Statistics> statistics_;
};

class MockVectorSearchFileIndexFactory final : public FileIndexerFactory {
 public:
    explicit MockVectorSearchFileIndexFactory(
        std::shared_ptr<MockSearchFileIndex::Statistics> statistics = nullptr)
        : statistics_(std::move(statistics)) {}

    const char* Identifier() const override;

    Result<std::unique_ptr<FileIndexer>> Create(
        const std::map<std::string, std::string>& options) const override;

 private:
    std::shared_ptr<MockSearchFileIndex::Statistics> statistics_;
};

class MockFullTextSearchFileIndexFactory final : public FileIndexerFactory {
 public:
    explicit MockFullTextSearchFileIndexFactory(
        std::shared_ptr<MockSearchFileIndex::Statistics> statistics = nullptr)
        : statistics_(std::move(statistics)) {}

    const char* Identifier() const override;

    Result<std::unique_ptr<FileIndexer>> Create(
        const std::map<std::string, std::string>& options) const override;

 private:
    std::shared_ptr<MockSearchFileIndex::Statistics> statistics_;
};

/// Reuse the mock search backend while requiring the exact options supplied to its factory.
class OptionsCheckingMockSearchFileIndexFactory final : public FileIndexerFactory {
 public:
    OptionsCheckingMockSearchFileIndexFactory(
        std::string identifier, SearchIndexType index_type,
        std::map<std::string, std::string> expected_options,
        std::shared_ptr<MockSearchFileIndex::Statistics> statistics = nullptr)
        : identifier_(std::move(identifier)),
          index_type_(index_type),
          expected_options_(std::move(expected_options)),
          statistics_(std::move(statistics)) {}

    const char* Identifier() const override {
        return identifier_.c_str();
    }

    Result<std::unique_ptr<FileIndexer>> Create(
        const std::map<std::string, std::string>& options) const override;

 private:
    std::string identifier_;
    SearchIndexType index_type_;
    std::map<std::string, std::string> expected_options_;
    std::shared_ptr<MockSearchFileIndex::Statistics> statistics_;
};

}  // namespace paimon::test
