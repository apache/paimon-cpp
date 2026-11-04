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
#include <optional>
#include <vector>

#include "paimon/file_index/file_index_result.h"
#include "paimon/file_index/scored_file_index_result.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/function_visitor.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class LeafPredicate;
struct FullTextMatch;
/// Evaluates filter predicates against a file-level index to determine file eligibility.
///
/// `FileIndexReader` implements the `FunctionVisitor` interface specialized to produce
/// `std::shared_ptr<FileIndexResult>` objects. It reads pre-built file-level index data
/// (e.g., bitmap, bsi or bloom filters) from index file and evaluates
/// whether a given data file may contain rows matching a specific predicate.
/// @note Callers of `Visit*` for DECIMAL fields must ensure each literal's scale matches the scale
/// of the indexed data; otherwise, index filtering results may be incorrect.
class PAIMON_EXPORT FileIndexReader : public FunctionVisitor<std::shared_ptr<FileIndexResult>> {
 public:
    enum class SearchType { FULL_TEXT_MATCH, FULL_TEXT_SEARCH, VECTOR_SEARCH };

    ~FileIndexReader() override = default;

    /// Whether this reader can execute the requested search without probing it with a query.
    virtual bool SupportsSearch(SearchType search_type) const;

    /// Estimates the fraction of rows in the data file expected to match the predicate.
    /// Returns a value in [0, 1], or std::nullopt if no estimate is available.
    /// Values outside [0, 1] are also treated as unknown.
    /// The estimate only guides predicate execution order; it is not used to filter rows.
    /// TODO(lisizhuo.lsz): Add selectivity estimates for scalar indexes such as bitmap indexes.
    virtual std::optional<double> EstimateSelectivity(
        const std::shared_ptr<LeafPredicate>& predicate) const;

    Result<std::shared_ptr<FileIndexResult>> VisitIsNotNull() override;

    Result<std::shared_ptr<FileIndexResult>> VisitIsNull() override;

    Result<std::shared_ptr<FileIndexResult>> VisitEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitNotEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitLessThan(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitLessOrEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitGreaterThan(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitGreaterOrEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitIn(const std::vector<Literal>& literals) override;

    Result<std::shared_ptr<FileIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;

    Result<std::shared_ptr<FileIndexResult>> VisitStartsWith(const Literal& prefix) override;

    Result<std::shared_ptr<FileIndexResult>> VisitEndsWith(const Literal& suffix) override;

    Result<std::shared_ptr<FileIndexResult>> VisitContains(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitLike(const Literal& literal) override;

    /// Execute vector search and return scored file-local physical row positions.
    virtual Result<std::shared_ptr<ScoredFileIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search);

    /// Execute full-text TopK search and return scored file-local physical row positions.
    /// Scores are required for merging results across files, regardless of score projection.
    virtual Result<std::shared_ptr<ScoredFileIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search);

    /// Return every matching file-local physical row position, without TopK truncation.
    /// When needs_scores is true, return a ScoredFileIndexResult, including for empty results.
    /// When needs_scores is false, return a BitmapIndexResult, not a ScoredFileIndexResult.
    virtual Result<std::shared_ptr<FileIndexResult>> VisitFullTextMatch(
        const std::shared_ptr<FullTextMatch>& full_text_match, bool needs_scores = false);
};

}  // namespace paimon
