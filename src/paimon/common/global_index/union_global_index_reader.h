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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/executor.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/utils/range.h"

namespace paimon {
/// A GlobalIndexReader that combines results from multiple readers by performing a union
/// operation on their results.
///
/// When an executor is provided, all sub-reader actions are submitted in parallel and results are
/// collected in submission order. Otherwise, sub-readers are evaluated sequentially.
class UnionGlobalIndexReader : public GlobalIndexReader {
 public:
    /// Constructs a UnionGlobalIndexReader.
    /// @param row_ranges    The global row ids covered by each reader, in the order of `readers`.
    /// @param search_ranges The global row ids each reader ranks in a full-text search, within its
    ///                      row range, in the order of `readers`; std::nullopt if it ranks none.
    ///                      If both are empty, each reader ranks all of its rows.
    UnionGlobalIndexReader(std::vector<std::shared_ptr<GlobalIndexReader>>&& readers,
                           std::vector<Range>&& row_ranges,
                           std::vector<std::optional<Range>>&& search_ranges,
                           const std::shared_ptr<Executor>& executor);

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override;

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override;

    /// Merges the scored results of all readers and keeps the `full_text_search->limit` rows with
    /// the highest scores among them, breaking ties by ascending row id. A row id returned by more
    /// than one reader keeps the score from the first of those readers. Returns nullptr if no
    /// reader returns a result, e.g. if the readers do not support full-text search; a reader that
    /// ranks none of the rows to search is not visited and counts as returning an empty result.
    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override;

    bool IsThreadSafe() const override;

    std::string GetIndexType() const override {
        return "union";
    }

 private:
    using ReaderAction = std::function<Result<std::shared_ptr<GlobalIndexResult>>(
        const std::shared_ptr<GlobalIndexReader>&)>;

    using ScoredReaderAction = std::function<Result<std::shared_ptr<ScoredGlobalIndexResult>>(
        const std::shared_ptr<GlobalIndexReader>&)>;

    /// Executes the given action on all readers and merges results with Union. Returns nullptr if
    /// any reader does not support the action.
    Result<std::shared_ptr<GlobalIndexResult>> Union(ReaderAction action);

    /// Executes the given action on all readers and merges the scored results with Union.
    Result<std::shared_ptr<ScoredGlobalIndexResult>> UnionScored(ScoredReaderAction action);

    /// Returns the full-text search to run on the reader at `index`, or nullptr if the reader
    /// ranks none of the rows to search.
    std::shared_ptr<FullTextSearch> FullTextSearchForReader(
        size_t index, const std::shared_ptr<FullTextSearch>& full_text_search) const;

    /// Executes the given action on all readers (parallel or sequential) and collects results.
    template <typename R>
    std::vector<R> ExecuteAllReaders(
        const std::function<R(const std::shared_ptr<GlobalIndexReader>&)>& action);

    /// Executes the given action on each reader and its index (parallel or sequential) and
    /// collects results in reader order.
    template <typename R>
    std::vector<R> ExecuteAllReadersWithIndex(
        const std::function<R(size_t, const std::shared_ptr<GlobalIndexReader>&)>& action);

    std::vector<std::shared_ptr<GlobalIndexReader>> readers_;
    std::shared_ptr<Executor> executor_;
    std::vector<Range> row_ranges_;
    std::vector<std::optional<Range>> search_ranges_;
};

}  // namespace paimon
