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
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

/// A GlobalIndexReader over one index shard that ranks only live rows in full-text searches, so
/// rows deleted by deletion vectors do not take top-k slots. As Java
/// `DataEvolutionFullTextRead#includeRowIds` does, the shard is searched with the live rows of its
/// range, intersected with the include row ids if given, while BM25 statistics still come from the
/// whole index. Predicate visits are delegated unchanged, as the read drops deleted rows from their
/// untruncated results, and so are vector searches.
class LiveRowGlobalIndexReader : public GlobalIndexReader {
 public:
    /// Supplies the global row ids of live rows, or null when every row is live.
    using LiveRowsSupplier = std::function<Result<std::shared_ptr<const RoaringBitmap64>>()>;

    /// Returns a supplier that calls `load` on its first call and shares the live rows it returns
    /// with later calls, so they are read at most once. A failed load is retried on the next call.
    static LiveRowsSupplier CacheLiveRows(
        std::function<Result<std::optional<RoaringBitmap64>>()> load);

    /// Creates a union of the shard readers of one index whose full-text searches rank only live
    /// rows. Each shard reader is wrapped in a LiveRowGlobalIndexReader. When a search may rank
    /// rows of a shard, live rows are loaded on the visiting thread before it fans out to the
    /// shards, so a load that reads manifests on `executor` never waits for it from one of its own
    /// threads.
    /// @param shard_readers      Readers of the shards. They take and return global row ids.
    /// @param shard_ranges       The global row id range (both ends inclusive) of each shard.
    /// @param search_ranges      The global row ids each shard ranks in a full-text search, as
    ///                           passed to UnionGlobalIndexReader.
    /// @param executor           Executor to search the shards in parallel, or null to search
    ///                           them one after another.
    /// @param live_rows_supplier Supplies the live rows to full-text searches.
    static std::shared_ptr<GlobalIndexReader> CreateUnion(
        std::vector<std::shared_ptr<GlobalIndexReader>>&& shard_readers,
        std::vector<Range>&& shard_ranges, std::vector<std::optional<Range>>&& search_ranges,
        const std::shared_ptr<Executor>& executor, const LiveRowsSupplier& live_rows_supplier);

    /// Constructs a LiveRowGlobalIndexReader.
    /// @param wrapped            The reader of the shard. It takes and returns global row ids.
    /// @param shard_range        The global row id range (both ends inclusive) of the shard.
    /// @param live_rows_supplier Supplies the live rows to full-text searches.
    LiveRowGlobalIndexReader(std::shared_ptr<GlobalIndexReader>&& wrapped, const Range& shard_range,
                             LiveRowsSupplier live_rows_supplier);

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

    /// Searches only the live rows of the shard that are in the include row ids, if given. The
    /// search is delegated unchanged when none of its include row ids is in the shard, or when it
    /// has none and the whole shard is live.
    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override;

    bool IsThreadSafe() const override {
        return wrapped_->IsThreadSafe();
    }

    std::string GetIndexType() const override {
        return wrapped_->GetIndexType();
    }

 private:
    std::shared_ptr<GlobalIndexReader> wrapped_;
    Range shard_range_;
    LiveRowsSupplier live_rows_supplier_;
};

}  // namespace paimon
