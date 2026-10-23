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

#include "paimon/core/global_index/live_row_global_index_reader.h"

#include <algorithm>
#include <cassert>
#include <mutex>
#include <utility>

#include "paimon/common/global_index/union_global_index_reader.h"

namespace paimon {
namespace {
/// Returns whether `full_text_search` may rank a row of `range`: it has no include row ids, or one
/// of them is in `range`.
bool MayRankRowsOf(const FullTextSearch& full_text_search, const Range& range) {
    if (!full_text_search.include_row_ids) {
        return true;
    }
    const RoaringBitmap64& include_row_ids = full_text_search.include_row_ids.value();
    auto first = include_row_ids.EqualOrLarger(range.from);
    return first != include_row_ids.End() && *first <= range.to;
}

/// A union of LiveRowGlobalIndexReaders that loads the live rows on the visiting thread before a
/// full-text search fans out to them.
class LiveRowUnionGlobalIndexReader : public UnionGlobalIndexReader {
 public:
    /// @param ranked_ranges The global row ids the shards rank in a full-text search, leaving out
    ///                      the shards that rank none.
    LiveRowUnionGlobalIndexReader(std::vector<std::shared_ptr<GlobalIndexReader>>&& readers,
                                  std::vector<Range>&& row_ranges,
                                  std::vector<std::optional<Range>>&& search_ranges,
                                  std::vector<Range>&& ranked_ranges,
                                  const std::shared_ptr<Executor>& executor,
                                  LiveRowGlobalIndexReader::LiveRowsSupplier live_rows_supplier)
        : UnionGlobalIndexReader(std::move(readers), std::move(row_ranges),
                                 std::move(search_ranges), executor),
          ranked_ranges_(std::move(ranked_ranges)),
          live_rows_supplier_(std::move(live_rows_supplier)) {}

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override {
        // Only a shard search needs live rows: the union rejects a search without a positive limit
        // before searching any shard, and skips the shards whose rows the include row ids miss.
        if (full_text_search && full_text_search->limit > 0 &&
            std::any_of(ranked_ranges_.begin(), ranked_ranges_.end(), [&](const Range& range) {
                return MayRankRowsOf(*full_text_search, range);
            })) {
            PAIMON_RETURN_NOT_OK(live_rows_supplier_());
        }
        return UnionGlobalIndexReader::VisitFullTextSearch(full_text_search);
    }

 private:
    std::vector<Range> ranked_ranges_;
    LiveRowGlobalIndexReader::LiveRowsSupplier live_rows_supplier_;
};
}  // namespace

LiveRowGlobalIndexReader::LiveRowsSupplier LiveRowGlobalIndexReader::CacheLiveRows(
    std::function<Result<std::optional<RoaringBitmap64>>()> load) {
    struct State {
        std::mutex mutex;
        bool loaded = false;
        std::shared_ptr<const RoaringBitmap64> live_rows;
    };
    auto state = std::make_shared<State>();
    return [state, load = std::move(load)]() -> Result<std::shared_ptr<const RoaringBitmap64>> {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (!state->loaded) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap64> live_rows, load());
            if (live_rows) {
                state->live_rows =
                    std::make_shared<const RoaringBitmap64>(std::move(live_rows).value());
            }
            state->loaded = true;
        }
        return state->live_rows;
    };
}

std::shared_ptr<GlobalIndexReader> LiveRowGlobalIndexReader::CreateUnion(
    std::vector<std::shared_ptr<GlobalIndexReader>>&& shard_readers,
    std::vector<Range>&& shard_ranges, std::vector<std::optional<Range>>&& search_ranges,
    const std::shared_ptr<Executor>& executor, const LiveRowsSupplier& live_rows_supplier) {
    assert(shard_readers.size() == shard_ranges.size());
    std::vector<std::shared_ptr<GlobalIndexReader>> readers;
    std::vector<Range> ranked_ranges;
    readers.reserve(shard_readers.size());
    ranked_ranges.reserve(shard_readers.size());
    for (size_t i = 0; i < shard_readers.size(); i++) {
        readers.push_back(std::make_shared<LiveRowGlobalIndexReader>(
            std::move(shard_readers[i]), shard_ranges[i], live_rows_supplier));
        if (search_ranges.empty()) {
            ranked_ranges.push_back(shard_ranges[i]);
        } else if (search_ranges[i]) {
            ranked_ranges.push_back(search_ranges[i].value());
        }
    }
    return std::make_shared<LiveRowUnionGlobalIndexReader>(
        std::move(readers), std::move(shard_ranges), std::move(search_ranges),
        std::move(ranked_ranges), executor, live_rows_supplier);
}

LiveRowGlobalIndexReader::LiveRowGlobalIndexReader(std::shared_ptr<GlobalIndexReader>&& wrapped,
                                                   const Range& shard_range,
                                                   LiveRowsSupplier live_rows_supplier)
    : wrapped_(std::move(wrapped)),
      shard_range_(shard_range),
      live_rows_supplier_(std::move(live_rows_supplier)) {}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitIsNotNull() {
    return wrapped_->VisitIsNotNull();
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitIsNull() {
    return wrapped_->VisitIsNull();
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitEqual(
    const Literal& literal) {
    return wrapped_->VisitEqual(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitNotEqual(
    const Literal& literal) {
    return wrapped_->VisitNotEqual(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitLessThan(
    const Literal& literal) {
    return wrapped_->VisitLessThan(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    return wrapped_->VisitLessOrEqual(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitGreaterThan(
    const Literal& literal) {
    return wrapped_->VisitGreaterThan(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return wrapped_->VisitGreaterOrEqual(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    return wrapped_->VisitIn(literals);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    return wrapped_->VisitNotIn(literals);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitStartsWith(
    const Literal& prefix) {
    return wrapped_->VisitStartsWith(prefix);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitEndsWith(
    const Literal& suffix) {
    return wrapped_->VisitEndsWith(suffix);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitContains(
    const Literal& literal) {
    return wrapped_->VisitContains(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LiveRowGlobalIndexReader::VisitLike(
    const Literal& literal) {
    return wrapped_->VisitLike(literal);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LiveRowGlobalIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    return wrapped_->VisitVectorSearch(vector_search);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LiveRowGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    if (!full_text_search || !MayRankRowsOf(*full_text_search, shard_range_)) {
        return wrapped_->VisitFullTextSearch(full_text_search);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const RoaringBitmap64> live_rows, live_rows_supplier_());
    if (!live_rows) {
        return wrapped_->VisitFullTextSearch(full_text_search);
    }
    RoaringBitmap64 include_row_ids;
    // `to` may be the largest int64 value, so add it apart from the half-open range.
    include_row_ids.AddRange(shard_range_.from, shard_range_.to);
    include_row_ids.Add(shard_range_.to);
    include_row_ids &= *live_rows;
    if (full_text_search->include_row_ids) {
        include_row_ids &= full_text_search->include_row_ids.value();
    }
    if (include_row_ids.Cardinality() == shard_range_.Count()) {
        if (!full_text_search->include_row_ids) {
            return wrapped_->VisitFullTextSearch(full_text_search);
        }
        return wrapped_->VisitFullTextSearch(std::make_shared<FullTextSearch>(
            full_text_search->field_name, full_text_search->query, full_text_search->limit));
    }
    return wrapped_->VisitFullTextSearch(
        std::make_shared<FullTextSearch>(full_text_search->field_name, full_text_search->query,
                                         full_text_search->limit, std::move(include_row_ids)));
}

}  // namespace paimon
