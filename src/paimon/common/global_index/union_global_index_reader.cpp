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

#include "paimon/common/global_index/union_global_index_reader.h"

#include <algorithm>
#include <cassert>
#include <future>
#include <unordered_set>
#include <utility>

#include "paimon/common/executor/future.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
namespace {
std::shared_ptr<ScoredGlobalIndexResult> EmptyScoredResult() {
    return std::make_shared<BitmapScoredGlobalIndexResult>(RoaringBitmap64(), std::vector<float>());
}

RoaringBitmap64 RowIdsOf(const Range& range) {
    // `range.to` may be the largest int64 value, so add it apart from the half-open range.
    RoaringBitmap64 row_ids;
    row_ids.AddRange(range.from, range.to);
    row_ids.Add(range.to);
    return row_ids;
}

/// Returns the row ids of `row_ids` within `range`. Trimming a copy costs time in proportion to
/// the containers of `row_ids`, not to the rows or the span of `range`.
RoaringBitmap64 RowIdsWithin(const RoaringBitmap64& row_ids, const Range& range) {
    RoaringBitmap64 result = row_ids;
    result.RemoveRange(0, range.from);
    if (range.to < RoaringBitmap64::MAX_VALUE) {
        // RemoveRange leaves out its end, so the largest int64 value is removed on its own.
        result.RemoveRange(range.to + 1, RoaringBitmap64::MAX_VALUE);
        result -= RoaringBitmap64::From({RoaringBitmap64::MAX_VALUE});
    }
    return result;
}

/// Returns whether `left` ranks before `right`: a higher score first, then a lower row id. Scores
/// are ordered as -infinity < -0.0 < +0.0 < +infinity < NaN.
bool RanksBefore(const std::pair<int64_t, float>& left, const std::pair<int64_t, float>& right) {
    int32_t score_order = FieldsComparator::CompareFloatingPoint(left.second, right.second);
    if (score_order != 0) {
        return score_order > 0;
    }
    return left.first < right.first;
}

/// Merges scored results and keeps the `limit` rows that rank first. A row id in more than one
/// result keeps its first score.
Result<std::shared_ptr<ScoredGlobalIndexResult>> MergeTopK(
    const std::vector<std::shared_ptr<ScoredGlobalIndexResult>>& results, int32_t limit) {
    std::unordered_set<int64_t> merged_row_ids;
    std::vector<std::pair<int64_t, float>> rows;
    for (const auto& result : results) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScoredGlobalIndexResult::ScoredIterator> iter,
                               result->CreateScoredIterator());
        while (iter->HasNext()) {
            std::pair<int64_t, float> row = iter->NextWithScore();
            if (merged_row_ids.insert(row.first).second) {
                rows.push_back(row);
            }
        }
    }
    auto top_k = static_cast<size_t>(limit);
    if (rows.size() > top_k) {
        std::nth_element(rows.begin(), rows.begin() + top_k, rows.end(), RanksBefore);
        rows.resize(top_k);
    }
    std::sort(rows.begin(), rows.end());
    RoaringBitmap64 bitmap;
    std::vector<float> scores;
    scores.reserve(rows.size());
    for (const auto& [row_id, score] : rows) {
        bitmap.Add(row_id);
        scores.push_back(score);
    }
    return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap), std::move(scores));
}
}  // namespace

UnionGlobalIndexReader::UnionGlobalIndexReader(
    std::vector<std::shared_ptr<GlobalIndexReader>>&& readers, std::vector<Range>&& row_ranges,
    std::vector<std::optional<Range>>&& search_ranges, const std::shared_ptr<Executor>& executor)
    : readers_(std::move(readers)),
      executor_(executor),
      row_ranges_(std::move(row_ranges)),
      search_ranges_(std::move(search_ranges)) {
    assert(row_ranges_.size() == search_ranges_.size());
    assert(search_ranges_.empty() || search_ranges_.size() == readers_.size());
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitIsNotNull() {
    return Union(
        [](const std::shared_ptr<GlobalIndexReader>& reader) { return reader->VisitIsNotNull(); });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitIsNull() {
    return Union(
        [](const std::shared_ptr<GlobalIndexReader>& reader) { return reader->VisitIsNull(); });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitEqual(
    const Literal& literal) {
    return Union([literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitEqual(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitNotEqual(
    const Literal& literal) {
    return Union([literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitNotEqual(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitLessThan(
    const Literal& literal) {
    return Union([literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitLessThan(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    return Union([literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitLessOrEqual(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitGreaterThan(
    const Literal& literal) {
    return Union([literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitGreaterThan(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return Union([literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitGreaterOrEqual(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    return Union([literals](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitIn(literals);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    return Union([literals](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitNotIn(literals);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitStartsWith(
    const Literal& prefix) {
    return Union([prefix](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitStartsWith(prefix);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitEndsWith(
    const Literal& suffix) {
    return Union([suffix](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitEndsWith(suffix);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitContains(
    const Literal& literal) {
    return Union([literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitContains(literal);
    });
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::VisitLike(
    const Literal& literal) {
    return Union([literal](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitLike(literal);
    });
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> UnionGlobalIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    return UnionScored([vector_search](const std::shared_ptr<GlobalIndexReader>& reader) {
        return reader->VisitVectorSearch(vector_search);
    });
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> UnionGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    if (!full_text_search) {
        return Status::Invalid("VisitFullTextSearch: null FullTextSearch pointer");
    }
    if (full_text_search->limit <= 0) {
        return Status::Invalid("full-text search requires a positive limit");
    }
    // If the readers rank disjoint rows, the top `limit` rows of each reader include all of its
    // rows scoring above the `limit`-th highest score of all readers, so merging them loses none
    // of those rows. Rows tied at that score may already have been cut by a reader's engine.
    auto reader_searches = std::make_shared<std::vector<std::shared_ptr<FullTextSearch>>>();
    reader_searches->reserve(readers_.size());
    for (size_t index = 0; index < readers_.size(); ++index) {
        reader_searches->push_back(FullTextSearchForReader(index, full_text_search));
    }
    auto results = ExecuteAllReadersWithIndex<Result<std::shared_ptr<ScoredGlobalIndexResult>>>(
        [reader_searches](size_t index, const std::shared_ptr<GlobalIndexReader>& reader)
            -> Result<std::shared_ptr<ScoredGlobalIndexResult>> {
            const std::shared_ptr<FullTextSearch>& reader_search = (*reader_searches)[index];
            if (!reader_search) {
                return EmptyScoredResult();
            }
            return reader->VisitFullTextSearch(reader_search);
        });
    std::vector<std::shared_ptr<ScoredGlobalIndexResult>> scored_results;
    scored_results.reserve(results.size());
    for (auto& result_or_status : results) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> result,
                               std::move(result_or_status));
        if (result != nullptr) {
            scored_results.push_back(std::move(result));
        }
    }
    // No reader answered the search, which differs from a search that matches no rows.
    if (scored_results.empty()) {
        return std::shared_ptr<ScoredGlobalIndexResult>();
    }
    return MergeTopK(scored_results, full_text_search->limit);
}

std::shared_ptr<FullTextSearch> UnionGlobalIndexReader::FullTextSearchForReader(
    size_t index, const std::shared_ptr<FullTextSearch>& full_text_search) const {
    if (search_ranges_.empty()) {
        return full_text_search;
    }
    const std::optional<Range>& search_range = search_ranges_[index];
    if (!search_range) {
        return nullptr;
    }
    bool ranks_all_rows = search_range.value() == row_ranges_[index];
    if (!full_text_search->include_row_ids) {
        if (ranks_all_rows) {
            return full_text_search;
        }
        return std::make_shared<FullTextSearch>(full_text_search->field_name,
                                                full_text_search->query, full_text_search->limit,
                                                RowIdsOf(search_range.value()));
    }
    const RoaringBitmap64& include_row_ids = full_text_search->include_row_ids.value();
    auto first = include_row_ids.EqualOrLarger(search_range->from);
    if (first == include_row_ids.End() || *first > search_range->to) {
        return nullptr;
    }
    // The reader drops include row ids outside its rows, so they need no clipping here.
    if (ranks_all_rows) {
        return full_text_search;
    }
    return std::make_shared<FullTextSearch>(full_text_search->field_name, full_text_search->query,
                                            full_text_search->limit,
                                            RowIdsWithin(include_row_ids, search_range.value()));
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> UnionGlobalIndexReader::UnionScored(
    ScoredReaderAction action) {
    auto results = ExecuteAllReaders<Result<std::shared_ptr<ScoredGlobalIndexResult>>>(action);

    std::shared_ptr<ScoredGlobalIndexResult> merged_result = nullptr;
    for (auto& result_or_status : results) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> result,
                               std::move(result_or_status));
        if (result == nullptr) {
            continue;
        }
        if (merged_result == nullptr) {
            merged_result = std::move(result);
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> merged_as_base,
                                   merged_result->Or(result));
            merged_result = std::dynamic_pointer_cast<ScoredGlobalIndexResult>(merged_as_base);
            if (!merged_result) {
                return Status::Invalid(
                    "Or of ScoredGlobalIndexResult did not return ScoredGlobalIndexResult in "
                    "UnionGlobalIndexReader");
            }
        }
    }

    return merged_result;
}

Result<std::shared_ptr<GlobalIndexResult>> UnionGlobalIndexReader::Union(ReaderAction action) {
    auto results = ExecuteAllReaders<Result<std::shared_ptr<GlobalIndexResult>>>(action);

    std::shared_ptr<GlobalIndexResult> merged_result = nullptr;
    for (auto& result_or_status : results) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                               std::move(result_or_status));
        if (result == nullptr) {
            return std::shared_ptr<GlobalIndexResult>(nullptr);
        }
        if (merged_result == nullptr) {
            merged_result = std::move(result);
        } else {
            PAIMON_ASSIGN_OR_RAISE(merged_result, merged_result->Or(result));
        }
    }

    return merged_result;
}

bool UnionGlobalIndexReader::IsThreadSafe() const {
    for (const auto& reader : readers_) {
        if (!reader->IsThreadSafe()) {
            return false;
        }
    }
    return true;
}

template <typename R>
std::vector<R> UnionGlobalIndexReader::ExecuteAllReaders(
    const std::function<R(const std::shared_ptr<GlobalIndexReader>&)>& action) {
    return ExecuteAllReadersWithIndex<R>(
        [action](size_t, const std::shared_ptr<GlobalIndexReader>& reader) -> R {
            return action(reader);
        });
}

template <typename R>
std::vector<R> UnionGlobalIndexReader::ExecuteAllReadersWithIndex(
    const std::function<R(size_t, const std::shared_ptr<GlobalIndexReader>&)>& action) {
    if (executor_ == nullptr || readers_.size() == 1) {
        std::vector<R> results;
        results.reserve(readers_.size());
        for (size_t index = 0; index < readers_.size(); ++index) {
            results.push_back(action(index, readers_[index]));
        }
        return results;
    }

    // Parallel: submit all tasks to executor, then collect results in submission order
    std::vector<std::future<R>> futures;
    futures.reserve(readers_.size());
    for (size_t index = 0; index < readers_.size(); ++index) {
        futures.push_back(Via(executor_.get(), [action, index, reader = readers_[index]]() -> R {
            return action(index, reader);
        }));
    }
    return CollectAll(futures);
}

}  // namespace paimon
