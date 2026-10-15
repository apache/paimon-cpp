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
#include <future>
#include <unordered_set>
#include <utility>

#include "paimon/common/executor/future.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
namespace {
/// Returns whether `left` ranks before `right` as in Java `ScoredGlobalIndexResult#topK`: a higher
/// score first, then a lower row id. Scores are compared as Java `Double#compare` does.
bool RanksBefore(const std::pair<int64_t, float>& left, const std::pair<int64_t, float>& right) {
    int32_t score_order = FieldsComparator::CompareFloatingPoint(left.second, right.second);
    if (score_order != 0) {
        return score_order > 0;
    }
    return left.first < right.first;
}

/// Merges scored results and keeps the `limit` rows that rank first, as Java
/// `ScoredGlobalIndexResult#merge` followed by `#topK` does. A row id in more than one result keeps
/// its first score.
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
    std::vector<std::shared_ptr<GlobalIndexReader>>&& readers,
    const std::shared_ptr<Executor>& executor)
    : readers_(std::move(readers)), executor_(executor) {}

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
    ScoredReaderAction action =
        [full_text_search](const std::shared_ptr<GlobalIndexReader>& reader) {
            return reader->VisitFullTextSearch(full_text_search);
        };
    auto results = ExecuteAllReaders<Result<std::shared_ptr<ScoredGlobalIndexResult>>>(action);
    std::vector<std::shared_ptr<ScoredGlobalIndexResult>> scored_results;
    scored_results.reserve(results.size());
    for (auto& result_or_status : results) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> result,
                               std::move(result_or_status));
        if (result != nullptr) {
            scored_results.push_back(std::move(result));
        }
    }
    if (scored_results.empty()) {
        return std::shared_ptr<ScoredGlobalIndexResult>();
    }
    return MergeTopK(scored_results, full_text_search->limit);
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
    if (executor_ == nullptr || readers_.size() == 1) {
        std::vector<R> results;
        results.reserve(readers_.size());
        for (const auto& reader : readers_) {
            results.push_back(action(reader));
        }
        return results;
    }

    // Parallel: submit all tasks to executor, then collect results in submission order
    std::vector<std::future<R>> futures;
    futures.reserve(readers_.size());
    for (const auto& reader : readers_) {
        futures.push_back(Via(executor_.get(), [action, reader]() -> R { return action(reader); }));
    }
    return CollectAll(futures);
}

}  // namespace paimon
