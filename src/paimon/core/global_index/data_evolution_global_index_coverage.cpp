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

#include "paimon/core/global_index/data_evolution_global_index_coverage.h"

#include <map>
#include <optional>
#include <utility>

namespace paimon {

std::vector<Range> DataEvolutionGlobalIndexCoverage::IndexedRanges(
    const std::vector<std::shared_ptr<IndexFileMeta>>& index_files,
    const std::set<int32_t>& field_ids) {
    std::map<int32_t, std::vector<Range>> coverage_by_field;
    for (const auto& file : index_files) {
        const std::optional<GlobalIndexMeta>& meta = file->GetGlobalIndexMeta();
        if (!meta || (meta->extra_field_ids && !meta->extra_field_ids->empty())) {
            continue;
        }
        coverage_by_field[meta->index_field_id].emplace_back(meta->row_range_start,
                                                             meta->row_range_end);
    }

    std::vector<Range> indexed;
    for (int32_t field_id : field_ids) {
        auto iter = coverage_by_field.find(field_id);
        if (iter == coverage_by_field.end()) {
            return {};
        }
        std::vector<Range> field_ranges = Range::SortAndMergeOverlap(iter->second, true);
        indexed = indexed.empty() ? std::move(field_ranges) : Range::And(indexed, field_ranges);
        if (indexed.empty()) {
            break;
        }
    }
    return indexed;
}

Result<std::vector<Range>> DataEvolutionGlobalIndexCoverage::UnindexedRanges(
    GlobalIndexSearchMode mode, std::optional<int64_t> next_row_id,
    const std::vector<Range>& indexed_ranges, const DataRangesSupplier& data_ranges) {
    if (mode == GlobalIndexSearchMode::FAST || !next_row_id || next_row_id.value() <= 0) {
        return std::vector<Range>();
    }
    std::vector<Range> all_data_ranges;
    if (mode == GlobalIndexSearchMode::DETAIL) {
        PAIMON_ASSIGN_OR_RAISE(all_data_ranges, data_ranges());
    } else {
        all_data_ranges.emplace_back(0, next_row_id.value() - 1);
    }
    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex covered, RowRangeIndex::Create(indexed_ranges));
    return ExcludeRanges(Range::SortAndMergeOverlap(all_data_ranges, /*adjacent=*/true), covered);
}

std::vector<Range> DataEvolutionGlobalIndexCoverage::ExcludeRanges(const std::vector<Range>& ranges,
                                                                   const RowRangeIndex& covered) {
    std::vector<Range> uncovered;
    for (const Range& range : ranges) {
        std::vector<Range> remaining =
            range.Exclude(covered.IntersectedRanges(range.from, range.to));
        uncovered.insert(uncovered.end(), remaining.begin(), remaining.end());
    }
    return uncovered;
}

}  // namespace paimon
