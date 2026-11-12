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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <vector>

#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/options/global_index_search_mode.h"
#include "paimon/result.h"
#include "paimon/utils/range.h"
#include "paimon/utils/row_range_index.h"

namespace paimon {
class DataEvolutionGlobalIndexCoverage {
 public:
    using DataRangesSupplier = std::function<Result<std::vector<Range>>()>;

    DataEvolutionGlobalIndexCoverage() = delete;
    ~DataEvolutionGlobalIndexCoverage() = delete;

    /// Intersects single-field index coverage across the requested fields. Multi-field definitions
    /// do not contribute coverage. An empty field set or an uncovered field yields no ranges.
    static std::vector<Range> IndexedRanges(
        const std::vector<std::shared_ptr<IndexFileMeta>>& index_files,
        const std::set<int32_t>& field_ids);

    /// Subtracts covered rows from sorted, disjoint ranges.
    static std::vector<Range> ExcludeRanges(const std::vector<Range>& ranges,
                                            const RowRangeIndex& covered);

    /// Returns sorted, disjoint gaps below the snapshot's next row id in FULL mode or within
    /// the supplied data file ranges in DETAIL mode. Only DETAIL loads the data ranges.
    /// FAST and snapshots without a positive next row id yield no gaps.
    static Result<std::vector<Range>> UnindexedRanges(GlobalIndexSearchMode mode,
                                                      std::optional<int64_t> next_row_id,
                                                      const std::vector<Range>& indexed_ranges,
                                                      const DataRangesSupplier& data_ranges);
};

}  // namespace paimon
