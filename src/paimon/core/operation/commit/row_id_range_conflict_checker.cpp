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

#include "paimon/core/operation/commit/row_id_range_conflict_checker.h"

#include <memory>
#include <utility>
#include <vector>

#include "paimon/utils/range.h"

namespace paimon {

Result<std::shared_ptr<RowIdRangeConflictChecker>> RowIdRangeConflictChecker::FromDataFiles(
    const std::vector<std::shared_ptr<DataFileMeta>>& delta_files) {
    std::vector<Range> ranges;
    ranges.reserve(delta_files.size());
    for (const std::shared_ptr<DataFileMeta>& file : delta_files) {
        if (!file || !file->first_row_id || file->row_count <= 0) {
            continue;
        }
        int64_t range_from = file->first_row_id.value();
        ranges.emplace_back(range_from, range_from + file->row_count - 1);
    }
    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex row_range_index, RowRangeIndex::Create(ranges));
    return std::shared_ptr<RowIdRangeConflictChecker>(
        new RowIdRangeConflictChecker(std::move(row_range_index)));
}

Result<bool> RowIdRangeConflictChecker::ConflictsWith(
    const std::shared_ptr<DataFileMeta>& file) const {
    if (!file || !file->first_row_id || file->row_count <= 0) {
        return false;
    }
    int64_t range_from = file->first_row_id.value();
    return row_range_index_.Intersects(range_from, range_from + file->row_count - 1);
}

}  // namespace paimon
