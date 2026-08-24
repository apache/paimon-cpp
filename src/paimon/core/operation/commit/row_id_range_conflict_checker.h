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

#include <memory>
#include <utility>
#include <vector>

#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/commit/row_id_conflict_checker.h"
#include "paimon/utils/row_range_index.h"

namespace paimon {

/// Detects a row id conflict by range overlap alone.
///
/// This is the rule a commit needs when it does not merely update columns of the rows it
/// touches but takes those rows away: materializing deletion vectors drops the deleted rows and
/// lets the commit assign fresh row ids to the survivors, so anything another writer added over
/// the old range in the meantime would be left addressing rows that no longer exist there.
/// Which columns either side wrote does not matter, so unlike `RowIdColumnConflictChecker` this
/// one never looks at a schema.
class RowIdRangeConflictChecker : public RowIdConflictChecker {
 public:
    /// Builds a checker over the row id ranges of `delta_files`. Files without a row id hold no
    /// range to conflict with and are ignored.
    static Result<std::shared_ptr<RowIdRangeConflictChecker>> FromDataFiles(
        const std::vector<std::shared_ptr<DataFileMeta>>& delta_files);

    bool IsEmpty() const override {
        return row_range_index_.Ranges().empty();
    }

    Result<bool> ConflictsWith(const std::shared_ptr<DataFileMeta>& file) const override;

 private:
    explicit RowIdRangeConflictChecker(RowRangeIndex row_range_index)
        : row_range_index_(std::move(row_range_index)) {}

    RowRangeIndex row_range_index_;
};

}  // namespace paimon
