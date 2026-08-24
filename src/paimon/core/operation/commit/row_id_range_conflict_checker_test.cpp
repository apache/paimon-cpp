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
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

std::shared_ptr<DataFileMeta> CreateFile(const std::string& file_name,
                                         const std::optional<int64_t>& first_row_id,
                                         int64_t row_count) {
    return std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/1024, row_count, DataFileMeta::EmptyMinKey(),
        DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_seq_no=*/1, /*max_seq_no=*/2, /*schema_id=*/0, /*level=*/0,
        /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(0, 0), /*delete_row_count=*/std::nullopt,
        /*embedded_index=*/nullptr, /*file_source=*/std::nullopt, /*external_path=*/std::nullopt,
        /*value_stats_cols=*/std::nullopt, first_row_id, /*write_cols=*/std::nullopt);
}

}  // namespace

TEST(RowIdRangeConflictCheckerTest, ConflictsWithAnyOverlappingRange) {
    // The commit takes rows [0, 3] and [10, 11] away, so anything written over those rows
    // conflicts, whatever columns it wrote - which is what tells this rule apart from the
    // column-overlap one.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RowIdRangeConflictChecker> checker,
                         RowIdRangeConflictChecker::FromDataFiles(
                             {CreateFile("dropped-0.parquet", /*first_row_id=*/0, /*row_count=*/4),
                              CreateFile("dropped-1.parquet", /*first_row_id=*/10,
                                         /*row_count=*/2)}));
    ASSERT_FALSE(checker->IsEmpty());

    // Fully inside, partially overlapping and spanning a taken range all conflict.
    for (const auto& conflicting :
         {CreateFile("inside.parquet", /*first_row_id=*/1, /*row_count=*/2),
          CreateFile("straddling.parquet", /*first_row_id=*/3, /*row_count=*/4),
          CreateFile("spanning.parquet", /*first_row_id=*/0, /*row_count=*/12)}) {
        ASSERT_OK_AND_ASSIGN(bool conflicts, checker->ConflictsWith(conflicting));
        ASSERT_TRUE(conflicts) << conflicting->file_name;
    }

    // The gap between the two ranges, and rows past the last one, are untouched.
    for (const auto& disjoint :
         {CreateFile("gap.parquet", /*first_row_id=*/4, /*row_count=*/6),
          CreateFile("beyond.parquet", /*first_row_id=*/12, /*row_count=*/4)}) {
        ASSERT_OK_AND_ASSIGN(bool conflicts, checker->ConflictsWith(disjoint));
        ASSERT_FALSE(conflicts) << disjoint->file_name;
    }

    // A file carrying no row id claims no rows of its own.
    ASSERT_OK_AND_ASSIGN(bool conflicts,
                         checker->ConflictsWith(CreateFile("fresh.parquet",
                                                           /*first_row_id=*/std::nullopt,
                                                           /*row_count=*/4)));
    ASSERT_FALSE(conflicts);
}

TEST(RowIdRangeConflictCheckerTest, IsEmptyWithoutRowIdRanges) {
    // A commit that takes no existing rows away has nothing to conflict with, and the caller
    // skips replaying the snapshots since the checked one entirely.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RowIdRangeConflictChecker> empty,
                         RowIdRangeConflictChecker::FromDataFiles({}));
    ASSERT_TRUE(empty->IsEmpty());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RowIdRangeConflictChecker> without_row_ids,
                         RowIdRangeConflictChecker::FromDataFiles({CreateFile(
                             "fresh.parquet", /*first_row_id=*/std::nullopt, /*row_count=*/4)}));
    ASSERT_TRUE(without_row_ids->IsEmpty());
}

}  // namespace paimon::test
