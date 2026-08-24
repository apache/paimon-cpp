/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/utils/data_evolution_utils.h"

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
std::shared_ptr<DataFileMeta> CreateFile(const std::string& file_name,
                                         int64_t max_sequence_number) {
    return DataFileMeta::ForAppend(file_name, /*file_size=*/1, /*row_count=*/1,
                                   /*row_stats=*/SimpleStats::EmptyStats(),
                                   /*min_sequence_number=*/0, max_sequence_number,
                                   /*schema_id=*/0, FileSource::Append(),
                                   /*value_stats_cols=*/std::nullopt,
                                   /*external_path=*/std::nullopt, /*first_row_id=*/0,
                                   /*write_cols=*/std::nullopt)
        .value();
}

std::shared_ptr<DataFileMeta> CreateRangeFile(const std::string& file_name,
                                              const std::optional<int64_t>& first_row_id,
                                              int64_t row_count) {
    return DataFileMeta::ForAppend(file_name, /*file_size=*/1, row_count,
                                   /*row_stats=*/SimpleStats::EmptyStats(),
                                   /*min_sequence_number=*/0, /*max_sequence_number=*/1,
                                   /*schema_id=*/0, FileSource::Append(),
                                   /*value_stats_cols=*/std::nullopt,
                                   /*external_path=*/std::nullopt, first_row_id,
                                   /*write_cols=*/std::nullopt)
        .value();
}
}  // namespace

TEST(DataEvolutionUtilsTest, TestRetrieveAnchorFileSkipsBlobAndVectorStoreFiles) {
    auto blob_file = CreateFile("blob-file.blob", 1);
    auto vector_store_file = CreateFile("vector-store.vector.parquet", 2);
    auto oldest_normal_file = CreateFile("oldest-normal.parquet", 3);
    auto newest_normal_file = CreateFile("newest-normal.parquet", 4);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> anchor,
        DataEvolutionUtils::RetrieveAnchorFile(
            {blob_file, vector_store_file, newest_normal_file, oldest_normal_file}));
    ASSERT_EQ(anchor, oldest_normal_file);

    // the same rule a caller applies to tell whether a group can be anchored at all
    ASSERT_TRUE(DataEvolutionUtils::IsNormalFile(oldest_normal_file->file_name));
    ASSERT_FALSE(DataEvolutionUtils::IsNormalFile(blob_file->file_name));
    ASSERT_FALSE(DataEvolutionUtils::IsNormalFile(vector_store_file->file_name));
}

TEST(DataEvolutionUtilsTest, TestRetrieveAnchorFileFailsWithoutNormalFile) {
    auto blob_file_1 = CreateFile("blob-file-1.blob", 1);
    auto blob_file_2 = CreateFile("blob-file-2.blob", 2);
    auto vector_store_file = CreateFile("vector-store.vector.parquet", 3);

    ASSERT_NOK_WITH_MSG(
        DataEvolutionUtils::RetrieveAnchorFile({blob_file_1, blob_file_2, vector_store_file}),
        "should have a normal anchor file in each row range group");
    ASSERT_NOK_WITH_MSG(DataEvolutionUtils::RetrieveAnchorFile({}),
                        "should have a normal anchor file in each row range group");
}

TEST(DataEvolutionUtilsTest, TestRetrieveAnchorFileTieBreaksWithFileName) {
    auto larger_file_name = CreateFile("normal-2.parquet", 1);
    auto smaller_file_name = CreateFile("normal-1.parquet", 1);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> anchor,
        DataEvolutionUtils::RetrieveAnchorFile({larger_file_name, smaller_file_name}));
    ASSERT_EQ(anchor, smaller_file_name);
}

TEST(DataEvolutionUtilsTest, TestCheckContiguousRowRangeMergesOverlappingAndAdjacentFiles) {
    // The shape a compact task hands over: two evolved field groups covering the same rows,
    // followed by the group after them. Overlapping and adjacent ranges both merge, and the
    // result is the range the rewritten file has to end up covering.
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateRangeFile("group-a-full.parquet", /*first_row_id=*/0, /*row_count=*/4),
        CreateRangeFile("group-a-f2.parquet", /*first_row_id=*/0, /*row_count=*/4),
        CreateRangeFile("group-b.parquet", /*first_row_id=*/4, /*row_count=*/2)};

    ASSERT_OK_AND_ASSIGN(Range range, DataEvolutionUtils::CheckContiguousRowRange(files));
    ASSERT_EQ(range.from, 0);
    ASSERT_EQ(range.to, 5);

    // A single file is contiguous by itself.
    ASSERT_OK_AND_ASSIGN(Range single, DataEvolutionUtils::CheckContiguousRowRange(
                                           {CreateRangeFile("only.parquet", 7, 3)}));
    ASSERT_EQ(single.from, 7);
    ASSERT_EQ(single.to, 9);
}

TEST(DataEvolutionUtilsTest, TestCheckContiguousRowRangeRejectsAGap) {
    // A hole between the inputs would leave the rows in the hole claimed by a file that does
    // not hold them, so the merged ranges must collapse into exactly one.
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateRangeFile("a.parquet", /*first_row_id=*/0, /*row_count=*/2),
        CreateRangeFile("b.parquet", /*first_row_id=*/100, /*row_count=*/2)};
    ASSERT_NOK_WITH_MSG(DataEvolutionUtils::CheckContiguousRowRange(files),
                        "should have a contiguous row range");
}

TEST(DataEvolutionUtilsTest, TestCheckContiguousRowRangeRejectsInvalidFileMeta) {
    // A public entry validates its own input; each case must fail for its own reason.
    ASSERT_NOK_WITH_MSG(DataEvolutionUtils::CheckContiguousRowRange({}),
                        "compact files should not be empty");
    ASSERT_NOK_WITH_MSG(DataEvolutionUtils::CheckContiguousRowRange({nullptr}),
                        "compact files must not be null");
    // A file without a row id has no range to preserve.
    ASSERT_NOK_WITH_MSG(DataEvolutionUtils::CheckContiguousRowRange(
                            {CreateRangeFile("f1.parquet", /*first_row_id=*/std::nullopt,
                                             /*row_count=*/2)}),
                        "First row id of f1.parquet should not be null");
    // Zero row count.
    ASSERT_NOK_WITH_MSG(DataEvolutionUtils::CheckContiguousRowRange(
                            {CreateRangeFile("f1.parquet", /*first_row_id=*/0, /*row_count=*/0)}),
                        "must form a valid row id range");
    // Negative first row id.
    ASSERT_NOK_WITH_MSG(DataEvolutionUtils::CheckContiguousRowRange({CreateRangeFile(
                            "f1.parquet", /*first_row_id=*/-5, /*row_count=*/100)}),
                        "must form a valid row id range");
    // A row range overflowing int64.
    ASSERT_NOK_WITH_MSG(DataEvolutionUtils::CheckContiguousRowRange({CreateRangeFile(
                            "f1.parquet", /*first_row_id=*/std::numeric_limits<int64_t>::max() - 1,
                            /*row_count=*/100)}),
                        "must form a valid row id range");
}

TEST(DataEvolutionUtilsTest, TestIsMaterializedCompaction) {
    std::shared_ptr<DataFileMeta> normal_before =
        CreateRangeFile("before.parquet", /*first_row_id=*/0, /*row_count=*/2);
    std::shared_ptr<DataFileMeta> blob_before =
        CreateRangeFile("before.blob", /*first_row_id=*/0, /*row_count=*/2);
    std::shared_ptr<DataFileMeta> keeps_row_id =
        CreateRangeFile("after.parquet", /*first_row_id=*/0, /*row_count=*/2);
    std::shared_ptr<DataFileMeta> renumbered =
        CreateRangeFile("after.parquet", /*first_row_id=*/std::nullopt, /*row_count=*/2);

    // Outputs written without a row id: the commit assigns fresh ones, which is materializing.
    ASSERT_TRUE(DataEvolutionUtils::IsMaterializedCompaction({normal_before}, {renumbered}));
    // A range whose rows were all deleted rewrites to nothing at all, and still counts.
    ASSERT_TRUE(DataEvolutionUtils::IsMaterializedCompaction({normal_before}, {}));
    // A normal compaction keeps the row ids of what it replaced.
    ASSERT_FALSE(DataEvolutionUtils::IsMaterializedCompaction({normal_before}, {keeps_row_id}));
    // One output keeping its row id is enough not to be a materialized rewrite.
    ASSERT_FALSE(
        DataEvolutionUtils::IsMaterializedCompaction({normal_before}, {renumbered, keeps_row_id}));
    // Replacing no normal file at all is neither: an index-only or dedicated-file-only message.
    ASSERT_FALSE(DataEvolutionUtils::IsMaterializedCompaction({}, {renumbered}));
    ASSERT_FALSE(DataEvolutionUtils::IsMaterializedCompaction({blob_before}, {renumbered}));

    ASSERT_TRUE(DataEvolutionUtils::HasNormalFile({blob_before, normal_before}));
    ASSERT_FALSE(DataEvolutionUtils::HasNormalFile({blob_before}));
    ASSERT_FALSE(DataEvolutionUtils::HasNormalFile({}));
}

}  // namespace paimon::test
