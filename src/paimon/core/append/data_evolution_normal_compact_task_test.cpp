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

#include "paimon/core/append/data_evolution_normal_compact_task.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
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

BinaryRow EmptyPartition() {
    BinaryRow row(0);
    BinaryRowWriter writer(&row, 8, GetDefaultPool().get());
    writer.Complete();
    return row;
}

}  // namespace

TEST(DataEvolutionNormalCompactTaskTest, CreateCoversTheUnionOfItsInputs) {
    // Two evolved field groups over the same rows plus the group after them: one contiguous run,
    // and the task's range is what the rewritten file has to end up covering exactly.
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateFile("group-a-full.parquet", /*first_row_id=*/0, /*row_count=*/4),
        CreateFile("group-a-f2.parquet", /*first_row_id=*/0, /*row_count=*/4),
        CreateFile("group-b.parquet", /*first_row_id=*/4, /*row_count=*/2)};

    ASSERT_OK_AND_ASSIGN(DataEvolutionNormalCompactTask task,
                         DataEvolutionNormalCompactTask::Create(EmptyPartition(), files));
    ASSERT_EQ(task.RowRange().from, 0);
    ASSERT_EQ(task.RowRange().to, 5);
    ASSERT_EQ(task.CompactBefore().size(), 3);
    ASSERT_TRUE(task.CompactAfter().empty());
    ASSERT_NE(task.ToString().find("group-a-full.parquet"), std::string::npos);
}

TEST(DataEvolutionNormalCompactTaskTest, CreateRejectsADiscontiguousRun) {
    // The rewritten file covers one range, so a hole between the inputs would leave the rows in
    // the hole claimed by a file that does not hold them.
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateFile("a.parquet", /*first_row_id=*/0, /*row_count=*/2),
        CreateFile("b.parquet", /*first_row_id=*/100, /*row_count=*/2)};
    ASSERT_NOK_WITH_MSG(DataEvolutionNormalCompactTask::Create(EmptyPartition(), files),
                        "contiguous row range");
}

TEST(DataEvolutionNormalCompactTaskTest, CreateRejectsUnusableInputs) {
    // Create() validates before it touches its input, so the two shapes that would otherwise
    // fault it - no file at all and a null file - come back as errors. The rest of the row id
    // range matrix belongs to `data_evolution_utils_test.cpp`, which owns the shared rule this
    // delegates to; `CreateRejectsADiscontiguousRun` above is what proves the delegation.
    ASSERT_NOK_WITH_MSG(DataEvolutionNormalCompactTask::Create(EmptyPartition(), /*files=*/{}),
                        "compact files should not be empty");
    ASSERT_NOK_WITH_MSG(DataEvolutionNormalCompactTask::Create(EmptyPartition(), {nullptr}),
                        "compact files must not be null");
}

}  // namespace paimon::test
