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

#include "paimon/core/append/data_evolution_materialize_deletion_compact_task.h"

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

std::shared_ptr<DataFileMeta> CreateFile(const std::string& file_name, int64_t first_row_id,
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

DeletionFile SomeDeletionFile() {
    return DeletionFile("/tmp/index-0", /*offset=*/1, /*length=*/8, /*cardinality=*/2);
}

}  // namespace

TEST(DataEvolutionMaterializeDeletionCompactTaskTest, CreateAcceptsAContiguousRangeWithDeletions) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateFile("a.parquet", /*first_row_id=*/0, /*row_count=*/4),
        CreateFile("b.parquet", /*first_row_id=*/4, /*row_count=*/4)};
    std::vector<std::optional<DeletionFile>> deletion_files = {SomeDeletionFile(), std::nullopt};

    ASSERT_OK_AND_ASSIGN(DataEvolutionMaterializeDeletionCompactTask task,
                         DataEvolutionMaterializeDeletionCompactTask::Create(
                             EmptyPartition(), files, deletion_files));
    ASSERT_EQ(task.CompactBefore().size(), 2);
    ASSERT_EQ(task.DeletionFiles().size(), 2);
    // Nothing is written until DoCompact runs.
    ASSERT_TRUE(task.CompactAfter().empty());
    ASSERT_NE(task.ToString().find("a.parquet"), std::string::npos);
}

TEST(DataEvolutionMaterializeDeletionCompactTaskTest, CreateRejectsMisalignedDeletionFiles) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateFile("a.parquet", /*first_row_id=*/0, /*row_count=*/4)};
    ASSERT_NOK_WITH_MSG(DataEvolutionMaterializeDeletionCompactTask::Create(EmptyPartition(), files,
                                                                            /*deletion_files=*/{}),
                        "one deletion file slot per data file");
}

TEST(DataEvolutionMaterializeDeletionCompactTaskTest, CreateRejectsARangeWithoutDeletions) {
    // Materializing a range that deletes nothing would renumber its row ids for no gain, so the
    // planner must not produce such a task and Create refuses one.
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateFile("a.parquet", /*first_row_id=*/0, /*row_count=*/4)};
    std::vector<std::optional<DeletionFile>> deletion_files = {std::nullopt};
    ASSERT_NOK_WITH_MSG(DataEvolutionMaterializeDeletionCompactTask::Create(EmptyPartition(), files,
                                                                            deletion_files),
                        "at least one file carrying a deletion vector");
}

TEST(DataEvolutionMaterializeDeletionCompactTaskTest, CreateRejectsADiscontiguousRange) {
    // Row ids are reassigned over the whole task, so a hole in the middle would renumber rows
    // that were never read together.
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateFile("a.parquet", /*first_row_id=*/0, /*row_count=*/4),
        CreateFile("b.parquet", /*first_row_id=*/100, /*row_count=*/4)};
    std::vector<std::optional<DeletionFile>> deletion_files = {SomeDeletionFile(), std::nullopt};
    ASSERT_NOK_WITH_MSG(DataEvolutionMaterializeDeletionCompactTask::Create(EmptyPartition(), files,
                                                                            deletion_files),
                        "contiguous row range");
}

TEST(DataEvolutionMaterializeDeletionCompactTaskTest, CreateRejectsDedicatedStorageFiles) {
    // Paimon C++ does not rewrite blob or vector-store files, so their row ids cannot be
    // reassigned in step with the rows they belong to. Refusing is the only safe answer;
    // rewriting the normal file alone would silently break the alignment they are read through.
    std::vector<std::optional<DeletionFile>> deletion_files = {SomeDeletionFile(), std::nullopt};

    std::vector<std::shared_ptr<DataFileMeta>> with_blob = {
        CreateFile("a.parquet", /*first_row_id=*/0, /*row_count=*/4),
        CreateFile("b-f2-0.blob", /*first_row_id=*/0, /*row_count=*/4)};
    ASSERT_NOK_WITH_MSG(DataEvolutionMaterializeDeletionCompactTask::Create(
                            EmptyPartition(), with_blob, deletion_files),
                        "blob file");

    std::vector<std::shared_ptr<DataFileMeta>> with_vector_store = {
        CreateFile("a.parquet", /*first_row_id=*/0, /*row_count=*/4),
        CreateFile("b-f2-0.vector.parquet", /*first_row_id=*/0, /*row_count=*/4)};
    ASSERT_NOK_WITH_MSG(DataEvolutionMaterializeDeletionCompactTask::Create(
                            EmptyPartition(), with_vector_store, deletion_files),
                        "vector-store file");
}

}  // namespace paimon::test
