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

#include "paimon/core/append/data_evolution_compact_coordinator.h"

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/append/data_evolution_normal_compact_task.h"
#include "paimon/core/core_options.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class DataEvolutionCompactCoordinatorTest : public testing::Test {
 protected:
    static std::shared_ptr<DataFileMeta> NewFile(const std::string& file_name, int64_t file_size,
                                                 int64_t first_row_id, int64_t row_count,
                                                 int64_t min_sequence_number,
                                                 int64_t max_sequence_number) {
        return DataFileMeta::ForAppend(file_name, file_size, row_count, SimpleStats::EmptyStats(),
                                       min_sequence_number, max_sequence_number, /*schema_id=*/0,
                                       FileSource::Append(),
                                       /*value_stats_cols=*/std::nullopt,
                                       /*external_path=*/std::nullopt, first_row_id,
                                       /*write_cols=*/std::nullopt)
            .value();
    }

    static BinaryRow CreateIntRow(int32_t value) {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
        writer.WriteInt(0, value);
        writer.Complete();
        return row;
    }

    static CoreOptions CreateOptions(const std::string& target_file_size,
                                     const std::string& open_file_cost,
                                     const std::string& min_file_num) {
        return CoreOptions::FromMap({{Options::TARGET_FILE_SIZE, target_file_size},
                                     {Options::SOURCE_SPLIT_OPEN_FILE_COST, open_file_cost},
                                     {Options::COMPACTION_MIN_FILE_NUM, min_file_num},
                                     {Options::ROW_TRACKING_ENABLED, "true"},
                                     {Options::DATA_EVOLUTION_ENABLED, "true"}})
            .value();
    }

    static Result<std::vector<DataEvolutionNormalCompactTask>> Plan(
        const std::vector<std::shared_ptr<DataFileMeta>>& files, const CoreOptions& options,
        const BinaryRow& partition = BinaryRow::EmptyRow()) {
        LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>> partition_files;
        partition_files[partition] = files;
        return DataEvolutionCompactCoordinator::PlanCompactTasks(partition_files, options);
    }

    static std::vector<std::string> FileNames(const DataEvolutionNormalCompactTask& task) {
        std::vector<std::string> names;
        names.reserve(task.CompactBefore().size());
        for (const auto& file : task.CompactBefore()) {
            names.push_back(file->file_name);
        }
        return names;
    }
};

TEST_F(DataEvolutionCompactCoordinatorTest, TestSingleFileProducesNoTask) {
    CoreOptions options = CreateOptions("1024", "1", "2");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks,
                         Plan({NewFile("f1", 100, 0, 100, 1, 1)}, options));
    ASSERT_TRUE(tasks.empty());
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestContiguousFilesPackedByTargetSize) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {NewFile("f1", 100, 0, 100, 1, 1),
                                                        NewFile("f2", 100, 100, 100, 2, 2),
                                                        NewFile("f3", 100, 200, 100, 3, 3)};

    // The bin is emitted once its weight strictly exceeds the target: with target 199 the first
    // two files trigger a task and the third stays alone (too few files for its own task).
    CoreOptions options = CreateOptions("199", "1", "2");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks, Plan(files, options));
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"f1", "f2"}));
    EXPECT_EQ(tasks[0].RowRange(), Range(0, 199));

    // With target 200 two files weigh exactly the target, so all three files pack together.
    options = CreateOptions("200", "1", "2");
    ASSERT_OK_AND_ASSIGN(tasks, Plan(files, options));
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"f1", "f2", "f3"}));
    EXPECT_EQ(tasks[0].RowRange(), Range(0, 299));
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestOpenFileCostWeighsSmallFiles) {
    // A bin weighs each file by max(file_size, open-file-cost), so many tiny files still cost
    // an open each and fill a bin far sooner than their bytes suggest.
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        NewFile("f1", 1, 0, 1, 1, 1), NewFile("f2", 1, 1, 1, 2, 2), NewFile("f3", 1, 2, 1, 3, 3)};

    // With an open-file cost of 100 each file weighs 100: the first two exceed the target and
    // form a task, leaving the third below the minimum file count.
    CoreOptions options = CreateOptions("199", "100", "2");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks, Plan(files, options));
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"f1", "f2"}));
    EXPECT_EQ(tasks[0].RowRange(), Range(0, 1));

    // Weighed by their bytes alone the same files never fill the bin and pack together.
    options = CreateOptions("199", "1", "2");
    ASSERT_OK_AND_ASSIGN(tasks, Plan(files, options));
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"f1", "f2", "f3"}));
    EXPECT_EQ(tasks[0].RowRange(), Range(0, 2));
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestRowIdGapCutsBin) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        NewFile("f1", 100, 0, 100, 1, 1), NewFile("f2", 100, 100, 100, 2, 2),
        NewFile("f3", 100, 1000, 100, 3, 3), NewFile("f4", 100, 1100, 100, 4, 4)};
    CoreOptions options = CreateOptions("1024", "1", "2");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks, Plan(files, options));
    ASSERT_EQ(tasks.size(), 2);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"f1", "f2"}));
    EXPECT_EQ(tasks[0].RowRange(), Range(0, 199));
    EXPECT_EQ(FileNames(tasks[1]), (std::vector<std::string>{"f3", "f4"}));
    EXPECT_EQ(tasks[1].RowRange(), Range(1000, 1199));
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestLargeFileSkippedAndCutsBin) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        NewFile("f1", 100, 0, 100, 1, 1), NewFile("f2", 100, 100, 100, 2, 2),
        NewFile("big", 5000, 200, 100, 3, 3), NewFile("f4", 100, 300, 100, 4, 4),
        NewFile("f5", 100, 400, 100, 5, 5)};
    CoreOptions options = CreateOptions("1024", "1", "2");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks, Plan(files, options));
    // The large file exceeds the target on its own: it cuts the bin, is not packed with its
    // neighbors, and alone it stays below the configured minimum file count.
    ASSERT_EQ(tasks.size(), 2);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"f1", "f2"}));
    // The rewritten ranges stop short of the large file's rows and resume after them.
    EXPECT_EQ(tasks[0].RowRange(), Range(0, 199));
    EXPECT_EQ(FileNames(tasks[1]), (std::vector<std::string>{"f4", "f5"}));
    EXPECT_EQ(tasks[1].RowRange(), Range(300, 499));
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestEvolvedFieldGroupsPackTogether) {
    // f1 holds all columns of rows [0, 99], f2 is a newer partial-column file over the exact
    // same rows (one evolved field group), f3 continues the row id run.
    std::vector<std::shared_ptr<DataFileMeta>> files = {NewFile("f1", 100, 0, 100, 1, 1),
                                                        NewFile("f2", 50, 0, 100, 2, 2),
                                                        NewFile("f3", 100, 100, 100, 3, 3)};
    CoreOptions options = CreateOptions("1024", "1", "2");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks, Plan(files, options));
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"f1", "f2", "f3"}));
    EXPECT_EQ(tasks[0].RowRange(), Range(0, 199));
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestHeavyFieldGroupCompactedAlone) {
    // The field group of rows [100, 199] outweighs the target by itself: it still becomes a
    // task (merging versions is worthwhile), but nothing else is packed on top of it.
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        NewFile("f1", 100, 0, 100, 1, 1), NewFile("g1", 600, 100, 100, 2, 2),
        NewFile("g2", 600, 100, 100, 3, 3), NewFile("f4", 100, 200, 100, 4, 4)};
    CoreOptions options = CreateOptions("1024", "1", "2");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks, Plan(files, options));
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"g1", "g2"}));
    EXPECT_EQ(tasks[0].RowRange(), Range(100, 199));
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestMismatchedFieldGroupRangesRejected) {
    // Overlapping files that do not cover the exact same rows cannot form a field group.
    std::vector<std::shared_ptr<DataFileMeta>> files = {NewFile("f1", 100, 0, 100, 1, 1),
                                                        NewFile("f2", 100, 0, 50, 2, 2)};
    CoreOptions options = CreateOptions("1024", "1", "2");
    Result<std::vector<DataEvolutionNormalCompactTask>> result = Plan(files, options);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().ToString().find("same row range") != std::string::npos)
        << result.status().ToString();
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestBlobFilesExcluded) {
    // Blob files share the row range of their data files but are dedicated storage: they must
    // not join a field group or a task.
    std::vector<std::shared_ptr<DataFileMeta>> files = {NewFile("f1", 100, 0, 100, 1, 1),
                                                        NewFile("f2", 50, 0, 100, 2, 2),
                                                        NewFile("f3.blob", 100, 0, 100, 2, 2)};
    CoreOptions options = CreateOptions("1024", "1", "2");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks, Plan(files, options));
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"f1", "f2"}));
    // The excluded blob file shares these rows, so the range must stay the data files' own.
    EXPECT_EQ(tasks[0].RowRange(), Range(0, 99));
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestVectorStoreFilesRejected) {
    // Without a VECTOR schema type the rewrite cannot exclude vector columns the way Java
    // does, so a table holding vector-store files is rejected instead of silently mis-planned.
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        NewFile("f1", 100, 0, 100, 1, 1), NewFile("f2", 50, 0, 100, 2, 2),
        NewFile("f3.vector.lance", 100, 0, 100, 2, 2)};
    CoreOptions options = CreateOptions("1024", "1", "2");
    Result<std::vector<DataEvolutionNormalCompactTask>> result = Plan(files, options);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().ToString().find("vector-store") != std::string::npos)
        << result.status().ToString();
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestPartitionsPlanIndependently) {
    CoreOptions options = CreateOptions("1024", "1", "2");
    LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>> partition_files;
    partition_files[CreateIntRow(0)] = {NewFile("p0-f1", 100, 0, 100, 1, 1),
                                        NewFile("p0-f2", 100, 100, 100, 2, 2)};
    partition_files[CreateIntRow(1)] = {NewFile("p1-f1", 100, 0, 100, 1, 1),
                                        NewFile("p1-f2", 100, 100, 100, 2, 2)};
    ASSERT_OK_AND_ASSIGN(
        std::vector<DataEvolutionNormalCompactTask> tasks,
        DataEvolutionCompactCoordinator::PlanCompactTasks(partition_files, options));
    ASSERT_EQ(tasks.size(), 2);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"p0-f1", "p0-f2"}));
    EXPECT_EQ(FileNames(tasks[1]), (std::vector<std::string>{"p1-f1", "p1-f2"}));
    EXPECT_EQ(tasks[0].Partition(), CreateIntRow(0));
    EXPECT_EQ(tasks[1].Partition(), CreateIntRow(1));
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestMinFileNumOneAllowsSingleFileTask) {
    // Java places no lower bound on a normal task's input size: with compaction.min.file-num=1
    // even a lone file may be rewritten.
    CoreOptions options = CreateOptions("1024", "1", "1");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks,
                         Plan({NewFile("f1", 100, 0, 100, 1, 1)}, options));
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"f1"}));
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestMinFileNumSuppressesSmallBins) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {NewFile("f1", 100, 0, 100, 1, 1),
                                                        NewFile("f2", 100, 100, 100, 2, 2)};
    CoreOptions options = CreateOptions("1024", "1", "5");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks, Plan(files, options));
    ASSERT_TRUE(tasks.empty());
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestTaskCreateRejectsInvalidFileMeta) {
    // Create() is a public entry and validates its input on its own, without relying on the
    // coordinator's planning checks. Each input must fail for its own reason.
    auto expect_task_error = [](const Result<DataEvolutionNormalCompactTask>& result,
                                const std::string& expected) {
        EXPECT_FALSE(result.ok());
        EXPECT_NE(result.status().ToString().find(expected), std::string::npos)
            << result.status().ToString();
    };
    expect_task_error(DataEvolutionNormalCompactTask::Create(BinaryRow::EmptyRow(), {}),
                      "compact files should not be empty");
    expect_task_error(DataEvolutionNormalCompactTask::Create(BinaryRow::EmptyRow(), {nullptr}),
                      "compact files must not be null");
    // Zero row count.
    expect_task_error(
        DataEvolutionNormalCompactTask::Create(
            BinaryRow::EmptyRow(), {NewFile("f1", 100, /*first_row_id=*/0, /*row_count=*/0, 1, 1)}),
        "must form a valid row id range");
    // Negative first row id.
    expect_task_error(DataEvolutionNormalCompactTask::Create(
                          BinaryRow::EmptyRow(),
                          {NewFile("f1", 100, /*first_row_id=*/-5, /*row_count=*/100, 1, 1)}),
                      "must form a valid row id range");
    // A row range overflowing int64.
    expect_task_error(
        DataEvolutionNormalCompactTask::Create(
            BinaryRow::EmptyRow(),
            {NewFile("f1", 100, /*first_row_id=*/std::numeric_limits<int64_t>::max() - 1,
                     /*row_count=*/100, 1, 1)}),
        "must form a valid row id range");
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestTaskCreateRejectsDisjointRowRanges) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {NewFile("f1", 100, 0, 100, 1, 1),
                                                        NewFile("f2", 100, 1000, 100, 2, 2)};
    Result<DataEvolutionNormalCompactTask> task =
        DataEvolutionNormalCompactTask::Create(BinaryRow::EmptyRow(), files);
    ASSERT_FALSE(task.ok());
    EXPECT_TRUE(task.status().ToString().find("contiguous row range") != std::string::npos)
        << task.status().ToString();
}

TEST_F(DataEvolutionCompactCoordinatorTest, TestPlanRejectsInvalidFileMeta) {
    CoreOptions options = CreateOptions("1024", "1", "2");
    auto expect_plan_error = [](const Result<std::vector<DataEvolutionNormalCompactTask>>& result,
                                const std::string& expected) {
        EXPECT_FALSE(result.ok());
        EXPECT_NE(result.status().ToString().find(expected), std::string::npos)
            << result.status().ToString();
    };
    // A null file must fail the plan instead of crashing it.
    expect_plan_error(Plan({nullptr}, options), "compaction files must not be null");
    // A zero row count cannot form a valid row id range.
    expect_plan_error(
        Plan({NewFile("f1", 100, /*first_row_id=*/0, /*row_count=*/0, 1, 1)}, options),
        "must form a valid file and row id range");
    // A negative file size cannot weigh a bin.
    expect_plan_error(
        Plan({NewFile("f1", -1, /*first_row_id=*/0, /*row_count=*/100, 1, 1)}, options),
        "must form a valid file and row id range");
    // A row range overflowing int64 is rejected before any packing arithmetic runs on it.
    expect_plan_error(
        Plan({NewFile("f1", 100, /*first_row_id=*/std::numeric_limits<int64_t>::max() - 1,
                      /*row_count=*/100, 1, 1)},
             options),
        "must form a valid file and row id range");
    // A file without a first row id cannot take part in data evolution planning.
    auto file_without_row_id =
        DataFileMeta::ForAppend("f1", 100, 100, SimpleStats::EmptyStats(), 1, 1, /*schema_id=*/0,
                                FileSource::Append(), /*value_stats_cols=*/std::nullopt,
                                /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                                /*write_cols=*/std::nullopt)
            .value();
    expect_plan_error(Plan({file_without_row_id, NewFile("f2", 100, 100, 100, 2, 2)}, options),
                      "First row id of f1 should not be null");
}

}  // namespace paimon::test
