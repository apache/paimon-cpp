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

#include "paimon/core/append/data_evolution_compact_planner.h"

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/append/data_evolution_normal_compact_task.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/logging.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"

namespace paimon::test {

/// The `(path, size)` pair `ManifestList::Write` returns. Named so it can be spelled without a
/// comma inside a `PAIMON_ASSIGN_OR_RAISE` argument.
using ManifestListFile = std::pair<std::string, int64_t>;

class DataEvolutionCompactPlannerTest : public testing::Test {
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

    static ManifestFileMeta NewManifest(const std::string& file_name, int64_t num_added_files,
                                        int64_t num_deleted_files,
                                        const std::optional<int64_t>& min_row_id,
                                        const std::optional<int64_t>& max_row_id) {
        return ManifestFileMeta(file_name, /*file_size=*/1024, num_added_files, num_deleted_files,
                                SimpleStats::EmptyStats(), /*schema_id=*/0,
                                /*min_bucket=*/std::nullopt, /*max_bucket=*/std::nullopt,
                                /*min_level=*/std::nullopt, /*max_level=*/std::nullopt, min_row_id,
                                max_row_id);
    }

    /// Writes `metas` as the manifest lists of one snapshot and plans the row id windows over
    /// it. The metas are split across the base and the delta list, because a snapshot always
    /// has both and the planner reads them together: which list a manifest sits in must not
    /// change the windows. Requires at least two metas so neither list is written empty.
    static Result<std::vector<Range>> PlanWindows(const std::vector<ManifestFileMeta>& metas,
                                                  int64_t candidate_files_per_round) {
        EXPECT_GE(metas.size(), 2u);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        EXPECT_TRUE(dir);
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap({}));
        std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("f0", arrow::int32())});
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                               FileStorePathFactory::Create(
                                   dir->Str(), schema, /*partition_keys=*/{},
                                   /*default_part_value=*/"", options.GetFileFormat()->Identifier(),
                                   /*data_file_prefix=*/"data-",
                                   /*legacy_partition_name_enabled=*/true,
                                   /*external_paths=*/{},
                                   /*global_index_external_path=*/std::nullopt,
                                   /*index_file_in_data_file_dir=*/false, GetDefaultPool()));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<ManifestList> manifest_list,
            ManifestList::Create(dir->GetFileSystem(), options.GetManifestFormat(),
                                 options.GetManifestCompression(), path_factory, options.GetCache(),
                                 GetDefaultPool()));
        std::vector<ManifestFileMeta> delta_metas = {metas.front()};
        std::vector<ManifestFileMeta> base_metas(metas.begin() + 1, metas.end());
        PAIMON_ASSIGN_OR_RAISE(ManifestListFile base, manifest_list->Write(base_metas));
        PAIMON_ASSIGN_OR_RAISE(ManifestListFile delta, manifest_list->Write(delta_metas));

        Snapshot snapshot(/*id=*/1, /*schema_id=*/0, base.first,
                          /*base_manifest_list_size=*/base.second, delta.first,
                          /*delta_manifest_list_size=*/delta.second,
                          /*changelog_manifest_list=*/std::nullopt,
                          /*changelog_manifest_list_size=*/std::nullopt,
                          /*index_manifest=*/std::nullopt, /*commit_user=*/"test-user",
                          /*commit_identifier=*/1, Snapshot::CommitKind::Append(),
                          /*time_millis=*/0, /*total_record_count=*/0, /*delta_record_count=*/0,
                          /*changelog_record_count=*/std::nullopt, /*watermark=*/std::nullopt,
                          /*statistics=*/std::nullopt, /*properties=*/std::nullopt,
                          /*next_row_id=*/std::nullopt);
        auto logger = Logger::GetLogger("DataEvolutionCompactPlannerTest");
        return DataEvolutionCompactPlanner::PlanRowIdWindows(
            manifest_list, snapshot, /*partition_filter=*/nullptr, /*partition_schema=*/nullptr,
            candidate_files_per_round, logger.get());
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
        return DataEvolutionCompactPlanner::PlanCompactTasks(partition_files, options);
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

TEST_F(DataEvolutionCompactPlannerTest, TestSingleFileProducesNoTask) {
    CoreOptions options = CreateOptions("1024", "1", "2");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks,
                         Plan({NewFile("f1", 100, 0, 100, 1, 1)}, options));
    ASSERT_TRUE(tasks.empty());
}

TEST_F(DataEvolutionCompactPlannerTest, TestContiguousFilesPackedByTargetSize) {
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

TEST_F(DataEvolutionCompactPlannerTest, TestOpenFileCostWeighsSmallFiles) {
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

TEST_F(DataEvolutionCompactPlannerTest, TestRowIdGapCutsBin) {
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

TEST_F(DataEvolutionCompactPlannerTest, TestLargeFileSkippedAndCutsBin) {
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

TEST_F(DataEvolutionCompactPlannerTest, TestEvolvedFieldGroupsPackTogether) {
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

TEST_F(DataEvolutionCompactPlannerTest, TestHeavyFieldGroupCompactedAlone) {
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

TEST_F(DataEvolutionCompactPlannerTest, TestMismatchedFieldGroupRangesRejected) {
    // Overlapping files that do not cover the exact same rows cannot form a field group.
    std::vector<std::shared_ptr<DataFileMeta>> files = {NewFile("f1", 100, 0, 100, 1, 1),
                                                        NewFile("f2", 100, 0, 50, 2, 2)};
    CoreOptions options = CreateOptions("1024", "1", "2");
    Result<std::vector<DataEvolutionNormalCompactTask>> result = Plan(files, options);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().ToString().find("same row range") != std::string::npos)
        << result.status().ToString();
}

TEST_F(DataEvolutionCompactPlannerTest, TestBlobFilesExcluded) {
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

TEST_F(DataEvolutionCompactPlannerTest, TestVectorStoreFilesRejected) {
    // Without a VECTOR schema type the rewrite cannot exclude vector columns, so a table
    // holding vector-store files is rejected instead of silently mis-planned.
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        NewFile("f1", 100, 0, 100, 1, 1), NewFile("f2", 50, 0, 100, 2, 2),
        NewFile("f3.vector.lance", 100, 0, 100, 2, 2)};
    CoreOptions options = CreateOptions("1024", "1", "2");
    Result<std::vector<DataEvolutionNormalCompactTask>> result = Plan(files, options);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().ToString().find("vector-store") != std::string::npos)
        << result.status().ToString();

    // The rejection is reported even when the vector-store file's own metadata is unusable:
    // an unsupported table should say so rather than blame whichever field looks invalid.
    std::vector<std::shared_ptr<DataFileMeta>> files_with_bad_vector_meta = {
        NewFile("f1", 100, 0, 100, 1, 1), NewFile("f2", 50, 0, 100, 2, 2),
        NewFile("f3.vector.lance", 100, /*first_row_id=*/0, /*row_count=*/0, 2, 2)};
    Result<std::vector<DataEvolutionNormalCompactTask>> bad_meta_result =
        Plan(files_with_bad_vector_meta, options);
    ASSERT_FALSE(bad_meta_result.ok());
    EXPECT_TRUE(bad_meta_result.status().ToString().find("vector-store") != std::string::npos)
        << bad_meta_result.status().ToString();
}

TEST_F(DataEvolutionCompactPlannerTest, TestPartitionsPlanIndependently) {
    CoreOptions options = CreateOptions("1024", "1", "2");
    LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>> partition_files;
    partition_files[CreateIntRow(0)] = {NewFile("p0-f1", 100, 0, 100, 1, 1),
                                        NewFile("p0-f2", 100, 100, 100, 2, 2)};
    partition_files[CreateIntRow(1)] = {NewFile("p1-f1", 100, 0, 100, 1, 1),
                                        NewFile("p1-f2", 100, 100, 100, 2, 2)};
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks,
                         DataEvolutionCompactPlanner::PlanCompactTasks(partition_files, options));
    ASSERT_EQ(tasks.size(), 2);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"p0-f1", "p0-f2"}));
    EXPECT_EQ(FileNames(tasks[1]), (std::vector<std::string>{"p1-f1", "p1-f2"}));
    EXPECT_EQ(tasks[0].Partition(), CreateIntRow(0));
    EXPECT_EQ(tasks[1].Partition(), CreateIntRow(1));
}

TEST_F(DataEvolutionCompactPlannerTest, TestMinFileNumOneAllowsSingleFileTask) {
    // There is no lower bound on a normal task's input size: with compaction.min.file-num=1
    // even a lone file may be rewritten.
    CoreOptions options = CreateOptions("1024", "1", "1");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks,
                         Plan({NewFile("f1", 100, 0, 100, 1, 1)}, options));
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(FileNames(tasks[0]), (std::vector<std::string>{"f1"}));
}

TEST_F(DataEvolutionCompactPlannerTest, TestMinFileNumSuppressesSmallBins) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {NewFile("f1", 100, 0, 100, 1, 1),
                                                        NewFile("f2", 100, 100, 100, 2, 2)};
    CoreOptions options = CreateOptions("1024", "1", "5");
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionNormalCompactTask> tasks, Plan(files, options));
    ASSERT_TRUE(tasks.empty());
}

TEST_F(DataEvolutionCompactPlannerTest, TestPlanRejectsInvalidFileMeta) {
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

/// The open ended tail of the last window: every row id the table can still grow into.
constexpr int64_t kOpenEnd = std::numeric_limits<int64_t>::max();

TEST_F(DataEvolutionCompactPlannerTest, TestPlanRowIdWindowsCutsAtCoverageGaps) {
    // Three manifests, none reaching into the next: with a one-file budget every manifest is a
    // round of its own, and the last window stays open ended so rows appended later belong to
    // it rather than to no window at all.
    ASSERT_OK_AND_ASSIGN(
        std::vector<Range> windows,
        PlanWindows({NewManifest("m0", /*num_added_files=*/1,
                                 /*num_deleted_files=*/0, 0, 9),
                     NewManifest("m1", 1, 0, 10, 19), NewManifest("m2", 1, 0, 20, 29)},
                    /*candidate_files_per_round=*/1));
    ASSERT_EQ(windows, (std::vector<Range>{Range(0, 9), Range(10, 19), Range(20, kOpenEnd)}));
}

TEST_F(DataEvolutionCompactPlannerTest, TestPlanRowIdWindowsNeverCutsInsideCoveredRows) {
    // A cut anywhere but a coverage gap would hand a file straddling it to two rounds, so an
    // overlapping manifest holds the window open even though the budget is already reached.
    ASSERT_OK_AND_ASSIGN(std::vector<Range> windows,
                         PlanWindows({NewManifest("m0", 1, 0, 0, 9), NewManifest("m1", 1, 0, 5, 14),
                                      NewManifest("m2", 1, 0, 20, 29)},
                                     /*candidate_files_per_round=*/1));
    ASSERT_EQ(windows, (std::vector<Range>{Range(0, 14), Range(15, kOpenEnd)}));
}

TEST_F(DataEvolutionCompactPlannerTest, TestPlanRowIdWindowsHonorsTheFileBudget) {
    // The same three manifests with a two-file budget: the first gap is passed over because the
    // window has not collected enough candidates yet.
    ASSERT_OK_AND_ASSIGN(std::vector<Range> windows, PlanWindows({NewManifest("m0", 1, 0, 0, 9),
                                                                  NewManifest("m1", 1, 0, 10, 19),
                                                                  NewManifest("m2", 1, 0, 20, 29)},
                                                                 /*candidate_files_per_round=*/2));
    ASSERT_EQ(windows, (std::vector<Range>{Range(0, 19), Range(20, kOpenEnd)}));

    // A budget no window reaches leaves the whole row id space in one round.
    ASSERT_OK_AND_ASSIGN(
        windows, PlanWindows({NewManifest("m0", 1, 0, 0, 9), NewManifest("m1", 1, 0, 10, 19),
                              NewManifest("m2", 1, 0, 20, 29)},
                             /*candidate_files_per_round=*/100));
    ASSERT_EQ(windows, (std::vector<Range>{Range(0, kOpenEnd)}));
}

TEST_F(DataEvolutionCompactPlannerTest, TestPlanRowIdWindowsFallsBackWithoutRowIdStatistics) {
    // A manifest whose files cannot be placed would leave a hole in the windows, so the split
    // is abandoned rather than silently skipping those files. An empty result asks the caller
    // for a single unbounded round.
    ASSERT_OK_AND_ASSIGN(std::vector<Range> windows,
                         PlanWindows({NewManifest("m0", 1, 0, 0, 9),
                                      NewManifest("m1", 1, 0, /*min_row_id=*/std::nullopt,
                                                  /*max_row_id=*/std::nullopt)},
                                     /*candidate_files_per_round=*/1));
    ASSERT_TRUE(windows.empty());

    // Statistics that cannot describe a range are just as unusable.
    ASSERT_OK_AND_ASSIGN(windows,
                         PlanWindows({NewManifest("m0", 1, 0, 0, 9),
                                      NewManifest("m1", 1, 0, /*min_row_id=*/5, /*max_row_id=*/4)},
                                     /*candidate_files_per_round=*/1));
    ASSERT_TRUE(windows.empty());
}

TEST_F(DataEvolutionCompactPlannerTest, TestPlanRowIdWindowsIgnoresDeleteOnlyManifests) {
    // A manifest holding no ADD entry carries no candidate, so it can neither pad a window nor
    // disable the split by lacking row id statistics - which is exactly what the manifest left
    // behind by materializing a fully deleted range looks like.
    ASSERT_OK_AND_ASSIGN(
        std::vector<Range> windows,
        PlanWindows({NewManifest("m0", /*num_added_files=*/1, /*num_deleted_files=*/0, 0, 9),
                     NewManifest("deletes", /*num_added_files=*/0, /*num_deleted_files=*/3,
                                 /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt),
                     NewManifest("m1", 1, 0, 20, 29)},
                    /*candidate_files_per_round=*/1));
    ASSERT_EQ(windows, (std::vector<Range>{Range(0, 9), Range(10, kOpenEnd)}));

    // With nothing but delete-only manifests there is no candidate at all, so there is nothing
    // to split and the caller falls back to one round.
    ASSERT_OK_AND_ASSIGN(
        windows, PlanWindows({NewManifest("d0", 0, 3, 0, 9), NewManifest("d1", 0, 3, 20, 29)},
                             /*candidate_files_per_round=*/1));
    ASSERT_TRUE(windows.empty());
}

}  // namespace paimon::test
