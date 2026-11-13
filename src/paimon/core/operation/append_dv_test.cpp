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

#include <iterator>
#include <map>
#include <tuple>
#include <utility>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/api.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/deletionvectors/bitmap64_deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/deletion_vector_write.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class CountingDvReadFileSystem : public LocalFileSystem {
 public:
    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        ++open_count;
        ++file_open_counts[path];
        if (StringUtils::EndsWith(path, DataFilePathFactory::INDEX_PATH_SUFFIX)) {
            ++index_open_count;
        }
        return LocalFileSystem::Open(path);
    }

    mutable int64_t open_count = 0;
    mutable int64_t index_open_count = 0;
    mutable std::map<std::string, int64_t> file_open_counts;
};

class AppendDvTest : public testing::TestWithParam<std::tuple<int32_t, int32_t>> {
 protected:
    void SetUp() override {
        directory_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(directory_);
        schema_ = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("value", arrow::int32()),
                                 arrow::field("pt", arrow::utf8())});
        options_ = {{"bucket", std::to_string(std::get<0>(GetParam()))},
                    {"file.format", "orc"},
                    {"write-only", "true"},
                    {"deletion-vectors.enabled", "true"},
                    {"deletion-vectors.bitmap64", "false"},
                    {"file-index.bitmap.columns", "id"},
                    {"file-index.bloom-filter.columns", "value"},
                    {"file-index.in-manifest-threshold", std::to_string(std::get<1>(GetParam()))}};
        if (std::get<0>(GetParam()) > 0) {
            options_[Options::BUCKET_KEY] = "id";
        }
        ASSERT_OK_AND_ASSIGN(helper_,
                             TestHelper::Create(directory_->Str(), schema_, {"pt"}, {}, options_,
                                                /*is_streaming_mode=*/true));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                             TestHelper::MakeRecordBatch(arrow::struct_(schema_->fields()),
                                                         R"([[1,10,"a"],[2,20,"a"],[3,30,"a"]])",
                                                         {{"pt", "a"}}, 0, {}));
        ASSERT_OK(helper_->WriteAndCommit(std::move(first), next_commit_++, std::nullopt));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> second,
            TestHelper::MakeRecordBatch(arrow::struct_(schema_->fields()),
                                        R"([[4,40,"a"],[5,50,"a"]])", {{"pt", "a"}}, 0, {}));
        ASSERT_OK(helper_->WriteAndCommit(std::move(second), next_commit_++, std::nullopt));
    }

    std::shared_ptr<Predicate> Id(int32_t id) const {
        return PredicateBuilder::Equal(0, "id", FieldType::INT, Literal(id));
    }

    Result<std::shared_ptr<Plan>> PlanSnapshot(const std::optional<int64_t>& snapshot_id) {
        ScanContextBuilder scan_builder(helper_->TablePath());
        scan_builder.SetOptions(options_).WithStreamingMode(false);
        if (snapshot_id) {
            scan_builder.AddOption(Options::SCAN_SNAPSHOT_ID, std::to_string(*snapshot_id));
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan,
                               TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, scan->CreatePlan());
        if (!plan->SnapshotId()) {
            return Status::Invalid("Expected a snapshot in the test table");
        }
        last_query_snapshot_id_ = *plan->SnapshotId();
        return plan;
    }

    Result<std::unique_ptr<FileIndexRead>> NewIndexRead(const std::shared_ptr<Split>& split) {
        return FileIndexRead::Create(helper_->TablePath(), "main", options_, split,
                                     std::make_shared<LocalFileSystem>(), GetDefaultPool());
    }

    Result<std::vector<FileRowSelection>> Query(
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<int64_t>& snapshot_id = std::nullopt) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, PlanSnapshot(snapshot_id));
        std::vector<FileRowSelection> result;
        for (const auto& split : plan->Splits()) {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexRead> read, NewIndexRead(split));
            PAIMON_ASSIGN_OR_RAISE(std::vector<FileRowSelection> matches, read->Query(predicate));
            result.insert(result.end(), std::make_move_iterator(matches.begin()),
                          std::make_move_iterator(matches.end()));
        }
        return result;
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> PrepareDeletions(
        const std::vector<FileRowSelection>& deletions,
        const std::optional<int64_t>& snapshot_id = std::nullopt) {
        return DeletionVectorWrite::PrepareCommit(
            helper_->TablePath(), "main", options_, snapshot_id, deletions,
            std::make_shared<LocalFileSystem>(), GetDefaultPool(), CreateDefaultExecutor());
    }

    Status Commit(const std::vector<std::shared_ptr<CommitMessage>>& messages) {
        CommitContextBuilder builder(helper_->TablePath(), "external_update");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        return commit->Commit(messages, next_commit_++);
    }

    std::unique_ptr<UniqueTestDirectory> directory_;
    std::unique_ptr<TestHelper> helper_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
    int64_t next_commit_ = 1;
    int64_t last_query_snapshot_id_ = -1;
};

TEST_P(AppendDvTest, ExactPositionsWithIndexAndScan) {
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> matches, Query(Id(2)));
    ASSERT_EQ(matches.size(), 1);
    ASSERT_EQ(matches[0].file.row_count, 3);
    ASSERT_EQ(matches[0].bucket, 0);
    ASSERT_EQ(matches[0].partition.at("pt"), "a");
    ASSERT_EQ(matches[0].positions.Cardinality(), 1);
    ASSERT_EQ(*matches[0].positions.Begin(), 1);

    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> empty, Query(Id(99)));
    ASSERT_TRUE(empty.empty());
    auto inexact = PredicateBuilder::Equal(1, "value", FieldType::INT, Literal(10));
    ASSERT_OK_AND_ASSIGN(matches, Query(inexact));
    ASSERT_EQ(matches.size(), 1);
    ASSERT_EQ(matches[0].positions, RoaringBitmap32::From({0}));
    ASSERT_OK_AND_ASSIGN(matches,
                         Query(PredicateBuilder::Equal(2, "pt", FieldType::STRING,
                                                       Literal(FieldType::STRING, "a", 1))));
    ASSERT_EQ(matches.size(), 2);
    ASSERT_EQ(matches[0].positions.Cardinality() + matches[1].positions.Cardinality(), 5);
    ASSERT_OK_AND_ASSIGN(matches,
                         Query(PredicateBuilder::GreaterThan(0, "id", FieldType::INT, Literal(0))));
    ASSERT_EQ(matches.size(), 2);
    ASSERT_EQ(matches[0].positions.Cardinality() + matches[1].positions.Cardinality(), 5);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> conjunction,
                         PredicateBuilder::And({Id(1), Id(3), inexact}));
    ASSERT_OK_AND_ASSIGN(empty, Query(conjunction));
    ASSERT_TRUE(empty.empty());
    ASSERT_OK_AND_ASSIGN(conjunction, PredicateBuilder::And({Id(1), inexact}));
    ASSERT_OK_AND_ASSIGN(matches, Query(conjunction));
    ASSERT_EQ(matches.size(), 1);
    ASSERT_EQ(matches[0].positions, RoaringBitmap32::From({0}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> disjunction,
                         PredicateBuilder::Or({Id(1), inexact}));
    ASSERT_OK_AND_ASSIGN(matches, Query(disjunction));
    ASSERT_EQ(matches.size(), 1);
    ASSERT_EQ(matches[0].positions, RoaringBitmap32::From({0}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> exact_or, PredicateBuilder::Or({Id(2), Id(4)}));
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> two_files, Query(exact_or));
    ASSERT_EQ(two_files.size(), 2);
}

TEST_P(AppendDvTest, MissingIndexesScanLivePhysicalPositions) {
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> deleted, Query(Id(1)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         PrepareDeletions(deleted, last_query_snapshot_id_));
    ASSERT_OK(Commit(messages));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, PlanSnapshot(std::nullopt));
    size_t matched_files = 0;
    int64_t live_rows = 0;
    for (const auto& split : plan->Splits()) {
        auto source = std::dynamic_pointer_cast<DataSplitImpl>(split);
        ASSERT_TRUE(source);
        std::vector<std::shared_ptr<DataFileMeta>> files;
        for (const auto& file : source->DataFiles()) {
            auto without_index = std::make_shared<DataFileMeta>(*file);
            without_index->embedded_index.reset();
            without_index->extra_files.clear();
            files.push_back(std::move(without_index));
        }
        DataSplitImpl::Builder builder(source->Partition(), source->Bucket(), source->BucketPath(),
                                       std::move(files));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> without_indexes,
                             builder.WithSnapshot(*plan->SnapshotId())
                                 .WithDataDeletionFiles(source->DeletionFiles())
                                 .RawConvertible(true)
                                 .Build());
        auto read_options = options_;
        read_options[Options::READ_BATCH_SIZE] = "1";
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FileIndexRead> read,
            FileIndexRead::Create(helper_->TablePath(), "main", read_options, without_indexes,
                                  std::make_shared<LocalFileSystem>(), GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> matches, read->Query(Id(1)));
        ASSERT_TRUE(matches.empty());
        ASSERT_OK_AND_ASSIGN(matches, read->Query(Id(2)));
        matched_files += matches.size();
        if (!matches.empty()) {
            ASSERT_EQ(matches[0].positions, RoaringBitmap32::From({1}));
        }
        ASSERT_OK_AND_ASSIGN(matches, read->Query(PredicateBuilder::GreaterThan(
                                          0, "id", FieldType::INT, Literal(0))));
        for (const auto& match : matches) {
            live_rows += match.positions.Cardinality();
            ASSERT_EQ(match.positions, match.file.row_count == 3 ? RoaringBitmap32::From({1, 2})
                                                                 : RoaringBitmap32::From({0, 1}));
        }
    }
    ASSERT_EQ(matched_files, 1);
    ASSERT_EQ(live_rows, 4);
}

TEST_P(AppendDvTest, MissingDeclaredIndexPropagatesError) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, PlanSnapshot(std::nullopt));
    ASSERT_FALSE(plan->Splits().empty());
    auto source = std::dynamic_pointer_cast<DataSplitImpl>(plan->Splits()[0]);
    ASSERT_TRUE(source);
    ASSERT_FALSE(source->DataFiles().empty());
    auto file = std::make_shared<DataFileMeta>(*source->DataFiles()[0]);
    file->embedded_index.reset();
    file->extra_files = {std::string("missing.index")};
    DataSplitImpl::Builder builder(source->Partition(), source->Bucket(), source->BucketPath(),
                                   {file});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> missing_index,
                         builder.WithSnapshot(*plan->SnapshotId()).RawConvertible(true).Build());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileIndexRead> read, NewIndexRead(missing_index));
    ASSERT_NOK(read->Query(PredicateBuilder::GreaterThan(0, "id", FieldType::INT, Literal(0))));
}

TEST_P(AppendDvTest, ReusesSplitReadersAcrossPredicates) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, PlanSnapshot(std::nullopt));
    ASSERT_FALSE(plan->Splits().empty());
    auto fs = std::make_shared<CountingDvReadFileSystem>();
    ASSERT_NOK(FileIndexRead::Create(helper_->TablePath(), "main", options_, plan->Splits()[0],
                                     nullptr, GetDefaultPool()));
    ASSERT_NOK(FileIndexRead::Create(helper_->TablePath(), "main", options_, plan->Splits()[0], fs,
                                     nullptr));
    ASSERT_NOK(FileIndexRead::Create(helper_->TablePath(), "../bad", options_, plan->Splits()[0],
                                     fs, GetDefaultPool()));
    size_t first_count = 0;
    size_t second_count = 0;
    for (const auto& split : plan->Splits()) {
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FileIndexRead> read,
            FileIndexRead::Create(helper_->TablePath(), "main", {}, split, fs, GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> warmup,
                             PredicateBuilder::Or({Id(1), Id(4)}));
        ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> warmed, read->Query(warmup));
        ASSERT_FALSE(warmed.empty());
        ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> first, read->Query(Id(1)));
        const int64_t opened = fs->index_open_count;
        if (std::get<1>(GetParam()) == 0) {
            ASSERT_GT(opened, 0);
        }
        ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> second, read->Query(Id(2)));
        ASSERT_EQ(fs->index_open_count, opened);
        first_count += first.size();
        second_count += second.size();
        if (!first.empty()) {
            ASSERT_EQ(first[0].positions, RoaringBitmap32::From({0}));
            ASSERT_EQ(second.size(), 1);
            ASSERT_EQ(second[0].positions, RoaringBitmap32::From({1}));
            first[0].positions.Add(2);
        }
        ASSERT_NOK(read->Query(nullptr));
        ASSERT_OK_AND_ASSIGN(
            std::vector<FileRowSelection> scanned,
            read->Query(PredicateBuilder::GreaterThan(0, "id", FieldType::INT, Literal(0))));
        ASSERT_FALSE(scanned.empty());
        auto inexact =
            PredicateBuilder::Equal(1, "value", FieldType::INT, Literal(first.empty() ? 40 : 10));
        ASSERT_OK_AND_ASSIGN(scanned, read->Query(inexact));
        ASSERT_EQ(scanned.size(), 1);
        ASSERT_EQ(scanned[0].positions, RoaringBitmap32::From({0}));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<Predicate> conjunction,
            PredicateBuilder::And({Id(first.empty() ? 4 : 1), Id(first.empty() ? 5 : 3), inexact}));
        ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> empty, read->Query(conjunction));
        ASSERT_TRUE(empty.empty());
        ASSERT_OK_AND_ASSIGN(conjunction,
                             PredicateBuilder::And({Id(first.empty() ? 4 : 1), inexact}));
        ASSERT_OK_AND_ASSIGN(scanned, read->Query(conjunction));
        ASSERT_EQ(scanned.size(), 1);
        ASSERT_EQ(scanned[0].positions, RoaringBitmap32::From({0}));
        const int64_t opened_after_scan = fs->index_open_count;
        ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> repeated, read->Query(Id(1)));
        ASSERT_EQ(repeated.size(), first.size());
        if (!repeated.empty()) {
            ASSERT_EQ(repeated[0].positions, RoaringBitmap32::From({0}));
        }
        ASSERT_EQ(fs->index_open_count, opened_after_scan);
    }
    ASSERT_EQ(first_count, 1);
    ASSERT_EQ(second_count, 1);
}

TEST_P(AppendDvTest, EmptySplitReturnsEmptyMatches) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, PlanSnapshot(std::nullopt));
    ASSERT_FALSE(plan->Splits().empty());
    auto source = std::dynamic_pointer_cast<DataSplitImpl>(plan->Splits()[0]);
    ASSERT_TRUE(source);
    DataSplitImpl::Builder builder(source->Partition(), source->Bucket(), source->BucketPath(), {});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> empty_split,
                         builder.WithSnapshot(*plan->SnapshotId()).RawConvertible(true).Build());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileIndexRead> read, NewIndexRead(empty_split));
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> matches, read->Query(Id(1)));
    ASSERT_TRUE(matches.empty());
    ASSERT_NOK(read->Query(nullptr));
}

TEST_P(AppendDvTest, StatisticsPruneBeforeOpeningIndexes) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, PlanSnapshot(std::nullopt));
    auto fs = std::make_shared<CountingDvReadFileSystem>();
    for (const auto& split : plan->Splits()) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileIndexRead> read,
                             FileIndexRead::Create(helper_->TablePath(), "main", options_, split,
                                                   fs, GetDefaultPool()));
        const int64_t opened = fs->open_count;
        ASSERT_OK_AND_ASSIGN(
            std::vector<FileRowSelection> matches,
            read->Query(PredicateBuilder::GreaterThan(0, "id", FieldType::INT, Literal(99))));
        ASSERT_TRUE(matches.empty());
        ASSERT_OK_AND_ASSIGN(matches, read->Query(PredicateBuilder::Equal(
                                          1, "value", FieldType::INT, Literal(999))));
        ASSERT_TRUE(matches.empty());
        ASSERT_EQ(fs->open_count, opened);
        ASSERT_OK_AND_ASSIGN(matches, read->Query(PredicateBuilder::GreaterThan(
                                          0, "id", FieldType::INT, Literal(0))));
        ASSERT_FALSE(matches.empty());
    }
}

TEST_P(AppendDvTest, UnknownStatisticsUseIndexesAndScan) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, PlanSnapshot(std::nullopt));
    size_t match_count = 0;
    size_t scan_match_count = 0;
    for (const auto& split : plan->Splits()) {
        auto source = std::dynamic_pointer_cast<DataSplitImpl>(split);
        ASSERT_TRUE(source);
        std::vector<std::shared_ptr<DataFileMeta>> files;
        for (const auto& file : source->DataFiles()) {
            auto without_stats = std::make_shared<DataFileMeta>(*file);
            without_stats->value_stats = SimpleStats::EmptyStats();
            without_stats->value_stats_cols = std::nullopt;
            files.push_back(std::move(without_stats));
        }
        DataSplitImpl::Builder builder(source->Partition(), source->Bucket(), source->BucketPath(),
                                       std::move(files));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<DataSplitImpl> without_stats,
            builder.WithSnapshot(*plan->SnapshotId()).RawConvertible(true).Build());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileIndexRead> read, NewIndexRead(without_stats));
        ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> matches, read->Query(Id(2)));
        match_count += matches.size();
        ASSERT_OK_AND_ASSIGN(
            matches, read->Query(PredicateBuilder::Equal(1, "value", FieldType::INT, Literal(10))));
        scan_match_count += matches.size();
        if (!matches.empty()) {
            ASSERT_EQ(matches[0].positions, RoaringBitmap32::From({0}));
        }
    }
    ASSERT_EQ(match_count, 1);
    ASSERT_EQ(scan_match_count, 1);
}

TEST_P(AppendDvTest, AddedColumnIsAllNullInHistoricalFiles) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, PlanSnapshot(std::nullopt));
    auto fs = std::make_shared<CountingDvReadFileSystem>();
    SchemaManager manager(fs, helper_->TablePath());
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> schema, manager.Latest());
    ASSERT_TRUE(schema);
    auto fields = (*schema)->Fields();
    const int32_t field_index = static_cast<int32_t>(fields.size());
    const int32_t field_id = (*schema)->HighestFieldId() + 1;
    fields.emplace_back(field_id, arrow::field("added", arrow::int32()));
    ASSERT_OK(TestHelper::WriteNextSchema(fs, helper_->TablePath(), fields, field_id,
                                          (*schema)->Options()));
    for (const auto& split : plan->Splits()) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileIndexRead> read,
                             FileIndexRead::Create(helper_->TablePath(), "main", options_, split,
                                                   fs, GetDefaultPool()));
        auto equal = PredicateBuilder::Equal(field_index, "added", FieldType::INT, Literal(1));
        ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> matches, read->Query(equal));
        ASSERT_TRUE(matches.empty());
        const int64_t opened = fs->open_count;
        ASSERT_OK_AND_ASSIGN(matches, read->Query(PredicateBuilder::IsNotNull(field_index, "added",
                                                                              FieldType::INT)));
        ASSERT_TRUE(matches.empty());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> conjunction,
                             PredicateBuilder::And({equal, Id(2)}));
        ASSERT_OK_AND_ASSIGN(matches, read->Query(conjunction));
        ASSERT_TRUE(matches.empty());
        ASSERT_EQ(fs->open_count, opened);
        ASSERT_OK_AND_ASSIGN(
            matches, read->Query(PredicateBuilder::IsNull(field_index, "added", FieldType::INT)));
        ASSERT_FALSE(matches.empty());
        for (const auto& match : matches) {
            ASSERT_EQ(match.positions.Cardinality(), match.file.row_count);
        }
    }
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> unchanged, Query(Id(2)));
    ASSERT_EQ(unchanged.size(), 1);
    ASSERT_EQ(unchanged[0].positions, RoaringBitmap32::From({1}));
}

TEST_P(AppendDvTest, BoundSplitRetainsHistoricalDeletions) {
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> targets, Query(Id(1)));
    ASSERT_EQ(targets.size(), 1);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         PrepareDeletions(targets, last_query_snapshot_id_));
    ASSERT_OK(Commit(messages));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, PlanSnapshot(std::nullopt));
    auto fs = std::make_shared<CountingDvReadFileSystem>();
    bool found = false;
    for (const auto& split : plan->Splits()) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileIndexRead> read,
                             FileIndexRead::Create(helper_->TablePath(), "main", options_, split,
                                                   fs, GetDefaultPool()));
        ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> deleted, read->Query(Id(1)));
        ASSERT_TRUE(deleted.empty());
        const auto opened = fs->file_open_counts;
        const int64_t indexes_opened = fs->index_open_count;
        ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> live, read->Query(Id(2)));
        ASSERT_EQ(fs->index_open_count, indexes_opened);
        if (live.empty()) {
            continue;
        }
        found = true;
        ASSERT_OK_AND_ASSIGN(messages, PrepareDeletions(live, *plan->SnapshotId()));
        ASSERT_OK(Commit(messages));

        ASSERT_OK_AND_ASSIGN(live, read->Query(Id(2)));
        ASSERT_EQ(live.size(), 1);
        ASSERT_EQ(live[0].positions, RoaringBitmap32::From({1}));
        ASSERT_OK_AND_ASSIGN(deleted, read->Query(Id(1)));
        ASSERT_TRUE(deleted.empty());
        ASSERT_EQ(fs->index_open_count, indexes_opened);
        auto source = std::dynamic_pointer_cast<DataSplitImpl>(split);
        ASSERT_TRUE(source);
        bool checked_dv = false;
        for (const auto& deletion : source->DeletionFiles()) {
            if (deletion) {
                ASSERT_TRUE(opened.count(deletion->path));
                ASSERT_EQ(fs->file_open_counts.at(deletion->path), opened.at(deletion->path));
                checked_dv = true;
            }
        }
        ASSERT_TRUE(checked_dv);
    }
    ASSERT_TRUE(found);
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> latest, Query(Id(2)));
    ASSERT_TRUE(latest.empty());
}

TEST_P(AppendDvTest, MergeHistoryAndAppendInOneCommit) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> first_deletes,
                         PredicateBuilder::Or({Id(1), Id(4)}));
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> targets, Query(first_deletes));
    ASSERT_EQ(targets.size(), 2);
    const int64_t base_snapshot_id = last_query_snapshot_id_;
    auto duplicate_targets = targets;
    duplicate_targets.insert(duplicate_targets.end(), targets.begin(), targets.end());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         PrepareDeletions(duplicate_targets, base_snapshot_id));
    ASSERT_EQ(messages.size(), 1);

    ASSERT_OK_AND_ASSIGN(std::string bytes,
                         CommitMessage::SerializeList(messages, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> decoded,
        CommitMessage::DeserializeList(CommitMessage::CurrentVersion(), bytes.data(), bytes.size(),
                                       GetDefaultPool()));
    ASSERT_OK(Commit(decoded));
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> deleted, Query(first_deletes));
    ASSERT_TRUE(deleted.empty());
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> historical,
                         Query(first_deletes, base_snapshot_id));
    ASSERT_EQ(historical.size(), 2);

    // Restore historical DV, not the latest snapshot's DV.
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> historical_messages,
                         PrepareDeletions({historical[0]}, base_snapshot_id));
    ASSERT_EQ(historical_messages.size(), 1);

    // Rewriting a shared DV container must preserve untouched files.
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> update, Query(Id(2)));
    ASSERT_EQ(update.size(), 1);
    ASSERT_EQ(update[0].file.delete_row_count, std::optional<int64_t>(1));
    ASSERT_OK_AND_ASSIGN(messages, PrepareDeletions(update));
    WriteContextBuilder builder(helper_->TablePath(), "external_update");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> append_context,
                         builder.SetOptions(options_).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> append,
                         FileStoreWrite::Create(std::move(append_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> replacement,
                         TestHelper::MakeRecordBatch(arrow::struct_(schema_->fields()),
                                                     R"([[2,200,"a"]])", {{"pt", "a"}}, 0, {}));
    ASSERT_OK(append->Write(std::move(replacement)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> appended,
                         append->PrepareCommit(/*wait_compaction=*/true, next_commit_));
    messages.insert(messages.end(), appended.begin(), appended.end());
    ASSERT_OK(Commit(messages));
    ASSERT_OK(append->Close());
    ASSERT_OK_AND_ASSIGN(deleted, Query(first_deletes));
    ASSERT_TRUE(deleted.empty());
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> replacement_rows, Query(Id(2)));
    ASSERT_EQ(replacement_rows.size(), 1);
    ASSERT_EQ(replacement_rows[0].file.delete_row_count, std::optional<int64_t>(0));
    ASSERT_NE(replacement_rows[0].file.file_path, update[0].file.file_path);
    ASSERT_EQ(*replacement_rows[0].positions.Begin(), 0);
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> unchanged, Query(Id(5)));
    ASSERT_EQ(unchanged.size(), 1);
    ASSERT_EQ(unchanged[0].file.delete_row_count, std::optional<int64_t>(1));
    ASSERT_EQ(*unchanged[0].positions.Begin(), 1);

    // Committed deletions are idempotent.
    ASSERT_OK_AND_ASSIGN(messages, PrepareDeletions({targets[0]}, std::nullopt));
    ASSERT_TRUE(messages.empty());
}

TEST_P(AppendDvTest, RejectInvalidTargetsBeforeWriting) {
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> matches, Query(Id(2)));
    ASSERT_EQ(matches.size(), 1);
    FileRowSelection invalid = matches[0];
    invalid.positions.Add(invalid.file.row_count);
    ASSERT_NOK(PrepareDeletions({matches[0], invalid}, last_query_snapshot_id_));
    invalid = matches[0];
    ++invalid.file.schema_id;
    ASSERT_NOK(PrepareDeletions({invalid}, last_query_snapshot_id_));
    invalid = matches[0];
    invalid.partition["pt"] = "other";
    ASSERT_NOK(PrepareDeletions({invalid}, last_query_snapshot_id_));
    invalid = matches[0];
    invalid.file.file_path += ".other";
    ASSERT_NOK(PrepareDeletions({invalid}, last_query_snapshot_id_));
    invalid = matches[0];
    ++invalid.bucket;
    ASSERT_NOK(PrepareDeletions({invalid}, last_query_snapshot_id_));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         PrepareDeletions({}, last_query_snapshot_id_));
    ASSERT_TRUE(messages.empty());
    ASSERT_NOK(PrepareDeletions({}, -1));
    ASSERT_NOK(PrepareDeletions({}, 9999));
}

TEST_P(AppendDvTest, PartitionAndBucketIsolationAndNulls) {
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> other_partition,
        TestHelper::MakeRecordBatch(arrow::struct_(schema_->fields()),
                                    R"([[1,70,"b"],[null,80,"b"]])", {{"pt", "b"}}, 0, {}));
    ASSERT_OK(helper_->WriteAndCommit(std::move(other_partition), next_commit_++, std::nullopt));
    if (std::get<0>(GetParam()) > 0) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> other_bucket,
                             TestHelper::MakeRecordBatch(arrow::struct_(schema_->fields()),
                                                         R"([[1,90,"a"]])", {{"pt", "a"}}, 1, {}));
        ASSERT_OK(helper_->WriteAndCommit(std::move(other_bucket), next_commit_++, std::nullopt));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> matches, Query(Id(1)));
    ASSERT_EQ(matches.size(), std::get<0>(GetParam()) > 0 ? 3 : 2);
    std::vector<FileRowSelection> deletions;
    for (const auto& match : matches) {
        if (match.partition.at("pt") == "b") {
            deletions.push_back(match);
        }
    }
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         PrepareDeletions(deletions, last_query_snapshot_id_));
    ASSERT_EQ(messages.size(), 1);
    ASSERT_OK(Commit(messages));
    ASSERT_OK_AND_ASSIGN(matches, Query(Id(1)));
    ASSERT_EQ(matches.size(), std::get<0>(GetParam()) > 0 ? 2 : 1);
    for (const auto& match : matches) {
        ASSERT_EQ(match.partition.at("pt"), "a");
    }
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> nulls,
                         Query(PredicateBuilder::IsNull(0, "id", FieldType::INT)));
    ASSERT_EQ(nulls.size(), 1);
    ASSERT_EQ(nulls[0].partition.at("pt"), "b");
    ASSERT_EQ(*nulls[0].positions.Begin(), 1);
}

TEST_P(AppendDvTest, BatchUsesOneSnapshotAcrossPartitionsAndBuckets) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> other_partition,
                         TestHelper::MakeRecordBatch(arrow::struct_(schema_->fields()),
                                                     R"([[1,70,"b"]])", {{"pt", "b"}}, 0, {}));
    ASSERT_OK(helper_->WriteAndCommit(std::move(other_partition), next_commit_++, std::nullopt));
    if (std::get<0>(GetParam()) > 0) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> other_bucket,
                             TestHelper::MakeRecordBatch(arrow::struct_(schema_->fields()),
                                                         R"([[1,90,"a"]])", {{"pt", "a"}}, 1, {}));
        ASSERT_OK(helper_->WriteAndCommit(std::move(other_bucket), next_commit_++, std::nullopt));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> targets, Query(Id(1)));
    const int64_t base_snapshot_id = last_query_snapshot_id_;
    const size_t bucket_count = std::get<0>(GetParam()) > 0 ? 3 : 2;
    ASSERT_EQ(targets.size(), bucket_count);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         PrepareDeletions(targets, base_snapshot_id));
    ASSERT_EQ(messages.size(), bucket_count);
    ASSERT_OK(Commit(messages));
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> deleted, Query(Id(1)));
    ASSERT_TRUE(deleted.empty());

    // Every bucket restores the requested history, even after a newer snapshot is committed.
    ASSERT_OK_AND_ASSIGN(messages, PrepareDeletions(targets, base_snapshot_id));
    ASSERT_EQ(messages.size(), bucket_count);
    ASSERT_OK_AND_ASSIGN(messages, PrepareDeletions(targets, std::nullopt));
    ASSERT_TRUE(messages.empty());
}

TEST_P(AppendDvTest, LatestSnapshotRequiresExistingSnapshot) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TestHelper> empty_table,
        TestHelper::Create(directory->Str(), schema_, {"pt"}, {}, options_, false));
    ASSERT_NOK_WITH_MSG(
        DeletionVectorWrite::PrepareCommit(empty_table->TablePath(), "main", options_, std::nullopt,
                                           {}, std::make_shared<LocalFileSystem>(),
                                           GetDefaultPool(), CreateDefaultExecutor()),
        "existing snapshot");
}

TEST_P(AppendDvTest, RejectRealtimeConfiguration) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, PlanSnapshot(std::nullopt));
    ASSERT_FALSE(plan->Splits().empty());
    options_[Options::REALTIME_ENABLED] = "true";
    ASSERT_NOK_WITH_MSG(NewIndexRead(plan->Splits().front()), "ordinary managed append table");
    ASSERT_NOK_WITH_MSG(PrepareDeletions({}, last_query_snapshot_id_),
                        "ordinary managed append table");
}

TEST_P(AppendDvTest, RejectBitmap64Configuration) {
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> matches, Query(Id(2)));
    options_["deletion-vectors.bitmap64"] = "true";
    ASSERT_NOK_WITH_MSG(Query(Id(2)), "bitmap64");
    ASSERT_NOK_WITH_MSG(PrepareDeletions(matches, last_query_snapshot_id_), "bitmap64");
}

TEST_P(AppendDvTest, RejectHistoricalBitmap64WithBitmap32Option) {
    ASSERT_OK_AND_ASSIGN(std::vector<FileRowSelection> matches, Query(Id(2)));
    ASSERT_EQ(matches.size(), 1);
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(options_));
    FileStore store(helper_->TablePath(), options);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> paths,
                         store.NewPathFactory(schema_, {"pt"}, "orc", GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(BinaryRow partition, paths->ToBinaryRow(matches[0].partition));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexPathFactory> index_paths,
                         paths->CreateIndexFileFactory(partition, matches[0].bucket));
    DeletionVectorsIndexFile index_file(options.GetFileSystem(), std::move(index_paths),
                                        /*bitmap64=*/true, GetDefaultPool());
    auto dv = std::make_shared<Bitmap64DeletionVector>();
    ASSERT_OK(dv->Delete(0));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<IndexFileMeta> index,
        index_file.WriteSingleFile({{PathUtil::GetName(matches[0].file.file_path), dv}}));
    auto message = std::make_shared<CommitMessageImpl>(partition, matches[0].bucket, std::nullopt,
                                                       DataIncrement({}, {}, {}, {index}, {}),
                                                       CompactIncrement({}, {}, {}));
    ASSERT_OK(Commit({message}));

    // Reject historical bitmap64 DV even with bitmap32 options.
    ASSERT_NOK_WITH_MSG(Query(Id(2)), "bitmap64");
    ASSERT_NOK_WITH_MSG(PrepareDeletions(matches, last_query_snapshot_id_), "bitmap64");
}

TEST(AppendDvInvalidContextTest, NullContextsAndMissingSplit) {
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_NOK(FileIndexRead::Create("", "main", {}, nullptr, fs, GetDefaultPool()));
    auto file_system = std::make_shared<LocalFileSystem>();
    auto pool = GetDefaultPool();
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    ASSERT_NOK(DeletionVectorWrite::PrepareCommit("", "main", {}, std::nullopt, {}, file_system,
                                                  pool, executor));
    ASSERT_NOK(DeletionVectorWrite::PrepareCommit("table", "main", {}, std::nullopt, {}, nullptr,
                                                  pool, executor));
    ASSERT_NOK(DeletionVectorWrite::PrepareCommit("table", "main", {}, std::nullopt, {},
                                                  file_system, nullptr, executor));
    ASSERT_NOK(DeletionVectorWrite::PrepareCommit("table", "main", {}, std::nullopt, {},
                                                  file_system, pool, nullptr));
    ASSERT_NOK(DeletionVectorWrite::PrepareCommit("table", "../invalid", {}, std::nullopt, {},
                                                  file_system, pool, executor));
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    ASSERT_NOK(FileIndexRead::Create(directory->Str(), "main", {}, nullptr, fs, GetDefaultPool()));
}

INSTANTIATE_TEST_SUITE_P(BucketAndEncoding, AppendDvTest,
                         testing::Combine(testing::Values(-1, 2), testing::Values(0, 4096)));

}  // namespace paimon::test
