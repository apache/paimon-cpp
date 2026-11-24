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

#include "paimon/append/append_file_compaction_planner.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/executor.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/memory/bytes.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/deletion_vector_test_helper.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class AppendFileCompactionPlannerTest : public testing::Test,
                                        public testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        pool_ = GetDefaultPool();
        executor_ = CreateDefaultExecutor();
        auto schema =
            arrow::schema({arrow::field("p", arrow::int32()), arrow::field("id", arrow::int32()),
                           arrow::field("value", arrow::utf8())});
        std::map<std::string, std::string> options = {
            {Options::FILE_FORMAT, "parquet"},
            {Options::BUCKET, "2"},
            {Options::BUCKET_KEY, "id"},
            {Options::BUCKET_APPEND_ORDERED, "false"},
            {Options::COMPACTION_MIN_FILE_NUM, "100"},
            {Options::DELETION_VECTORS_ENABLED, "true"},
            {Options::DELETION_VECTOR_BITMAP64, GetParam() ? "true" : "false"}};
        ASSERT_OK_AND_ASSIGN(helper_, TestHelper::Create(dir_->Str(), schema, {"p"}, {}, options,
                                                         /*is_streaming_mode=*/true));
    }

    void TearDown() override {
        if (helper_) {
            EXPECT_OK(helper_->write_->Close());
        }
    }

    Result<std::shared_ptr<DataFileMeta>> NewFile(const std::string& name, int64_t rows,
                                                  int64_t sequence = 0) const {
        return DataFileMeta::ForAppend(name, /*file_size=*/100, rows, SimpleStats::EmptyStats(),
                                       sequence, sequence + rows - 1, /*schema_id=*/0,
                                       FileSource::Append(), std::nullopt, std::nullopt,
                                       std::nullopt, std::nullopt);
    }

    Status CommitFiles(std::vector<std::shared_ptr<DataFileMeta>> files, int32_t partition = 10,
                       int32_t bucket = 0) {
        auto message = std::make_shared<CommitMessageImpl>(
            BinaryRowGenerator::GenerateRow({partition}, pool_.get()), bucket, 2,
            DataIncrement(std::move(files), {}, {}), CompactIncrement({}, {}, {}));
        PAIMON_RETURN_NOT_OK(helper_->commit_->Commit({message}, next_commit_id_++));
        base_commit_message_ = message;
        return Status::OK();
    }

    Status CommitDeletions(const std::map<std::string, std::vector<int64_t>>& deletions) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CommitMessage> message,
                               DeletionVectorTestHelper::CreateDeletionVectorCommitMessage(
                                   helper_->fs_, helper_->table_path_, "parquet",
                                   base_commit_message_, deletions, pool_));
        return helper_->commit_->Commit({message}, next_commit_id_++);
    }

    Result<AppendFileCompactionPlan> Plan(
        const std::set<std::string>& files, std::optional<int64_t> snapshot = std::nullopt,
        const std::map<std::string, std::string>& partition = {{"p", "10"}}, int32_t bucket = 0,
        const std::map<std::string, std::string>& options = {}) const {
        return AppendFileCompactionPlanner::CreatePlan(helper_->table_path_, "", partition, bucket,
                                                       files, snapshot, options, helper_->fs_,
                                                       pool_, executor_);
    }

    void CheckMapping(const FileCompactionInfo& file,
                      const std::vector<std::optional<int64_t>>& expected) const {
        ASSERT_EQ(expected.size(), file.source_row_count);
        int64_t live_rows = 0;
        for (int64_t row = 0; row < file.source_row_count; ++row) {
            SCOPED_TRACE(row);
            ASSERT_OK_AND_ASSIGN(auto mapped, file.MapRowId(row));
            ASSERT_EQ(expected[row], mapped);
            live_rows += expected[row].has_value() ? 1 : 0;
        }
        ASSERT_EQ(live_rows, file.live_row_count);
        ASSERT_NOK_WITH_MSG(file.MapRowId(-1), "out of range");
        ASSERT_NOK_WITH_MSG(file.MapRowId(file.source_row_count), "out of range");
    }

    Result<std::shared_ptr<Bytes>> MakeIndexes(int32_t version) const {
        auto output =
            std::make_shared<ByteArrayOutputStream>(std::make_unique<MemorySegmentOutputStream>(
                MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexFormat::Writer> writer,
                               FileIndexFormat::CreateWriter(output, version, pool_));
        for (const auto& [column, indexes] : IndexPayloads()) {
            for (const auto& [type, payload] : indexes) {
                PAIMON_RETURN_NOT_OK(writer->WriteIndex(
                    column, type, [payload](OutputStream* stream) -> Result<bool> {
                        if (!payload) {
                            return false;
                        }
                        PAIMON_ASSIGN_OR_RAISE(int64_t written,
                                               stream->Write(payload->data(), payload->size()));
                        if (written != static_cast<int64_t>(payload->size())) {
                            return Status::IOError("incomplete index payload");
                        }
                        return true;
                    }));
            }
        }
        PAIMON_RETURN_NOT_OK(writer->Finish());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, output->Finish(pool_.get()));
        return bytes;
    }

    static std::map<std::string, std::map<std::string, std::optional<std::string>>>
    IndexPayloads() {
        return {{"id", {{"unknown-a", "first-payload"}, {"unknown-b", "second"}}},
                {"value", {{"empty", std::nullopt}, {"unknown-c", "third-payload"}}}};
    }

 protected:
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<Executor> executor_;
    std::unique_ptr<TestHelper> helper_;
    std::shared_ptr<CommitMessage> base_commit_message_;
    int64_t next_commit_id_ = 0;
};

INSTANTIATE_TEST_SUITE_P(BitmapEncoding, AppendFileCompactionPlannerTest, testing::Bool());

TEST_P(AppendFileCompactionPlannerTest, RowMappingsUseSelectedSnapshot) {
    ASSERT_OK_AND_ASSIGN(auto f1, NewFile("f1.parquet", /*rows=*/3, /*sequence=*/100));
    ASSERT_OK_AND_ASSIGN(auto f2, NewFile("f2.parquet", /*rows=*/2, /*sequence=*/0));
    ASSERT_OK(CommitFiles({f2, f1}));

    ASSERT_OK_AND_ASSIGN(auto before, Plan({"f2.parquet", "f1.parquet"}));
    ASSERT_EQ(1, before.snapshot_id);
    ASSERT_EQ(5, before.output_row_count);
    ASSERT_EQ(2, before.files.size());
    ASSERT_EQ("f1.parquet", before.files[0].file_name);
    ASSERT_EQ("f2.parquet", before.files[1].file_name);
    ASSERT_NO_FATAL_FAILURE(CheckMapping(before.files[0], {0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(CheckMapping(before.files[1], {3, 4}));
    for (const auto& file : before.files) {
        ASSERT_EQ(0, file.schema_id);
        ASSERT_FALSE(file.index_meta);
    }

    ASSERT_OK(CommitDeletions({{"f1.parquet", {0}}}));
    ASSERT_OK_AND_ASSIGN(auto latest, Plan({"f1.parquet", "f2.parquet"}));
    ASSERT_EQ(2, latest.snapshot_id);
    ASSERT_EQ(4, latest.output_row_count);
    ASSERT_EQ(2, latest.files.size());
    ASSERT_NO_FATAL_FAILURE(CheckMapping(latest.files[0], {std::nullopt, 0, 1}));
    ASSERT_NO_FATAL_FAILURE(CheckMapping(latest.files[1], {2, 3}));

    ASSERT_OK_AND_ASSIGN(auto historical, Plan({"f1.parquet", "f2.parquet"}, before.snapshot_id));
    ASSERT_EQ(before.snapshot_id, historical.snapshot_id);
    ASSERT_EQ(5, historical.output_row_count);
    ASSERT_EQ(2, historical.files.size());
    ASSERT_NO_FATAL_FAILURE(CheckMapping(historical.files[0], {0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(CheckMapping(historical.files[1], {3, 4}));
}

TEST_P(AppendFileCompactionPlannerTest, FullyDeletedFilesAndSeparatedSurvivingRanges) {
    ASSERT_OK_AND_ASSIGN(auto f1, NewFile("f1.parquet", /*rows=*/2));
    ASSERT_OK_AND_ASSIGN(auto f2, NewFile("f2.parquet", /*rows=*/6));
    ASSERT_OK_AND_ASSIGN(auto f3, NewFile("f3.parquet", /*rows=*/2));
    ASSERT_OK(CommitFiles({f1, f2, f3}));
    ASSERT_OK(CommitDeletions({{"f1.parquet", {0, 1}}, {"f2.parquet", {0, 2, 5}}}));

    ASSERT_OK_AND_ASSIGN(auto plan, Plan({"f1.parquet", "f2.parquet", "f3.parquet"}));
    ASSERT_EQ(5, plan.output_row_count);
    ASSERT_EQ(3, plan.files.size());
    ASSERT_EQ("f1.parquet", plan.files[0].file_name);
    ASSERT_EQ("f2.parquet", plan.files[1].file_name);
    ASSERT_EQ("f3.parquet", plan.files[2].file_name);
    ASSERT_TRUE(plan.files[0].row_id_mapping.empty());
    ASSERT_NO_FATAL_FAILURE(CheckMapping(plan.files[0], {std::nullopt, std::nullopt}));
    ASSERT_NO_FATAL_FAILURE(
        CheckMapping(plan.files[1], {std::nullopt, 0, std::nullopt, 1, 2, std::nullopt}));
    ASSERT_NO_FATAL_FAILURE(CheckMapping(plan.files[2], {3, 4}));

    ASSERT_OK_AND_ASSIGN(auto deleted, Plan({"f1.parquet"}));
    ASSERT_EQ(0, deleted.output_row_count);
    ASSERT_EQ(1, deleted.files.size());
    ASSERT_NO_FATAL_FAILURE(CheckMapping(deleted.files[0], {std::nullopt, std::nullopt}));

    ASSERT_OK_AND_ASSIGN(auto subset, Plan({"f3.parquet"}));
    ASSERT_EQ(2, subset.output_row_count);
    ASSERT_EQ(1, subset.files.size());
    ASSERT_NO_FATAL_FAILURE(CheckMapping(subset.files[0], {0, 1}));
}

TEST_P(AppendFileCompactionPlannerTest, FiltersPartitionAndBucket) {
    ASSERT_OK_AND_ASSIGN(auto selected, NewFile("same.parquet", /*rows=*/3));
    ASSERT_OK_AND_ASSIGN(auto other_partition, NewFile("same.parquet", /*rows=*/7));
    ASSERT_OK_AND_ASSIGN(auto other_bucket, NewFile("same.parquet", /*rows=*/9));
    ASSERT_OK_AND_ASSIGN(auto outside, NewFile("outside.parquet", /*rows=*/1));
    ASSERT_OK(CommitFiles({selected}));
    ASSERT_OK(CommitFiles({other_partition, outside}, 20, 0));
    ASSERT_OK(CommitFiles({other_bucket}, 10, 1));

    ASSERT_OK_AND_ASSIGN(auto plan, Plan({"same.parquet"}));
    ASSERT_EQ(1, plan.files.size());
    ASSERT_EQ(3, plan.output_row_count);
    ASSERT_NO_FATAL_FAILURE(CheckMapping(plan.files[0], {0, 1, 2}));
    ASSERT_OK_AND_ASSIGN(auto partition_plan,
                         Plan({"same.parquet"}, std::nullopt, {{"p", "20"}}, 0));
    ASSERT_EQ(7, partition_plan.output_row_count);
    ASSERT_OK_AND_ASSIGN(auto bucket_plan, Plan({"same.parquet"}, std::nullopt, {{"p", "10"}}, 1));
    ASSERT_EQ(9, bucket_plan.output_row_count);
    ASSERT_NOK_WITH_MSG(Plan({"outside.parquet"}), "not active");
    ASSERT_NOK_WITH_MSG(Plan({"same.parquet", "missing.parquet"}), "not active");
}

TEST_P(AppendFileCompactionPlannerTest, RejectsInvalidInputs) {
    ASSERT_NOK_WITH_MSG(Plan({"f1.parquet"}), "committed snapshot");
    ASSERT_NOK_WITH_MSG(Plan({}), "at least one input file");
    ASSERT_NOK(Plan({"../f1.parquet"}));
    ASSERT_NOK(Plan({"dir/f1.parquet"}));
    ASSERT_NOK_WITH_MSG(Plan({"f1.parquet"}, 0), "snapshot id");
    ASSERT_NOK_WITH_MSG(
        AppendFileCompactionPlanner::CreatePlan("", "", {}, 0, {"f1.parquet"}, std::nullopt, {},
                                                nullptr, nullptr, nullptr),
        "table path is empty");

    ASSERT_OK_AND_ASSIGN(auto file, NewFile("f1.parquet", /*rows=*/1));
    ASSERT_OK(CommitFiles({file}));
    for (int32_t bucket : {-1, 2}) {
        ASSERT_NOK_WITH_MSG(Plan({"f1.parquet"}, std::nullopt, {{"p", "10"}}, bucket),
                            "bucket is out of range");
    }
    ASSERT_NOK_WITH_MSG(Plan({"f1.parquet"}, std::nullopt, {}), "partition keys");
    ASSERT_NOK(Plan({"f1.parquet"}, std::nullopt, {{"unknown", "10"}}));
    ASSERT_NOK(Plan({"f1.parquet"}, 999));
    for (const auto& override_options : std::vector<std::map<std::string, std::string>>{
             {{Options::BUCKET, "-1"}},
             {{Options::REALTIME_ENABLED, "true"}},
             {{Options::DATA_EVOLUTION_ENABLED, "true"}}}) {
        ASSERT_NOK(Plan({"f1.parquet"}, std::nullopt, {{"p", "10"}}, 0, override_options));
    }
    ASSERT_OK_AND_ASSIGN(
        auto defaults, AppendFileCompactionPlanner::CreatePlan(
                           helper_->table_path_, "", {{"p", "10"}}, 0, {"f1.parquet"}, std::nullopt,
                           {}, nullptr, nullptr, nullptr));
    ASSERT_EQ(1, defaults.output_row_count);
}

TEST_P(AppendFileCompactionPlannerTest, ReadsEmbeddedAndExternalIndexRanges) {
    for (int32_t version : {FileIndexFormat::V_1, FileIndexFormat::V_2}) {
        for (bool embedded : {false, true}) {
            SCOPED_TRACE(version);
            SCOPED_TRACE(embedded);
            std::string name = "indexed-" + std::to_string(version) +
                               (embedded ? "-embedded.parquet" : "-external.parquet");
            ASSERT_OK_AND_ASSIGN(auto file, NewFile(name, /*rows=*/3));
            ASSERT_OK_AND_ASSIGN(auto bytes, MakeIndexes(version));
            std::string container(bytes->data(), bytes->size());
            std::string index_path;
            if (embedded) {
                file->embedded_index = bytes;
            } else {
                std::string external_dir = PathUtil::JoinPath(dir_->Str(), "external");
                ASSERT_OK(helper_->fs_->Mkdirs(external_dir));
                file->external_path = PathUtil::JoinPath(external_dir, name);
                file->extra_files = {name + ".index"};
                index_path = PathUtil::JoinPath(external_dir, name + ".index");
                ASSERT_OK(helper_->fs_->WriteFile(index_path, container, false));
            }
            ASSERT_OK(CommitFiles({file}));
            ASSERT_OK_AND_ASSIGN(auto plan, Plan({name}));
            ASSERT_EQ(3, plan.output_row_count);
            ASSERT_EQ(1, plan.files.size());
            ASSERT_TRUE(plan.files[0].index_meta);
            const auto& meta = plan.files[0].index_meta.value();
            ASSERT_EQ(embedded, meta.embedded_index != nullptr);
            ASSERT_EQ(!embedded, meta.index_file_path.has_value());
            if (embedded) {
                ASSERT_EQ(container,
                          std::string(meta.embedded_index->data(), meta.embedded_index->size()));
            } else {
                ASSERT_EQ(index_path, meta.index_file_path.value());
            }
            ASSERT_EQ(2, meta.indexes.size());
            for (const auto& [column, indexes] : IndexPayloads()) {
                ASSERT_EQ(1, meta.indexes.count(column));
                ASSERT_EQ(indexes.size(), meta.indexes.at(column).size());
                for (const auto& [type, payload] : indexes) {
                    ASSERT_EQ(1, meta.indexes.at(column).count(type));
                    const auto& range = meta.indexes.at(column).at(type);
                    if (!payload) {
                        ASSERT_EQ(-1, range.offset);
                        ASSERT_EQ(0, range.length);
                    } else {
                        ASSERT_GE(range.offset, 0);
                        ASSERT_EQ(payload->size(), range.length);
                        ASSERT_LE(range.offset + range.length, container.size());
                        ASSERT_EQ(payload.value(), container.substr(range.offset, range.length));
                    }
                }
            }
        }
    }
}

TEST_P(AppendFileCompactionPlannerTest, RejectsInvalidDeletionPositions) {
    ASSERT_OK_AND_ASSIGN(auto file, NewFile("f1.parquet", /*rows=*/3));
    ASSERT_OK(CommitFiles({file}));
    ASSERT_OK(CommitDeletions({{"f1.parquet", {3}}}));
    ASSERT_NOK_WITH_MSG(Plan({"f1.parquet"}), "invalid deletion vector positions");
}

TEST_P(AppendFileCompactionPlannerTest, PreservesRowIdsAbove32Bits) {
    constexpr int64_t kLargeRowId = int64_t{1} << 32;
    int64_t deleted_row = GetParam() ? kLargeRowId : 1;
    ASSERT_OK_AND_ASSIGN(auto f1, NewFile("f1.parquet", /*rows=*/kLargeRowId + 3));
    ASSERT_OK_AND_ASSIGN(auto f2, NewFile("f2.parquet", /*rows=*/1));
    ASSERT_OK(CommitFiles({f1, f2}));
    ASSERT_OK(CommitDeletions({{"f1.parquet", {deleted_row}}}));
    ASSERT_OK_AND_ASSIGN(auto plan, Plan({"f1.parquet", "f2.parquet"}));
    ASSERT_EQ(kLargeRowId + 3, plan.output_row_count);
    ASSERT_EQ(2, plan.files.size());
    ASSERT_OK_AND_ASSIGN(auto before, plan.files[0].MapRowId(deleted_row - 1));
    ASSERT_EQ(std::optional<int64_t>(deleted_row - 1), before);
    ASSERT_OK_AND_ASSIGN(auto deleted, plan.files[0].MapRowId(deleted_row));
    ASSERT_FALSE(deleted);
    ASSERT_OK_AND_ASSIGN(auto after, plan.files[0].MapRowId(deleted_row + 1));
    ASSERT_EQ(std::optional<int64_t>(deleted_row), after);
    ASSERT_OK_AND_ASSIGN(auto last, plan.files[0].MapRowId(kLargeRowId + 2));
    ASSERT_EQ(std::optional<int64_t>(kLargeRowId + 1), last);
    ASSERT_OK_AND_ASSIGN(auto next_file, plan.files[1].MapRowId(0));
    ASSERT_EQ(std::optional<int64_t>(kLargeRowId + 2), next_file);
}

TEST_P(AppendFileCompactionPlannerTest, RejectsMalformedIndexContainer) {
    ASSERT_OK_AND_ASSIGN(auto file, NewFile("f1.parquet", /*rows=*/3));
    file->embedded_index = std::make_shared<Bytes>("invalid index container", pool_.get());
    ASSERT_OK(CommitFiles({file}));
    ASSERT_NOK(Plan({"f1.parquet"}));
}

}  // namespace paimon::test
