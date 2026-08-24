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

#include "paimon/core/append/data_evolution_compact_deletion_vector_rewriter.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
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

std::shared_ptr<CommitMessage> CompactMessage(
    std::vector<std::shared_ptr<DataFileMeta>> before,
    std::vector<std::shared_ptr<DataFileMeta>> after, int32_t bucket = 0,
    std::vector<std::shared_ptr<IndexFileMeta>> new_index_files = {}) {
    CompactIncrement compact_increment(std::move(before), std::move(after), /*changelog_files=*/{},
                                       std::move(new_index_files), /*deleted_index_files=*/{});
    DataIncrement data_increment(/*new_files=*/{}, /*deleted_files=*/{}, /*changelog_files=*/{});
    return std::make_shared<CommitMessageImpl>(EmptyPartition(), bucket,
                                               /*total_buckets=*/std::nullopt, data_increment,
                                               compact_increment);
}

Snapshot MakeSnapshot() {
    return Snapshot(
        /*id=*/1, /*schema_id=*/0, /*base_manifest_list=*/"base",
        /*base_manifest_list_size=*/std::nullopt, /*delta_manifest_list=*/"delta",
        /*delta_manifest_list_size=*/std::nullopt, /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt, /*index_manifest=*/std::nullopt,
        /*commit_user=*/"test-user", /*commit_identifier=*/1, Snapshot::CommitKind::Compact(),
        /*time_millis=*/0, /*total_record_count=*/0, /*delta_record_count=*/0,
        /*changelog_record_count=*/std::nullopt, /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt, /*properties=*/std::nullopt, /*next_row_id=*/std::nullopt);
}

/// Every case below stops before the rewriter reads anything, which is why a null index handler
/// is enough: the validation and the two short circuits all run on the messages alone. The
/// paths that do read are covered end to end in `test/inte/data_evolution_table_test.cpp`.
Result<std::vector<std::shared_ptr<CommitMessage>>> Rewrite(
    const std::vector<std::shared_ptr<CommitMessage>>& messages, const CoreOptions& core_options) {
    return DataEvolutionCompactDeletionVectorRewriter::RewriteDeletionVectors(
        messages, MakeSnapshot(), core_options, /*index_file_handler=*/nullptr);
}

Result<CoreOptions> DeletionVectorOptions(bool enabled) {
    std::map<std::string, std::string> options;
    if (enabled) {
        options.emplace(Options::DELETION_VECTORS_ENABLED, "true");
    }
    return CoreOptions::FromMap(options);
}

}  // namespace

TEST(DataEvolutionCompactDeletionVectorRewriterTest, ShortCircuitsWithoutDeletionVectors) {
    // A table without deletion vectors has nothing to move, and the guard runs before anything
    // is read so a caller that forgets to pre-check stays correct rather than failing.
    ASSERT_OK_AND_ASSIGN(CoreOptions disabled, DeletionVectorOptions(/*enabled=*/false));
    std::vector<std::shared_ptr<CommitMessage>> messages = {
        CompactMessage({CreateFile("before.parquet", /*first_row_id=*/0, /*row_count=*/2)},
                       {CreateFile("after.parquet", /*first_row_id=*/0, /*row_count=*/2)})};
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> result,
                         Rewrite(messages, disabled));
    ASSERT_TRUE(result.empty());

    ASSERT_OK_AND_ASSIGN(CoreOptions enabled, DeletionVectorOptions(/*enabled=*/true));
    ASSERT_OK_AND_ASSIGN(result, Rewrite(/*messages=*/{}, enabled));
    ASSERT_TRUE(result.empty());
}

TEST(DataEvolutionCompactDeletionVectorRewriterTest, RejectsMessagesThatAlreadyChangedTheIndex) {
    // The rewriter owns every index change of the round; one produced earlier would be dropped
    // by the index-only messages it builds, so it refuses instead of losing it.
    ASSERT_OK_AND_ASSIGN(CoreOptions enabled, DeletionVectorOptions(/*enabled=*/true));
    auto index_file = std::make_shared<IndexFileMeta>(
        "DELETION_VECTORS", "index-0", /*file_size=*/10, /*row_count=*/1,
        /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt);
    std::vector<std::shared_ptr<CommitMessage>> messages = {
        CompactMessage({CreateFile("before.parquet", /*first_row_id=*/0, /*row_count=*/2)},
                       {CreateFile("after.parquet", /*first_row_id=*/0, /*row_count=*/2)},
                       /*bucket=*/0, {index_file})};
    ASSERT_NOK_WITH_MSG(Rewrite(messages, enabled),
                        "should not produce index changes before the deletion vector rewrite");
}

TEST(DataEvolutionCompactDeletionVectorRewriterTest, RejectsABucketedMessage) {
    // Data-evolution compaction only ever writes the unaware bucket, and the rewritten index
    // files have to land in the same one.
    ASSERT_OK_AND_ASSIGN(CoreOptions enabled, DeletionVectorOptions(/*enabled=*/true));
    std::vector<std::shared_ptr<CommitMessage>> messages = {
        CompactMessage({CreateFile("before.parquet", /*first_row_id=*/0, /*row_count=*/2)},
                       {CreateFile("after.parquet", /*first_row_id=*/0, /*row_count=*/2)},
                       /*bucket=*/3)};
    ASSERT_NOK_WITH_MSG(Rewrite(messages, enabled), "unaware-bucket commit messages");
}

TEST(DataEvolutionCompactDeletionVectorRewriterTest, RejectsARewriteThatMovedItsRows) {
    // A task that keeps its row ids must cover exactly the rows it replaced. A different range
    // would move the deletions onto rows that are not the ones they were written for.
    ASSERT_OK_AND_ASSIGN(CoreOptions enabled, DeletionVectorOptions(/*enabled=*/true));
    std::vector<std::shared_ptr<CommitMessage>> messages = {
        CompactMessage({CreateFile("before.parquet", /*first_row_id=*/0, /*row_count=*/4)},
                       {CreateFile("after.parquet", /*first_row_id=*/8, /*row_count=*/4)})};
    ASSERT_NOK_WITH_MSG(Rewrite(messages, enabled), "must keep the same row id range");
}

TEST(DataEvolutionCompactDeletionVectorRewriterTest, RejectsATaskWithSeveralNormalOutputs) {
    // A normal task merges its row range into a single file, and the moved vector is keyed by
    // that file. Two outputs would leave the rewriter without one file to key the deletions of
    // the range by, so the shape is refused rather than guessed at.
    ASSERT_OK_AND_ASSIGN(CoreOptions enabled, DeletionVectorOptions(/*enabled=*/true));
    std::vector<std::shared_ptr<CommitMessage>> messages = {
        CompactMessage({CreateFile("before.parquet", /*first_row_id=*/0, /*row_count=*/4)},
                       {CreateFile("after-0.parquet", /*first_row_id=*/0, /*row_count=*/2),
                        CreateFile("after-1.parquet", /*first_row_id=*/2, /*row_count=*/2)})};
    ASSERT_NOK_WITH_MSG(Rewrite(messages, enabled), "should produce exactly one normal file");
}

TEST(DataEvolutionCompactDeletionVectorRewriterTest, IgnoresMessagesCarryingNoNormalFile) {
    // A blob-only message replaces no rows whose deletions could move, so it never reaches the
    // index at all — which is what lets this run with no index handler.
    ASSERT_OK_AND_ASSIGN(CoreOptions enabled, DeletionVectorOptions(/*enabled=*/true));
    std::vector<std::shared_ptr<CommitMessage>> messages = {
        CompactMessage({CreateFile("before-f2-0.blob", /*first_row_id=*/0, /*row_count=*/2)},
                       {CreateFile("after-f2-0.blob", /*first_row_id=*/0, /*row_count=*/2)})};
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> result,
                         Rewrite(messages, enabled));
    ASSERT_TRUE(result.empty());
}

}  // namespace paimon::test
