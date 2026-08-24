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

#include "paimon/core/append/data_evolution_compact_global_index_dropper.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

std::shared_ptr<DataFileMeta> CreateFile(const std::string& file_name,
                                         const std::optional<int64_t>& first_row_id) {
    return std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/1024, /*row_count=*/4, DataFileMeta::EmptyMinKey(),
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
    const std::vector<std::shared_ptr<DataFileMeta>>& before,
    const std::vector<std::shared_ptr<DataFileMeta>>& after) {
    std::vector<std::shared_ptr<DataFileMeta>> before_copy = before;
    std::vector<std::shared_ptr<DataFileMeta>> after_copy = after;
    CompactIncrement compact_increment(std::move(before_copy), std::move(after_copy),
                                       /*changelog_files=*/{}, /*new_index_files=*/{},
                                       /*deleted_index_files=*/{});
    DataIncrement data_increment(/*new_files=*/{}, /*deleted_files=*/{}, /*changelog_files=*/{});
    return std::make_shared<CommitMessageImpl>(EmptyPartition(), /*bucket=*/0,
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

}  // namespace

TEST(DataEvolutionCompactGlobalIndexDropperTest, DropsNothingForANormalCompaction) {
    // A normal data-evolution compaction preserves row ids, which is exactly what keeps the
    // global indexes valid. Dropping them would throw away work for nothing, so the detection
    // has to say no here — and say it before touching the index handler, which is why passing a
    // null one is safe.
    std::vector<std::shared_ptr<CommitMessage>> messages = {
        CompactMessage({CreateFile("before.parquet", /*first_row_id=*/0)},
                       {CreateFile("after.parquet", /*first_row_id=*/0)})};

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> dropped,
                         DataEvolutionCompactGlobalIndexDropper::DropGlobalIndexes(
                             messages, MakeSnapshot(), /*index_file_handler=*/nullptr));
    ASSERT_TRUE(dropped.empty());
}

TEST(DataEvolutionCompactGlobalIndexDropperTest, DropsNothingWithoutReplacedNormalFiles) {
    // An index-only or blob-only message replaces no rows, so it invalidates no index however
    // its outputs look.
    std::vector<std::shared_ptr<CommitMessage>> messages = {
        CompactMessage(/*before=*/{}, /*after=*/{}),
        CompactMessage({CreateFile("before-f2-0.blob", /*first_row_id=*/0)},
                       {CreateFile("after-f2-0.blob", /*first_row_id=*/std::nullopt)})};

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> dropped,
                         DataEvolutionCompactGlobalIndexDropper::DropGlobalIndexes(
                             messages, MakeSnapshot(), /*index_file_handler=*/nullptr));
    ASSERT_TRUE(dropped.empty());
}

}  // namespace paimon::test
