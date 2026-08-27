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

#include "paimon/core/index/pksorted/pk_sorted_bucket_index_state.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_file.h"
#include "paimon/core/index/pk/primary_key_index_source_meta.h"
#include "paimon/core/index/pksorted/pk_sorted_index_group.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class PkSortedBucketIndexStateTest : public ::testing::Test {
 public:
    std::shared_ptr<DataFileMeta> MakeDataFile(const std::string& file_name, int64_t row_count,
                                               int32_t level,
                                               const std::optional<FileSource>& file_source) const {
        return std::make_shared<DataFileMeta>(
            file_name, /*file_size=*/1024, row_count, DataFileMeta::EmptyMinKey(),
            DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/0, level,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0), /*delete_row_count=*/std::nullopt,
            /*embedded_index=*/nullptr, file_source, /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
    }

    /// Builds a payload whose source metadata lists the given sources in the given order.
    std::shared_ptr<IndexFileMeta> MakeNamedPayload(
        const std::string& payload_name, int32_t field_id, const std::string& index_type,
        int32_t data_level, const std::vector<PrimaryKeyIndexSourceFile>& sources,
        int64_t total_row_count, int64_t row_range_start, int64_t row_range_end) const {
        EXPECT_OK_AND_ASSIGN(PrimaryKeyIndexSourceMeta source_meta,
                             PrimaryKeyIndexSourceMeta::Create(data_level, sources));
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<Bytes> source_meta_bytes,
                             source_meta.Serialize(pool_));
        return MakePayloadWithSourceMetaBytes(payload_name, field_id, index_type, total_row_count,
                                              row_range_start, row_range_end, source_meta_bytes);
    }

    std::shared_ptr<IndexFileMeta> MakePayload(
        int32_t field_id, const std::string& index_type, int32_t data_level,
        const std::vector<PrimaryKeyIndexSourceFile>& sources, int64_t total_row_count,
        int64_t row_range_start, int64_t row_range_end) const {
        return MakeNamedPayload("payload.index", field_id, index_type, data_level, sources,
                                total_row_count, row_range_start, row_range_end);
    }

    std::shared_ptr<IndexFileMeta> MakeNamedPayload(
        const std::string& payload_name, int32_t field_id, const std::string& index_type,
        int32_t data_level, const std::vector<PrimaryKeyIndexSourceFile>& sources,
        int64_t total_row_count) const {
        return MakeNamedPayload(payload_name, field_id, index_type, data_level, sources,
                                total_row_count, /*row_range_start=*/0,
                                /*row_range_end=*/total_row_count - 1);
    }

    std::shared_ptr<IndexFileMeta> MakePayload(
        int32_t field_id, const std::string& index_type, int32_t data_level,
        const std::vector<PrimaryKeyIndexSourceFile>& sources, int64_t total_row_count) const {
        return MakePayload(field_id, index_type, data_level, sources, total_row_count,
                           /*row_range_start=*/0, /*row_range_end=*/total_row_count - 1);
    }

    std::shared_ptr<IndexFileMeta> MakePayloadWithSourceMetaBytes(
        const std::string& payload_name, int32_t field_id, const std::string& index_type,
        int64_t total_row_count, int64_t row_range_start, int64_t row_range_end,
        const std::shared_ptr<Bytes>& source_meta_bytes) const {
        GlobalIndexMeta global_index_meta(row_range_start, row_range_end, field_id,
                                          /*extra_field_ids=*/std::nullopt,
                                          /*index_meta=*/nullptr, source_meta_bytes);
        return std::make_shared<IndexFileMeta>(index_type, payload_name,
                                               /*file_size=*/2048, total_row_count,
                                               /*dv_ranges=*/std::nullopt,
                                               /*external_path=*/std::nullopt, global_index_meta);
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(PkSortedBucketIndexStateTest, BuildsGroupWhenPayloadMatchesLevelSources) {
    // Files are handed over unsorted; the expected source order is sorted by file name.
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("b", 200, 5, FileSource::Compact()),
        MakeDataFile("a", 100, 5, FileSource::Compact())};
    std::vector<PrimaryKeyIndexSourceFile> expected_sources = {{"a", 100}, {"b", 200}};
    std::shared_ptr<IndexFileMeta> payload =
        MakePayload(/*field_id=*/7, "btree", /*data_level=*/5, expected_sources,
                    /*total_row_count=*/300);
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {payload});
    ASSERT_EQ(1, state.Groups().size());
    const std::shared_ptr<PkSortedIndexGroup>& group = state.Groups()[0];
    ASSERT_EQ(5, group->DataLevel());
    ASSERT_EQ(300, group->TotalSourceRowCount());
    ASSERT_EQ(expected_sources, group->SourceFiles());
    ASSERT_EQ(payload, group->Payload());
    ASSERT_EQ(expected_sources, state.CoveredSourceFiles());
    ASSERT_TRUE(state.UncoveredSourceFiles().empty());
    ASSERT_TRUE(state.RejectedPayloads().empty());
}

TEST_F(PkSortedBucketIndexStateTest, OnlyCompactedFilesAboveLevelZeroAreSources) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("level0", 10, 0, FileSource::Compact()),
        MakeDataFile("appended", 20, 5, FileSource::Append()),
        MakeDataFile("unknown_source", 30, 5, std::nullopt),
        MakeDataFile("c", 40, 5, FileSource::Compact())};
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {});
    ASSERT_TRUE(state.Groups().empty());
    ASSERT_TRUE(state.CoveredSourceFiles().empty());
    std::vector<PrimaryKeyIndexSourceFile> expected_uncovered = {{"c", 40}};
    ASSERT_EQ(expected_uncovered, state.UncoveredSourceFiles());
    ASSERT_TRUE(state.RejectedPayloads().empty());
}

TEST_F(PkSortedBucketIndexStateTest, RejectsPayloadWithMisorderedSources) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> payload =
        MakePayload(7, "btree", 5, {{"b", 200}, {"a", 100}}, 300);
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {payload});
    ASSERT_TRUE(state.Groups().empty());
    ASSERT_EQ(1, state.RejectedPayloads().size());
    ASSERT_EQ(payload, state.RejectedPayloads()[0]);
    ASSERT_TRUE(state.CoveredSourceFiles().empty());
    std::vector<PrimaryKeyIndexSourceFile> expected_uncovered = {{"a", 100}, {"b", 200}};
    ASSERT_EQ(expected_uncovered, state.UncoveredSourceFiles());
}

TEST_F(PkSortedBucketIndexStateTest, RejectsPayloadWithMismatchedSourceRowCount) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> payload =
        MakePayload(7, "btree", 5, {{"a", 100}, {"b", 201}}, 301);
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {payload});
    ASSERT_TRUE(state.Groups().empty());
    ASSERT_EQ(1, state.RejectedPayloads().size());
    ASSERT_EQ(2, state.UncoveredSourceFiles().size());
}

TEST_F(PkSortedBucketIndexStateTest, AcceptsActiveSubsetAndLeavesOtherFilesUncovered) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> subset_payload = MakePayload(7, "btree", 5, {{"a", 100}}, 100);
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {subset_payload});
    ASSERT_EQ(1, state.Groups().size());
    ASSERT_EQ(subset_payload, state.Groups()[0]->Payload());
    ASSERT_EQ((std::vector<PrimaryKeyIndexSourceFile>{{"a", 100}}), state.CoveredSourceFiles());
    ASSERT_EQ((std::vector<PrimaryKeyIndexSourceFile>{{"b", 200}}), state.UncoveredSourceFiles());
    ASSERT_TRUE(state.RejectedPayloads().empty());
}

TEST_F(PkSortedBucketIndexStateTest, AcceptsDisjointPayloadGroupsAtSameLevel) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact()),
        MakeDataFile("c", 50, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> first_payload =
        MakeNamedPayload("first.index", 7, "btree", 5, {{"a", 100}, {"b", 200}}, 300);
    std::shared_ptr<IndexFileMeta> second_payload =
        MakeNamedPayload("second.index", 7, "btree", 5, {{"c", 50}}, 50);
    PkSortedBucketIndexState state = PkSortedBucketIndexState::FromActiveDataFiles(
        7, "btree", data_files, {second_payload, first_payload});
    ASSERT_EQ(2, state.Groups().size());
    ASSERT_EQ(first_payload, state.Groups()[0]->Payload());
    ASSERT_EQ(second_payload, state.Groups()[1]->Payload());
    ASSERT_EQ((std::vector<PrimaryKeyIndexSourceFile>{{"a", 100}, {"b", 200}, {"c", 50}}),
              state.CoveredSourceFiles());
    ASSERT_TRUE(state.UncoveredSourceFiles().empty());
    ASSERT_TRUE(state.RejectedPayloads().empty());
}

TEST_F(PkSortedBucketIndexStateTest, RetainsRetiredSourcesButCoversOnlyActiveIntersection) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> mixed_payload =
        MakeNamedPayload("mixed.index", 7, "btree", 5, {{"a", 100}, {"retired", 50}}, 150);
    std::shared_ptr<IndexFileMeta> valid_payload =
        MakeNamedPayload("valid.index", 7, "btree", 5, {{"b", 200}}, 200);
    PkSortedBucketIndexState state = PkSortedBucketIndexState::FromActiveDataFiles(
        7, "btree", data_files, {mixed_payload, valid_payload});
    ASSERT_EQ(2, state.Groups().size());
    ASSERT_EQ(mixed_payload, state.Groups()[0]->Payload());
    ASSERT_EQ((std::vector<PrimaryKeyIndexSourceFile>{{"a", 100}, {"retired", 50}}),
              state.Groups()[0]->SourceFiles());
    ASSERT_EQ(valid_payload, state.Groups()[1]->Payload());
    ASSERT_EQ((std::vector<PrimaryKeyIndexSourceFile>{{"a", 100}, {"b", 200}}),
              state.CoveredSourceFiles());
    ASSERT_TRUE(state.UncoveredSourceFiles().empty());
    ASSERT_TRUE(state.RejectedPayloads().empty());
}

TEST_F(PkSortedBucketIndexStateTest, RejectsPayloadWhoseSourcesAreAllRetired) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("active", 100, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> retired_payload =
        MakePayload(7, "btree", 5, {{"retired", 50}}, 50);
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {retired_payload});
    ASSERT_TRUE(state.Groups().empty());
    ASSERT_TRUE(state.CoveredSourceFiles().empty());
    ASSERT_EQ((std::vector<PrimaryKeyIndexSourceFile>{{"active", 100}}),
              state.UncoveredSourceFiles());
    ASSERT_EQ((std::vector<std::shared_ptr<IndexFileMeta>>{retired_payload}),
              state.RejectedPayloads());
}

TEST_F(PkSortedBucketIndexStateTest, RetiredSourceOverlapDoesNotConflict) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("b-active", 100, 5, FileSource::Compact()),
        MakeDataFile("c-active", 200, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> left_payload =
        MakeNamedPayload("left.index", 7, "btree", 5, {{"a-retired", 50}, {"b-active", 100}}, 150);
    std::shared_ptr<IndexFileMeta> right_payload =
        MakeNamedPayload("right.index", 7, "btree", 5, {{"a-retired", 50}, {"c-active", 200}}, 250);
    PkSortedBucketIndexState state = PkSortedBucketIndexState::FromActiveDataFiles(
        7, "btree", data_files, {left_payload, right_payload});
    ASSERT_EQ(2, state.Groups().size());
    ASSERT_EQ((std::vector<PrimaryKeyIndexSourceFile>{{"b-active", 100}, {"c-active", 200}}),
              state.CoveredSourceFiles());
    ASSERT_TRUE(state.UncoveredSourceFiles().empty());
    ASSERT_TRUE(state.RejectedPayloads().empty());
}

TEST_F(PkSortedBucketIndexStateTest, RejectsOverlappingGroupsButKeepsDisjointGroup) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact()),
        MakeDataFile("c", 50, 5, FileSource::Compact()),
        MakeDataFile("d", 75, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> left_payload =
        MakeNamedPayload("left.index", 7, "btree", 5, {{"a", 100}, {"b", 200}}, 300);
    std::shared_ptr<IndexFileMeta> right_payload =
        MakeNamedPayload("right.index", 7, "btree", 5, {{"b", 200}, {"c", 50}}, 250);
    std::shared_ptr<IndexFileMeta> disjoint_payload =
        MakeNamedPayload("disjoint.index", 7, "btree", 5, {{"d", 75}}, 75);
    PkSortedBucketIndexState state = PkSortedBucketIndexState::FromActiveDataFiles(
        7, "btree", data_files, {left_payload, disjoint_payload, right_payload});
    ASSERT_EQ(1, state.Groups().size());
    ASSERT_EQ(disjoint_payload, state.Groups()[0]->Payload());
    ASSERT_EQ((std::vector<PrimaryKeyIndexSourceFile>{{"d", 75}}), state.CoveredSourceFiles());
    ASSERT_EQ((std::vector<PrimaryKeyIndexSourceFile>{{"a", 100}, {"b", 200}, {"c", 50}}),
              state.UncoveredSourceFiles());
    ASSERT_EQ(2, state.RejectedPayloads().size());
}

TEST_F(PkSortedBucketIndexStateTest, RejectsPayloadsClaimingTheSameActiveSources) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact())};
    std::vector<PrimaryKeyIndexSourceFile> sources = {{"a", 100}, {"b", 200}};
    std::shared_ptr<IndexFileMeta> first_payload = MakePayload(7, "btree", 5, sources, 300);
    std::shared_ptr<IndexFileMeta> second_payload = MakePayload(7, "btree", 5, sources, 300);
    PkSortedBucketIndexState state = PkSortedBucketIndexState::FromActiveDataFiles(
        7, "btree", data_files, {first_payload, second_payload});
    ASSERT_TRUE(state.Groups().empty());
    ASSERT_EQ(2, state.RejectedPayloads().size());
    ASSERT_TRUE(state.CoveredSourceFiles().empty());
    ASSERT_EQ(sources, state.UncoveredSourceFiles());
}

TEST_F(PkSortedBucketIndexStateTest, RejectsPayloadWithWrongFieldId) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> payload =
        MakePayload(/*field_id=*/8, "btree", 5, {{"a", 100}}, 100);
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {payload});
    ASSERT_TRUE(state.Groups().empty());
    ASSERT_EQ(1, state.RejectedPayloads().size());
    ASSERT_EQ(1, state.UncoveredSourceFiles().size());
}

TEST_F(PkSortedBucketIndexStateTest, RejectsPayloadWithWrongIndexType) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> payload = MakePayload(7, "bitmap", 5, {{"a", 100}}, 100);
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {payload});
    ASSERT_TRUE(state.Groups().empty());
    ASSERT_EQ(1, state.RejectedPayloads().size());
    ASSERT_EQ(1, state.UncoveredSourceFiles().size());
}

TEST_F(PkSortedBucketIndexStateTest, WrongCandidateDoesNotMaskValidPayload) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> valid_payload = MakePayload(7, "btree", 5, {{"a", 100}}, 100);
    std::shared_ptr<IndexFileMeta> wrong_payload =
        MakePayload(/*field_id=*/8, "btree", 5, {{"a", 100}}, 100);
    PkSortedBucketIndexState state = PkSortedBucketIndexState::FromActiveDataFiles(
        7, "btree", data_files, {valid_payload, wrong_payload});
    ASSERT_EQ(1, state.Groups().size());
    ASSERT_EQ(valid_payload, state.Groups()[0]->Payload());
    ASSERT_EQ(1, state.RejectedPayloads().size());
    ASSERT_EQ(wrong_payload, state.RejectedPayloads()[0]);
    ASSERT_TRUE(state.UncoveredSourceFiles().empty());
}

TEST_F(PkSortedBucketIndexStateTest, RejectsPayloadWithWrongRowRange) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact())};
    std::vector<PrimaryKeyIndexSourceFile> sources = {{"a", 100}, {"b", 200}};
    // The exclusive end row 300 violates the required inclusive range [0, 299].
    std::shared_ptr<IndexFileMeta> payload = MakePayload(7, "btree", 5, sources, 300,
                                                         /*row_range_start=*/0,
                                                         /*row_range_end=*/300);
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {payload});
    ASSERT_TRUE(state.Groups().empty());
    ASSERT_EQ(1, state.RejectedPayloads().size());
    ASSERT_EQ(sources, state.UncoveredSourceFiles());
}

TEST_F(PkSortedBucketIndexStateTest, RejectsPayloadWithWrongRowCount) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact())};
    std::vector<PrimaryKeyIndexSourceFile> sources = {{"a", 100}, {"b", 200}};
    // The row range is valid but the payload row count 299 differs from the 300 source rows.
    std::shared_ptr<IndexFileMeta> payload = MakePayload(7, "btree", 5, sources,
                                                         /*total_row_count=*/299,
                                                         /*row_range_start=*/0,
                                                         /*row_range_end=*/299);
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {payload});
    ASSERT_TRUE(state.Groups().empty());
    ASSERT_EQ(1, state.RejectedPayloads().size());
    ASSERT_EQ(sources, state.UncoveredSourceFiles());
}

TEST_F(PkSortedBucketIndexStateTest, RejectsPayloadWithCorruptSourceMeta) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("a", 100, 5, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact())};
    // Version 1 followed by a truncated data level.
    std::shared_ptr<Bytes> corrupt_source_meta =
        std::make_shared<Bytes>(std::string("\x00\x00\x00\x01\x00\x00", 6), pool_.get());
    std::shared_ptr<IndexFileMeta> payload = MakePayloadWithSourceMetaBytes(
        "payload.index", 7, "btree", /*total_row_count=*/300,
        /*row_range_start=*/0, /*row_range_end=*/299, corrupt_source_meta);
    PkSortedBucketIndexState state =
        PkSortedBucketIndexState::FromActiveDataFiles(7, "btree", data_files, {payload});
    ASSERT_TRUE(state.Groups().empty());
    ASSERT_EQ(1, state.RejectedPayloads().size());
    ASSERT_EQ(payload, state.RejectedPayloads()[0]);
    ASSERT_EQ(2, state.UncoveredSourceFiles().size());
}

TEST_F(PkSortedBucketIndexStateTest, KeepsValidLevelAndLeavesBrokenLevelUncovered) {
    std::vector<std::shared_ptr<DataFileMeta>> data_files = {
        MakeDataFile("c", 50, 4, FileSource::Compact()),
        MakeDataFile("b", 200, 5, FileSource::Compact()),
        MakeDataFile("a", 100, 5, FileSource::Compact())};
    std::shared_ptr<IndexFileMeta> valid_payload = MakePayload(7, "btree", 4, {{"c", 50}}, 50);
    std::shared_ptr<IndexFileMeta> broken_payload =
        MakePayload(7, "btree", 5, {{"a", 100}, {"b", 999}}, 1099);
    PkSortedBucketIndexState state = PkSortedBucketIndexState::FromActiveDataFiles(
        7, "btree", data_files, {valid_payload, broken_payload});
    ASSERT_EQ(1, state.Groups().size());
    ASSERT_EQ(4, state.Groups()[0]->DataLevel());
    ASSERT_EQ(valid_payload, state.Groups()[0]->Payload());
    std::vector<PrimaryKeyIndexSourceFile> expected_covered = {{"c", 50}};
    ASSERT_EQ(expected_covered, state.CoveredSourceFiles());
    std::vector<PrimaryKeyIndexSourceFile> expected_uncovered = {{"a", 100}, {"b", 200}};
    ASSERT_EQ(expected_uncovered, state.UncoveredSourceFiles());
    ASSERT_EQ(1, state.RejectedPayloads().size());
    ASSERT_EQ(broken_payload, state.RejectedPayloads()[0]);
}

}  // namespace paimon::test
