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

#include "paimon/core/operation/commit/materialized_index_changes_provider.h"

#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/snapshot.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

BinaryRow CreateIntRow(int32_t value) {
    BinaryRow row(1);
    BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
    writer.WriteInt(0, value);
    writer.Complete();
    return row;
}

IndexManifestEntry CreateGlobalIndexEntry(const std::string& file_name, int32_t partition_value,
                                          const FileKind& kind = FileKind::Add()) {
    GlobalIndexMeta global_index_meta(/*row_range_start=*/0, /*row_range_end=*/1,
                                      /*index_field_id=*/0, /*extra_field_ids=*/std::nullopt,
                                      /*index_meta=*/nullptr);
    auto index_file = std::make_shared<IndexFileMeta>(
        /*index_type=*/"GLOBAL_INDEX", file_name, /*file_size=*/10, /*row_count=*/1,
        /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt, global_index_meta);
    return IndexManifestEntry(kind, CreateIntRow(partition_value), /*bucket=*/0, index_file);
}

IndexManifestEntry CreateDeletionVectorEntry(const std::string& file_name, int32_t partition_value,
                                             const FileKind& kind = FileKind::Add()) {
    auto index_file = std::make_shared<IndexFileMeta>(
        /*index_type=*/"DELETION_VECTORS", file_name, /*file_size=*/10, /*row_count=*/1,
        /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt);
    return IndexManifestEntry(kind, CreateIntRow(partition_value), /*bucket=*/0, index_file);
}

Snapshot MakeSnapshot(const std::optional<std::string>& index_manifest) {
    return Snapshot(
        /*id=*/1, /*schema_id=*/0, /*base_manifest_list=*/"base",
        /*base_manifest_list_size=*/std::nullopt, /*delta_manifest_list=*/"delta",
        /*delta_manifest_list_size=*/std::nullopt, /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt, index_manifest,
        /*commit_user=*/"test-user", /*commit_identifier=*/1, Snapshot::CommitKind::Compact(),
        /*time_millis=*/0, /*total_record_count=*/0, /*delta_record_count=*/0,
        /*changelog_record_count=*/std::nullopt, /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt, /*properties=*/std::nullopt, /*next_row_id=*/std::nullopt);
}

std::set<std::string> DeletedIndexNames(const std::vector<IndexManifestEntry>& entries) {
    std::set<std::string> result;
    for (const IndexManifestEntry& entry : entries) {
        if (entry.kind == FileKind::Delete()) {
            result.insert(entry.index_file->FileName());
        }
    }
    return result;
}

}  // namespace

TEST(MaterializedIndexChangesProviderTest, TestRescansGlobalIndexesOnEveryAttempt) {
    // The commit was prepared when only `planned-index` existed. By the time it is attempted a
    // second index is live, and the second attempt has to drop that one as well - otherwise it
    // would survive against row ids this commit reassigns.
    MaterializedBuckets materialized_buckets;
    materialized_buckets.insert(MaterializedBucket{CreateIntRow(10), /*bucket=*/0});

    std::vector<IndexManifestEntry> prepared = {
        CreateGlobalIndexEntry("planned-index", /*partition_value=*/10, FileKind::Delete()),
        CreateDeletionVectorEntry("dv-new", /*partition_value=*/10)};

    int32_t scan_calls = 0;
    MaterializedIndexChangesProvider provider(
        /*delta_files=*/{}, /*changelog_files=*/{}, prepared, materialized_buckets,
        [&scan_calls](const Snapshot&) -> Result<std::vector<IndexManifestEntry>> {
            ++scan_calls;
            std::vector<IndexManifestEntry> live = {
                CreateGlobalIndexEntry("planned-index", /*partition_value=*/10)};
            if (scan_calls > 1) {
                live.push_back(CreateGlobalIndexEntry("late-index", /*partition_value=*/10));
            }
            return live;
        });

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitChanges> first,
                         provider.Provide(MakeSnapshot("index-manifest-1")));
    ASSERT_EQ(DeletedIndexNames(first->index_entries), std::set<std::string>({"planned-index"}));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitChanges> second,
                         provider.Provide(MakeSnapshot("index-manifest-2")));
    ASSERT_EQ(DeletedIndexNames(second->index_entries),
              std::set<std::string>({"planned-index", "late-index"}));
    ASSERT_EQ(scan_calls, 2);

    // Whatever else the commit carries travels unchanged: the rewritten deletion vector index
    // is not a global index and is neither dropped nor re-derived.
    int32_t dv_additions = 0;
    for (const IndexManifestEntry& entry : second->index_entries) {
        if (entry.kind == FileKind::Add() && entry.index_file->FileName() == "dv-new") {
            dv_additions++;
        }
    }
    ASSERT_EQ(dv_additions, 1);
}

TEST(MaterializedIndexChangesProviderTest, TestKeepsIndexesOfOtherBuckets) {
    // Only the buckets this commit materializes lose their global indexes; another partition's
    // stays live, and the deletion prepared for it - if any - is left alone.
    MaterializedBuckets materialized_buckets;
    materialized_buckets.insert(MaterializedBucket{CreateIntRow(10), /*bucket=*/0});

    std::vector<IndexManifestEntry> prepared = {CreateGlobalIndexEntry(
        "other-partition-index", /*partition_value=*/20, FileKind::Delete())};

    MaterializedIndexChangesProvider provider(
        /*delta_files=*/{}, /*changelog_files=*/{}, prepared, materialized_buckets,
        [](const Snapshot&) -> Result<std::vector<IndexManifestEntry>> {
            return std::vector<IndexManifestEntry>{
                CreateGlobalIndexEntry("elsewhere-index", /*partition_value=*/20)};
        });

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitChanges> changes,
                         provider.Provide(MakeSnapshot("index-manifest-1")));
    ASSERT_EQ(DeletedIndexNames(changes->index_entries),
              std::set<std::string>({"other-partition-index"}));
}

TEST(MaterializedIndexChangesProviderTest, TestWithoutLatestSnapshotKeepsPreparedChanges) {
    // An empty table has no index manifest to re-read; the prepared changes are what commits.
    MaterializedBuckets materialized_buckets;
    materialized_buckets.insert(MaterializedBucket{CreateIntRow(10), /*bucket=*/0});

    int32_t scan_calls = 0;
    MaterializedIndexChangesProvider provider(
        /*delta_files=*/{}, /*changelog_files=*/{},
        {CreateDeletionVectorEntry("dv-new", /*partition_value=*/10)}, materialized_buckets,
        [&scan_calls](const Snapshot&) -> Result<std::vector<IndexManifestEntry>> {
            ++scan_calls;
            return std::vector<IndexManifestEntry>{};
        });

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitChanges> changes,
                         provider.Provide(/*latest_snapshot=*/std::nullopt));
    ASSERT_EQ(changes->index_entries.size(), 1);
    ASSERT_EQ(scan_calls, 0);
}

}  // namespace paimon::test
