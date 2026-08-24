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

#include "paimon/core/operation/commit/conflict_detection.h"

#include <optional>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/memory/bytes.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

Snapshot MakeSnapshot(const Snapshot::CommitKind& commit_kind) {
    return Snapshot(
        /*id=*/1,
        /*schema_id=*/1,
        /*base_manifest_list=*/"base-manifest-list",
        /*base_manifest_list_size=*/std::nullopt,
        /*delta_manifest_list=*/"delta-manifest-list",
        /*delta_manifest_list_size=*/std::nullopt,
        /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt,
        /*index_manifest=*/std::nullopt,
        /*commit_user=*/"test-user",
        /*commit_identifier=*/1, commit_kind,
        /*time_millis=*/0,
        /*total_record_count=*/0,
        /*delta_record_count=*/0,
        /*changelog_record_count=*/std::nullopt,
        /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt,
        /*properties=*/std::nullopt,
        /*next_row_id=*/std::nullopt);
}

Snapshot MakeSnapshotWithNextRowId(const Snapshot::CommitKind& commit_kind,
                                   const std::optional<int64_t>& next_row_id) {
    return Snapshot(
        /*id=*/1,
        /*schema_id=*/1,
        /*base_manifest_list=*/"base-manifest-list",
        /*base_manifest_list_size=*/std::nullopt,
        /*delta_manifest_list=*/"delta-manifest-list",
        /*delta_manifest_list_size=*/std::nullopt,
        /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt,
        /*index_manifest=*/std::nullopt,
        /*commit_user=*/"test-user",
        /*commit_identifier=*/1, commit_kind,
        /*time_millis=*/0,
        /*total_record_count=*/0,
        /*delta_record_count=*/0,
        /*changelog_record_count=*/std::nullopt,
        /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt,
        /*properties=*/std::nullopt, next_row_id);
}

Status CheckConflicts(const ConflictDetection& detection,
                      const std::vector<ManifestEntry>& base_entries,
                      const std::vector<ManifestEntry>& delta_entries,
                      const Snapshot::CommitKind& commit_kind) {
    return detection.CheckConflicts(MakeSnapshot(commit_kind), base_entries, delta_entries,
                                    /*delta_index_entries=*/{},
                                    /*row_id_conflict_checker=*/std::nullopt, commit_kind);
}

Status CheckConflicts(const ConflictDetection& detection,
                      const std::vector<ManifestEntry>& base_entries,
                      const std::vector<ManifestEntry>& delta_entries,
                      const std::vector<IndexManifestEntry>& delta_index_entries,
                      const Snapshot::CommitKind& commit_kind) {
    return detection.CheckConflicts(MakeSnapshot(commit_kind), base_entries, delta_entries,
                                    delta_index_entries,
                                    /*row_id_conflict_checker=*/std::nullopt, commit_kind);
}

Status CheckConflictsWithNextRowId(const ConflictDetection& detection,
                                   const std::vector<ManifestEntry>& base_entries,
                                   const std::vector<ManifestEntry>& delta_entries,
                                   const std::optional<int64_t>& next_row_id,
                                   const Snapshot::CommitKind& commit_kind) {
    return detection.CheckConflicts(MakeSnapshotWithNextRowId(commit_kind, next_row_id),
                                    base_entries, delta_entries,
                                    /*delta_index_entries=*/{},
                                    /*row_id_conflict_checker=*/std::nullopt, commit_kind);
}

}  // namespace

class ConflictDetectionTest : public testing::Test {
 public:
    void SetUp() override {
        fields_ = {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                   arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    }

 protected:
    std::shared_ptr<FileStorePathFactory> CreatePathFactory(
        const std::vector<std::string>& partition_keys) const {
        EXPECT_OK_AND_ASSIGN(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                /*root=*/"/tmp/conflict_detection_test", arrow::schema(fields_), partition_keys,
                /*default_part_value=*/"__DEFAULT_PARTITION__", /*identifier=*/"orc",
                /*data_file_prefix=*/"data-", /*legacy_partition_name_enabled=*/false,
                /*external_paths=*/{}, /*global_index_external_path=*/std::nullopt,
                /*index_file_in_data_file_dir=*/false, GetDefaultPool()));
        return path_factory;
    }

    ManifestEntry CreateManifestEntry(const std::string& file_name, const FileKind& kind) const {
        int32_t arity = 1;
        BinaryRow row(arity);
        BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
        writer.WriteInt(0, 10);
        writer.Complete();
        return CreateManifestEntry(file_name, row, kind);
    }

    ManifestEntry CreateManifestEntry(const std::string& file_name, const BinaryRow& partition,
                                      const FileKind& kind) const {
        return CreateManifestEntry(file_name, partition, kind, DataFileMeta::EmptyMinKey(),
                                   DataFileMeta::EmptyMaxKey(), /*level=*/2, /*bucket=*/0);
    }

    ManifestEntry CreateManifestEntry(const std::string& file_name, const BinaryRow& partition,
                                      const FileKind& kind, const BinaryRow& min_key,
                                      const BinaryRow& max_key, int32_t level, int32_t bucket = 0,
                                      int32_t total_buckets = 2) const {
        auto data_file_meta = std::make_shared<DataFileMeta>(
            file_name, 1024, 8, min_key, max_key, SimpleStats::EmptyStats(),
            SimpleStats::EmptyStats(), /*min_seq_no=*/16, /*max_seq_no=*/32,
            /*schema_id=*/1, level,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/3,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        return ManifestEntry(kind, partition, bucket, total_buckets, data_file_meta);
    }

    ManifestEntry CreateManifestEntryWithFirstRowId(const std::string& file_name,
                                                    const BinaryRow& partition,
                                                    const FileKind& kind, int32_t bucket,
                                                    int64_t first_row_id, int64_t row_count) const {
        auto data_file_meta = std::make_shared<DataFileMeta>(
            file_name, 1024, row_count, DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), /*min_seq_no=*/16,
            /*max_seq_no=*/32,
            /*schema_id=*/1, /*level=*/2,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, first_row_id,
            /*write_cols=*/std::nullopt);
        return ManifestEntry(kind, partition, bucket, /*total_buckets=*/2, data_file_meta);
    }

    IndexManifestEntry CreateGlobalIndexEntry(const std::string& file_name,
                                              const BinaryRow& partition, int32_t bucket,
                                              int64_t row_range_start,
                                              int64_t row_range_end) const {
        return CreateGlobalIndexEntry(file_name, partition, bucket, FileKind::Add(),
                                      row_range_start, row_range_end);
    }

    IndexManifestEntry CreateGlobalIndexEntry(const std::string& file_name,
                                              const BinaryRow& partition, int32_t bucket,
                                              const FileKind& kind, int64_t row_range_start,
                                              int64_t row_range_end) const {
        GlobalIndexMeta global_index_meta(row_range_start, row_range_end, /*index_field_id=*/1,
                                          /*extra_field_ids=*/std::nullopt,
                                          std::make_shared<Bytes>("meta", GetDefaultPool().get()));
        auto index_file_meta = std::make_shared<IndexFileMeta>(
            "HASH", file_name, /*file_size=*/100, /*row_count=*/5,
            /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt, global_index_meta);
        return IndexManifestEntry(kind, partition, bucket, index_file_meta);
    }

    IndexManifestEntry CreateDvIndexEntry(const std::string& file_name, const BinaryRow& partition,
                                          int32_t bucket) const {
        auto index_file_meta = std::make_shared<IndexFileMeta>(
            DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, file_name, /*file_size=*/100,
            /*row_count=*/1, /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt);
        return IndexManifestEntry(FileKind::Add(), partition, bucket, index_file_meta);
    }

    /// A deletion-vector index entry that records a vector for each of `covered_data_files`,
    /// which is what pairs a data file with the index file holding its deletions. A nullopt
    /// cardinality is what index metadata written before Paimon recorded it looks like.
    IndexManifestEntry CreateDvIndexEntry(const std::string& file_name, const BinaryRow& partition,
                                          int32_t bucket, const FileKind& kind,
                                          const std::vector<std::string>& covered_data_files,
                                          std::optional<int64_t> cardinality = 1) const {
        LinkedHashMap<std::string, DeletionVectorMeta> dv_ranges;
        int32_t offset = 1;
        for (const std::string& data_file : covered_data_files) {
            dv_ranges.insert_or_assign(
                data_file, DeletionVectorMeta(data_file, offset, /*length=*/8, cardinality));
            offset += 8;
        }
        auto index_file_meta = std::make_shared<IndexFileMeta>(
            DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, file_name, /*file_size=*/100,
            /*row_count=*/static_cast<int64_t>(covered_data_files.size()), dv_ranges,
            /*external_path=*/std::nullopt);
        return IndexManifestEntry(kind, partition, bucket, index_file_meta);
    }

    BinaryRow CreateIntRow(int32_t value) const {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 20, GetDefaultPool().get());
        writer.WriteInt(0, value);
        writer.Complete();
        return row;
    }

    arrow::FieldVector fields_;
};

TEST_F(ConflictDetectionTest, TestFileDeletionConflicts) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("f1", FileKind::Add()));

        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("f1", FileKind::Delete()));

        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("f2", FileKind::Add()));

        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("f1", FileKind::Delete()));
        changes.push_back(CreateManifestEntry("f3", FileKind::Add()));

        ASSERT_NOK_WITH_MSG(
            CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()),
            "Trying to delete file f1");
    }
    {
        std::vector<ManifestEntry> base_entries;
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("f1", FileKind::Delete()));

        ASSERT_NOK_WITH_MSG(
            CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()),
            "Trying to delete file f1");
    }
}

TEST_F(ConflictDetectionTest, TestGlobalIndexRowIdExistenceConflicts) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    const BinaryRow partition = CreateIntRow(10);
    std::vector<ManifestEntry> base_entries;
    base_entries.push_back(CreateManifestEntryWithFirstRowId("base-1", partition, FileKind::Add(),
                                                             /*bucket=*/0, /*first_row_id=*/0,
                                                             /*row_count=*/10));
    base_entries.push_back(CreateManifestEntryWithFirstRowId("base-2", partition, FileKind::Add(),
                                                             /*bucket=*/0, /*first_row_id=*/10,
                                                             /*row_count=*/10));
    std::vector<ManifestEntry> changes;

    ASSERT_OK(
        CheckConflicts(detection, base_entries, changes,
                       {CreateGlobalIndexEntry("global-index-covered", partition, /*bucket=*/0,
                                               /*row_range_start=*/0, /*row_range_end=*/19)},
                       Snapshot::CommitKind::Append()));

    ASSERT_NOK_WITH_MSG(
        CheckConflicts(detection, base_entries, changes,
                       {CreateGlobalIndexEntry("global-index-missing", partition, /*bucket=*/0,
                                               /*row_range_start=*/0, /*row_range_end=*/20)},
                       Snapshot::CommitKind::Append()),
        "Global index row ID existence conflict");
}

TEST_F(ConflictDetectionTest, TestDedicatedStorageRowIdRangeConflicts) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    const BinaryRow partition = CreateIntRow(10);
    std::vector<ManifestEntry> base_entries;
    base_entries.push_back(CreateManifestEntryWithFirstRowId("data-0.orc", partition,
                                                             FileKind::Add(), /*bucket=*/0,
                                                             /*first_row_id=*/0,
                                                             /*row_count=*/10));

    std::vector<ManifestEntry> out_of_range_dedicated_entries;
    out_of_range_dedicated_entries.push_back(CreateManifestEntryWithFirstRowId(
        "blob-0.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/5,
        /*row_count=*/10));
    ASSERT_NOK_WITH_MSG(CheckConflicts(detection, base_entries, out_of_range_dedicated_entries,
                                       Snapshot::CommitKind::Compact()),
                        "is not covered by one data file range");

    std::vector<ManifestEntry> contained_dedicated_entries;
    contained_dedicated_entries.push_back(CreateManifestEntryWithFirstRowId(
        "blob-1.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/5,
        /*row_count=*/4));
    ASSERT_OK(CheckConflicts(detection, base_entries, contained_dedicated_entries,
                             Snapshot::CommitKind::Compact()));

    std::vector<ManifestEntry> disjoint_data_entries;
    disjoint_data_entries.push_back(CreateManifestEntryWithFirstRowId(
        "data-a.orc", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0,
        /*row_count=*/5));
    disjoint_data_entries.push_back(CreateManifestEntryWithFirstRowId(
        "data-b.orc", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/10,
        /*row_count=*/5));
    std::vector<ManifestEntry> spanning_dedicated_entries;
    spanning_dedicated_entries.push_back(CreateManifestEntryWithFirstRowId(
        "vector-0.vector.data", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/3,
        /*row_count=*/10));
    ASSERT_NOK_WITH_MSG(CheckConflicts(detection, disjoint_data_entries, spanning_dedicated_entries,
                                       Snapshot::CommitKind::Compact()),
                        "spans multiple data file ranges");

    std::vector<ManifestEntry> no_data_entries;
    std::vector<ManifestEntry> dedicated_only_entries;
    dedicated_only_entries.push_back(CreateManifestEntryWithFirstRowId(
        "blob-only.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0,
        /*row_count=*/1));
    ASSERT_NOK_WITH_MSG(CheckConflicts(detection, no_data_entries, dedicated_only_entries,
                                       Snapshot::CommitKind::Compact()),
                        "is not covered by one data file range");
}

TEST_F(ConflictDetectionTest, TestBucketKeepSame) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));

    const BinaryRow partition = CreateIntRow(10);
    {
        ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                    /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                    /*commit_scanner=*/nullptr, "test_user", "test_table",
                                    /*path_factory=*/nullptr);
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   DataFileMeta::EmptyMinKey(),
                                                   DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                                   /*bucket=*/0, /*total_buckets=*/4));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(),
                                              DataFileMeta::EmptyMinKey(),
                                              DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                              /*bucket=*/0, /*total_buckets=*/4));

        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                    /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                    /*commit_scanner=*/nullptr, "test_user", "test_table",
                                    /*path_factory=*/nullptr);
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   DataFileMeta::EmptyMinKey(),
                                                   DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                                   /*bucket=*/0, /*total_buckets=*/2));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(),
                                              DataFileMeta::EmptyMinKey(),
                                              DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                              /*bucket=*/0, /*total_buckets=*/4));

        ASSERT_NOK_WITH_MSG(
            CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()),
            "Total buckets of partition");
    }
    {
        ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                    /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                    /*commit_scanner=*/nullptr, "test_user", "test_table",
                                    /*path_factory=*/nullptr);
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   DataFileMeta::EmptyMinKey(),
                                                   DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                                   /*bucket=*/0, /*total_buckets=*/2));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", CreateIntRow(20), FileKind::Add(),
                                              DataFileMeta::EmptyMinKey(),
                                              DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                              /*bucket=*/0, /*total_buckets=*/4));

        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                    /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                    /*commit_scanner=*/nullptr, "test_user", "test_table",
                                    /*path_factory=*/nullptr);
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   DataFileMeta::EmptyMinKey(),
                                                   DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                                   /*bucket=*/0, /*total_buckets=*/2));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(),
                                              DataFileMeta::EmptyMinKey(),
                                              DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                              /*bucket=*/0, /*total_buckets=*/4));

        ASSERT_OK(
            CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Overwrite()));
    }
}

TEST_F(ConflictDetectionTest, TestBucketKeepSameHelpers) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    const BinaryRow partition = CreateIntRow(10);
    std::vector<ManifestEntry> changes;
    changes.push_back(CreateManifestEntry("delta-1", partition, FileKind::Add(),
                                          DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
                                          /*level=*/1,
                                          /*bucket=*/0, /*total_buckets=*/4));
    changes.push_back(CreateManifestEntry("delta-2", partition, FileKind::Add(),
                                          DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
                                          /*level=*/1,
                                          /*bucket=*/1, /*total_buckets=*/4));

    std::unordered_map<BinaryRow, int32_t> expected_total_buckets;
    ASSERT_OK(detection.CollectUncheckedBucketPartitions(changes, &expected_total_buckets));
    ASSERT_EQ(1U, expected_total_buckets.size());
    ASSERT_EQ(4, expected_total_buckets.at(partition));

    std::unordered_map<BinaryRow, int32_t> previous_total_buckets;
    previous_total_buckets.emplace(partition, 4);
    ASSERT_OK(
        detection.CheckSameBucketByTotalBuckets(expected_total_buckets, previous_total_buckets));

    std::unordered_map<BinaryRow, int32_t> cached_total_buckets;
    ASSERT_OK(detection.CollectUncheckedBucketPartitions(changes, &cached_total_buckets));
    ASSERT_TRUE(cached_total_buckets.empty());

    ConflictDetection mismatch_detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                         /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                         /*commit_scanner=*/nullptr, "test_user", "test_table",
                                         CreatePathFactory({"f1"}));
    ASSERT_NOK_WITH_MSG(
        mismatch_detection.CheckSameBucketByTotalBuckets(expected_total_buckets, {{partition, 2}}),
        "new bucket num");
}

TEST_F(ConflictDetectionTest, TestCollectUncheckedBucketPartitionsMismatch) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                CreatePathFactory({"f1"}));

    const BinaryRow partition = CreateIntRow(10);
    std::vector<ManifestEntry> changes;
    changes.push_back(CreateManifestEntry("delta-1", partition, FileKind::Add(),
                                          DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
                                          /*level=*/1,
                                          /*bucket=*/0, /*total_buckets=*/2));
    changes.push_back(CreateManifestEntry("delta-2", partition, FileKind::Add(),
                                          DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
                                          /*level=*/1,
                                          /*bucket=*/1, /*total_buckets=*/4));

    std::unordered_map<BinaryRow, int32_t> total_buckets;
    // Verify the partition value is rendered via FileStorePathFactory::GetPartitionString
    // (i.e. "partition {f1=10...}"), not dropped. Before the fmt escaping fix this printed a
    // literal "partition {}" with the partition string silently discarded.
    ASSERT_NOK_WITH_MSG(detection.CollectUncheckedBucketPartitions(changes, &total_buckets),
                        "partition {f1=10");
}

TEST_F(ConflictDetectionTest, TestBucketKeepSameCacheEviction) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    const int32_t total_buckets = 4;
    for (int32_t value = 0; value <= 1000; ++value) {
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", CreateIntRow(value), FileKind::Add(),
                                              DataFileMeta::EmptyMinKey(),
                                              DataFileMeta::EmptyMaxKey(), /*level=*/1,
                                              /*bucket=*/0, total_buckets));

        std::unordered_map<BinaryRow, int32_t> expected_total_buckets;
        ASSERT_OK(detection.CollectUncheckedBucketPartitions(changes, &expected_total_buckets));
        ASSERT_EQ(1U, expected_total_buckets.size());
        ASSERT_OK(detection.CheckSameBucketByTotalBuckets(expected_total_buckets,
                                                          expected_total_buckets));
    }

    std::vector<ManifestEntry> evicted_partition_changes;
    evicted_partition_changes.push_back(
        CreateManifestEntry("delta", CreateIntRow(0), FileKind::Add(), DataFileMeta::EmptyMinKey(),
                            DataFileMeta::EmptyMaxKey(), /*level=*/1, /*bucket=*/0, total_buckets));
    std::unordered_map<BinaryRow, int32_t> evicted_partition_buckets;
    ASSERT_OK(detection.CollectUncheckedBucketPartitions(evicted_partition_changes,
                                                         &evicted_partition_buckets));
    ASSERT_EQ(1U, evicted_partition_buckets.size());
}

TEST_F(ConflictDetectionTest, TestDeletionVectorsAllowedWithBucketUnawareMode) {
    // an unaware bucket table may carry deletion vectors, told apart by index file name rather
    // than by bucket. Adding files commits; dropping one needs the pairing this class does not
    // build and is refused
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::BUCKET, "-1"},
                                               {Options::DELETION_VECTORS_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    ASSERT_OK(CheckConflicts(detection, /*base_entries=*/{}, /*delta_entries=*/{},
                             Snapshot::CommitKind::Append()));

    ASSERT_OK(CheckConflicts(detection, /*base_entries=*/{},
                             {CreateManifestEntry("added.parquet", FileKind::Add())},
                             Snapshot::CommitKind::Append()));

    ASSERT_NOK_WITH_MSG(
        CheckConflicts(detection, {CreateManifestEntry("dropped.parquet", FileKind::Add())},
                       {CreateManifestEntry("dropped.parquet", FileKind::Delete())},
                       Snapshot::CommitKind::Append()),
        "Committing the deletion of data file dropped.parquet is not supported");
}

namespace {

/// The pieces every data-evolution compaction conflict test needs: a table whose deletion
/// vectors are keyed by data file alone, and a detector over it.
ConflictDetection DataEvolutionDetector(const std::shared_ptr<TableSchema>& table_schema,
                                        const CoreOptions& core_options) {
    return ConflictDetection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                             /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                             /*commit_scanner=*/nullptr, "test_user", "test_table",
                             /*path_factory=*/nullptr);
}

Status CheckCompaction(const ConflictDetection& detection,
                       const std::vector<ManifestEntry>& base_entries,
                       const std::vector<ManifestEntry>& delta_entries,
                       const std::vector<IndexManifestEntry>& delta_index_entries,
                       const std::vector<IndexManifestEntry>& base_index_entries) {
    return detection.CheckConflicts(MakeSnapshot(Snapshot::CommitKind::Compact()), base_entries,
                                    delta_entries, delta_index_entries,
                                    /*row_id_conflict_checker=*/std::nullopt,
                                    Snapshot::CommitKind::Compact(), base_index_entries);
}

}  // namespace

/// A data-evolution table whose files carry row ids, which is what lets the migration check
/// attribute each dropped file to the output that took over its rows.
class DataEvolutionConflictDetectionTest : public ConflictDetectionTest {
 protected:
    void SetUp() override {
        ConflictDetectionTest::SetUp();
        ASSERT_OK_AND_ASSIGN(
            table_schema_,
            TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                                /*primary_keys=*/{}, /*options=*/{}));
        ASSERT_OK_AND_ASSIGN(core_options_,
                             CoreOptions::FromMap({{Options::BUCKET, "-1"},
                                                   {Options::DELETION_VECTORS_ENABLED, "true"},
                                                   {Options::ROW_TRACKING_ENABLED, "true"},
                                                   {Options::DATA_EVOLUTION_ENABLED, "true"}}));
        partition_ = CreateIntRow(10);
    }

    ManifestEntry DataFile(const std::string& file_name, const FileKind& kind, int64_t first_row_id,
                           int64_t row_count) const {
        return CreateManifestEntryWithFirstRowId(file_name, partition_, kind, /*bucket=*/0,
                                                 first_row_id, row_count);
    }

    IndexManifestEntry DvIndex(const std::string& file_name, const FileKind& kind,
                               const std::vector<std::string>& covered_data_files,
                               std::optional<int64_t> cardinality = 1) const {
        return CreateDvIndexEntry(file_name, partition_, /*bucket=*/0, kind, covered_data_files,
                                  cardinality);
    }

    std::shared_ptr<TableSchema> table_schema_;
    CoreOptions core_options_;
    BinaryRow partition_;
};

TEST_F(DataEvolutionConflictDetectionTest, TestMayDropDataFilesWithDeletionVectors) {
    // A data-evolution compaction is the one shape that drops data files and migrates their
    // deletion vectors in the same commit, so the blanket refusal must not apply to it. Without
    // this carve-out the whole compaction of such a table cannot be committed at all.
    ConflictDetection detection = DataEvolutionDetector(table_schema_, core_options_);

    // The files the compaction replaces have to be live in the base, or the generic delete check
    // rejects the commit before the deletion-vector accounting is reached. That is what
    // FileStoreCommitImpl hands in: the existing files of the changed partitions.
    std::vector<ManifestEntry> base_entries = {
        DataFile("anchor.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("field-group.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2)};
    std::vector<ManifestEntry> compact_delta = {
        DataFile("anchor.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("field-group.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("compacted.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2)};

    // The base holds the anchor's vector in dv-1, and the commit removes dv-1 while giving the
    // compacted file a vector deleting the same number of rows.
    std::vector<IndexManifestEntry> base_index_entries = {
        DvIndex("dv-1", FileKind::Add(), {"anchor.parquet"})};
    std::vector<IndexManifestEntry> delta_index_entries = {
        DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet"}),
        DvIndex("dv-2", FileKind::Add(), {"compacted.parquet"})};
    ASSERT_OK(CheckCompaction(detection, base_entries, compact_delta, delta_index_entries,
                              base_index_entries));

    // The base index entries are read through a filter that keeps only what can decide this
    // migration, so a round of a batched compaction does not retain the whole table's index.
    std::function<Result<bool>(const IndexManifestEntry&)> filter = detection.BaseIndexEntryFilter(
        compact_delta, delta_index_entries, Snapshot::CommitKind::Compact());
    ASSERT_TRUE(filter != nullptr);
    // Kept: the index file this commit replaces, and one holding a dropped file's vector.
    ASSERT_OK_AND_ASSIGN(bool kept, filter(base_index_entries[0]));
    ASSERT_TRUE(kept);
    ASSERT_OK_AND_ASSIGN(kept, filter(DvIndex("dv-other", FileKind::Add(), {"anchor.parquet"})));
    ASSERT_TRUE(kept);
    // Dropped: a vector of a file this commit does not touch, and another partition's.
    ASSERT_OK_AND_ASSIGN(kept,
                         filter(DvIndex("dv-elsewhere", FileKind::Add(), {"unrelated.parquet"})));
    ASSERT_FALSE(kept);
    ASSERT_OK_AND_ASSIGN(kept, filter(CreateDvIndexEntry("dv-p2", CreateIntRow(11), /*bucket=*/0,
                                                         FileKind::Add(), {"anchor.parquet"})));
    ASSERT_FALSE(kept);

    // A partition with no vectors at all needs no accounting.
    ASSERT_OK(CheckCompaction(detection, base_entries, compact_delta, delta_index_entries,
                              /*base_index_entries=*/{}));

    // An append on the same table still refuses to drop a data file: it carries no migration.
    ASSERT_NOK_WITH_MSG(
        CheckConflicts(detection, {CreateManifestEntry("anchor.parquet", FileKind::Add())},
                       {CreateManifestEntry("anchor.parquet", FileKind::Delete())},
                       Snapshot::CommitKind::Append()),
        "Committing the deletion of data file anchor.parquet is not supported");
    ASSERT_TRUE(detection.BaseIndexEntryFilter(compact_delta, delta_index_entries,
                                               Snapshot::CommitKind::Append()) == nullptr);
}

TEST_F(DataEvolutionConflictDetectionTest, TestRefusesLeavingBehindADeletionVector) {
    // Another engine wrote a vector for the anchor after the round was planned, so the
    // compaction's own index changes do not cover it. Committing anyway would drop the anchor
    // while the new vector kept pointing at it, and its deleted rows would come back.
    ConflictDetection detection = DataEvolutionDetector(table_schema_, core_options_);

    std::vector<ManifestEntry> base_entries = {
        DataFile("anchor.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2)};
    std::vector<ManifestEntry> compact_delta = {
        DataFile("anchor.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("compacted.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2)};
    // The latest snapshot keys the anchor's vector by dv-concurrent, which this commit does not
    // remove; it only adds the vector it moved onto the compacted file.
    std::vector<IndexManifestEntry> base_index_entries = {
        DvIndex("dv-concurrent", FileKind::Add(), {"anchor.parquet"})};
    std::vector<IndexManifestEntry> delta_index_entries = {
        DvIndex("dv-2", FileKind::Add(), {"compacted.parquet"})};

    ASSERT_NOK_WITH_MSG(
        CheckCompaction(detection, base_entries, compact_delta, delta_index_entries,
                        base_index_entries),
        "its deletion vector in index file dv-concurrent is not removed by the same commit");
}

TEST_F(DataEvolutionConflictDetectionTest, TestRefusesLosingTheDroppedDeletions) {
    // The commit removes the index file that held the anchor's vector but writes no vector for
    // the file it rewrote the group into, so those deletions land nowhere.
    ConflictDetection detection = DataEvolutionDetector(table_schema_, core_options_);

    std::vector<ManifestEntry> base_entries = {
        DataFile("anchor.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2)};
    std::vector<ManifestEntry> compact_delta = {
        DataFile("anchor.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("compacted.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2)};
    std::vector<IndexManifestEntry> base_index_entries = {
        DvIndex("dv-1", FileKind::Add(), {"anchor.parquet"})};
    // dv-1 goes away and nothing takes its place.
    std::vector<IndexManifestEntry> delta_index_entries = {
        DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet"})};

    ASSERT_NOK_WITH_MSG(CheckCompaction(detection, base_entries, compact_delta, delta_index_entries,
                                        base_index_entries),
                        "but writes no deletion vector for it");

    // A vector that deletes nothing still has to be taken away with the file it was keyed by:
    // the rewriter removes it whether or not it is empty, so an entry outliving its data file
    // is a migration that did not happen, not a harmless leftover.
    ASSERT_NOK_WITH_MSG(CheckCompaction(detection, base_entries, compact_delta,
                                        /*delta_index_entries=*/{},
                                        {DvIndex("dv-1", FileKind::Add(), {"anchor.parquet"},
                                                 /*cardinality=*/0)}),
                        "its deletion vector in index file dv-1 is not removed by the same "
                        "commit");

    // Taken away, and no vector demanded of the output in return: nothing was deleted, so
    // nothing had to move.
    ASSERT_OK(CheckCompaction(detection, base_entries, compact_delta,
                              {DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet"},
                                       /*cardinality=*/0)},
                              {DvIndex("dv-1", FileKind::Add(), {"anchor.parquet"},
                                       /*cardinality=*/0)}));

    // Index metadata old enough not to record a cardinality at all. The vector may well have
    // been empty, so demanding one for the output would reject a correct commit for good
    // instead of for a race - and unlike a race, replanning would never make it pass.
    ASSERT_OK(CheckCompaction(detection, base_entries, compact_delta,
                              {DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet"},
                                       /*cardinality=*/std::nullopt)},
                              {DvIndex("dv-1", FileKind::Add(), {"anchor.parquet"},
                                       /*cardinality=*/std::nullopt)}));
}

TEST_F(DataEvolutionConflictDetectionTest, TestRefusesLosingOneGroupOfSeveral) {
    // Two groups compacted in the same commit, both carrying deletions, but only one output gets
    // a vector. Accounting per partition would accept this; accounting per group does not.
    ConflictDetection detection = DataEvolutionDetector(table_schema_, core_options_);

    std::vector<ManifestEntry> base_entries = {
        DataFile("anchor-a.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("anchor-b.parquet", FileKind::Add(), /*first_row_id=*/4, /*row_count=*/2)};
    std::vector<ManifestEntry> compact_delta = {
        DataFile("anchor-a.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("anchor-b.parquet", FileKind::Delete(), /*first_row_id=*/4, /*row_count=*/2),
        DataFile("compacted-a.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("compacted-b.parquet", FileKind::Add(), /*first_row_id=*/4, /*row_count=*/2)};
    std::vector<IndexManifestEntry> base_index_entries = {
        DvIndex("dv-1", FileKind::Add(), {"anchor-a.parquet", "anchor-b.parquet"})};

    ASSERT_NOK_WITH_MSG(CheckCompaction(detection, base_entries, compact_delta,
                                        {DvIndex("dv-1", FileKind::Delete(),
                                                 {"anchor-a.parquet", "anchor-b.parquet"}),
                                         DvIndex("dv-2", FileKind::Add(), {"compacted-a.parquet"})},
                                        base_index_entries),
                        "compacted-b.parquet, but writes no deletion vector for it");

    // Both groups carried over is what the rewriter produces, and that commit is accepted.
    ASSERT_OK(CheckCompaction(
        detection, base_entries, compact_delta,
        {DvIndex("dv-1", FileKind::Delete(), {"anchor-a.parquet", "anchor-b.parquet"}),
         DvIndex("dv-2", FileKind::Add(), {"compacted-a.parquet", "compacted-b.parquet"})},
        base_index_entries));
}

TEST_F(DataEvolutionConflictDetectionTest, TestRefusesADroppedGroupNoOutputCoversItsRows) {
    // The commit compacts one group of the partition and drops a second one whose rows no
    // output covers. Its vector is removed with it, so those deletions have nowhere to go and
    // the rows they hid would come back. The partition does write data, which is what tells
    // this apart from a range that was rewritten to nothing at all.
    ConflictDetection detection = DataEvolutionDetector(table_schema_, core_options_);

    std::vector<ManifestEntry> base_entries = {
        DataFile("anchor-a.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("anchor-b.parquet", FileKind::Add(), /*first_row_id=*/4, /*row_count=*/2)};
    // Both groups are dropped, but only the second one is rewritten.
    std::vector<ManifestEntry> compact_delta = {
        DataFile("anchor-a.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("anchor-b.parquet", FileKind::Delete(), /*first_row_id=*/4, /*row_count=*/2),
        DataFile("compacted-b.parquet", FileKind::Add(), /*first_row_id=*/4, /*row_count=*/2)};
    std::vector<IndexManifestEntry> base_index_entries = {
        DvIndex("dv-1", FileKind::Add(), {"anchor-a.parquet"})};
    std::vector<IndexManifestEntry> delta_index_entries = {
        DvIndex("dv-1", FileKind::Delete(), {"anchor-a.parquet"})};

    ASSERT_NOK_WITH_MSG(CheckCompaction(detection, base_entries, compact_delta, delta_index_entries,
                                        base_index_entries),
                        "drops data file anchor-a.parquet and removes its deletion vector, but "
                        "writes no file covering its rows");

    // Rewriting that group too, with its deletions moved onto the output, is accepted.
    std::vector<ManifestEntry> full_compact_delta = compact_delta;
    full_compact_delta.push_back(
        DataFile("compacted-a.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2));
    ASSERT_OK(CheckCompaction(detection, base_entries, full_compact_delta,
                              {DvIndex("dv-1", FileKind::Delete(), {"anchor-a.parquet"}),
                               DvIndex("dv-2", FileKind::Add(), {"compacted-a.parquet"})},
                              base_index_entries));
}

TEST_F(DataEvolutionConflictDetectionTest, TestRefusesTwoIndexFilesClaimingTheSameDataFile) {
    // A data file is covered by at most one deletion vector, so two index files of the same
    // partition and bucket claiming the same file leave the migration undecidable: which
    // cardinality the output has to carry over depends on which one is believed. The commit is
    // refused rather than resolved by iteration order.
    ConflictDetection detection = DataEvolutionDetector(table_schema_, core_options_);

    std::vector<ManifestEntry> base_entries = {
        DataFile("anchor.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2)};
    std::vector<ManifestEntry> compact_delta = {
        DataFile("anchor.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("compacted.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2)};
    std::vector<IndexManifestEntry> base_index_entries = {
        DvIndex("dv-1", FileKind::Add(), {"anchor.parquet"}),
        DvIndex("dv-2", FileKind::Add(), {"anchor.parquet"})};
    std::vector<IndexManifestEntry> delta_index_entries = {
        DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet"}),
        DvIndex("dv-3", FileKind::Add(), {"compacted.parquet"})};

    ASSERT_NOK_WITH_MSG(CheckCompaction(detection, base_entries, compact_delta, delta_index_entries,
                                        base_index_entries),
                        "anchor.parquet has a deletion vector in both index file");
}

TEST_F(DataEvolutionConflictDetectionTest, TestRefusesAShrunkenMigratedDeletionVector) {
    // The vector reaches the compacted file but deletes fewer rows than the groups it absorbed.
    // Row ranges of distinct groups are disjoint, so the merged vector has to delete exactly as
    // many rows as they did together.
    ConflictDetection detection = DataEvolutionDetector(table_schema_, core_options_);

    std::vector<ManifestEntry> base_entries = {
        DataFile("anchor.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/20)};
    std::vector<ManifestEntry> compact_delta = {
        DataFile("anchor.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/20),
        DataFile("compacted.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/20)};
    std::vector<IndexManifestEntry> base_index_entries = {
        DvIndex("dv-1", FileKind::Add(), {"anchor.parquet"}, /*cardinality=*/10)};

    ASSERT_NOK_WITH_MSG(
        CheckCompaction(
            detection, base_entries, compact_delta,
            {DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet"}, /*cardinality=*/10),
             DvIndex("dv-2", FileKind::Add(), {"compacted.parquet"}, /*cardinality=*/1)},
            base_index_entries),
        "carrying 10 deleted rows into compacted.parquet, but its new deletion vector deletes 1");

    ASSERT_OK(CheckCompaction(
        detection, base_entries, compact_delta,
        {DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet"}, /*cardinality=*/10),
         DvIndex("dv-2", FileKind::Add(), {"compacted.parquet"}, /*cardinality=*/10)},
        base_index_entries));
}

TEST_F(DataEvolutionConflictDetectionTest, TestAcceptsAMaterializedCompaction) {
    // Materializing applies the deletions to the rows themselves and writes them without row
    // ids, which the commit assigns afresh. The old vectors are meant to disappear rather than
    // move, so the group accounting must not demand a new vector for the rewritten files.
    ConflictDetection detection = DataEvolutionDetector(table_schema_, core_options_);

    std::vector<ManifestEntry> base_entries = {
        DataFile("anchor.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/10)};
    // The rewritten file carries no row id: that is the materialization marker.
    std::vector<ManifestEntry> materialize_delta = {
        DataFile("anchor.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/10),
        CreateManifestEntry("materialized.parquet", partition_, FileKind::Add())};
    std::vector<IndexManifestEntry> base_index_entries = {
        DvIndex("dv-1", FileKind::Add(), {"anchor.parquet"}, /*cardinality=*/4)};
    // dv-1 goes away and nothing replaces it, because the deleted rows are gone for real.
    std::vector<IndexManifestEntry> delta_index_entries = {
        DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet"}, /*cardinality=*/4)};

    ASSERT_OK(CheckCompaction(detection, base_entries, materialize_delta, delta_index_entries,
                              base_index_entries));

    // The vector still has to be removed: leaving it behind would keep deleting rows of a file
    // that no longer exists, and the check does not stop caring just because rows were rewritten.
    ASSERT_NOK_WITH_MSG(CheckCompaction(detection, base_entries, materialize_delta,
                                        /*delta_index_entries=*/{}, base_index_entries),
                        "is not removed by the same commit");
}

TEST_F(DataEvolutionConflictDetectionTest, TestAcceptsAFullyDeletedRangeRewrittenToNothing) {
    // Materializing a range whose rows are all deleted produces no output file at all. There is
    // then nothing to attribute the deletions to, and nothing needs to be: the rows are gone
    // with their files, so removing the vector is the whole migration.
    ConflictDetection detection = DataEvolutionDetector(table_schema_, core_options_);

    std::vector<ManifestEntry> base_entries = {
        DataFile("anchor.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2)};
    std::vector<ManifestEntry> delta = {
        DataFile("anchor.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/2)};
    std::vector<IndexManifestEntry> base_index_entries = {
        DvIndex("dv-1", FileKind::Add(), {"anchor.parquet"}, /*cardinality=*/2)};
    std::vector<IndexManifestEntry> delta_index_entries = {
        DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet"}, /*cardinality=*/2)};

    ASSERT_OK(
        CheckCompaction(detection, base_entries, delta, delta_index_entries, base_index_entries));

    // The vector still has to go: leaving it would keep an entry for a file that no longer
    // exists, which is the one thing this check is here to catch.
    ASSERT_NOK_WITH_MSG(CheckCompaction(detection, base_entries, delta,
                                        /*delta_index_entries=*/{}, base_index_entries),
                        "is not removed by the same commit");
}

TEST_F(DataEvolutionConflictDetectionTest, TestRefusesDroppingAKeptFilesDeletions) {
    // The index file the compaction replaces also held a vector for a file outside the compacted
    // groups. Removing it without writing that vector back would resurrect its deleted rows.
    ConflictDetection detection = DataEvolutionDetector(table_schema_, core_options_);

    std::vector<ManifestEntry> base_entries = {
        DataFile("anchor.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("untouched.parquet", FileKind::Add(), /*first_row_id=*/8, /*row_count=*/2)};
    std::vector<ManifestEntry> compact_delta = {
        DataFile("anchor.parquet", FileKind::Delete(), /*first_row_id=*/0, /*row_count=*/2),
        DataFile("compacted.parquet", FileKind::Add(), /*first_row_id=*/0, /*row_count=*/2)};
    std::vector<IndexManifestEntry> base_index_entries = {
        DvIndex("dv-1", FileKind::Add(), {"anchor.parquet", "untouched.parquet"})};

    // dv-2 only carries the moved vector; untouched.parquet's vector is dropped with dv-1.
    ASSERT_NOK_WITH_MSG(
        CheckCompaction(
            detection, base_entries, compact_delta,
            {DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet", "untouched.parquet"}),
             DvIndex("dv-2", FileKind::Add(), {"compacted.parquet"})},
            base_index_entries),
        "data file untouched.parquet, which the commit keeps, does not get its vector back");

    // Carried over, but shrunk on the way: an untouched vector is rewritten verbatim, so this is
    // a regression rather than a legitimate rewrite.
    ASSERT_NOK_WITH_MSG(
        CheckCompaction(
            detection, base_entries, compact_delta,
            {DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet", "untouched.parquet"}),
             DvIndex("dv-2", FileKind::Add(), {"compacted.parquet"}),
             DvIndex("dv-3", FileKind::Add(), {"untouched.parquet"}, /*cardinality=*/0)},
            base_index_entries),
        "which it keeps, from 1 deleted rows to 0");

    // Carried over unchanged is what the rewriter does, and that commit is accepted.
    ASSERT_OK(CheckCompaction(
        detection, base_entries, compact_delta,
        {DvIndex("dv-1", FileKind::Delete(), {"anchor.parquet", "untouched.parquet"}),
         DvIndex("dv-2", FileKind::Add(), {"compacted.parquet", "untouched.parquet"})},
        base_index_entries));
}

TEST_F(ConflictDetectionTest, TestCompactionOnPlainAppendTableStillRefusesDroppingDataFiles) {
    // The carve-out is specific to data evolution: a plain append table's compaction reorders
    // rows and has no vector migration, so it must stay refused.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::BUCKET, "-1"},
                                               {Options::DELETION_VECTORS_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    ASSERT_NOK_WITH_MSG(
        CheckConflicts(detection, {CreateManifestEntry("dropped.parquet", FileKind::Add())},
                       {CreateManifestEntry("dropped.parquet", FileKind::Delete())},
                       Snapshot::CommitKind::Compact()),
        "Committing the deletion of data file dropped.parquet is not supported");
}

TEST_F(ConflictDetectionTest, TestDeletionVectorsDeleteAllowedWithoutBucketUnawareMode) {
    // the guard above is specific to a table whose deletion vectors the entries cannot reference:
    // a bucketed table pairs them by bucket, so deleting a data file stays allowed there
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::BUCKET, "2"},
                                               {Options::DELETION_VECTORS_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    ASSERT_OK(CheckConflicts(detection, {CreateManifestEntry("dropped.parquet", FileKind::Add())},
                             {CreateManifestEntry("dropped.parquet", FileKind::Delete())},
                             Snapshot::CommitKind::Append()));
}

TEST_F(ConflictDetectionTest, TestDeletionVectorsAllowedWithResolvedDynamicBucketMode) {
    auto fields = {arrow::field("f0", arrow::int32(), /*nullable=*/false),
                   arrow::field("f1", arrow::int32(), /*nullable=*/false),
                   arrow::field("f2", arrow::int32())};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{"f1", "f0"}, {{Options::BUCKET, "-1"}}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::BUCKET, "-1"},
                                               {Options::DELETION_VECTORS_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    ASSERT_OK(CheckConflicts(detection, /*base_entries=*/{}, /*delta_entries=*/{},
                             Snapshot::CommitKind::Append()));
}

TEST_F(ConflictDetectionTest, TestCheckLsmKeyRangeConflict) {
    auto fields = {arrow::field("f0", arrow::int32(), /*nullable=*/false),
                   arrow::field("f1", arrow::int32(), /*nullable=*/false),
                   arrow::field("f2", arrow::int32())};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{"f1", "f0"}, {{Options::BUCKET, "4"}}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({{Options::BUCKET, "4"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                CreatePathFactory({"f1"}));

    const BinaryRow partition = CreateIntRow(10);
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   CreateIntRow(1), CreateIntRow(3),
                                                   /*level=*/1));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(), CreateIntRow(3),
                                              CreateIntRow(5), /*level=*/1));
        ASSERT_NOK_WITH_MSG(
            CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()),
            "LSM conflicts detected");
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   CreateIntRow(1), CreateIntRow(3),
                                                   /*level=*/1));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(), CreateIntRow(4),
                                              CreateIntRow(5), /*level=*/1));
        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   CreateIntRow(1), CreateIntRow(3),
                                                   /*level=*/0));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(), CreateIntRow(2),
                                              CreateIntRow(5), /*level=*/0));
        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   CreateIntRow(1), CreateIntRow(3),
                                                   /*level=*/1, /*bucket=*/0));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", partition, FileKind::Add(), CreateIntRow(2),
                                              CreateIntRow(5), /*level=*/1,
                                              /*bucket=*/1));
        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntry("base", partition, FileKind::Add(),
                                                   CreateIntRow(1), CreateIntRow(3),
                                                   /*level=*/1));
        std::vector<ManifestEntry> changes;
        changes.push_back(CreateManifestEntry("delta", CreateIntRow(20), FileKind::Add(),
                                              CreateIntRow(2), CreateIntRow(5), /*level=*/1));
        ASSERT_OK(CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()));
    }
}

TEST_F(ConflictDetectionTest, TestShouldBeOverwriteCommit) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    std::vector<ManifestEntry> add_only_entries;
    add_only_entries.push_back(CreateManifestEntry("f1", FileKind::Add()));
    add_only_entries.push_back(CreateManifestEntry("f2", FileKind::Add()));
    ASSERT_FALSE(detection.ShouldBeOverwriteCommit(add_only_entries, /*append_index_files=*/{}));

    ASSERT_FALSE(detection.ShouldBeOverwriteCommit(/*append_table_files=*/{},
                                                   /*append_index_files=*/{}));

    std::vector<ManifestEntry> delete_entries;
    delete_entries.push_back(CreateManifestEntry("f1", FileKind::Delete()));
    delete_entries.push_back(CreateManifestEntry("f2", FileKind::Add()));
    ASSERT_TRUE(detection.ShouldBeOverwriteCommit(delete_entries, /*append_index_files=*/{}));

    const BinaryRow partition = CreateIntRow(10);
    std::vector<IndexManifestEntry> dv_index_files;
    dv_index_files.push_back(CreateDvIndexEntry("dv1", partition, /*bucket=*/0));
    ASSERT_TRUE(detection.ShouldBeOverwriteCommit(/*append_table_files=*/{}, dv_index_files));
}

TEST_F(ConflictDetectionTest, TestCheckRowIdExistenceNormalFiles) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);
    const BinaryRow partition = CreateIntRow(10);

    // No conflict: delta references the same row-id range as an existing data file.
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntryWithFirstRowId(
            "f1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/100));
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "p1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/100));
        ASSERT_OK(CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                              /*next_row_id=*/100, Snapshot::CommitKind::Append()));
    }

    // Base data file removed: no matching range remains.
    {
        std::vector<ManifestEntry> base_entries;
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "p1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/100));
        ASSERT_NOK_WITH_MSG(
            CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                        /*next_row_id=*/100, Snapshot::CommitKind::Append()),
            "Row ID existence conflict");
    }

    // Base data file rewritten with a different range.
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntryWithFirstRowId(
            "f2", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/200));
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "p1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/100));
        ASSERT_NOK_WITH_MSG(
            CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                        /*next_row_id=*/200, Snapshot::CommitKind::Append()),
            "Row ID existence conflict");
    }

    // Normal file must match exactly one data range, not span adjacent files.
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntryWithFirstRowId(
            "f1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/2));
        base_entries.push_back(CreateManifestEntryWithFirstRowId(
            "f2", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/2, /*row_count=*/2));
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "p1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/4));
        ASSERT_NOK_WITH_MSG(
            CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                        /*next_row_id=*/4, Snapshot::CommitKind::Append()),
            "Row ID existence conflict");
    }

    // Newly appended files (firstRowId >= nextRowId) are skipped.
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntryWithFirstRowId(
            "f1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/100));
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "p1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/100));
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "new1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/100,
            /*row_count=*/50));
        ASSERT_OK(CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                              /*next_row_id=*/100, Snapshot::CommitKind::Append()));
    }

    // Files without a pre-assigned first row id are skipped.
    {
        std::vector<ManifestEntry> base_entries;
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntry("f1", partition, FileKind::Add()));
        ASSERT_OK(CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                              /*next_row_id=*/100, Snapshot::CommitKind::Append()));
    }

    // A null nextRowId disables row-id existence checking.
    {
        std::vector<ManifestEntry> base_entries;
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "p1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/100));
        ASSERT_OK(CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                              /*next_row_id=*/std::nullopt,
                                              Snapshot::CommitKind::Append()));
    }
}

TEST_F(ConflictDetectionTest, TestCheckRowIdExistenceDedicatedFiles) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);
    const BinaryRow partition = CreateIntRow(10);

    // Dedicated file contained within a single data range is allowed.
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntryWithFirstRowId(
            "f1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/4));
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "p1.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0,
            /*row_count=*/2));
        ASSERT_OK(CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                              /*next_row_id=*/4, Snapshot::CommitKind::Append()));
    }

    // Dedicated file spanning adjacent data files is rejected.
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntryWithFirstRowId(
            "f1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/2));
        base_entries.push_back(CreateManifestEntryWithFirstRowId(
            "f2", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/2, /*row_count=*/2));
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "p1.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0,
            /*row_count=*/4));
        ASSERT_NOK_WITH_MSG(
            CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                        /*next_row_id=*/4, Snapshot::CommitKind::Append()),
            "Row ID existence conflict");
    }

    // Dedicated file whose range is not covered by one data file is rejected.
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntryWithFirstRowId(
            "f1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/2));
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "p1.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0,
            /*row_count=*/3));
        ASSERT_NOK_WITH_MSG(
            CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                        /*next_row_id=*/3, Snapshot::CommitKind::Append()),
            "Row ID existence conflict");
    }

    // Base dedicated files are ignored when building existing data ranges.
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntryWithFirstRowId(
            "old.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0,
            /*row_count=*/2));
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId(
            "p1.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0,
            /*row_count=*/2));
        ASSERT_NOK_WITH_MSG(
            CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                        /*next_row_id=*/2, Snapshot::CommitKind::Append()),
            "Row ID existence conflict");
    }
}

TEST_F(ConflictDetectionTest, TestCheckRowIdExistenceOnCompaction) {
    // A compaction rewrites rows the table already holds, so every range it adds has to be
    // covered by the current snapshot's ranges of the same partition and bucket. A range that
    // is not means a concurrent commit reassigned those rows after the round was planned, and
    // committing would move the row ids back onto rows that no longer carry them.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);
    const BinaryRow partition = CreateIntRow(10);

    std::vector<ManifestEntry> base_entries;
    base_entries.push_back(CreateManifestEntryWithFirstRowId(
        "base-0", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/2));
    base_entries.push_back(CreateManifestEntryWithFirstRowId(
        "base-1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/2, /*row_count=*/2));
    std::vector<ManifestEntry> replaced = {
        CreateManifestEntryWithFirstRowId("base-0", partition, FileKind::Delete(), /*bucket=*/0,
                                          /*first_row_id=*/0, /*row_count=*/2),
        CreateManifestEntryWithFirstRowId("base-1", partition, FileKind::Delete(), /*bucket=*/0,
                                          /*first_row_id=*/2, /*row_count=*/2)};

    // The output spans both base files: adjacent ranges are merged, so a contiguous run of them
    // counts as covered. `next_row_id` is irrelevant here - a compaction reuses existing ids.
    {
        std::vector<ManifestEntry> delta_entries = replaced;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId("compacted", partition,
                                                                  FileKind::Add(), /*bucket=*/0,
                                                                  /*first_row_id=*/0,
                                                                  /*row_count=*/4));
        ASSERT_OK(CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                              /*next_row_id=*/std::nullopt,
                                              Snapshot::CommitKind::Compact()));
    }

    // A range the snapshot no longer holds: a concurrent commit reassigned those rows to
    // [10, 13], so committing the planned output would move the row ids back to [0, 3].
    {
        std::vector<ManifestEntry> reassigned_base;
        reassigned_base.push_back(CreateManifestEntryWithFirstRowId(
            "base-2", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/10,
            /*row_count=*/4));
        std::vector<ManifestEntry> delta_entries;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId("compacted", partition,
                                                                  FileKind::Add(), /*bucket=*/0,
                                                                  /*first_row_id=*/0,
                                                                  /*row_count=*/4));
        ASSERT_NOK_WITH_MSG(CheckConflictsWithNextRowId(detection, reassigned_base, delta_entries,
                                                        /*next_row_id=*/std::nullopt,
                                                        Snapshot::CommitKind::Compact()),
                            "Row ID existence conflict");
    }

    // The rows exist, but in another bucket: an output may only take over files of its own.
    {
        std::vector<ManifestEntry> delta_entries = replaced;
        delta_entries.push_back(CreateManifestEntryWithFirstRowId("compacted", partition,
                                                                  FileKind::Add(), /*bucket=*/1,
                                                                  /*first_row_id=*/0,
                                                                  /*row_count=*/4));
        ASSERT_NOK_WITH_MSG(CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                                        /*next_row_id=*/std::nullopt,
                                                        Snapshot::CommitKind::Compact()),
                            "Row ID existence conflict");
    }

    // A materialized compaction writes its files without row ids, which the commit assigns
    // afresh, so there is nothing to look up.
    {
        std::vector<ManifestEntry> delta_entries = replaced;
        delta_entries.push_back(CreateManifestEntry("materialized", partition, FileKind::Add()));
        ASSERT_OK(CheckConflictsWithNextRowId(detection, base_entries, delta_entries,
                                              /*next_row_id=*/std::nullopt,
                                              Snapshot::CommitKind::Compact()));
    }
}

TEST_F(ConflictDetectionTest, TestGlobalIndexRowIdExistenceByPartitionAndBucket) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    const BinaryRow partition0 = CreateIntRow(0);
    const BinaryRow partition1 = CreateIntRow(1);

    // Index in partition0 but data lives in partition1 -> conflict.
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntryWithFirstRowId("f1", partition1, FileKind::Add(),
                                                                 /*bucket=*/0, /*first_row_id=*/0,
                                                                 /*row_count=*/150));
        ASSERT_NOK_WITH_MSG(
            CheckConflicts(detection, base_entries, /*delta_entries=*/{},
                           {CreateGlobalIndexEntry("idx", partition0, /*bucket=*/0,
                                                   /*row_range_start=*/0, /*row_range_end=*/149)},
                           Snapshot::CommitKind::Append()),
            "Global index row ID existence conflict");
    }

    // Index in bucket 0 but data lives in bucket 1 -> conflict.
    {
        std::vector<ManifestEntry> base_entries;
        base_entries.push_back(CreateManifestEntryWithFirstRowId("f1", partition0, FileKind::Add(),
                                                                 /*bucket=*/1, /*first_row_id=*/0,
                                                                 /*row_count=*/150));
        ASSERT_NOK_WITH_MSG(
            CheckConflicts(detection, base_entries, /*delta_entries=*/{},
                           {CreateGlobalIndexEntry("idx", partition0, /*bucket=*/0,
                                                   /*row_range_start=*/0, /*row_range_end=*/149)},
                           Snapshot::CommitKind::Append()),
            "Global index row ID existence conflict");
    }
}

TEST_F(ConflictDetectionTest, TestGlobalIndexRowIdExistenceSkipsDeleteIndexEntry) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);
    const BinaryRow partition = CreateIntRow(10);

    ASSERT_OK(
        CheckConflicts(detection, /*base_entries=*/{}, /*delta_entries=*/{},
                       {CreateGlobalIndexEntry("idx", partition, /*bucket=*/0, FileKind::Delete(),
                                               /*row_range_start=*/0, /*row_range_end=*/149)},
                       Snapshot::CommitKind::Append()));
}

TEST_F(ConflictDetectionTest, TestCheckRowIdRangeConflictsAllowsAdjacentDataFiles) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);
    const BinaryRow partition = CreateIntRow(10);

    std::vector<ManifestEntry> base_entries;
    base_entries.push_back(CreateManifestEntryWithFirstRowId(
        "f1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/2));
    base_entries.push_back(CreateManifestEntryWithFirstRowId(
        "f2", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/2, /*row_count=*/2));
    ASSERT_OK(CheckConflicts(detection, base_entries, /*delta_entries=*/{},
                             Snapshot::CommitKind::Compact()));
}

TEST_F(ConflictDetectionTest, TestCheckRowIdRangeConflictsAllowsDedicatedFileCoveredByOneDataFile) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::DATA_EVOLUTION_ENABLED, "true"}}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);
    const BinaryRow partition = CreateIntRow(10);

    std::vector<ManifestEntry> base_entries;
    base_entries.push_back(CreateManifestEntryWithFirstRowId(
        "f1", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/0, /*row_count=*/4));
    std::vector<ManifestEntry> delta_entries;
    delta_entries.push_back(CreateManifestEntryWithFirstRowId(
        "p1.blob", partition, FileKind::Add(), /*bucket=*/0, /*first_row_id=*/1, /*row_count=*/2));
    ASSERT_OK(
        CheckConflicts(detection, base_entries, delta_entries, Snapshot::CommitKind::Compact()));
}

TEST_F(ConflictDetectionTest, TestConflictMessageTruncatesLargeEntryList) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow::schema(fields_), /*partition_keys=*/{"f1"},
                            /*primary_keys=*/{}, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap({}));
    ConflictDetection detection(table_schema, core_options, /*snapshot_manager=*/nullptr,
                                /*manifest_list=*/nullptr, /*manifest_file=*/nullptr,
                                /*commit_scanner=*/nullptr, "test_user", "test_table",
                                /*path_factory=*/nullptr);

    // kMaxEntry in conflict_detection.cpp is 50. Exceed it so the conflict message appends the
    // "not fully displayed" truncation hint. Each lone DELETE (no matching base add) is a
    // deletion conflict, so the delta entry list drives the message.
    constexpr int kEntryCount = 60;
    std::vector<ManifestEntry> base_entries;
    std::vector<ManifestEntry> changes;
    changes.reserve(kEntryCount);
    for (int i = 0; i < kEntryCount; ++i) {
        changes.push_back(CreateManifestEntry("delete-" + std::to_string(i), FileKind::Delete()));
    }

    ASSERT_NOK_WITH_MSG(
        CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()),
        "which is not previously added");
    ASSERT_NOK_WITH_MSG(
        CheckConflicts(detection, base_entries, changes, Snapshot::CommitKind::Append()),
        "not fully displayed");
}

}  // namespace paimon::test
