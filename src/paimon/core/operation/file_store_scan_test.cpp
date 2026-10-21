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

#include "paimon/core/operation/file_store_scan.h"

#include <string>

#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/core/manifest/snapshot_live_manifest_entries.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_segment.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class FileStoreScanTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

 private:
    std::shared_ptr<arrow::Schema> schema_ = arrow::schema(arrow::FieldVector(
        {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
         arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())}));
};

TEST_F(FileStoreScanTest, TestCreatePartitionPredicate) {
    std::vector<std::string> partition_keys = {"f1"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, {partition_filters}));
    ASSERT_EQ(partition_predicate->ToString(), "Equal(f1, 1)");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithMultiPartitionKeys) {
    std::vector<std::string> partition_keys = {"f1", "f0"};

    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    partition_filters["f0"] = "hello";
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, {partition_filters}));
    ASSERT_EQ(partition_predicate->ToString(), "And([Equal(f0, hello), Equal(f1, 1)])");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithDefaultPartitionName) {
    std::vector<std::string> partition_keys = {"f1", "f0"};

    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    partition_filters["f0"] = partition_default_name;
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, {partition_filters}));
    ASSERT_EQ(partition_predicate->ToString(), "And([IsNull(f0), Equal(f1, 1)])");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithMultiPartitionFilters) {
    std::vector<std::string> partition_keys = {"f1", "f0"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::vector<std::map<std::string, std::string>> partition_filters = {
        {{"f1", "1"}, {"f0", "hello"}}, {{"f1", "2"}, {"f0", "world"}}};
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, partition_filters));
    ASSERT_EQ(partition_predicate->ToString(),
              "Or([And([Equal(f0, hello), Equal(f1, 1)]), And([Equal(f0, world), Equal(f1, 2)])])");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithOneEmptyPartitionFilter) {
    std::vector<std::string> partition_keys = {"f1", "f0"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    // empty map in partition_filters indicates a all_true partition filter
    std::vector<std::map<std::string, std::string>> partition_filters = {
        {{"f1", "1"}, {"f0", "hello"}}, {}};
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, partition_filters));
    ASSERT_FALSE(partition_predicate);
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithEmptyPartitionFilters) {
    std::vector<std::string> partition_keys = {"f1", "f0"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::vector<std::map<std::string, std::string>> partition_filters = {};
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, partition_filters));
    ASSERT_FALSE(partition_predicate);
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithInvalidPartitionKeys) {
    std::vector<std::string> partition_keys = {"invalid"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    ASSERT_NOK_WITH_MSG(FileStoreScan::CreatePartitionPredicate(
                            partition_keys, partition_default_name, schema_, {partition_filters}),
                        "field invalid does not exist in schema");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithInvalidPartitionFilter) {
    std::vector<std::string> partition_keys = {"f1"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["invalid"] = "1";
    ASSERT_NOK_WITH_MSG(FileStoreScan::CreatePartitionPredicate(
                            partition_keys, partition_default_name, schema_, {partition_filters}),
                        "field invalid does not exist in partition keys");
}

TEST_F(FileStoreScanTest, TestFullTextMatchDoesNotEnterPartitionOrStatsFilters) {
    auto match = std::make_shared<FullTextMatch>("f0", R"({"match":{"query":"apple"}})");
    ASSERT_OK_AND_ASSIGN(auto full_text, PredicateBuilder::FullTextMatch(/*field_index=*/0, match));
    auto scalar = PredicateBuilder::Equal(/*field_index=*/1, "f1", FieldType::INT, Literal(1));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::Or({scalar, full_text}));
    auto filters = std::make_shared<ScanFilter>(
        predicate, std::vector<std::map<std::string, std::string>>{}, std::nullopt);
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    for (const std::vector<std::string>& partition_keys :
         {std::vector<std::string>{}, std::vector<std::string>{"f0"}}) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema_, partition_keys, /*primary_keys=*/{}, {}));
        auto result = AppendOnlyFileStoreScan::Create(
            /*snapshot_manager=*/nullptr, /*schema_manager=*/nullptr, /*manifest_list=*/nullptr,
            /*manifest_file=*/nullptr, table_schema, schema_, filters, options,
            CreateDefaultExecutor(), GetDefaultPool());
        ASSERT_NOK_WITH_MSG(result, "FullTextMatch predicate execution is not implemented yet");
        ASSERT_TRUE(result.status().IsNotImplemented());
    }
}

TEST_F(FileStoreScanTest, TestFilterManifestByRowRanges) {
    class FakeFileStoreScan : public FileStoreScan {
     public:
        FakeFileStoreScan(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                          const std::shared_ptr<SchemaManager>& schema_manager,
                          const std::shared_ptr<ManifestList>& manifest_list,
                          const std::shared_ptr<ManifestFile>& manifest_file,
                          const std::shared_ptr<TableSchema>& table_schema,
                          const std::shared_ptr<arrow::Schema>& schema,
                          const CoreOptions& core_options,
                          const std::shared_ptr<Executor>& executor,
                          const std::shared_ptr<MemoryPool>& pool)
            : FileStoreScan(snapshot_manager, schema_manager, manifest_list, manifest_file,
                            table_schema, schema, core_options, executor, pool) {}
        Result<bool> FilterByStats(const ManifestEntry& entry) const override {
            return false;
        }
    };
    // row id [10, 20]
    auto manifest1 =
        ManifestFileMeta("manifest-65b0d403-a1bc-4157-b242-bff73c46596d-0", /*file_size=*/2779,
                         /*num_added_files=*/1, /*num_deleted_files=*/0, SimpleStats::EmptyStats(),
                         /*schema_id=*/0, /*min_bucket=*/0, /*max_bucket=*/0,
                         /*min_level=*/0, /*max_level=*/0,
                         /*min_row_id=*/10, /*max_row_id=*/20);

    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({{}}));
    auto file_store_scan = std::make_shared<FakeFileStoreScan>(
        /*snapshot_manager=*/nullptr, /*schema_manager=*/nullptr, /*manifest_list=*/nullptr,
        /*manifest_file=*/nullptr, /*table_schema=*/nullptr, /*schema=*/nullptr, options,
        /*executor=*/CreateDefaultExecutor(), GetDefaultPool());
    ASSERT_TRUE(file_store_scan->FilterManifestByRowRanges(manifest1));

    ASSERT_OK_AND_ASSIGN(
        RowRangeIndex row_range_index,
        RowRangeIndex::Create(std::vector<Range>({Range(0, 15), Range(100, 200)})));
    file_store_scan->WithRowRangeIndex(row_range_index);
    ASSERT_TRUE(file_store_scan->FilterManifestByRowRanges(manifest1));

    ASSERT_OK_AND_ASSIGN(row_range_index,
                         RowRangeIndex::Create(std::vector<Range>({Range(0, 5), Range(100, 200)})));
    file_store_scan->WithRowRangeIndex(row_range_index);
    ASSERT_FALSE(file_store_scan->FilterManifestByRowRanges(manifest1));

    auto manifest2 =
        ManifestFileMeta("manifest-65b0d403-a1bc-4157-b242-bff73c46596d-0", /*file_size=*/2779,
                         /*num_added_files=*/1, /*num_deleted_files=*/0, SimpleStats::EmptyStats(),
                         /*schema_id=*/0, /*min_bucket=*/0, /*max_bucket=*/0,
                         /*min_level=*/0, /*max_level=*/0,
                         /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    ASSERT_OK_AND_ASSIGN(row_range_index, RowRangeIndex::Create(std::vector<Range>({Range(0, 0)})));
    file_store_scan->WithRowRangeIndex(row_range_index);
    ASSERT_TRUE(file_store_scan->FilterManifestByRowRanges(manifest2));
}

TEST_F(FileStoreScanTest, TestSnapshotLiveManifestEntriesSerialization) {
    std::vector<ManifestEntry> manifest_entries;
    ASSERT_OK_AND_ASSIGN(
        auto file1,
        DataFileMeta::ForAppend("file-1", /*file_size=*/10, /*row_count=*/1,
                                SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
                                /*max_sequence_number=*/0, /*schema_id=*/0,
                                /*file_source=*/std::nullopt, /*value_stats_cols=*/std::nullopt,
                                /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                                /*write_cols=*/std::nullopt));
    manifest_entries.emplace_back(FileKind::Add(), BinaryRow::EmptyRow(), /*bucket=*/0,
                                  /*total_buckets=*/1, file1);
    SnapshotLiveManifestEntries entries(/*max_snapshots=*/2);
    entries.Put(/*snapshot_id=*/1, std::move(manifest_entries));
    entries.Put(/*snapshot_id=*/3, {});

    ASSERT_OK_AND_ASSIGN(auto bytes, entries.Serialize(GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(auto deserialized,
                         SnapshotLiveManifestEntries::Deserialize(
                             MemorySegment::Wrap(bytes), /*max_snapshots=*/2, GetDefaultPool()));
    ASSERT_EQ(deserialized.Size(), 2);
    ASSERT_OK_AND_ASSIGN(auto round_trip, deserialized.Serialize(GetDefaultPool()));
    ASSERT_EQ(*bytes, *round_trip);

    ASSERT_OK_AND_ASSIGN(auto evicted_deserialized, SnapshotLiveManifestEntries::Deserialize(
                                                        MemorySegment::Wrap(bytes),
                                                        /*max_snapshots=*/1, GetDefaultPool()));
    ASSERT_EQ(evicted_deserialized.Size(), 1);
    ASSERT_OK_AND_ASSIGN(auto retained, evicted_deserialized.Serialize(GetDefaultPool()));
    // Use a larger read limit to verify that Deserialize itself evicted the older snapshot.
    ASSERT_OK_AND_ASSIGN(
        auto evicted, SnapshotLiveManifestEntries::ReadSnapshot(MemorySegment::Wrap(retained), 1, 2,
                                                                nullptr, GetDefaultPool()));
    ASSERT_FALSE(evicted);
    ASSERT_OK_AND_ASSIGN(
        auto empty, SnapshotLiveManifestEntries::ReadSnapshot(MemorySegment::Wrap(retained), 3, 2,
                                                              nullptr, GetDefaultPool()));
    ASSERT_TRUE(empty);
    ASSERT_TRUE(empty->entries.empty());
}

TEST_F(FileStoreScanTest, SelectExactRetainedSnapshot) {
    auto pool = GetDefaultPool();
    std::vector<ManifestEntry> records;
    for (int32_t partition = 0; partition < 3; ++partition) {
        ASSERT_OK_AND_ASSIGN(
            auto file,
            DataFileMeta::ForAppend(fmt::format("file-{}", partition), 10, partition + 1,
                                    SimpleStats::EmptyStats(), 0, 0, 0, std::nullopt, std::nullopt,
                                    std::nullopt, std::nullopt, std::nullopt));
        records.emplace_back(FileKind::Add(),
                             BinaryRowGenerator::GenerateRow({partition}, pool.get()), 0, 1, file);
    }
    SnapshotLiveManifestEntries entries(3);
    entries.Put(1, {records[0]});
    entries.Put(3, {records[1], records[2]});
    entries.Put(5, {});
    ASSERT_OK_AND_ASSIGN(auto bytes, entries.Serialize(pool));
    const MemorySegment segment = MemorySegment::Wrap(bytes);
    ASSERT_OK_AND_ASSIGN(auto first,
                         SnapshotLiveManifestEntries::ReadSnapshot(segment, 1, 3, nullptr, pool));
    ASSERT_TRUE(first);
    ASSERT_EQ(first->entries, std::vector<ManifestEntry>({records[0]}));
    ASSERT_OK_AND_ASSIGN(auto third,
                         SnapshotLiveManifestEntries::ReadSnapshot(segment, 3, 3, nullptr, pool));
    ASSERT_TRUE(third);
    ASSERT_EQ(third->entries, std::vector<ManifestEntry>({records[1], records[2]}));
    int32_t filter_calls = 0;
    ASSERT_OK_AND_ASSIGN(auto selected,
                         SnapshotLiveManifestEntries::ReadSnapshot(
                             segment, 3, 2,
                             [&filter_calls](const InternalRow& row) -> Result<bool> {
                                 ++filter_calls;
                                 PAIMON_ASSIGN_OR_RAISE(
                                     BinaryRow partition,
                                     ManifestEntrySerializer::GetPartitionView(row));
                                 return partition.GetInt(0) == 2;
                             },
                             pool));
    ASSERT_TRUE(selected);
    ASSERT_EQ(selected->candidate_entries, 2);
    ASSERT_EQ(filter_calls, 2);
    ASSERT_EQ(selected->entries, std::vector<ManifestEntry>({records[2]}));
    ASSERT_OK_AND_ASSIGN(auto absent,
                         SnapshotLiveManifestEntries::ReadSnapshot(segment, 2, 3, nullptr, pool));
    ASSERT_FALSE(absent);
    ASSERT_OK_AND_ASSIGN(auto evicted,
                         SnapshotLiveManifestEntries::ReadSnapshot(segment, 3, 1, nullptr, pool));
    ASSERT_FALSE(evicted);
    ASSERT_OK_AND_ASSIGN(auto disabled,
                         SnapshotLiveManifestEntries::ReadSnapshot(segment, 3, 0, nullptr, pool));
    ASSERT_FALSE(disabled);
    // Skipping other snapshots must not allocate row payloads in the reader's pool.
    std::shared_ptr<MemoryPool> read_pool = GetMemoryPool();
    ASSERT_OK_AND_ASSIGN(
        auto empty, SnapshotLiveManifestEntries::ReadSnapshot(segment, 5, 1, nullptr, read_pool));
    ASSERT_TRUE(empty);
    ASSERT_TRUE(empty->entries.empty());
    ASSERT_EQ(read_pool->MaxMemoryUsage(), 0);
    ASSERT_NOK_WITH_MSG(SnapshotLiveManifestEntries::ReadSnapshot(
                            segment, 3, 3,
                            [](const InternalRow&) -> Result<bool> {
                                return Status::IOError("injected filter error");
                            },
                            pool),
                        "injected filter error");

    entries.Put(7, {});
    ASSERT_EQ(entries.Size(), 3);
    ASSERT_OK_AND_ASSIGN(auto updated, entries.Serialize(pool));
    // Put must evict the oldest snapshot even when the reader permits more snapshots.
    ASSERT_OK_AND_ASSIGN(auto oldest, SnapshotLiveManifestEntries::ReadSnapshot(
                                          MemorySegment::Wrap(updated), 1, 4, nullptr, pool));
    ASSERT_FALSE(oldest);
    ASSERT_OK_AND_ASSIGN(auto retained, SnapshotLiveManifestEntries::ReadSnapshot(
                                            MemorySegment::Wrap(updated), 3, 4, nullptr, pool));
    ASSERT_TRUE(retained);
    ASSERT_EQ(retained->entries, std::vector<ManifestEntry>({records[1], records[2]}));
}

TEST_F(FileStoreScanTest, SkipDecodingFilteredManifestEntries) {
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(
        auto file,
        DataFileMeta::ForAppend("file", 10, 1, SimpleStats::EmptyStats(), 0, 0, 0, std::nullopt,
                                std::nullopt, std::nullopt, std::nullopt, std::nullopt));
    // Like the bucket-pruning tests, use an invalid kind to detect full entry decoding.
    ManifestEntry excluded(FileKind(static_cast<int8_t>(2)),
                           BinaryRowGenerator::GenerateRow({10}, pool.get()), 0, 1, file);
    SnapshotLiveManifestEntries entries(1);
    entries.Put(1, {excluded});
    ASSERT_OK_AND_ASSIGN(auto bytes, entries.Serialize(pool));
    const MemorySegment segment = MemorySegment::Wrap(bytes);
    ASSERT_OK_AND_ASSIGN(auto selected, SnapshotLiveManifestEntries::ReadSnapshot(
                                            segment, 1, 1,
                                            [](const InternalRow& row) -> Result<bool> {
                                                PAIMON_ASSIGN_OR_RAISE(
                                                    BinaryRow partition,
                                                    ManifestEntrySerializer::GetPartitionView(row));
                                                return partition.GetInt(0) == 20;
                                            },
                                            pool));
    ASSERT_TRUE(selected);
    ASSERT_TRUE(selected->entries.empty());
    ASSERT_NOK_WITH_MSG(SnapshotLiveManifestEntries::ReadSnapshot(segment, 1, 1, nullptr, pool),
                        "Unsupported byte value 2 for file kind.");
}

TEST_F(FileStoreScanTest, RejectUnsupportedSnapshotEntryVersionBeforeFiltering) {
    auto pool = GetDefaultPool();
    // Only the version is present: the reader must reject it before the filter accesses fields.
    BinaryRow row = BinaryRowGenerator::GenerateRow({3}, pool.get());
    BinaryRowSerializer serializer(1, pool);
    MemorySegmentOutputStream out(64, pool);
    out.WriteValue<int32_t>(0x534d4543);
    out.WriteValue<int32_t>(1);
    out.WriteValue<int64_t>(1);
    out.WriteValue<int32_t>(1);
    ASSERT_OK(serializer.Serialize(row, &out));
    bool filter_called = false;
    ASSERT_NOK_WITH_MSG(SnapshotLiveManifestEntries::ReadSnapshot(
                            out.Segments().front(), 1, 1,
                            [&filter_called](const InternalRow&) -> Result<bool> {
                                filter_called = true;
                                return false;
                            },
                            pool),
                        "Unsupported version: 3");
    ASSERT_FALSE(filter_called);
}

TEST_F(FileStoreScanTest, RejectInvalidSnapshotSelectionHeader) {
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(auto absent, SnapshotLiveManifestEntries::ReadSnapshot(MemorySegment(), 1,
                                                                                3, nullptr, pool));
    ASSERT_FALSE(absent);
    auto header = [&pool](int32_t magic, int32_t snapshots, int32_t entries) {
        MemorySegmentOutputStream out(64, pool);
        out.WriteValue<int32_t>(magic);
        out.WriteValue<int32_t>(snapshots);
        out.WriteValue<int64_t>(1);
        out.WriteValue<int32_t>(entries);
        return out.Segments().front();
    };
    auto segment = header(0, 1, 0);
    ASSERT_NOK_WITH_MSG(SnapshotLiveManifestEntries::ReadSnapshot(segment, 1, 3, nullptr, pool),
                        "invalid snapshot live manifest entries magic");
    segment = header(0x534d4543, -1, 0);
    ASSERT_NOK_WITH_MSG(SnapshotLiveManifestEntries::ReadSnapshot(segment, 1, 3, nullptr, pool),
                        "snapshot count is negative");
    segment = header(0x534d4543, 1, -1);
    ASSERT_NOK_WITH_MSG(SnapshotLiveManifestEntries::ReadSnapshot(segment, 1, 3, nullptr, pool),
                        "entry count is negative");
    auto truncated = MemorySegment::WrapView(segment.Data(), 7);
    ASSERT_NOK(SnapshotLiveManifestEntries::ReadSnapshot(truncated, 1, 3, nullptr, pool));
}

}  // namespace paimon::test
