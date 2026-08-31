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

#include "paimon/core/operation/append_only_file_store_scan.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/partition_entry.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolution.h"
#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/scan_metrics.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"
namespace paimon::test {

TEST(AppendOnlyFileStoreScanTest, TestReconstructPredicateWithNonCastedFields) {
    std::string table_root =
        paimon::test::GetDataDir() +
        "/orc/append_table_alter_table_with_cast.db/append_table_alter_table_with_cast";
    auto fs = std::make_shared<LocalFileSystem>();
    auto pool = GetDefaultPool();
    SchemaManager manager(fs, table_root);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, manager.ReadSchema(/*schema_id=*/0));
    ASSERT_TRUE(schema);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema1, manager.ReadSchema(/*schema_id=*/1));
    ASSERT_TRUE(schema1);
    auto evo = std::make_shared<SimpleStatsEvolution>(schema->Fields(), schema1->Fields(),
                                                      /*need_mapping=*/true, pool);
    ASSERT_OK_AND_ASSIGN(
        auto child1,
        PredicateBuilder::Or(
            {PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f4", FieldType::TIMESTAMP),
             PredicateBuilder::IsNull(/*field_index=*/1, /*field_name=*/"key0", FieldType::INT),
             PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"key1", FieldType::INT)}));

    auto sub_child1 =
        PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3", FieldType::INT);
    auto sub_child2 =
        PredicateBuilder::IsNull(/*field_index=*/4, /*field_name=*/"f1", FieldType::STRING);
    auto sub_child3 =
        PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f2", FieldType::DECIMAL);
    ASSERT_OK_AND_ASSIGN(auto child2, PredicateBuilder::And({sub_child1, sub_child2, sub_child3}));

    auto child3 =
        PredicateBuilder::IsNull(/*field_index=*/6, /*field_name=*/"f0", FieldType::BOOLEAN);
    auto child4 = PredicateBuilder::IsNull(/*field_index=*/7, /*field_name=*/"f6", FieldType::INT);

    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2, child3, child4}));

    ASSERT_OK_AND_ASSIGN(
        auto result,
        AppendOnlyFileStoreScan::ReconstructPredicateWithNonCastedFields(predicate, evo));
    ASSERT_EQ(*result, *child4);

    auto key_predicate =
        PredicateBuilder::IsNull(/*field_index=*/1, /*field_name=*/"key0", FieldType::INT);
    ASSERT_OK_AND_ASSIGN(result, AppendOnlyFileStoreScan::ReconstructPredicateWithNonCastedFields(
                                     key_predicate, evo));
    ASSERT_EQ(*result, *key_predicate);
}

TEST(AppendOnlyFileStoreScanTest, TestReadPartitionEntries) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::FILE_FORMAT, "orc")
        .AddOption(Options::MANIFEST_FORMAT, "orc")
        .AddOption(Options::SCAN_SNAPSHOT_ID, "5");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    auto typed_table_scan = dynamic_cast<AbstractTableScan*>(table_scan.get());
    ASSERT_TRUE(typed_table_scan);

    auto file_store_scan = typed_table_scan->snapshot_reader_->scan_;
    ASSERT_TRUE(file_store_scan);
    ASSERT_OK_AND_ASSIGN(std::vector<PartitionEntry> result_partition_entries,
                         file_store_scan->ReadPartitionEntries());

    auto GenerateRow = [&](int32_t value) {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 0, pool.get());
        writer.WriteInt(0, value);
        writer.Complete();
        return row;
    };

    std::vector<PartitionEntry> expected_partition_entries = {
        PartitionEntry(GenerateRow(10), /*record_count=*/9, /*file_size_in_bytes=*/1183,
                       /*file_count=*/2, /*last_file_creation_time=*/1721643834472l - 28800000l,
                       /*total_buckets=*/2),
        PartitionEntry(GenerateRow(20), /*record_count=*/2, /*file_size_in_bytes=*/1047,
                       /*file_count=*/2, /*last_file_creation_time=*/1721643267404l - 28800000l,
                       /*total_buckets=*/2)};
    auto ComparePartitionEntryByPartition = [](const PartitionEntry& lhs,
                                               const PartitionEntry& rhs) -> bool {
        return lhs.Partition().GetInt(0) < rhs.Partition().GetInt(0);
    };

    std::stable_sort(result_partition_entries.begin(), result_partition_entries.end(),
                     ComparePartitionEntryByPartition);
    std::stable_sort(expected_partition_entries.begin(), expected_partition_entries.end(),
                     ComparePartitionEntryByPartition);

    ASSERT_EQ(result_partition_entries, expected_partition_entries);
}

TEST(AppendOnlyFileStoreScanTest, TestScanDurationMetric) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::FILE_FORMAT, "orc")
        .AddOption(Options::MANIFEST_FORMAT, "orc")
        .AddOption(Options::SCAN_SNAPSHOT_ID, "5");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    auto typed_table_scan = dynamic_cast<AbstractTableScan*>(table_scan.get());
    ASSERT_TRUE(typed_table_scan);

    auto file_store_scan = typed_table_scan->snapshot_reader_->scan_;
    ASSERT_TRUE(file_store_scan);

    constexpr uint64_t kPlanCount = 5;
    for (uint64_t i = 0; i < kPlanCount; ++i) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> raw_plan,
                             file_store_scan->CreatePlan());
        (void)raw_plan;
    }

    std::shared_ptr<Metrics> metrics = file_store_scan->GetScanMetrics();
    ASSERT_TRUE(metrics);

    ASSERT_OK_AND_ASSIGN(uint64_t last_scan_duration,
                         metrics->GetCounter(ScanMetrics::LAST_SCAN_DURATION));
    ASSERT_OK_AND_ASSIGN(HistogramStats stats,
                         metrics->GetHistogramStats(ScanMetrics::SCAN_DURATION));
    ASSERT_OK_AND_ASSIGN(uint64_t manifest_read_duration,
                         metrics->GetCounter(ScanMetrics::LAST_MANIFEST_READ_DURATION));
    ASSERT_OK_AND_ASSIGN(HistogramStats manifest_stats,
                         metrics->GetHistogramStats(ScanMetrics::MANIFEST_READ_DURATION));
    ASSERT_OK_AND_ASSIGN(uint64_t scanned_rows,
                         metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_SCANNED_ROWS));
    ASSERT_OK_AND_ASSIGN(uint64_t materialized_rows,
                         metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_MATERIALIZED_ROWS));
    ASSERT_EQ(stats.count, kPlanCount);
    ASSERT_EQ(manifest_stats.count, kPlanCount);
    ASSERT_LE(manifest_stats.min, static_cast<double>(manifest_read_duration));
    ASSERT_LE(static_cast<double>(manifest_read_duration), manifest_stats.max);
    ASSERT_GT(scanned_rows, 0);
    ASSERT_GE(scanned_rows, materialized_rows);
    ASSERT_LE(stats.min, stats.max);
    ASSERT_LE(stats.min, static_cast<double>(last_scan_duration));
    ASSERT_LE(static_cast<double>(last_scan_duration), stats.max);
    ASSERT_LE(stats.min, stats.p99);
    ASSERT_LE(stats.p50, stats.p99);
    ASSERT_LE(stats.p99, stats.max);
}

namespace {

std::shared_ptr<FileStoreScan> BuildScan(const std::string& table_path,
                                         const std::shared_ptr<Cache>& cache,
                                         const std::optional<int32_t>& bucket = std::nullopt,
                                         const std::shared_ptr<Predicate>& predicate = nullptr,
                                         bool manifest_entry_lazy_decode_enabled = true) {
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::FILE_FORMAT, "orc")
        .AddOption(Options::MANIFEST_FORMAT, "orc")
        .AddOption(Options::SCAN_MANIFEST_ENTRY_CACHE_MAX_SNAPSHOTS, "8")
        .AddOption(Options::SCAN_MANIFEST_ENTRY_LAZY_DECODE_ENABLED,
                   manifest_entry_lazy_decode_enabled ? "true" : "false")
        .WithCache(cache);
    if (bucket) {
        context_builder.SetBucketFilter(bucket.value());
    }
    if (predicate) {
        context_builder.SetPredicate(predicate);
    }
    EXPECT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    EXPECT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    auto typed_table_scan = dynamic_cast<AbstractTableScan*>(table_scan.get());
    EXPECT_TRUE(typed_table_scan);
    return typed_table_scan->snapshot_reader_->scan_;
}

std::vector<std::string> SortedFileNames(std::vector<ManifestEntry>&& entries) {
    std::vector<std::string> file_names;
    file_names.reserve(entries.size());
    for (const auto& entry : entries) {
        file_names.push_back(entry.FileName());
    }
    std::sort(file_names.begin(), file_names.end());
    return file_names;
}

}  // namespace

TEST(AppendOnlyFileStoreScanTest, TestDropStatsAfterFiltering) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    std::shared_ptr<Predicate> predicate = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, "David", 5));

    std::shared_ptr<FileStoreScan> scan_with_stats =
        BuildScan(table_path, /*cache=*/nullptr, /*bucket=*/std::nullopt, predicate);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot,
                         scan_with_stats->GetSnapshotManager()->LoadSnapshot(/*snapshot_id=*/3));
    scan_with_stats->WithSnapshot(snapshot);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> plan_with_stats,
                         scan_with_stats->CreatePlan());
    std::vector<ManifestEntry> entries_with_stats = plan_with_stats->Files();
    ASSERT_FALSE(entries_with_stats.empty());

    std::shared_ptr<FileStoreScan> scan_without_stats =
        BuildScan(table_path, /*cache=*/nullptr, /*bucket=*/std::nullopt, predicate);
    scan_without_stats->WithSnapshot(snapshot)->EnableDropStats();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> plan_without_stats,
                         scan_without_stats->CreatePlan());
    std::vector<ManifestEntry> entries_without_stats = plan_without_stats->Files();

    ASSERT_EQ(entries_with_stats.size(), entries_without_stats.size());
    for (size_t i = 0; i < entries_with_stats.size(); ++i) {
        ASSERT_EQ(entries_with_stats[i].CreateIdentifier(),
                  entries_without_stats[i].CreateIdentifier());
        ASSERT_FALSE(entries_with_stats[i].File()->value_stats == SimpleStats::EmptyStats());
        ASSERT_EQ(SimpleStats::EmptyStats(), entries_without_stats[i].File()->value_stats);
        ASSERT_TRUE(entries_without_stats[i].File()->value_stats_cols.has_value());
        ASSERT_TRUE(entries_without_stats[i].File()->value_stats_cols->empty());
    }
}

TEST(AppendOnlyFileStoreScanTest, TestSnapshotLiveManifestCachePath) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto cache = std::make_shared<LruCache>(/*max_weight=*/16 * 1024 * 1024);

    // First scan on snapshot 5: cache miss, entries rebuilt from all manifests.
    auto scan_first = BuildScan(table_path, cache, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot_5,
                         scan_first->GetSnapshotManager()->LoadSnapshot(/*snapshot_id=*/5));
    scan_first->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_first, scan_first->CreatePlan());
    std::vector<std::string> first_file_names = SortedFileNames(plan_first->Files());
    std::shared_ptr<Metrics> first_metrics = scan_first->GetScanMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t first_cache_enabled,
                         first_metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_ENABLED));
    ASSERT_OK_AND_ASSIGN(uint64_t first_cache_hit,
                         first_metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));
    ASSERT_OK_AND_ASSIGN(uint64_t first_cache_misses,
                         first_metrics->GetCounter(ScanMetrics::SNAPSHOT_CACHE_MISSES));
    ASSERT_OK(first_metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_LOAD_DURATION));
    ASSERT_OK(first_metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_STORE_DURATION));
    ASSERT_OK(first_metrics->GetHistogramStats(ScanMetrics::SNAPSHOT_CACHE_LOAD_DURATION));
    ASSERT_OK(first_metrics->GetHistogramStats(ScanMetrics::SNAPSHOT_CACHE_STORE_DURATION));
    ASSERT_EQ(first_cache_enabled, 1);
    ASSERT_EQ(first_cache_hit, 0);
    ASSERT_EQ(first_cache_misses, 1);

    // Second scan on the same snapshot should read the same bucket live entries from cache.
    auto scan_second = BuildScan(table_path, cache, /*bucket=*/0);
    scan_second->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_second, scan_second->CreatePlan());
    ASSERT_EQ(first_file_names, SortedFileNames(plan_second->Files()));
    std::shared_ptr<Metrics> second_metrics = scan_second->GetScanMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t second_cache_hit,
                         second_metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));
    ASSERT_OK_AND_ASSIGN(uint64_t second_cache_hits,
                         second_metrics->GetCounter(ScanMetrics::SNAPSHOT_CACHE_HITS));
    ASSERT_OK_AND_ASSIGN(uint64_t scanned_rows,
                         second_metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_SCANNED_ROWS));
    ASSERT_OK_AND_ASSIGN(
        uint64_t materialized_rows,
        second_metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_MATERIALIZED_ROWS));
    ASSERT_EQ(second_cache_hit, 1);
    ASSERT_EQ(second_cache_hits, 1);
    ASSERT_GE(scanned_rows, materialized_rows);
    ASSERT_OK(second_metrics->GetHistogramStats(ScanMetrics::SNAPSHOT_CACHE_LOAD_DURATION));
}

TEST(AppendOnlyFileStoreScanTest, TestSnapshotLiveManifestCacheRebuildOnMiss) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto cache = std::make_shared<LruCache>(/*max_weight=*/16 * 1024 * 1024);

    // Seed the cache with an earlier snapshot.
    auto scan_base = BuildScan(table_path, cache, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot_3,
                         scan_base->GetSnapshotManager()->LoadSnapshot(/*snapshot_id=*/3));
    scan_base->WithSnapshot(snapshot_3);
    ASSERT_OK_AND_ASSIGN(auto plan_base, scan_base->CreatePlan());
    (void)plan_base;

    // Advance to a newer snapshot: cache miss rebuilds the target snapshot bucket.
    auto scan_next = BuildScan(table_path, cache, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot_5,
                         scan_next->GetSnapshotManager()->LoadSnapshot(/*snapshot_id=*/5));
    scan_next->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_next, scan_next->CreatePlan());

    auto scan_expected = BuildScan(table_path, /*cache=*/nullptr, /*bucket=*/0);
    scan_expected->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_expected, scan_expected->CreatePlan());
    ASSERT_EQ(SortedFileNames(plan_expected->Files()), SortedFileNames(plan_next->Files()));
}

TEST(AppendOnlyFileStoreScanTest, TestSnapshotLiveManifestCacheFallbackWithoutLazyDecode) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto cache = std::make_shared<LruCache>(/*max_weight=*/16 * 1024 * 1024);

    auto scan_fallback = BuildScan(table_path, cache, /*bucket=*/0, /*predicate=*/nullptr,
                                   /*manifest_entry_lazy_decode_enabled=*/false);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot_5,
                         scan_fallback->GetSnapshotManager()->LoadSnapshot(/*snapshot_id=*/5));
    scan_fallback->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_fallback, scan_fallback->CreatePlan());

    auto scan_expected = BuildScan(table_path, /*cache=*/nullptr, /*bucket=*/0);
    scan_expected->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_expected, scan_expected->CreatePlan());
    ASSERT_EQ(SortedFileNames(plan_expected->Files()), SortedFileNames(plan_fallback->Files()));
}
}  // namespace paimon::test
