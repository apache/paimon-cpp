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

#include "paimon/table/source/table_scan.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/metrics.h"
#include "paimon/read_context.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/scan_metrics.h"
#include "paimon/table/source/table_read.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class DefaultMetricsTableScan : public TableScan {
 public:
    Result<std::shared_ptr<Plan>> CreatePlan() override {
        return Status::NotImplemented("not implemented");
    }
};

}  // namespace

TEST(TableScanTest, TestDefaultMetricsSnapshot) {
    DefaultMetricsTableScan table_scan;
    std::shared_ptr<Metrics> metrics = table_scan.GetMetrics();
    ASSERT_TRUE(metrics);
    metrics->SetCounter("external", 1);

    std::shared_ptr<Metrics> second_metrics = table_scan.GetMetrics();
    ASSERT_TRUE(second_metrics);
    Result<uint64_t> external_counter = second_metrics->GetCounter("external");
    ASSERT_FALSE(external_counter.ok());
    ASSERT_EQ(external_counter.status().code(), StatusCode::KeyError);
}

TEST(TableScanTest, TestNoSnapshot) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/append_table_with_nested_type.db/append_table_with_nested_type/";
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(auto plan, table_scan->CreatePlan());
    ASSERT_FALSE(plan->SnapshotId());
    ASSERT_TRUE(plan->Splits().empty());
}

TEST(TableScanTest, TestNonExistTable) {
    std::string path = paimon::test::GetDataDir() + "/non-exist.db/non-exist/";
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(context)), "not found latest schema");
}

TEST(TableScanTest, TestPkSchemaEvolutionScan) {
    std::string path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table/";
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(auto plan, table_scan->CreatePlan());
    ASSERT_TRUE(plan->SnapshotId());
    ASSERT_FALSE(plan->Splits().empty());

    std::shared_ptr<Metrics> metrics = table_scan->GetMetrics();
    ASSERT_TRUE(metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t scanned_snapshot_id,
                         metrics->GetCounter(ScanMetrics::LAST_SCANNED_SNAPSHOT_ID));
    ASSERT_EQ(scanned_snapshot_id, static_cast<uint64_t>(plan->SnapshotId().value()));
    ASSERT_OK_AND_ASSIGN(uint64_t resulted_table_files,
                         metrics->GetCounter(ScanMetrics::LAST_SCAN_RESULTED_TABLE_FILES));
    ASSERT_GT(resulted_table_files, 0);
    ASSERT_OK(metrics->GetCounter(ScanMetrics::LAST_MANIFEST_READ_DURATION));
    ASSERT_OK(metrics->GetHistogramStats(ScanMetrics::MANIFEST_READ_DURATION));
    ASSERT_OK_AND_ASSIGN(uint64_t lazy_decode_scanned_rows,
                         metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_SCANNED_ROWS));
    ASSERT_OK_AND_ASSIGN(uint64_t lazy_decode_materialized_rows,
                         metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_MATERIALIZED_ROWS));
    ASSERT_GE(lazy_decode_scanned_rows, lazy_decode_materialized_rows);

    metrics->SetCounter(ScanMetrics::LAST_SCANNED_SNAPSHOT_ID, 0);
    ASSERT_OK_AND_ASSIGN(uint64_t internal_snapshot_id, table_scan->GetMetrics()->GetCounter(
                                                            ScanMetrics::LAST_SCANNED_SNAPSHOT_ID));
    ASSERT_EQ(internal_snapshot_id, static_cast<uint64_t>(plan->SnapshotId().value()));
}

TEST(TableScanTest, TestReadOptimizedPrimaryKeyStreamingScanUnsupported) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table$ro";
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());

    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(context)),
                        "read-optimized system table does not support streaming scan for primary "
                        "key table");
}

namespace {

struct CachedScanResult {
    std::vector<std::string> rows;
    uint64_t cache_hit = 0;
};

// Plans bucket 0 through the snapshot live manifest entry cache and reads every row back as
// "k=v", sorted.
Result<CachedScanResult> ScanBucketThroughCache(const std::string& table_path,
                                                const std::map<std::string, std::string>& options,
                                                const std::shared_ptr<Cache>& cache) {
    ScanContextBuilder scan_builder(table_path);
    scan_builder.SetOptions(options).WithCache(cache).SetBucketFilter(0);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> table_scan,
                           TableScan::Create(std::move(scan_context)));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    CachedScanResult result;
    std::shared_ptr<Metrics> metrics = table_scan->GetMetrics();
    PAIMON_ASSIGN_OR_RAISE(uint64_t cache_enabled,
                           metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_ENABLED));
    if (cache_enabled != 1) {
        return Status::Invalid("the snapshot live manifest entry cache is not in use");
    }
    PAIMON_ASSIGN_OR_RAISE(result.cache_hit,
                           metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));

    ReadContextBuilder read_builder(table_path);
    read_builder.SetOptions(options).WithCache(cache);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           TableRead::Create(std::move(read_context)));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                           table_read->CreateReader(plan->Splits()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> chunks,
                           ReadResultCollector::CollectResult(std::move(reader)));
    for (const std::shared_ptr<arrow::Array>& chunk : chunks->chunks()) {
        const auto& rows = checked_cast<const arrow::StructArray&>(*chunk);
        auto keys = std::static_pointer_cast<arrow::StringArray>(rows.GetFieldByName("k"));
        auto values = std::static_pointer_cast<arrow::StringArray>(rows.GetFieldByName("v"));
        if (!keys || !values) {
            return Status::Invalid("read result misses column k or v");
        }
        for (int64_t i = 0; i < rows.length(); ++i) {
            result.rows.push_back(keys->GetString(i) + "=" + values->GetString(i));
        }
    }
    std::sort(result.rows.begin(), result.rows.end());
    return result;
}

}  // namespace

// A rollback deletes the newest snapshots, and the commits that follow reuse their ids for
// different content. Entries cached for the deleted snapshot must not serve the rewritten one.
TEST(TableScanTest, TestManifestEntryCacheIgnoresRewrittenSnapshot) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create("local");
    std::map<std::string, std::string> options = {
        {Options::BUCKET, "1"},
        {Options::SCAN_MANIFEST_ENTRY_CACHE_MAX_SNAPSHOTS, "2"},
        {Options::SCAN_MANIFEST_ENTRY_LAZY_DECODE_ENABLED, "true"}};
    arrow::FieldVector fields = {arrow::field("k", arrow::utf8(), /*nullable=*/false),
                                 arrow::field("v", arrow::utf8())};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> helper,
                         TestHelper::Create(dir->Str(), arrow::schema(fields),
                                            /*partition_keys=*/{}, /*primary_keys=*/{"k"}, options,
                                            /*is_streaming_mode=*/true));
    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    auto row_type = arrow::struct_(fields);
    auto write = [&](TestHelper* writer, const std::string& rows, int64_t commit_identifier) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             TestHelper::MakeRecordBatch(row_type, rows, /*partition_map=*/{},
                                                         /*bucket=*/0, /*row_kinds=*/{}));
        ASSERT_OK(writer->WriteAndCommit(std::move(batch), commit_identifier, std::nullopt));
    };
    write(helper.get(), R"([["k1", "v1"], ["k2", "v2"]])", 1);
    write(helper.get(), R"([["k3", "v3"]])", 2);
    write(helper.get(), R"([["k1", "v1b"]])", 3);

    auto cache = std::make_shared<LruCache>(/*max_weight=*/64 * 1024 * 1024);
    ASSERT_OK_AND_ASSIGN(CachedScanResult before,
                         ScanBucketThroughCache(table_path, options, cache));
    ASSERT_EQ(before.rows, (std::vector<std::string>{"k1=v1b", "k2=v2", "k3=v3"}));
    ASSERT_EQ(before.cache_hit, 0);

    // Roll back to snapshot 1 the way `rollback_to` does: drop the newer snapshot files and move
    // the LATEST hint. The next commits create a new snapshot 2 and 3.
    std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    SnapshotManager snapshot_manager(fs, table_path);
    ASSERT_OK_AND_ASSIGN(Snapshot old_snapshot_3, snapshot_manager.LoadSnapshot(3));
    ASSERT_OK(fs->Delete(snapshot_manager.SnapshotPath(3), /*recursive=*/false));
    ASSERT_OK(fs->Delete(snapshot_manager.SnapshotPath(2), /*recursive=*/false));
    ASSERT_OK(snapshot_manager.CommitLatestHint(1));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TestHelper> writer_after_rollback,
                         TestHelper::Create(table_path, options, /*is_streaming_mode=*/true));
    write(writer_after_rollback.get(), R"([["k4", "v4"]])", 4);
    write(writer_after_rollback.get(), R"([["k1", "v1c"]])", 5);
    ASSERT_OK_AND_ASSIGN(Snapshot new_snapshot_3, snapshot_manager.LoadSnapshot(3));
    ASSERT_NE(new_snapshot_3.DeltaManifestList(), old_snapshot_3.DeltaManifestList());

    ASSERT_OK_AND_ASSIGN(CachedScanResult after,
                         ScanBucketThroughCache(table_path, options, cache));
    ASSERT_EQ(after.rows, (std::vector<std::string>{"k1=v1c", "k2=v2", "k4=v4"}));
    ASSERT_EQ(after.cache_hit, 0);

    // The rewritten snapshot 3 is cached in its own right afterwards.
    ASSERT_OK_AND_ASSIGN(CachedScanResult again,
                         ScanBucketThroughCache(table_path, options, cache));
    ASSERT_EQ(again.rows, after.rows);
    ASSERT_EQ(again.cache_hit, 1);
}

}  // namespace paimon::test
