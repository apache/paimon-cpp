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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/metrics.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_catalog.h"
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

TEST(TableScanTest, TestCatalogScanReadsSnapshotFromCatalog) {
    std::string fixture = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    // Copy the table so its snapshot file can be removed without touching the shared fixture.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_NE(nullptr, dir);
    std::string path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(fixture, path));
    auto table_fs = dir->GetFileSystem();
    ASSERT_NE(nullptr, table_fs);

    SchemaManager schema_manager(table_fs, path);
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                         schema_manager.Latest());
    ASSERT_TRUE(latest_schema.has_value());
    SnapshotManager snapshot_manager(table_fs, path);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, snapshot_manager.LatestSnapshot());
    ASSERT_TRUE(snapshot.has_value());

    // A version-managed catalog can hold a snapshot it never published to the table path. Remove
    // the snapshot file so that re-reading the body from the path would fail with NotExist: the
    // plan must be built from the body the catalog returns, while the manifests it points at stay
    // on the file system.
    ASSERT_OK(table_fs->Delete(snapshot_manager.SnapshotDirectory(), /*recursive=*/true));
    ASSERT_OK_AND_ASSIGN(bool snapshot_file_exists,
                         table_fs->Exists(snapshot_manager.SnapshotPath(snapshot.value().Id())));
    ASSERT_FALSE(snapshot_file_exists);

    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(table_fs);
    catalog->SetTableSchema(latest_schema.value());
    catalog->SetHeldSnapshot(snapshot.value());

    ScanContextBuilder builder(path);
    builder.WithCatalog(catalog, Identifier("append_09.db", "append_09"));
    builder.AddOption(Options::FILE_FORMAT, "orc");
    ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(auto plan, table_scan->CreatePlan());
    ASSERT_TRUE(plan->SnapshotId());
    ASSERT_FALSE(plan->Splits().empty());
    ASSERT_GT(catalog->LoadSnapshotCalls(), 0U);
}

TEST(TableScanTest, TestCatalogScanRejectsSystemTablePath) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09$snapshots";
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableFileSystem(std::make_shared<LocalFileSystem>());
    ScanContextBuilder builder(path);
    builder.WithCatalog(catalog, Identifier("append_09.db", "append_09$snapshots"));
    builder.AddOption(Options::FILE_FORMAT, "orc");
    ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(context)),
                        "this catalog manages the versions of its tables and publishes no snapshot "
                        "there");
}

}  // namespace paimon::test
