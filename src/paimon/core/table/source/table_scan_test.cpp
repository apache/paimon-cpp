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
#include "paimon/defs.h"
#include "paimon/metrics.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
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

}  // namespace paimon::test
