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
#include "paimon/common/utils/path_util.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/metrics.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_read.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class DefaultMetricsTableScan : public TableScan {
 public:
    Result<std::shared_ptr<Plan>> CreatePlan() override {
        return Status::NotImplemented("not implemented");
    }
};

class TableMetadataTrackingFileSystem : public FileSystem {
 public:
    explicit TableMetadataTrackingFileSystem(const std::string& table_path)
        : snapshot_directory_(PathUtil::JoinPath(table_path, "snapshot")),
          schema_directory_(PathUtil::JoinPath(table_path, "schema")) {}

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        PAIMON_RETURN_NOT_OK(CheckTableMetadataAccess(path));
        return local_.Open(path);
    }

    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        return local_.Create(path, overwrite);
    }

    Status Mkdirs(const std::string& path) const override {
        return local_.Mkdirs(path);
    }

    Status Rename(const std::string& src, const std::string& dst) const override {
        return local_.Rename(src, dst);
    }

    Status Delete(const std::string& path, bool recursive = true) const override {
        return local_.Delete(path, recursive);
    }

    Result<FileStatus> GetFileStatus(const std::string& path) const override {
        PAIMON_RETURN_NOT_OK(CheckTableMetadataAccess(path));
        return local_.GetFileStatus(path);
    }

    Status ListDir(const std::string& directory,
                   std::vector<BasicFileStatus>* file_status_list) const override {
        PAIMON_RETURN_NOT_OK(CheckTableMetadataAccess(directory));
        return local_.ListDir(directory, file_status_list);
    }

    Status ListFileStatus(const std::string& path,
                          std::vector<FileStatus>* file_status_list) const override {
        PAIMON_RETURN_NOT_OK(CheckTableMetadataAccess(path));
        return local_.ListFileStatus(path, file_status_list);
    }

    Result<bool> Exists(const std::string& path) const override {
        PAIMON_RETURN_NOT_OK(CheckTableMetadataAccess(path));
        return local_.Exists(path);
    }

    Status ReadFile(const std::string& path, std::string* content) override {
        PAIMON_RETURN_NOT_OK(CheckTableMetadataAccess(path));
        return local_.ReadFile(path, content);
    }

    void BlockAndResetTableMetadataAccess() {
        snapshot_metadata_access_count_ = 0;
        schema_metadata_access_count_ = 0;
        block_table_metadata_ = true;
    }

    int32_t SnapshotMetadataAccessCount() const {
        return snapshot_metadata_access_count_;
    }

    int32_t SchemaMetadataAccessCount() const {
        return schema_metadata_access_count_;
    }

 private:
    bool IsSnapshotMetadata(const std::string& path) const {
        return path == snapshot_directory_ ||
               (path.size() > snapshot_directory_.size() &&
                path.compare(0, snapshot_directory_.size(), snapshot_directory_) == 0 &&
                path[snapshot_directory_.size()] == '/');
    }

    bool IsSchemaMetadata(const std::string& path) const {
        return path == schema_directory_ ||
               (path.size() > schema_directory_.size() &&
                path.compare(0, schema_directory_.size(), schema_directory_) == 0 &&
                path[schema_directory_.size()] == '/');
    }

    Status CheckTableMetadataAccess(const std::string& path) const {
        if (IsSnapshotMetadata(path)) {
            ++snapshot_metadata_access_count_;
        } else if (IsSchemaMetadata(path)) {
            ++schema_metadata_access_count_;
        } else {
            return Status::OK();
        }
        if (block_table_metadata_) {
            return Status::IOError("table metadata access is blocked by test: ", path);
        }
        return Status::OK();
    }

    LocalFileSystem local_;
    std::string snapshot_directory_;
    std::string schema_directory_;
    mutable int32_t snapshot_metadata_access_count_ = 0;
    mutable int32_t schema_metadata_access_count_ = 0;
    bool block_table_metadata_ = false;
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
    ASSERT_OK_AND_ASSIGN(std::string normalized_path, PathUtil::NormalizePath(path));
    auto file_system = std::make_shared<TableMetadataTrackingFileSystem>(normalized_path);
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.WithFileSystem(file_system);
    ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(auto plan, table_scan->CreatePlan());
    ASSERT_FALSE(plan->SnapshotId());
    ASSERT_TRUE(plan->Splits().empty());
    ASSERT_TRUE(plan->GetSnapshotReadView());
    ASSERT_FALSE(plan->GetSnapshotReadView()->SnapshotId());
    ASSERT_GT(file_system->SnapshotMetadataAccessCount(), 0);
    ASSERT_GT(file_system->SchemaMetadataAccessCount(), 0);

    file_system->BlockAndResetTableMetadataAccess();
    ScanContextBuilder reused_builder(path);
    reused_builder.AddOption(Options::FILE_FORMAT, "orc");
    reused_builder.WithFileSystem(file_system);
    reused_builder.WithSnapshotReadView(plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(auto reused_context, reused_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto reused_scan, TableScan::Create(std::move(reused_context)));
    ASSERT_OK_AND_ASSIGN(auto reused_plan, reused_scan->CreatePlan());
    ASSERT_FALSE(reused_plan->SnapshotId());
    ASSERT_TRUE(reused_plan->Splits().empty());
    ASSERT_EQ(plan->GetSnapshotReadView(), reused_plan->GetSnapshotReadView());
    ASSERT_EQ(0, file_system->SnapshotMetadataAccessCount());
    ASSERT_EQ(0, file_system->SchemaMetadataAccessCount());
}

TEST(TableScanTest, TestReuseSnapshotReadViewSkipsSnapshotMetadata) {
    std::string path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table/";
    ASSERT_OK_AND_ASSIGN(std::string normalized_path, PathUtil::NormalizePath(path));
    auto file_system = std::make_shared<TableMetadataTrackingFileSystem>(normalized_path);
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.WithFileSystem(file_system);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    ASSERT_TRUE(plan->SnapshotId());
    ASSERT_TRUE(plan->GetSnapshotReadView());
    ASSERT_EQ(plan->SnapshotId(), plan->GetSnapshotReadView()->SnapshotId());
    ASSERT_GT(file_system->SnapshotMetadataAccessCount(), 0);
    ASSERT_GT(file_system->SchemaMetadataAccessCount(), 0);

    file_system->BlockAndResetTableMetadataAccess();
    ScanContextBuilder reused_builder(path);
    reused_builder.AddOption(Options::FILE_FORMAT, "orc");
    reused_builder.WithFileSystem(file_system);
    reused_builder.WithSnapshotReadView(plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> reused_context, reused_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> reused_scan,
                         TableScan::Create(std::move(reused_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> reused_plan, reused_scan->CreatePlan());
    ASSERT_EQ(plan->SnapshotId(), reused_plan->SnapshotId());
    ASSERT_EQ(plan->Splits().size(), reused_plan->Splits().size());
    ASSERT_EQ(plan->GetSnapshotReadView(), reused_plan->GetSnapshotReadView());
    ASSERT_EQ(0, file_system->SnapshotMetadataAccessCount());
    ASSERT_EQ(0, file_system->SchemaMetadataAccessCount());
}

TEST(TableScanTest, TestDataEvolutionGlobalIndexSnapshotReadViewSkipsTableMetadata) {
    std::string path =
        paimon::test::GetDataDir() + "/orc/append_with_global_index.db/append_with_global_index";
    ASSERT_OK_AND_ASSIGN(std::string normalized_path, PathUtil::NormalizePath(path));
    auto file_system = std::make_shared<TableMetadataTrackingFileSystem>(normalized_path);
    std::shared_ptr<Predicate> predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, "Alice", 5));
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.AddOption("bitmap-global-index.legacy-format.enabled-for-testing", "true");
    builder.SetPredicate(predicate);
    builder.WithFileSystem(file_system);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    ASSERT_TRUE(plan->SnapshotId());
    ASSERT_TRUE(plan->GetSnapshotReadView());
    ASSERT_FALSE(plan->Splits().empty());
    ASSERT_GT(file_system->SnapshotMetadataAccessCount(), 0);
    ASSERT_GT(file_system->SchemaMetadataAccessCount(), 0);

    file_system->BlockAndResetTableMetadataAccess();
    ScanContextBuilder reused_builder(path);
    reused_builder.AddOption(Options::FILE_FORMAT, "orc");
    reused_builder.AddOption("bitmap-global-index.legacy-format.enabled-for-testing", "true");
    reused_builder.SetPredicate(predicate);
    reused_builder.WithFileSystem(file_system);
    reused_builder.WithSnapshotReadView(plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> reused_context, reused_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> reused_scan,
                         TableScan::Create(std::move(reused_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> reused_plan, reused_scan->CreatePlan());
    ASSERT_EQ(plan->SnapshotId(), reused_plan->SnapshotId());
    ASSERT_EQ(plan->Splits().size(), reused_plan->Splits().size());
    ASSERT_EQ(plan->GetSnapshotReadView(), reused_plan->GetSnapshotReadView());
    ASSERT_EQ(0, file_system->SnapshotMetadataAccessCount());
    ASSERT_EQ(0, file_system->SchemaMetadataAccessCount());
}

TEST(TableScanTest, TestDataEvolutionEmptyGlobalIndexPublishesReusableSnapshotReadView) {
    std::string path =
        paimon::test::GetDataDir() + "/orc/append_with_global_index.db/append_with_global_index";
    ASSERT_OK_AND_ASSIGN(std::string normalized_path, PathUtil::NormalizePath(path));
    auto file_system = std::make_shared<TableMetadataTrackingFileSystem>(normalized_path);
    std::shared_ptr<Predicate> predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, "not-found", 9));
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.AddOption("bitmap-global-index.legacy-format.enabled-for-testing", "true");
    builder.SetPredicate(predicate);
    builder.WithFileSystem(file_system);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    ASSERT_TRUE(plan->SnapshotId());
    ASSERT_TRUE(plan->Splits().empty());
    ASSERT_TRUE(plan->GetSnapshotReadView());
    ASSERT_EQ(plan->SnapshotId(), plan->GetSnapshotReadView()->SnapshotId());
    ASSERT_GT(file_system->SnapshotMetadataAccessCount(), 0);
    ASSERT_GT(file_system->SchemaMetadataAccessCount(), 0);

    file_system->BlockAndResetTableMetadataAccess();
    ScanContextBuilder reused_builder(path);
    reused_builder.AddOption(Options::FILE_FORMAT, "orc");
    reused_builder.AddOption("bitmap-global-index.legacy-format.enabled-for-testing", "true");
    reused_builder.SetPredicate(predicate);
    reused_builder.WithFileSystem(file_system);
    reused_builder.WithSnapshotReadView(plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> reused_context, reused_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> reused_scan,
                         TableScan::Create(std::move(reused_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> reused_plan, reused_scan->CreatePlan());
    ASSERT_EQ(plan->SnapshotId(), reused_plan->SnapshotId());
    ASSERT_TRUE(reused_plan->Splits().empty());
    ASSERT_EQ(plan->GetSnapshotReadView(), reused_plan->GetSnapshotReadView());
    ASSERT_EQ(0, file_system->SnapshotMetadataAccessCount());
    ASSERT_EQ(0, file_system->SchemaMetadataAccessCount());
}

TEST(TableScanTest, TestDataEvolutionSuppliedEmptyGlobalIndexPreservesInjectedSnapshotReadView) {
    std::string path =
        paimon::test::GetDataDir() + "/orc/append_with_global_index.db/append_with_global_index";
    ScanContextBuilder unbound_builder(path);
    unbound_builder.AddOption(Options::FILE_FORMAT, "orc");
    unbound_builder.SetGlobalIndexResult(BitmapGlobalIndexResult::FromRanges({}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> unbound_context, unbound_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> unbound_scan,
                         TableScan::Create(std::move(unbound_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> unbound_plan, unbound_scan->CreatePlan());
    ASSERT_FALSE(unbound_plan->SnapshotId());
    ASSERT_FALSE(unbound_plan->GetSnapshotReadView());

    ScanContextBuilder view_builder(path);
    view_builder.AddOption(Options::FILE_FORMAT, "orc");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> view_context, view_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> view_scan,
                         TableScan::Create(std::move(view_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> source_plan, view_scan->CreatePlan());
    ASSERT_TRUE(source_plan->GetSnapshotReadView());

    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.SetGlobalIndexResult(BitmapGlobalIndexResult::FromRanges({}));
    builder.WithSnapshotReadView(source_plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    ASSERT_EQ(source_plan->SnapshotId(), plan->SnapshotId());
    ASSERT_TRUE(plan->Splits().empty());
    ASSERT_EQ(source_plan->GetSnapshotReadView(), plan->GetSnapshotReadView());
}

TEST(TableScanTest, TestDataEvolutionExplicitSnapshotEmptyGlobalIndexIsNotReboundToLatest) {
    std::string path =
        paimon::test::GetDataDir() + "/orc/append_with_global_index.db/append_with_global_index";
    std::shared_ptr<Predicate> predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, "not-found", 9));
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.AddOption(Options::SCAN_MODE, "from-snapshot");
    builder.AddOption(Options::SCAN_SNAPSHOT_ID, "4");
    builder.AddOption("bitmap-global-index.legacy-format.enabled-for-testing", "true");
    builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    ASSERT_TRUE(plan->Splits().empty());
    ASSERT_FALSE(plan->SnapshotId());
    ASSERT_FALSE(plan->GetSnapshotReadView());
}

TEST(TableScanTest, TestReadOptimizedSnapshotReadViewSkipsTableMetadata) {
    std::string base_path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table";
    std::string read_optimized_path = base_path + "$ro";
    ASSERT_OK_AND_ASSIGN(std::string normalized_base_path, PathUtil::NormalizePath(base_path));
    auto file_system = std::make_shared<TableMetadataTrackingFileSystem>(normalized_base_path);
    ScanContextBuilder builder(read_optimized_path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.WithFileSystem(file_system);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    ASSERT_TRUE(plan->SnapshotId());
    ASSERT_TRUE(plan->GetSnapshotReadView());
    ASSERT_EQ(normalized_base_path, plan->GetSnapshotReadView()->TablePath());
    ASSERT_EQ("main", plan->GetSnapshotReadView()->Branch());
    ASSERT_OK_AND_ASSIGN(uint64_t scanned_snapshot_id, table_scan->GetMetrics()->GetCounter(
                                                           ScanMetrics::LAST_SCANNED_SNAPSHOT_ID));
    ASSERT_EQ(scanned_snapshot_id, static_cast<uint64_t>(plan->SnapshotId().value()));
    ASSERT_GT(file_system->SnapshotMetadataAccessCount(), 0);
    ASSERT_GT(file_system->SchemaMetadataAccessCount(), 0);

    file_system->BlockAndResetTableMetadataAccess();
    ScanContextBuilder reused_builder(read_optimized_path);
    reused_builder.AddOption(Options::FILE_FORMAT, "orc");
    reused_builder.WithFileSystem(file_system);
    reused_builder.WithSnapshotReadView(plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> reused_context, reused_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> reused_scan,
                         TableScan::Create(std::move(reused_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> reused_plan, reused_scan->CreatePlan());
    ASSERT_EQ(plan->SnapshotId(), reused_plan->SnapshotId());
    ASSERT_EQ(plan->Splits().size(), reused_plan->Splits().size());
    ASSERT_TRUE(reused_plan->GetSnapshotReadView());
    ASSERT_EQ(plan->GetSnapshotReadView(), reused_plan->GetSnapshotReadView());
    ASSERT_EQ(normalized_base_path, reused_plan->GetSnapshotReadView()->TablePath());
    ASSERT_EQ("main", reused_plan->GetSnapshotReadView()->Branch());
    ASSERT_EQ(0, file_system->SnapshotMetadataAccessCount());
    ASSERT_EQ(0, file_system->SchemaMetadataAccessCount());
}

TEST(TableScanTest, TestBaseSnapshotReadViewFeedsReadOptimizedScan) {
    std::string base_path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table";
    std::string read_optimized_path = base_path + "$ro";
    ASSERT_OK_AND_ASSIGN(std::string normalized_base_path, PathUtil::NormalizePath(base_path));
    auto file_system = std::make_shared<TableMetadataTrackingFileSystem>(normalized_base_path);

    ScanContextBuilder base_builder(base_path);
    base_builder.AddOption(Options::FILE_FORMAT, "orc");
    base_builder.WithFileSystem(file_system);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> base_context, base_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> base_scan,
                         TableScan::Create(std::move(base_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> base_plan, base_scan->CreatePlan());
    ASSERT_TRUE(base_plan->GetSnapshotReadView());
    ASSERT_EQ(normalized_base_path, base_plan->GetSnapshotReadView()->TablePath());

    file_system->BlockAndResetTableMetadataAccess();
    ScanContextBuilder read_optimized_builder(read_optimized_path);
    read_optimized_builder.AddOption(Options::FILE_FORMAT, "orc");
    read_optimized_builder.WithFileSystem(file_system);
    read_optimized_builder.WithSnapshotReadView(base_plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> read_optimized_context,
                         read_optimized_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> read_optimized_scan,
                         TableScan::Create(std::move(read_optimized_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> read_optimized_plan,
                         read_optimized_scan->CreatePlan());
    ASSERT_EQ(base_plan->SnapshotId(), read_optimized_plan->SnapshotId());
    ASSERT_EQ(base_plan->GetSnapshotReadView(), read_optimized_plan->GetSnapshotReadView());
    ASSERT_EQ(0, file_system->SnapshotMetadataAccessCount());
    ASSERT_EQ(0, file_system->SchemaMetadataAccessCount());
}

TEST(TableScanTest, TestReadOptimizedSnapshotReadViewRejectsRealtimeContext) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/append_table_with_nested_type.db/append_table_with_nested_type$ro";
    ScanContextBuilder source_builder(path);
    source_builder.AddOption(Options::FILE_FORMAT, "orc");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> source_context, source_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> source_scan,
                         TableScan::Create(std::move(source_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> source_plan, source_scan->CreatePlan());
    ASSERT_TRUE(source_plan->GetSnapshotReadView());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.WithSnapshotReadView(source_plan->GetSnapshotReadView());
    builder.WithRealtimeContext(realtime_context);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(context)),
                        "snapshot read view does not support real-time union scan");
}

TEST(TableScanTest, TestReadOptimizedTableReadUsesProvidedSchemaWithoutTableMetadata) {
    std::string base_path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table";
    std::string read_optimized_path = base_path + "$ro";
    ASSERT_OK_AND_ASSIGN(std::string normalized_base_path, PathUtil::NormalizePath(base_path));
    auto file_system = std::make_shared<TableMetadataTrackingFileSystem>(normalized_base_path);
    SchemaManager schema_manager(file_system, normalized_base_path);
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                         schema_manager.Latest());
    ASSERT_TRUE(latest_schema);
    ASSERT_OK_AND_ASSIGN(std::string table_schema_json, latest_schema.value()->ToJsonString());

    file_system->BlockAndResetTableMetadataAccess();
    ReadContextBuilder builder(read_optimized_path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.WithFileSystem(file_system);
    builder.SetTableSchema(table_schema_json);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(context)));
    ASSERT_TRUE(table_read);
    ASSERT_EQ(0, file_system->SnapshotMetadataAccessCount());
    ASSERT_EQ(0, file_system->SchemaMetadataAccessCount());
}

TEST(TableScanTest, TestReadOptimizedBranchSnapshotReadViewSkipsTableMetadata) {
    std::string base_path = paimon::test::GetDataDir() +
                            "/orc/append_table_with_rt_branch.db/append_table_with_rt_branch";
    std::string read_optimized_path = base_path + "$branch_rt$ro";
    ASSERT_OK_AND_ASSIGN(std::string normalized_base_path, PathUtil::NormalizePath(base_path));
    ASSERT_OK_AND_ASSIGN(std::string normalized_branch_path,
                         PathUtil::NormalizePath(base_path + "/branch/branch-rt"));
    auto file_system = std::make_shared<TableMetadataTrackingFileSystem>(normalized_branch_path);
    ScanContextBuilder builder(read_optimized_path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    builder.WithFileSystem(file_system);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    ASSERT_TRUE(plan->SnapshotId());
    ASSERT_TRUE(plan->GetSnapshotReadView());
    ASSERT_EQ(normalized_base_path, plan->GetSnapshotReadView()->TablePath());
    ASSERT_EQ("rt", plan->GetSnapshotReadView()->Branch());
    ASSERT_GT(file_system->SnapshotMetadataAccessCount(), 0);
    ASSERT_GT(file_system->SchemaMetadataAccessCount(), 0);

    file_system->BlockAndResetTableMetadataAccess();
    ScanContextBuilder reused_builder(read_optimized_path);
    reused_builder.AddOption(Options::FILE_FORMAT, "orc");
    reused_builder.WithFileSystem(file_system);
    reused_builder.WithSnapshotReadView(plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> reused_context, reused_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> reused_scan,
                         TableScan::Create(std::move(reused_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> reused_plan, reused_scan->CreatePlan());
    ASSERT_EQ(plan->SnapshotId(), reused_plan->SnapshotId());
    ASSERT_EQ(plan->Splits().size(), reused_plan->Splits().size());
    ASSERT_TRUE(reused_plan->GetSnapshotReadView());
    ASSERT_EQ(plan->GetSnapshotReadView(), reused_plan->GetSnapshotReadView());
    ASSERT_EQ(normalized_base_path, reused_plan->GetSnapshotReadView()->TablePath());
    ASSERT_EQ("rt", reused_plan->GetSnapshotReadView()->Branch());
    ASSERT_EQ(0, file_system->SnapshotMetadataAccessCount());
    ASSERT_EQ(0, file_system->SchemaMetadataAccessCount());
}

TEST(TableScanTest, TestSnapshotReadViewValidatesTableAndBranchBinding) {
    std::string path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table/";
    ScanContextBuilder builder(path);
    builder.AddOption(Options::FILE_FORMAT, "orc");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    ASSERT_TRUE(plan->GetSnapshotReadView());

    ScanContextBuilder other_table_builder(path + "other");
    other_table_builder.AddOption(Options::FILE_FORMAT, "orc");
    other_table_builder.WithSnapshotReadView(plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> other_table_context,
                         other_table_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(other_table_context)), "bound to table");

    ScanContextBuilder other_branch_builder(path);
    other_branch_builder.AddOption(Options::FILE_FORMAT, "orc");
    other_branch_builder.AddOption(Options::BRANCH, "other");
    other_branch_builder.WithSnapshotReadView(plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> other_branch_context,
                         other_branch_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(other_branch_context)), "bound to branch");

    ScanContextBuilder other_system_table_builder(plan->GetSnapshotReadView()->TablePath() +
                                                  "$snapshots");
    other_system_table_builder.AddOption(Options::FILE_FORMAT, "orc");
    other_system_table_builder.WithSnapshotReadView(plan->GetSnapshotReadView());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> other_system_table_context,
                         other_system_table_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(other_system_table_context)),
                        "only be used with the read-optimized system table");
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
