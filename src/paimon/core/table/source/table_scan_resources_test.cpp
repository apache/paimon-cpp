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

#include "paimon/table/source/table_scan_resources.h"

#include <atomic>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/table_scan_resources_impl.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/scan_context.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class SchemaCountingFileSystem : public LocalFileSystem {
 public:
    Status ReadFile(const std::string& path, std::string* content) override {
        if (path.find("/schema/schema-") != std::string::npos) {
            schema_reads.fetch_add(1);
            if (fail_schema_read.exchange(false)) {
                return Status::IOError("injected schema read failure");
            }
        }
        if (path.find("/snapshot/snapshot-") != std::string::npos) {
            snapshot_reads.fetch_add(1);
        }
        return LocalFileSystem::ReadFile(path, content);
    }

    std::atomic<int32_t> schema_reads{0};
    std::atomic<int32_t> snapshot_reads{0};
    std::atomic<bool> fail_schema_read{false};
};

void CheckPlans(const std::shared_ptr<Plan>& expected, const std::shared_ptr<Plan>& actual) {
    ASSERT_EQ(expected->SnapshotId(), actual->SnapshotId());
    ASSERT_EQ(expected->Splits().size(), actual->Splits().size());
    for (size_t i = 0; i < expected->Splits().size(); ++i) {
        auto expected_split = std::dynamic_pointer_cast<DataSplitImpl>(expected->Splits()[i]);
        auto actual_split = std::dynamic_pointer_cast<DataSplitImpl>(actual->Splits()[i]);
        ASSERT_TRUE(expected_split);
        ASSERT_TRUE(actual_split);
        ASSERT_EQ(*expected_split, *actual_split);
    }
}

}  // namespace

class TableScanResourcesTest : public testing::Test {
 protected:
    Result<std::unique_ptr<TableScan>> NewScan(const std::string& path,
                                               const std::shared_ptr<TableScanResources>& resources,
                                               const std::shared_ptr<Predicate>& predicate,
                                               const std::map<std::string, std::string>& options,
                                               bool streaming) {
        ScanContextBuilder builder(path);
        builder.WithFileSystem(fs_)
            .WithExecutor(executor_)
            .WithTableResources(resources)
            .SetPredicate(predicate)
            .SetOptions(options)
            .WithStreamingMode(streaming);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> context, builder.Finish());
        return TableScan::Create(std::move(context));
    }

    const std::string append_path_ = GetDataDir() + "/orc/append_09.db/append_09";
    const std::string pk_path_ =
        GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table";
    std::shared_ptr<SchemaCountingFileSystem> fs_ = std::make_shared<SchemaCountingFileSystem>();
    std::shared_ptr<Executor> executor_ = CreateDefaultExecutor();
};

TEST_F(TableScanResourcesTest, RepeatedScansReuseSchemasAndKeepFiltersIndependent) {
    for (const auto& path : {append_path_, pk_path_}) {
        const bool pk = path == pk_path_;
        auto predicate = PredicateBuilder::IsNotNull(pk ? 3 : 0, pk ? "c" : "f0",
                                                     pk ? FieldType::INT : FieldType::STRING);
        ASSERT_OK_AND_ASSIGN(auto expected_scan, NewScan(path, nullptr, predicate, {}, false));
        ASSERT_OK_AND_ASSIGN(auto expected, expected_scan->CreatePlan());
        ASSERT_FALSE(expected->Splits().empty());
        ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(path, fs_, "main"));
        ASSERT_OK_AND_ASSIGN(auto first, NewScan(path, resources, predicate, {}, false));
        ASSERT_OK_AND_ASSIGN(auto first_plan, first->CreatePlan());
        CheckPlans(expected, first_plan);
        int32_t reads = fs_->schema_reads.load();
        ASSERT_OK_AND_ASSIGN(auto second, NewScan(path, resources, predicate, {}, false));
        ASSERT_OK_AND_ASSIGN(auto second_plan, second->CreatePlan());
        CheckPlans(expected, second_plan);
        ASSERT_EQ(fs_->schema_reads.load(), reads);

        ScanContextBuilder missing_partition(path);
        missing_partition.WithTableResources(resources).WithExecutor(executor_).SetPartitionFilter(
            {{{pk ? "key0" : "f1", "-999"}}});
        ASSERT_OK_AND_ASSIGN(auto context, missing_partition.Finish());
        ASSERT_OK_AND_ASSIGN(auto filtered, TableScan::Create(std::move(context)));
        ASSERT_OK_AND_ASSIGN(auto filtered_plan, filtered->CreatePlan());
        ASSERT_TRUE(filtered_plan->Splits().empty());
        ASSERT_EQ(fs_->schema_reads.load(), reads);
        ASSERT_NOK_WITH_MSG(first->CreatePlan(), "end of scan");
    }
}

TEST_F(TableScanResourcesTest, DirectContextRequiresResourcesFileSystem) {
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(append_path_, fs_, "main"));
    ScanContextBuilder builder(append_path_);
    ASSERT_OK_AND_ASSIGN(auto defaults, builder.Finish());
    const std::vector<std::shared_ptr<FileSystem>> file_systems = {
        nullptr, std::make_shared<LocalFileSystem>(), fs_};
    for (const auto& file_system : file_systems) {
        auto context = std::make_unique<ScanContext>(
            append_path_, false, std::nullopt, defaults->GetScanFilters(), nullptr, nullptr,
            GetDefaultPool(), executor_, file_system, std::nullopt, defaults->GetOptions(), nullptr,
            nullptr, resources);
        if (file_system != fs_) {
            ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(context)),
                                "file system does not match");
        } else {
            ASSERT_OK_AND_ASSIGN(auto scan, TableScan::Create(std::move(context)));
            ASSERT_OK_AND_ASSIGN(auto plan, scan->CreatePlan());
            ASSERT_FALSE(plan->Splits().empty());
        }
    }
}

TEST_F(TableScanResourcesTest, SharedSnapshotCacheDiscoversNewSnapshot) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    const std::string path = directory->Str() + "/table";
    ASSERT_TRUE(TestUtil::CopyDirectory(append_path_, path));
    const std::string next_snapshot_path = path + "/snapshot/snapshot-5";
    std::string next_snapshot_json;
    ASSERT_OK(fs_->ReadFile(next_snapshot_path, &next_snapshot_json));
    ASSERT_OK(fs_->Delete(next_snapshot_path));
    ASSERT_OK(fs_->WriteFile(path + "/snapshot/LATEST", "4", true));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableScanResources> resources,
                         TableScanResources::Create(path, fs_, "main"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> first,
                         NewScan(path, resources, nullptr, {}, false));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> first_plan, first->CreatePlan());
    ASSERT_EQ(first_plan->SnapshotId(), 4);
    int32_t reads = fs_->snapshot_reads.load();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> repeated,
                         NewScan(path, resources, nullptr, {}, false));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> repeated_plan, repeated->CreatePlan());
    CheckPlans(first_plan, repeated_plan);
    ASSERT_EQ(fs_->snapshot_reads.load(), reads);

    // Publish the next snapshot while LATEST still points at the previous one.
    ASSERT_OK(fs_->WriteFile(next_snapshot_path, next_snapshot_json, false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> next,
                         NewScan(path, resources, nullptr, {}, false));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> next_plan, next->CreatePlan());
    ASSERT_EQ(next_plan->SnapshotId(), 5);
    ASSERT_EQ(fs_->snapshot_reads.load(), reads + 1);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> baseline,
                         NewScan(path, nullptr, nullptr, {}, false));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> baseline_plan, baseline->CreatePlan());
    CheckPlans(baseline_plan, next_plan);
}

TEST_F(TableScanResourcesTest, NewSchemaDoesNotChangeExistingScans) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    std::string path = directory->Str() + "/table";
    ASSERT_TRUE(TestUtil::CopyDirectory(append_path_, path));
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(path, fs_, "main"));
    auto old_predicate = PredicateBuilder::IsNotNull(0, "f0", FieldType::STRING);
    ASSERT_OK_AND_ASSIGN(auto baseline_scan, NewScan(path, nullptr, old_predicate, {}, false));
    ASSERT_OK_AND_ASSIGN(auto baseline, baseline_scan->CreatePlan());
    ASSERT_FALSE(baseline->Splits().empty());
    ASSERT_OK_AND_ASSIGN(auto old_scan, NewScan(path, resources, old_predicate, {}, false));
    auto new_predicate = PredicateBuilder::IsNotNull(4, "added", FieldType::INT);
    ASSERT_NOK(NewScan(path, resources, new_predicate, {}, false));

    std::string schema_json;
    ASSERT_OK(fs_->ReadFile(path + "/schema/schema-0", &schema_json));
    ASSERT_OK_AND_ASSIGN(auto old_schema, TableSchema::CreateFromJson(schema_json));
    auto schema =
        arrow::schema({arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                       arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64()),
                       arrow::field("added", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(auto new_schema,
                         TableSchema::Create(1, schema, {"f1"}, {}, old_schema->Options()));
    ASSERT_OK_AND_ASSIGN(auto new_json, new_schema->GetJsonSchema());
    ASSERT_OK(fs_->WriteFile(path + "/schema/schema-1", new_json, false));
    ASSERT_OK_AND_ASSIGN(auto new_scan, NewScan(path, resources, new_predicate, {}, false));
    ASSERT_OK_AND_ASSIGN(auto new_plan, new_scan->CreatePlan());
    ASSERT_TRUE(new_plan->Splits().empty());
    ASSERT_OK_AND_ASSIGN(auto old_plan, old_scan->CreatePlan());
    CheckPlans(baseline, old_plan);

    int32_t reads = fs_->schema_reads.load();
    ASSERT_OK_AND_ASSIGN(auto again, NewScan(path, resources, new_predicate, {}, false));
    ASSERT_OK_AND_ASSIGN(auto again_plan, again->CreatePlan());
    ASSERT_TRUE(again_plan->Splits().empty());
    ASSERT_EQ(fs_->schema_reads.load(), reads);
}

TEST_F(TableScanResourcesTest, SchemaResourcesRetainedBeyond64Versions) {
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(append_path_, fs_, "main"));
    auto predicate = PredicateBuilder::IsNotNull(0, "f0", FieldType::STRING);
    ASSERT_OK_AND_ASSIGN(auto baseline_scan, NewScan(append_path_, nullptr, predicate, {}, false));
    ASSERT_OK_AND_ASSIGN(auto baseline, baseline_scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(auto active_scan, NewScan(append_path_, resources, predicate, {}, false));

    auto& impl = TableScanResourcesAccess::Get(*resources);
    ASSERT_OK_AND_ASSIGN(auto original_schema, impl.schema_manager_->ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto original, impl.GetSchemaResources(original_schema));
    std::weak_ptr<const ScanSchemaResources> retained = original;
    auto original_arrow_schema = original->arrow_schema;
    original.reset();
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(original_schema->Fields());
    for (int64_t id = 1; id <= 64; ++id) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> schema,
            TableSchema::Create(id, arrow_schema, original_schema->PartitionKeys(),
                                original_schema->PrimaryKeys(), original_schema->Options()));
        ASSERT_OK(impl.GetSchemaResources(schema));
    }
    ASSERT_FALSE(retained.expired());
    ASSERT_OK_AND_ASSIGN(auto actual, active_scan->CreatePlan());
    CheckPlans(baseline, actual);
    ASSERT_OK_AND_ASSIGN(auto reloaded, impl.GetSchemaResources(original_schema));
    ASSERT_EQ(reloaded, retained.lock());
    ASSERT_EQ(reloaded->arrow_schema, original_arrow_schema);
    ASSERT_TRUE(reloaded->arrow_schema->Equals(*original_arrow_schema));
    ASSERT_OK_AND_ASSIGN(auto repeated, impl.GetSchemaResources(original_schema));
    ASSERT_EQ(repeated, reloaded);
}

TEST_F(TableScanResourcesTest, SuppliedSchemaDoesNotPopulateSharedSchemaCache) {
    std::string json;
    ASSERT_OK(fs_->ReadFile(append_path_ + "/schema/schema-0", &json));
    ASSERT_OK_AND_ASSIGN(auto schema, TableSchema::CreateFromJson(json));
    auto external_arrow_schema = arrow::schema(
        {arrow::field("external_f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
         arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())});
    ASSERT_OK_AND_ASSIGN(auto external_schema, TableSchema::Create(0, external_arrow_schema, {"f1"},
                                                                   {}, schema->Options()));
    ASSERT_OK_AND_ASSIGN(auto external_json, external_schema->GetJsonSchema());
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(append_path_, fs_, "main"));
    int32_t reads = fs_->schema_reads.load();
    ScanContextBuilder builder(append_path_);
    builder.WithTableResources(resources)
        .WithExecutor(executor_)
        .SetTableSchema(external_json)
        .SetPredicate(PredicateBuilder::IsNotNull(0, "external_f0", FieldType::STRING));
    ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto external_scan, TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(auto external_plan, external_scan->CreatePlan());
    ASSERT_FALSE(external_plan->Splits().empty());
    ASSERT_EQ(fs_->schema_reads.load(), reads);

    auto predicate = PredicateBuilder::IsNotNull(0, "f0", FieldType::STRING);
    ASSERT_OK_AND_ASSIGN(auto normal_scan, NewScan(append_path_, resources, predicate, {}, false));
    ASSERT_OK_AND_ASSIGN(auto normal_plan, normal_scan->CreatePlan());
    ASSERT_FALSE(normal_plan->Splits().empty());
    ASSERT_EQ(fs_->schema_reads.load(), reads + 1);
    ASSERT_NOK(NewScan(append_path_, resources,
                       PredicateBuilder::IsNotNull(0, "external_f0", FieldType::STRING), {},
                       false));
}

TEST_F(TableScanResourcesTest, ConcurrentScansAndSchemaReadRetry) {
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(append_path_, fs_, "main"));
    fs_->fail_schema_read = true;
    ASSERT_NOK_WITH_MSG(NewScan(append_path_, resources, nullptr, {}, false),
                        "injected schema read failure");
    std::vector<std::future<Result<std::shared_ptr<Plan>>>> futures;
    for (int32_t i = 0; i < 8; ++i) {
        futures.push_back(
            std::async(std::launch::async, [this, resources]() -> Result<std::shared_ptr<Plan>> {
                PAIMON_ASSIGN_OR_RAISE(
                    std::unique_ptr<TableScan> scan,
                    NewScan(append_path_, resources,
                            PredicateBuilder::IsNotNull(0, "f0", FieldType::STRING), {}, false));
                return scan->CreatePlan();
            }));
    }
    ASSERT_OK_AND_ASSIGN(auto expected, futures[0].get());
    ASSERT_FALSE(expected->Splits().empty());
    for (size_t i = 1; i < futures.size(); ++i) {
        ASSERT_OK_AND_ASSIGN(auto actual, futures[i].get());
        CheckPlans(expected, actual);
    }
    int32_t reads = fs_->schema_reads.load();
    ASSERT_OK_AND_ASSIGN(auto scan, NewScan(append_path_, resources, nullptr, {}, false));
    ASSERT_OK_AND_ASSIGN(auto plan, scan->CreatePlan());
    ASSERT_EQ(fs_->schema_reads.load(), reads);
}

TEST_F(TableScanResourcesTest, SnapshotsAndStreamingProgressStayIndependent) {
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(append_path_, fs_, "main"));
    for (const std::string snapshot : {"1", "5"}) {
        std::map<std::string, std::string> options = {{Options::SCAN_SNAPSHOT_ID, snapshot}};
        ASSERT_OK_AND_ASSIGN(auto expected_scan,
                             NewScan(append_path_, nullptr, nullptr, options, false));
        ASSERT_OK_AND_ASSIGN(auto expected, expected_scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(auto actual_scan,
                             NewScan(append_path_, resources, nullptr, options, false));
        ASSERT_OK_AND_ASSIGN(auto actual, actual_scan->CreatePlan());
        CheckPlans(expected, actual);
    }
    std::map<std::string, std::string> options = {{Options::SCAN_SNAPSHOT_ID, "1"}};
    ASSERT_OK_AND_ASSIGN(auto first, NewScan(append_path_, resources, nullptr, options, true));
    ASSERT_OK_AND_ASSIGN(auto second, NewScan(append_path_, resources, nullptr, options, true));
    ASSERT_OK_AND_ASSIGN(auto first_plan, first->CreatePlan());
    ASSERT_OK_AND_ASSIGN(auto next_plan, first->CreatePlan());
    ASSERT_OK_AND_ASSIGN(auto second_plan, second->CreatePlan());
    CheckPlans(first_plan, second_plan);
    ASSERT_NE(first_plan->SnapshotId(), next_plan->SnapshotId());
}

TEST_F(TableScanResourcesTest, SystemTableScansForwardResources) {
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(pk_path_, fs_, "main"));
    for (const std::string suffix : {"$ro", "$audit_log"}) {
        ASSERT_OK_AND_ASSIGN(auto expected_scan,
                             NewScan(pk_path_ + suffix, nullptr, nullptr, {}, false));
        ASSERT_OK_AND_ASSIGN(auto expected, expected_scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(auto first, NewScan(pk_path_ + suffix, resources, nullptr, {}, false));
        ASSERT_OK_AND_ASSIGN(auto first_plan, first->CreatePlan());
        CheckPlans(expected, first_plan);
        int32_t reads = fs_->schema_reads.load();
        ASSERT_OK_AND_ASSIGN(auto second,
                             NewScan(pk_path_ + suffix, resources, nullptr, {}, false));
        ASSERT_OK_AND_ASSIGN(auto second_plan, second->CreatePlan());
        CheckPlans(expected, second_plan);
        ASSERT_EQ(fs_->schema_reads.load(), reads);
    }
}

TEST_F(TableScanResourcesTest, BranchResourcesUseBranchSchemaAndSnapshot) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    std::string path = directory->Str() + "/table";
    ASSERT_TRUE(TestUtil::CopyDirectory(append_path_, path));
    std::string branch_path = BranchManager::BranchPath(path, "dev");
    ASSERT_OK(fs_->Mkdirs(branch_path));
    ASSERT_TRUE(TestUtil::CopyDirectory(append_path_ + "/schema", branch_path + "/schema"));
    ASSERT_TRUE(TestUtil::CopyDirectory(append_path_ + "/snapshot", branch_path + "/snapshot"));
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(path, fs_, "dev"));
    std::map<std::string, std::string> options = {{Options::BRANCH, "dev"}};
    ASSERT_OK_AND_ASSIGN(auto expected_scan, NewScan(path, nullptr, nullptr, options, false));
    ASSERT_OK_AND_ASSIGN(auto expected, expected_scan->CreatePlan());
    ASSERT_FALSE(expected->Splits().empty());
    ScanContextBuilder builder(path);
    // A supplied schema is only used on main; it must not enter the branch's schema cache.
    builder.WithTableResources(resources).WithExecutor(executor_).SetTableSchema("not JSON");
    ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto scan, TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(auto actual, scan->CreatePlan());
    CheckPlans(expected, actual);
    int32_t reads = fs_->schema_reads.load();
    ASSERT_OK_AND_ASSIGN(auto second, NewScan(path, resources, nullptr, {}, false));
    ASSERT_OK_AND_ASSIGN(auto second_plan, second->CreatePlan());
    CheckPlans(expected, second_plan);
    ASSERT_EQ(fs_->schema_reads.load(), reads);
}

TEST_F(TableScanResourcesTest, LimitDoesNotAffectOtherScans) {
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(append_path_, fs_, "main"));
    ScanContextBuilder builder(append_path_);
    builder.WithTableResources(resources).WithExecutor(executor_).SetLimit(1);
    ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto limited, TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(auto limited_plan, limited->CreatePlan());
    ASSERT_OK_AND_ASSIGN(auto full, NewScan(append_path_, resources, nullptr, {}, false));
    ASSERT_OK_AND_ASSIGN(auto full_plan, full->CreatePlan());
    ASSERT_FALSE(limited_plan->Splits().empty());
    ASSERT_LT(limited_plan->Splits().size(), full_plan->Splits().size());
    ASSERT_OK_AND_ASSIGN(auto baseline, NewScan(append_path_, nullptr, nullptr, {}, false));
    ASSERT_OK_AND_ASSIGN(auto baseline_plan, baseline->CreatePlan());
    CheckPlans(baseline_plan, full_plan);
}

TEST_F(TableScanResourcesTest, FormatTableRejectsManagedResources) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    std::string path = directory->Str();
    ASSERT_OK_AND_ASSIGN(
        auto schema,
        TableSchema::Create(0, arrow::schema({arrow::field("value", arrow::int32())}), {}, {},
                            {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}}));
    ASSERT_OK_AND_ASSIGN(auto json, schema->GetJsonSchema());
    ASSERT_OK(fs_->Mkdirs(path + "/schema"));
    ASSERT_OK(fs_->WriteFile(path + "/schema/schema-0", json, false));
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(path, fs_, "main"));
    ASSERT_NOK_WITH_MSG(NewScan(path, resources, nullptr, {}, false),
                        "cannot be used with a format table");
    ASSERT_OK_AND_ASSIGN(auto table, FormatTable::Create(fs_, path, Identifier("db", "table"), {}));
    ScanContextBuilder builder(table);
    builder.WithTableResources(resources);
    ASSERT_NOK_WITH_MSG(builder.Finish(), "cannot be used with a format table");
}

TEST_F(TableScanResourcesTest, ActiveScanOutlivesResources) {
    ASSERT_OK_AND_ASSIGN(auto resources, TableScanResources::Create(append_path_, fs_, "main"));
    std::weak_ptr<TableScanResources> weak_resources = resources;
    ASSERT_OK_AND_ASSIGN(
        auto scan, NewScan(append_path_, resources,
                           PredicateBuilder::IsNotNull(0, "f0", FieldType::STRING), {}, false));
    resources.reset();
    ASSERT_TRUE(weak_resources.expired());
    ASSERT_OK_AND_ASSIGN(auto plan, scan->CreatePlan());
    ASSERT_FALSE(plan->Splits().empty());
}

}  // namespace paimon::test
