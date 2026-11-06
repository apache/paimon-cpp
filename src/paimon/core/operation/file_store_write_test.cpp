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

#include "paimon/file_store_write.h"

#include <map>
#include <optional>
#include <string>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/catalog/table_load_options.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/operation/key_value_file_store_write.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_schema_layout.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/testing/mock/mock_catalog.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

TEST(FileStoreWriteTest, TestRealtimeCatalogCommitRefreshAndRecovery) {
    for (RealtimeStoreMode mode :
         {RealtimeStoreMode::APPEND_ONLY, RealtimeStoreMode::PRIMARY_KEY}) {
        SCOPED_TRACE(static_cast<int>(mode));
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        const std::string table_path = dir->Str();
        const Identifier identifier("db", "table");
        const auto logical_schema = arrow::schema(
            {arrow::field("id", arrow::int64(), false), arrow::field("value", arrow::utf8())});
        const std::map<std::string, std::string> options = {{Options::BUCKET, "1"},
                                                            {Options::BUCKET_KEY, "id"},
                                                            {Options::FILE_FORMAT, "parquet"},
                                                            {Options::WRITE_ONLY, "true"},
                                                            {Options::REALTIME_ENABLED, "true"}};
        const std::vector<std::string> primary_keys = mode == RealtimeStoreMode::PRIMARY_KEY
                                                          ? std::vector<std::string>{"id"}
                                                          : std::vector<std::string>{};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema,
                             TableSchema::Create(0, logical_schema, {}, primary_keys, options));
        auto catalog = std::make_shared<MockVersionManagedCatalog>();
        catalog->SetTableSchema(schema);
        catalog->SetFileSystem(dir->GetFileSystem());
        catalog->CheckBaseSnapshotUuid();
        ASSERT_OK_AND_ASSIGN(auto schema_layout,
                             RealtimeSchemaLayout::Create(mode, logical_schema));
        auto make_batch = [&](const std::string& json) -> Result<std::unique_ptr<RecordBatch>> {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> array,
                arrow::ipc::internal::json::ArrayFromJSON(
                    arrow::struct_(schema_layout->InputSchema()->fields()), json));
            ArrowArray c_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
            return RecordBatchBuilder(&c_array).SetBucket(0).Finish();
        };
        auto create_writer = [&](const std::shared_ptr<RealtimeContext>& realtime_context)
            -> Result<std::unique_ptr<FileStoreWrite>> {
            TableLoadOptions builder_load_options{table_path};
            std::map<std::string, std::string> builder_options;
            WriteContextBuilder builder("writer");
            builder_load_options.catalog = catalog;
            builder_load_options.identifier.emplace(identifier);
            builder_options[Options::FILE_SYSTEM] = "unregistered-file-system";
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<WriteContext> ctx,
                builder.WithStreamingMode(true).WithRealtimeContext(realtime_context).Finish());
            return FileStoreWrite::Create(builder_load_options, std::move(ctx), builder_options);
        };
        ASSERT_OK_AND_ASSIGN(auto realtime_context, RealtimeContext::Create());
        ASSERT_OK_AND_ASSIGN(auto writer, create_writer(realtime_context));
        ASSERT_NOK_WITH_MSG(writer->GetBaseSnapshotId({}, 0), "writer does not exist");
        ASSERT_OK_AND_ASSIGN(auto batch, make_batch(R"([[10, 1, "a"], [20, 2, "b"]])"));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(auto base_snapshot_id, writer->GetBaseSnapshotId({}, 0));
        ASSERT_EQ(base_snapshot_id, std::nullopt);
        ASSERT_OK_AND_ASSIGN(auto progress, writer->PrepareCommitWithProgress(1));
        ASSERT_EQ(progress.size(), 1u);
        TableLoadOptions commit_builder_load_options{table_path};
        std::map<std::string, std::string> commit_builder_options;
        CommitContextBuilder commit_builder("writer");
        commit_builder_load_options.catalog = catalog;
        commit_builder_load_options.identifier.emplace(identifier);
        ASSERT_OK_AND_ASSIGN(auto commit_ctx, commit_builder.Finish());
        ASSERT_OK_AND_ASSIGN(
            auto commit, FileStoreCommit::Create(commit_builder_load_options, std::move(commit_ctx),
                                                 commit_builder_options));
        ASSERT_OK_AND_ASSIGN(int64_t snapshot_id,
                             commit->CommitWithProgress(progress, 1, std::nullopt));
        ASSERT_EQ(snapshot_id, 1);
        ASSERT_OK_AND_ASSIGN(bool has_schema, dir->GetFileSystem()->Exists(
                                                  PathUtil::JoinPath(table_path, "schema")));
        ASSERT_FALSE(has_schema);
        ASSERT_OK_AND_ASSIGN(bool has_snapshot, dir->GetFileSystem()->Exists(PathUtil::JoinPath(
                                                    table_path, "snapshot/snapshot-1")));
        ASSERT_FALSE(has_snapshot);

        ASSERT_OK_AND_ASSIGN(base_snapshot_id, writer->GetBaseSnapshotId({}, 0));
        ASSERT_EQ(base_snapshot_id, std::nullopt);
        catalog->SetLoadSnapshotStatus(Status::IOError("catalog unavailable"));
        ASSERT_NOK_WITH_MSG(writer->RefreshCommittedSnapshot(snapshot_id), "catalog unavailable");
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, writer->GetBaseSnapshotId({}, 0));
        ASSERT_EQ(base_snapshot_id, std::nullopt);
        catalog->SetLoadSnapshotStatus(Status::OK());
        ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, writer->GetBaseSnapshotId({}, 0));
        ASSERT_EQ(base_snapshot_id, 1);
        ASSERT_OK_AND_ASSIGN(double retained_rows,
                             writer->GetMetrics()->GetGauge(RealtimeMetrics::kTotalRowCount));
        ASSERT_EQ(retained_rows, 0);
        ASSERT_OK(writer->Close());
        ASSERT_NOK_WITH_MSG(writer->GetBaseSnapshotId({}, 0), "writer does not exist");

        ASSERT_OK_AND_ASSIGN(auto recovered_context, RealtimeContext::Create());
        ASSERT_OK_AND_ASSIGN(auto recovered_writer, create_writer(recovered_context));
        ASSERT_OK_AND_ASSIGN(auto recovered_impl, RealtimeContextImpl::Cast(recovered_context));
        ASSERT_OK_AND_ASSIGN(auto read_state, recovered_impl->AcquireReadState());
        const RealtimeOffsetMap expected_offsets = {{RealtimePartitionBucket({}, 0), 21}};
        ASSERT_EQ(read_state.committed_offsets, expected_offsets);
        ASSERT_OK_AND_ASSIGN(auto next_batch, make_batch(R"([[21, 1, "updated"]])"));
        ASSERT_OK(recovered_writer->Write(std::move(next_batch)));
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, recovered_writer->GetBaseSnapshotId({}, 0));
        ASSERT_EQ(base_snapshot_id, 1);
        ASSERT_OK_AND_ASSIGN(auto shared_writer, create_writer(recovered_context));
        ASSERT_OK_AND_ASSIGN(auto empty_batch, make_batch("[]"));
        ASSERT_OK(shared_writer->Write(std::move(empty_batch)));
        ASSERT_OK_AND_ASSIGN(auto next_progress, recovered_writer->PrepareCommitWithProgress(2));
        ASSERT_OK_AND_ASSIGN(snapshot_id,
                             commit->CommitWithProgress(next_progress, 2, std::nullopt));
        ASSERT_EQ(snapshot_id, 2);
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, recovered_writer->GetBaseSnapshotId({}, 0));
        ASSERT_EQ(base_snapshot_id, 1);
        ASSERT_OK(recovered_writer->RefreshCommittedSnapshot(snapshot_id));
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, recovered_writer->GetBaseSnapshotId({}, 0));
        ASSERT_EQ(base_snapshot_id, 2);
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, shared_writer->GetBaseSnapshotId({}, 0));
        ASSERT_EQ(base_snapshot_id, 2);
        ASSERT_OK(shared_writer->Close());
        ASSERT_OK(recovered_writer->Close());

        catalog->SetLoadSnapshotStatus(Status::IOError("catalog unavailable"));
        ASSERT_OK_AND_ASSIGN(auto failed_context, RealtimeContext::Create());
        ASSERT_NOK_WITH_MSG(create_writer(failed_context), "catalog unavailable");
    }
}

TEST(FileStoreWriteTest, TestBucketBaseSnapshotTracksActualLazyRestore) {
    for (bool primary_key : {false, true}) {
        SCOPED_TRACE(primary_key);
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        const Identifier identifier("db", "table");
        const auto logical_schema = arrow::schema(
            {arrow::field("dt", arrow::utf8(), false), arrow::field("id", arrow::int64(), false)});
        const std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                            {Options::BUCKET_KEY, "id"},
                                                            {Options::FILE_FORMAT, "parquet"},
                                                            {Options::WRITE_ONLY, "true"}};
        const std::vector<std::string> primary_keys =
            primary_key ? std::vector<std::string>{"dt", "id"} : std::vector<std::string>{};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema,
                             TableSchema::Create(0, logical_schema, {"dt"}, primary_keys, options));
        auto catalog = std::make_shared<MockVersionManagedCatalog>();
        catalog->SetTableSchema(schema);
        catalog->SetFileSystem(dir->GetFileSystem());
        catalog->CheckBaseSnapshotUuid();
        auto create_writer = [&](bool ignore_previous) -> Result<std::unique_ptr<FileStoreWrite>> {
            TableLoadOptions builder_load_options{dir->Str()};
            std::map<std::string, std::string> builder_options;
            WriteContextBuilder builder("writer");
            builder_load_options.catalog = catalog;
            builder_load_options.identifier.emplace(identifier);
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<WriteContext> context,
                builder.WithStreamingMode(true).WithIgnorePreviousFiles(ignore_previous).Finish());
            return FileStoreWrite::Create(builder_load_options, std::move(context),
                                          builder_options);
        };
        auto make_batch = [&](const std::string& dt, int32_t bucket,
                              const std::string& json) -> Result<std::unique_ptr<RecordBatch>> {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                              arrow::ipc::internal::json::ArrayFromJSON(
                                                  arrow::struct_(logical_schema->fields()), json));
            ArrowArray c_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
            return RecordBatchBuilder(&c_array)
                .SetPartition({{"dt", dt}})
                .SetBucket(bucket)
                .Finish();
        };
        TableLoadOptions commit_builder_load_options{dir->Str()};
        std::map<std::string, std::string> commit_builder_options;
        CommitContextBuilder commit_builder("writer");
        commit_builder_load_options.catalog = catalog;
        commit_builder_load_options.identifier.emplace(identifier);
        ASSERT_OK_AND_ASSIGN(auto commit_context, commit_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(commit_builder_load_options,
                                                                  std::move(commit_context),
                                                                  commit_builder_options));
        auto seed_snapshot = [&](int32_t bucket, int64_t identifier) -> Status {
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> writer, create_writer(false));
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> batch,
                                   make_batch("a", bucket, R"([["a", 1]])"));
            PAIMON_RETURN_NOT_OK(writer->Write(std::move(batch)));
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> messages,
                                   writer->PrepareCommit(false, identifier));
            PAIMON_RETURN_NOT_OK(commit->Commit(messages, identifier));
            return writer->Close();
        };

        ASSERT_OK_AND_ASSIGN(auto writer, create_writer(false));
        ASSERT_NOK_WITH_MSG(writer->GetBaseSnapshotId({{"dt", "empty"}}, 0),
                            "writer does not exist");
        ASSERT_OK_AND_ASSIGN(auto batch, make_batch("empty", 0, R"([["empty", 2]])"));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(auto base_snapshot_id,
                             writer->GetBaseSnapshotId({{"dt", "empty"}}, 0));
        ASSERT_EQ(base_snapshot_id, std::nullopt);

        ASSERT_OK(seed_snapshot(0, 1));
        ASSERT_EQ(catalog->AcceptedSnapshots().back().Id(), 1);
        ASSERT_OK_AND_ASSIGN(batch, make_batch("a", 0, R"([["a", 2]])"));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, writer->GetBaseSnapshotId({{"dt", "a"}}, 0));
        ASSERT_EQ(base_snapshot_id, 1);

        ASSERT_OK(seed_snapshot(1, 2));
        ASSERT_EQ(catalog->AcceptedSnapshots().back().Id(), 2);
        ASSERT_OK_AND_ASSIGN(batch, make_batch("a", 1, R"([["a", 3]])"));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, writer->GetBaseSnapshotId({{"dt", "a"}}, 1));
        ASSERT_EQ(base_snapshot_id, 2);
        ASSERT_OK_AND_ASSIGN(batch, make_batch("b", 0, R"([["b", 4]])"));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, writer->GetBaseSnapshotId({{"dt", "b"}}, 0));
        ASSERT_EQ(base_snapshot_id, 2);

        ASSERT_OK_AND_ASSIGN(auto ignoring_writer, create_writer(true));
        ASSERT_OK_AND_ASSIGN(batch, make_batch("a", 0, R"([["a", 5]])"));
        ASSERT_OK(ignoring_writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(base_snapshot_id,
                             ignoring_writer->GetBaseSnapshotId({{"dt", "a"}}, 0));
        ASSERT_EQ(base_snapshot_id, std::nullopt);
        ASSERT_OK(ignoring_writer->Close());

        const size_t snapshot_loads = catalog->LoadSnapshotCalls();
        catalog->SetLoadSnapshotStatus(Status::IOError("catalog unavailable"));
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, writer->GetBaseSnapshotId({{"dt", "a"}}, 0));
        ASSERT_EQ(base_snapshot_id, 1);
        ASSERT_OK_AND_ASSIGN(base_snapshot_id, writer->GetBaseSnapshotId({{"dt", "empty"}}, 0));
        ASSERT_EQ(base_snapshot_id, std::nullopt);
        ASSERT_NOK_WITH_MSG(writer->GetBaseSnapshotId({{"dt", "b"}}, 1), "writer does not exist");
        ASSERT_EQ(catalog->LoadSnapshotCalls(), snapshot_loads);
        ASSERT_OK(writer->Close());
        ASSERT_NOK_WITH_MSG(writer->GetBaseSnapshotId({{"dt", "a"}}, 0), "writer does not exist");
    }
}

TEST(FileStoreWriteTest, TestCreateWithInvalidInput) {
    auto dir = UniqueTestDirectory::Create();
    TableLoadOptions builder_load_options{dir->Str()};
    std::map<std::string, std::string> builder_options;
    WriteContextBuilder builder("commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> ctx1,
                         builder.WithMemoryPool(nullptr).Finish());
    ASSERT_NOK(FileStoreWrite::Create(builder_load_options, std::move(ctx1), builder_options));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> ctx2,
                         builder.WithExecutor(nullptr).Finish());
    ASSERT_NOK(FileStoreWrite::Create(builder_load_options, std::move(ctx2), builder_options));
    ASSERT_NOK(FileStoreWrite::Create({}, /*context=*/nullptr));
}

TEST(FileStoreWriteTest, TestCreateAppendTable) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/true));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{"f0", "f3"}, /*primary_keys=*/{},
                                   /*options=*/{}, /*ignore_if_exists=*/false));
    std::shared_ptr<Catalog> shared_catalog(std::move(catalog));
    ASSERT_FALSE(shared_catalog->SupportsVersionManagement());
    for (bool use_catalog : {false, true}) {
        SCOPED_TRACE(use_catalog);
        TableLoadOptions context_builder_load_options{PathUtil::JoinPath(dir->Str(), "foo.db/bar")};
        std::map<std::string, std::string> context_builder_options;
        WriteContextBuilder context_builder("commit_user_1");
        if (use_catalog) {
            context_builder_load_options.catalog = shared_catalog;
            context_builder_load_options.identifier.emplace(Identifier("foo", "bar"));
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FileStoreWrite> file_store_write,
            FileStoreWrite::Create(context_builder_load_options, std::move(write_context),
                                   context_builder_options));
        ASSERT_OK(file_store_write->Close());
    }
}

TEST(FileStoreWriteTest, TestCatalogWriteAsksTheCatalogOnlyForTheMainBranchSchema) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const auto logical_schema = arrow::schema(
        {arrow::field("id", arrow::int64(), false), arrow::field("value", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema,
                         TableSchema::Create(0, logical_schema, /*partition_keys=*/{},
                                             /*primary_keys=*/{}, /*options=*/{}));
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableSchema(schema);
    catalog->SetFileSystem(dir->GetFileSystem());

    TableLoadOptions main_builder_load_options{dir->Str()};
    std::map<std::string, std::string> main_builder_options;
    WriteContextBuilder main_builder("commit_user_1");
    main_builder_load_options.catalog = catalog;
    main_builder_load_options.identifier.emplace(Identifier("db", "table$branch_main"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> main_context, main_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> main_write,
                         FileStoreWrite::Create(main_builder_load_options, std::move(main_context),
                                                main_builder_options));
    ASSERT_OK(main_write->Close());
    ASSERT_EQ(catalog->LoadTableSchemaIdentifiers().size(), 1u);
    ASSERT_EQ(catalog->LoadTableSchemaIdentifiers().front(), Identifier("db", "table"));

    SchemaManager branch_schema_manager(dir->GetFileSystem(), dir->Str(), "dev");
    ASSERT_OK(branch_schema_manager.CreateTable(logical_schema, /*partition_keys=*/{},
                                                /*primary_keys=*/{}, /*options=*/{}));
    catalog->SetTableSchema(nullptr);
    TableLoadOptions branch_builder_load_options{dir->Str()};
    std::map<std::string, std::string> branch_builder_options;
    WriteContextBuilder branch_builder("commit_user_1");
    branch_builder_load_options.catalog = catalog;
    branch_builder_load_options.identifier.emplace(Identifier("db", "table$branch_dev"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> branch_context, branch_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> branch_write,
                         FileStoreWrite::Create(branch_builder_load_options,
                                                std::move(branch_context), branch_builder_options));
    ASSERT_OK(branch_write->Close());
    ASSERT_EQ(catalog->LoadTableSchemaIdentifiers().size(), 1u);
}

TEST(FileStoreWriteTest, TestCatalogWriteUsesPerTableFileSystem) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    const Identifier identifier("foo", "bar");
    const auto logical_schema = arrow::schema(
        {arrow::field("id", arrow::int64(), false), arrow::field("value", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> schema,
        TableSchema::Create(0, logical_schema, /*partition_keys=*/{},
                            /*primary_keys=*/{}, {{Options::FILE_FORMAT, "parquet"}}));
    auto catalog = std::make_shared<MockVersionManagedCatalog>();
    catalog->SetTableSchema(schema);
    // The catalog-wide file system is a different instance, so a write that reached for it
    // instead of the table's own would not be served the credentials the table needs.
    catalog->SetFileSystem(std::make_shared<LocalFileSystem>());
    catalog->SetTableFileSystem(dir->GetFileSystem());

    TableLoadOptions builder_load_options{table_path};
    std::map<std::string, std::string> builder_options;
    WriteContextBuilder builder("writer");
    builder_load_options.catalog = catalog;
    builder_load_options.identifier.emplace(identifier);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> ctx, builder.Finish());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileStoreWrite> writer,
        FileStoreWrite::Create(builder_load_options, std::move(ctx), builder_options));
    ASSERT_OK(writer->Close());

    // The file system was resolved per table, not from the catalog-wide one.
    const std::vector<Identifier>& requests = catalog->TableFileSystemRequests();
    ASSERT_FALSE(requests.empty());
    ASSERT_EQ(requests.back().GetFullName(), identifier.GetFullName());
}

TEST(FileStoreWriteTest, TestCreateAppendTableWithInvalidBucket) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    std::map<std::string, std::string> options;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
    ASSERT_OK(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/true));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{"f0", "f3"},
                                   /*primary_keys=*/{}, options, /*ignore_if_exists=*/false));
    TableLoadOptions context_builder_load_options{PathUtil::JoinPath(dir->Str(), "foo.db/bar")};
    std::map<std::string, std::string> context_builder_options;
    WriteContextBuilder context_builder("commit_user_1");
    context_builder_options[Options::BUCKET] = "-2";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreWrite::Create(context_builder_load_options,
                                               std::move(write_context), context_builder_options),
                        "not support bucket -2 in append table");
}

TEST(FileStoreWriteTest, TestCreateAppendTableWithInvalidWriteType) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    std::map<std::string, std::string> options = {
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
    };
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
    ASSERT_OK(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/true));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{},
                                   /*primary_keys=*/{}, options, /*ignore_if_exists=*/false));
    TableLoadOptions context_builder_load_options{PathUtil::JoinPath(dir->Str(), "foo.db/bar")};
    std::map<std::string, std::string> context_builder_options;
    WriteContextBuilder context_builder("commit_user_1");
    context_builder_options[Options::BUCKET] = "-1";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.WithWriteSchema({"field_non_exist"}).Finish());
    ASSERT_NOK_WITH_MSG(FileStoreWrite::Create(context_builder_load_options,
                                               std::move(write_context), context_builder_options),
                        "write field field_non_exist does not exist in table schema");
}

TEST(FileStoreWriteTest, TestCreatePrimaryKeyTable) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/true));
    std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f1"}};
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{"f0"}, /*primary_keys=*/{"f0", "f1", "f4"},
                                   options, /*ignore_if_exists=*/false));
    TableLoadOptions context_builder_load_options{PathUtil::JoinPath(dir->Str(), "foo.db/bar")};
    std::map<std::string, std::string> context_builder_options;
    WriteContextBuilder context_builder("commit_user_1");
    context_builder_options[Options::BUCKET] = "2";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(context_builder_load_options,
                                                std::move(write_context), context_builder_options));

    auto fs = std::make_shared<LocalFileSystem>();
    SchemaManager schema_manager(fs, PathUtil::JoinPath(dir->Str(), "foo.db/bar"));
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.Latest());
    ASSERT_TRUE(table_schema);

    ASSERT_OK_AND_ASSIGN(auto trimmed_pk, table_schema.value()->TrimmedPrimaryKeys());
    auto key_value_file_store_write = dynamic_cast<KeyValueFileStoreWrite*>(file_store_write.get());
    ASSERT_TRUE(key_value_file_store_write);
}

TEST(FileStoreWriteTest, TestCreatePrimaryKeyTableWithInvalidBucket) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),    arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),   arrow::field("f5", arrow::int32())};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    std::map<std::string, std::string> options;
    options[Options::BUCKET] = "-1";
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
    ASSERT_OK(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/true));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{"f0", "f3"},
                                   /*primary_keys=*/{"f1", "f4"}, options,
                                   /*ignore_if_exists=*/false));
    TableLoadOptions context_builder_load_options{PathUtil::JoinPath(dir->Str(), "foo.db/bar")};
    std::map<std::string, std::string> context_builder_options;
    WriteContextBuilder context_builder("commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreWrite::Create(context_builder_load_options,
                                               std::move(write_context), context_builder_options),
                        "not support bucket -1 in key value table");
}

}  // namespace paimon::test
