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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_primary_key_reader.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/data/shredding/map_shared_shredding_schema_utils.h"
#include "paimon/data/variant.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/orphan_files_cleaner.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/reader/count_reader.h"
#include "paimon/realtime/arrow_realtime_store_factory.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/realtime/realtime_store.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/variant_test_data.h"
#include "paimon/write_context.h"

namespace paimon::test {
namespace {

class TrackingRealtimeReadView final : public RealtimeReadView {
 public:
    explicit TrackingRealtimeReadView(std::shared_ptr<RealtimeReadView> delegate)
        : delegate_(std::move(delegate)) {}

    std::optional<OffsetRange> GetOffsetRange() const override {
        return delegate_->GetOffsetRange();
    }

    const std::shared_ptr<RealtimeReadView>& Delegate() const {
        return delegate_;
    }

 private:
    std::shared_ptr<RealtimeReadView> delegate_;
};

class DelegatingRealtimeStore : public RealtimeStore {
 public:
    explicit DelegatingRealtimeStore(const std::shared_ptr<RealtimeStore>& delegate)
        : delegate_(delegate) {}

    Status Write(RealtimeWriteBatch&& batch) override {
        return delegate_->Write(std::move(batch));
    }

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() override {
        return delegate_->SealForCommit();
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) override {
        return delegate_->CreateCommitReaders(segment);
    }

    Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() override {
        return delegate_->AcquireReadView();
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view,
        const RealtimeQueryContext& context) override {
        return delegate_->CreateQueryReaders(view, context);
    }

    Status AdvanceCommittedOffset(int64_t committed_offset) override {
        return delegate_->AdvanceCommittedOffset(committed_offset);
    }

    uint64_t GetMemoryUsage() const override {
        return delegate_->GetMemoryUsage();
    }

 protected:
    std::shared_ptr<RealtimeStore> delegate_;
};

class DecoratingRealtimeStoreFactory final : public RealtimeStoreFactory {
 public:
    using Decorator =
        std::function<std::shared_ptr<RealtimeStore>(const std::shared_ptr<RealtimeStore>&)>;

    explicit DecoratingRealtimeStoreFactory(Decorator decorator)
        : decorator_(std::move(decorator)) {}

    Result<std::shared_ptr<RealtimeStore>> Create(RealtimeStoreCreateRequest&& request) override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeStore> delegate,
                               delegate_.Create(std::move(request)));
        return decorator_(delegate);
    }

 private:
    ArrowRealtimeStoreFactory delegate_;
    Decorator decorator_;
};

template <typename Store, typename... Args>
std::shared_ptr<RealtimeStoreFactory> MakeDecoratingFactory(Args... args) {
    return std::make_shared<DecoratingRealtimeStoreFactory>(
        [=](const std::shared_ptr<RealtimeStore>& delegate) -> std::shared_ptr<RealtimeStore> {
            return std::make_shared<Store>(delegate, args...);
        });
}

class QueryTrackingRealtimeStore final : public DelegatingRealtimeStore {
 public:
    QueryTrackingRealtimeStore(const std::shared_ptr<RealtimeStore>& delegate,
                               const std::shared_ptr<std::atomic<bool>>& saw_query_predicate,
                               const std::shared_ptr<std::weak_ptr<RealtimeReadView>>& query_view)
        : DelegatingRealtimeStore(delegate),
          saw_query_predicate_(saw_query_predicate),
          query_view_(query_view) {}

    Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeReadView> delegate_view,
                               delegate_->AcquireReadView());
        return std::shared_ptr<RealtimeReadView>(
            std::make_shared<TrackingRealtimeReadView>(delegate_view));
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view,
        const RealtimeQueryContext& context) override {
        if (context.predicate) {
            saw_query_predicate_->store(true, std::memory_order_release);
        }
        *query_view_ = view;
        std::shared_ptr<TrackingRealtimeReadView> tracking_view =
            std::dynamic_pointer_cast<TrackingRealtimeReadView>(view);
        if (!tracking_view) {
            return Status::Invalid("query tracking store received an unexpected read view");
        }
        return delegate_->CreateQueryReaders(tracking_view->Delegate(), context);
    }

 private:
    std::shared_ptr<std::atomic<bool>> saw_query_predicate_;
    std::shared_ptr<std::weak_ptr<RealtimeReadView>> query_view_;
};

}  // namespace

namespace {

constexpr char kDropPartitionCommitUser[] = "drop_partition_commit_user";
constexpr char kRollbackCommitUser[] = "rollback_commit_user";
constexpr char kTruncateCommitUser[] = "truncate_commit_user";

}  // namespace

class UnsupportedFunction : public Function {
 public:
    Type GetType() const override {
        return Type::EQUAL;
    }

    std::string ToString() const override {
        return "unsupported";
    }
};

class UnsupportedPredicate : public Predicate {
 public:
    bool operator==(const Predicate&) const override {
        return false;
    }

    const Function& GetFunction() const override {
        return function_;
    }

    std::shared_ptr<Predicate> Negate() const override {
        return nullptr;
    }

    std::string ToString() const override {
        return function_.ToString();
    }

 private:
    UnsupportedFunction function_;
};

class ConcurrentTestState {
 public:
    void WaitForStart() {
        ready_threads_.fetch_add(1, std::memory_order_release);
        while (!start_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    void StartWhenReady(int32_t worker_count) {
        while (ready_threads_.load(std::memory_order_acquire) < worker_count) {
            std::this_thread::yield();
        }
        start_.store(true, std::memory_order_release);
    }

    void RecordError(const Status& status) {
        RecordError(status.ToString());
    }

    void RecordError(std::string error) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            errors_.push_back(std::move(error));
        }
        stop_.store(true, std::memory_order_release);
        progress_cv.notify_all();
        snapshot_cv.notify_all();
    }

    bool RecordErrorIfNotOk(const Status& status) {
        if (status.ok()) {
            return false;
        }
        RecordError(status);
        return true;
    }

    template <typename T>
    bool RecordErrorIfNotOk(const Result<T>& result) {
        if (result.ok()) {
            return false;
        }
        RecordError(result.status());
        return true;
    }

    bool ShouldStop() const {
        return stop_.load(std::memory_order_acquire);
    }

    const std::vector<std::string>& Errors() const {
        return errors_;
    }

    std::mutex mutex;
    std::condition_variable progress_cv;
    std::condition_variable snapshot_cv;

 private:
    std::atomic<bool> start_{false};
    std::atomic<bool> stop_{false};
    std::atomic<int32_t> ready_threads_{0};
    std::vector<std::string> errors_;
};

class RealtimeWriteInteTest : public ::testing::Test {
 protected:
    using Row = std::tuple<int64_t, std::string, std::string>;

    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create("local");
        ASSERT_NE(nullptr, dir_);
        table_path_ = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
        fields_ = {arrow::field("id", arrow::int64()), arrow::field("payload", arrow::utf8()),
                   arrow::field("pt", arrow::utf8())};
        schema_ = arrow::schema(fields_);
        options_ = {
            {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, "orc"},
            {Options::FILE_SYSTEM, "local"},     {Options::BUCKET, "1"},
            {Options::BUCKET_KEY, "id"},         {Options::TARGET_FILE_SIZE, "1048576"},
            {Options::REALTIME_ENABLED, "true"},
        };
    }

    void TearDown() override {
        dir_.reset();
    }

    void CreateTable(const std::vector<std::string>& partition_keys) const {
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*schema_, c_schema.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> catalog,
                             Catalog::Create(dir_->Str(), options_));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), c_schema.get(), partition_keys,
                                       /*primary_keys=*/{}, options_,
                                       /*ignore_if_exists=*/false));
    }

    void CreatePkTable(const std::vector<std::string>& partition_keys = {},
                       const std::vector<std::string>& primary_keys = {"id"}) const {
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*schema_, c_schema.get()).ok());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> catalog,
                             Catalog::Create(dir_->Str(), options_));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        std::vector<std::string> table_primary_keys = partition_keys;
        table_primary_keys.insert(table_primary_keys.end(), primary_keys.begin(),
                                  primary_keys.end());
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), c_schema.get(), partition_keys,
                                       table_primary_keys, options_, /*ignore_if_exists=*/false));
    }

    Result<std::unique_ptr<FileStoreWrite>> CreateRealtimeWriter(
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        WriteContextBuilder builder(table_path_, commit_user_);
        builder.SetOptions(options_).WithStreamingMode(true).WithRealtimeContext(realtime_context);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> context, builder.Finish());
        return FileStoreWrite::Create(std::move(context));
    }

    Result<std::unique_ptr<FileStoreWrite>> CreateRealtimeWriter() const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContext> realtime_context,
                               RealtimeContext::Create());
        return CreateRealtimeWriter(realtime_context);
    }

    Result<std::unique_ptr<RecordBatch>> MakeBatch(const std::vector<Row>& rows,
                                                   bool partitioned) const {
        return MakeBatch(rows, partitioned, /*bucket=*/0);
    }

    Result<std::unique_ptr<RecordBatch>> MakeBatch(const std::vector<Row>& rows, bool partitioned,
                                                   int32_t bucket) const {
        return MakeBatch(rows, partitioned, bucket, /*row_kinds=*/{});
    }

    Result<std::unique_ptr<RecordBatch>> MakeBatch(
        const std::vector<Row>& rows, bool partitioned, int32_t bucket,
        const std::vector<RecordBatch::RowKind>& row_kinds) const {
        if (rows.empty()) {
            return Status::Invalid("cannot create an empty test batch");
        }
        const std::string& partition = std::get<2>(rows.front());
        std::string json = "[";
        for (size_t i = 0; i < rows.size(); ++i) {
            const auto& [id, payload, pt] = rows[i];
            if (partitioned && pt != partition) {
                return Status::Invalid("one test batch must contain only one partition");
            }
            if (i > 0) {
                json += ",";
            }
            json += "[" + std::to_string(id) + ",\"" + payload + "\",\"" + pt + "\"]";
        }
        json += "]";

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), json));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        RecordBatchBuilder builder(&c_array);
        builder.SetRowKinds(row_kinds);
        if (partitioned) {
            builder.SetPartition({{"pt", partition}});
        }
        return builder.SetBucket(bucket).Finish();
    }

    Result<std::unique_ptr<RecordBatch>> MakeDatePartitionBatch(
        int64_t first_id, int64_t count, int32_t date, const std::string& partition) const {
        if (count <= 0) {
            return Status::Invalid("cannot create an empty test batch");
        }
        std::string json = "[";
        for (int64_t i = 0; i < count; ++i) {
            if (i > 0) {
                json += ",";
            }
            int64_t id = first_id + i;
            json += "[" + std::to_string(id) + ",\"value-" + std::to_string(id) + "\"," +
                    std::to_string(date) + "]";
        }
        json += "]";

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), json));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        return RecordBatchBuilder(&c_array)
            .SetPartition({{"pt", partition}})
            .SetBucket(/*bucket=*/0)
            .Finish();
    }

    Result<std::unique_ptr<RecordBatch>> MakeUnpartitionedBatchFromJson(
        const std::string& json) const {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), json));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        return RecordBatchBuilder(&c_array).SetBucket(/*bucket=*/0).Finish();
    }

    static std::vector<Row> MakeRows(int64_t first_id, int64_t count,
                                     const std::string& partition) {
        std::vector<Row> rows;
        rows.reserve(count);
        for (int64_t i = 0; i < count; ++i) {
            int64_t id = first_id + i;
            rows.emplace_back(id, "value-" + std::to_string(id), partition);
        }
        return rows;
    }

    Result<int64_t> Commit(const std::vector<RealtimeCommitProgress>& realtime_commits,
                           int64_t commit_identifier) const {
        CommitContextBuilder builder(table_path_, commit_user_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        return commit->CommitWithProgress(realtime_commits, commit_identifier,
                                          /*watermark=*/std::nullopt);
    }

    Result<int64_t> DropPartition(const std::map<std::string, std::string>& partition,
                                  int64_t commit_identifier) const {
        CommitContextBuilder builder(table_path_, kDropPartitionCommitUser);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        PAIMON_RETURN_NOT_OK(commit->DropPartition({partition}, commit_identifier));
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_));
        SnapshotManager snapshot_manager(options.GetFileSystem(), table_path_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager.LatestSnapshot());
        if (!latest_snapshot) {
            return Status::Invalid("drop partition did not produce a snapshot");
        }
        return latest_snapshot->Id();
    }

    Result<int64_t> TruncateTable(int64_t commit_identifier) const {
        CommitContextBuilder builder(table_path_, kTruncateCommitUser);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        PAIMON_RETURN_NOT_OK(commit->TruncateTable(commit_identifier));
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_));
        SnapshotManager snapshot_manager(options.GetFileSystem(), table_path_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager.LatestSnapshot());
        if (!latest_snapshot) {
            return Status::Invalid("truncate did not produce a snapshot");
        }
        return latest_snapshot->Id();
    }

    Result<int64_t> RollbackToAsLatest(int64_t target_snapshot_id) const {
        CommitContextBuilder builder(table_path_, kRollbackCommitUser);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        PAIMON_ASSIGN_OR_RAISE(bool rolled_back, commit->RollbackToAsLatest(target_snapshot_id));
        if (!rolled_back) {
            return Status::Invalid("failed to commit rollback snapshot");
        }
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_));
        SnapshotManager snapshot_manager(options.GetFileSystem(), table_path_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                               snapshot_manager.LatestSnapshot());
        if (!latest_snapshot) {
            return Status::Invalid("rollback did not produce a snapshot");
        }
        return latest_snapshot->Id();
    }

    Result<Snapshot> CompactAndCommit(const std::map<std::string, std::string>& partition,
                                      int32_t bucket, int64_t commit_identifier) const {
        WriteContextBuilder write_builder(table_path_, commit_user_);
        write_builder.SetOptions(options_).WithStreamingMode(true);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> compaction_writer,
                               FileStoreWrite::Create(std::move(write_context)));
        PAIMON_RETURN_NOT_OK(compaction_writer->Compact(partition, bucket,
                                                        /*full_compaction=*/true));
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::shared_ptr<CommitMessage>> compaction_messages,
            compaction_writer->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
        if (compaction_messages.empty()) {
            return Status::Invalid("compaction did not produce a commit message");
        }

        CommitContextBuilder commit_builder(table_path_, commit_user_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                               commit_builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> compaction_commit,
                               FileStoreCommit::Create(std::move(commit_context)));
        PAIMON_RETURN_NOT_OK(compaction_commit->Commit(compaction_messages, commit_identifier));
        PAIMON_RETURN_NOT_OK(compaction_writer->Close());

        PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_));
        SnapshotManager snapshot_manager(core_options.GetFileSystem(), table_path_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> compact_snapshot,
                               snapshot_manager.LatestSnapshot());
        if (!compact_snapshot) {
            return Status::Invalid("compaction did not produce a snapshot");
        }
        return compact_snapshot.value();
    }

    Result<int32_t> ExpireSnapshots() const {
        CommitContextBuilder builder(table_path_, commit_user_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        return commit->Expire();
    }

    Result<std::shared_ptr<Plan>> CreatePlan(
        const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::shared_ptr<Predicate>& predicate) const {
        ScanContextBuilder scan_builder(table_path_);
        if (realtime_context) {
            scan_builder.WithRealtimeContext(realtime_context);
        }
        scan_builder.SetPredicate(predicate).WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context,
                               scan_builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan,
                               TableScan::Create(std::move(scan_context)));
        return scan->CreatePlan();
    }

    Result<std::unique_ptr<BatchReader>> CreateQueryReader(
        const std::shared_ptr<Plan>& plan,
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        ReadContextBuilder read_builder(table_path_);
        read_builder.SetOptions(options_)
            .SetReadFieldNames({"id", "payload", "pt"})
            .WithRealtimeContext(realtime_context)
            .WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                               TableRead::Create(std::move(read_context)));
        return table_read->CreateReader(plan->Splits());
    }

    Result<std::unique_ptr<BatchReader>> CreateQueryReader(
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan,
                               CreatePlan(realtime_context, /*predicate=*/nullptr));
        return CreateQueryReader(plan, realtime_context);
    }

    Result<std::shared_ptr<arrow::ChunkedArray>> ReadPlan(
        const std::shared_ptr<Plan>& plan, const std::shared_ptr<RealtimeContext>& realtime_context,
        const std::vector<std::string>& read_fields, const std::shared_ptr<Predicate>& predicate,
        bool enable_predicate_filter) const {
        ReadContextBuilder read_builder(table_path_);
        read_builder.SetOptions(options_)
            .SetReadFieldNames(read_fields)
            .SetPredicate(predicate)
            .EnablePredicateFilter(enable_predicate_filter)
            .WithRealtimeContext(realtime_context)
            .WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                               TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                               table_read->CreateReader(plan->Splits()));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> result,
                               ReadResultCollector::CollectResult(std::move(reader)));
        return result;
    }

    void ReadPlanWithSchemaAndCheck(const std::shared_ptr<Plan>& plan,
                                    const std::shared_ptr<RealtimeContext>& realtime_context,
                                    const std::shared_ptr<arrow::Schema>& read_schema,
                                    const std::string& expected_json) const {
        std::unique_ptr<ArrowSchema> c_read_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_read_schema.get()).ok());
        ReadContextBuilder read_builder(table_path_);
        read_builder.SetOptions(options_)
            .SetReadSchema(std::move(c_read_schema))
            .WithRealtimeContext(realtime_context)
            .WithMemoryPool(pool_);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                             TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                             table_read->CreateReader(plan->Splits()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                             ReadResultCollector::CollectResult(std::move(reader)));

        arrow::FieldVector result_fields = {arrow::field("_VALUE_KIND", arrow::int8())};
        result_fields.insert(result_fields.end(), read_schema->fields().begin(),
                             read_schema->fields().end());
        std::shared_ptr<arrow::Array> expected =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(result_fields), expected_json)
                .ValueOrDie();
        ASSERT_NE(nullptr, result);
        ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*result))
            << result->ToString();
    }

    Result<std::vector<Row>> ReadRows(
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan,
                               CreatePlan(realtime_context, /*predicate=*/nullptr));
        return ReadRows(plan, realtime_context);
    }

    Result<std::vector<Row>> ReadRows(
        const std::shared_ptr<Plan>& plan,
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> read_result,
                               ReadPlan(plan, realtime_context, {"id", "payload", "pt"},
                                        /*predicate=*/nullptr,
                                        /*enable_predicate_filter=*/false));
        const std::shared_ptr<arrow::ChunkedArray>& result = read_result;

        std::vector<Row> rows;
        if (!result) {
            return rows;
        }
        for (const std::shared_ptr<arrow::Array>& chunk : result->chunks()) {
            std::shared_ptr<arrow::StructArray> data =
                std::dynamic_pointer_cast<arrow::StructArray>(chunk);
            if (!data || data->num_fields() != 4) {
                return Status::Invalid("unexpected real-time test read schema");
            }
            std::shared_ptr<arrow::Int8Array> row_kinds =
                std::dynamic_pointer_cast<arrow::Int8Array>(data->field(0));
            std::shared_ptr<arrow::Int64Array> ids =
                std::dynamic_pointer_cast<arrow::Int64Array>(data->field(1));
            std::shared_ptr<arrow::StringArray> payloads =
                std::dynamic_pointer_cast<arrow::StringArray>(data->field(2));
            std::shared_ptr<arrow::StringArray> partitions =
                std::dynamic_pointer_cast<arrow::StringArray>(data->field(3));
            if (!row_kinds || !ids || !payloads || !partitions) {
                return Status::Invalid("unexpected real-time test read field type");
            }
            for (int64_t i = 0; i < data->length(); ++i) {
                if (row_kinds->IsNull(i) ||
                    row_kinds->Value(i) != static_cast<int8_t>(RecordBatch::RowKind::INSERT) ||
                    ids->IsNull(i) || payloads->IsNull(i) || partitions->IsNull(i)) {
                    return Status::Invalid("unexpected null or row kind in real-time test result");
                }
                rows.emplace_back(ids->Value(i), payloads->GetString(i), partitions->GetString(i));
            }
        }
        return rows;
    }

    Result<std::vector<Row>> ReadRows() const {
        return ReadRows(/*realtime_context=*/nullptr);
    }

    Result<int64_t> CountRows(const std::shared_ptr<Plan>& plan,
                              const std::shared_ptr<RealtimeContext>& realtime_context) const {
        ReadContextBuilder read_builder(table_path_);
        read_builder.SetOptions(options_)
            .WithRealtimeContext(realtime_context)
            .WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                               TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CountReader> count_reader,
                               table_read->CreateCountReader(plan->Splits()));
        return count_reader->CountRows();
    }

    Result<uint64_t> GetRealtimeMemoryUsage(
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                               RealtimeContextImpl::Cast(realtime_context));
        PAIMON_ASSIGN_OR_RAISE(std::vector<RealtimePartitionBucketView> views,
                               realtime_context_impl->AcquireReadViews());
        uint64_t memory_usage = 0;
        for (const RealtimePartitionBucketView& view : views) {
            memory_usage += view.store->GetMemoryUsage();
        }
        return memory_usage;
    }

    Result<std::vector<int64_t>> ReadPkSequences(
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                               RealtimeContextImpl::Cast(realtime_context));
        PAIMON_ASSIGN_OR_RAISE(std::vector<RealtimePartitionBucketView> views,
                               realtime_context_impl->AcquireReadViews());
        if (views.size() != 1) {
            return Status::Invalid("expected one PK real-time read view");
        }
        PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_));
        SchemaManager schema_manager(core_options.GetFileSystem(), table_path_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> table_schema,
                               schema_manager.Latest());
        if (!table_schema) {
            return Status::Invalid("expected a table schema");
        }
        auto read_schema = std::make_unique<ArrowSchema>();
        std::shared_ptr<arrow::Schema> value_schema =
            DataField::ConvertDataFieldsToArrowSchema(table_schema.value()->Fields());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(
            *RealtimePrimaryKeyLayout::CreateWriteSchema(value_schema->fields()),
            read_schema.get()));
        ScopeGuard schema_guard([schema = read_schema.get()]() { ArrowSchemaRelease(schema); });
        RealtimeQueryContext query_context{read_schema.get(), /*predicate=*/nullptr};
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::unique_ptr<BatchReader>> readers,
            views[0].store->CreateQueryReaders(views[0].read_view, query_context));
        std::vector<int64_t> sequences;
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            while (true) {
                PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
                if (BatchReader::IsEofBatch(batch)) {
                    break;
                }
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> imported,
                    arrow::ImportArray(batch.first.get(), batch.second.get()));
                std::shared_ptr<arrow::StructArray> values =
                    std::dynamic_pointer_cast<arrow::StructArray>(imported);
                if (!values) {
                    return Status::Invalid("PK query reader did not return a StructArray");
                }
                std::shared_ptr<arrow::Int64Array> sequence_array =
                    std::dynamic_pointer_cast<arrow::Int64Array>(
                        values->GetFieldByName(SpecialFields::SequenceNumber().Name()));
                if (!sequence_array) {
                    return Status::Invalid("PK query reader did not return sequence numbers");
                }
                for (int64_t row = 0; row < sequence_array->length(); ++row) {
                    sequences.push_back(sequence_array->Value(row));
                }
            }
            reader->Close();
        }
        return sequences;
    }

    static std::vector<std::shared_ptr<DataFileMeta>> NewFiles(
        const std::vector<RealtimeCommitProgress>& progresses) {
        std::vector<std::shared_ptr<DataFileMeta>> files;
        for (const RealtimeCommitProgress& progress : progresses) {
            std::shared_ptr<CommitMessageImpl> message =
                std::dynamic_pointer_cast<CommitMessageImpl>(progress.commit_message);
            if (!message) {
                continue;
            }
            const std::vector<std::shared_ptr<DataFileMeta>>& new_files =
                message->GetNewFilesIncrement().NewFiles();
            files.insert(files.end(), new_files.begin(), new_files.end());
        }
        return files;
    }

    static Status ValidateReadPrefix(const std::vector<Row>& rows, int64_t total_rows) {
        std::vector<bool> seen(static_cast<size_t>(total_rows), false);
        int64_t max_id = -1;
        for (const Row& row : rows) {
            const auto& [id, payload, partition] = row;
            if (id < 0 || id >= total_rows) {
                return Status::Invalid("real-time read id is out of range");
            }
            if (seen[static_cast<size_t>(id)]) {
                return Status::Invalid("real-time read contains duplicate ids");
            }
            if (payload != "value-" + std::to_string(id) || partition != "p0") {
                return Status::Invalid("real-time read row does not match its id");
            }
            seen[static_cast<size_t>(id)] = true;
            max_id = std::max(max_id, id);
        }
        for (int64_t id = 0; id <= max_id; ++id) {
            if (!seen[static_cast<size_t>(id)]) {
                return Status::Invalid("real-time read contains an id gap");
            }
        }
        return Status::OK();
    }

    void RunUnionReadWithSelectedMapKeys(bool primary_key);

    void RunUnionReadWithVariantAccess(bool primary_key);

    void RunConcurrencyTest(bool primary_key);

    Result<RealtimeOffsetMap> ReadCommittedOffsets() const {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_));
        SnapshotManager snapshot_manager(options.GetFileSystem(), table_path_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, snapshot_manager.LatestSnapshot());
        return RealtimeCommitProperties::ReadOffsets(snapshot, options.GetFileSystem());
    }

    void FinalizeCommitAndCheck(FileStoreWrite* writer,
                                std::vector<RealtimeCommitProgress> realtime_commits,
                                int64_t prepare_identifier, std::vector<Row> expected_rows) const {
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> final_commits,
                             writer->PrepareCommitWithProgress(prepare_identifier));
        realtime_commits.insert(realtime_commits.end(),
                                std::make_move_iterator(final_commits.begin()),
                                std::make_move_iterator(final_commits.end()));
        ASSERT_OK(Commit(realtime_commits, prepare_identifier));
        ASSERT_OK(writer->Close());

        ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
        ASSERT_EQ(expected_rows, actual_rows);
    }

    void ReplayPkWalAndCommit(const std::vector<Row>& wal,
                              const std::vector<RecordBatch::RowKind>& row_kinds,
                              int64_t commit_identifier,
                              const std::vector<Row>& expected_rows) const {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                             RealtimeContext::Create());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                             CreateRealtimeWriter(realtime_context));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(wal, /*partitioned=*/false, /*bucket=*/0, row_kinds));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                             writer->PrepareCommitWithProgress(commit_identifier));
        ASSERT_EQ(1, progress.size());
        ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(progress, commit_identifier));
        ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));
        ASSERT_OK(writer->Close());
        writer.reset();
        realtime_context.reset();

        ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
        ASSERT_EQ(expected_rows, actual_rows);
    }

    void CheckDropDatePartitionRemovesOffset(bool legacy_partition_name_enabled) {
        fields_ = {arrow::field("id", arrow::int64()), arrow::field("payload", arrow::utf8()),
                   arrow::field("pt", arrow::date32())};
        schema_ = arrow::schema(fields_);
        options_[Options::PARTITION_GENERATE_LEGACY_NAME] =
            legacy_partition_name_enabled ? "true" : "false";
        CreateTable(/*partition_keys=*/{"pt"});
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                             RealtimeContext::Create());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                             CreateRealtimeWriter(realtime_context));
        constexpr int32_t kDate = 19723;
        constexpr int64_t kRowCount = 3;
        const std::string partition = "2024-01-01";
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeDatePartitionBatch(/*first_id=*/0, kRowCount, kDate, partition));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                             writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
        ASSERT_EQ(1, commits.size());
        ASSERT_OK(Commit(commits, /*commit_identifier=*/0));

        const std::string normalized_partition =
            legacy_partition_name_enabled ? std::to_string(kDate) : partition;
        RealtimePartitionBucket partition_bucket({{"pt", normalized_partition}}, /*bucket=*/0);
        ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap offsets_before_drop, ReadCommittedOffsets());
        ASSERT_EQ(1, offsets_before_drop.size());
        ASSERT_EQ(kRowCount, offsets_before_drop.at(partition_bucket));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan_before_drop,
                             CreatePlan(/*realtime_context=*/nullptr, /*predicate=*/nullptr));
        ASSERT_OK_AND_ASSIGN(int64_t rows_before_drop,
                             CountRows(plan_before_drop, /*realtime_context=*/nullptr));
        ASSERT_EQ(kRowCount, rows_before_drop);

        ASSERT_OK(DropPartition({{"pt", partition}}, /*commit_identifier=*/1));
        ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap offsets_after_drop, ReadCommittedOffsets());
        ASSERT_TRUE(offsets_after_drop.empty());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan_after_drop,
                             CreatePlan(/*realtime_context=*/nullptr, /*predicate=*/nullptr));
        ASSERT_OK_AND_ASSIGN(int64_t rows_after_drop,
                             CountRows(plan_after_drop, /*realtime_context=*/nullptr));
        ASSERT_EQ(0, rows_after_drop);
        ASSERT_OK(writer->Close());
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::string table_path_;
    std::string commit_user_ = "realtime_commit_user";
    arrow::FieldVector fields_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(RealtimeWriteInteTest, TestRealtimeOperationsRequireEnabledOption) {
    CreateTable(/*partition_keys=*/{});
    std::map<std::string, std::string> disabled_options = options_;
    disabled_options[Options::REALTIME_ENABLED] = "false";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());

    WriteContextBuilder write_builder(table_path_, commit_user_);
    write_builder.SetOptions(disabled_options)
        .WithStreamingMode(true)
        .WithRealtimeContext(realtime_context);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreWrite::Create(std::move(write_context)),
                        "real-time write requires realtime.enabled=true");

    ScanContextBuilder scan_builder(table_path_);
    scan_builder.SetOptions(disabled_options).WithRealtimeContext(realtime_context);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)),
                        "real-time scan requires realtime.enabled=true");

    ReadContextBuilder read_builder(table_path_);
    read_builder.SetOptions(disabled_options).WithRealtimeContext(realtime_context);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                        "real-time read requires realtime.enabled=true");

    CommitContextBuilder commit_builder(table_path_, commit_user_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_builder.SetOptions(disabled_options).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_NOK_WITH_MSG(commit->CommitWithProgress(/*realtime_commits=*/{},
                                                   /*commit_identifier=*/0,
                                                   /*watermark=*/std::nullopt),
                        "CommitWithProgress requires realtime.enabled=true");
}

TEST_F(RealtimeWriteInteTest, TestAppendCommitAndRead) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/10, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    FinalizeCommitAndCheck(writer.get(), /*realtime_commits=*/{}, /*prepare_identifier=*/0, rows);
}

TEST_F(RealtimeWriteInteTest, TestPkRead) {
    CreatePkTable();
    auto saw_query_predicate = std::make_shared<std::atomic<bool>>(false);
    auto query_view = std::make_shared<std::weak_ptr<RealtimeReadView>>();
    auto factory =
        MakeDecoratingFactory<QueryTrackingRealtimeStore>(saw_query_predicate, query_view);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create(factory));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> first_rows = {{1, "old", "p0"}, {2, "two", "p0"}, {1, "new-in-run", "p0"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false, /*bucket=*/0,
                                   {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                                    RecordBatch::RowKind::UPDATE_AFTER}));
    ASSERT_OK(writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> update_batch,
                         MakeBatch({Row{1, "new", "p0"}}, /*partitioned=*/false, /*bucket=*/0,
                                   {RecordBatch::RowKind::UPDATE_AFTER}));
    ASSERT_OK(writer->Write(std::move(update_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> memory_rows, ReadRows(realtime_context));
    ASSERT_EQ((std::vector<Row>{{1, "new", "p0"}, {2, "two", "p0"}}), memory_rows);

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, progress.size());
    ASSERT_OK(Commit(progress, /*commit_identifier=*/0));

    std::vector<Row> second_rows = {{1, "latest", "p0"}, {2, "gone", "p0"}, {3, "three", "p0"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false, /*bucket=*/0,
                                   {RecordBatch::RowKind::UPDATE_AFTER,
                                    RecordBatch::RowKind::DELETE, RecordBatch::RowKind::INSERT}));
    ASSERT_OK(writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> union_rows, ReadRows(realtime_context));
    ASSERT_EQ((std::vector<Row>{{1, "latest", "p0"}, {3, "three", "p0"}}), union_rows);

    const std::string expected_payload = "new";
    std::shared_ptr<Predicate> predicate = PredicateBuilder::Equal(
        /*field_index=*/1, /*field_name=*/"payload", FieldType::STRING,
        Literal(FieldType::STRING, expected_payload.data(), expected_payload.size()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> filtered_plan,
                         CreatePlan(realtime_context, predicate));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> filtered_result,
                         ReadPlan(filtered_plan, realtime_context, {"id", "payload", "pt"},
                                  predicate, /*enable_predicate_filter=*/true));
    ASSERT_EQ(nullptr, filtered_result);
    ASSERT_FALSE(saw_query_predicate->load(std::memory_order_acquire));
    ASSERT_OK(writer->Close());
    writer.reset();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> lifetime_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ReadContextBuilder read_builder(table_path_);
    read_builder.SetOptions(options_)
        .SetReadFieldNames({"id", "payload", "pt"})
        .WithRealtimeContext(realtime_context)
        .WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                         table_read->CreateReader(lifetime_plan->Splits()));
    ASSERT_FALSE(query_view->expired());

    std::weak_ptr<RealtimeContext> weak_context = realtime_context;
    table_read.reset();
    lifetime_plan.reset();
    realtime_context.reset();
    ASSERT_TRUE(weak_context.expired());
    ASSERT_FALSE(query_view->expired());
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch read_batch, reader->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(read_batch));
    reader->Close();
    reader.reset();
    ASSERT_TRUE(query_view->expired());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> read_array,
                         ReadResultCollector::GetArray(std::move(read_batch)));
    ASSERT_NE(nullptr, read_array);
    read_array.reset();
}

TEST_F(RealtimeWriteInteTest, TestPkRealtimeReadOptimizedScanUnsupported) {
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    const std::vector<Row> rows = {{1, "one", "p0"}, {2, "two", "p0"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(progress, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> disk_rows, ReadRows());
    ASSERT_EQ(rows, disk_rows);

    ScanContextBuilder scan_builder(table_path_ + "$ro");
    scan_builder.SetOptions(options_).WithRealtimeContext(realtime_context).WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    Result<std::unique_ptr<TableScan>> scan = TableScan::Create(std::move(scan_context));
    ASSERT_TRUE(scan.status().IsNotImplemented()) << scan.status().ToString();
    ASSERT_NE(std::string::npos, scan.status().ToString().find(
                                     "PK real-time union read does not support read-optimized"));
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkDeleteInsertAndPinnedReadsAcrossRefresh) {
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> delete_batch,
                         MakeBatch({Row{1, "deleted", "p0"}}, /*partitioned=*/false, /*bucket=*/0,
                                   {RecordBatch::RowKind::DELETE}));
    ASSERT_OK(writer->Write(std::move(delete_batch)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> insert_batch,
                         MakeBatch({Row{1, "inserted", "p0"}}, /*partitioned=*/false, /*bucket=*/0,
                                   {RecordBatch::RowKind::INSERT}));
    ASSERT_OK(writer->Write(std::move(insert_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, progress.size());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> pinned_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> reader_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ReadContextBuilder read_builder(table_path_);
    read_builder.SetOptions(options_)
        .SetReadFieldNames({"id", "payload", "pt"})
        .WithRealtimeContext(realtime_context)
        .WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> pinned_reader,
                         table_read->CreateReader(reader_plan->Splits()));

    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(progress, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> plan_rows, ReadRows(pinned_plan, realtime_context));
    ASSERT_EQ((std::vector<Row>{{1, "inserted", "p0"}}), plan_rows);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> reader_rows,
                         ReadResultCollector::CollectResult(std::move(pinned_reader)));
    ASSERT_EQ(1, reader_rows->length());
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkMergeDiskSealedAndActive) {
    options_[Options::READ_BATCH_SIZE] = "2";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    const std::vector<std::vector<Row>> disk_batches = {
        {{1, "disk-1", "p0"}, {2, "disk-2", "p0"}, {3, "disk-3", "p0"}},
        {{10, "disk-10", "p0"}, {11, "disk-11", "p0"}},
    };
    int64_t commit_identifier = 0;
    for (const std::vector<Row>& disk_rows : disk_batches) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(disk_rows, /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                             writer->PrepareCommitWithProgress(commit_identifier));
        ASSERT_EQ(1, progress.size());
        ASSERT_EQ(1, NewFiles(progress).size());
        ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(progress, commit_identifier));
        ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));
        ++commit_identifier;
    }

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> sealed_batch,
        MakeBatch({Row{1, "sealed-1", "p0"}, Row{2, "deleted-2", "p0"}, Row{4, "sealed-4", "p0"}},
                  /*partitioned=*/false, /*bucket=*/0,
                  {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE,
                   RecordBatch::RowKind::INSERT}));
    ASSERT_OK(writer->Write(std::move(sealed_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> sealed_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/2));
    ASSERT_EQ(1, sealed_progress.size());

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> active_batch,
        MakeBatch({Row{1, "active-1", "p0"}, Row{4, "deleted-4", "p0"}, Row{5, "active-5", "p0"}},
                  /*partitioned=*/false, /*bucket=*/0,
                  {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE,
                   RecordBatch::RowKind::INSERT}));
    ASSERT_OK(writer->Write(std::move(active_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadPlan(plan, realtime_context, {"payload", "id"}, /*predicate=*/nullptr,
                                  /*enable_predicate_filter=*/false));
    ASSERT_NE(nullptr, result);
    ASSERT_GT(result->num_chunks(), 1);
    for (const std::shared_ptr<arrow::Array>& chunk : result->chunks()) {
        ASSERT_LE(chunk->length(), 2);
    }
    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("payload", arrow::utf8()),
         arrow::field("id", arrow::int64())});
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, "active-1", 1],
            [0, "disk-3", 3],
            [0, "active-5", 5],
            [0, "disk-10", 10],
            [0, "disk-11", 11]
        ])")
            .ValueOrDie();
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*result))
        << result->ToString();
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkMergeAllDiskSplitsWithMemory) {
    options_[Options::SOURCE_SPLIT_OPEN_FILE_COST] = "1";
    options_[Options::SOURCE_SPLIT_TARGET_SIZE] = "1";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    const std::vector<std::vector<Row>> disk_batches = {
        {{1, "disk-1", "p0"}, {2, "disk-2", "p0"}},
        {{10, "disk-10", "p0"}, {11, "disk-11", "p0"}},
        {{20, "disk-20", "p0"}, {21, "disk-21", "p0"}},
    };
    for (int64_t commit_identifier = 0;
         commit_identifier < static_cast<int64_t>(disk_batches.size()); ++commit_identifier) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(disk_batches[commit_identifier], /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                             writer->PrepareCommitWithProgress(commit_identifier));
        ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(progress, commit_identifier));
        ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));
    }

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> memory_batch,
        MakeBatch({Row{1, "memory-1", "p0"}, Row{10, "deleted-10", "p0"}},
                  /*partitioned=*/false, /*bucket=*/0,
                  {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE}));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_EQ(1, plan->Splits().size());
    std::shared_ptr<RealtimeSplit> realtime_split =
        std::dynamic_pointer_cast<RealtimeSplit>(plan->Splits()[0]);
    ASSERT_NE(nullptr, realtime_split);
    ASSERT_EQ(3, realtime_split->DiskSplits().size());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(plan, realtime_context));
    ASSERT_EQ((std::vector<Row>{{1, "memory-1", "p0"},
                                {2, "disk-2", "p0"},
                                {11, "disk-11", "p0"},
                                {20, "disk-20", "p0"},
                                {21, "disk-21", "p0"}}),
              actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkNestedProjectionAcrossDiskAndMemory) {
    const std::shared_ptr<arrow::Field> projected_b = arrow::field("b", arrow::int64());
    fields_ = {
        arrow::field("id", arrow::int64()),
        arrow::field("payload", arrow::struct_({arrow::field("a", arrow::int64()), projected_b})),
        arrow::field("pt", arrow::utf8()),
    };
    schema_ = arrow::schema(fields_);
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    auto make_batch = [&](const std::string& json) -> Result<std::unique_ptr<RecordBatch>> {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), json));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        RecordBatchBuilder builder(&c_array);
        return builder.SetBucket(0).Finish();
    };

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         make_batch(R"([[1, [101, 1001], "p0"], [2, [102, 1002], "p0"]])"));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(disk_progress, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> sealed_batch,
                         make_batch(R"([[1, [201, 2001], "p0"], [3, [203, 2003], "p0"]])"));
    ASSERT_OK(writer->Write(std::move(sealed_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> sealed_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, sealed_progress.size());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> active_batch,
                         make_batch(R"([[1, [301, 3001], "p0"], [4, [304, null], "p0"]])"));
    ASSERT_OK(writer->Write(std::move(active_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));

    auto projected_schema = arrow::schema({
        arrow::field("payload", arrow::struct_({projected_b})),
        arrow::field("id", arrow::int64()),
    });
    auto c_schema = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, c_schema.get()).ok());
    ReadContextBuilder read_builder(table_path_);
    read_builder.SetOptions(options_)
        .SetReadSchema(std::move(c_schema))
        .WithRealtimeContext(realtime_context)
        .WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                         table_read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                         ReadResultCollector::CollectResult(std::move(reader)));
    const std::shared_ptr<arrow::DataType> result_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("payload", arrow::struct_({projected_b})),
        arrow::field("id", arrow::int64()),
    });
    const std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, [3001], 1],
            [0, [1002], 2],
            [0, [2003], 3],
            [0, [null], 4]
        ])")
            .ValueOrDie();
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*actual))
        << actual->ToString();
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkKeylessProjection) {
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch({Row{1, "disk", "p0"}}, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(disk_progress, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch({Row{1, "memory", "p0"}}, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ReadPlanWithSchemaAndCheck(plan, realtime_context,
                               arrow::schema({arrow::field("payload", arrow::utf8())}), R"([
        [0, "memory"]
    ])");
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestCompositePkKeylessProjection) {
    CreatePkTable(/*partition_keys=*/{}, /*primary_keys=*/{"id", "payload"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch({Row{1, "key", "disk"}}, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(disk_progress, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch({Row{1, "key", "memory"}}, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ReadPlanWithSchemaAndCheck(plan, realtime_context,
                               arrow::schema({arrow::field("pt", arrow::utf8())}), R"([
        [0, "memory"]
    ])");
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkCompositeMerge) {
    CreatePkTable(/*partition_keys=*/{}, /*primary_keys=*/{"id", "payload"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch({Row{1, "a", "disk-1a"}, Row{1, "b", "disk-1b"},
                                    Row{2, "a", "disk-2a"}, Row{3, "c", "disk-3c"}},
                                   /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, disk_progress.size());
    ASSERT_EQ(OffsetRange(0, 4), disk_progress[0].offset_range);
    ASSERT_EQ(1, NewFiles(disk_progress).size());
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(disk_progress, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> sealed_batch,
        MakeBatch({Row{1, "a", "sealed-1a"}, Row{1, "b", "deleted-1b"}, Row{2, "b", "sealed-2b"}},
                  /*partitioned=*/false, /*bucket=*/0,
                  {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::DELETE,
                   RecordBatch::RowKind::INSERT}));
    ASSERT_OK(writer->Write(std::move(sealed_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> sealed_progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, sealed_progress.size());
    ASSERT_EQ(OffsetRange(4, 7), sealed_progress[0].offset_range);
    ASSERT_EQ(1, NewFiles(sealed_progress).size());

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> active_batch,
        MakeBatch({Row{1, "a", "active-1a"}, Row{1, "c", "active-1c"}, Row{2, "a", "active-2a"}},
                  /*partitioned=*/false, /*bucket=*/0,
                  {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::INSERT,
                   RecordBatch::RowKind::UPDATE_AFTER}));
    ASSERT_OK(writer->Write(std::move(active_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_EQ(1, plan->Splits().size());
    std::shared_ptr<RealtimeSplit> split =
        std::dynamic_pointer_cast<RealtimeSplit>(plan->Splits()[0]);
    ASSERT_NE(nullptr, split);
    ASSERT_FALSE(split->DiskSplits().empty());
    ASSERT_EQ(4, split->CommittedEndOffset());
    ASSERT_EQ(10, split->MemoryEndOffset());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(plan, realtime_context));
    ASSERT_EQ((std::vector<Row>{{1, "a", "active-1a"},
                                {1, "c", "active-1c"},
                                {2, "a", "active-2a"},
                                {2, "b", "sealed-2b"},
                                {3, "c", "disk-3c"}}),
              actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkWriterHandoff) {
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer,
                         CreateRealtimeWriter(realtime_context));
    const std::vector<Row> first_rows = {
        {0, "value-0", "p0"}, {1, "value-1", "p0"}, {2, "value-2", "p0"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false));
    ASSERT_OK(first_writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_progress,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, first_progress.size());
    ASSERT_EQ(OffsetRange(0, 3), first_progress[0].offset_range);
    ASSERT_EQ(1, NewFiles(first_progress).size());
    ASSERT_EQ(0, NewFiles(first_progress)[0]->min_sequence_number);
    ASSERT_EQ(2, NewFiles(first_progress)[0]->max_sequence_number);
    ASSERT_OK(first_writer->Close());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer,
                         CreateRealtimeWriter(realtime_context));
    const std::vector<Row> second_rows = {{0, "updated-0", "p0"}, {3, "value-3", "p0"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(second_writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_progress,
                         second_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, second_progress.size());
    ASSERT_EQ(OffsetRange(3, 5), second_progress[0].offset_range);
    ASSERT_EQ(1, NewFiles(second_progress).size());
    ASSERT_EQ(3, NewFiles(second_progress)[0]->min_sequence_number);
    ASSERT_EQ(4, NewFiles(second_progress)[0]->max_sequence_number);

    first_progress.push_back(std::move(second_progress[0]));
    ASSERT_OK(Commit(first_progress, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ((std::vector<Row>{{0, "updated-0", "p0"},
                                {1, "value-1", "p0"},
                                {2, "value-2", "p0"},
                                {3, "value-3", "p0"}}),
              actual_rows);
    ASSERT_OK(second_writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkPartitionBucketRecovery) {
    options_[Options::BUCKET] = "2";
    CreatePkTable(/*partition_keys=*/{"pt"});
    const RealtimePartitionBucket p0b0({{"pt", "p0"}}, /*bucket=*/0);
    const RealtimePartitionBucket p1b1({{"pt", "p1"}}, /*bucket=*/1);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> first_context, RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer,
                         CreateRealtimeWriter(first_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p0_first_batch,
                         MakeBatch({Row{0, "p0-zero", "p0"}, Row{1, "p0-one", "p0"}},
                                   /*partitioned=*/true, /*bucket=*/0));
    ASSERT_OK(first_writer->Write(std::move(p0_first_batch)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> p1_first_batch,
        MakeBatch({Row{10, "p1-ten", "p1"}, Row{11, "p1-eleven", "p1"}, Row{12, "p1-twelve", "p1"}},
                  /*partitioned=*/true, /*bucket=*/1));
    ASSERT_OK(first_writer->Write(std::move(p1_first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_progress,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(2, first_progress.size());
    std::map<RealtimePartitionBucket, OffsetRange> first_ranges;
    std::map<RealtimePartitionBucket, std::pair<int64_t, int64_t>> first_sequences;
    for (const RealtimeCommitProgress& progress : first_progress) {
        first_ranges.emplace(progress.partition_bucket, progress.offset_range);
        std::shared_ptr<CommitMessageImpl> message =
            std::dynamic_pointer_cast<CommitMessageImpl>(progress.commit_message);
        ASSERT_NE(nullptr, message);
        const std::vector<std::shared_ptr<DataFileMeta>>& files =
            message->GetNewFilesIncrement().NewFiles();
        ASSERT_EQ(1, files.size());
        first_sequences.emplace(
            progress.partition_bucket,
            std::make_pair(files[0]->min_sequence_number, files[0]->max_sequence_number));
    }
    ASSERT_EQ(OffsetRange(0, 2), first_ranges.at(p0b0));
    ASSERT_EQ(OffsetRange(0, 3), first_ranges.at(p1b1));
    ASSERT_EQ((std::pair<int64_t, int64_t>(0, 1)), first_sequences.at(p0b0));
    ASSERT_EQ((std::pair<int64_t, int64_t>(0, 2)), first_sequences.at(p1b1));
    ASSERT_OK_AND_ASSIGN(int64_t first_snapshot_id,
                         Commit(first_progress, /*commit_identifier=*/0));
    ASSERT_OK(first_writer->RefreshCommittedSnapshot(first_snapshot_id));
    ASSERT_OK(first_writer->Close());
    first_writer.reset();
    first_context.reset();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> second_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer,
                         CreateRealtimeWriter(second_context));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> p0_second_batch,
        MakeBatch({Row{0, "p0-zero-new", "p0"}, Row{2, "p0-two", "p0"}},
                  /*partitioned=*/true, /*bucket=*/0,
                  {RecordBatch::RowKind::UPDATE_AFTER, RecordBatch::RowKind::INSERT}));
    ASSERT_OK(second_writer->Write(std::move(p0_second_batch)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p1_second_batch,
                         MakeBatch({Row{10, "p1-ten-deleted", "p1"}, Row{13, "p1-thirteen", "p1"}},
                                   /*partitioned=*/true, /*bucket=*/1,
                                   {RecordBatch::RowKind::DELETE, RecordBatch::RowKind::INSERT}));
    ASSERT_OK(second_writer->Write(std::move(p1_second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_progress,
                         second_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(2, second_progress.size());
    std::map<RealtimePartitionBucket, OffsetRange> second_ranges;
    std::map<RealtimePartitionBucket, std::pair<int64_t, int64_t>> second_sequences;
    for (const RealtimeCommitProgress& progress : second_progress) {
        second_ranges.emplace(progress.partition_bucket, progress.offset_range);
        std::shared_ptr<CommitMessageImpl> message =
            std::dynamic_pointer_cast<CommitMessageImpl>(progress.commit_message);
        ASSERT_NE(nullptr, message);
        const std::vector<std::shared_ptr<DataFileMeta>>& files =
            message->GetNewFilesIncrement().NewFiles();
        ASSERT_EQ(1, files.size());
        second_sequences.emplace(
            progress.partition_bucket,
            std::make_pair(files[0]->min_sequence_number, files[0]->max_sequence_number));
    }
    ASSERT_EQ(OffsetRange(2, 4), second_ranges.at(p0b0));
    ASSERT_EQ(OffsetRange(3, 5), second_ranges.at(p1b1));
    ASSERT_EQ((std::pair<int64_t, int64_t>(2, 3)), second_sequences.at(p0b0));
    ASSERT_EQ((std::pair<int64_t, int64_t>(3, 4)), second_sequences.at(p1b1));
    ASSERT_OK_AND_ASSIGN(int64_t second_snapshot_id,
                         Commit(second_progress, /*commit_identifier=*/1));
    ASSERT_OK(second_writer->RefreshCommittedSnapshot(second_snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(second_context));
    std::sort(actual_rows.begin(), actual_rows.end());
    ASSERT_EQ((std::vector<Row>{{0, "p0-zero-new", "p0"},
                                {1, "p0-one", "p0"},
                                {2, "p0-two", "p0"},
                                {11, "p1-eleven", "p1"},
                                {12, "p1-twelve", "p1"},
                                {13, "p1-thirteen", "p1"}}),
              actual_rows);
    ASSERT_OK(second_writer->Close());

    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap offsets, ReadCommittedOffsets());
    ASSERT_EQ(2, offsets.size());
    ASSERT_EQ(4, offsets.at(p0b0));
    ASSERT_EQ(5, offsets.at(p1b1));
}

TEST_F(RealtimeWriteInteTest, TestPkRecovery) {
    CreatePkTable();

    WriteContextBuilder seed_builder(table_path_, commit_user_);
    seed_builder.SetOptions(options_).WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> seed_context, seed_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> seed_writer,
                         FileStoreWrite::Create(std::move(seed_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> seed_batch,
                         MakeBatch({Row{99, "seed", "p0"}}, /*partitioned=*/false));
    ASSERT_OK(seed_writer->Write(std::move(seed_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> seed_messages,
                         seed_writer->PrepareCommit(/*wait_compaction=*/false,
                                                    /*commit_identifier=*/0));
    CommitContextBuilder seed_commit_builder(table_path_, commit_user_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> seed_commit_context,
                         seed_commit_builder.SetOptions(options_).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> seed_commit,
                         FileStoreCommit::Create(std::move(seed_commit_context)));
    ASSERT_OK(seed_commit->Commit(seed_messages, /*commit_identifier=*/0));
    ASSERT_OK(seed_writer->Close());
    const std::vector<Row> mutations = {
        {1, "one", "p0"}, {1, "one-new", "p0"}, {2, "deleted", "p0"}, {3, "three", "p0"}};
    const std::vector<RecordBatch::RowKind> mutation_kinds = {
        RecordBatch::RowKind::INSERT, RecordBatch::RowKind::UPDATE_AFTER,
        RecordBatch::RowKind::DELETE, RecordBatch::RowKind::INSERT};

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> first_context, RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer,
                         CreateRealtimeWriter(first_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(mutations, /*partitioned=*/false, /*bucket=*/0, mutation_kinds));
    ASSERT_OK(first_writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> memory_sequences, ReadPkSequences(first_context));
    ASSERT_EQ((std::vector<int64_t>{1, 2, 3, 4}), memory_sequences);
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, progress.size());
    ASSERT_EQ(OffsetRange(0, 4), progress[0].offset_range);
    ASSERT_EQ(1, NewFiles(progress).size());
    ASSERT_EQ(2, NewFiles(progress)[0]->min_sequence_number);
    ASSERT_EQ(memory_sequences.back(), NewFiles(progress)[0]->max_sequence_number);
    ASSERT_EQ(1, NewFiles(progress)[0]->delete_row_count);
    ASSERT_OK(Commit(progress, /*commit_identifier=*/1));
    ASSERT_OK(first_writer->Close());
    first_context.reset();
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows_after_replay, ReadRows());
    ASSERT_EQ((std::vector<Row>{{1, "one-new", "p0"}, {3, "three", "p0"}, {99, "seed", "p0"}}),
              rows_after_replay);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> second_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer,
                         CreateRealtimeWriter(second_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> restart_batch,
                         MakeBatch({Row{4, "four", "p0"}}, /*partitioned=*/false));
    ASSERT_OK(second_writer->Write(std::move(restart_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> restart_sequences, ReadPkSequences(second_context));
    ASSERT_EQ((std::vector<int64_t>{5}), restart_sequences);
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> restart_progress,
                         second_writer->PrepareCommitWithProgress(/*commit_identifier=*/2));
    ASSERT_EQ(1, restart_progress.size());
    ASSERT_EQ(OffsetRange(4, 5), restart_progress[0].offset_range);
    ASSERT_EQ(5, NewFiles(restart_progress)[0]->min_sequence_number);
    ASSERT_EQ(5, NewFiles(restart_progress)[0]->max_sequence_number);
    ASSERT_OK(second_writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPkCompaction) {
    options_[Options::NUM_SORTED_RUNS_COMPACTION_TRIGGER] = "1";
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    int64_t latest_snapshot_id = -1;
    constexpr int64_t kCommitRoundsBeforeCompaction = 4;
    std::set<std::string> committed_file_names;
    for (int64_t round = 0; round < kCommitRoundsBeforeCompaction; ++round) {
        const bool delete_latest_live_row = round == kCommitRoundsBeforeCompaction - 1;
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<RecordBatch> batch,
            MakeBatch(
                {Row{delete_latest_live_row ? round - 1 : round,
                     delete_latest_live_row ? "deleted" : "value-" + std::to_string(round), "p0"}},
                /*partitioned=*/false, /*bucket=*/0,
                delete_latest_live_row
                    ? std::vector<RecordBatch::RowKind>{RecordBatch::RowKind::DELETE}
                    : std::vector<RecordBatch::RowKind>{}));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                             writer->PrepareCommitWithProgress(round));
        ASSERT_EQ(1, progress.size());
        std::shared_ptr<CommitMessageImpl> message =
            std::dynamic_pointer_cast<CommitMessageImpl>(progress[0].commit_message);
        ASSERT_NE(nullptr, message);
        ASSERT_TRUE(message->GetCompactIncrement().IsEmpty());
        ASSERT_EQ(1, NewFiles(progress).size());
        committed_file_names.insert(NewFiles(progress)[0]->file_name);
        ASSERT_OK_AND_ASSIGN(latest_snapshot_id, Commit(progress, round));
        ASSERT_OK(writer->RefreshCommittedSnapshot(latest_snapshot_id));
        ASSERT_OK_AND_ASSIGN(uint64_t memory_usage, GetRealtimeMemoryUsage(realtime_context));
        ASSERT_EQ(0, memory_usage);
    }
    WriteContextBuilder compact_builder(table_path_, commit_user_);
    compact_builder.SetOptions(options_).WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> compact_context, compact_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> compact_writer,
                         FileStoreWrite::Create(std::move(compact_context)));
    ASSERT_OK(compact_writer->Compact(/*partition=*/{}, /*bucket=*/0,
                                      /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> compact_messages,
        compact_writer->PrepareCommit(/*wait_compaction=*/true, /*commit_identifier=*/4));
    ASSERT_EQ(1, compact_messages.size());
    std::shared_ptr<CommitMessageImpl> compact_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(compact_messages[0]);
    ASSERT_NE(nullptr, compact_message);
    ASSERT_TRUE(compact_message->GetNewFilesIncrement().IsEmpty());
    ASSERT_EQ(kCommitRoundsBeforeCompaction,
              compact_message->GetCompactIncrement().CompactBefore().size());
    std::set<std::string> compacted_file_names;
    for (const std::shared_ptr<DataFileMeta>& file :
         compact_message->GetCompactIncrement().CompactBefore()) {
        compacted_file_names.insert(file->file_name);
    }
    ASSERT_EQ(committed_file_names, compacted_file_names);
    ASSERT_FALSE(compact_message->GetCompactIncrement().CompactAfter().empty());
    constexpr int64_t kHistoricalMaxSequenceNumber = kCommitRoundsBeforeCompaction - 1;
    int64_t compacted_live_max_sequence_number = -1;
    for (const std::shared_ptr<DataFileMeta>& file :
         compact_message->GetCompactIncrement().CompactAfter()) {
        compacted_live_max_sequence_number =
            std::max(compacted_live_max_sequence_number, file->max_sequence_number);
    }
    ASSERT_LT(compacted_live_max_sequence_number, kHistoricalMaxSequenceNumber);
    CommitContextBuilder commit_builder(table_path_, commit_user_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_builder.SetOptions(options_).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(compact_messages, /*commit_identifier=*/4));
    ASSERT_OK(compact_writer->Close());

    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(options_));
    SnapshotManager snapshot_manager(options.GetFileSystem(), table_path_);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> compact_snapshot,
                         snapshot_manager.LatestSnapshot());
    ASSERT_TRUE(compact_snapshot);
    ASSERT_EQ(Snapshot::CommitKind::Compact(), compact_snapshot->GetCommitKind());
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap offsets, ReadCommittedOffsets());
    ASSERT_EQ(4, offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> compacted_rows, ReadRows());
    ASSERT_EQ((std::vector<Row>{{0, "value-0", "p0"}, {1, "value-1", "p0"}}), compacted_rows);
    ASSERT_OK(writer->Close());
    writer.reset();
    realtime_context.reset();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> fresh_context, RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> fresh_writer,
                         CreateRealtimeWriter(fresh_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> fresh_batch,
                         MakeBatch({Row{4, "value-4", "p0"}},
                                   /*partitioned=*/false));
    ASSERT_OK(fresh_writer->Write(std::move(fresh_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<int64_t> fresh_sequences, ReadPkSequences(fresh_context));
    ASSERT_EQ((std::vector<int64_t>{compacted_live_max_sequence_number + 1}), fresh_sequences);
    ASSERT_LT(fresh_sequences.front(), kHistoricalMaxSequenceNumber);
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> fresh_progress,
                         fresh_writer->PrepareCommitWithProgress(/*commit_identifier=*/5));
    ASSERT_EQ(1, fresh_progress.size());
    ASSERT_EQ(OffsetRange(4, 5), fresh_progress[0].offset_range);
    ASSERT_EQ(compacted_live_max_sequence_number + 1,
              NewFiles(fresh_progress)[0]->min_sequence_number);
    ASSERT_EQ(compacted_live_max_sequence_number + 1,
              NewFiles(fresh_progress)[0]->max_sequence_number);
    ASSERT_OK_AND_ASSIGN(latest_snapshot_id, Commit(fresh_progress, /*commit_identifier=*/5));
    ASSERT_OK(fresh_writer->Close());

    ASSERT_OK_AND_ASSIGN(offsets, ReadCommittedOffsets());
    ASSERT_EQ(5, offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> final_rows, ReadRows());
    ASSERT_EQ((std::vector<Row>{{0, "value-0", "p0"}, {1, "value-1", "p0"}, {4, "value-4", "p0"}}),
              final_rows);
}

TEST_F(RealtimeWriteInteTest, TestPkMultipleStoredBatchesMergeForQueryAndCommit) {
    CreatePkTable();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch({Row{4, "four", "p0"}, Row{2, "two", "p0"}, Row{1, "one", "p0"}},
                                   /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> second_batch,
        MakeBatch({Row{3, "three", "p0"}, Row{2, "deleted", "p0"}, Row{1, "one-new", "p0"}},
                  /*partitioned=*/false, /*bucket=*/0,
                  {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::DELETE,
                   RecordBatch::RowKind::UPDATE_AFTER}));
    ASSERT_OK(writer->Write(std::move(second_batch)));

    const std::vector<Row> expected = {{1, "one-new", "p0"}, {3, "three", "p0"}, {4, "four", "p0"}};
    ASSERT_OK_AND_ASSIGN(std::vector<Row> query_rows, ReadRows(realtime_context));
    ASSERT_EQ(expected, query_rows);

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progress,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, progress.size());
    ASSERT_EQ(OffsetRange(0, 6), progress[0].offset_range);
    ASSERT_OK(Commit(progress, /*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows, ReadRows());
    ASSERT_EQ(expected, rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestRollingFilesPreserveProgress) {
    options_[Options::TARGET_FILE_ROW_NUM] = "10";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());

    std::vector<Row> expected_rows;
    constexpr int64_t kBatchCount = 3;
    constexpr int64_t kRowsPerBatch = 10;
    for (int64_t batch_index = 0; batch_index < kBatchCount; ++batch_index) {
        std::vector<Row> rows =
            MakeRows(batch_index * kRowsPerBatch, kRowsPerBatch, /*partition=*/"p0");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
        expected_rows.insert(expected_rows.end(), rows.begin(), rows.end());
    }

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, commits.size());
    ASSERT_EQ(OffsetRange(0, kBatchCount * kRowsPerBatch), commits[0].offset_range);
    std::shared_ptr<CommitMessageImpl> commit_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(commits[0].commit_message);
    ASSERT_NE(nullptr, commit_message);
    ASSERT_EQ(3, commit_message->GetNewFilesIncrement().NewFiles().size());
    ASSERT_OK(Commit(commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
    ASSERT_EQ(expected_rows, actual_rows);
}

TEST_F(RealtimeWriteInteTest, TestAppendScanKeepsDiskSplitsIndependent) {
    options_[Options::TARGET_FILE_ROW_NUM] = "2";
    options_[Options::SOURCE_SPLIT_OPEN_FILE_COST] = "1";
    options_[Options::SOURCE_SPLIT_TARGET_SIZE] = "1";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> expected_rows;
    for (int64_t first_id = 0; first_id < 6; first_id += 2) {
        std::vector<Row> rows = MakeRows(first_id, /*count=*/2, /*partition=*/"p0");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
        expected_rows.insert(expected_rows.end(), rows.begin(), rows.end());
    }
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id, Commit(commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(snapshot_id));

    std::vector<Row> memory_rows = MakeRows(/*first_id=*/6, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));
    expected_rows.insert(expected_rows.end(), memory_rows.begin(), memory_rows.end());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_EQ(3, plan->Splits().size());
    ASSERT_EQ(nullptr, std::dynamic_pointer_cast<RealtimeSplit>(plan->Splits()[0]));
    ASSERT_EQ(nullptr, std::dynamic_pointer_cast<RealtimeSplit>(plan->Splits()[1]));
    std::shared_ptr<RealtimeSplit> realtime_split =
        std::dynamic_pointer_cast<RealtimeSplit>(plan->Splits()[2]);
    ASSERT_NE(nullptr, realtime_split);
    ASSERT_EQ(1, realtime_split->DiskSplits().size());
    ASSERT_EQ(6, realtime_split->CommittedEndOffset());
    ASSERT_EQ(8, realtime_split->MemoryEndOffset());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(plan, realtime_context));
    ASSERT_EQ(expected_rows, actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestCommitOrdersPreparedOffsetRanges) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());

    std::vector<Row> first_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, commits.size());
    ASSERT_EQ(OffsetRange(0, 3), commits[0].offset_range);

    std::vector<Row> second_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, second_commits.size());
    ASSERT_EQ(OffsetRange(3, 5), second_commits[0].offset_range);

    commits.push_back(std::move(second_commits[0]));
    std::reverse(commits.begin(), commits.end());
    ASSERT_OK(Commit(commits, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(5, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK(writer->Close());

    std::vector<Row> expected_rows = first_rows;
    expected_rows.insert(expected_rows.end(), second_rows.begin(), second_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
    ASSERT_EQ(expected_rows, actual_rows);
}

TEST_F(RealtimeWriteInteTest, TestCommitWithProgressRetryReturnsLatestSnapshot) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());
    std::vector<Row> expected_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(expected_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));

    ASSERT_OK_AND_ASSIGN(int64_t first_snapshot_id, Commit(commits, /*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t retry_snapshot_id, Commit(commits, /*commit_identifier=*/0));
    ASSERT_EQ(first_snapshot_id, retry_snapshot_id);

    ASSERT_OK(writer->RefreshCommittedSnapshot(first_snapshot_id));
    std::vector<Row> second_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int64_t second_snapshot_id,
                         Commit(second_commits, /*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(retry_snapshot_id, Commit(commits, /*commit_identifier=*/0));
    ASSERT_EQ(second_snapshot_id, retry_snapshot_id);
    ASSERT_NE(first_snapshot_id, retry_snapshot_id);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(5, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));

    expected_rows.insert(expected_rows.end(), second_rows.begin(), second_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
    ASSERT_EQ(expected_rows, actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestCommitWithProgressRejectsCoveredRangesFromAnotherUser) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());
    std::vector<Row> expected_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(expected_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(commits, /*commit_identifier=*/0));

    CommitContextBuilder builder(table_path_, "another_realtime_commit_user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> context,
                         builder.SetOptions(options_).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(context)));
    ASSERT_NOK_WITH_MSG(commit->CommitWithProgress(commits, /*commit_identifier=*/0,
                                                   /*watermark=*/std::nullopt),
                        "another commit user or identifier");

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
    ASSERT_EQ(expected_rows, actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestRealtimeWriteAcrossAppendCompaction) {
    options_[Options::TARGET_FILE_ROW_NUM] = "2";
    options_[Options::COMPACTION_MIN_FILE_NUM] = "2";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> first_rows;
    for (int64_t first_id = 0; first_id < 5; first_id += 2) {
        std::vector<Row> rows =
            MakeRows(first_id, std::min<int64_t>(2, 5 - first_id), /*partition=*/"p0");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
        first_rows.insert(first_rows.end(), rows.begin(), rows.end());
    }
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, first_commits.size());
    ASSERT_EQ(OffsetRange(0, 5), first_commits[0].offset_range);
    std::shared_ptr<CommitMessageImpl> first_commit_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(first_commits[0].commit_message);
    ASSERT_NE(nullptr, first_commit_message);
    ASSERT_EQ(3, first_commit_message->GetNewFilesIncrement().NewFiles().size());
    ASSERT_OK_AND_ASSIGN(int64_t first_snapshot_id, Commit(first_commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(first_snapshot_id));

    ASSERT_OK_AND_ASSIGN(Snapshot compact_snapshot, CompactAndCommit(/*partition=*/{}, /*bucket=*/0,
                                                                     /*commit_identifier=*/1));
    ASSERT_EQ(Snapshot::CommitKind::Compact(), compact_snapshot.GetCommitKind());
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap compacted_offsets, ReadCommittedOffsets());
    ASSERT_EQ(5, compacted_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));

    ASSERT_OK(writer->RefreshCommittedSnapshot(compact_snapshot.Id()));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows_after_compaction, ReadRows(realtime_context));
    ASSERT_EQ(first_rows, rows_after_compaction);

    std::vector<Row> second_rows = MakeRows(/*first_id=*/5, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(second_batch)));
    std::vector<Row> expected_rows = first_rows;
    expected_rows.insert(expected_rows.end(), second_rows.begin(), second_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows_with_building_memory, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, rows_with_building_memory);

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/2));
    ASSERT_EQ(1, second_commits.size());
    ASSERT_EQ(OffsetRange(5, 7), second_commits[0].offset_range);
    ASSERT_OK_AND_ASSIGN(int64_t final_snapshot_id,
                         Commit(second_commits, /*commit_identifier=*/2));
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap final_offsets, ReadCommittedOffsets());
    ASSERT_EQ(7, final_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> final_rows_before_refresh, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, final_rows_before_refresh);
    ASSERT_OK(writer->RefreshCommittedSnapshot(final_snapshot_id));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> final_rows_after_refresh, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, final_rows_after_refresh);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestRealtimeOffsetFileLifecycle) {
    options_[Options::SNAPSHOT_NUM_RETAINED_MIN] = "1";
    options_[Options::SNAPSHOT_NUM_RETAINED_MAX] = "1";
    options_[Options::SNAPSHOT_TIME_RETAINED] = "1ms";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());

    std::vector<Row> first_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t first_snapshot_id, Commit(first_commits, /*commit_identifier=*/0));

    std::vector<Row> second_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int64_t second_snapshot_id,
                         Commit(second_commits, /*commit_identifier=*/1));

    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options_));
    std::shared_ptr<FileSystem> file_system = core_options.GetFileSystem();
    SnapshotManager snapshot_manager(file_system, table_path_);
    ASSERT_OK_AND_ASSIGN(Snapshot first_snapshot, snapshot_manager.LoadSnapshot(first_snapshot_id));
    ASSERT_OK_AND_ASSIGN(Snapshot second_snapshot,
                         snapshot_manager.LoadSnapshot(second_snapshot_id));
    std::optional<std::string> first_offsets_path =
        RealtimeCommitProperties::GetOffsetsPath(first_snapshot);
    std::optional<std::string> second_offsets_path =
        RealtimeCommitProperties::GetOffsetsPath(second_snapshot);
    ASSERT_TRUE(first_offsets_path);
    ASSERT_TRUE(second_offsets_path);
    ASSERT_NE(first_offsets_path, second_offsets_path);

    std::string orphan_offsets_path = PathUtil::JoinPath(
        RealtimeCommitProperties::OffsetsDirectory(table_path_, core_options.GetBranch()),
        "orphan.offsets");
    ASSERT_OK(file_system->WriteFile(orphan_offsets_path, "orphan", /*overwrite=*/false));
    CleanContextBuilder clean_builder(table_path_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_builder.WithFileSystem(file_system)
                             .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                             .Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OrphanFilesCleaner> cleaner,
                         OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
    ASSERT_EQ(std::set<std::string>({orphan_offsets_path}), cleaned_paths);
    ASSERT_OK_AND_ASSIGN(bool first_offsets_exist, file_system->Exists(first_offsets_path.value()));
    ASSERT_TRUE(first_offsets_exist);
    ASSERT_OK_AND_ASSIGN(bool second_offsets_exist,
                         file_system->Exists(second_offsets_path.value()));
    ASSERT_TRUE(second_offsets_exist);

    ASSERT_OK_AND_ASSIGN(int32_t expired_snapshots, ExpireSnapshots());
    ASSERT_EQ(1, expired_snapshots);
    ASSERT_OK_AND_ASSIGN(first_offsets_exist, file_system->Exists(first_offsets_path.value()));
    ASSERT_FALSE(first_offsets_exist);
    ASSERT_OK_AND_ASSIGN(second_offsets_exist, file_system->Exists(second_offsets_path.value()));
    ASSERT_TRUE(second_offsets_exist);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets,
                         RealtimeCommitProperties::ReadOffsets(second_snapshot, file_system));
    ASSERT_EQ(5, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestCompactionSnapshotRetainsSharedOffsetFile) {
    options_[Options::TARGET_FILE_ROW_NUM] = "2";
    options_[Options::COMPACTION_MIN_FILE_NUM] = "2";
    options_[Options::SNAPSHOT_NUM_RETAINED_MIN] = "1";
    options_[Options::SNAPSHOT_NUM_RETAINED_MAX] = "1";
    options_[Options::SNAPSHOT_TIME_RETAINED] = "1ms";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());

    for (int64_t first_id = 0; first_id < 5; first_id += 2) {
        std::vector<Row> rows =
            MakeRows(first_id, std::min<int64_t>(2, 5 - first_id), /*partition=*/"p0");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t realtime_snapshot_id, Commit(commits, /*commit_identifier=*/0));

    ASSERT_OK_AND_ASSIGN(Snapshot compact_snapshot, CompactAndCommit(/*partition=*/{}, /*bucket=*/0,
                                                                     /*commit_identifier=*/1));
    ASSERT_EQ(Snapshot::CommitKind::Compact(), compact_snapshot.GetCommitKind());

    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options_));
    std::shared_ptr<FileSystem> file_system = core_options.GetFileSystem();
    SnapshotManager snapshot_manager(file_system, table_path_);
    ASSERT_OK_AND_ASSIGN(Snapshot realtime_snapshot,
                         snapshot_manager.LoadSnapshot(realtime_snapshot_id));
    std::optional<std::string> realtime_offsets_path =
        RealtimeCommitProperties::GetOffsetsPath(realtime_snapshot);
    std::optional<std::string> compact_offsets_path =
        RealtimeCommitProperties::GetOffsetsPath(compact_snapshot);
    ASSERT_TRUE(realtime_offsets_path);
    ASSERT_TRUE(compact_offsets_path);
    ASSERT_EQ(realtime_offsets_path, compact_offsets_path);

    ASSERT_OK_AND_ASSIGN(int32_t expired_snapshots, ExpireSnapshots());
    ASSERT_EQ(1, expired_snapshots);
    ASSERT_OK_AND_ASSIGN(bool offsets_exist, file_system->Exists(compact_offsets_path.value()));
    ASSERT_TRUE(offsets_exist);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets,
                         RealtimeCommitProperties::ReadOffsets(compact_snapshot, file_system));
    ASSERT_EQ(5, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestReadMemoryBeforePrepareCommit) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/10, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_EQ(1, plan->Splits().size());
    std::shared_ptr<RealtimeSplit> realtime_split =
        std::dynamic_pointer_cast<RealtimeSplit>(plan->Splits()[0]);
    ASSERT_NE(nullptr, realtime_split);
    ASSERT_EQ(RealtimeSplit::kCurrentVersion, realtime_split->Version());
    ASSERT_FALSE(realtime_split->SnapshotId().has_value());
    ASSERT_EQ(0, realtime_split->CommittedEndOffset());
    ASSERT_EQ(10, realtime_split->MemoryEndOffset());
    ASSERT_FALSE(realtime_split->OpaqueTicket().empty());
    ASSERT_NOK_WITH_MSG(ReadRows(plan, /*realtime_context=*/nullptr),
                        "requires a real-time context");
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(plan, realtime_context));
    ASSERT_EQ(rows, actual_rows);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                         RealtimeContextImpl::Cast(realtime_context));
    ASSERT_NOK_WITH_MSG(realtime_context_impl->ResolveReadView(realtime_split->OpaqueTicket()),
                        "ticket does not exist or has expired");
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPlanExcludesRowsWrittenAfterMemoryEndOffset) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> first_rows = MakeRows(/*first_id=*/0, /*count=*/10, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> first_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_EQ(1, first_plan->Splits().size());
    std::shared_ptr<RealtimeSplit> first_realtime_split =
        std::dynamic_pointer_cast<RealtimeSplit>(first_plan->Splits()[0]);
    ASSERT_NE(nullptr, first_realtime_split);
    ASSERT_EQ(10, first_realtime_split->MemoryEndOffset());

    std::vector<Row> second_rows = MakeRows(/*first_id=*/10, /*count=*/5, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(second_batch)));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> first_actual_rows,
                         ReadRows(first_plan, realtime_context));
    ASSERT_EQ(first_rows, first_actual_rows);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> second_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_EQ(1, second_plan->Splits().size());
    std::shared_ptr<RealtimeSplit> second_realtime_split =
        std::dynamic_pointer_cast<RealtimeSplit>(second_plan->Splits()[0]);
    ASSERT_NE(nullptr, second_realtime_split);
    ASSERT_EQ(15, second_realtime_split->MemoryEndOffset());

    std::vector<Row> expected_rows = first_rows;
    expected_rows.insert(expected_rows.end(), second_rows.begin(), second_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> second_actual_rows,
                         ReadRows(second_plan, realtime_context));
    ASSERT_EQ(expected_rows, second_actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestReadFailsAfterRealtimeSplitTicketExpires) {
    options_[Options::REALTIME_READ_VIEW_TTL] = "10 ms";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_NOK_WITH_MSG(ReadRows(plan, realtime_context), "ticket does not exist or has expired");
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestSuccessfulReaderCreationConsumesRealtimeSplitTicket) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));

    ReadContextBuilder read_builder(table_path_);
    read_builder.SetOptions(options_)
        .SetReadFieldNames({"id", "payload", "pt"})
        .WithRealtimeContext(realtime_context)
        .WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> first_reader,
                         table_read->CreateReader(plan->Splits()));
    first_reader->Close();
    first_reader.reset();

    ASSERT_NOK_WITH_MSG(table_read->CreateReader(plan->Splits()),
                        "ticket does not exist or has expired");
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestFailedReaderCreationPreservesRealtimeSplitTicket) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_EQ(1, plan->Splits().size());

    ReadContextBuilder read_builder(table_path_);
    read_builder.SetOptions(options_)
        .SetReadFieldNames({"id", "payload", "pt"})
        .SetPredicate(std::make_shared<UnsupportedPredicate>())
        .EnablePredicateFilter(true)
        .WithRealtimeContext(realtime_context)
        .WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_NOK_WITH_MSG(table_read->CreateReader(plan->Splits()),
                        "cannot cast predicate unsupported");

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(plan, realtime_context));
    ASSERT_EQ(rows, actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestCloseWriterKeepsContextReadable) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ(rows, actual_rows);
}

TEST_F(RealtimeWriteInteTest, TestPinnedPlanRemainsReadableAfterWriterClose) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(plan, realtime_context));
    ASSERT_EQ(rows, actual_rows);
}

TEST_F(RealtimeWriteInteTest, TestCloseWriterAllowsContextReuseByLaterWriter) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> first_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false));
    ASSERT_OK(first_writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, commits.size());
    ASSERT_EQ(OffsetRange(0, 3), commits[0].offset_range);
    ASSERT_OK(first_writer->Close());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> second_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(second_writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         second_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, second_commits.size());
    ASSERT_EQ(OffsetRange(3, 5), second_commits[0].offset_range);

    commits.push_back(std::move(second_commits[0]));
    ASSERT_OK(Commit(commits, /*commit_identifier=*/1));
    std::vector<Row> expected_rows = first_rows;
    expected_rows.insert(expected_rows.end(), second_rows.begin(), second_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, actual_rows);
    ASSERT_OK(second_writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestReadCommittedDiskAndBuildingMemory) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, disk_commits.size());
    ASSERT_EQ(OffsetRange(0, 3), disk_commits[0].offset_range);

    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));

    std::vector<Row> expected_rows = disk_rows;
    expected_rows.insert(expected_rows.end(), memory_rows.begin(), memory_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestCountMemoryAndDiskAcrossRefresh) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> memory_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_OK_AND_ASSIGN(int64_t memory_count, CountRows(memory_plan, realtime_context));
    ASSERT_EQ(3, memory_count);

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot_id,
                         Commit(disk_commits, /*commit_identifier=*/0));
    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> union_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_OK_AND_ASSIGN(int64_t union_count, CountRows(union_plan, realtime_context));
    ASSERT_EQ(5, union_count);

    ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot_id));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> refreshed_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_OK_AND_ASSIGN(int64_t refreshed_count, CountRows(refreshed_plan, realtime_context));
    ASSERT_EQ(union_count, refreshed_count);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestProjectionAndPredicateForMemoryAndDisk) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::shared_ptr<Predicate> scan_predicate =
        PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT,
                                      Literal(static_cast<int64_t>(1)));
    std::shared_ptr<Predicate> read_predicate =
        PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"id", FieldType::BIGINT,
                                      Literal(static_cast<int64_t>(1)));
    const std::vector<std::string> read_fields = {"payload", "id"};
    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("payload", arrow::utf8()),
         arrow::field("id", arrow::int64())});

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> memory_plan,
                         CreatePlan(realtime_context, scan_predicate));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> memory_result,
                         ReadPlan(memory_plan, realtime_context, read_fields, read_predicate,
                                  /*enable_predicate_filter=*/true));
    std::shared_ptr<arrow::Array> expected_memory =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, "value-2", 2]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, memory_result);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected_memory)->Equals(*memory_result));

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));
    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> union_plan,
                         CreatePlan(realtime_context, scan_predicate));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> union_result,
                         ReadPlan(union_plan, realtime_context, read_fields, read_predicate,
                                  /*enable_predicate_filter=*/true));
    std::shared_ptr<arrow::Array> expected_union =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, "value-2", 2],
            [0, "value-3", 3],
            [0, "value-4", 4],
            [0, "value-5", 5]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, union_result);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected_union)->Equals(*union_result));
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestDiskPredicatePushdownWithoutMemoryFiltering) {
    options_[Options::FILE_FORMAT] = "parquet";
    options_[Options::WRITE_BATCH_SIZE] = "1";
    options_["parquet.page.size"] = "1";
    options_["parquet.enable-dictionary"] = "false";
    options_["parquet.write.enable-page-index"] = "true";
    options_["parquet.read.enable-page-index-filter"] = "true";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::shared_ptr<Predicate> predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT,
                                Literal(static_cast<int64_t>(1)));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));

    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, CreatePlan(realtime_context, predicate));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadPlan(plan, realtime_context, {"id", "payload", "pt"}, predicate,
                                  /*enable_predicate_filter=*/false));
    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("id", arrow::int64()),
         arrow::field("payload", arrow::utf8()), arrow::field("pt", arrow::utf8())});
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, 1, "value-1", "p0"],
            [0, 3, "value-3", "p0"],
            [0, 4, "value-4", "p0"],
            [0, 5, "value-5", "p0"]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, result);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*result))
        << result->ToString();
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestMemoryBatchStatisticsPredicatePushdown) {
    CreateTable(/*partition_keys=*/{});
    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("id", arrow::int64()),
         arrow::field("payload", arrow::utf8()), arrow::field("pt", arrow::utf8())});
    auto make_expected = [&](const std::string& json) {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(result_type, json).ValueOrDie();
        return std::make_shared<arrow::ChunkedArray>(array);
    };
    std::vector<std::shared_ptr<Predicate>> predicates = {
        PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT,
                                      Literal(static_cast<int64_t>(100))),
        PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT,
                                      Literal(static_cast<int64_t>(5))),
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT,
                                Literal(static_cast<int64_t>(10))),
    };

    auto check_candidates =
        [&](const std::string& statistics_mode,
            const std::vector<std::shared_ptr<arrow::ChunkedArray>>& expected) -> Status {
        if (expected.size() != predicates.size()) {
            return Status::Invalid("unexpected real-time candidate result count");
        }
        options_[Options::REALTIME_STORE_STATS_MODE] = statistics_mode;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContext> realtime_context,
                               RealtimeContext::Create());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> writer,
                               CreateRealtimeWriter(realtime_context));
        for (int64_t first_id : {0, 10, 20}) {
            std::vector<Row> rows = MakeRows(first_id, /*count=*/3, /*partition=*/"p0");
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> batch,
                                   MakeBatch(rows, /*partitioned=*/false));
            PAIMON_RETURN_NOT_OK(writer->Write(std::move(batch)));
        }

        for (size_t i = 0; i < predicates.size(); ++i) {
            const std::shared_ptr<Predicate>& predicate = predicates[i];
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan,
                                   CreatePlan(realtime_context, predicate));
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<arrow::ChunkedArray> result,
                ReadPlan(plan, realtime_context, {"id", "payload", "pt"}, predicate,
                         /*enable_predicate_filter=*/false));
            std::shared_ptr<arrow::ChunkedArray> actual = result ? result : make_expected("[]");
            if (!expected[i]->Equals(*actual)) {
                return Status::Invalid("unexpected real-time candidate rows: " +
                                       actual->ToString());
            }
        }
        PAIMON_RETURN_NOT_OK(writer->Close());
        return Status::OK();
    };

    std::shared_ptr<arrow::ChunkedArray> all_rows = make_expected(R"([
        [0, 0, "value-0", "p0"],
        [0, 1, "value-1", "p0"],
        [0, 2, "value-2", "p0"],
        [0, 10, "value-10", "p0"],
        [0, 11, "value-11", "p0"],
        [0, 12, "value-12", "p0"],
        [0, 20, "value-20", "p0"],
        [0, 21, "value-21", "p0"],
        [0, 22, "value-22", "p0"]
    ])");
    ASSERT_OK(check_candidates("none", {all_rows, all_rows, all_rows}));

    std::shared_ptr<arrow::ChunkedArray> partially_filtered = make_expected(R"([
        [0, 10, "value-10", "p0"],
        [0, 11, "value-11", "p0"],
        [0, 12, "value-12", "p0"],
        [0, 20, "value-20", "p0"],
        [0, 21, "value-21", "p0"],
        [0, 22, "value-22", "p0"]
    ])");
    std::shared_ptr<arrow::ChunkedArray> matching_batch = make_expected(R"([
        [0, 10, "value-10", "p0"],
        [0, 11, "value-11", "p0"],
        [0, 12, "value-12", "p0"]
    ])");
    ASSERT_OK(check_candidates("full", {make_expected("[]"), partially_filtered, matching_batch}));
}

TEST_F(RealtimeWriteInteTest, TestMemoryBatchStatisticsPredicatePushdownWithDisk) {
    options_[Options::FILE_FORMAT] = "parquet";
    options_[Options::WRITE_BATCH_SIZE] = "1";
    options_[Options::REALTIME_STORE_STATS_MODE] = "full";
    options_["parquet.page.size"] = "1";
    options_["parquet.enable-dictionary"] = "false";
    options_["parquet.write.enable-page-index"] = "true";
    options_["parquet.read.enable-page-index-filter"] = "true";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/6, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));

    for (int64_t first_id : {0, 10}) {
        std::vector<Row> rows = MakeRows(first_id, /*count=*/3, /*partition=*/"p0");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
    }
    std::shared_ptr<Predicate> predicate =
        PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT,
                                      Literal(static_cast<int64_t>(3)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, CreatePlan(realtime_context, predicate));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadPlan(plan, realtime_context, {"id", "payload", "pt"}, predicate,
                                  /*enable_predicate_filter=*/false));

    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("id", arrow::int64()),
         arrow::field("payload", arrow::utf8()), arrow::field("pt", arrow::utf8())});
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, 4, "value-4", "p0"],
            [0, 5, "value-5", "p0"],
            [0, 10, "value-10", "p0"],
            [0, 11, "value-11", "p0"],
            [0, 12, "value-12", "p0"]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, result);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*result))
        << result->ToString();
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestNullPredicateForMemoryAndDisk) {
    options_[Options::FILE_FORMAT] = "parquet";
    options_[Options::WRITE_BATCH_SIZE] = "1";
    options_[Options::REALTIME_STORE_STATS_MODE] = "full";
    options_["parquet.page.size"] = "1";
    options_["parquet.enable-dictionary"] = "false";
    options_["parquet.write.enable-page-index"] = "true";
    options_["parquet.write.max-row-group-length"] = "1";
    options_["parquet.read.enable-page-index-filter"] = "true";
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeUnpartitionedBatchFromJson(R"([
                             [0, null, "p0"],
                             [1, "disk-value", "p0"]
                         ])"));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> non_null_memory_batch,
                         MakeUnpartitionedBatchFromJson(R"([
                             [2, "memory-value-2", "p0"],
                             [3, "memory-value-3", "p0"]
                         ])"));
    ASSERT_OK(writer->Write(std::move(non_null_memory_batch)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> nullable_memory_batch,
                         MakeUnpartitionedBatchFromJson(R"([
                             [4, null, "p0"],
                             [5, "memory-value-5", "p0"]
                         ])"));
    ASSERT_OK(writer->Write(std::move(nullable_memory_batch)));

    std::shared_ptr<Predicate> predicate = PredicateBuilder::IsNull(
        /*field_index=*/1, /*field_name=*/"payload", FieldType::STRING);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, CreatePlan(realtime_context, predicate));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadPlan(plan, realtime_context, {"id", "payload", "pt"}, predicate,
                                  /*enable_predicate_filter=*/false));

    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("id", arrow::int64()),
         arrow::field("payload", arrow::utf8()), arrow::field("pt", arrow::utf8())});
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, 0, null, "p0"],
            [0, 4, null, "p0"],
            [0, 5, "memory-value-5", "p0"]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, result);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*result))
        << result->ToString();
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestUnionReadAfterColumnRename) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> first_context, RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer,
                         CreateRealtimeWriter(first_context));
    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(first_writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));
    ASSERT_OK(first_writer->Close());

    std::shared_ptr<arrow::Field> renamed_payload = arrow::field("renamed_payload", arrow::utf8());
    ASSERT_OK(TestHelper::WriteNextSchema(
        dir_->GetFileSystem(), table_path_,
        {DataField(0, fields_[0]), DataField(1, renamed_payload), DataField(2, fields_[2])},
        /*highest_field_id=*/2, options_));
    fields_[1] = renamed_payload;
    schema_ = arrow::schema(fields_);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> second_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer,
                         CreateRealtimeWriter(second_context));
    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(second_writer->Write(std::move(memory_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(second_context, /*predicate=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadPlan(plan, second_context, {"id", "renamed_payload", "pt"},
                                  /*predicate=*/nullptr, /*enable_predicate_filter=*/false));
    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("id", arrow::int64()),
         renamed_payload, arrow::field("pt", arrow::utf8())});
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, 0, "value-0", "p0"],
            [0, 1, "value-1", "p0"],
            [0, 2, "value-2", "p0"],
            [0, 3, "value-3", "p0"],
            [0, 4, "value-4", "p0"]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, result);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*result))
        << result->ToString();
    ASSERT_OK(second_writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestUnionReadWithNestedStructProjection) {
    std::shared_ptr<arrow::DataType> address_type =
        arrow::struct_({arrow::field("city", arrow::utf8()), arrow::field("zip", arrow::int64())});
    std::shared_ptr<arrow::DataType> profile_type = arrow::struct_(
        {arrow::field("name", arrow::utf8()), arrow::field("address", address_type)});
    fields_ = {arrow::field("id", arrow::int64()), arrow::field("profile", profile_type),
               arrow::field("pt", arrow::utf8())};
    schema_ = arrow::schema(fields_);
    CreateTable(/*partition_keys=*/{});

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeUnpartitionedBatchFromJson(R"([
                             [0, ["disk-0", ["hangzhou", 310000]], "p0"],
                             [1, ["disk-1", ["shanghai", 200000]], "p0"]
                         ])"));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t disk_snapshot_id, Commit(disk_commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(disk_snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeUnpartitionedBatchFromJson(R"([
                             [2, ["memory-2", ["beijing", 100000]], "p0"],
                             [3, ["memory-3", ["shenzhen", 518000]], "p0"]
                         ])"));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    std::shared_ptr<arrow::DataType> projected_address_type =
        arrow::struct_({arrow::field("city", arrow::utf8())});
    std::shared_ptr<arrow::DataType> projected_profile_type =
        arrow::struct_({arrow::field("address", projected_address_type)});
    std::shared_ptr<arrow::Schema> projected_schema = arrow::schema(
        {arrow::field("id", arrow::int64()), arrow::field("profile", projected_profile_type),
         arrow::field("pt", arrow::utf8())});
    ReadPlanWithSchemaAndCheck(plan, realtime_context, projected_schema, R"([
        [0, 0, [["hangzhou"]], "p0"],
        [0, 1, [["shanghai"]], "p0"],
        [0, 2, [["beijing"]], "p0"],
        [0, 3, [["shenzhen"]], "p0"]
    ])");
    ASSERT_OK(writer->Close());
}

void RealtimeWriteInteTest::RunUnionReadWithSelectedMapKeys(bool primary_key) {
    std::shared_ptr<arrow::DataType> map_type = arrow::map(arrow::utf8(), arrow::int64());
    fields_ = {arrow::field("id", arrow::int64()), arrow::field("tags", map_type),
               arrow::field("pt", arrow::utf8())};
    schema_ = arrow::schema(fields_);
    options_["fields.tags.map.storage-layout"] = "shared-shredding";
    options_["fields.tags.map.shared-shredding.max-columns"] = "2";
    if (primary_key) {
        CreatePkTable();
    } else {
        CreateTable(/*partition_keys=*/{});
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeUnpartitionedBatchFromJson(R"([
                             [0, [["a", 10], ["b", 20]], "p0"],
                             [1, [["c", 30]], "p0"]
                         ])"));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t disk_snapshot_id, Commit(disk_commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(disk_snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeUnpartitionedBatchFromJson(R"([
                             [2, [["a", 40], ["c", 50]], "p0"],
                             [3, null, "p0"]
                         ])"));
    ASSERT_OK(writer->Write(std::move(memory_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));

    std::shared_ptr<arrow::KeyValueMetadata> selected_keys =
        arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"c,a,missing"});
    std::shared_ptr<arrow::Field> selected_map_field = fields_[1]->WithMetadata(selected_keys);
    auto selected_map_schema = arrow::schema({fields_[0], selected_map_field, fields_[2]});
    ReadPlanWithSchemaAndCheck(plan, realtime_context, selected_map_schema, R"([
        [0, 0, [["a", 10]], "p0"],
        [0, 1, [["c", 30]], "p0"],
        [0, 2, [["c", 50], ["a", 40]], "p0"],
        [0, 3, null, "p0"]
    ])");

    ASSERT_OK_AND_ASSIGN(plan, CreatePlan(realtime_context, /*predicate=*/nullptr));
    auto c_map_field = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportField(*fields_[1], c_map_field.get()).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MapSharedShreddingAccessBuilder> access_builder,
                         MapSharedShreddingAccessBuilder::Create(c_map_field.get()));
    ASSERT_OK(access_builder->AddKey("a"));
    ASSERT_OK(access_builder->AddKey("missing"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowSchema> c_access_field, access_builder->Build());
    auto access_field_result = arrow::ImportField(c_access_field.get());
    ASSERT_TRUE(access_field_result.ok()) << access_field_result.status().ToString();
    auto selected_struct_schema =
        arrow::schema({fields_[0], access_field_result.ValueOrDie(), fields_[2]});
    ReadPlanWithSchemaAndCheck(plan, realtime_context, selected_struct_schema, R"([
        [0, 0, [10, null], "p0"],
        [0, 1, [null, null], "p0"],
        [0, 2, [40, null], "p0"],
        [0, 3, null, "p0"]
    ])");
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestUnionReadWithSelectedMapKeys) {
    RunUnionReadWithSelectedMapKeys(/*primary_key=*/false);
}

TEST_F(RealtimeWriteInteTest, TestPkUnionReadWithSelectedMapKeys) {
    RunUnionReadWithSelectedMapKeys(/*primary_key=*/true);
}

void RealtimeWriteInteTest::RunUnionReadWithVariantAccess(bool primary_key) {
    fields_ = {arrow::field("id", arrow::int32()), VariantTypeUtils::ToArrowField("v")};
    schema_ = arrow::schema(fields_);
    options_[Options::MANIFEST_FORMAT] = "avro";
    options_[Options::FILE_FORMAT] = "parquet";
    options_[Options::VARIANT_SHREDDING_SCHEMA] = R"({
        "type": "ROW",
        "fields": [{
            "id": 0,
            "name": "v",
            "type": {
                "type": "ROW",
                "fields": [
                    {"id": 1, "name": "age", "type": "BIGINT"},
                    {"id": 2, "name": "city", "type": "STRING"}
                ]
            }
        }]
    })";
    if (primary_key) {
        CreatePkTable();
    } else {
        CreateTable(/*partition_keys=*/{});
    }

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> disk_data,
                         VariantTestData::BuildVariantBatch(
                             fields_[0], fields_[1],
                             {R"({"age":10,"city":"disk-a","note":"disk-fallback-a"})",
                              R"({"age":20,"city":"disk-b","note":"disk-fallback-b"})"},
                             pool_));
    ArrowArray c_disk_data;
    ASSERT_TRUE(arrow::ExportArray(*disk_data, &c_disk_data).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         RecordBatchBuilder(&c_disk_data).SetBucket(/*bucket=*/0).Finish());
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t disk_snapshot_id, Commit(disk_commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(disk_snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> memory_data,
                         VariantTestData::BuildVariantBatch(
                             fields_[0], fields_[1],
                             {R"({"age":30,"city":"memory-a"})",
                              R"({"age":40,"city":"memory-b","note":"memory-fallback-b"})"},
                             pool_, /*id_offset=*/2));
    ArrowArray c_memory_data;
    ASSERT_TRUE(arrow::ExportArray(*memory_data, &c_memory_data).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         RecordBatchBuilder(&c_memory_data).SetBucket(/*bucket=*/0).Finish());
    ASSERT_OK(writer->Write(std::move(memory_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));

    VariantAccessBuilder access_builder;
    auto age_target = std::make_unique<ArrowSchema>();
    auto city_target = std::make_unique<ArrowSchema>();
    auto note_target = std::make_unique<ArrowSchema>();
    ASSERT_TRUE(arrow::ExportField(*arrow::field("age", arrow::int64()), age_target.get()).ok());
    ASSERT_TRUE(arrow::ExportField(*arrow::field("city", arrow::utf8()), city_target.get()).ok());
    ASSERT_TRUE(arrow::ExportField(*arrow::field("note", arrow::utf8()), note_target.get()).ok());
    ASSERT_OK(access_builder.AddField(age_target.get(), "$.age", /*fail_on_error=*/false));
    ASSERT_OK(access_builder.AddField(city_target.get(), "$.city", /*fail_on_error=*/false));
    // `note` is intentionally absent from variant.shreddingSchema and must use binary fallback.
    ASSERT_OK(access_builder.AddField(note_target.get(), "$.note", /*fail_on_error=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowSchema> c_access_field, access_builder.Build("v"));
    auto access_field_result = arrow::ImportField(c_access_field.get());
    ASSERT_TRUE(access_field_result.ok()) << access_field_result.status().ToString();
    auto access_schema = arrow::schema({fields_[0], access_field_result.ValueOrDie()});
    ReadPlanWithSchemaAndCheck(plan, realtime_context, access_schema, R"([
        [0, 0, [10, "disk-a", "disk-fallback-a"]],
        [0, 1, [20, "disk-b", "disk-fallback-b"]],
        [0, 2, [30, "memory-a", null]],
        [0, 3, [40, "memory-b", "memory-fallback-b"]]
    ])");
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestUnionReadWithVariantAccess) {
    RunUnionReadWithVariantAccess(/*primary_key=*/false);
}

TEST_F(RealtimeWriteInteTest, TestPkUnionReadWithVariantAccess) {
    RunUnionReadWithVariantAccess(/*primary_key=*/true);
}

TEST_F(RealtimeWriteInteTest, TestRefreshCommittedSnapshotReclaimsMemory) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/10, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, disk_commits.size());
    ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot_id,
                         Commit(disk_commits, /*commit_identifier=*/0));

    std::vector<Row> memory_rows = MakeRows(/*first_id=*/10, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    std::vector<Row> expected_rows = disk_rows;
    expected_rows.insert(expected_rows.end(), memory_rows.begin(), memory_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> read1, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, read1);
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_before_refresh,
                         GetRealtimeMemoryUsage(realtime_context));

    ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> read2, ReadRows(realtime_context));
    ASSERT_EQ(read1, read2);
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_after_refresh,
                         GetRealtimeMemoryUsage(realtime_context));
    ASSERT_LT(memory_usage_after_refresh, memory_usage_before_refresh);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestPlanPinsMemoryAcrossRefresh) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot_id,
                         Commit(disk_commits, /*commit_identifier=*/0));

    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> pinned_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));

    ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot_id));

    std::vector<Row> expected_rows = disk_rows;
    expected_rows.insert(expected_rows.end(), memory_rows.begin(), memory_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> pinned_rows, ReadRows(pinned_plan, realtime_context));
    ASSERT_EQ(expected_rows, pinned_rows);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> refreshed_plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> refreshed_rows,
                         ReadRows(refreshed_plan, realtime_context));
    ASSERT_EQ(pinned_rows, refreshed_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestReaderPinsMemoryAcrossRefresh) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, commits.size());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_EQ(1, plan->Splits().size());
    std::shared_ptr<RealtimeSplit> realtime_split =
        std::dynamic_pointer_cast<RealtimeSplit>(plan->Splits()[0]);
    ASSERT_NE(nullptr, realtime_split);

    ReadContextBuilder read_builder(table_path_);
    read_builder.SetOptions(options_)
        .SetReadFieldNames({"id", "payload", "pt"})
        .WithRealtimeContext(realtime_context)
        .WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                         table_read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                         RealtimeContextImpl::Cast(realtime_context));
    ASSERT_NOK_WITH_MSG(realtime_context_impl->ResolveReadView(realtime_split->OpaqueTicket()),
                        "ticket does not exist or has expired");
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_before_refresh,
                         GetRealtimeMemoryUsage(realtime_context));
    ASSERT_GT(memory_usage_before_refresh, 0);

    ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot_id, Commit(commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot_id));
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_after_refresh,
                         GetRealtimeMemoryUsage(realtime_context));
    ASSERT_LT(memory_usage_after_refresh, memory_usage_before_refresh);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(std::move(reader)));
    std::shared_ptr<arrow::DataType> result_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("id", arrow::int64()),
         arrow::field("payload", arrow::utf8()), arrow::field("pt", arrow::utf8())});
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, 0, "value-0", "p0"],
            [0, 1, "value-1", "p0"],
            [0, 2, "value-2", "p0"]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, result);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*result))
        << result->ToString();
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestRepeatedCommitReadAndRefresh) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    constexpr int64_t kRoundCount = 3;
    constexpr int64_t kRowsPerRound = 4;
    std::vector<Row> expected_rows;
    for (int64_t round = 0; round < kRoundCount; ++round) {
        std::vector<Row> rows = MakeRows(round * kRowsPerRound, kRowsPerRound, /*partition=*/"p0");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                             writer->PrepareCommitWithProgress(/*commit_identifier=*/round));
        ASSERT_EQ(1, commits.size());
        ASSERT_EQ(OffsetRange(round * kRowsPerRound, (round + 1) * kRowsPerRound),
                  commits[0].offset_range);
        ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot_id,
                             Commit(commits, /*commit_identifier=*/round));
        expected_rows.insert(expected_rows.end(), rows.begin(), rows.end());

        ASSERT_OK_AND_ASSIGN(std::vector<Row> read_before_refresh, ReadRows(realtime_context));
        ASSERT_EQ(expected_rows, read_before_refresh);
        ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_before_refresh,
                             GetRealtimeMemoryUsage(realtime_context));
        ASSERT_GT(memory_usage_before_refresh, 0);

        ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot_id));

        ASSERT_OK_AND_ASSIGN(std::vector<Row> read_after_refresh, ReadRows(realtime_context));
        ASSERT_EQ(read_before_refresh, read_after_refresh);
        ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_after_refresh,
                             GetRealtimeMemoryUsage(realtime_context));
        ASSERT_EQ(0, memory_usage_after_refresh);
    }
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestRefreshLatestSnapshotReclaimsMultipleCommittedSegments) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    constexpr int64_t kSnapshotCount = 3;
    constexpr int64_t kRowsPerSnapshot = 2;
    std::vector<Row> expected_rows;
    int64_t latest_snapshot_id = -1;
    for (int64_t snapshot_index = 0; snapshot_index < kSnapshotCount; ++snapshot_index) {
        std::vector<Row> rows =
            MakeRows(snapshot_index * kRowsPerSnapshot, kRowsPerSnapshot, /*partition=*/"p0");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/false));
        ASSERT_OK(writer->Write(std::move(batch)));
        ASSERT_OK_AND_ASSIGN(
            std::vector<RealtimeCommitProgress> commits,
            writer->PrepareCommitWithProgress(/*commit_identifier=*/snapshot_index));
        ASSERT_EQ(1, commits.size());
        ASSERT_OK_AND_ASSIGN(latest_snapshot_id,
                             Commit(commits, /*commit_identifier=*/snapshot_index));
        expected_rows.insert(expected_rows.end(), rows.begin(), rows.end());
    }

    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows_before_refresh, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, rows_before_refresh);
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_before_refresh,
                         GetRealtimeMemoryUsage(realtime_context));
    ASSERT_GT(memory_usage_before_refresh, 0);

    ASSERT_OK(writer->RefreshCommittedSnapshot(latest_snapshot_id));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows_after_refresh, ReadRows(realtime_context));
    ASSERT_EQ(rows_before_refresh, rows_after_refresh);
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_after_refresh,
                         GetRealtimeMemoryUsage(realtime_context));
    ASSERT_EQ(0, memory_usage_after_refresh);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestOverwriteRequiresReopenRealtimeContext) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> committed_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> committed_batch,
                         MakeBatch(committed_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(committed_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t committed_snapshot_id, Commit(commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(committed_snapshot_id));

    std::vector<Row> building_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> building_batch,
                         MakeBatch(building_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(building_batch)));
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_before_overwrite,
                         GetRealtimeMemoryUsage(realtime_context));
    ASSERT_GT(memory_usage_before_overwrite, 0);

    ASSERT_OK_AND_ASSIGN(int64_t overwrite_snapshot_id, TruncateTable(/*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options_));
    SnapshotManager snapshot_manager(core_options.GetFileSystem(), table_path_);
    ASSERT_OK_AND_ASSIGN(Snapshot overwrite_snapshot,
                         snapshot_manager.LoadSnapshot(overwrite_snapshot_id));
    ASSERT_EQ(Snapshot::CommitKind::Overwrite(), overwrite_snapshot.GetCommitKind());
    ASSERT_FALSE(RealtimeCommitProperties::GetOffsetsPath(overwrite_snapshot));

    ASSERT_NOK_WITH_MSG(writer->RefreshCommittedSnapshot(overwrite_snapshot_id),
                        "recreate RealtimeContext");
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_after_failed_refresh,
                         GetRealtimeMemoryUsage(realtime_context));
    ASSERT_EQ(memory_usage_before_overwrite, memory_usage_after_failed_refresh);
    ASSERT_OK(writer->Close());
    writer.reset();
    realtime_context.reset();

    ASSERT_OK_AND_ASSIGN(realtime_context, RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(writer, CreateRealtimeWriter(realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> replay_batch,
                         MakeBatch(building_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(replay_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> replay_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/2));
    ASSERT_EQ(1, replay_commits.size());
    ASSERT_EQ(OffsetRange(0, 2), replay_commits[0].offset_range);
    ASSERT_OK_AND_ASSIGN(int64_t replay_snapshot_id,
                         Commit(replay_commits, /*commit_identifier=*/2));
    ASSERT_OK(writer->RefreshCommittedSnapshot(replay_snapshot_id));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> replayed_rows, ReadRows(realtime_context));
    ASSERT_EQ(building_rows, replayed_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestReopenRealtimeContextAfterRollback) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> first_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t first_snapshot_id, Commit(first_commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(first_snapshot_id));

    std::vector<Row> second_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(int64_t second_snapshot_id,
                         Commit(second_commits, /*commit_identifier=*/1));
    ASSERT_OK(writer->RefreshCommittedSnapshot(second_snapshot_id));

    ASSERT_OK_AND_ASSIGN(int64_t rollback_snapshot_id, RollbackToAsLatest(first_snapshot_id));
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap rollback_offsets, ReadCommittedOffsets());
    ASSERT_EQ(1, rollback_offsets.size());
    ASSERT_EQ(3, rollback_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_NOK_WITH_MSG(writer->RefreshCommittedSnapshot(rollback_snapshot_id),
                        "recreate RealtimeContext");
    ASSERT_OK(writer->Close());
    writer.reset();
    realtime_context.reset();

    // Reopen the same real-time writer identity from the target snapshot's progress and replay
    // input after that restored boundary.
    ASSERT_OK_AND_ASSIGN(realtime_context, RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(writer, CreateRealtimeWriter(realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> replay_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(replay_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> replay_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/2));
    ASSERT_EQ(1, replay_commits.size());
    ASSERT_EQ(OffsetRange(3, 5), replay_commits[0].offset_range);
    ASSERT_OK(Commit(replay_commits, /*commit_identifier=*/2));

    std::vector<Row> expected_rows = first_rows;
    expected_rows.insert(expected_rows.end(), second_rows.begin(), second_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
    ASSERT_EQ(expected_rows, actual_rows);
    ASSERT_OK(writer->Close());
}

void RealtimeWriteInteTest::RunConcurrencyTest(bool primary_key) {
    if (primary_key) {
        CreatePkTable();
    } else {
        CreateTable(/*partition_keys=*/{});
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    constexpr int32_t kPrepareThreadCount = 4;
    constexpr int32_t kReadThreadCount = 4;
    constexpr int64_t kBatchCount = 12;
    constexpr int64_t kRowsPerBatch = 2;
    const int64_t total_rows = kBatchCount * (primary_key ? 3 : kRowsPerBatch);

    std::vector<std::vector<Row>> pk_batches;
    std::vector<std::vector<RecordBatch::RowKind>> pk_row_kinds;
    std::vector<std::vector<Row>> pk_expected_states(1);
    if (primary_key) {
        std::map<int64_t, Row> current_rows;
        for (int64_t batch_index = 0; batch_index < kBatchCount; ++batch_index) {
            const int64_t key = batch_index % 4;
            const int64_t deleted_key = (key + 2) % 4;
            std::vector<Row> rows = {{key, "update-" + std::to_string(batch_index), "p0"},
                                     {key, "latest-" + std::to_string(batch_index), "p0"},
                                     {deleted_key, "deleted-" + std::to_string(batch_index), "p0"}};
            pk_batches.push_back(rows);
            pk_row_kinds.push_back({batch_index < 4 ? RecordBatch::RowKind::INSERT
                                                    : RecordBatch::RowKind::UPDATE_AFTER,
                                    RecordBatch::RowKind::UPDATE_AFTER,
                                    RecordBatch::RowKind::DELETE});
            current_rows[key] = rows[1];
            current_rows.erase(deleted_key);
            std::vector<Row> expected;
            for (const auto& [id, row] : current_rows) {
                static_cast<void>(id);
                expected.push_back(row);
            }
            pk_expected_states.push_back(std::move(expected));
        }
    }

    auto validate_read = [&](const std::vector<Row>& rows) {
        if (!primary_key) {
            return ValidateReadPrefix(rows, total_rows);
        }
        if (std::find(pk_expected_states.begin(), pk_expected_states.end(), rows) ==
            pk_expected_states.end()) {
            return Status::Invalid("PK real-time read does not match any completed write");
        }
        return Status::OK();
    };

    std::atomic<bool> writer_done{false};
    std::atomic<bool> prepare_done{false};
    std::atomic<bool> commit_done{false};
    std::atomic<bool> refresh_done{false};
    std::atomic<int64_t> next_prepare_identifier{0};
    std::atomic<int32_t> commit_count{0};
    std::atomic<int32_t> refresh_count{0};
    ConcurrentTestState state;
    std::map<int64_t, RealtimeCommitProgress> pending_commits;
    std::deque<int64_t> pending_snapshot_ids;
    std::vector<int32_t> prepare_call_counts(kPrepareThreadCount, 0);
    std::vector<int32_t> read_call_counts(kReadThreadCount, 0);

    auto enqueue_prepared_commits = [&](std::vector<RealtimeCommitProgress>&& commits) {
        std::string error;
        {
            std::lock_guard<std::mutex> lock(state.mutex);
            for (RealtimeCommitProgress& commit : commits) {
                int64_t offset_begin = commit.offset_range.begin;
                if (!pending_commits.emplace(offset_begin, std::move(commit)).second) {
                    error = "duplicate prepared real-time offset range";
                    break;
                }
            }
        }
        if (!error.empty()) {
            state.RecordError(error);
        }
        state.progress_cv.notify_all();
    };

    std::thread write_thread([&]() {
        state.WaitForStart();
        for (int64_t batch_index = 0; batch_index < kBatchCount && !state.ShouldStop();
             ++batch_index) {
            std::vector<Row> rows = primary_key
                                        ? pk_batches[static_cast<size_t>(batch_index)]
                                        : MakeRows(batch_index * kRowsPerBatch, kRowsPerBatch,
                                                   /*partition=*/"p0");
            Result<std::unique_ptr<RecordBatch>> batch_result =
                primary_key ? MakeBatch(rows, /*partitioned=*/false, /*bucket=*/0,
                                        pk_row_kinds[static_cast<size_t>(batch_index)])
                            : MakeBatch(rows, /*partitioned=*/false);
            if (state.RecordErrorIfNotOk(batch_result)) {
                break;
            }
            Status status = writer->Write(std::move(batch_result).value());
            if (state.RecordErrorIfNotOk(status)) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            // Pause midway until one refresh completes to guarantee write and refresh overlap.
            if (batch_index + 1 == kBatchCount / 2) {
                auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
                while (refresh_count.load(std::memory_order_acquire) == 0 && !state.ShouldStop() &&
                       std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                if (refresh_count.load(std::memory_order_acquire) == 0 && !state.ShouldStop()) {
                    state.RecordError("timed out waiting for a refresh while writing");
                    break;
                }
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> prepare_threads;
    prepare_threads.reserve(kPrepareThreadCount);
    for (int32_t thread_index = 0; thread_index < kPrepareThreadCount; ++thread_index) {
        prepare_threads.emplace_back([&, thread_index]() {
            state.WaitForStart();
            do {
                int64_t identifier = next_prepare_identifier.fetch_add(1);
                Result<std::vector<RealtimeCommitProgress>> result =
                    writer->PrepareCommitWithProgress(identifier);
                ++prepare_call_counts[thread_index];
                if (state.RecordErrorIfNotOk(result)) {
                    break;
                }
                enqueue_prepared_commits(std::move(result).value());
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } while (!writer_done.load(std::memory_order_acquire) && !state.ShouldStop());
        });
    }

    std::thread commit_thread([&]() {
        state.WaitForStart();
        int64_t next_offset = 0;
        int64_t commit_identifier = 0;
        while (!state.ShouldStop()) {
            std::optional<RealtimeCommitProgress> next_commit;
            {
                std::unique_lock<std::mutex> lock(state.mutex);
                state.progress_cv.wait(lock, [&]() {
                    return state.ShouldStop() || pending_commits.count(next_offset) > 0 ||
                           prepare_done.load(std::memory_order_acquire);
                });
                if (state.ShouldStop()) {
                    break;
                }
                auto iter = pending_commits.find(next_offset);
                if (iter == pending_commits.end()) {
                    if (prepare_done.load(std::memory_order_acquire)) {
                        if (!pending_commits.empty()) {
                            lock.unlock();
                            state.RecordError("prepared real-time offset ranges contain a gap");
                        }
                        break;
                    }
                    continue;
                }
                next_commit = std::move(iter->second);
                pending_commits.erase(iter);
            }

            std::vector<RealtimeCommitProgress> commits;
            commits.push_back(std::move(next_commit).value());
            int64_t committed_end_offset = commits[0].offset_range.end;
            Result<int64_t> commit_result = Commit(commits, commit_identifier++);
            if (state.RecordErrorIfNotOk(commit_result)) {
                break;
            }
            next_offset = committed_end_offset;
            {
                std::lock_guard<std::mutex> lock(state.mutex);
                pending_snapshot_ids.push_back(std::move(commit_result).value());
            }
            ++commit_count;
            state.snapshot_cv.notify_all();
        }
        commit_done.store(true, std::memory_order_release);
        state.snapshot_cv.notify_all();
    });

    std::thread refresh_thread([&]() {
        state.WaitForStart();
        while (!state.ShouldStop()) {
            std::optional<int64_t> snapshot_id;
            {
                std::unique_lock<std::mutex> lock(state.mutex);
                state.snapshot_cv.wait(lock, [&]() {
                    return state.ShouldStop() || !pending_snapshot_ids.empty() ||
                           commit_done.load(std::memory_order_acquire);
                });
                if (state.ShouldStop()) {
                    break;
                }
                if (pending_snapshot_ids.empty()) {
                    if (commit_done.load(std::memory_order_acquire)) {
                        break;
                    }
                    continue;
                }
                snapshot_id = pending_snapshot_ids.front();
                pending_snapshot_ids.pop_front();
            }
            Status status = writer->RefreshCommittedSnapshot(snapshot_id.value());
            if (state.RecordErrorIfNotOk(status)) {
                break;
            }
            ++refresh_count;
        }
        refresh_done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> read_threads;
    read_threads.reserve(kReadThreadCount);
    for (int32_t thread_index = 0; thread_index < kReadThreadCount; ++thread_index) {
        read_threads.emplace_back([&, thread_index]() {
            state.WaitForStart();
            while (!refresh_done.load(std::memory_order_acquire) && !state.ShouldStop()) {
                Result<std::vector<Row>> result = ReadRows(realtime_context);
                ++read_call_counts[thread_index];
                if (state.RecordErrorIfNotOk(result)) {
                    break;
                }
                Status status = validate_read(result.value());
                if (state.RecordErrorIfNotOk(status)) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }

    constexpr int32_t kWorkerCount = 1 + kPrepareThreadCount + 1 + 1 + kReadThreadCount;
    state.StartWhenReady(kWorkerCount);

    write_thread.join();
    for (std::thread& thread : prepare_threads) {
        thread.join();
    }
    if (!state.ShouldStop()) {
        Result<std::vector<RealtimeCommitProgress>> final_result =
            writer->PrepareCommitWithProgress(next_prepare_identifier.fetch_add(1));
        if (!state.RecordErrorIfNotOk(final_result)) {
            enqueue_prepared_commits(std::move(final_result).value());
        }
    }
    prepare_done.store(true, std::memory_order_release);
    state.progress_cv.notify_all();

    commit_thread.join();
    refresh_thread.join();
    for (std::thread& thread : read_threads) {
        thread.join();
    }

    ASSERT_TRUE(state.Errors().empty()) << (state.Errors().empty() ? "" : state.Errors().front());
    for (int32_t call_count : prepare_call_counts) {
        ASSERT_GT(call_count, 0);
    }
    for (int32_t call_count : read_call_counts) {
        ASSERT_GT(call_count, 0);
    }
    ASSERT_GE(commit_count.load(), 2);
    ASSERT_GE(refresh_count.load(), 2);
    ASSERT_OK_AND_ASSIGN(std::vector<Row> final_rows, ReadRows(realtime_context));
    if (primary_key) {
        ASSERT_EQ(pk_expected_states.back(), final_rows);
    } else {
        ASSERT_EQ(total_rows, static_cast<int64_t>(final_rows.size()));
        ASSERT_OK(ValidateReadPrefix(final_rows, total_rows));
    }
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(total_rows,
              committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage, GetRealtimeMemoryUsage(realtime_context));
    ASSERT_EQ(0, memory_usage);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestConcurrentWritePrepareCommitReadAndRefresh) {
    RunConcurrencyTest(/*primary_key=*/false);
}

TEST_F(RealtimeWriteInteTest, TestPkConcurrency) {
    RunConcurrencyTest(/*primary_key=*/true);
}

TEST_F(RealtimeWriteInteTest, TestMultiplePartitions) {
    CreateTable(/*partition_keys=*/{"pt"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<std::vector<Row>> disk_rows;
    for (int64_t partition_index = 0; partition_index < 2; ++partition_index) {
        std::string partition = "p" + std::to_string(partition_index);
        std::vector<Row> rows = MakeRows(partition_index * 10, /*count=*/10, partition);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(rows, /*partitioned=*/true));
        ASSERT_OK(writer->Write(std::move(batch)));
        disk_rows.push_back(std::move(rows));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(2, disk_commits.size());
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));

    std::vector<Row> p0_memory_rows = MakeRows(/*first_id=*/20, /*count=*/5, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p0_memory_batch,
                         MakeBatch(p0_memory_rows, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p0_memory_batch)));
    std::vector<Row> p2_memory_rows = MakeRows(/*first_id=*/30, /*count=*/5, /*partition=*/"p2");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p2_memory_batch,
                         MakeBatch(p2_memory_rows, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p2_memory_batch)));

    // p0 has disk and memory rows, p1 is disk-only, and p2 is memory-only.
    std::vector<Row> expected_rows = disk_rows[0];
    expected_rows.insert(expected_rows.end(), p0_memory_rows.begin(), p0_memory_rows.end());
    expected_rows.insert(expected_rows.end(), disk_rows[1].begin(), disk_rows[1].end());
    expected_rows.insert(expected_rows.end(), p2_memory_rows.begin(), p2_memory_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, actual_rows);

    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(2, committed_offsets.size());
    for (int64_t partition_index = 0; partition_index < 2; ++partition_index) {
        RealtimePartitionBucket partition_bucket({{"pt", "p" + std::to_string(partition_index)}},
                                                 /*bucket=*/0);
        ASSERT_EQ(10, committed_offsets.at(partition_bucket));
    }
    ASSERT_EQ(committed_offsets.end(),
              committed_offsets.find(RealtimePartitionBucket({{"pt", "p2"}}, /*bucket=*/0)));

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> final_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(2, final_commits.size());
    ASSERT_OK_AND_ASSIGN(int64_t final_snapshot_id, Commit(final_commits, /*commit_identifier=*/1));
    ASSERT_OK(writer->RefreshCommittedSnapshot(final_snapshot_id));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> committed_rows, ReadRows(realtime_context));
    ASSERT_EQ(expected_rows, committed_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestDropPartitionRequiresReopenRealtimeContext) {
    CreateTable(/*partition_keys=*/{"pt"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));

    std::vector<Row> disk_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> disk_batch,
                         MakeBatch(disk_rows, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(disk_batch)));
    std::vector<Row> retained_disk_rows =
        MakeRows(/*first_id=*/10, /*count=*/3, /*partition=*/"p1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> retained_disk_batch,
                         MakeBatch(retained_disk_rows, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(retained_disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK_AND_ASSIGN(int64_t disk_snapshot_id, Commit(disk_commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->RefreshCommittedSnapshot(disk_snapshot_id));

    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(memory_batch)));
    std::vector<Row> rows_before_drop = disk_rows;
    rows_before_drop.insert(rows_before_drop.end(), memory_rows.begin(), memory_rows.end());
    rows_before_drop.insert(rows_before_drop.end(), retained_disk_rows.begin(),
                            retained_disk_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows_before_drop, ReadRows(realtime_context));
    ASSERT_EQ(rows_before_drop, actual_rows_before_drop);

    ASSERT_OK_AND_ASSIGN(int64_t drop_snapshot_id,
                         DropPartition({{"pt", "p0"}}, /*commit_identifier=*/1));
    RealtimePartitionBucket partition_bucket({{"pt", "p0"}}, /*bucket=*/0);
    RealtimePartitionBucket retained_partition_bucket({{"pt", "p1"}}, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap offsets_after_drop, ReadCommittedOffsets());
    ASSERT_EQ(1, offsets_after_drop.size());
    ASSERT_EQ(3, offsets_after_drop.at(retained_partition_bucket));
    ASSERT_EQ(offsets_after_drop.end(), offsets_after_drop.find(partition_bucket));
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_before_refresh,
                         GetRealtimeMemoryUsage(realtime_context));
    ASSERT_NOK_WITH_MSG(writer->RefreshCommittedSnapshot(drop_snapshot_id),
                        "recreate RealtimeContext");
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage_after_refresh,
                         GetRealtimeMemoryUsage(realtime_context));
    ASSERT_EQ(memory_usage_before_refresh, memory_usage_after_refresh);
    ASSERT_OK(writer->Close());
    writer.reset();
    realtime_context.reset();

    // Reopen with p1's retained progress and replay p0 input that existed only in the old context.
    ASSERT_OK_AND_ASSIGN(realtime_context, RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(writer, CreateRealtimeWriter(realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> replay_batch,
                         MakeBatch(memory_rows, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(replay_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> replayed_rows, ReadRows(realtime_context));
    // The retained p1 disk split is read before the tail real-time split containing p0.
    std::vector<Row> expected_replayed_rows = retained_disk_rows;
    expected_replayed_rows.insert(expected_replayed_rows.end(), memory_rows.begin(),
                                  memory_rows.end());
    ASSERT_EQ(expected_replayed_rows, replayed_rows);

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> memory_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/2));
    ASSERT_EQ(1, memory_commits.size());
    ASSERT_EQ(OffsetRange(0, 2), memory_commits[0].offset_range);
    ASSERT_OK_AND_ASSIGN(int64_t memory_snapshot_id,
                         Commit(memory_commits, /*commit_identifier=*/2));
    ASSERT_OK(writer->RefreshCommittedSnapshot(memory_snapshot_id));
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows_after_memory_commit, ReadRows(realtime_context));
    std::vector<Row> expected_committed_rows = memory_rows;
    expected_committed_rows.insert(expected_committed_rows.end(), retained_disk_rows.begin(),
                                   retained_disk_rows.end());
    ASSERT_EQ(expected_committed_rows, rows_after_memory_commit);
    ASSERT_OK_AND_ASSIGN(uint64_t final_memory_usage, GetRealtimeMemoryUsage(realtime_context));
    ASSERT_EQ(0, final_memory_usage);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap final_offsets, ReadCommittedOffsets());
    ASSERT_EQ(2, final_offsets.size());
    ASSERT_EQ(2, final_offsets.at(partition_bucket));
    ASSERT_EQ(3, final_offsets.at(retained_partition_bucket));
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestDropInactivePartitionDoesNotRequireReopenRealtimeContext) {
    CreateTable(/*partition_keys=*/{"pt"});

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> seed_context, RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> seed_writer,
                         CreateRealtimeWriter(seed_context));
    for (int64_t partition_index = 0; partition_index < 2; ++partition_index) {
        std::string partition = "p" + std::to_string(partition_index);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch(MakeRows(partition_index * 10, /*count=*/3, partition),
                                       /*partitioned=*/true));
        ASSERT_OK(seed_writer->Write(std::move(batch)));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> seed_commits,
                         seed_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(2, seed_commits.size());
    ASSERT_OK(Commit(seed_commits, /*commit_identifier=*/0));
    ASSERT_OK(seed_writer->Close());
    seed_writer.reset();
    seed_context.reset();

    // The new context loads offsets for both partitions, but creates a store only for p0.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p0_batch,
                         MakeBatch(MakeRows(/*first_id=*/20, /*count=*/1, /*partition=*/"p0"),
                                   /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p0_batch)));

    ASSERT_OK_AND_ASSIGN(int64_t drop_snapshot_id,
                         DropPartition({{"pt", "p1"}}, /*commit_identifier=*/1));
    ASSERT_OK(writer->RefreshCommittedSnapshot(drop_snapshot_id));
    const RealtimePartitionBucket p0_partition_bucket({{"pt", "p0"}}, /*bucket=*/0);
    const RealtimePartitionBucket p1_partition_bucket({{"pt", "p1"}}, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap offsets_after_drop, ReadCommittedOffsets());
    ASSERT_EQ(1, offsets_after_drop.size());
    ASSERT_EQ(3, offsets_after_drop.at(p0_partition_bucket));
    ASSERT_EQ(offsets_after_drop.end(), offsets_after_drop.find(p1_partition_bucket));

    // Since p1 was never active in this context, writing it after the drop starts from zero.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p1_batch,
                         MakeBatch(MakeRows(/*first_id=*/30, /*count=*/2, /*partition=*/"p1"),
                                   /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p1_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/2));
    ASSERT_EQ(2, commits.size());
    auto p1_commit =
        std::find_if(commits.begin(), commits.end(), [&](const RealtimeCommitProgress& commit) {
            return commit.partition_bucket == p1_partition_bucket;
        });
    ASSERT_NE(commits.end(), p1_commit);
    ASSERT_EQ(OffsetRange(0, 2), p1_commit->offset_range);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestDropDatePartitionRemovesOffsetWithLegacyPartitionName) {
    CheckDropDatePartitionRemovesOffset(/*legacy_partition_name_enabled=*/true);
}

TEST_F(RealtimeWriteInteTest, TestDropDatePartitionRemovesOffsetWithoutLegacyPartitionName) {
    CheckDropDatePartitionRemovesOffset(/*legacy_partition_name_enabled=*/false);
}

TEST_F(RealtimeWriteInteTest, TestMultipleBucketsRestoreIndependentOffsets) {
    options_[Options::BUCKET] = "2";
    CreateTable(/*partition_keys=*/{});

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer, CreateRealtimeWriter());
    std::vector<Row> bucket0_disk_rows = MakeRows(/*first_id=*/0, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> bucket0_disk_batch,
                         MakeBatch(bucket0_disk_rows, /*partitioned=*/false, /*bucket=*/0));
    ASSERT_OK(first_writer->Write(std::move(bucket0_disk_batch)));
    std::vector<Row> bucket1_disk_rows = MakeRows(/*first_id=*/10, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> bucket1_disk_batch,
                         MakeBatch(bucket1_disk_rows, /*partitioned=*/false, /*bucket=*/1));
    ASSERT_OK(first_writer->Write(std::move(bucket1_disk_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_commits,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(2, first_commits.size());
    ASSERT_OK(Commit(first_commits, /*commit_identifier=*/0));
    ASSERT_OK(first_writer->Close());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> bucket0_memory_rows =
        MakeRows(/*first_id=*/2, /*count=*/1, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> bucket0_memory_batch,
                         MakeBatch(bucket0_memory_rows, /*partitioned=*/false, /*bucket=*/0));
    ASSERT_OK(second_writer->Write(std::move(bucket0_memory_batch)));
    std::vector<Row> bucket1_memory_rows =
        MakeRows(/*first_id=*/13, /*count=*/1, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> bucket1_memory_batch,
                         MakeBatch(bucket1_memory_rows, /*partitioned=*/false, /*bucket=*/1));
    ASSERT_OK(second_writer->Write(std::move(bucket1_memory_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         second_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(2, second_commits.size());
    std::map<int32_t, OffsetRange> prepared_ranges;
    for (const RealtimeCommitProgress& commit : second_commits) {
        prepared_ranges.emplace(commit.partition_bucket.bucket, commit.offset_range);
    }
    ASSERT_EQ(OffsetRange(2, 3), prepared_ranges.at(0));
    ASSERT_EQ(OffsetRange(3, 4), prepared_ranges.at(1));

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(realtime_context));
    std::vector<Row> expected_rows = bucket0_disk_rows;
    expected_rows.insert(expected_rows.end(), bucket0_memory_rows.begin(),
                         bucket0_memory_rows.end());
    expected_rows.insert(expected_rows.end(), bucket1_disk_rows.begin(), bucket1_disk_rows.end());
    expected_rows.insert(expected_rows.end(), bucket1_memory_rows.begin(),
                         bucket1_memory_rows.end());
    std::sort(expected_rows.begin(), expected_rows.end());
    std::sort(actual_rows.begin(), actual_rows.end());
    ASSERT_EQ(expected_rows, actual_rows);

    ASSERT_OK(Commit(second_commits, /*commit_identifier=*/1));
    ASSERT_OK(second_writer->Close());
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(2, committed_offsets.size());
    ASSERT_EQ(3, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_EQ(4, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/1)));
}

TEST_F(RealtimeWriteInteTest, TestRestoreOffsetFromCommittedSnapshot) {
    CreateTable(/*partition_keys=*/{});

    std::vector<Row> first_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> first_writer, CreateRealtimeWriter());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first_batch,
                         MakeBatch(first_rows, /*partitioned=*/false));
    ASSERT_OK(first_writer->Write(std::move(first_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> first_commits,
                         first_writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_EQ(1, first_commits.size());
    ASSERT_EQ(OffsetRange(0, 3), first_commits[0].offset_range);
    ASSERT_OK(Commit(first_commits, /*commit_identifier=*/0));
    ASSERT_OK(first_writer->Close());

    std::vector<Row> second_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> second_writer, CreateRealtimeWriter());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(second_writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         second_writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, second_commits.size());
    ASSERT_EQ(OffsetRange(3, 5), second_commits[0].offset_range);

    std::vector<Row> expected_rows = MakeRows(/*first_id=*/0, /*count=*/5, /*partition=*/"p0");
    FinalizeCommitAndCheck(second_writer.get(), std::move(second_commits),
                           /*prepare_identifier=*/1, std::move(expected_rows));

    RealtimePartitionBucket partition_bucket(/*partition=*/{}, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap second_committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(5, second_committed_offsets.at(partition_bucket));
}

}  // namespace paimon::test
