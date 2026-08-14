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
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
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
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/realtime_split.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/realtime/mem_indexer.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

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

    struct CollectedReadResult {
        std::unique_ptr<BatchReader> reader;
        std::shared_ptr<arrow::ChunkedArray> data;
    };

    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create("local");
        ASSERT_NE(nullptr, dir_);
        table_path_ = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
        fields_ = {arrow::field("id", arrow::int64()), arrow::field("payload", arrow::utf8()),
                   arrow::field("pt", arrow::utf8())};
        schema_ = arrow::schema(fields_);
        options_ = {
            {Options::MANIFEST_FORMAT, "orc"}, {Options::FILE_FORMAT, "orc"},
            {Options::FILE_SYSTEM, "local"},   {Options::BUCKET, "1"},
            {Options::BUCKET_KEY, "id"},       {Options::TARGET_FILE_SIZE, "1048576"},
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
        if (rows.empty()) {
            return Status::Invalid("cannot create an empty test batch");
        }
        const std::string& partition = std::get<2>(rows.front());
        std::string json = "[";
        for (size_t i = 0; i < rows.size(); ++i) {
            const auto& [id, payload, pt] = rows[i];
            if (pt != partition) {
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
        if (partitioned) {
            builder.SetPartition({{"pt", partition}});
        }
        return builder.SetBucket(bucket).Finish();
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

    Result<CollectedReadResult> ReadPlan(const std::shared_ptr<Plan>& plan,
                                         const std::shared_ptr<RealtimeContext>& realtime_context,
                                         const std::vector<std::string>& read_fields,
                                         const std::shared_ptr<Predicate>& predicate,
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
                               ReadResultCollector::CollectResult(reader.get()));
        return CollectedReadResult{std::move(reader), std::move(result)};
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
        PAIMON_ASSIGN_OR_RAISE(CollectedReadResult read_result,
                               ReadPlan(plan, realtime_context, {"id", "payload", "pt"},
                                        /*predicate=*/nullptr,
                                        /*enable_predicate_filter=*/false));
        const std::shared_ptr<arrow::ChunkedArray>& result = read_result.data;

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

    Result<uint64_t> GetRealtimeMemoryUsage(
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                               RealtimeContextImpl::Cast(realtime_context));
        PAIMON_ASSIGN_OR_RAISE(std::vector<RealtimePartitionBucketView> views,
                               realtime_context_impl->AcquireReadViews());
        uint64_t memory_usage = 0;
        for (const RealtimePartitionBucketView& view : views) {
            memory_usage += view.indexer->GetMemoryUsage();
        }
        return memory_usage;
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

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::string table_path_;
    std::string commit_user_ = "realtime_commit_user";
    arrow::FieldVector fields_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(RealtimeWriteInteTest, TestAppendCommitAndRead) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer, CreateRealtimeWriter());
    std::vector<Row> rows = MakeRows(/*first_id=*/0, /*count=*/10, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch(rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(batch)));
    FinalizeCommitAndCheck(writer.get(), /*realtime_commits=*/{}, /*prepare_identifier=*/0, rows);
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
    ASSERT_EQ(Range(0, kBatchCount * kRowsPerBatch - 1), commits[0].offset_range);
    std::shared_ptr<CommitMessageImpl> commit_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(commits[0].commit_message);
    ASSERT_NE(nullptr, commit_message);
    ASSERT_EQ(3, commit_message->GetNewFilesIncrement().NewFiles().size());
    ASSERT_OK(Commit(commits, /*commit_identifier=*/0));
    ASSERT_OK(writer->Close());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
    ASSERT_EQ(expected_rows, actual_rows);
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
    ASSERT_EQ(Range(0, 2), commits[0].offset_range);

    std::vector<Row> second_rows = MakeRows(/*first_id=*/3, /*count=*/2, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeBatch(second_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> second_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/1));
    ASSERT_EQ(1, second_commits.size());
    ASSERT_EQ(Range(3, 4), second_commits[0].offset_range);

    commits.push_back(std::move(second_commits[0]));
    std::reverse(commits.begin(), commits.end());
    ASSERT_OK(Commit(commits, /*commit_identifier=*/1));
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(4, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK(writer->Close());

    std::vector<Row> expected_rows = first_rows;
    expected_rows.insert(expected_rows.end(), second_rows.begin(), second_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows());
    ASSERT_EQ(expected_rows, actual_rows);
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
    ASSERT_EQ(-1, realtime_split->CommittedOffset());
    ASSERT_EQ(9, realtime_split->MemoryUpperOffset());
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
    ASSERT_NOK_WITH_MSG(table_read->CreateReader(plan->Splits()), "does not support Test");

    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(plan, realtime_context));
    ASSERT_EQ(rows, actual_rows);
    ASSERT_OK(writer->Close());
}

TEST_F(RealtimeWriteInteTest, TestVectorReaderFailurePreservesEarlierSplitTicket) {
    CreateTable(/*partition_keys=*/{"pt"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    std::vector<Row> p0_rows = MakeRows(/*first_id=*/0, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p0_batch,
                         MakeBatch(p0_rows, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p0_batch)));
    std::vector<Row> p1_rows = MakeRows(/*first_id=*/10, /*count=*/3, /*partition=*/"p1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> p1_batch,
                         MakeBatch(p1_rows, /*partitioned=*/true));
    ASSERT_OK(writer->Write(std::move(p1_batch)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan,
                         CreatePlan(realtime_context, /*predicate=*/nullptr));
    ASSERT_EQ(2, plan->Splits().size());

    std::vector<std::shared_ptr<Split>> invalid_splits = plan->Splits();
    std::shared_ptr<RealtimeSplit> second_split =
        std::dynamic_pointer_cast<RealtimeSplit>(invalid_splits[1]);
    ASSERT_NE(nullptr, second_split);
    std::vector<std::shared_ptr<Split>> second_disk_splits = second_split->DiskSplits();
    invalid_splits[1] = std::make_shared<RealtimeSplit>(
        RealtimeSplit::kCurrentVersion + 1, second_split->SnapshotId(), second_split->Partition(),
        second_split->Bucket(), std::move(second_disk_splits), second_split->CommittedOffset(),
        second_split->MemoryUpperOffset(), second_split->OpaqueTicket());

    ReadContextBuilder read_builder(table_path_);
    read_builder.SetOptions(options_)
        .SetReadFieldNames({"id", "payload", "pt"})
        .WithRealtimeContext(realtime_context)
        .WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_NOK_WITH_MSG(table_read->CreateReader(invalid_splits),
                        "unsupported real-time split version");

    std::vector<Row> expected_rows = p0_rows;
    expected_rows.insert(expected_rows.end(), p1_rows.begin(), p1_rows.end());
    ASSERT_OK_AND_ASSIGN(std::vector<Row> actual_rows, ReadRows(plan, realtime_context));
    ASSERT_EQ(expected_rows, actual_rows);
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
    ASSERT_EQ(Range(0, 2), commits[0].offset_range);
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
    ASSERT_EQ(Range(3, 4), second_commits[0].offset_range);

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
    ASSERT_EQ(Range(0, 2), disk_commits[0].offset_range);

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
    ASSERT_OK_AND_ASSIGN(CollectedReadResult memory_result,
                         ReadPlan(memory_plan, realtime_context, read_fields, read_predicate,
                                  /*enable_predicate_filter=*/true));
    std::shared_ptr<arrow::Array> expected_memory =
        arrow::ipc::internal::json::ArrayFromJSON(result_type, R"([
            [0, "value-2", 2]
        ])")
            .ValueOrDie();
    ASSERT_NE(nullptr, memory_result.data);
    ASSERT_TRUE(
        std::make_shared<arrow::ChunkedArray>(expected_memory)->Equals(*memory_result.data));

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> disk_commits,
                         writer->PrepareCommitWithProgress(/*commit_identifier=*/0));
    ASSERT_OK(Commit(disk_commits, /*commit_identifier=*/0));
    std::vector<Row> memory_rows = MakeRows(/*first_id=*/3, /*count=*/3, /*partition=*/"p0");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> memory_batch,
                         MakeBatch(memory_rows, /*partitioned=*/false));
    ASSERT_OK(writer->Write(std::move(memory_batch)));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> union_plan,
                         CreatePlan(realtime_context, scan_predicate));
    ASSERT_OK_AND_ASSIGN(CollectedReadResult union_result,
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
    ASSERT_NE(nullptr, union_result.data);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected_union)->Equals(*union_result.data));
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
    ASSERT_OK_AND_ASSIGN(CollectedReadResult result,
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
    ASSERT_NE(nullptr, result.data);
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(*result.data))
        << result.data->ToString();
    ASSERT_OK(writer->Close());
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
                         ReadResultCollector::CollectResult(reader.get()));
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
        ASSERT_EQ(Range(round * kRowsPerRound, (round + 1) * kRowsPerRound - 1),
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

TEST_F(RealtimeWriteInteTest, TestConcurrentWritePrepareCommitReadAndRefresh) {
    CreateTable(/*partition_keys=*/{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateRealtimeWriter(realtime_context));
    constexpr int32_t kPrepareThreadCount = 4;
    constexpr int32_t kReadThreadCount = 4;
    constexpr int64_t kBatchCount = 12;
    constexpr int64_t kRowsPerBatch = 2;
    constexpr int64_t kTotalRows = kBatchCount * kRowsPerBatch;

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
                int64_t offset_from = commit.offset_range.from;
                if (!pending_commits.emplace(offset_from, std::move(commit)).second) {
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
            std::vector<Row> rows =
                MakeRows(batch_index * kRowsPerBatch, kRowsPerBatch, /*partition=*/"p0");
            Result<std::unique_ptr<RecordBatch>> batch_result =
                MakeBatch(rows, /*partitioned=*/false);
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
            int64_t committed_offset = commits[0].offset_range.to;
            Result<int64_t> commit_result = Commit(commits, commit_identifier++);
            if (state.RecordErrorIfNotOk(commit_result)) {
                break;
            }
            next_offset = committed_offset + 1;
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
                Status status = ValidateReadPrefix(result.value(), kTotalRows);
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
    ASSERT_EQ(kTotalRows, static_cast<int64_t>(final_rows.size()));
    ASSERT_OK(ValidateReadPrefix(final_rows, kTotalRows));
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(kTotalRows - 1,
              committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_OK_AND_ASSIGN(uint64_t memory_usage, GetRealtimeMemoryUsage(realtime_context));
    ASSERT_EQ(0, memory_usage);
    ASSERT_OK(writer->Close());
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
        ASSERT_EQ(9, committed_offsets.at(partition_bucket));
    }
    ASSERT_EQ(committed_offsets.end(),
              committed_offsets.find(RealtimePartitionBucket({{"pt", "p2"}}, /*bucket=*/0)));
    ASSERT_OK(writer->Close());
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
    std::map<int32_t, Range> prepared_ranges;
    for (const RealtimeCommitProgress& commit : second_commits) {
        prepared_ranges.emplace(commit.partition_bucket.bucket, commit.offset_range);
    }
    ASSERT_EQ(Range(2, 2), prepared_ranges.at(0));
    ASSERT_EQ(Range(3, 3), prepared_ranges.at(1));

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
    ASSERT_EQ(2, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)));
    ASSERT_EQ(3, committed_offsets.at(RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/1)));
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
    ASSERT_EQ(Range(0, 2), first_commits[0].offset_range);
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
    ASSERT_EQ(Range(3, 4), second_commits[0].offset_range);

    std::vector<Row> expected_rows = MakeRows(/*first_id=*/0, /*count=*/5, /*partition=*/"p0");
    FinalizeCommitAndCheck(second_writer.get(), std::move(second_commits),
                           /*prepare_identifier=*/1, std::move(expected_rows));

    RealtimePartitionBucket partition_bucket(/*partition=*/{}, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap second_committed_offsets, ReadCommittedOffsets());
    ASSERT_EQ(4, second_committed_offsets.at(partition_bucket));
}

}  // namespace paimon::test
