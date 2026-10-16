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

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/predicate/greater_or_equal.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/data_table_batch_scan.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/macros.h"
#include "paimon/metrics.h"
#include "paimon/scan_cancellation.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/plan.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/counting_cache_test_utils.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class DefaultMetricsTableScan : public TableScan {
 public:
    Result<std::shared_ptr<Plan>> CreatePlan() override {
        return Status::NotImplemented("not implemented");
    }
};

class RecordingPlanningExecutor : public Executor {
 public:
    void Add(std::function<void()> task) override {
        ++submissions;
        task();
    }
    void ShutdownNow() override {
        ++shutdowns;
    }
    uint32_t GetThreadNum() const override {
        return 1;
    }
    int32_t submissions = 0;
    int32_t shutdowns = 0;
};

enum class CancelMetadataAt {
    None,
    Schema,
    Hint,
    Snapshot,
    ManifestListOpen,
    ManifestOpen,
    ManifestRead,
    Listing,
    SnapshotListing
};

struct PlanningStreamState {
    int32_t live = 0;
    int32_t closes = 0;
    int32_t unclosed = 0;
    int32_t reads = 0;
};

class PlanningInputStream : public InputStream {
 public:
    PlanningInputStream(std::unique_ptr<InputStream> input,
                        const std::shared_ptr<PlanningStreamState>& state,
                        ScanCancellationToken* token, bool cancel_read, bool fail_close)
        : input_(std::move(input)),
          state_(state),
          token_(token),
          cancel_read_(cancel_read),
          fail_close_(fail_close) {
        ++state_->live;
    }
    ~PlanningInputStream() override {
        --state_->live;
        if (!closed_) {
            ++state_->unclosed;
        }
    }
    Status Seek(int64_t offset, SeekOrigin origin) override {
        return input_->Seek(offset, origin);
    }
    Result<int64_t> GetPos() const override {
        return input_->GetPos();
    }
    Result<int64_t> Read(char* data, int64_t size) override {
        auto result = input_->Read(data, size);
        ObserveRead(result.ok());
        return result;
    }
    Result<int64_t> Read(char* data, int64_t size, int64_t offset) override {
        auto result = input_->Read(data, size, offset);
        ObserveRead(result.ok());
        return result;
    }
    void ReadAsync(char* data, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        input_->ReadAsync(data, size, offset, std::move(callback));
    }
    Result<std::string> GetUri() const override {
        return input_->GetUri();
    }
    Result<int64_t> Length() const override {
        return input_->Length();
    }
    Status Close() override {
        ++state_->closes;
        closed_ = true;
        PAIMON_RETURN_NOT_OK(input_->Close());
        return fail_close_ ? Status::IOError("metadata stream close failed") : Status::OK();
    }

 private:
    void ObserveRead(bool success) {
        ++state_->reads;
        if (success && cancel_read_) {
            token_->Cancel();
        }
    }
    std::unique_ptr<InputStream> input_;
    std::shared_ptr<PlanningStreamState> state_;
    ScanCancellationToken* token_;
    bool cancel_read_;
    bool fail_close_;
    bool closed_ = false;
};

class CancellingPlanningFileSystem : public LocalFileSystem {
 public:
    CancellingPlanningFileSystem(ScanCancellationToken* token, CancelMetadataAt phase)
        : token_(token), phase_(phase) {}

    using LocalFileSystem::Open;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        Record();
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> stream, LocalFileSystem::Open(path));
        const auto name = PathUtil::GetName(path);
        if (StringUtils::StartsWith(name, "manifest-list-")) {
            ++manifest_list_opens;
        } else if (StringUtils::StartsWith(name, "manifest-")) {
            ++manifest_file_opens;
        }
        if ((phase_ == CancelMetadataAt::ManifestListOpen &&
             StringUtils::StartsWith(name, "manifest-list-")) ||
            (phase_ == CancelMetadataAt::ManifestOpen &&
             StringUtils::StartsWith(name, "manifest-") &&
             !StringUtils::StartsWith(name, "manifest-list-"))) {
            token_->Cancel();
        }
        const bool manifest = StringUtils::StartsWith(name, "manifest-");
        if (manifest && !StringUtils::StartsWith(name, "manifest-list-") && pause_manifest_open) {
            pause_manifest_open();
        }
        return std::unique_ptr<InputStream>(new PlanningInputStream(
            std::move(stream), streams, token_,
            manifest && phase_ == CancelMetadataAt::ManifestRead,
            manifest && !StringUtils::StartsWith(name, "manifest-list-") && fail_manifest_close));
    }

    Result<std::unique_ptr<InputStream>> Open(const FileStatus& status) const override {
        ++known_size_opens;
        return Open(status.GetPath());
    }

    Status ReadFile(const std::string& path, std::string* content) override {
        Record();
        const auto name = PathUtil::GetName(path);
        if (phase_ == CancelMetadataAt::Hint && (name == "LATEST" || name == "EARLIEST") &&
            !hint_status.ok()) {
            token_->Cancel();
            return hint_status;
        }
        PAIMON_RETURN_NOT_OK(LocalFileSystem::ReadFile(path, content));
        if ((phase_ == CancelMetadataAt::Schema && StringUtils::StartsWith(name, "schema-")) ||
            (phase_ == CancelMetadataAt::Hint && (name == "LATEST" || name == "EARLIEST")) ||
            (phase_ == CancelMetadataAt::Snapshot && StringUtils::StartsWith(name, "snapshot-"))) {
            token_->Cancel();
        }
        return Status::OK();
    }

    Status ListDir(const std::string& path, std::vector<BasicFileStatus>* files) const override {
        Record();
        PAIMON_RETURN_NOT_OK(LocalFileSystem::ListDir(path, files));
        if (phase_ == CancelMetadataAt::Listing || (phase_ == CancelMetadataAt::SnapshotListing &&
                                                    PathUtil::GetName(path) == "snapshot")) {
            token_->Cancel();
        }
        return Status::OK();
    }

    Result<bool> Exists(const std::string& path) const override {
        Record();
        return LocalFileSystem::Exists(path);
    }

    mutable int32_t operations = 0;
    Status hint_status;
    std::function<void()> pause_manifest_open;
    mutable int32_t after_cancel = 0;
    mutable int32_t known_size_opens = 0;
    mutable int32_t manifest_list_opens = 0;
    mutable int32_t manifest_file_opens = 0;
    std::shared_ptr<PlanningStreamState> streams = std::make_shared<PlanningStreamState>();
    bool fail_manifest_close = false;

 private:
    void Record() const {
        ++operations;
        if (token_->IsCancelled()) {
            ++after_cancel;
        }
    }
    ScanCancellationToken* token_;
    CancelMetadataAt phase_;
};

Result<std::unique_ptr<TableScan>> FinishPlanningScan(ScanContextBuilder& builder) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> context, builder.Finish());
    return TableScan::Create(std::move(context));
}

Result<int64_t> PlanningRowCount(const std::shared_ptr<Plan>& plan) {
    int64_t rows = 0;
    for (const auto& split : plan->Splits()) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        if (!data_split) {
            return Status::Invalid("expected a data split");
        }
        rows += data_split->RowCount();
    }
    return rows;
}

// Only metadata is written: this fixture qualifies planning, not payload reading or writer history.
Status WriteCancellationPhaseTable(const std::string& path) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto pool = GetDefaultPool();
    PAIMON_RETURN_NOT_OK(fs->Mkdirs(path + "/schema"));
    PAIMON_RETURN_NOT_OK(fs->Mkdirs(path + "/manifest"));
    PAIMON_RETURN_NOT_OK(fs->Mkdirs(path + "/snapshot"));
    std::string schema;
    PAIMON_RETURN_NOT_OK(
        fs->ReadFile(GetDataDir() + "/parquet/append_09.db/append_09/schema/schema-0", &schema));
    PAIMON_RETURN_NOT_OK(fs->WriteFile(path + "/schema/schema-0", schema, false));
    auto row_type =
        arrow::schema({arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                       arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())});
    auto partition_type = arrow::schema({arrow::field("f1", arrow::int32())});
    PAIMON_ASSIGN_OR_RAISE(
        auto paths, FileStorePathFactory::Create(path, row_type, {"f1"}, "", "parquet", "data-",
                                                 true, {}, std::nullopt, false, pool));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileFormat> format, FileFormatFactory::Get("avro", {}));
    PAIMON_ASSIGN_OR_RAISE(auto options, CoreOptions::FromMap({}));
    PAIMON_ASSIGN_OR_RAISE(
        auto manifests, ManifestFile::Create(fs, format, "null", paths, 1024 * 1024, pool, options,
                                             partition_type));
    PAIMON_ASSIGN_OR_RAISE(auto lists,
                           ManifestList::Create(fs, format, "null", paths, nullptr, pool));
    std::vector<ManifestEntry> base;
    std::vector<ManifestEntry> delta;
    for (const auto& bounds :
         {std::make_pair(15, 0), std::make_pair(19, 16), std::make_pair(23, 20)}) {
        for (int32_t index = bounds.first; index >= bounds.second; --index) {
            PAIMON_ASSIGN_OR_RAISE(
                auto file,
                DataFileMeta::ForAppend("data-phase-" + std::to_string(index) + ".parquet", 8, 3,
                                        SimpleStats::EmptyStats(), index * 3, index * 3 + 2, 0,
                                        FileSource::Append(), std::nullopt, std::nullopt,
                                        std::nullopt, std::nullopt));
            const int32_t bucket = index >= 16 && index < 20 ? 1 : 0;
            auto partition = BinaryRowGenerator::GenerateRow(
                {static_cast<int32_t>(bucket == 0 ? 10 : 20)}, pool.get());
            base.emplace_back(FileKind::Add(), partition, bucket, 2, file);
            if (index >= 20) {
                delta.emplace_back(FileKind::Delete(), partition, bucket, 2, file);
            }
        }
    }
    PAIMON_ASSIGN_OR_RAISE(auto base_files, manifests->Write(base));
    PAIMON_ASSIGN_OR_RAISE(auto delta_files, manifests->Write(delta));
    PAIMON_ASSIGN_OR_RAISE(auto base_list, lists->Write(base_files));
    PAIMON_ASSIGN_OR_RAISE(auto delta_list, lists->Write(delta_files));
    Snapshot snapshot(1, 0, base_list.first, base_list.second, delta_list.first, delta_list.second,
                      std::nullopt, std::nullopt, std::nullopt, "phase-test", 1,
                      Snapshot::CommitKind::Append(), 0, 60, 60, std::nullopt, std::nullopt,
                      std::nullopt, std::nullopt, std::nullopt);
    PAIMON_ASSIGN_OR_RAISE(auto json, snapshot.ToJsonString());
    PAIMON_RETURN_NOT_OK(fs->WriteFile(path + "/snapshot/snapshot-1", json, false));
    SnapshotManager snapshots(fs, path);
    PAIMON_RETURN_NOT_OK(snapshots.CommitEarliestHint(1));
    return snapshots.CommitLatestHint(1);
}

void ConfigureCancellationPhaseScan(ScanContextBuilder* builder) {
    builder->AddOption(Options::SCAN_SNAPSHOT_ID, "1")
        .AddOption(Options::SOURCE_SPLIT_OPEN_FILE_COST, "0")
        .AddOption(Options::SOURCE_SPLIT_TARGET_SIZE, "16");
}

class CancelBeforePublicationLogger : public Logger {
 public:
    explicit CancelBeforePublicationLogger(ScanCancellationToken* token) : token_(token) {}
    bool IsLevelEnabled(PaimonLogLevel level) const override {
        return level == PAIMON_LOG_LEVEL_DEBUG;
    }
    void LogV(PaimonLogLevel, const char*, int, const char* function, const char* format,
              ...) override {
        if (std::strcmp(function, "ApplyPushDownLimit") == 0 &&
            std::strncmp(format, "Limit push down kept ", 21) == 0) {
            ++calls;
            token_->Cancel();
        }
    }
    int32_t calls = 0;

 private:
    ScanCancellationToken* token_;
};

// External debugger qualification uses these test-only symbols; production has no injection hook.
PAIMON_NOINLINE void PlanningCancellationProbe(ScanCancellationToken* token) {
    (void)token;
    std::atomic_signal_fence(std::memory_order_seq_cst);
}

class PlanningCountingCache : public CountingRoutingCache {
 public:
    PlanningCountingCache()
        : CountingRoutingCache(std::map<CacheKind, int64_t>{
              {CacheKind::MANIFEST, 64 * 1024 * 1024},
              {CacheKind::SNAPSHOT_LIVE_MANIFEST, 64 * 1024 * 1024}}) {}

    Status Put(const std::shared_ptr<CacheKey>& key,
               const std::shared_ptr<CacheValue>& value) override {
        ++puts;
        return CountingRoutingCache::Put(key, value);
    }
    std::atomic<int32_t> puts{0};
};

class CancellingStatsFunction : public LeafFunction {
 public:
    CancellingStatsFunction(ScanCancellationToken* token, bool fail) : token_(token), fail_(fail) {}

    Result<std::vector<char>> Test(const arrow::Array& array, const std::vector<Literal>& literals,
                                   arrow::MemoryPool* pool) const override {
        return delegate_.Test(array, literals, pool);
    }
    Result<bool> Test(const Literal& value, const std::vector<Literal>& literals) const override {
        return delegate_.Test(value, literals);
    }
    Result<bool> Test(int64_t rows, const Literal& min, const Literal& max,
                      const std::optional<int64_t>& nulls,
                      const std::vector<Literal>& literals) const override {
        ++visits;
        token_->Cancel();
        if (fail_) {
            return Status::IOError("original metadata predicate failure");
        }
        return delegate_.Test(rows, min, max, nulls, literals);
    }
    Type GetType() const override {
        return delegate_.GetType();
    }
    std::string ToString() const override {
        return delegate_.ToString();
    }
    const LeafFunction* Negate() const override {
        return delegate_.Negate();
    }
    mutable int32_t visits = 0;

 private:
    ScanCancellationToken* token_;
    bool fail_;
    const LeafFunction& delegate_ = GreaterOrEqual::Instance();
};

}  // namespace

TEST(TableScanTest, CancellationPhaseProbe) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    const auto path = directory->Str() + "/table";
    ASSERT_OK(WriteCancellationPhaseTable(path));
    auto token = std::make_shared<ScanCancellationToken>();
    auto executor = std::make_shared<RecordingPlanningExecutor>();
    ScanContextBuilder builder(path);
    ConfigureCancellationPhaseScan(&builder);
    builder.WithCancellationToken(token).WithExecutor(executor).SetLimit(7);
    ASSERT_OK_AND_ASSIGN(auto scan, FinishPlanningScan(builder));
    PlanningCancellationProbe(token.get());
    auto result = scan->CreatePlan();
    if (std::getenv("PAIMON_NATIVE_CANCELLATION_PROBE")) {
        ASSERT_TRUE(token->IsCancelled()) << "external cancellation trigger did not fire";
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().IsCancelled()) << result.status().ToString();
        ASSERT_TRUE(scan->CreatePlan().status().IsCancelled());
    } else {
        ASSERT_OK_AND_ASSIGN(auto plan, std::move(result));
        ASSERT_EQ(plan->SnapshotId(), std::optional<int64_t>(1));
        ASSERT_EQ(plan->Splits().size(), 2);
        ASSERT_OK_AND_ASSIGN(auto rows, PlanningRowCount(plan));
        ASSERT_EQ(rows, 12);
    }
    ASSERT_EQ(executor->submissions, 0);
    ASSERT_EQ(executor->shutdowns, 0);
}

TEST(TableScanTest, CancellationAfterLimitCannotPublishSuccessfulPlan) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    const auto path = directory->Str() + "/table";
    ASSERT_OK(WriteCancellationPhaseTable(path));
    for (int32_t limit : {-1, 7}) {
        SCOPED_TRACE(limit);
        ScanContextBuilder builder(path);
        ConfigureCancellationPhaseScan(&builder);
        if (limit >= 0) {
            builder.SetLimit(limit);
        }
        ASSERT_OK_AND_ASSIGN(auto scan, FinishPlanningScan(builder));
        ASSERT_OK_AND_ASSIGN(auto plan, scan->CreatePlan());
        ASSERT_EQ(plan->Splits().size(), limit < 0 ? 10 : 2);
        ASSERT_OK_AND_ASSIGN(auto rows, PlanningRowCount(plan));
        ASSERT_EQ(rows, limit < 0 ? 60 : 12);
    }
    auto token = std::make_shared<ScanCancellationToken>();
    auto executor = std::make_shared<RecordingPlanningExecutor>();
    ScanContextBuilder builder(path);
    ConfigureCancellationPhaseScan(&builder);
    builder.WithCancellationToken(token).WithExecutor(executor).SetLimit(7);
    ASSERT_OK_AND_ASSIGN(auto scan, FinishPlanningScan(builder));
    auto* batch_scan = dynamic_cast<DataTableBatchScan*>(scan.get());
    ASSERT_NE(batch_scan, nullptr);
    auto logger = std::make_unique<CancelBeforePublicationLogger>(token.get());
    auto* observation = logger.get();
    batch_scan->logger_ = std::move(logger);
    auto result = scan->CreatePlan();
    ASSERT_EQ(observation->calls, 1);
    ASSERT_TRUE(token->IsCancelled());
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsCancelled()) << result.status().ToString();
    ASSERT_TRUE(scan->CreatePlan().status().IsCancelled());
    ASSERT_EQ(executor->submissions, 0);
    ASSERT_EQ(executor->shutdowns, 0);
    ScanContextBuilder recovery_builder(path);
    ConfigureCancellationPhaseScan(&recovery_builder);
    recovery_builder.WithCancellationToken(std::make_shared<ScanCancellationToken>());
    ASSERT_OK_AND_ASSIGN(auto recovery, FinishPlanningScan(recovery_builder));
    ASSERT_OK_AND_ASSIGN(auto plan, recovery->CreatePlan());
    ASSERT_EQ(plan->Splits().size(), 10);
    ASSERT_OK_AND_ASSIGN(auto rows, PlanningRowCount(plan));
    ASSERT_EQ(rows, 60);
}

TEST(TableScanTest, CancellationPreservesAppendPlansWithoutSubmittingWork) {
    const auto path = GetDataDir() + "/parquet/append_09.db/append_09";
    const std::map<int64_t, int64_t> expected_rows = {{1, 5}, {3, 10}, {5, 11}};
    for (int64_t snapshot : {1, 3, 5, 0}) {
        SCOPED_TRACE(snapshot);
        const std::vector<std::string> modes =
            snapshot == 0 ? std::vector<std::string>{"latest", "latest-full"}
                          : std::vector<std::string>{"from-snapshot", "from-snapshot-full"};
        for (const auto& mode : modes) {
            SCOPED_TRACE(mode);
            ScanContextBuilder baseline_builder(path);
            ScanContextBuilder controlled_builder(path);
            baseline_builder.AddOption(Options::SCAN_MODE, mode);
            controlled_builder.AddOption(Options::SCAN_MODE, mode);
            if (snapshot != 0) {
                baseline_builder.AddOption(Options::SCAN_SNAPSHOT_ID, std::to_string(snapshot));
                controlled_builder.AddOption(Options::SCAN_SNAPSHOT_ID, std::to_string(snapshot));
            }
            auto token = std::make_shared<ScanCancellationToken>();
            auto executor = std::make_shared<RecordingPlanningExecutor>();
            controlled_builder.WithCancellationToken(token).WithExecutor(executor);
            ASSERT_OK_AND_ASSIGN(auto baseline, FinishPlanningScan(baseline_builder));
            ASSERT_OK_AND_ASSIGN(auto controlled, FinishPlanningScan(controlled_builder));
            ASSERT_OK_AND_ASSIGN(auto expected, baseline->CreatePlan());
            ASSERT_OK_AND_ASSIGN(auto actual, controlled->CreatePlan());
            ASSERT_EQ(actual->SnapshotId(), expected->SnapshotId());
            ASSERT_EQ(actual->SnapshotId(), std::optional<int64_t>(snapshot == 0 ? 5 : snapshot));
            ASSERT_OK_AND_ASSIGN(auto rows, PlanningRowCount(actual));
            ASSERT_EQ(rows, expected_rows.at(snapshot == 0 ? 5 : snapshot));
            ASSERT_EQ(actual->Splits().size(), expected->Splits().size());
            for (size_t index = 0; index < actual->Splits().size(); ++index) {
                auto left = std::dynamic_pointer_cast<DataSplitImpl>(expected->Splits()[index]);
                auto right = std::dynamic_pointer_cast<DataSplitImpl>(actual->Splits()[index]);
                ASSERT_TRUE(left);
                ASSERT_TRUE(right);
                ASSERT_TRUE(left->TEST_Equal(*right));
            }
            ASSERT_EQ(executor->submissions, 0);
            ASSERT_EQ(executor->shutdowns, 0);
        }
    }
}

TEST(TableScanTest, CancellationPreservesUnawareBucketPlans) {
    const auto path = GetDataDir() +
                      "/parquet/append_with_bitmap_no_embedding.db/append_with_bitmap_no_embedding";
    ScanContextBuilder baseline_builder(path);
    ScanContextBuilder controlled_builder(path);
    auto token = std::make_shared<ScanCancellationToken>();
    auto executor = std::make_shared<RecordingPlanningExecutor>();
    controlled_builder.WithCancellationToken(token).WithExecutor(executor);
    ASSERT_OK_AND_ASSIGN(auto baseline, FinishPlanningScan(baseline_builder));
    ASSERT_OK_AND_ASSIGN(auto controlled, FinishPlanningScan(controlled_builder));
    ASSERT_OK_AND_ASSIGN(auto expected, baseline->CreatePlan());
    ASSERT_OK_AND_ASSIGN(auto actual, controlled->CreatePlan());
    ASSERT_EQ(actual->SnapshotId(), std::optional<int64_t>(1));
    ASSERT_OK_AND_ASSIGN(auto rows, PlanningRowCount(actual));
    ASSERT_EQ(rows, 8);
    ASSERT_EQ(actual->Splits().size(), expected->Splits().size());
    for (size_t index = 0; index < actual->Splits().size(); ++index) {
        auto left = std::dynamic_pointer_cast<DataSplitImpl>(expected->Splits()[index]);
        auto right = std::dynamic_pointer_cast<DataSplitImpl>(actual->Splits()[index]);
        ASSERT_TRUE(left && right);
        ASSERT_TRUE(left->TEST_Equal(*right));
    }
    ASSERT_FALSE(token->IsCancelled());
    ASSERT_EQ(executor->submissions, 0);
    ASSERT_EQ(executor->shutdowns, 0);
}

TEST(TableScanTest, CancellationAfterContextConsumptionIsTerminalAndRetained) {
    const auto path = GetDataDir() + "/parquet/append_09.db/append_09";
    auto token = std::make_shared<ScanCancellationToken>();
    std::weak_ptr<ScanCancellationToken> retained = token;
    ScanContextBuilder builder(path);
    builder.WithCancellationToken(token);
    ASSERT_OK_AND_ASSIGN(auto scan, FinishPlanningScan(builder));
    token.reset();
    ASSERT_FALSE(retained.expired());
    retained.lock()->Cancel();
    ASSERT_TRUE(scan->CreatePlan().status().IsCancelled());
    ASSERT_TRUE(scan->CreatePlan().status().IsCancelled());
    scan.reset();
    ASSERT_TRUE(retained.expired());
    ASSERT_OK_AND_ASSIGN(auto next_scan, FinishPlanningScan(builder));
    ASSERT_OK_AND_ASSIGN(auto next_plan, next_scan->CreatePlan());
    ASSERT_EQ(next_plan->SnapshotId(), std::optional<int64_t>(5));
}

TEST(TableScanTest, CancellationInsideManifestFilterStopsTheRealPlan) {
    const auto path = GetDataDir() + "/parquet/append_09.db/append_09";
    for (bool native_failure : {false, true}) {
        SCOPED_TRACE(native_failure);
        auto token = std::make_shared<ScanCancellationToken>();
        auto fs =
            std::make_shared<CancellingPlanningFileSystem>(token.get(), CancelMetadataAt::None);
        auto executor = std::make_shared<RecordingPlanningExecutor>();
        CancellingStatsFunction function(token.get(), native_failure);
        auto predicate = std::make_shared<LeafPredicateImpl>(function, 3, "f3", FieldType::DOUBLE,
                                                             std::vector<Literal>{Literal(0.0)});
        ScanContextBuilder builder(path);
        builder.WithCancellationToken(token).WithFileSystem(fs).WithExecutor(executor).SetPredicate(
            predicate);
        ASSERT_OK_AND_ASSIGN(auto scan, FinishPlanningScan(builder));
        auto result = scan->CreatePlan();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(result.status().IsIOError(), native_failure) << result.status().ToString();
        ASSERT_EQ(result.status().IsCancelled(), !native_failure) << result.status().ToString();
        ASSERT_TRUE(token->IsCancelled());
        ASSERT_EQ(function.visits, 1);
        ASSERT_EQ(fs->after_cancel, 0);
        ASSERT_EQ(executor->submissions, 0);
        ASSERT_EQ(executor->shutdowns, 0);
    }
}

TEST(TableScanTest, CancellationAtFactoryAndMetadataReturnsNoPlanOrFurtherIo) {
    const auto path = GetDataDir() + "/parquet/append_09.db/append_09";
    for (auto phase : {CancelMetadataAt::None, CancelMetadataAt::Schema, CancelMetadataAt::Hint,
                       CancelMetadataAt::Snapshot, CancelMetadataAt::ManifestListOpen,
                       CancelMetadataAt::ManifestOpen, CancelMetadataAt::ManifestRead,
                       CancelMetadataAt::Listing}) {
        SCOPED_TRACE(static_cast<int32_t>(phase));
        auto token = std::make_shared<ScanCancellationToken>();
        auto fs = std::make_shared<CancellingPlanningFileSystem>(token.get(), phase);
        auto executor = std::make_shared<RecordingPlanningExecutor>();
        ScanContextBuilder builder(path);
        builder.WithCancellationToken(token).WithFileSystem(fs).WithExecutor(executor);
        ASSERT_OK_AND_ASSIGN(auto context, builder.Finish());
        if (phase == CancelMetadataAt::None) {
            token->Cancel();
        }
        auto result = TableScan::Create(std::move(context));
        if (result.ok()) {
            ASSERT_TRUE(result.value()->CreatePlan().status().IsCancelled());
            ASSERT_TRUE(result.value()->CreatePlan().status().IsCancelled());
        } else {
            ASSERT_TRUE(result.status().IsCancelled());
        }
        ASSERT_TRUE(token->IsCancelled());
        ASSERT_EQ(fs->after_cancel, 0);
        ASSERT_EQ(fs->streams->live, 0);
        ASSERT_EQ(fs->streams->unclosed, 0);
        if (phase == CancelMetadataAt::None) {
            ASSERT_EQ(fs->operations, 0);
        } else {
            ASSERT_GT(fs->operations, 0);
        }
        if (phase == CancelMetadataAt::ManifestListOpen) {
            ASSERT_GT(fs->manifest_list_opens, 0);
            ASSERT_EQ(fs->manifest_file_opens, 0);
        } else if (phase == CancelMetadataAt::ManifestOpen) {
            ASSERT_GT(fs->manifest_file_opens, 0);
        }
        ASSERT_EQ(executor->submissions, 0);
        ASSERT_EQ(executor->shutdowns, 0);
    }
}

TEST(TableScanTest, CancellationDuringFailedHintPreservesOriginalError) {
    const auto path = GetDataDir() + "/parquet/append_09.db/append_09";
    for (const auto& mode : {"latest", "latest-full"}) {
        SCOPED_TRACE(mode);
        auto token = std::make_shared<ScanCancellationToken>();
        auto fs =
            std::make_shared<CancellingPlanningFileSystem>(token.get(), CancelMetadataAt::Hint);
        fs->hint_status = Status::IOError("original failed planning hint");
        ScanContextBuilder builder(path);
        builder.WithCancellationToken(token).WithFileSystem(fs).AddOption(Options::SCAN_MODE, mode);
        ASSERT_OK_AND_ASSIGN(auto scan, FinishPlanningScan(builder));
        ASSERT_FALSE(token->IsCancelled());
        auto result = scan->CreatePlan();
        ASSERT_TRUE(token->IsCancelled());
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().IsIOError());
        ASSERT_NOK_WITH_MSG(result, fs->hint_status.message());
        ASSERT_EQ(fs->after_cancel, 0);
        ASSERT_EQ(fs->manifest_list_opens, 0);
        ASSERT_EQ(fs->manifest_file_opens, 0);
    }
}

TEST(TableScanTest, CancellationPreservesPrimaryFailureWhenCleanupAlsoFails) {
    const auto path = GetDataDir() + "/parquet/append_09.db/append_09";
    for (bool cancel : {false, true}) {
        SCOPED_TRACE(cancel);
        auto token = std::make_shared<ScanCancellationToken>();
        auto fs = std::make_shared<CancellingPlanningFileSystem>(
            token.get(), cancel ? CancelMetadataAt::ManifestOpen : CancelMetadataAt::None);
        fs->fail_manifest_close = true;
        ScanContextBuilder builder(path);
        builder.WithCancellationToken(token).WithFileSystem(fs);
        ASSERT_OK_AND_ASSIGN(auto scan, FinishPlanningScan(builder));
        auto result = scan->CreatePlan();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(token->IsCancelled(), cancel);
        ASSERT_EQ(result.status().IsCancelled(), cancel) << result.status().ToString();
        ASSERT_EQ(result.status().IsIOError(), !cancel) << result.status().ToString();
        ASSERT_GT(fs->manifest_list_opens + fs->manifest_file_opens, 0);
        ASSERT_GT(fs->streams->closes, 0);
        ASSERT_EQ(fs->streams->live, 0);
        ASSERT_EQ(fs->streams->unclosed, 0);
    }
}

TEST(TableScanTest, CancellationRefusesUnsupportedMetadataAndModesBeforeManifestWork) {
    for (const auto& relative :
         {"/orc/append_09.db/append_09",
          "/parquet/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality"}) {
        auto token = std::make_shared<ScanCancellationToken>();
        auto fs =
            std::make_shared<CancellingPlanningFileSystem>(token.get(), CancelMetadataAt::None);
        ScanContextBuilder builder(GetDataDir() + relative);
        builder.WithCancellationToken(token).WithFileSystem(fs);
        auto result = FinishPlanningScan(builder);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().IsNotImplemented()) << result.status().ToString();
        ASSERT_EQ(fs->manifest_list_opens, 0);
        ASSERT_EQ(fs->manifest_file_opens, 0);
    }
    auto token = std::make_shared<ScanCancellationToken>();
    auto fs = std::make_shared<CancellingPlanningFileSystem>(token.get(), CancelMetadataAt::None);
    ScanContextBuilder builder(GetDataDir() + "/parquet/append_09.db/append_09");
    builder.WithCancellationToken(token).WithFileSystem(fs).WithStreamingMode(true);
    auto result = FinishPlanningScan(builder);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsNotImplemented());
    ASSERT_EQ(fs->operations, 0);
}

TEST(TableScanTest, CancellationRefusesIndependentUnsupportedOptions) {
    const auto path = GetDataDir() + "/parquet/append_09.db/append_09";
    const std::vector<std::pair<std::string, std::string>> unsupported = {
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
        {Options::BUCKET, "0"},
        {Options::BUCKET, "-2"},
        {Options::SCAN_TIMESTAMP_MILLIS, "1"},
        {Options::SCAN_TAG_NAME, "unused-tag"}};
    for (const auto& [key, value] : unsupported) {
        SCOPED_TRACE(key + "=" + value);
        auto token = std::make_shared<ScanCancellationToken>();
        auto fs =
            std::make_shared<CancellingPlanningFileSystem>(token.get(), CancelMetadataAt::None);
        ScanContextBuilder builder(path);
        builder.WithCancellationToken(token).WithFileSystem(fs).AddOption(key, value);
        auto result = FinishPlanningScan(builder);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().IsNotImplemented()) << result.status().ToString();
        ASSERT_FALSE(token->IsCancelled());
        ASSERT_EQ(fs->manifest_list_opens, 0);
        ASSERT_EQ(fs->manifest_file_opens, 0);
    }
}

TEST(TableScanTest, CancellationRefusesActualEmbeddedIndexEvaluation) {
    const auto path = GetDataDir() + "/parquet/append_with_bitmap_v1.db/append_with_bitmap_v1";
    auto predicate = std::make_shared<LeafPredicateImpl>(
        GreaterOrEqual::Instance(), 1, "f1", FieldType::INT, std::vector<Literal>{Literal(15)});
    ScanContextBuilder baseline_builder(path);
    baseline_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto baseline, FinishPlanningScan(baseline_builder));
    ASSERT_OK_AND_ASSIGN(auto expected, baseline->CreatePlan());
    ASSERT_FALSE(expected->Splits().empty());
    auto token = std::make_shared<ScanCancellationToken>();
    auto executor = std::make_shared<RecordingPlanningExecutor>();
    ScanContextBuilder controlled_builder(path);
    controlled_builder.SetPredicate(predicate).WithCancellationToken(token).WithExecutor(executor);
    ASSERT_OK_AND_ASSIGN(auto controlled, FinishPlanningScan(controlled_builder));
    auto result = controlled->CreatePlan();
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsNotImplemented()) << result.status().ToString();
    ASSERT_NE(result.status().ToString().find("embedded file-index evaluation"), std::string::npos);
    ASSERT_FALSE(token->IsCancelled());
    ASSERT_EQ(executor->submissions, 0);
    ASSERT_EQ(executor->shutdowns, 0);
}

TEST(TableScanTest, CancellationDoesNotBecomeAnEmptySuccessfulPlan) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    const auto path = directory->Str() + "/table";
    std::filesystem::copy(GetDataDir() + "/parquet/append_09.db/append_09", path,
                          std::filesystem::copy_options::recursive);
    std::filesystem::remove_all(path + "/snapshot");
    for (bool before_first_plan : {false, true}) {
        SCOPED_TRACE(before_first_plan);
        auto token = std::make_shared<ScanCancellationToken>();
        auto fs =
            std::make_shared<CancellingPlanningFileSystem>(token.get(), CancelMetadataAt::None);
        ScanContextBuilder builder(path);
        builder.WithCancellationToken(token).WithFileSystem(fs);
        ASSERT_OK_AND_ASSIGN(auto scan, FinishPlanningScan(builder));
        if (!before_first_plan) {
            ASSERT_OK_AND_ASSIGN(auto empty, scan->CreatePlan());
            ASSERT_FALSE(empty->SnapshotId());
            ASSERT_TRUE(empty->Splits().empty());
        }
        const int32_t operations = fs->operations;
        token->Cancel();
        ASSERT_TRUE(scan->CreatePlan().status().IsCancelled());
        ASSERT_TRUE(scan->CreatePlan().status().IsCancelled());
        ASSERT_EQ(fs->operations, operations);
        ASSERT_EQ(fs->after_cancel, 0);
    }
    auto token = std::make_shared<ScanCancellationToken>();
    auto fs = std::make_shared<CancellingPlanningFileSystem>(token.get(),
                                                             CancelMetadataAt::SnapshotListing);
    ScanContextBuilder builder(path);
    builder.WithCancellationToken(token).WithFileSystem(fs);
    ASSERT_OK_AND_ASSIGN(auto scan, FinishPlanningScan(builder));
    ASSERT_TRUE(scan->CreatePlan().status().IsCancelled());
    ASSERT_TRUE(token->IsCancelled());
    ASSERT_EQ(fs->after_cancel, 0);
}

TEST(TableScanTest, CancellationBypassesColdAndWarmSharedCachesWithoutPoisoningThem) {
    const auto path = GetDataDir() + "/parquet/append_09.db/append_09";
    for (bool warm : {false, true}) {
        SCOPED_TRACE(warm);
        auto cache = std::make_shared<PlanningCountingCache>();
        auto run = [&](const std::shared_ptr<ScanCancellationToken>& token,
                       const std::shared_ptr<FileSystem>& fs =
                           nullptr) -> Result<std::shared_ptr<Plan>> {
            ScanContextBuilder builder(path);
            if (fs) {
                builder.WithFileSystem(fs);
            }
            builder.WithCache(cache)
                .SetBucketFilter(0)
                .WithCancellationToken(token)
                .AddOption(Options::SCAN_SNAPSHOT_ID, "5")
                .AddOption(Options::SCAN_MANIFEST_ENTRY_CACHE_MAX_SNAPSHOTS, "3");
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan, FinishPlanningScan(builder));
            return scan->CreatePlan();
        };
        if (warm) {
            ASSERT_OK_AND_ASSIGN(auto baseline, run(nullptr));
            ASSERT_FALSE(baseline->Splits().empty());
            ASSERT_GT(cache->GetCount(CacheKind::MANIFEST), 0);
            ASSERT_GT(cache->GetCount(CacheKind::SNAPSHOT_LIVE_MANIFEST), 0);
        }
        const auto gets = cache->GetCount();
        const auto suppliers = cache->SupplierCallCount();
        const auto puts = cache->puts.load();
        const auto size = cache->Size();
        auto token = std::make_shared<ScanCancellationToken>();
        ASSERT_OK_AND_ASSIGN(auto actual, run(token));
        ASSERT_FALSE(actual->Splits().empty());
        ASSERT_EQ(cache->GetCount(), gets);
        ASSERT_EQ(cache->SupplierCallCount(), suppliers);
        ASSERT_EQ(cache->puts.load(), puts);
        ASSERT_EQ(cache->Size(), size);
        token->Cancel();
        ASSERT_TRUE(run(token).status().IsCancelled());
        ASSERT_EQ(cache->GetCount(), gets);
        ASSERT_EQ(cache->puts.load(), puts);
        auto mid_read = std::make_shared<ScanCancellationToken>();
        std::weak_ptr<ScanCancellationToken> weak_mid_read = mid_read;
        {
            auto fs = std::make_shared<CancellingPlanningFileSystem>(
                mid_read.get(), CancelMetadataAt::ManifestRead);
            auto cancelled = run(mid_read, fs);
            ASSERT_TRUE(mid_read->IsCancelled());
            ASSERT_FALSE(cancelled.ok());
            ASSERT_TRUE(cancelled.status().IsCancelled()) << cancelled.status().ToString();
            ASSERT_GT(fs->streams->reads, 0);
            ASSERT_EQ(fs->streams->live, 0);
            ASSERT_EQ(fs->streams->unclosed, 0);
            ASSERT_EQ(cache->GetCount(), gets);
            ASSERT_EQ(cache->SupplierCallCount(), suppliers);
            ASSERT_EQ(cache->puts.load(), puts);
            ASSERT_EQ(cache->Size(), size);
        }
        mid_read.reset();
        ASSERT_TRUE(weak_mid_read.expired());
        ASSERT_OK_AND_ASSIGN(auto independent, run(nullptr));
        ASSERT_EQ(independent->SnapshotId(), actual->SnapshotId());
        ASSERT_OK_AND_ASSIGN(auto expected_rows, PlanningRowCount(independent));
        ASSERT_OK_AND_ASSIGN(auto actual_rows, PlanningRowCount(actual));
        ASSERT_EQ(actual_rows, expected_rows);
        ASSERT_GT(cache->GetCount(), gets);
    }
}

TEST(TableScanTest, CancellationFromAnotherThreadWhileManifestIoIsPaused) {
    const auto path = GetDataDir() + "/parquet/append_09.db/append_09";
    std::promise<void> entered;
    std::promise<void> resume;
    auto waiting_for_io = entered.get_future();
    auto resume_io = resume.get_future().share();
    auto token = std::make_shared<ScanCancellationToken>();
    auto fs = std::make_shared<CancellingPlanningFileSystem>(token.get(), CancelMetadataAt::None);
    auto executor = std::make_shared<RecordingPlanningExecutor>();
    fs->pause_manifest_open = [&]() {
        entered.set_value();
        resume_io.wait();
    };
    ScanContextBuilder builder(path);
    builder.WithCancellationToken(token).WithFileSystem(fs).WithExecutor(executor);
    ASSERT_OK_AND_ASSIGN(auto scan, FinishPlanningScan(builder));
    auto planning = std::async(std::launch::async, [&]() { return scan->CreatePlan(); });
    ScopeGuard unblock_before_join([&]() { resume.set_value(); });
    ASSERT_EQ(waiting_for_io.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(fs->manifest_file_opens, 1);
    ASSERT_FALSE(token->IsCancelled());
    const auto reads_before_cancel = fs->streams->reads;
    token->Cancel();
    ASSERT_EQ(planning.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    resume.set_value();
    unblock_before_join.Release();
    auto result = planning.get();
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsCancelled()) << result.status().ToString();
    ASSERT_EQ(fs->streams->reads, reads_before_cancel);
    ASSERT_EQ(fs->after_cancel, 0);
    ASSERT_EQ(fs->streams->live, 0);
    ASSERT_EQ(fs->streams->unclosed, 0);
    ASSERT_EQ(executor->submissions, 0);
    ASSERT_EQ(executor->shutdowns, 0);
}

TEST(TableScanTest, CancellationOfOneConcurrentScanDoesNotCancelAnother) {
    const auto path = GetDataDir() + "/parquet/append_09.db/append_09";
    auto cancelled_token = std::make_shared<ScanCancellationToken>();
    auto independent_token = std::make_shared<ScanCancellationToken>();
    auto fs = std::make_shared<CancellingPlanningFileSystem>(cancelled_token.get(),
                                                             CancelMetadataAt::ManifestRead);
    ScanContextBuilder cancelled_builder(path);
    ScanContextBuilder independent_builder(path);
    cancelled_builder.WithCancellationToken(cancelled_token).WithFileSystem(fs);
    independent_builder.WithCancellationToken(independent_token);
    ASSERT_OK_AND_ASSIGN(auto cancelled_scan, FinishPlanningScan(cancelled_builder));
    ASSERT_OK_AND_ASSIGN(auto independent_scan, FinishPlanningScan(independent_builder));
    std::promise<void> start;
    auto ready = start.get_future().share();
    auto cancelled_future = std::async(std::launch::async, [&]() {
        ready.wait();
        return cancelled_scan->CreatePlan();
    });
    auto independent_future = std::async(std::launch::async, [&]() {
        ready.wait();
        return independent_scan->CreatePlan();
    });
    start.set_value();
    auto cancelled = cancelled_future.get();
    auto independent = independent_future.get();
    ASSERT_TRUE(cancelled_token->IsCancelled());
    ASSERT_FALSE(cancelled.ok());
    ASSERT_TRUE(cancelled.status().IsCancelled());
    ASSERT_FALSE(independent_token->IsCancelled());
    ASSERT_OK_AND_ASSIGN(auto plan, std::move(independent));
    ASSERT_EQ(plan->SnapshotId(), std::optional<int64_t>(5));
    ASSERT_OK_AND_ASSIGN(auto rows, PlanningRowCount(plan));
    ASSERT_EQ(rows, 11);
    cancelled_scan.reset();
    ASSERT_EQ(fs->streams->live, 0);
    ASSERT_EQ(fs->streams->unclosed, 0);
}

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

TEST(TableScanTest, TestStreamingSnapshotsRemoved) {
    auto dir = UniqueTestDirectory::Create("local");
    ASSERT_TRUE(dir);
    ASSERT_TRUE(TestUtil::CopyDirectory(GetDataDir() + "/orc/append_09.db/append_09", dir->Str()));
    ScanContextBuilder builder(dir->Str());
    builder.AddOption(Options::FILE_FORMAT, "orc")
        .AddOption(Options::SCAN_MODE, "latest")
        .WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> initial_plan, table_scan->CreatePlan());
    ASSERT_TRUE(initial_plan->Splits().empty());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> waiting_plan, table_scan->CreatePlan());
    ASSERT_TRUE(waiting_plan->Splits().empty());

    LocalFileSystem fs;
    ASSERT_OK(fs.Delete(dir->Str() + "/snapshot", /*recursive=*/true));
    ASSERT_NOK_WITH_MSG(table_scan->CreatePlan(),
                        "The next snapshot id is 6, while the latest snapshot id is null");
}

TEST(TableScanTest, TestStreamingNextSnapshotBoundary) {
    std::string path = GetDataDir() + "/orc/append_09.db/append_09";
    for (const std::string next_snapshot_id : {"6", "7"}) {
        SCOPED_TRACE(next_snapshot_id);
        ScanContextBuilder builder(path);
        builder.AddOption(Options::FILE_FORMAT, "orc")
            .AddOption(Options::SCAN_MODE, "from-snapshot")
            .AddOption(Options::SCAN_SNAPSHOT_ID, next_snapshot_id)
            .WithStreamingMode(true);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                             TableScan::Create(std::move(context)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> initial_plan, table_scan->CreatePlan());
        ASSERT_TRUE(initial_plan->Splits().empty());

        auto result = table_scan->CreatePlan();
        if (next_snapshot_id == "6") {
            ASSERT_OK(result);
            ASSERT_TRUE(result.value()->Splits().empty());
            ASSERT_FALSE(result.value()->SnapshotId());
        } else {
            ASSERT_FALSE(result.ok());
            ASSERT_TRUE(result.status().IsInvalid());
            ASSERT_NOK_WITH_MSG(result,
                                "The next snapshot id is 7, while the latest snapshot id is 5");
        }
    }
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
