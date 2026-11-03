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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <future>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>

#include "arrow/memory_pool.h"
#include "gtest/gtest.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/executor.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
class InlinePlanningExecutor final : public Executor {
 public:
    void Add(std::function<void()> task) override {
        task();
    }
    void ShutdownNow() override {}
    uint32_t GetThreadNum() const override {
        return 1;
    }
};

class PlanningFaultPool final : public MemoryPool {
 public:
    void* Malloc(uint64_t size, uint64_t alignment) override {
        std::lock_guard<std::mutex> lock(mutex_);
        Check();
        ++malloc_calls;
        return upstream_->Malloc(size, alignment);
    }
    void* Realloc(void* pointer, size_t old_size, size_t new_size, uint64_t alignment) override {
        std::lock_guard<std::mutex> lock(mutex_);
        Check();
        ++realloc_calls;
        return upstream_->Realloc(pointer, old_size, new_size, alignment);
    }
    void Free(void* pointer, uint64_t size) override {
        std::lock_guard<std::mutex> lock(mutex_);
        upstream_->Free(pointer, size);
    }
    void Free(void* pointer, uint64_t size, uint64_t alignment) override {
        std::lock_guard<std::mutex> lock(mutex_);
        upstream_->Free(pointer, size, alignment);
    }
    uint64_t CurrentUsage() const override {
        return upstream_->CurrentUsage();
    }
    uint64_t MaxMemoryUsage() const override {
        return upstream_->MaxMemoryUsage();
    }
    std::atomic<uint64_t> calls{0};
    std::atomic<uint64_t> fail_at{std::numeric_limits<uint64_t>::max()};
    std::atomic<uint64_t> malloc_calls{0};
    std::atomic<uint64_t> realloc_calls{0};
    std::atomic<bool> rejected{false};

 private:
    void Check() {
        if (calls.fetch_add(1) == fail_at.load()) {
            rejected = true;
            throw std::bad_alloc();
        }
    }
    std::unique_ptr<MemoryPool> upstream_ = GetMemoryPool();
    std::mutex mutex_;
};

std::string PlanningFixture() {
    return GetDataDir() + "/parquet/append_types_compatibility.db/rust_time_types";
}

struct PlanningInput {
    std::string path = PlanningFixture();
    std::string snapshot = "1";
    std::shared_ptr<Cache> cache;
    std::optional<int32_t> bucket;
    std::shared_ptr<FileSystem> file_system;
};

Result<std::unique_ptr<TableScan>> OrdinaryScan(
    const std::shared_ptr<PlanningFaultPool>& pool,
    const std::shared_ptr<Executor>& executor = std::make_shared<InlinePlanningExecutor>(),
    const PlanningInput& input = {}) {
    std::ifstream schema_file(input.path + "/schema/schema-0");
    std::string schema{std::istreambuf_iterator<char>(schema_file),
                       std::istreambuf_iterator<char>()};
    ScanContextBuilder builder(input.path);
    builder.SetTableSchema(schema).WithMemoryPool(pool).WithExecutor(executor).AddOption(
        "scan.snapshot-id", input.snapshot);
    if (input.cache) {
        builder.WithCache(input.cache)
            .AddOption("scan.manifest-entry-cache.max-snapshots", "8")
            .AddOption("scan.manifest-entry.lazy-decode.enabled", "true");
    }
    if (input.bucket) builder.SetBucketFilter(*input.bucket);
    if (input.file_system) builder.WithFileSystem(input.file_system);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> context, builder.Finish());
    return TableScan::Create(std::move(context));
}

class RejectingPlanningExecutor final : public Executor {
 public:
    explicit RejectingPlanningExecutor(uint32_t rejected_submission = 2,
                                       uint32_t blocked_submission = 0)
        : rejected_submission_(rejected_submission),
          blocked_submission_(blocked_submission ? blocked_submission : rejected_submission - 1) {}
    ~RejectingPlanningExecutor() override {
        Release(false);
        worker_.reset();
    }
    void Add(std::function<void()> task) override {
        uint32_t submission = ++submissions_;
        if (submission == rejected_submission_) {
            rejected_ = true;
            throw std::bad_alloc();
        }
        if (submission != blocked_submission_) {
            worker_->Add([this, submission, task = std::move(task)] {
                task();
                if (submission == 1) {
                    std::lock_guard<std::mutex> lock(mutex_);
                    first_completed_ = true;
                    changed_.notify_all();
                }
            });
            return;
        }
        worker_->Add([this, task = std::move(task)] {
            bool execute;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                blocked_ = true;
                changed_.notify_all();
                changed_.wait(lock, [this] { return released_; });
                execute = execute_;
            }
            if (execute) task();
        });
    }
    void ShutdownNow() override {
        worker_->ShutdownNow();
    }
    uint32_t GetThreadNum() const override {
        return 2;
    }
    bool WaitUntilBlocked() {
        std::unique_lock<std::mutex> lock(mutex_);
        return changed_.wait_for(lock, std::chrono::seconds(5), [this] { return blocked_; });
    }
    void Release(bool execute) {
        std::lock_guard<std::mutex> lock(mutex_);
        execute_ = execute;
        released_ = true;
        changed_.notify_all();
    }
    bool Rejected() const {
        return rejected_;
    }
    bool WaitUntilFirstCompleted() {
        std::unique_lock<std::mutex> lock(mutex_);
        return changed_.wait_for(lock, std::chrono::seconds(5),
                                 [this] { return first_completed_; });
    }

 private:
    std::unique_ptr<Executor> worker_ = CreateDefaultExecutor(2).value();
    std::atomic<uint32_t> submissions_{0};
    uint32_t rejected_submission_;
    uint32_t blocked_submission_;
    std::atomic<bool> rejected_{false};
    std::mutex mutex_;
    std::condition_variable changed_;
    bool blocked_ = false;
    bool released_ = false;
    bool execute_ = false;
    bool first_completed_ = false;
};

class PoolRefusingManifestFileSystem final : public LocalFileSystem {
 public:
    explicit PoolRefusingManifestFileSystem(std::shared_ptr<PlanningFaultPool> pool)
        : pool_(std::move(pool)) {}
    using LocalFileSystem::Open;
    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        if (path.find("/manifest/") != std::string::npos && !refused_.exchange(true)) {
            pool_->fail_at = pool_->calls.load();
            // A supplied file system may allocate through the caller's pool too.
            // Force refusal inside the real manifest worker, before opening a file.
            void* allocation = pool_->Malloc(1, 64);
            pool_->Free(allocation, 1);
        }
        return LocalFileSystem::Open(path);
    }

 private:
    std::shared_ptr<PlanningFaultPool> pool_;
    mutable std::atomic<bool> refused_{false};
};
}  // namespace

TEST(ScanExceptionSafetyTest, RejectEachPlanningAllocationWithoutLeaking) {
    auto baseline_pool = std::make_shared<PlanningFaultPool>();
    ASSERT_OK_AND_ASSIGN(auto baseline_scan, OrdinaryScan(baseline_pool));
    RecordProperty("factory_pool_calls", baseline_pool->calls.load());
    baseline_pool->calls = 0;
    baseline_pool->malloc_calls = 0;
    baseline_pool->realloc_calls = 0;
    ASSERT_OK_AND_ASSIGN(auto baseline, baseline_scan->CreatePlan());
    ASSERT_FALSE(baseline->Splits().empty());
    const uint64_t allocation_count = baseline_pool->calls;
    ASSERT_GT(allocation_count, 0);
    RecordProperty("planning_malloc_calls", baseline_pool->malloc_calls.load());
    RecordProperty("planning_realloc_calls", baseline_pool->realloc_calls.load());
    baseline.reset();
    baseline_scan.reset();
    ASSERT_EQ(baseline_pool->CurrentUsage(), 0);

    for (uint64_t ordinal = 0; ordinal < allocation_count; ++ordinal) {
        SCOPED_TRACE(ordinal);
        auto pool = std::make_shared<PlanningFaultPool>();
        ASSERT_OK_AND_ASSIGN(auto scan, OrdinaryScan(pool));
        pool->calls = 0;
        pool->fail_at = ordinal;
        bool failed = false;
        try {
            auto plan = scan->CreatePlan();
            failed = !plan.ok();
        } catch (const std::bad_alloc&) {
            failed = true;
        }
        ASSERT_TRUE(pool->rejected);
        ASSERT_TRUE(failed) << "allocation refusal returned a successful plan";
        scan.reset();
        EXPECT_EQ(pool->CurrentUsage(), 0) << "pool storage survived failed planning";
        pool->fail_at = std::numeric_limits<uint64_t>::max();
        {
            ASSERT_OK_AND_ASSIGN(auto recovery, OrdinaryScan(pool));
            ASSERT_OK_AND_ASSIGN(auto plan, recovery->CreatePlan());
            EXPECT_FALSE(plan->Splits().empty());
        }
        EXPECT_EQ(pool->CurrentUsage(), 0);
    }
}

TEST(ScanExceptionSafetyTest, RetainedSplitRemainsUsableWithCallerOwnedPool) {
    auto pool = std::make_shared<PlanningFaultPool>();
    ASSERT_OK_AND_ASSIGN(auto scan, OrdinaryScan(pool));
    ASSERT_OK_AND_ASSIGN(auto plan, scan->CreatePlan());
    ASSERT_FALSE(plan->Splits().empty());
    auto split = plan->Splits().front();
    ASSERT_OK_AND_ASSIGN(auto original, Split::Serialize(split, pool));
    scan.reset();
    EXPECT_EQ(plan->SnapshotId(), 1);
    plan.reset();
    ASSERT_OK_AND_ASSIGN(auto retained, Split::Serialize(split, pool));
    EXPECT_EQ(original, retained);
    split.reset();
    EXPECT_EQ(pool->CurrentUsage(), 0);
}

TEST(ScanExceptionSafetyTest, RefusedArrowReallocationPreservesExistingBuffer) {
    auto pool = std::make_shared<PlanningFaultPool>();
    auto arrow_pool = GetArrowPool(pool);
    uint8_t* data = nullptr;
    ASSERT_OK(ToPaimonStatus(arrow_pool->Allocate(16, 64, &data)));
    ASSERT_NE(data, nullptr);
    data[0] = 42;
    uint8_t* original = data;
    pool->fail_at = pool->calls.load();
    auto status = arrow_pool->Reallocate(16, 32, 64, &data);
    EXPECT_NOK(ToPaimonStatus(status));
    EXPECT_TRUE(status.IsOutOfMemory());
    EXPECT_TRUE(pool->rejected);
    EXPECT_EQ(data, original);
    EXPECT_EQ(data[0], 42);
    EXPECT_EQ(pool->CurrentUsage(), 16);
    arrow_pool->Free(data, 16, 64);
    EXPECT_EQ(pool->CurrentUsage(), 0);
}

TEST(ScanExceptionSafetyTest, SubmissionFailureDrainsAcceptedManifestTask) {
    for (uint32_t phase = 0; phase < 3; ++phase) {
        SCOPED_TRACE(phase);
        PlanningInput input;
        uint32_t rejected_submission = 2;
        if (phase > 0) {
            input.path = GetDataDir() + "/orc/append_09.db/append_09";
            input.snapshot = "5";
            rejected_submission = 4;
        }
        if (phase == 2) {
            input.cache = std::make_shared<LruCache>(16 * 1024 * 1024);
            input.bucket = 0;
        }
        auto pool = std::make_shared<PlanningFaultPool>();
        auto executor = std::make_shared<RejectingPlanningExecutor>(rejected_submission);
        ASSERT_OK_AND_ASSIGN(auto scan, OrdinaryScan(pool, executor, input));
        auto planning = std::async(std::launch::async, [&scan] {
            try {
                auto plan = scan->CreatePlan();
                return !plan.ok();
            } catch (const std::bad_alloc&) {
                return true;
            }
        });
        bool blocked = executor->WaitUntilBlocked();
        bool returned_early =
            planning.wait_for(std::chrono::milliseconds(200)) == std::future_status::ready;
        // On the faulty path the task borrows an already unwound snapshot. Drop it
        // without dereferencing that snapshot so the regression reports the ordering.
        executor->Release(!returned_early);
        EXPECT_TRUE(blocked);
        EXPECT_TRUE(executor->Rejected());
        EXPECT_FALSE(returned_early)
            << "CreatePlan returned with accepted manifest work still pending";
        EXPECT_TRUE(planning.get());
        scan.reset();
        executor.reset();
        input.cache.reset();
        EXPECT_EQ(pool->CurrentUsage(), 0);
    }
}

TEST(ScanExceptionSafetyTest, WorkerPoolExceptionDrainsAcceptedManifestTask) {
    auto pool = std::make_shared<PlanningFaultPool>();
    auto executor = std::make_shared<RejectingPlanningExecutor>(0, 2);
    PlanningInput input;
    input.file_system = std::make_shared<PoolRefusingManifestFileSystem>(pool);
    ASSERT_OK_AND_ASSIGN(auto scan, OrdinaryScan(pool, executor, input));
    auto planning = std::async(std::launch::async, [&scan] {
        try {
            (void)scan->CreatePlan();
            return false;
        } catch (const std::bad_alloc&) {
            return true;
        }
    });
    bool blocked = executor->WaitUntilBlocked();
    bool first_completed = executor->WaitUntilFirstCompleted();
    bool returned_early =
        planning.wait_for(std::chrono::milliseconds(200)) == std::future_status::ready;
    executor->Release(!returned_early);
    EXPECT_TRUE(blocked);
    EXPECT_TRUE(first_completed);
    EXPECT_TRUE(pool->rejected);
    EXPECT_FALSE(returned_early) << "worker exception unwound before the other manifest task";
    EXPECT_TRUE(planning.get());
    scan.reset();
    executor.reset();
    input.file_system.reset();
    EXPECT_EQ(pool->CurrentUsage(), 0);
}
}  // namespace paimon::test
