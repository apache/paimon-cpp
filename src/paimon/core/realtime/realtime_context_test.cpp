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

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/realtime/realtime_store.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class TestingReadView : public RealtimeReadView {
 public:
    std::optional<OffsetRange> GetOffsetRange() const override {
        return std::nullopt;
    }
};

class TestingRealtimeStore : public RealtimeStore {
 public:
    Status Write(RealtimeWriteBatch&&) override {
        return Status::OK();
    }

    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() override {
        return std::optional<std::shared_ptr<RealtimeSegmentHandle>>();
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>&) override {
        return std::vector<std::unique_ptr<BatchReader>>();
    }

    Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() override {
        ++acquire_count;
        return std::make_shared<TestingReadView>();
    }

    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>&, int64_t, const RealtimeQueryContext&) override {
        return std::vector<std::unique_ptr<BatchReader>>();
    }

    Status AdvanceCommittedOffset(int64_t committed_offset) override {
        ++advance_count;
        if (fail_next_advance) {
            fail_next_advance = false;
            return Status::Invalid("injected committed offset failure");
        }
        committed_offsets.push_back(committed_offset);
        return Status::OK();
    }

    uint64_t GetMemoryUsage() const override {
        return 0;
    }

    int32_t acquire_count = 0;
    int32_t advance_count = 0;
    bool fail_next_advance = false;
    std::vector<int64_t> committed_offsets;
};

class TestingRealtimeStoreFactory : public RealtimeStoreFactory {
 public:
    Result<std::shared_ptr<RealtimeStore>> Create(std::unique_ptr<ArrowSchema> write_schema,
                                                  StatisticsMode,
                                                  const std::map<std::string, std::string>&,
                                                  const std::shared_ptr<MemoryPool>&) override {
        if (!write_schema || !write_schema->release) {
            return Status::Invalid("testing write schema is null");
        }
        ArrowSchemaRelease(write_schema.get());
        auto store = std::make_shared<TestingRealtimeStore>();
        stores.push_back(store);
        return store;
    }

    std::vector<std::shared_ptr<TestingRealtimeStore>> stores;
};

std::unique_ptr<ArrowSchema> MakeWriteSchema() {
    auto c_schema = std::make_unique<ArrowSchema>();
    EXPECT_TRUE(
        arrow::ExportSchema(*arrow::schema({arrow::field("id", arrow::int64())}), c_schema.get())
            .ok());
    return c_schema;
}

Result<std::shared_ptr<RealtimeContextImpl>> CreateContext(
    const std::shared_ptr<RealtimeStoreFactory>& factory) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContext> context,
                           RealtimeContext::Create(factory));
    return RealtimeContextImpl::Cast(context);
}

TEST(RealtimeContextTest, TestReusesStoreAndCapturesRegisteredViews) {
    auto factory = std::make_shared<TestingRealtimeStoreFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> context, CreateContext(factory));
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();

    ASSERT_OK_AND_ASSIGN(
        RealtimeStoreState first_state,
        context->GetOrCreateRealtimeStore({{"dt", "2026-08-02"}}, 0, MakeWriteSchema(),
                                          StatisticsMode::NONE, {{"k", "v"}}, pool));
    ASSERT_EQ(0, first_state.initial_offset);
    ASSERT_OK_AND_ASSIGN(
        RealtimeStoreState first_again_state,
        context->GetOrCreateRealtimeStore({{"dt", "2026-08-02"}}, 0, MakeWriteSchema(),
                                          StatisticsMode::NONE, {}, pool));
    ASSERT_EQ(first_state.store, first_again_state.store);
    ASSERT_EQ(0, first_again_state.initial_offset);
    ASSERT_EQ(1, factory->stores.size());
    ASSERT_EQ(1, factory->stores[0]->acquire_count);

    ASSERT_OK_AND_ASSIGN(
        RealtimeStoreState second_state,
        context->GetOrCreateRealtimeStore({{"dt", "2026-08-02"}}, 1, MakeWriteSchema(),
                                          StatisticsMode::NONE, {}, pool));
    ASSERT_OK_AND_ASSIGN(
        RealtimeStoreState third_state,
        context->GetOrCreateRealtimeStore({{"dt", "2026-08-03"}}, 0, MakeWriteSchema(),
                                          StatisticsMode::NONE, {}, pool));
    ASSERT_NE(first_state.store, second_state.store);
    ASSERT_NE(first_state.store, third_state.store);
    ASSERT_EQ(3, factory->stores.size());

    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         context->AcquireReadViews());
    ASSERT_EQ(3, views.size());
    const RealtimePartitionBucket expected_partition_bucket({{"dt", "2026-08-02"}}, 0);
    ASSERT_EQ(expected_partition_bucket, views[0].partition_bucket);
    ASSERT_EQ(first_state.store, views[0].store);
    ASSERT_TRUE(views[0].read_view);
    ASSERT_EQ(2, factory->stores[0]->acquire_count);
    ASSERT_EQ(1, factory->stores[1]->acquire_count);
    ASSERT_EQ(1, factory->stores[2]->acquire_count);
}

TEST(RealtimeContextTest, TestCommittedProgressIsMonotonicAndSelective) {
    auto factory = std::make_shared<TestingRealtimeStoreFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> context, CreateContext(factory));
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-02"}};

    ASSERT_OK(context->GetOrCreateRealtimeStore(partition, 0, MakeWriteSchema(),
                                                StatisticsMode::NONE, {}, pool));
    ASSERT_OK(context->GetOrCreateRealtimeStore(partition, 1, MakeWriteSchema(),
                                                StatisticsMode::NONE, {}, pool));
    ASSERT_EQ(2, factory->stores.size());

    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(-1, {}),
                        "snapshot id must not be negative");
    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(
                            4, {{RealtimePartitionBucket(partition, /*bucket=*/-1), /*offset=*/3}}),
                        "invalid partition-bucket committed offset");
    ASSERT_TRUE(factory->stores[0]->committed_offsets.empty());
    ASSERT_TRUE(factory->stores[1]->committed_offsets.empty());

    ASSERT_OK(context->AdvanceCommittedProgress(
        5, {{RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/7},
            {RealtimePartitionBucket({{"dt", "unknown"}}, /*bucket=*/0), /*offset=*/9}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->stores[0]->committed_offsets);
    ASSERT_TRUE(factory->stores[1]->committed_offsets.empty());

    ASSERT_OK_AND_ASSIGN(
        RealtimeStoreState restored_state,
        context->GetOrCreateRealtimeStore({{"dt", "unknown"}}, 0, MakeWriteSchema(),
                                          StatisticsMode::NONE, {}, pool));
    ASSERT_EQ(9, restored_state.initial_offset);

    ASSERT_OK(context->AdvanceCommittedProgress(
        5, {{RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/10}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->stores[0]->committed_offsets);
    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(4, {}),
                        "committed snapshot cannot move backwards");

    ASSERT_OK(context->AdvanceCommittedProgress(
        6, {{RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/7},
            {RealtimePartitionBucket(partition, /*bucket=*/1), /*offset=*/8},
            {RealtimePartitionBucket({{"dt", "unknown"}}, /*bucket=*/0), /*offset=*/9}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->stores[0]->committed_offsets);
    ASSERT_EQ(std::vector<int64_t>({8}), factory->stores[1]->committed_offsets);
}

TEST(RealtimeContextTest, TestRemovedInactivePartitionDoesNotRequireReopen) {
    auto factory = std::make_shared<TestingRealtimeStoreFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> context, CreateContext(factory));
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    const std::map<std::string, std::string> active_partition = {{"dt", "2026-08-02"}};
    const std::map<std::string, std::string> inactive_partition = {{"dt", "2026-08-03"}};
    const RealtimePartitionBucket active_partition_bucket(active_partition, /*bucket=*/0);
    const RealtimePartitionBucket inactive_partition_bucket(inactive_partition, /*bucket=*/0);

    ASSERT_OK(context->AdvanceCommittedProgress(
        5, {{active_partition_bucket, /*offset=*/7}, {inactive_partition_bucket, /*offset=*/9}}));
    ASSERT_OK_AND_ASSIGN(RealtimeStoreState active_state,
                         context->GetOrCreateRealtimeStore(active_partition, 0, MakeWriteSchema(),
                                                           StatisticsMode::NONE, {}, pool));
    ASSERT_EQ(7, active_state.initial_offset);

    ASSERT_OK(context->AdvanceCommittedProgress(6, {{active_partition_bucket, /*offset=*/7}}));
    ASSERT_OK_AND_ASSIGN(RealtimeStoreState inactive_state,
                         context->GetOrCreateRealtimeStore(inactive_partition, 0, MakeWriteSchema(),
                                                           StatisticsMode::NONE, {}, pool));
    ASSERT_EQ(0, inactive_state.initial_offset);
}

TEST(RealtimeContextTest, TestRetriesOnlyIncompleteReclamation) {
    auto factory = std::make_shared<TestingRealtimeStoreFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> context, CreateContext(factory));
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-02"}};

    ASSERT_OK(context->GetOrCreateRealtimeStore(partition, 0, MakeWriteSchema(),
                                                StatisticsMode::NONE, {}, pool));
    ASSERT_OK(context->GetOrCreateRealtimeStore(partition, 1, MakeWriteSchema(),
                                                StatisticsMode::NONE, {}, pool));
    ASSERT_OK(context->GetOrCreateRealtimeStore(partition, 2, MakeWriteSchema(),
                                                StatisticsMode::NONE, {}, pool));
    ASSERT_EQ(3, factory->stores.size());
    factory->stores[1]->fail_next_advance = true;

    const RealtimeOffsetMap committed_offsets = {
        {RealtimePartitionBucket(partition, /*bucket=*/0), /*offset=*/7},
        {RealtimePartitionBucket(partition, /*bucket=*/1), /*offset=*/8},
        {RealtimePartitionBucket(partition, /*bucket=*/2), /*offset=*/9}};
    ASSERT_NOK_WITH_MSG(context->AdvanceCommittedProgress(5, committed_offsets),
                        "injected committed offset failure");
    ASSERT_EQ(std::vector<int64_t>({7}), factory->stores[0]->committed_offsets);
    ASSERT_TRUE(factory->stores[1]->committed_offsets.empty());
    ASSERT_EQ(std::vector<int64_t>({9}), factory->stores[2]->committed_offsets);

    ASSERT_OK_AND_ASSIGN(RealtimeStoreState failed_store_state,
                         context->GetOrCreateRealtimeStore(partition, 1, MakeWriteSchema(),
                                                           StatisticsMode::NONE, {}, pool));
    ASSERT_EQ(8, failed_store_state.initial_offset);

    ASSERT_OK(context->AdvanceCommittedProgress(5, committed_offsets));
    ASSERT_EQ(1, factory->stores[0]->advance_count);
    ASSERT_EQ(2, factory->stores[1]->advance_count);
    ASSERT_EQ(1, factory->stores[2]->advance_count);
    ASSERT_EQ(std::vector<int64_t>({8}), factory->stores[1]->committed_offsets);
}

TEST(RealtimeContextTest, TestRequiresReopenWhenCommittedProgressMovesBackwards) {
    auto factory = std::make_shared<TestingRealtimeStoreFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> context, CreateContext(factory));
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    const std::map<std::string, std::string> first_partition = {{"dt", "2026-08-02"}};
    const std::map<std::string, std::string> second_partition = {{"dt", "2026-08-03"}};
    const RealtimePartitionBucket first_partition_bucket(first_partition, /*bucket=*/0);
    const RealtimePartitionBucket second_partition_bucket(second_partition, /*bucket=*/0);

    ASSERT_OK(context->GetOrCreateRealtimeStore(first_partition, 0, MakeWriteSchema(),
                                                StatisticsMode::NONE, {}, pool));
    ASSERT_OK(context->GetOrCreateRealtimeStore(second_partition, 0, MakeWriteSchema(),
                                                StatisticsMode::NONE, {}, pool));
    ASSERT_OK(context->AdvanceCommittedProgress(
        5, {{first_partition_bucket, /*offset=*/7}, {second_partition_bucket, /*offset=*/9}}));
    ASSERT_EQ(std::vector<int64_t>({7}), factory->stores[0]->committed_offsets);
    ASSERT_EQ(std::vector<int64_t>({9}), factory->stores[1]->committed_offsets);

    ASSERT_NOK_WITH_MSG(
        context->AdvanceCommittedProgress(
            6, {{first_partition_bucket, /*offset=*/6}, {second_partition_bucket, /*offset=*/10}}),
        "recreate RealtimeContext");
    ASSERT_NOK_WITH_MSG(
        context->AdvanceCommittedProgress(6, {{first_partition_bucket, /*offset=*/10}}),
        "recreate RealtimeContext");
    ASSERT_EQ(std::vector<int64_t>({7}), factory->stores[0]->committed_offsets);
    ASSERT_EQ(std::vector<int64_t>({9}), factory->stores[1]->committed_offsets);

    ASSERT_OK(context->AdvanceCommittedProgress(
        6, {{first_partition_bucket, /*offset=*/10}, {second_partition_bucket, /*offset=*/11}}));
    ASSERT_EQ(std::vector<int64_t>({7, 10}), factory->stores[0]->committed_offsets);
    ASSERT_EQ(std::vector<int64_t>({9, 11}), factory->stores[1]->committed_offsets);
}

TEST(RealtimeContextTest, TestPinsResolvesAndReleasesReadViewTicket) {
    auto factory = std::make_shared<TestingRealtimeStoreFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> context, CreateContext(factory));
    ASSERT_OK(context->GetOrCreateRealtimeStore(/*partition=*/{}, /*bucket=*/0, MakeWriteSchema(),
                                                StatisticsMode::NONE, {}, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         context->AcquireReadViews());
    ASSERT_EQ(1, views.size());

    ASSERT_NOK_WITH_MSG(context->PinReadView(views[0], /*ttl_millis=*/0),
                        "TTL must be greater than zero");
    ASSERT_OK_AND_ASSIGN(std::string ticket,
                         context->PinReadView(views[0], /*ttl_millis=*/60 * 1000));
    ASSERT_FALSE(ticket.empty());
    ASSERT_OK_AND_ASSIGN(RealtimePartitionBucketView resolved, context->ResolveReadView(ticket));
    ASSERT_EQ(views[0].partition_bucket, resolved.partition_bucket);
    ASSERT_EQ(views[0].store, resolved.store);
    ASSERT_EQ(views[0].read_view, resolved.read_view);

    ASSERT_OK(context->ReleaseReadView(ticket));
    ASSERT_OK(context->ReleaseReadView(ticket));
    ASSERT_NOK_WITH_MSG(context->ResolveReadView(ticket), "ticket does not exist or has expired");
}

TEST(RealtimeContextTest, TestExpiresAbandonedReadViewTicket) {
    auto factory = std::make_shared<TestingRealtimeStoreFactory>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> context, CreateContext(factory));
    ASSERT_OK(context->GetOrCreateRealtimeStore(/*partition=*/{}, /*bucket=*/0, MakeWriteSchema(),
                                                StatisticsMode::NONE, {}, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         context->AcquireReadViews());
    ASSERT_EQ(1, views.size());
    std::weak_ptr<RealtimeReadView> weak_view = views[0].read_view;
    ASSERT_OK_AND_ASSIGN(std::string ticket, context->PinReadView(views[0], /*ttl_millis=*/10));
    views.clear();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!weak_view.expired() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(weak_view.expired());
    ASSERT_NOK_WITH_MSG(context->ResolveReadView(ticket), "ticket does not exist or has expired");
}

TEST(RealtimeContextTest, TestRejectsNullFactory) {
    ASSERT_NOK_WITH_MSG(RealtimeContext::Create(/*factory=*/nullptr),
                        "real-time store factory is null");
}

}  // namespace
}  // namespace paimon::test
