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

#include "paimon/core/utils/snapshot_manager.h"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <future>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/snapshot_test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class SnapshotCountingFileSystem : public LocalFileSystem {
 public:
    Status ReadFile(const std::string& path, std::string* content) override {
        if (path.find("/snapshot/snapshot-") != std::string::npos) {
            snapshot_reads.fetch_add(1);
            if (fail_snapshot_read.exchange(false)) {
                return Status::IOError("injected snapshot read failure");
            }
        }
        return LocalFileSystem::ReadFile(path, content);
    }

    std::atomic<int32_t> snapshot_reads{0};
    std::atomic<bool> fail_snapshot_read{false};
};

}  // namespace

class SnapshotManagerCacheTest : public testing::Test {
 protected:
    void SetUp() override {
        directory_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(directory_);
        manager_ = std::make_unique<SnapshotManager>(fs_, directory_->Str());
        ASSERT_OK(fs_->Mkdirs(manager_->SnapshotDirectory()));
    }

    Status WriteSnapshot(const SnapshotManager& manager, int64_t id,
                         const std::string& user = "user") {
        Snapshot snapshot(id, /*schema_id=*/0, /*base_manifest_list=*/"base",
                          /*base_manifest_list_size=*/std::nullopt, /*delta_manifest_list=*/"delta",
                          /*delta_manifest_list_size=*/std::nullopt,
                          /*changelog_manifest_list=*/std::nullopt,
                          /*changelog_manifest_list_size=*/std::nullopt,
                          /*index_manifest=*/std::nullopt, user, /*commit_identifier=*/id,
                          Snapshot::CommitKind::Append(), /*time_millis=*/id,
                          /*total_record_count=*/id, /*delta_record_count=*/1,
                          /*changelog_record_count=*/std::nullopt, /*watermark=*/std::nullopt,
                          /*statistics=*/std::nullopt, /*properties=*/std::nullopt,
                          /*next_row_id=*/std::nullopt);
        PAIMON_ASSIGN_OR_RAISE(std::string json, snapshot.ToJsonString());
        return fs_->WriteFile(manager.SnapshotPath(id), json, /*overwrite=*/true);
    }

    std::unique_ptr<UniqueTestDirectory> directory_;
    std::shared_ptr<SnapshotCountingFileSystem> fs_ =
        std::make_shared<SnapshotCountingFileSystem>();
    std::unique_ptr<SnapshotManager> manager_;
};

TEST_F(SnapshotManagerCacheTest, LatestSnapshotAdvancesWithCachedHistory) {
    ASSERT_OK(WriteSnapshot(*manager_, 1));
    ASSERT_OK(manager_->CommitLatestHint(1));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> first, manager_->LatestSnapshot());
    ASSERT_TRUE(first);
    ASSERT_EQ(first->Id(), 1);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> repeated, manager_->LatestSnapshot());
    ASSERT_TRUE(repeated);
    ASSERT_EQ(*first, *repeated);
    ASSERT_EQ(fs_->snapshot_reads.load(), 1);

    // The new snapshot is visible before its LATEST hint is updated.
    ASSERT_OK(WriteSnapshot(*manager_, 2));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> latest, manager_->LatestSnapshot());
    ASSERT_TRUE(latest);
    ASSERT_EQ(latest->Id(), 2);
    ASSERT_EQ(fs_->snapshot_reads.load(), 2);
    ASSERT_OK(manager_->CommitLatestHint(2));
    ASSERT_OK_AND_ASSIGN(Snapshot historical, manager_->LoadSnapshot(1));
    ASSERT_EQ(historical, *first);
    ASSERT_OK_AND_ASSIGN(latest, manager_->LatestSnapshot());
    ASSERT_TRUE(latest);
    ASSERT_EQ(latest->Id(), 2);
    ASSERT_EQ(fs_->snapshot_reads.load(), 2);
}

TEST_F(SnapshotManagerCacheTest, MissingMalformedAndFailedReadsAreRetried) {
    ASSERT_NOK(manager_->LoadSnapshot(1));
    ASSERT_OK(fs_->WriteFile(manager_->SnapshotPath(1), "invalid JSON", true));
    ASSERT_NOK(manager_->LoadSnapshot(1));
    ASSERT_OK(WriteSnapshot(*manager_, 1));
    fs_->fail_snapshot_read = true;
    ASSERT_NOK_WITH_MSG(manager_->LoadSnapshot(1), "injected snapshot read failure");
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, manager_->LoadSnapshot(1));
    ASSERT_EQ(snapshot.Id(), 1);
    ASSERT_EQ(fs_->snapshot_reads.load(), 4);
    ASSERT_OK(manager_->LoadSnapshot(1));
    ASSERT_EQ(fs_->snapshot_reads.load(), 4);
}

TEST_F(SnapshotManagerCacheTest, EvictsLeastRecentlyUsedSnapshot) {
    // The manager retains up to 64 snapshots.
    constexpr int64_t kCapacity = 64;
    for (int64_t id = 1; id <= kCapacity; ++id) {
        ASSERT_OK(WriteSnapshot(*manager_, id));
        ASSERT_OK(manager_->LoadSnapshot(id));
    }
    ASSERT_EQ(fs_->snapshot_reads.load(), kCapacity);
    ASSERT_OK(manager_->LoadSnapshot(1));
    ASSERT_EQ(fs_->snapshot_reads.load(), kCapacity);
    ASSERT_OK(WriteSnapshot(*manager_, kCapacity + 1));
    ASSERT_OK(manager_->LoadSnapshot(kCapacity + 1));
    ASSERT_OK(manager_->LoadSnapshot(1));
    ASSERT_EQ(fs_->snapshot_reads.load(), kCapacity + 1);
    ASSERT_OK_AND_ASSIGN(Snapshot reloaded, manager_->LoadSnapshot(2));
    ASSERT_EQ(reloaded.Id(), 2);
    ASSERT_EQ(fs_->snapshot_reads.load(), kCapacity + 2);
}

TEST_F(SnapshotManagerCacheTest, CachedMetadataDoesNotMakeExpiredSnapshotExist) {
    ASSERT_OK(WriteSnapshot(*manager_, 1));
    ASSERT_OK(WriteSnapshot(*manager_, 2));
    ASSERT_OK(manager_->CommitEarliestHint(1));
    ASSERT_OK(manager_->CommitLatestHint(2));
    ASSERT_OK_AND_ASSIGN(Snapshot cached, manager_->LoadSnapshot(1));
    ASSERT_OK(fs_->Delete(manager_->SnapshotPath(1)));
    ASSERT_OK(manager_->CommitEarliestHint(2));
    ASSERT_OK_AND_ASSIGN(bool exists, manager_->SnapshotExists(1));
    ASSERT_FALSE(exists);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> earliest, manager_->EarliestSnapshotId());
    ASSERT_EQ(earliest, 2);
    ASSERT_OK_AND_ASSIGN(Snapshot historical, manager_->LoadSnapshot(1));
    ASSERT_EQ(historical, cached);
    ASSERT_EQ(fs_->snapshot_reads.load(), 1);
    SnapshotManager fresh(fs_, directory_->Str());
    ASSERT_NOK(fresh.LoadSnapshot(1));
}

TEST_F(SnapshotManagerCacheTest, RecreatedManagerLoadsReplacedSnapshot) {
    ASSERT_OK(WriteSnapshot(*manager_, 1, "before"));
    ASSERT_OK_AND_ASSIGN(Snapshot first, manager_->LoadSnapshot(1));
    ASSERT_EQ(first.CommitUser(), "before");
    // Fast-forward can replace contents under the same snapshot ID.
    ASSERT_OK(WriteSnapshot(*manager_, 1, "after"));
    ASSERT_OK_AND_ASSIGN(Snapshot cached, manager_->LoadSnapshot(1));
    ASSERT_EQ(cached.CommitUser(), "before");
    SnapshotManager fresh(fs_, directory_->Str());
    ASSERT_OK_AND_ASSIGN(Snapshot reloaded, fresh.LoadSnapshot(1));
    ASSERT_EQ(reloaded.CommitUser(), "after");
}

TEST_F(SnapshotManagerCacheTest, TablesAndBranchesKeepSeparateCaches) {
    SnapshotManager branch(fs_, directory_->Str(), "dev");
    SnapshotManager other(fs_, PathUtil::JoinPath(directory_->Str(), "other"));
    ASSERT_OK(fs_->Mkdirs(branch.SnapshotDirectory()));
    ASSERT_OK(fs_->Mkdirs(other.SnapshotDirectory()));
    ASSERT_OK(WriteSnapshot(*manager_, 1, "main"));
    ASSERT_OK(WriteSnapshot(branch, 1, "dev"));
    ASSERT_OK(WriteSnapshot(other, 1, "other"));
    for (int32_t i = 0; i < 2; ++i) {
        ASSERT_OK_AND_ASSIGN(Snapshot main_snapshot, manager_->LoadSnapshot(1));
        ASSERT_EQ(main_snapshot.CommitUser(), "main");
        ASSERT_OK_AND_ASSIGN(Snapshot branch_snapshot, branch.LoadSnapshot(1));
        ASSERT_EQ(branch_snapshot.CommitUser(), "dev");
        ASSERT_OK_AND_ASSIGN(Snapshot other_snapshot, other.LoadSnapshot(1));
        ASSERT_EQ(other_snapshot.CommitUser(), "other");
    }
    ASSERT_EQ(fs_->snapshot_reads.load(), 3);
}

TEST_F(SnapshotManagerCacheTest, ConcurrentLoadsReturnCompleteSnapshots) {
    ASSERT_OK(WriteSnapshot(*manager_, 1));
    std::promise<void> start;
    std::shared_future<void> ready = start.get_future().share();
    std::vector<std::future<Result<Snapshot>>> futures;
    for (int32_t i = 0; i < 8; ++i) {
        futures.push_back(std::async(std::launch::async, [this, ready]() {
            ready.wait();
            return manager_->LoadSnapshot(1);
        }));
    }
    start.set_value();
    ASSERT_OK_AND_ASSIGN(Snapshot expected, futures.front().get());
    ASSERT_EQ(expected.Id(), 1);
    for (size_t i = 1; i < futures.size(); ++i) {
        ASSERT_OK_AND_ASSIGN(Snapshot actual, futures[i].get());
        ASSERT_EQ(actual, expected);
    }
    int32_t reads = fs_->snapshot_reads.load();
    ASSERT_GE(reads, 1);
    ASSERT_OK(manager_->LoadSnapshot(1));
    ASSERT_EQ(fs_->snapshot_reads.load(), reads);
}

TEST(SnapshotManagerTest, TestSnapshotDirectory) {
    auto fs = std::make_shared<LocalFileSystem>();
    SnapshotManager manager(fs, paimon::test::GetDataDir() + "/append_09.db/append_09");
    ASSERT_EQ(manager.SnapshotDirectory(),
              paimon::test::GetDataDir() + "/append_09.db/append_09/snapshot");
    ASSERT_EQ(manager.Branch(), BranchManager::DEFAULT_MAIN_BRANCH);
}

TEST(SnapshotManagerTest, TestSnapshotDirectoryWithBranch) {
    auto fs = std::make_shared<LocalFileSystem>();
    SnapshotManager manager(
        fs,
        paimon::test::GetDataDir() + "/append_table_with_rt_branch.db/append_table_with_rt_branch",
        /*branch=*/"rt");
    ASSERT_EQ(manager.SnapshotDirectory(),
              paimon::test::GetDataDir() +
                  "/append_table_with_rt_branch.db/append_table_with_rt_branch/branch/branch-rt/"
                  "snapshot");
    ASSERT_EQ(manager.Branch(), "rt");
}

TEST(SnapshotManagerTest, TestSimple) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id1, mgr.EarliestSnapshotId());
    ASSERT_EQ(id1.value(), 1);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id2, mgr.LatestSnapshotId());
    ASSERT_EQ(id2.value(), 5);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.LatestSnapshotOfUser("b02e4322-9c5f-41e1-a560-c0156fdf7b9c"));
    ASSERT_EQ(snapshot.value().CommitUser(), "b02e4322-9c5f-41e1-a560-c0156fdf7b9c");
    ASSERT_EQ(snapshot.value().CommitIdentifier(), 9223372036854775807);
}

TEST(SnapshotManagerTest, TestSimpleWithBranch) {
    std::string test_data_path = paimon::test::GetDataDir() +
                                 "/orc/append_table_with_rt_branch.db/append_table_with_rt_branch";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path, /*branch=*/"rt");
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id1, mgr.EarliestSnapshotId());
    ASSERT_EQ(id1.value(), 1);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id2, mgr.LatestSnapshotId());
    ASSERT_EQ(id2.value(), 1);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.LatestSnapshotOfUser("884df499-c17a-4c78-a865-6fbbfea02f0b"));
    ASSERT_EQ(snapshot.value().CommitUser(), "884df499-c17a-4c78-a865-6fbbfea02f0b");
    ASSERT_EQ(snapshot.value().CommitIdentifier(), 1);
}

TEST(SnapshotManagerTest, SnapshotLoaderAnswersWhichSnapshotIsLatest) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> from_files, mgr.LatestSnapshotId());
    ASSERT_EQ(from_files.value(), 5);
    ASSERT_OK_AND_ASSIGN(Snapshot second, mgr.LoadSnapshot(2));

    mgr.SetSnapshotLoader(
        [second]() -> Result<std::optional<Snapshot>> { return std::optional<Snapshot>(second); });
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> from_loader, mgr.LatestSnapshotId());
    ASSERT_EQ(from_loader.value(), 2);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> latest, mgr.LatestSnapshot());
    ASSERT_TRUE(latest.has_value());
    ASSERT_EQ(latest.value().Id(), 2);

    mgr.SetSnapshotLoader(
        []() -> Result<std::optional<Snapshot>> { return std::optional<Snapshot>(); });
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> empty_id, mgr.LatestSnapshotId());
    ASSERT_EQ(empty_id, std::nullopt);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> empty, mgr.LatestSnapshot());
    ASSERT_FALSE(empty.has_value());

    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> earliest, mgr.EarliestSnapshotId());
    ASSERT_EQ(earliest.value(), 1);
    ASSERT_OK_AND_ASSIGN(Snapshot loaded, mgr.LoadSnapshot(5));
    ASSERT_EQ(loaded.Id(), 5);
}

TEST(SnapshotManagerTest, LatestSnapshotOfUserReadsTheLoadedSnapshot) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(Snapshot fifth, mgr.LoadSnapshot(5));

    mgr.SetSnapshotLoader(
        [fifth]() -> Result<std::optional<Snapshot>> { return std::optional<Snapshot>(fifth); });
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> found,
                         mgr.LatestSnapshotOfUser(fifth.CommitUser()));
    ASSERT_TRUE(found.has_value());
    ASSERT_EQ(found.value().Id(), 5);

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> missing, mgr.LatestSnapshotOfUser("nobody"));
    ASSERT_FALSE(missing.has_value());

    mgr.SetSnapshotLoader(
        []() -> Result<std::optional<Snapshot>> { return std::optional<Snapshot>(); });
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> none,
                         mgr.LatestSnapshotOfUser(fifth.CommitUser()));
    ASSERT_FALSE(none.has_value());
}

TEST(SnapshotManagerTest, LatestSnapshotOfUserFailsWhenTheHistoryIsUnreadable) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, dir->Str());

    Snapshot only_in_catalog = BuildTestSnapshot(9, "snapshot-uuid-9");
    mgr.SetSnapshotLoader([only_in_catalog]() -> Result<std::optional<Snapshot>> {
        return std::optional<Snapshot>(only_in_catalog);
    });

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> own,
                         mgr.LatestSnapshotOfUser(only_in_catalog.CommitUser()));
    ASSERT_TRUE(own.has_value());
    ASSERT_EQ(own.value().Id(), 9);

    ASSERT_NOK_WITH_MSG(mgr.LatestSnapshotOfUser("somebody-else"),
                        "cannot tell which snapshot of table");
}

TEST(SnapshotManagerTest, LatestSnapshotOfUserPropagatesCorruptHistory) {
    for (bool from_catalog : {false, true}) {
        SCOPED_TRACE(from_catalog);
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        auto file_system = std::make_shared<LocalFileSystem>();
        SnapshotManager mgr(file_system, dir->Str());
        ASSERT_OK(file_system->Mkdirs(mgr.SnapshotDirectory()));
        ASSERT_OK(file_system->WriteFile(mgr.SnapshotPath(1), "not json", false));
        Snapshot latest = BuildTestSnapshot(2);
        ASSERT_OK_AND_ASSIGN(std::string json, latest.ToJsonString());
        ASSERT_OK(file_system->WriteFile(mgr.SnapshotPath(2), json, false));
        if (from_catalog) {
            mgr.SetSnapshotLoader([latest]() -> Result<std::optional<Snapshot>> {
                return std::optional<Snapshot>(latest);
            });
        }

        ASSERT_NOK_WITH_MSG(mgr.LatestSnapshotOfUser("somebody-else"), "deserialize failed");
    }
}

TEST(SnapshotManagerTest, LatestSnapshotOfUserStopsAtTheEarliestRetainedSnapshot) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, dir->Str());
    ASSERT_OK(file_system->Mkdirs(mgr.SnapshotDirectory()));
    for (int64_t id : {5, 6}) {
        ASSERT_OK_AND_ASSIGN(std::string json, BuildTestSnapshot(id).ToJsonString());
        ASSERT_OK(file_system->WriteFile(mgr.SnapshotPath(id), json, false));
    }
    ASSERT_OK(mgr.CommitEarliestHint(5));

    Snapshot sixth = BuildTestSnapshot(6, "snapshot-uuid-6");
    mgr.SetSnapshotLoader(
        [sixth]() -> Result<std::optional<Snapshot>> { return std::optional<Snapshot>(sixth); });
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> other, mgr.LatestSnapshotOfUser("somebody-else"));
    ASSERT_FALSE(other.has_value());

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> bounded,
                         mgr.LatestSnapshotOfUserAtOrBefore("somebody-else", sixth, true));
    ASSERT_FALSE(bounded.has_value());

    ASSERT_OK(mgr.CommitEarliestHint(2));
    ASSERT_NOK_WITH_MSG(mgr.LatestSnapshotOfUser("somebody-else"),
                        "is not under the table directory");
    ASSERT_NOK_WITH_MSG(mgr.LatestSnapshotOfUserAtOrBefore("somebody-else", sixth, true),
                        "is not under the table directory");
}

TEST(SnapshotManagerTest, LatestSnapshotOfUserAtOrBeforeWalksBelowTheGivenSnapshot) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(Snapshot third, mgr.LoadSnapshot(3));

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> own,
                         mgr.LatestSnapshotOfUserAtOrBefore(third.CommitUser(), third, false));
    ASSERT_TRUE(own.has_value());
    ASSERT_EQ(own.value().Id(), 3);

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> none,
                         mgr.LatestSnapshotOfUserAtOrBefore("nobody", third, false));
    ASSERT_FALSE(none.has_value());

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> empty,
                         mgr.LatestSnapshotOfUserAtOrBefore("whoever", std::nullopt, false));
    ASSERT_FALSE(empty.has_value());
}

TEST(SnapshotManagerTest, LatestSnapshotOfUserStopsAtAnExpiredSnapshotAfterFallingBack) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, dir->Str());
    ASSERT_OK(file_system->Mkdirs(mgr.SnapshotDirectory()));
    for (int64_t id : {5, 6}) {
        ASSERT_OK_AND_ASSIGN(std::string json, BuildTestSnapshot(id).ToJsonString());
        ASSERT_OK(file_system->WriteFile(mgr.SnapshotPath(id), json, false));
    }
    mgr.SetSnapshotLoader([]() -> Result<std::optional<Snapshot>> {
        return Status::NotImplemented("this catalog does not serve snapshots");
    });

    ASSERT_OK_AND_ASSIGN(SnapshotManager::LatestSnapshotResult latest,
                         mgr.LatestSnapshotWithSource());
    ASSERT_FALSE(latest.from_catalog);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> other, mgr.LatestSnapshotOfUser("somebody-else"));
    ASSERT_FALSE(other.has_value());

    ASSERT_OK_AND_ASSIGN(
        std::optional<Snapshot> bounded,
        mgr.LatestSnapshotOfUserAtOrBefore("somebody-else", latest.snapshot, latest.from_catalog));
    ASSERT_FALSE(bounded.has_value());
}

namespace {
class ExpiringHintFileSystem : public LocalFileSystem {
 public:
    ExpiringHintFileSystem(const std::string& hint_path, const std::string& refreshed,
                           int32_t stale_reads)
        : hint_path_(hint_path), refreshed_(refreshed), stale_reads_(stale_reads) {}

    Status ReadFile(const std::string& path, std::string* content) override {
        if (path == hint_path_ && hint_reads_++ >= stale_reads_) {
            *content = refreshed_;
            return Status::OK();
        }
        return LocalFileSystem::ReadFile(path, content);
    }

    int32_t HintReads() const {
        return hint_reads_;
    }

 private:
    std::string hint_path_;
    std::string refreshed_;
    int32_t stale_reads_;
    int32_t hint_reads_ = 0;
};

std::shared_ptr<ExpiringHintFileSystem> SetUpDeletedButUnannouncedSnapshots(
    const std::string& root_path, int32_t stale_reads) {
    SnapshotManager paths(std::make_shared<LocalFileSystem>(), root_path);
    auto file_system = std::make_shared<ExpiringHintFileSystem>(
        PathUtil::JoinPath(paths.SnapshotDirectory(), SnapshotManager::EARLIEST), "5", stale_reads);
    SnapshotManager writer(file_system, root_path);
    EXPECT_OK(file_system->Mkdirs(writer.SnapshotDirectory()));
    for (int64_t id : {5, 6}) {
        EXPECT_OK_AND_ASSIGN(std::string json, BuildTestSnapshot(id).ToJsonString());
        EXPECT_OK(file_system->WriteFile(writer.SnapshotPath(id), json, false));
    }
    EXPECT_OK(writer.CommitEarliestHint(1));
    return file_system;
}

void SetCatalogHoldingTheSixthSnapshot(SnapshotManager* mgr) {
    Snapshot sixth = BuildTestSnapshot(6, "snapshot-uuid-6");
    mgr->SetSnapshotLoader(
        [sixth]() -> Result<std::optional<Snapshot>> { return std::optional<Snapshot>(sixth); });
}
}  // namespace

TEST(SnapshotManagerTest, LatestSnapshotOfUserRetriesTheBoundaryWhileItIsStillStale) {
    for (int32_t stale_reads : {1, 2, 3}) {
        SCOPED_TRACE(stale_reads);
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        std::shared_ptr<ExpiringHintFileSystem> file_system =
            SetUpDeletedButUnannouncedSnapshots(dir->Str(), stale_reads);
        ASSERT_NE(nullptr, file_system);
        SnapshotManager mgr(file_system, dir->Str());
        SetCatalogHoldingTheSixthSnapshot(&mgr);

        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> other,
                             mgr.LatestSnapshotOfUser("somebody-else"));
        ASSERT_FALSE(other.has_value());
        ASSERT_EQ(stale_reads + 1, file_system->HintReads());
    }
}

TEST(SnapshotManagerTest, LatestSnapshotOfUserStillReportsAGapItCannotExplain) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<ExpiringHintFileSystem> file_system =
        SetUpDeletedButUnannouncedSnapshots(dir->Str(), 100);
    ASSERT_NE(nullptr, file_system);
    SnapshotManager mgr(file_system, dir->Str());
    SetCatalogHoldingTheSixthSnapshot(&mgr);

    ASSERT_NOK_WITH_MSG(mgr.LatestSnapshotOfUser("somebody-else"),
                        "is not under the table directory");
    ASSERT_GT(file_system->HintReads(), 1);
    ASSERT_LT(file_system->HintReads(), 100);
}

TEST(SnapshotManagerTest, LatestSnapshotOfUserStopsAtAnExpiredSnapshot) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, dir->Str());
    ASSERT_OK(file_system->Mkdirs(mgr.SnapshotDirectory()));
    for (int64_t id : {1, 3}) {
        ASSERT_OK_AND_ASSIGN(std::string json, BuildTestSnapshot(id).ToJsonString());
        ASSERT_OK(file_system->WriteFile(mgr.SnapshotPath(id), json, false));
    }

    Snapshot latest = BuildTestSnapshot(3);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> own,
                         mgr.LatestSnapshotOfUser(latest.CommitUser()));
    ASSERT_TRUE(own.has_value());
    ASSERT_EQ(own.value().Id(), 3);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> other, mgr.LatestSnapshotOfUser("somebody-else"));
    ASSERT_FALSE(other.has_value());
}

TEST(SnapshotManagerTest, SnapshotLoaderFallsBackAndPropagates) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);

    int32_t asked = 0;
    mgr.SetSnapshotLoader([&asked]() -> Result<std::optional<Snapshot>> {
        ++asked;
        return Status::NotImplemented("this catalog does not serve snapshots");
    });
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id, mgr.LatestSnapshotId());
    ASSERT_EQ(id.value(), 5);
    ASSERT_EQ(asked, 1);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> latest, mgr.LatestSnapshot());
    ASSERT_TRUE(latest.has_value());
    ASSERT_EQ(latest.value().Id(), 5);
    ASSERT_EQ(asked, 2);

    mgr.SetSnapshotLoader(
        []() -> Result<std::optional<Snapshot>> { return Status::IOError("catalog unreachable"); });
    ASSERT_NOK_WITH_MSG(mgr.LatestSnapshotId(), "catalog unreachable");
    ASSERT_NOK_WITH_MSG(mgr.LatestSnapshot(), "catalog unreachable");
}

TEST(SnapshotManagerTest, TestGetAllSnapshots) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(std::vector<Snapshot> snapshots, mgr.GetAllSnapshots());
    ASSERT_EQ(snapshots.size(), 5u);
}

TEST(SnapshotManagerTest, TestGetAllSnapshotsWithBranch) {
    std::string test_data_path =
        paimon::test::GetDataDir() +
        "/orc/append_table_with_append_pt_branch.db/append_table_with_append_pt_branch";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path, /*branch=*/"test");
    ASSERT_OK_AND_ASSIGN(std::vector<Snapshot> snapshots, mgr.GetAllSnapshots());
    ASSERT_EQ(snapshots.size(), 2u);
}

TEST(SnapshotManagerTest, TestGetNonSnapshotFiles) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string table_path = dir->Str();
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));

    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, table_path);
    ASSERT_OK(file_system->Mkdirs(PathUtil::JoinPath(table_path, "snapshot/orphan_dir")));
    ASSERT_OK(file_system->WriteFile(PathUtil::JoinPath(table_path, "snapshot/orphan_file"),
                                     "orphan", true));
    auto check_result = [](const std::set<std::string>& actual,
                           const std::set<std::string>& expected) -> bool {
        std::set<std::string> file_names;
        for (const auto& file_path : actual) {
            file_names.insert(PathUtil::GetName(file_path));
        }
        return file_names == expected;
    };
    ASSERT_OK_AND_ASSIGN(std::set<std::string> non_snapshot_files,
                         mgr.TryGetNonSnapshotFiles(std::numeric_limits<int64_t>::max()));
    ASSERT_TRUE(check_result(non_snapshot_files, {"orphan_dir", "orphan_file"}));
    ASSERT_OK_AND_ASSIGN(non_snapshot_files,
                         mgr.TryGetNonSnapshotFiles(std::numeric_limits<int64_t>::min()));
    ASSERT_EQ(non_snapshot_files.size(), 0);
}

TEST(SnapshotManagerTest, TestPathNotExist) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/not_exist";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id1, mgr.EarliestSnapshotId());
    ASSERT_EQ(id1, std::nullopt);
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> id2, mgr.LatestSnapshotId());
    ASSERT_EQ(id2, std::nullopt);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.LatestSnapshotOfUser("b02e4322-9c5f-41e1-a560-c0156fdf7b9c"));
    ASSERT_EQ(snapshot, std::nullopt);
}

TEST(SnapshotManagerTest, LatestSnapshotWithStaleMissingOrInvalidHint) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(fs, dir->Str());
    ASSERT_OK(fs->Mkdirs(mgr.SnapshotDirectory()));
    ASSERT_OK(fs->WriteFile(mgr.SnapshotPath(1), "{}", true));
    ASSERT_OK(mgr.CommitLatestHint(1));

    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> latest, mgr.LatestSnapshotId());
    ASSERT_EQ(latest, 1);

    // A commit can publish its snapshot before updating the hint.
    ASSERT_OK(fs->WriteFile(mgr.SnapshotPath(2), "{}", true));
    ASSERT_OK_AND_ASSIGN(latest, mgr.LatestSnapshotId());
    ASSERT_EQ(latest, 2);

    const std::string hint_path = PathUtil::JoinPath(mgr.SnapshotDirectory(), "LATEST");
    ASSERT_OK(fs->Delete(hint_path));
    ASSERT_OK_AND_ASSIGN(latest, mgr.LatestSnapshotId());
    ASSERT_EQ(latest, 2);

    ASSERT_OK(fs->WriteFile(hint_path, "invalid", true));
    ASSERT_OK_AND_ASSIGN(latest, mgr.LatestSnapshotId());
    ASSERT_EQ(latest, 2);
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisExactMatch) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // snapshot-3 has timeMillis = 1721614515032
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721614515032));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->Id(), 3);
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisBetweenSnapshots) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Between snapshot-3 (1721614515032) and snapshot-4 (1721615035363), should return snapshot-3
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721614600000));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->Id(), 3);
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisBeforeAll) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Before snapshot-1 (1721614343270), should return nullopt
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721614343269));
    ASSERT_FALSE(snapshot.has_value());
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisAfterAll) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // After snapshot-5 (1721615035453), should return snapshot-5
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721615099999));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->Id(), 5);
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisFirstSnapshot) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Exact match on snapshot-1
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721614343270));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->Id(), 1);
}

TEST(SnapshotManagerTest, TestEarlierOrEqualTimeMillisNoSnapshots) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/not_exist";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierOrEqualTimeMillis(1721614343270));
    ASSERT_FALSE(snapshot.has_value());
}

TEST(SnapshotManagerTest, TestEarlierThanTimeMillisExactMatch) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Exact match on snapshot-1 (1721614343270) should not include snapshot-1 for strict <
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierThanTimeMillis(1721614343270));
    ASSERT_FALSE(snapshot.has_value());
}

TEST(SnapshotManagerTest, TestEarlierThanTimeMillisBetweenSnapshots) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Between snapshot-1 (1721614343270) and snapshot-2 (1721614468258), should return snapshot-1
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierThanTimeMillis(1721614400000));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->Id(), 1);
}

TEST(SnapshotManagerTest, TestEarlierThanTimeMillisBeforeAll) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    // Before snapshot-1 (1721614343270), should return no snapshot
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot,
                         mgr.EarlierThanTimeMillis(1721614343269));
    ASSERT_FALSE(snapshot.has_value());
}

TEST(SnapshotManagerTest, TestCommitLatestHint) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string test_data_path = dir->Str();
    auto file_system = std::make_shared<LocalFileSystem>();
    SnapshotManager mgr(file_system, test_data_path);
    ASSERT_OK(mgr.CommitLatestHint(1));
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> latest_snapshot_id, mgr.LatestSnapshotId());
    ASSERT_EQ(latest_snapshot_id.value(), 1);

    ASSERT_OK_AND_ASSIGN(bool exists, file_system->Exists(test_data_path));
    ASSERT_TRUE(exists);
}

}  // namespace paimon::test
