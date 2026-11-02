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

#include "paimon/core/operation/commit/realtime_deduplicate_dv_materializer.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/operation/commit/realtime_deduplicate_commit_changes_provider.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class RealtimeDeduplicateDvMaterializerTest : public testing::Test {
 protected:
    using Materializer = RealtimeDeduplicateDvMaterializer;

    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
        ASSERT_NE(nullptr, dir_);
        const std::map<std::string, std::string> options = {{Options::FILE_SYSTEM, "local"},
                                                            {Options::BUCKET, "1"}};
        ASSERT_OK_AND_ASSIGN(options_, CoreOptions::FromMap(options));
        const auto schema = arrow::schema({arrow::field("id", arrow::int64())});
        ASSERT_OK_AND_ASSIGN(table_schema_, TableSchema::Create(0, schema, {}, {}, options));
        ASSERT_OK_AND_ASSIGN(
            paths_, FileStorePathFactory::Create(
                        dir_->Str(), schema, {}, "__DEFAULT_PARTITION__", "parquet", "data-",
                        /*legacy_partition_name_enabled=*/true, {}, std::nullopt,
                        /*index_file_in_data_file_dir=*/false, GetDefaultPool()));
        materializer_ = std::make_unique<Materializer>(table_schema_, paths_, /*scanner=*/nullptr,
                                                       options_, GetDefaultPool());
    }

    Result<std::shared_ptr<DeletionVectorsIndexFile>> CreateDvFile(int32_t bucket) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexPathFactory> paths,
                               paths_->CreateIndexFileFactory(BinaryRow(0), bucket));
        return std::make_shared<DeletionVectorsIndexFile>(options_.GetFileSystem(), paths,
                                                          /*bitmap64=*/false, GetDefaultPool());
    }

    std::shared_ptr<DeletionVector> MakeDv(const std::vector<uint32_t>& positions) const {
        RoaringBitmap32 bitmap;
        for (uint32_t position : positions) {
            bitmap.Add(position);
        }
        return std::make_shared<BitmapDeletionVector>(bitmap);
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    CoreOptions options_;
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<FileStorePathFactory> paths_;
    std::unique_ptr<Materializer> materializer_;
};

TEST_F(RealtimeDeduplicateDvMaterializerTest, TestEmptySnapshotAndNoopUpdate) {
    ASSERT_OK_AND_ASSIGN(Materializer::SnapshotBuckets buckets,
                         materializer_->LoadSnapshot(std::nullopt, {BinaryRow(0)}));
    ASSERT_TRUE(buckets.empty());
    Materializer::BucketSnapshot snapshot;
    Materializer::BucketUpdate update;
    CommitChanges changes;
    ASSERT_OK(
        materializer_->MaterializeBucket(BinaryRow(0), 0, snapshot, {}, update, {}, &changes));
    ASSERT_TRUE(changes.index_entries.empty());
    ASSERT_TRUE(changes.temporary_files.empty());
}

TEST_F(RealtimeDeduplicateDvMaterializerTest, TestUnchangedBaselineDoesNotRewrite) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVectorsIndexFile> file, CreateDvFile(0));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> index,
                         file->WriteSingleFile({{"kept", MakeDv({1, 3})}}));
    Materializer::BucketSnapshot snapshot;
    snapshot.dv_index_files = {index};
    Materializer::BucketUpdate update;
    update.final_file_names = {"kept"};
    ASSERT_OK_AND_ASSIGN(auto baseline,
                         materializer_->ReadDeletionVectors(BinaryRow(0), 0, snapshot));
    CommitChanges changes;
    ASSERT_OK(materializer_->MaterializeBucket(BinaryRow(0), 0, snapshot, std::move(baseline),
                                               update, {}, &changes));
    ASSERT_TRUE(changes.index_entries.empty());
    ASSERT_TRUE(changes.temporary_files.empty());
    ASSERT_OK_AND_ASSIGN(bool exists, file->Exists(index));
    ASSERT_TRUE(exists);
}

TEST_F(RealtimeDeduplicateDvMaterializerTest, TestPruneRetiredFilesAndCleanUpAttempt) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVectorsIndexFile> file, CreateDvFile(0));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<IndexFileMeta> index,
        file->WriteSingleFile({{"retired", MakeDv({0})}, {"kept", MakeDv({1, 3})}}));
    Materializer::BucketSnapshot snapshot;
    snapshot.dv_index_files = {index};
    Materializer::BucketUpdate update;
    update.final_file_names = {"kept"};
    ASSERT_OK_AND_ASSIGN(auto baseline,
                         materializer_->ReadDeletionVectors(BinaryRow(0), 0, snapshot));
    CommitChanges changes;
    ASSERT_OK(materializer_->MaterializeBucket(BinaryRow(0), 0, snapshot, std::move(baseline),
                                               update, {}, &changes));
    ASSERT_EQ(1, changes.index_entries.size());
    ASSERT_EQ(FileKind::Add(), changes.index_entries[0].kind);
    ASSERT_EQ(1, changes.temporary_files.size());
    ASSERT_OK_AND_ASSIGN(auto written,
                         file->ReadAllDeletionVectors(changes.index_entries[0].index_file));
    ASSERT_EQ(1, written.size());
    ASSERT_EQ(1, written.count("kept"));
    ASSERT_OK_AND_ASSIGN(int64_t count, written.at("kept")->GetCardinality());
    ASSERT_EQ(2, count);

    materializer_->CleanUpUnpublishedFiles(changes);
    ASSERT_OK_AND_ASSIGN(bool attempt_exists,
                         options_.GetFileSystem()->Exists(changes.temporary_files[0]));
    ASSERT_FALSE(attempt_exists);
    ASSERT_OK_AND_ASSIGN(bool baseline_exists, file->Exists(index));
    ASSERT_TRUE(baseline_exists);
    // A retry reloads the original snapshot; cleanup and pruning never modify its DV file.
    ASSERT_OK_AND_ASSIGN(auto reloaded,
                         materializer_->ReadDeletionVectors(BinaryRow(0), 0, snapshot));
    ASSERT_EQ(2, reloaded.size());
    ASSERT_EQ(1, reloaded.count("retired"));
}

TEST_F(RealtimeDeduplicateDvMaterializerTest, TestEmptyFinalDvRemovesOldIndexReference) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVectorsIndexFile> file, CreateDvFile(0));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> index,
                         file->WriteSingleFile({{"retired", MakeDv({0})}}));
    Materializer::BucketSnapshot snapshot;
    snapshot.dv_index_files = {index};
    Materializer::BucketUpdate update;
    ASSERT_OK_AND_ASSIGN(auto baseline,
                         materializer_->ReadDeletionVectors(BinaryRow(0), 0, snapshot));
    CommitChanges changes;
    ASSERT_OK(materializer_->MaterializeBucket(BinaryRow(0), 0, snapshot, std::move(baseline),
                                               update, {}, &changes));
    ASSERT_EQ(1, changes.index_entries.size());
    ASSERT_EQ(FileKind::Delete(), changes.index_entries[0].kind);
    ASSERT_EQ(index, changes.index_entries[0].index_file);
    ASSERT_TRUE(changes.temporary_files.empty());
    // Publication removes the manifest reference, not the file needed by older snapshots.
    ASSERT_OK_AND_ASSIGN(bool exists, file->Exists(index));
    ASSERT_TRUE(exists);
}

TEST_F(RealtimeDeduplicateDvMaterializerTest, TestSameFileNameInDifferentBucketsIsIsolated) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVectorsIndexFile> file0, CreateDvFile(0));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVectorsIndexFile> file1, CreateDvFile(1));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> index0,
                         file0->WriteSingleFile({{"same-name", MakeDv({0})}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFileMeta> index1,
                         file1->WriteSingleFile({{"same-name", MakeDv({2, 4})}}));
    for (int32_t bucket = 0; bucket < 2; ++bucket) {
        Materializer::BucketSnapshot snapshot;
        snapshot.dv_index_files = {bucket == 0 ? index0 : index1};
        Materializer::BucketUpdate update;
        if (bucket == 1) {
            update.final_file_names = {"same-name"};
        }
        ASSERT_OK_AND_ASSIGN(auto baseline,
                             materializer_->ReadDeletionVectors(BinaryRow(0), bucket, snapshot));
        CommitChanges changes;
        ASSERT_OK(materializer_->MaterializeBucket(BinaryRow(0), bucket, snapshot,
                                                   std::move(baseline), update, {}, &changes));
        ASSERT_EQ(bucket == 0 ? 1u : 0u, changes.index_entries.size());
        if (bucket == 0) {
            ASSERT_EQ(0, changes.index_entries[0].bucket);
            ASSERT_EQ(index0, changes.index_entries[0].index_file);
        }
    }
    ASSERT_OK_AND_ASSIGN(auto untouched, file1->ReadAllDeletionVectors(index1));
    ASSERT_OK_AND_ASSIGN(int64_t count, untouched.at("same-name")->GetCardinality());
    ASSERT_EQ(2, count);
}

TEST_F(RealtimeDeduplicateDvMaterializerTest, TestProgressOnlyProviderNeedsNoSnapshotScan) {
    auto index = std::make_shared<IndexFileMeta>("HASH", "unrelated-index", 10, 1, std::nullopt,
                                                 std::nullopt);
    ManifestEntryChanges input(1, /*drop_delete_file_stats=*/false);
    input.append_index_files.emplace_back(FileKind::Add(), BinaryRow(0), 0, index);
    const std::map<RealtimePartitionBucket, RoaringBitmap64> offsets = {
        {RealtimePartitionBucket({}, 0), RoaringBitmap64()}};
    RealtimeDeduplicateCommitChangesProvider provider(std::move(input), offsets, table_schema_,
                                                      paths_, /*scanner=*/nullptr, options_,
                                                      GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitChanges> changes, provider.Provide(std::nullopt));
    ASSERT_TRUE(changes->delta_files.empty());
    ASSERT_EQ(1, changes->index_entries.size());
    ASSERT_EQ(index, changes->index_entries[0].index_file);
    ASSERT_TRUE(changes->temporary_files.empty());
}

TEST_F(RealtimeDeduplicateDvMaterializerTest, TestDeletionWithoutMatchingFilesNeedsNoDv) {
    RoaringBitmap64 offsets;
    offsets.Add(42);
    RealtimeDeduplicateCommitChangesProvider provider(
        ManifestEntryChanges(1, /*drop_delete_file_stats=*/false),
        {{RealtimePartitionBucket({}, 0), offsets}}, table_schema_, paths_,
        /*scanner=*/nullptr, options_, GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitChanges> changes, provider.Provide(std::nullopt));
    ASSERT_TRUE(changes->index_entries.empty());
    ASSERT_TRUE(changes->temporary_files.empty());
}

TEST_F(RealtimeDeduplicateDvMaterializerTest, TestWriteProviderRejectsCompactionChanges) {
    auto index =
        std::make_shared<IndexFileMeta>("HASH", "compact-index", 10, 1, std::nullopt, std::nullopt);
    ManifestEntryChanges input(1, /*drop_delete_file_stats=*/false);
    input.compact_index_files.emplace_back(FileKind::Add(), BinaryRow(0), 0, index);
    RealtimeDeduplicateCommitChangesProvider provider(std::move(input), {}, table_schema_, paths_,
                                                      /*scanner=*/nullptr, options_,
                                                      GetDefaultPool());
    ASSERT_NOK_WITH_MSG(provider.Provide(std::nullopt),
                        "must not contain physical compaction changes");
}

}  // namespace paimon::test
