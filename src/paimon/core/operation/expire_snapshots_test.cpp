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

#include "paimon/core/operation/expire_snapshots.h"

#include <cstdint>
#include <optional>
#include <set>
#include <utility>

#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/core/index/pk/primary_key_index_source_meta.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/core/utils/tag_manager.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/format/file_format.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class ExpireSnapshotsTest : public testing::Test {
 public:
    void SetUp() override {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        mem_pool_ = GetDefaultPool();
        executor_ = CreateDefaultExecutor();
        partition_keys_ = {"f0", "f3"};
        arrow::FieldVector fields = {arrow::field("f0", arrow::boolean()),
                                     arrow::field("f1", arrow::int8()),
                                     arrow::field("f2", arrow::int8()),
                                     arrow::field("f3", arrow::int16()),
                                     arrow::field("f4", arrow::int16()),
                                     arrow::field("f5", arrow::int32()),
                                     arrow::field("f6", arrow::int32()),
                                     arrow::field("f7", arrow::int64()),
                                     arrow::field("f8", arrow::int64()),
                                     arrow::field("f9", arrow::float32()),
                                     arrow::field("f10", arrow::float64()),
                                     arrow::field("f11", arrow::utf8()),
                                     arrow::field("f12", arrow::binary()),
                                     arrow::field("non-partition-field", arrow::int32())};
        schema_ = arrow::schema(fields);
        ASSERT_OK_AND_ASSIGN(partition_schema_,
                             FieldMapping::GetPartitionSchema(schema_, partition_keys_));
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = dir_->GetFileSystem();

        test_data_path_ = dir_->Str();
        path_factory_ = CreateFactory(test_data_path_);

        ASSERT_OK_AND_ASSIGN(
            manifest_list_,
            ManifestList::Create(fs_, options.GetManifestFormat(), options.GetManifestCompression(),
                                 path_factory_, options.GetCache(), mem_pool_));

        ASSERT_OK_AND_ASSIGN(
            manifest_file_,
            ManifestFile::Create(fs_, options.GetManifestFormat(), options.GetManifestCompression(),
                                 path_factory_, options.GetManifestTargetFileSize(), mem_pool_,
                                 options, partition_schema_));

        ASSERT_OK_AND_ASSIGN(index_manifest_file_,
                             IndexManifestFile::Create(
                                 fs_, options.GetManifestFormat(), options.GetManifestCompression(),
                                 path_factory_, options.GetBucket(), mem_pool_, options));
    }
    void TearDown() override {}

    template <typename K, typename T>
    static bool IsEqualMap(const std::map<K, T>& expected, const std::map<K, T>& actual) {
        if (&expected == &actual) {
            return true;
        }
        if (expected.size() != actual.size()) {
            return false;
        }
        for (const auto& [key, value] : actual) {
            auto iter = expected.find(key);
            if (iter == expected.end()) {
                return false;
            }
            if (iter->second == value) {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    std::shared_ptr<FileStorePathFactory> CreateFactory(const std::string& root) const {
        std::map<std::string, std::string> raw_options;
        raw_options[Options::FILE_FORMAT] = "orc";
        raw_options[Options::MANIFEST_FORMAT] = "orc";
        raw_options[Options::FILE_SYSTEM] = "local";
        EXPECT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));
        EXPECT_OK_AND_ASSIGN(std::vector<std::string> external_paths,
                             options.CreateExternalPaths());
        EXPECT_OK_AND_ASSIGN(std::optional<std::string> global_index_external_path,
                             options.CreateGlobalIndexExternalPath());
        EXPECT_OK_AND_ASSIGN(
            auto path_factory,
            FileStorePathFactory::Create(
                root, schema_, partition_keys_, options.GetPartitionDefaultName(),
                options.GetFileFormat()->Identifier(), options.DataFilePrefix(),
                options.LegacyPartitionNameEnabled(), external_paths, global_index_external_path,
                options.IndexFileInDataFileDir(), mem_pool_));
        return path_factory;
    }

    ManifestEntry CreateManifestEntry(const std::string& file_name, int32_t bucket,
                                      const FileKind& kind) const {
        int32_t arity = 2;
        BinaryRow row(arity);
        BinaryRowWriter writer(&row, 20, mem_pool_.get());
        writer.WriteBoolean(0, true);
        writer.WriteShort(1, 3);
        writer.Complete();

        auto data_file_meta = std::make_shared<DataFileMeta>(
            file_name, 1024, 8, DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), /*min_seq_no=*/16,
            /*max_seq_no=*/32,
            /*schema_id=*/1, /*level=*/2, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0), /*delete_row_count=*/3,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        return ManifestEntry(kind, row, bucket, /*total_buckets=*/3, data_file_meta);
    }

    Result<IndexManifestEntry> CreateSourceBackedBTreeEntry(
        const std::string& file_name, int32_t bucket,
        const std::optional<std::string>& external_path = std::nullopt) const {
        constexpr int64_t kRowCount = 3;
        PAIMON_ASSIGN_OR_RAISE(
            PrimaryKeyIndexSourceMeta source_meta,
            PrimaryKeyIndexSourceMeta::Create(
                /*data_level=*/2, {{fmt::format("{}.data", file_name), kRowCount}}));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> source_meta_bytes,
                               source_meta.Serialize(mem_pool_));
        auto index_file = std::make_shared<IndexFileMeta>(
            "btree", file_name, /*file_size=*/7, kRowCount, /*dv_ranges=*/std::nullopt,
            external_path,
            GlobalIndexMeta(/*row_range_start=*/0, /*row_range_end=*/kRowCount - 1,
                            /*index_field_id=*/5, /*extra_field_ids=*/std::nullopt,
                            /*index_meta=*/nullptr, source_meta_bytes));
        BinaryRow partition =
            CreateManifestEntry("partition-source.data", bucket, FileKind::Add()).Partition();
        return IndexManifestEntry(FileKind::Add(), partition, bucket, index_file);
    }

    Result<std::string> IndexFilePath(const IndexManifestEntry& entry) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<IndexPathFactory> index_path_factory,
            path_factory_->CreateIndexFileFactory(entry.partition, entry.bucket));
        return index_path_factory->ToPath(entry.index_file);
    }

 private:
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::string test_data_path_;
    std::vector<std::string> partition_keys_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::Schema> partition_schema_;
    std::shared_ptr<MemoryPool> mem_pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<ManifestList> manifest_list_;
    std::shared_ptr<ManifestFile> manifest_file_;
    std::shared_ptr<IndexManifestFile> index_manifest_file_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
};

TEST_F(ExpireSnapshotsTest, TestInvalidInput) {
    auto mgr = std::make_shared<SnapshotManager>(fs_, test_data_path_);
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ExpireSnapshots expire(mgr, path_factory_, manifest_list_, manifest_file_,
                               index_manifest_file_, fs_, options.GetExpireConfig(),
                               options.RealtimeEnabled(), executor_);
        ASSERT_OK_AND_ASSIGN(int32_t count, expire.Expire());
        ASSERT_EQ(count, 0);
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::SNAPSHOT_NUM_RETAINED_MIN, "0"}}));
        ExpireSnapshots expire(mgr, path_factory_, manifest_list_, manifest_file_,
                               index_manifest_file_, fs_, options.GetExpireConfig(),
                               options.RealtimeEnabled(), executor_);
        ASSERT_NOK(expire.Expire());
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::SNAPSHOT_NUM_RETAINED_MIN, "10"},
                                                   {Options::SNAPSHOT_NUM_RETAINED_MAX, "9"}}));
        ExpireSnapshots expire(mgr, path_factory_, manifest_list_, manifest_file_,
                               index_manifest_file_, fs_, options.GetExpireConfig(),
                               options.RealtimeEnabled(), executor_);
        ASSERT_NOK(expire.Expire());
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::SNAPSHOT_NUM_RETAINED_MIN, "10"},
                                                   {Options::SNAPSHOT_NUM_RETAINED_MAX, "10"}}));
        ExpireSnapshots expire(mgr, path_factory_, manifest_list_, manifest_file_,
                               index_manifest_file_, fs_, options.GetExpireConfig(),
                               options.RealtimeEnabled(), executor_);
        ASSERT_OK_AND_ASSIGN(int32_t count, expire.Expire());
        ASSERT_EQ(count, 0);
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::SNAPSHOT_EXPIRE_LIMIT, "-1"},
                                                   {Options::SNAPSHOT_NUM_RETAINED_MIN, "10"},
                                                   {Options::SNAPSHOT_NUM_RETAINED_MAX, "10"}}));
        ExpireSnapshots expire(mgr, path_factory_, manifest_list_, manifest_file_,
                               index_manifest_file_, fs_, options.GetExpireConfig(),
                               options.RealtimeEnabled(), executor_);
        ASSERT_NOK(expire.Expire());
    }
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::SNAPSHOT_NUM_RETAINED_MIN, "10"},
                                                   {Options::SNAPSHOT_NUM_RETAINED_MAX, "10"}}));
        ExpireSnapshots expire(nullptr, path_factory_, manifest_list_, manifest_file_,
                               index_manifest_file_, fs_, options.GetExpireConfig(),
                               options.RealtimeEnabled(), executor_);
        ASSERT_NOK(expire.Expire());
    }
}

TEST_F(ExpireSnapshotsTest, TestExpireSourceBackedBTreeIndexFiles) {
    auto mgr = std::make_shared<SnapshotManager>(fs_, test_data_path_);
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    ExpireSnapshots expire(mgr, path_factory_, manifest_list_, manifest_file_, index_manifest_file_,
                           fs_, options.GetExpireConfig(), options.RealtimeEnabled(), executor_);

    ASSERT_OK_AND_ASSIGN(IndexManifestEntry shared_index,
                         CreateSourceBackedBTreeEntry("shared-btree.index", /*bucket=*/0));
    std::string expired_external_path = test_data_path_ + "/external-expired-btree.index";
    ASSERT_OK_AND_ASSIGN(
        IndexManifestEntry expired_index,
        CreateSourceBackedBTreeEntry("expired-btree.index", /*bucket=*/0, expired_external_path));
    ASSERT_OK_AND_ASSIGN(IndexManifestEntry retained_index,
                         CreateSourceBackedBTreeEntry("retained-btree.index", /*bucket=*/0));
    ASSERT_OK_AND_ASSIGN(std::string shared_path, IndexFilePath(shared_index));
    ASSERT_OK_AND_ASSIGN(std::string expired_path, IndexFilePath(expired_index));
    ASSERT_OK_AND_ASSIGN(std::string retained_path, IndexFilePath(retained_index));
    ASSERT_OK(fs_->WriteFile(shared_path, "payload", /*overwrite=*/false));
    ASSERT_OK(fs_->WriteFile(expired_path, "payload", /*overwrite=*/false));
    ASSERT_OK(fs_->WriteFile(retained_path, "payload", /*overwrite=*/false));

    ASSERT_OK_AND_ASSIGN(
        std::optional<std::string> expired_manifest,
        index_manifest_file_->WriteIndexFiles(std::nullopt, {shared_index, expired_index}));
    ASSERT_TRUE(expired_manifest);
    ASSERT_OK_AND_ASSIGN(
        std::optional<std::string> retained_manifest,
        index_manifest_file_->WriteIndexFiles(std::nullopt, {shared_index, retained_index}));
    ASSERT_TRUE(retained_manifest);

    std::set<std::string> skipping_set;
    ASSERT_OK(expire.AddIndexManifestToSkippingSet(retained_manifest, &skipping_set));
    ASSERT_GT(skipping_set.count(retained_manifest.value()), 0);
    ASSERT_GT(skipping_set.count(shared_index.index_file->FileName()), 0);
    ASSERT_GT(skipping_set.count(retained_index.index_file->FileName()), 0);

    ASSERT_OK(expire.CleanUnusedIndexManifest(expired_manifest, &skipping_set));
    ASSERT_OK_AND_ASSIGN(bool shared_exists, fs_->Exists(shared_path));
    ASSERT_TRUE(shared_exists);
    ASSERT_OK_AND_ASSIGN(bool expired_exists, fs_->Exists(expired_path));
    ASSERT_FALSE(expired_exists);
    ASSERT_OK_AND_ASSIGN(bool retained_exists, fs_->Exists(retained_path));
    ASSERT_TRUE(retained_exists);
    ASSERT_OK_AND_ASSIGN(bool expired_manifest_exists,
                         fs_->Exists(path_factory_->ToManifestFilePath(expired_manifest.value())));
    ASSERT_FALSE(expired_manifest_exists);
    ASSERT_OK_AND_ASSIGN(bool retained_manifest_exists,
                         fs_->Exists(path_factory_->ToManifestFilePath(retained_manifest.value())));
    ASSERT_TRUE(retained_manifest_exists);

    ASSERT_OK(
        expire.CleanUnusedIndexManifest(std::string("missing-index-manifest"), &skipping_set));
    std::set<std::string> missing_skipping_set;
    ASSERT_NOK(expire.AddIndexManifestToSkippingSet(std::string("missing-index-manifest"),
                                                    &missing_skipping_set));
}

TEST_F(ExpireSnapshotsTest, TestExpireKeepsSourceBackedBTreeIndexReferencedByTag) {
    auto mgr = std::make_shared<SnapshotManager>(fs_, test_data_path_);
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::SNAPSHOT_NUM_RETAINED_MIN, "1"},
                                               {Options::SNAPSHOT_NUM_RETAINED_MAX, "1"}}));
    ExpireSnapshots expire(mgr, path_factory_, manifest_list_, manifest_file_, index_manifest_file_,
                           fs_, options.GetExpireConfig(), options.RealtimeEnabled(), executor_);

    ASSERT_OK_AND_ASSIGN(IndexManifestEntry tagged_index,
                         CreateSourceBackedBTreeEntry("tagged-btree.index", /*bucket=*/0));
    ASSERT_OK_AND_ASSIGN(std::string tagged_index_path, IndexFilePath(tagged_index));
    ASSERT_OK(fs_->WriteFile(tagged_index_path, "payload", /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> tagged_index_manifest,
                         index_manifest_file_->WriteIndexFiles(std::nullopt, {tagged_index}));
    ASSERT_TRUE(tagged_index_manifest);

    using ManifestListMeta = std::pair<std::string, int64_t>;
    ManifestEntry tagged_data = CreateManifestEntry("tagged.data", /*bucket=*/0, FileKind::Add());
    ASSERT_OK_AND_ASSIGN(std::string tagged_bucket_path,
                         path_factory_->BucketPath(tagged_data.Partition(), tagged_data.Bucket()));
    ASSERT_OK(fs_->Mkdirs(tagged_bucket_path));
    std::string tagged_data_path = PathUtil::JoinPath(tagged_bucket_path, tagged_data.FileName());
    ASSERT_OK(fs_->WriteFile(tagged_data_path, "data", /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestFileMeta> tagged_data_manifests,
                         manifest_file_->Write({tagged_data}));
    ASSERT_OK_AND_ASSIGN(ManifestListMeta tagged_base_manifest_list,
                         manifest_list_->Write(tagged_data_manifests));
    ASSERT_OK_AND_ASSIGN(ManifestListMeta tagged_delta_manifest_list, manifest_list_->Write({}));
    ASSERT_OK_AND_ASSIGN(ManifestListMeta retained_base_manifest_list, manifest_list_->Write({}));
    ManifestEntry deleted_tagged_data =
        CreateManifestEntry(tagged_data.FileName(), /*bucket=*/0, FileKind::Delete());
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestFileMeta> retained_data_manifests,
                         manifest_file_->Write({deleted_tagged_data}));
    ASSERT_OK_AND_ASSIGN(ManifestListMeta retained_delta_manifest_list,
                         manifest_list_->Write(retained_data_manifests));

    Snapshot tagged_snapshot(
        /*id=*/1, /*schema_id=*/0, tagged_base_manifest_list.first,
        tagged_base_manifest_list.second, tagged_delta_manifest_list.first,
        tagged_delta_manifest_list.second, /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt, tagged_index_manifest,
        /*commit_user=*/"test", /*commit_identifier=*/1, Snapshot::CommitKind::Append(),
        /*time_millis=*/0, /*total_record_count=*/3, /*delta_record_count=*/3,
        /*changelog_record_count=*/std::nullopt, /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt, /*properties=*/std::nullopt, /*next_row_id=*/std::nullopt);
    Snapshot retained_snapshot(
        /*id=*/2, /*schema_id=*/0, retained_base_manifest_list.first,
        retained_base_manifest_list.second, retained_delta_manifest_list.first,
        retained_delta_manifest_list.second, /*changelog_manifest_list=*/std::nullopt,
        /*changelog_manifest_list_size=*/std::nullopt, /*index_manifest=*/std::nullopt,
        /*commit_user=*/"test", /*commit_identifier=*/2, Snapshot::CommitKind::Append(),
        /*time_millis=*/0, /*total_record_count=*/3, /*delta_record_count=*/0,
        /*changelog_record_count=*/std::nullopt, /*watermark=*/std::nullopt,
        /*statistics=*/std::nullopt, /*properties=*/std::nullopt, /*next_row_id=*/std::nullopt);

    ASSERT_OK(fs_->Mkdirs(mgr->SnapshotDirectory()));
    ASSERT_OK_AND_ASSIGN(std::string tagged_snapshot_json, tagged_snapshot.ToJsonString());
    ASSERT_OK(fs_->WriteFile(mgr->SnapshotPath(tagged_snapshot.Id()), tagged_snapshot_json,
                             /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::string retained_snapshot_json, retained_snapshot.ToJsonString());
    ASSERT_OK(fs_->WriteFile(mgr->SnapshotPath(retained_snapshot.Id()), retained_snapshot_json,
                             /*overwrite=*/false));

    Tag tag(tagged_snapshot.Version(), tagged_snapshot.Id(), tagged_snapshot.SchemaId(),
            tagged_snapshot.BaseManifestList(), tagged_snapshot.BaseManifestListSize(),
            tagged_snapshot.DeltaManifestList(), tagged_snapshot.DeltaManifestListSize(),
            tagged_snapshot.ChangelogManifestList(), tagged_snapshot.ChangelogManifestListSize(),
            tagged_snapshot.IndexManifest(), tagged_snapshot.CommitUser(),
            tagged_snapshot.CommitIdentifier(), tagged_snapshot.GetCommitKind(),
            tagged_snapshot.TimeMillis(), tagged_snapshot.TotalRecordCount(),
            tagged_snapshot.DeltaRecordCount(), tagged_snapshot.ChangelogRecordCount(),
            tagged_snapshot.Watermark(), tagged_snapshot.Statistics(), tagged_snapshot.Properties(),
            tagged_snapshot.NextRowId(), /*tag_create_time=*/std::nullopt,
            /*tag_time_retained=*/std::nullopt);
    TagManager tag_manager(fs_, mgr->RootPath(), mgr->Branch());
    ASSERT_OK(fs_->Mkdirs(tag_manager.TagDirectory()));
    ASSERT_OK_AND_ASSIGN(std::string tag_json, tag.ToJsonString());
    ASSERT_OK(fs_->WriteFile(tag_manager.TagPath("tagged"), tag_json, /*overwrite=*/false));

    ASSERT_OK_AND_ASSIGN(int32_t expired_count, expire.Expire());
    ASSERT_EQ(expired_count, 1);
    ASSERT_OK_AND_ASSIGN(bool expired_snapshot_exists,
                         fs_->Exists(mgr->SnapshotPath(tagged_snapshot.Id())));
    ASSERT_FALSE(expired_snapshot_exists);
    ASSERT_OK_AND_ASSIGN(
        bool tagged_index_manifest_exists,
        fs_->Exists(path_factory_->ToManifestFilePath(tagged_index_manifest.value())));
    ASSERT_TRUE(tagged_index_manifest_exists);
    ASSERT_OK_AND_ASSIGN(bool tagged_index_exists, fs_->Exists(tagged_index_path));
    ASSERT_TRUE(tagged_index_exists);
    ASSERT_OK_AND_ASSIGN(bool tagged_data_exists, fs_->Exists(tagged_data_path));
    ASSERT_TRUE(tagged_data_exists);
}

TEST_F(ExpireSnapshotsTest, TestGetDataFileToDelete) {
    auto mgr = std::make_shared<SnapshotManager>(fs_, test_data_path_);
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    {
        ExpireSnapshots expire(mgr, path_factory_, manifest_list_, manifest_file_,
                               index_manifest_file_, fs_, options.GetExpireConfig(),
                               options.RealtimeEnabled(), executor_);
        std::map<std::string, ManifestEntry> data_file_to_delete;
        std::vector<ManifestEntry> data_file_entries;
        data_file_entries.push_back(CreateManifestEntry("file1", /*bucket=*/0, FileKind::Delete()));
        data_file_entries.push_back(CreateManifestEntry("file2", /*bucket=*/1, FileKind::Delete()));
        data_file_entries.push_back(CreateManifestEntry("file1", /*bucket=*/0, FileKind::Add()));
        data_file_entries.push_back(CreateManifestEntry("file3", /*bucket=*/2, FileKind::Delete()));
        ASSERT_OK(expire.GetDataFilesToDelete(data_file_entries, &data_file_to_delete));
        ASSERT_TRUE(
            IsEqualMap(data_file_to_delete,
                       {{test_data_path_ + "/f0=true/f3=3/bucket-1/file2", data_file_entries[1]},
                        {test_data_path_ + "/f0=true/f3=3/bucket-2/file3", data_file_entries[3]}}));
    }
    {
        ExpireSnapshots expire(mgr, path_factory_, manifest_list_, manifest_file_,
                               index_manifest_file_, fs_, options.GetExpireConfig(),
                               options.RealtimeEnabled(), executor_);
        std::map<std::string, ManifestEntry> data_file_to_delete;
        std::vector<ManifestEntry> data_file_entries;
        data_file_entries.push_back(CreateManifestEntry("file1", /*bucket=*/0, FileKind::Add()));
        data_file_entries.push_back(CreateManifestEntry("file2", /*bucket=*/1, FileKind::Delete()));
        data_file_entries.push_back(CreateManifestEntry("file1", /*bucket=*/0, FileKind::Delete()));
        data_file_entries.push_back(CreateManifestEntry("file3", /*bucket=*/2, FileKind::Delete()));
        ASSERT_OK(expire.GetDataFilesToDelete(data_file_entries, &data_file_to_delete));
        ASSERT_TRUE(
            IsEqualMap(data_file_to_delete,
                       {{test_data_path_ + "/f0=true/f3=3/bucket-0/file1", data_file_entries[2]},
                        {test_data_path_ + "/f0=true/f3=3/bucket-1/file2", data_file_entries[1]},
                        {test_data_path_ + "/f0=true/f3=3/bucket-2/file3", data_file_entries[3]}}));
    }
}

}  // namespace paimon::test
