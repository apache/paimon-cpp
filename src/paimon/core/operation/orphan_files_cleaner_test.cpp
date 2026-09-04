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

#include "paimon/orphan_files_cleaner.h"

#include <cstdint>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/core/index/pk/primary_key_index_source_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/orphan_files_cleaner_impl.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

Result<IndexManifestEntry> CreateSourceBackedBTreeEntry(
    const std::string& file_name, const BinaryRow& partition,
    const std::optional<std::string>& external_path, const std::shared_ptr<MemoryPool>& pool) {
    constexpr int64_t kRowCount = 3;
    PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexSourceMeta source_meta,
                           PrimaryKeyIndexSourceMeta::Create(
                               /*data_level=*/2, {{file_name + ".data", kRowCount}}));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> source_meta_bytes, source_meta.Serialize(pool));
    auto index_file = std::make_shared<IndexFileMeta>(
        "btree", file_name, /*file_size=*/7, kRowCount, /*dv_ranges=*/std::nullopt, external_path,
        GlobalIndexMeta(/*row_range_start=*/0, /*row_range_end=*/kRowCount - 1,
                        /*index_field_id=*/0, /*extra_field_ids=*/std::nullopt,
                        /*index_meta=*/nullptr, source_meta_bytes));
    return IndexManifestEntry(FileKind::Add(), partition, /*bucket=*/0, index_file);
}

Snapshot WithIndexManifest(const Snapshot& snapshot, const std::string& index_manifest) {
    return Snapshot(snapshot.Id(), snapshot.SchemaId(), snapshot.BaseManifestList(),
                    snapshot.BaseManifestListSize(), snapshot.DeltaManifestList(),
                    snapshot.DeltaManifestListSize(), snapshot.ChangelogManifestList(),
                    snapshot.ChangelogManifestListSize(), index_manifest, snapshot.CommitUser(),
                    snapshot.CommitIdentifier(), snapshot.GetCommitKind(), snapshot.TimeMillis(),
                    snapshot.TotalRecordCount(), snapshot.DeltaRecordCount(),
                    snapshot.ChangelogRecordCount(), snapshot.Watermark(), snapshot.Statistics(),
                    snapshot.Properties(), snapshot.NextRowId());
}

Snapshot WithDataManifests(const Snapshot& snapshot, const std::string& base_manifest_list,
                           const std::optional<int64_t>& base_manifest_list_size,
                           const std::string& delta_manifest_list,
                           const std::optional<int64_t>& delta_manifest_list_size) {
    return Snapshot(snapshot.Id(), snapshot.SchemaId(), base_manifest_list, base_manifest_list_size,
                    delta_manifest_list, delta_manifest_list_size, snapshot.ChangelogManifestList(),
                    snapshot.ChangelogManifestListSize(), snapshot.IndexManifest(),
                    snapshot.CommitUser(), snapshot.CommitIdentifier(), snapshot.GetCommitKind(),
                    snapshot.TimeMillis(), snapshot.TotalRecordCount(), snapshot.DeltaRecordCount(),
                    snapshot.ChangelogRecordCount(), snapshot.Watermark(), snapshot.Statistics(),
                    snapshot.Properties(), snapshot.NextRowId());
}

}  // namespace

TEST(OrphanFilesCleanerTest, TestSupportToClean) {
    ASSERT_TRUE(
        OrphanFilesCleanerImpl::SupportToClean("data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.orc"));
    ASSERT_TRUE(OrphanFilesCleanerImpl::SupportToClean(
        "data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.parquet"));
    ASSERT_TRUE(
        OrphanFilesCleanerImpl::SupportToClean("data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.avro"));
    ASSERT_TRUE(OrphanFilesCleanerImpl::SupportToClean(
        "data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.parquet"));
    ASSERT_TRUE(
        OrphanFilesCleanerImpl::SupportToClean("manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-0"));
    ASSERT_TRUE(OrphanFilesCleanerImpl::SupportToClean(
        "manifest-list-469f3a0f-f6f1-4027-91bf-d1e897e8ea23-1"));
    ASSERT_TRUE(OrphanFilesCleanerImpl::SupportToClean(
        ".snapshot-2.13c988c3-784d-493d-8884-016ddddb1fc2.tmp"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean("tmp"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean("snapshot-1"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean("schema-0"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean("bucket-0"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean(
        "changelog-ce64d06d-c4cd-456b-a1b3-ae570042620f-0.parquet"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean(
        "data-5515726b-0f0f-4556-a942-e795e9f94c4a-0.orc.index"));
    ASSERT_TRUE(
        OrphanFilesCleanerImpl::SupportToClean("index-aa60193d-d7cd-434f-bc1a-c1adb210e1f7-0"));
    ASSERT_TRUE(OrphanFilesCleanerImpl::SupportToClean(
        "index-manifest-aa60193d-d7cd-434f-bc1a-c1adb210e1f7-0"));
    ASSERT_TRUE(OrphanFilesCleanerImpl::SupportToClean(
        "btree-global-index-aa60193d-d7cd-434f-bc1a-c1adb210e1f7.index"));
    ASSERT_FALSE(
        OrphanFilesCleanerImpl::SupportToClean("data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.json"));
    ASSERT_FALSE(OrphanFilesCleanerImpl::SupportToClean(
        "some_data-2d5ea1ea-77c1-47ff-bb87-19a509962a37-0.orc"));
}

TEST(OrphanFilesCleanerTest, TestPkTable) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_mor.db/pk_table_with_mor/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<CleanContext> clean_context,
        clean_context_builder.AddOption(Options::FILE_SYSTEM, "local").WithOlderThanMs(0).Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
    ASSERT_TRUE(cleaned_paths.empty());
}

TEST(OrphanFilesCleanerTest, TestRetainLiveDataFileExtraFiles) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_mor.db/pk_table_with_mor/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();

    CleanContextBuilder preparation_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> preparation_context,
                         preparation_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                             .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto preparation_cleaner,
                         OrphanFilesCleaner::Create(std::move(preparation_context)));
    auto* cleaner_impl = dynamic_cast<OrphanFilesCleanerImpl*>(preparation_cleaner.get());
    ASSERT_NE(cleaner_impl, nullptr);

    const std::string snapshot_path = PathUtil::JoinPath(table_path, "snapshot/snapshot-3");
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, Snapshot::FromPath(file_system, snapshot_path));
    std::vector<ManifestFileMeta> manifest_metas;
    ASSERT_OK(cleaner_impl->manifest_list_->ReadDataManifests(snapshot, &manifest_metas));
    ASSERT_FALSE(manifest_metas.empty());
    std::vector<ManifestEntry> manifest_entries;
    ASSERT_OK(cleaner_impl->manifest_file_->Read(manifest_metas.front().FileName(),
                                                 /*filter=*/nullptr, &manifest_entries));
    ASSERT_FALSE(manifest_entries.empty());

    const std::string extra_file_name = "data-live-extra.orc";
    const ManifestEntry& source_entry = manifest_entries.front();
    auto file_with_extra = source_entry.File()->CopyWithExtraFiles({extra_file_name});
    ManifestEntry entry_with_extra(source_entry.Kind(), source_entry.Partition(),
                                   source_entry.Bucket(), source_entry.TotalBuckets(),
                                   file_with_extra);
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestFileMeta> extra_manifest_metas,
                         cleaner_impl->manifest_file_->Write({entry_with_extra}));
    using ManifestListWithSize = std::pair<std::string, int64_t>;
    ASSERT_OK_AND_ASSIGN(ManifestListWithSize extra_manifest_list,
                         cleaner_impl->manifest_list_->Write(extra_manifest_metas));

    Snapshot snapshot_with_extra =
        WithDataManifests(snapshot, extra_manifest_list.first, extra_manifest_list.second,
                          snapshot.DeltaManifestList(), snapshot.DeltaManifestListSize());
    ASSERT_OK_AND_ASSIGN(std::string snapshot_json, snapshot_with_extra.ToJsonString());
    ASSERT_OK(file_system->WriteFile(snapshot_path, snapshot_json, /*overwrite=*/true));

    const std::string extra_file_path =
        PathUtil::JoinPath(table_path, "p0=0/p1=0/bucket-0/" + extra_file_name);
    ASSERT_OK(file_system->WriteFile(extra_file_path, "extra", /*overwrite=*/false));

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                             .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());

    ASSERT_OK_AND_ASSIGN(bool extra_file_exists, file_system->Exists(extra_file_path));
    ASSERT_TRUE(extra_file_exists);
    for (const std::string& cleaned_path : cleaned_paths) {
        ASSERT_NE(PathUtil::GetName(cleaned_path), extra_file_name);
    }
}

TEST(OrphanFilesCleanerTest, TestTableWithTag) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK(file_system->WriteFile(PathUtil::JoinPath(table_path, "tag/tag-1"), " ", true));

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local").Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_NOK_WITH_MSG(cleaner->Clean(),
                        "OrphanFilesCleaner do not support cleaning table with tag");
}

TEST(OrphanFilesCleanerTest, TestTableWithBranch) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();
    ASSERT_OK(file_system->WriteFile(PathUtil::JoinPath(table_path, "branch/branch-1"), " ", true));

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local").Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_NOK_WITH_MSG(cleaner->Clean(),
                        "OrphanFilesCleaner do not support cleaning table with branch");
}

TEST(OrphanFilesCleanerTest, TestTableWithIndex) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/append_with_bsi.db/append_with_bsi/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                             .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
    ASSERT_TRUE(cleaned_paths.empty());
}

TEST(OrphanFilesCleanerTest, TestTableWithBrokenSnapshot) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto file_system = std::make_shared<LocalFileSystem>();
    auto check_result = [](const std::set<std::string>& actual,
                           const std::set<std::string>& expected) -> bool {
        std::set<std::string> file_names;
        for (const auto& file_path : actual) {
            file_names.insert(PathUtil::GetName(file_path));
        }
        return file_names == expected;
    };

    // test with non-exist manifest list, which manifest has reference by other manifest-list, so it
    // will not be cleaned
    {
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        ASSERT_OK(file_system->Delete(PathUtil::JoinPath(
            table_path, "manifest/manifest-list-616d1847-a02c-495f-9cca-2c8b7def0fec-1")));

        CleanContextBuilder clean_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                             clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                                 .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
        ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
        ASSERT_TRUE(cleaned_paths.empty());
    }
    // test with non-exist manifest list, which manifest has no other reference, so it will be
    // cleaned
    {
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        ASSERT_OK(file_system->Delete(PathUtil::JoinPath(
            table_path, "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-3")));

        CleanContextBuilder clean_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                             clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                                 .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
        ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
        ASSERT_TRUE(
            check_result(cleaned_paths, {"data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc",
                                         "manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-1"}));
    }
    // test with non-exist manifest
    {
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        ASSERT_OK(file_system->Delete(PathUtil::JoinPath(
            table_path, "manifest/manifest-f8b15cfc-437a-4d21-a6a0-e45b639ae7ed-0")));

        CleanContextBuilder clean_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                             clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                                 .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
        ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
        ASSERT_TRUE(
            check_result(cleaned_paths, {"data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc",
                                         "data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc"}));
    }
    // test with non-exist data file
    {
        std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
        auto dir = UniqueTestDirectory::Create();
        std::string table_path = dir->Str();
        ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
        auto file_system = std::make_shared<LocalFileSystem>();
        ASSERT_OK(file_system->Delete(PathUtil::JoinPath(
            table_path, "f1=10/bucket-0/data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc")));

        CleanContextBuilder clean_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                             clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                                 .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
        ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());
        ASSERT_TRUE(cleaned_paths.empty());
    }
}

TEST(OrphanFilesCleanerTest, TestTableWithChangelog) {
    std::string test_data_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();
    auto snapshot_str = R"({
  "version" : 3,
  "id" : 6,
  "schemaId" : 0,
  "baseManifestList" : "manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-0",
  "deltaManifestList" : "manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-1",
  "changelogManifestList" : "manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-2",
  "commitUser" : "febb1e71-79fc-4abc-9b9d-464ecbc198f7",
  "commitIdentifier" : 9223372036854775807,
  "commitKind" : "APPEND",
  "timeMillis" : 1721615035363,
  "totalRecordCount" : 11,
  "deltaRecordCount" : 1,
  "changelogRecordCount" : 0
})";
    ASSERT_OK(file_system->WriteFile(PathUtil::JoinPath(table_path, "snapshot/snapshot-6"),
                                     snapshot_str, true));

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local").Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_NOK_WITH_MSG(cleaner->Clean(), "OrphanFilesCleaner do not support clean changelog");
}

TEST(OrphanFilesCleanerTest, TestTableWithIndexManifest) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_mor.db/pk_table_with_mor/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    auto file_system = std::make_shared<LocalFileSystem>();
    auto external_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(external_dir);
    std::string external_index_root = "file:" + external_dir->Str();

    SchemaManager schema_manager(file_system, table_path);
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> optional_schema,
                         schema_manager.Latest());
    ASSERT_TRUE(optional_schema);
    const std::shared_ptr<TableSchema>& table_schema = optional_schema.value();
    std::map<std::string, std::string> raw_options = table_schema->Options();
    raw_options[Options::FILE_SYSTEM] = "local";
    raw_options[Options::GLOBAL_INDEX_EXTERNAL_PATH] = external_index_root;
    raw_options[Options::INDEX_FILE_IN_DATA_FILE_DIR] = "true";
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));
    std::shared_ptr<MemoryPool> memory_pool = GetDefaultPool();
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> external_paths, options.CreateExternalPaths());
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> global_index_external_path,
                         options.CreateGlobalIndexExternalPath());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            table_path, arrow_schema, table_schema->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_path, options.IndexFileInDataFileDir(), memory_pool));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexManifestFile> index_manifest_file,
                         IndexManifestFile::Create(file_system, options.GetManifestFormat(),
                                                   options.GetManifestCompression(), path_factory,
                                                   options.GetBucket(), memory_pool, options));
    ASSERT_OK_AND_ASSIGN(BinaryRow partition,
                         path_factory->ToBinaryRow({{"p0", "0"}, {"p1", "0"}}));

    const std::string live_internal_name = "btree-global-index-live.index";
    const std::string live_external_name = "btree-global-index-live-external.index";
    const std::string orphan_internal_name = "btree-global-index-orphan.index";
    const std::string live_external_path =
        PathUtil::JoinPath(external_index_root, live_external_name);
    ASSERT_OK_AND_ASSIGN(IndexManifestEntry live_internal,
                         CreateSourceBackedBTreeEntry(live_internal_name, partition,
                                                      /*external_path=*/std::nullopt, memory_pool));
    ASSERT_OK_AND_ASSIGN(IndexManifestEntry live_external,
                         CreateSourceBackedBTreeEntry(live_external_name, partition,
                                                      live_external_path, memory_pool));
    ASSERT_OK_AND_ASSIGN(IndexManifestEntry orphan_internal,
                         CreateSourceBackedBTreeEntry(orphan_internal_name, partition,
                                                      /*external_path=*/std::nullopt, memory_pool));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexPathFactory> index_path_factory,
                         path_factory->CreateIndexFileFactory(partition, /*bucket=*/0));
    const std::string live_internal_path = index_path_factory->ToPath(live_internal.index_file);
    ASSERT_EQ(PathUtil::JoinPath(table_path,
                                 "p0=0/p1=0/bucket-0/" + live_internal.index_file->FileName()),
              live_internal_path);
    ASSERT_EQ(live_external_path, index_path_factory->ToPath(live_external.index_file));
    const std::string orphan_internal_path = index_path_factory->ToPath(orphan_internal.index_file);
    ASSERT_OK(file_system->WriteFile(live_internal_path, "payload", /*overwrite=*/false));
    ASSERT_OK(file_system->WriteFile(live_external_path, "payload", /*overwrite=*/false));
    ASSERT_OK(file_system->WriteFile(orphan_internal_path, "payload", /*overwrite=*/false));

    ASSERT_OK_AND_ASSIGN(
        std::optional<std::string> live_manifest,
        index_manifest_file->WriteIndexFiles(std::nullopt, {live_internal, live_external}));
    ASSERT_TRUE(live_manifest);
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> orphan_manifest,
                         index_manifest_file->WriteIndexFiles(std::nullopt, {orphan_internal}));
    ASSERT_TRUE(orphan_manifest);

    const std::string snapshot_path = PathUtil::JoinPath(table_path, "snapshot/snapshot-3");
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, Snapshot::FromPath(file_system, snapshot_path));
    Snapshot indexed_snapshot = WithIndexManifest(snapshot, live_manifest.value());
    ASSERT_OK_AND_ASSIGN(std::string indexed_snapshot_json, indexed_snapshot.ToJsonString());
    ASSERT_OK(file_system->WriteFile(snapshot_path, indexed_snapshot_json, /*overwrite=*/true));

    CleanContextBuilder clean_context_builder(table_path);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CleanContext> clean_context,
                         clean_context_builder.AddOption(Options::FILE_SYSTEM, "local")
                             .AddOption(Options::GLOBAL_INDEX_EXTERNAL_PATH, external_index_root)
                             .AddOption(Options::INDEX_FILE_IN_DATA_FILE_DIR, "true")
                             .WithOlderThanMs(std::numeric_limits<int64_t>::max())
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto cleaner, OrphanFilesCleaner::Create(std::move(clean_context)));
    ASSERT_OK_AND_ASSIGN(std::set<std::string> cleaned_paths, cleaner->Clean());

    ASSERT_OK_AND_ASSIGN(bool live_internal_exists, file_system->Exists(live_internal_path));
    ASSERT_TRUE(live_internal_exists);
    ASSERT_OK_AND_ASSIGN(bool live_external_exists, file_system->Exists(live_external_path));
    ASSERT_TRUE(live_external_exists);
    ASSERT_OK_AND_ASSIGN(
        bool live_manifest_exists,
        file_system->Exists(path_factory->ToManifestFilePath(live_manifest.value())));
    ASSERT_TRUE(live_manifest_exists);
    ASSERT_OK_AND_ASSIGN(bool orphan_internal_exists, file_system->Exists(orphan_internal_path));
    ASSERT_FALSE(orphan_internal_exists);
    ASSERT_OK_AND_ASSIGN(
        bool orphan_manifest_exists,
        file_system->Exists(path_factory->ToManifestFilePath(orphan_manifest.value())));
    ASSERT_FALSE(orphan_manifest_exists);

    std::set<std::string> cleaned_names;
    for (const std::string& path : cleaned_paths) {
        cleaned_names.insert(PathUtil::GetName(path));
    }
    ASSERT_TRUE(cleaned_names.count(orphan_internal_name));
    ASSERT_TRUE(cleaned_names.count(orphan_manifest.value()));
    ASSERT_FALSE(cleaned_names.count(live_internal_name));
    ASSERT_FALSE(cleaned_names.count(live_external_name));
    ASSERT_FALSE(cleaned_names.count(live_manifest.value()));
}

}  // namespace paimon::test
