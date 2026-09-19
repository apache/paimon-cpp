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

#include "paimon/core/operation/file_system_write_restore.h"

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/key_value_file_store_scan.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/index_file_path_factories.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/executor.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/scan_context.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

Result<std::unique_ptr<FileSystemWriteRestore>> CreateRestore(
    const std::string& table_path, const std::shared_ptr<MemoryPool>& pool) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto schema_manager = std::make_shared<SchemaManager>(fs, table_path);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                           schema_manager->ReadSchema(/*schema_id=*/0));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(table_schema->Options()));
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            table_path, arrow_schema, table_schema->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_path, options.IndexFileInDataFileDir(), pool));
    auto snapshot_manager = std::make_shared<SnapshotManager>(fs, table_path);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(fs, options.GetManifestFormat(), options.GetManifestCompression(),
                             path_factory, options.GetCache(), pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(fs, options.GetManifestFormat(), options.GetManifestCompression(),
                             path_factory, options.GetManifestTargetFileSize(), pool, options,
                             partition_schema));
    auto scan_filter = std::make_shared<ScanFilter>(
        /*predicate=*/nullptr,
        /*partition_filters=*/std::vector<std::map<std::string, std::string>>{},
        /*bucket_filter=*/std::nullopt);
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<KeyValueFileStoreScan> scan,
        KeyValueFileStoreScan::Create(snapshot_manager, schema_manager, manifest_list,
                                      manifest_file, table_schema, arrow_schema, scan_filter,
                                      options, CreateDefaultExecutor(), pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexManifestFile> index_manifest_file,
        IndexManifestFile::Create(fs, options.GetManifestFormat(), options.GetManifestCompression(),
                                  path_factory, options.GetBucket(), pool, options));
    auto index_file_handler = std::make_shared<IndexFileHandler>(
        fs, std::move(index_manifest_file), std::make_shared<IndexFilePathFactories>(path_factory),
        options.DeletionVectorsBitmap64(), pool);
    return std::make_unique<FileSystemWriteRestore>(snapshot_manager, std::move(scan),
                                                    index_file_handler);
}

}  // namespace

TEST(FileSystemWriteRestoreTest, LatestCommittedIdentifierNoSnapshot) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto snapshot_manager = std::make_shared<SnapshotManager>(
        fs, paimon::test::GetDataDir() + "/orc/append_09.db/not_exist");

    FileSystemWriteRestore restore(snapshot_manager, /*scan=*/nullptr,
                                   /*index_file_handler=*/nullptr);

    ASSERT_OK_AND_ASSIGN(int64_t latest_identifier,
                         restore.LatestCommittedIdentifier("unknown_user"));
    ASSERT_EQ(latest_identifier, std::numeric_limits<int64_t>::min());
}

TEST(FileSystemWriteRestoreTest, LatestCommittedIdentifierWithSnapshot) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto snapshot_manager = std::make_shared<SnapshotManager>(
        fs, paimon::test::GetDataDir() + "/orc/append_09.db/append_09");

    FileSystemWriteRestore restore(snapshot_manager, /*scan=*/nullptr,
                                   /*index_file_handler=*/nullptr);

    ASSERT_OK_AND_ASSIGN(int64_t latest_identifier,
                         restore.LatestCommittedIdentifier("b02e4322-9c5f-41e1-a560-c0156fdf7b9c"));
    ASSERT_EQ(latest_identifier, std::numeric_limits<int64_t>::max());
}

TEST(FileSystemWriteRestoreTest, GetRestoreFilesReturnsEmptyWhenNoLatestSnapshot) {
    auto fs = std::make_shared<LocalFileSystem>();
    auto snapshot_manager = std::make_shared<SnapshotManager>(
        fs, paimon::test::GetDataDir() + "/orc/append_09.db/not_exist");

    FileSystemWriteRestore restore(snapshot_manager, /*scan=*/nullptr,
                                   /*index_file_handler=*/nullptr);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RestoreFiles> files,
                         restore.GetRestoreFiles(BinaryRow::EmptyRow(), /*bucket=*/0,
                                                 /*scan_deletion_vectors_index=*/true,
                                                 /*scan_source_index_payloads=*/false));
    ASSERT_FALSE(files->GetSnapshot().has_value());
    ASSERT_FALSE(files->TotalBuckets().has_value());
    ASSERT_TRUE(files->DataFiles().empty());
    ASSERT_TRUE(files->DeleteVectorsIndex().empty());
}

TEST(FileSystemWriteRestoreTest, RestoresSourceBackedIndexPayloads) {
    const std::string table_path =
        paimon::test::GetDataDir() + "/orc/pk_btree_source_meta.db/pk_btree_source_meta/";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileSystemWriteRestore> restore,
                         CreateRestore(table_path, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RestoreFiles> files,
                         restore->GetRestoreFiles(BinaryRow::EmptyRow(), /*bucket=*/0,
                                                  /*scan_deletion_vectors_index=*/false,
                                                  /*scan_source_index_payloads=*/true));

    ASSERT_EQ(files->PrimaryKeyIndexPayloads().size(), 1);
    const std::optional<GlobalIndexMeta>& global_index_meta =
        files->PrimaryKeyIndexPayloads()[0]->GetGlobalIndexMeta();
    ASSERT_TRUE(global_index_meta.has_value());
    ASSERT_NE(global_index_meta->source_meta, nullptr);
}

}  // namespace paimon::test
