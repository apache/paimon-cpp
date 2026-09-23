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

#include "paimon/core/utils/file_store_path_factory.h"

#include <atomic>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <variant>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_checkpoint_path_factory.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"

namespace paimon::test {

class FileStorePathFactoryTest : public ::testing::Test {
 public:
    void SetUp() override {
        mem_pool_ = GetDefaultPool();
    }
    void TearDown() override {}

    std::shared_ptr<FileStorePathFactory> CreateFactory(const std::string& root) const {
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
        auto schema = arrow::schema(fields);

        std::map<std::string, std::string> raw_options;
        raw_options[Options::FILE_FORMAT] = "mock_format";
        raw_options[Options::FILE_SYSTEM] = "local";
        EXPECT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));
        EXPECT_OK_AND_ASSIGN(std::vector<std::string> external_paths,
                             options.CreateExternalPaths());
        EXPECT_OK_AND_ASSIGN(auto path_factory,
                             FileStorePathFactory::Create(
                                 root, schema, {"f0", "f3"}, options.GetPartitionDefaultName(),
                                 options.GetFileFormat()->Identifier(), options.DataFilePrefix(),
                                 options.LegacyPartitionNameEnabled(), external_paths,
                                 /*global_index_external_path=*/std::nullopt,
                                 options.IndexFileInDataFileDir(), mem_pool_));
        return path_factory;
    }

    void CheckPartition(const std::optional<int64_t>& dt, const std::optional<int32_t> hr,
                        const FileStorePathFactory& path_factory,
                        const std::string& expected) const {
        BinaryRow partition(2);
        BinaryRowWriter writer(&partition, 1024, mem_pool_.get());
        if (dt != std::nullopt) {
            writer.WriteLong(0, dt.value());
        } else {
            writer.SetNullAt(0);
        }
        if (hr != std::nullopt) {
            writer.WriteInt(1, hr.value());
        } else {
            writer.SetNullAt(1);
        }
        writer.Complete();
        ASSERT_OK_AND_ASSIGN(auto data_file_path_factory,
                             path_factory.CreateDataFilePathFactory(partition, 123));
        ASSERT_EQ(data_file_path_factory->ToPath("my-data-file-name"),
                  path_factory.root_ + expected + "/bucket-123/my-data-file-name");
    }

 private:
    std::shared_ptr<MemoryPool> mem_pool_;
};

TEST_F(FileStorePathFactoryTest, TestManifestPaths) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    auto path_factory = CreateFactory(dir->Str());
    std::string uuid = path_factory->UUID();
    ASSERT_EQ(FileStorePathFactory::ManifestPath(dir->Str()), dir->Str() + "/manifest");
    for (int32_t i = 0; i < 20; i++) {
        std::string manifest_path = path_factory->NewManifestFile();
        ASSERT_EQ(manifest_path,
                  dir->Str() + "/manifest/manifest-" + uuid + "-" + std::to_string(i));
    }
    std::string manifest_file_path = path_factory->ToManifestFilePath("my-manifest-file-name");
    ASSERT_EQ(manifest_file_path, dir->Str() + "/manifest/my-manifest-file-name");

    for (int32_t i = 0; i < 20; i++) {
        std::string manifest_path = path_factory->NewManifestList();
        ASSERT_EQ(manifest_path,
                  dir->Str() + "/manifest/manifest-list-" + uuid + "-" + std::to_string(i));
    }
    std::string manifest_list_path = path_factory->ToManifestListPath("my-manifest-list-file-name");
    ASSERT_EQ(manifest_list_path, dir->Str() + "/manifest/my-manifest-list-file-name");
}

TEST_F(FileStorePathFactoryTest, TestCreateFactoryWithEmptyRow) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    auto path_factory = CreateFactory(dir->Str());
    BinaryRow empty_row = BinaryRow::EmptyRow();
    ASSERT_NOK(path_factory->CreateDataFilePathFactory(empty_row, 123));
}

TEST_F(FileStorePathFactoryTest, TestCreateFactoryWithNoPartition) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::uint8()),
        arrow::field("f2", arrow::int8()),     arrow::field("f3", arrow::uint16()),
        arrow::field("f4", arrow::int16()),    arrow::field("f5", arrow::uint32()),
        arrow::field("hr", arrow::int32()),    arrow::field("f7", arrow::uint64()),
        arrow::field("dt", arrow::int64()),    arrow::field("f9", arrow::float32()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto path_factory,
                         FileStorePathFactory::Create(
                             dir->Str(), schema, {}, "default", /*identifier=*/"mock_format",
                             /*data_file_prefix=*/"data-", /*legacy_partition_name_enabled=*/true,
                             /*external_paths=*/std::vector<std::string>(),
                             /*global_index_external_path=*/std::nullopt,
                             /*index_file_in_data_file_dir=*/false, mem_pool_));
    ASSERT_OK_AND_ASSIGN(auto data_file_path_factory,
                         path_factory->CreateDataFilePathFactory(BinaryRow::EmptyRow(), 123));
    ASSERT_EQ(data_file_path_factory->ToPath("my-data-file-name"),
              path_factory->root_ + "/bucket-123/my-data-file-name");
}

TEST_F(FileStorePathFactoryTest, TestIndexManifestAndStatsPaths) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    auto path_factory = CreateFactory(dir->Str());
    std::string uuid = path_factory->UUID();
    ASSERT_EQ(FileStorePathFactory::ManifestPath(dir->Str()), dir->Str() + "/manifest");
    ASSERT_EQ(FileStorePathFactory::IndexPath(dir->Str()), dir->Str() + "/index");
    ASSERT_EQ(FileStorePathFactory::StatisticsPath(dir->Str()), dir->Str() + "/statistics");

    for (int32_t i = 0; i < 20; i++) {
        std::string manifest_path = path_factory->NewIndexManifestFile();
        ASSERT_EQ(manifest_path,
                  dir->Str() + "/manifest/index-manifest-" + uuid + "-" + std::to_string(i));
    }
    for (int32_t i = 0; i < 20; i++) {
        std::string stats_file_path = path_factory->NewStatsFile();
        ASSERT_EQ(stats_file_path,
                  dir->Str() + "/statistics/stats-" + uuid + "-" + std::to_string(i));
    }
    for (int32_t i = 0; i < 20; i++) {
        std::string index_file_path = path_factory->NewIndexFile();
        ASSERT_EQ(index_file_path, dir->Str() + "/index/index-" + uuid + "-" + std::to_string(i));
    }
    std::string index_file_path = path_factory->ToIndexFilePath("my-index-file-name");
    ASSERT_EQ(index_file_path, dir->Str() + "/index/my-index-file-name");
    std::string stats_file_path = path_factory->ToStatsFilePath("my-stats-file-name");
    ASSERT_EQ(stats_file_path, dir->Str() + "/statistics/my-stats-file-name");
}

TEST_F(FileStorePathFactoryTest, TestCreateDataFilePathFactoryWithPartition) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::uint8()),
        arrow::field("f2", arrow::int8()),     arrow::field("f3", arrow::uint16()),
        arrow::field("f4", arrow::int16()),    arrow::field("f5", arrow::uint32()),
        arrow::field("hr", arrow::int32()),    arrow::field("f7", arrow::uint64()),
        arrow::field("dt", arrow::int64()),    arrow::field("f9", arrow::float32()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};
    auto schema = arrow::schema(fields);

    ASSERT_OK_AND_ASSIGN(auto path_factory, FileStorePathFactory::Create(
                                                dir->Str(), schema, {"dt", "hr"}, "default",
                                                /*identifier=*/"mock_format",
                                                /*data_file_prefix=*/"data-",
                                                /*legacy_partition_name_enabled=*/true,
                                                /*external_paths=*/std::vector<std::string>(),
                                                /*global_index_external_path=*/std::nullopt,
                                                /*index_file_in_data_file_dir=*/false, mem_pool_));
    CheckPartition(20211224, 18, *path_factory, "/dt=20211224/hr=18");
    CheckPartition(20211224, std::nullopt, *path_factory, "/dt=20211224/hr=default");
    CheckPartition(std::nullopt, 16, *path_factory, "/dt=default/hr=16");
    CheckPartition(std::nullopt, std::nullopt, *path_factory, "/dt=default/hr=default");
}

TEST_F(FileStorePathFactoryTest, TestGetHierarchicalPartitionPath) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::uint8()),
        arrow::field("f2", arrow::int8()),     arrow::field("f3", arrow::uint16()),
        arrow::field("f4", arrow::int16()),    arrow::field("f5", arrow::uint32()),
        arrow::field("hr", arrow::int32()),    arrow::field("f7", arrow::uint64()),
        arrow::field("dt", arrow::int64()),    arrow::field("f9", arrow::float32()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};
    auto schema = arrow::schema(fields);

    ASSERT_OK_AND_ASSIGN(
        auto path_factory,
        FileStorePathFactory::Create(dir->Str(), schema, {"dt", "hr"}, "default",
                                     /*identifier=*/"mock_format", /*data_file_prefix=*/"data-",
                                     /*legacy_partition_name_enabled=*/true,
                                     /*external_paths=*/std::vector<std::string>(),
                                     /*global_index_external_path=*/std::nullopt,
                                     /*index_file_in_data_file_dir=*/false, mem_pool_));

    {
        BinaryRow row = BinaryRowGenerator::GenerateRow({10l, 5}, mem_pool_.get());
        ASSERT_OK_AND_ASSIGN(auto paths, path_factory->GetHierarchicalPartitionPath(row));
        ASSERT_EQ(2u, paths.size());
        ASSERT_EQ(PathUtil::JoinPath(dir->Str(), "/dt=10/"), paths[0]);
        ASSERT_EQ(PathUtil::JoinPath(dir->Str(), "/dt=10/hr=5/"), paths[1]);
    }
    {
        BinaryRow row = BinaryRowGenerator::GenerateRow({15l, NullType()}, mem_pool_.get());
        ASSERT_OK_AND_ASSIGN(auto paths, path_factory->GetHierarchicalPartitionPath(row));
        ASSERT_EQ(2u, paths.size());
        ASSERT_EQ(PathUtil::JoinPath(dir->Str(), "/dt=15/"), paths[0]);
        ASSERT_EQ(PathUtil::JoinPath(dir->Str(), "/dt=15/hr=default/"), paths[1]);
    }
}

TEST_F(FileStorePathFactoryTest, TestToBinaryRowAndToPartitionString) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),     arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),    arrow::field("f5", arrow::int32()),
        arrow::field("hr", arrow::int32()),    arrow::field("f7", arrow::int64()),
        arrow::field("dt", arrow::utf8()),     arrow::field("f9", arrow::float32()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(auto path_factory, FileStorePathFactory::Create(
                                                dir->Str(), schema, {"dt", "hr"}, "default",
                                                /*identifier=*/"mock_format",
                                                /*data_file_prefix=*/"data-",
                                                /*legacy_partition_name_enabled=*/true,
                                                /*external_paths=*/std::vector<std::string>(),
                                                /*global_index_external_path=*/std::nullopt,
                                                /*index_file_in_data_file_dir=*/false, mem_pool_));
    {
        std::map<std::string, std::string> partition_map;
        partition_map["dt"] = "20211224";
        partition_map["hr"] = "23";
        std::string expected = "dt=20211224/hr=23/";
        ASSERT_OK_AND_ASSIGN(BinaryRow binary_row, path_factory->ToBinaryRow(partition_map));
        ASSERT_OK_AND_ASSIGN(std::string actual, path_factory->GetPartitionString(binary_row));
        ASSERT_EQ(expected, actual);
    }
    {
        std::map<std::string, std::string> partition_map;
        partition_map["dt"] = "default";
        partition_map["hr"] = "default";
        std::string expected = "dt=default/hr=default/";
        ASSERT_OK_AND_ASSIGN(BinaryRow binary_row, path_factory->ToBinaryRow(partition_map));
        ASSERT_OK_AND_ASSIGN(std::string actual, path_factory->GetPartitionString(binary_row));
        ASSERT_EQ(expected, actual);
    }
    {
        std::map<std::string, std::string> partition_map;
        partition_map["dt"] = " a ";
        partition_map["hr"] = "22";
        std::string expected = "dt= a /hr=22/";
        ASSERT_OK_AND_ASSIGN(BinaryRow binary_row, path_factory->ToBinaryRow(partition_map));
        ASSERT_OK_AND_ASSIGN(std::string actual, path_factory->GetPartitionString(binary_row));
        ASSERT_EQ(expected, actual);
    }
    {
        std::map<std::string, std::string> partition_map;
        partition_map["dt"] = "   ";
        partition_map["hr"] = "22";
        std::string expected = "dt=default/hr=22/";
        ASSERT_OK_AND_ASSIGN(BinaryRow binary_row, path_factory->ToBinaryRow(partition_map));
        ASSERT_OK_AND_ASSIGN(std::string actual, path_factory->GetPartitionString(binary_row));
        ASSERT_EQ(expected, actual);
    }
    {
        std::map<std::string, std::string> partition_map;
        partition_map["dt"] = "";
        partition_map["hr"] = "22";
        std::string expected = "dt=default/hr=22/";
        ASSERT_OK_AND_ASSIGN(BinaryRow binary_row, path_factory->ToBinaryRow(partition_map));
        ASSERT_OK_AND_ASSIGN(std::string actual, path_factory->GetPartitionString(binary_row));
        ASSERT_EQ(expected, actual);
    }
    {
        std::map<std::string, std::string> partition_map;
        partition_map["dt"] = "[a=bc]";
        partition_map["hr"] = "23";
        std::string expected = "dt=%5Ba%3Dbc%5D/hr=23/";
        ASSERT_OK_AND_ASSIGN(BinaryRow binary_row, path_factory->ToBinaryRow(partition_map));
        ASSERT_OK_AND_ASSIGN(std::string actual, path_factory->GetPartitionString(binary_row));
        ASSERT_EQ(expected, actual);
    }
    {
        std::map<std::string, std::string> partition_map;
        partition_map["dt"] = "20240812";
        partition_map["hr"] = "default";
        std::string expected = "dt=20240812/hr=default/";
        ASSERT_OK_AND_ASSIGN(BinaryRow binary_row, path_factory->ToBinaryRow(partition_map));
        ASSERT_OK_AND_ASSIGN(std::string actual, path_factory->GetPartitionString(binary_row));
        ASSERT_EQ(expected, actual);
    }
    {
        std::map<std::string, std::string> partition_map;
        partition_map["dt"] = "default";
        partition_map["hr"] = "17";
        std::string expected = "dt=default/hr=17/";
        ASSERT_OK_AND_ASSIGN(BinaryRow binary_row, path_factory->ToBinaryRow(partition_map));
        ASSERT_OK_AND_ASSIGN(std::string actual, path_factory->GetPartitionString(binary_row));
        ASSERT_EQ(expected, actual);
    }
    {
        std::map<std::string, std::string> partition_map;
        partition_map["dt"] = "20240812";
        partition_map["hr"] = "somestr";
        ASSERT_NOK(path_factory->ToBinaryRow(partition_map));
    }
}

TEST_F(FileStorePathFactoryTest, TestConcurrentToBinaryRowAndGetPartitionString) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),     arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),    arrow::field("f5", arrow::int32()),
        arrow::field("hr", arrow::int32()),    arrow::field("f7", arrow::int64()),
        arrow::field("dt", arrow::utf8()),     arrow::field("f9", arrow::float32()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};
    auto schema = arrow::schema(fields);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(dir->Str(), schema, {"dt", "hr"}, "default",
                                     /*identifier=*/"mock_format", /*data_file_prefix=*/"data-",
                                     /*legacy_partition_name_enabled=*/true,
                                     /*external_paths=*/std::vector<std::string>(),
                                     /*global_index_external_path=*/std::nullopt,
                                     /*index_file_in_data_file_dir=*/false, mem_pool_));

    const std::vector<std::pair<std::map<std::string, std::string>, std::string>> test_cases = {
        {{{"dt", "20211224"}, {"hr", "23"}}, "dt=20211224/hr=23/"},
        {{{"dt", "default"}, {"hr", "17"}}, "dt=default/hr=17/"},
        {{{"dt", "20240812"}, {"hr", "5"}}, "dt=20240812/hr=5/"},
        {{{"dt", " a "}, {"hr", "22"}}, "dt= a /hr=22/"},
    };

    std::atomic<bool> has_error = false;
    std::mutex error_mutex;
    std::string first_error;

    constexpr int32_t kThreadNum = 8;
    constexpr int32_t kIterationsPerThread = 500;
    std::vector<std::thread> threads;
    threads.reserve(kThreadNum);

    for (int32_t thread_id = 0; thread_id < kThreadNum; ++thread_id) {
        threads.emplace_back([&, thread_id]() {
            for (int32_t i = 0; i < kIterationsPerThread; ++i) {
                const auto& [partition_map, expected] =
                    test_cases[(thread_id + i) % test_cases.size()];

                auto row_result = path_factory->ToBinaryRow(partition_map);
                if (!row_result.ok()) {
                    std::lock_guard<std::mutex> lock(error_mutex);
                    has_error.store(true, std::memory_order_relaxed);
                    if (first_error.empty()) {
                        first_error = "ToBinaryRow failed: " + row_result.status().ToString();
                    }
                    return;
                }

                auto partition_str_result = path_factory->GetPartitionString(row_result.value());
                if (!partition_str_result.ok()) {
                    std::lock_guard<std::mutex> lock(error_mutex);
                    has_error.store(true, std::memory_order_relaxed);
                    if (first_error.empty()) {
                        first_error = "GetPartitionString failed: " +
                                      partition_str_result.status().ToString();
                    }
                    return;
                }

                if (partition_str_result.value() != expected) {
                    std::lock_guard<std::mutex> lock(error_mutex);
                    has_error.store(true, std::memory_order_relaxed);
                    if (first_error.empty()) {
                        first_error = "Unexpected partition string, expected=" + expected +
                                      ", actual=" + partition_str_result.value();
                    }
                    return;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_FALSE(has_error.load(std::memory_order_relaxed)) << first_error;
}

TEST_F(FileStorePathFactoryTest, TestCreateIndexFileFactory) {
    {
        // test without external path & index_file_in_data_file_dir = false
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::int64()), arrow::field("f3", arrow::int16())};
        auto schema = arrow::schema(fields);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FileStorePathFactory> file_store_path_factory,
            FileStorePathFactory::Create(dir->Str(), schema, {"f0", "f1"}, "default",
                                         /*identifier=*/"mock_format",
                                         /*data_file_prefix=*/"data-",
                                         /*legacy_partition_name_enabled=*/true,
                                         /*external_paths=*/{},
                                         /*global_index_external_path=*/std::nullopt,
                                         /*index_file_in_data_file_dir=*/false, mem_pool_));
        auto partition = BinaryRowGenerator::GenerateRow({true, 10}, mem_pool_.get());
        ASSERT_OK_AND_ASSIGN(
            auto index_path_factory,
            file_store_path_factory->CreateIndexFileFactory(partition, /*bucket=*/2));
        ASSERT_EQ(index_path_factory->NewPath(),
                  dir->Str() + "/index/index-" + file_store_path_factory->uuid_ + "-0");
        ASSERT_EQ(index_path_factory->ToPath("global_bitmap.index"),
                  dir->Str() + "/index/global_bitmap.index");
    }
    {
        // test with external path & index_file_in_data_file_dir = false
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::int64()), arrow::field("f3", arrow::int16())};
        auto schema = arrow::schema(fields);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FileStorePathFactory> file_store_path_factory,
            FileStorePathFactory::Create(dir->Str(), schema, {"f0", "f1"}, "default",
                                         /*identifier=*/"mock_format",
                                         /*data_file_prefix=*/"data-",
                                         /*legacy_partition_name_enabled=*/true,
                                         /*external_paths=*/{"/tmp/external-path"},
                                         /*global_index_external_path=*/std::nullopt,
                                         /*index_file_in_data_file_dir=*/false, mem_pool_));
        auto partition = BinaryRowGenerator::GenerateRow({true, 10}, mem_pool_.get());
        ASSERT_OK_AND_ASSIGN(
            auto index_path_factory,
            file_store_path_factory->CreateIndexFileFactory(partition, /*bucket=*/2));
        ASSERT_EQ(index_path_factory->NewPath(),
                  dir->Str() + "/index/index-" + file_store_path_factory->uuid_ + "-0");
        ASSERT_EQ(index_path_factory->ToPath("global_bitmap.index"),
                  dir->Str() + "/index/global_bitmap.index");
    }
    {
        // test without external path & index_file_in_data_file_dir = true
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::int64()), arrow::field("f3", arrow::int16())};
        auto schema = arrow::schema(fields);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FileStorePathFactory> file_store_path_factory,
            FileStorePathFactory::Create(dir->Str(), schema, {"f0", "f1"}, "default",
                                         /*identifier=*/"mock_format",
                                         /*data_file_prefix=*/"data-",
                                         /*legacy_partition_name_enabled=*/true,
                                         /*external_paths=*/{},
                                         /*global_index_external_path=*/std::nullopt,
                                         /*index_file_in_data_file_dir=*/true, mem_pool_));
        auto partition = BinaryRowGenerator::GenerateRow({true, 10}, mem_pool_.get());
        ASSERT_OK_AND_ASSIGN(
            auto index_path_factory,
            file_store_path_factory->CreateIndexFileFactory(partition, /*bucket=*/2));
        ASSERT_EQ(index_path_factory->NewPath(), dir->Str() + "/f0=true/f1=10/bucket-2/index-" +
                                                     file_store_path_factory->uuid_ + "-0");
    }
    {
        // test with external path & index_file_in_data_file_dir = true
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::int64()), arrow::field("f3", arrow::int16())};
        auto schema = arrow::schema(fields);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FileStorePathFactory> file_store_path_factory,
            FileStorePathFactory::Create(dir->Str(), schema, {"f0", "f1"}, "default",
                                         /*identifier=*/"mock_format",
                                         /*data_file_prefix=*/"data-",
                                         /*legacy_partition_name_enabled=*/true,
                                         /*external_paths=*/{"/tmp/external-path"},
                                         /*global_index_external_path=*/std::nullopt,
                                         /*index_file_in_data_file_dir=*/true, mem_pool_));
        auto partition = BinaryRowGenerator::GenerateRow({true, 10}, mem_pool_.get());
        ASSERT_OK_AND_ASSIGN(
            auto index_path_factory,
            file_store_path_factory->CreateIndexFileFactory(partition, /*bucket=*/2));
        ASSERT_EQ(index_path_factory->NewPath(),
                  "/tmp/external-path/f0=true/f1=10/bucket-2/index-" +
                      file_store_path_factory->uuid_ + "-0");
    }
    {
        // test with global index external path
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::boolean()), arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::int64()), arrow::field("f3", arrow::int16())};
        auto schema = arrow::schema(fields);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FileStorePathFactory> file_store_path_factory,
            FileStorePathFactory::Create(dir->Str(), schema, {"f0", "f1"}, "default",
                                         /*identifier=*/"mock_format",
                                         /*data_file_prefix=*/"data-",
                                         /*legacy_partition_name_enabled=*/true,
                                         /*external_paths=*/std::vector<std::string>(),
                                         /*global_index_external_path=*/{"/tmp/external-path"},
                                         /*index_file_in_data_file_dir=*/false, mem_pool_));
        auto partition = BinaryRowGenerator::GenerateRow({true, 10}, mem_pool_.get());
        ASSERT_OK_AND_ASSIGN(
            auto index_path_factory,
            file_store_path_factory->CreateIndexFileFactory(partition, /*bucket=*/2));
        ASSERT_EQ(index_path_factory->ToPath("bitmap.index"), "/tmp/external-path/bitmap.index");
        ASSERT_TRUE(index_path_factory->IsExternalPath());

        auto index_file_meta = std::make_shared<IndexFileMeta>(
            /*index_type=*/"bitmap", /*file_name=*/"bitmap.index", /*file_size=*/10,
            /*row_count=*/5, /*dv_ranges=*/std::nullopt,
            /*external_path=*/"/tmp/external-path/bitmap.index",
            /*global_index_meta=*/std::nullopt);
        ASSERT_EQ(index_path_factory->ToPath(index_file_meta), "/tmp/external-path/bitmap.index");
    }
}

TEST_F(FileStorePathFactoryTest, TestCreateGlobalIndexCheckpointPathFactory) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<FileStorePathFactory> file_store_path_factory = CreateFactory(dir->Str());
    std::shared_ptr<FileSystem> file_system = dir->GetFileSystem();

    std::string prefix = "lumina-global-index-vector-10-20-task-1-";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexCheckpointPathFactory> checkpoint_path_factory,
                         file_store_path_factory->CreateGlobalIndexCheckpointPathFactory(
                             "lumina", "vector", Range(10, 20), "task-1"));
    std::string checkpoint_dir = PathUtil::JoinPath(dir->Str(), "index/checkpoint");
    ASSERT_EQ(checkpoint_path_factory->GetDirectoryPath(), checkpoint_dir);
    std::string first_path = checkpoint_path_factory->NewPath(0);
    ASSERT_EQ(first_path, PathUtil::JoinPath(checkpoint_dir, prefix + "0.index.ckpt"));
    std::string second_path = checkpoint_path_factory->NewPath(1);
    ASSERT_EQ(second_path, PathUtil::JoinPath(checkpoint_dir, prefix + "1.index.ckpt"));
    ASSERT_EQ(checkpoint_path_factory->ToPath("checkpoint"),
              PathUtil::JoinPath(checkpoint_dir, "checkpoint"));
    ASSERT_EQ(checkpoint_path_factory->GetCheckpointId(prefix + "9.index.ckpt"), 9);
    ASSERT_EQ(checkpoint_path_factory->GetCheckpointId(
                  "lumina-global-index-other-10-20-task-1-9.index.ckpt"),
              std::nullopt);
    ASSERT_EQ(checkpoint_path_factory->GetCheckpointId(prefix + "invalid-10.index.ckpt"),
              std::nullopt);
    ASSERT_EQ(checkpoint_path_factory->GetCheckpointId(prefix + "-1.index.ckpt"), std::nullopt);
    ASSERT_EQ(checkpoint_path_factory->GetCheckpointId(
                  prefix + "00000000-0000-0000-0000-000000000000-9.index.ckpt"),
              std::nullopt);
    ASSERT_EQ(checkpoint_path_factory->GetCheckpointId(
                  "lumina-global-index-7-vector-10_20-0000000000000000-"
                  "00000000-0000-0000-0000-000000000000-9.index.ckpt"),
              std::nullopt);
    ASSERT_EQ(checkpoint_path_factory->GetCheckpointId(
                  "lumina-global-index-field=vector-range=10_20-task=task-1-"
                  "00000000-0000-0000-0000-000000000000-9.index.ckpt"),
              std::nullopt);

    ASSERT_OK_AND_ASSIGN(bool exists, file_system->Exists(checkpoint_dir));
    ASSERT_FALSE(exists);
}

TEST_F(FileStorePathFactoryTest, TestCheckpointFileIdPath) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto factory = CreateFactory(dir->Str());
    for (int64_t file_id : {int64_t{9}, std::numeric_limits<int64_t>::max() - 1,
                            std::numeric_limits<int64_t>::max()}) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexCheckpointPathFactory> checkpoint_factory,
                             factory->CreateGlobalIndexCheckpointPathFactory(
                                 "lumina", "vector", Range(10, 20), "task-1"));
        std::string path = checkpoint_factory->NewPath(file_id);
        ASSERT_EQ(checkpoint_factory->GetCheckpointId(PathUtil::GetName(path)), file_id);
        ASSERT_OK_AND_ASSIGN(bool exists,
                             dir->GetFileSystem()->Exists(checkpoint_factory->GetDirectoryPath()));
        ASSERT_FALSE(exists);
    }
}

TEST_F(FileStorePathFactoryTest, TestCheckpointTaskIsolation) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<FileSystem> fs = dir->GetFileSystem();
    std::shared_ptr<FileStorePathFactory> factory = CreateFactory(dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexCheckpointPathFactory> own_factory,
                         factory->CreateGlobalIndexCheckpointPathFactory("lumina", "vector",
                                                                         Range(10, 20), "task-1"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexCheckpointPathFactory> other_factory,
                         factory->CreateGlobalIndexCheckpointPathFactory("lumina", "vector",
                                                                         Range(10, 20), "task-2"));
    ASSERT_EQ(own_factory->GetDirectoryPath(), other_factory->GetDirectoryPath());
    ASSERT_OK(fs->Mkdirs(own_factory->GetDirectoryPath()));
    std::string own_path = own_factory->NewPath(0);
    ASSERT_OK(fs->WriteFile(own_path, "checkpoint", /*overwrite=*/false));
    std::string other_path = other_factory->NewPath(100);
    ASSERT_OK(fs->WriteFile(other_path, "foreign", /*overwrite=*/false));
    ASSERT_EQ(own_factory->GetCheckpointId(PathUtil::GetName(own_path)), 0);
    ASSERT_EQ(own_factory->GetCheckpointId(PathUtil::GetName(other_path)), std::nullopt);

    GlobalIndexFileManager manager(fs, factory->CreateGlobalIndexFileFactory(),
                                   std::move(own_factory));
    ASSERT_OK_AND_ASSIGN(bool exists, manager.CheckpointExists());
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> input, manager.OpenCheckpointInputStream());
    char buffer[10];
    ASSERT_OK_AND_ASSIGN(int64_t read, input->Read(buffer, sizeof(buffer)));
    ASSERT_EQ(read, 10);
    ASSERT_EQ(std::string(buffer, sizeof(buffer)), "checkpoint");
    ASSERT_OK(input->Close());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> output,
                         manager.CreateCheckpointOutputStream());
    ASSERT_OK_AND_ASSIGN(std::string uri, output->GetUri());
    ASSERT_TRUE(StringUtils::EndsWith(uri, "-1.index.ckpt"));
    ASSERT_OK(output->Close());
    ASSERT_OK(manager.DeleteCheckpoint());
    ASSERT_OK_AND_ASSIGN(exists, manager.CheckpointExists());
    ASSERT_FALSE(exists);
    ASSERT_NOK_WITH_MSG(manager.OpenCheckpointInputStream(), "checkpoint file does not exist");
    ASSERT_OK(manager.DeleteCheckpoint());
    ASSERT_OK_AND_ASSIGN(exists, fs->Exists(other_path));
    ASSERT_TRUE(exists);
}

TEST_F(FileStorePathFactoryTest, TestCheckpointRejectsInvalidPathComponents) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto factory = CreateFactory(dir->Str());
    for (const std::string& invalid : std::vector<std::string>{"", ".", "..", " ", "a/b", "a\\b",
                                                               "a\nb", std::string("a\0b", 3)}) {
        ASSERT_NOK(factory->CreateGlobalIndexCheckpointPathFactory(invalid, "vector", Range(10, 20),
                                                                   "task"));
        ASSERT_NOK(factory->CreateGlobalIndexCheckpointPathFactory("lumina", invalid, Range(10, 20),
                                                                   "task"));
        ASSERT_NOK(factory->CreateGlobalIndexCheckpointPathFactory("lumina", "vector",
                                                                   Range(10, 20), invalid));
    }
    ASSERT_OK_AND_ASSIGN(bool exists, dir->GetFileSystem()->Exists(
                                          PathUtil::JoinPath(dir->Str(), "index/checkpoint")));
    ASSERT_FALSE(exists);
}
}  // namespace paimon::test
