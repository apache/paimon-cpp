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

#include "paimon/core/utils/file_utils.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

/// Counts what a listing asks of the file system, so the round trips it saves stay saved. On a
/// remote store an `Exists()` in front of a `ListDir()` is a trip of its own, and the listing
/// already answers the same question: a directory that is not there comes back empty.
class ListingCountingFileSystem : public LocalFileSystem {
 public:
    Result<bool> Exists(const std::string& path) const override {
        ++exists_count;
        return LocalFileSystem::Exists(path);
    }

    Status ListDir(const std::string& directory,
                   std::vector<BasicFileStatus>* file_status_list) const override {
        ++list_dir_count;
        return LocalFileSystem::ListDir(directory, file_status_list);
    }

    mutable int exists_count = 0;
    mutable int list_dir_count = 0;
};

}  // namespace

TEST(FileUtilsTest, TestSimple) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/snapshot/";
    std::vector<int64_t> files;
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK(FileUtils::ListVersionedFiles(std::move(fs), test_data_path, "snapshot-", &files));
    ASSERT_EQ(files.size(), 5u);
}

TEST(FileUtilsTest, TestNotExist) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/not_exist/";
    std::vector<int64_t> files;
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK(FileUtils::ListVersionedFiles(std::move(fs), test_data_path, "snapshot-", &files));
    ASSERT_EQ(files.size(), 0u);
}

TEST(FileUtilsTest, TestNotNumber) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/manifest/";
    std::vector<int64_t> files;
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_NOK(FileUtils::ListVersionedFiles(std::move(fs), test_data_path, "manifest-", &files));
}

TEST(FileUtilsTest, TestListVersionedFilesListsWithoutProbingExistence) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/snapshot/";
    auto fs = std::make_shared<ListingCountingFileSystem>();
    std::vector<int64_t> files;
    ASSERT_OK(FileUtils::ListVersionedFiles(fs, test_data_path, "snapshot-", &files));
    ASSERT_EQ(files.size(), 5u);
    ASSERT_EQ(fs->list_dir_count, 1);
    ASSERT_EQ(fs->exists_count, 0);
}

// The missing directory is the case the probe used to be justified by, so it is the one that has
// to reach the same answer without it.
TEST(FileUtilsTest, TestListVersionedFilesMissingDirListsWithoutProbingExistence) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/not_exist/";
    auto fs = std::make_shared<ListingCountingFileSystem>();
    std::vector<int64_t> files;
    ASSERT_OK(FileUtils::ListVersionedFiles(fs, test_data_path, "snapshot-", &files));
    ASSERT_EQ(files.size(), 0u);
    ASSERT_EQ(fs->list_dir_count, 1);
    ASSERT_EQ(fs->exists_count, 0);
}

}  // namespace paimon::test
