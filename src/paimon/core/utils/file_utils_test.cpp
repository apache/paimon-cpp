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

}  // namespace paimon::test
