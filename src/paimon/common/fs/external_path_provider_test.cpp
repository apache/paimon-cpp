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

#include "paimon/common/fs/external_path_provider.h"

#include <set>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(ExternalPathProviderTest, TestInvalidExternalDataPath) {
    std::vector<std::string> external_table_paths;
    std::string relative_bucket_path = "p0=1/p1=0/bucket-0";

    auto provider = ExternalPathProvider::Create(external_table_paths, relative_bucket_path);
    ASSERT_NOK_WITH_MSG(provider.status(), "external table paths cannot be empty");
}

TEST(ExternalPathProviderTest, TestGetNextExternalDataPath) {
    std::vector<std::string> external_table_paths;
    external_table_paths.emplace_back("/tmp/external_path/");
    std::string relative_bucket_path = "p0=1/p1=0/bucket-0";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ExternalPathProvider> provider,
                         ExternalPathProvider::Create(external_table_paths, relative_bucket_path));
    ASSERT_EQ(provider->GetNextExternalDataPath("file.orc"),
              "/tmp/external_path/p0=1/p1=0/bucket-0/file.orc");
}

TEST(ExternalPathProviderTest, TestGetNextExternalDataPath2) {
    std::vector<std::string> external_table_paths;
    external_table_paths.emplace_back("/tmp/external_path_a/");
    external_table_paths.emplace_back("/tmp/external_path_b/");
    external_table_paths.emplace_back("/tmp/external_path_c/");
    std::string relative_bucket_path = "p0=1/p1=0/bucket-0";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ExternalPathProvider> provider,
                         ExternalPathProvider::Create(external_table_paths, relative_bucket_path));

    std::set<std::string> result_data_paths;
    result_data_paths.insert(provider->GetNextExternalDataPath("file.orc"));
    result_data_paths.insert(provider->GetNextExternalDataPath("file.orc"));
    result_data_paths.insert(provider->GetNextExternalDataPath("file.orc"));

    ASSERT_EQ(result_data_paths, std::set<std::string>({
                                     "/tmp/external_path_a/p0=1/p1=0/bucket-0/file.orc",
                                     "/tmp/external_path_b/p0=1/p1=0/bucket-0/file.orc",
                                     "/tmp/external_path_c/p0=1/p1=0/bucket-0/file.orc",
                                 }));
}
}  // namespace paimon::test
