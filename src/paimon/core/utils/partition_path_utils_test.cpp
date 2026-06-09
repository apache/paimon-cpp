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

#include "paimon/core/utils/partition_path_utils.h"

#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(PartitionPathUtilsTest, TestEmptyInput) {
    std::vector<std::pair<std::string, std::string>> partition_spec;
    ASSERT_OK_AND_ASSIGN(std::string partition_path_str,
                         PartitionPathUtils::GeneratePartitionPath(partition_spec));
    ASSERT_EQ(partition_path_str, "");
}

TEST(PartitionPathUtilsTest, TestSimple) {
    std::vector<std::pair<std::string, std::string>> partition_spec = {
        {"f1", "v1"},
        {"f2", "这是一段不是特别长的中文"},
        {"f0", "v0"},
    };
    ASSERT_OK_AND_ASSIGN(std::string partition_path_str,
                         PartitionPathUtils::GeneratePartitionPath(partition_spec));
    ASSERT_EQ(partition_path_str, "f1=v1/f2=这是一段不是特别长的中文/f0=v0/");
}

TEST(PartitionPathUtilsTest, TestCharToEscape) {
    std::vector<std::pair<std::string, std::string>> partition_spec = {
        {"f0", "v0"},
        {"f1", "v1="},
        {"/f2?", "这是一段不是特别长\n的[中文]"},
    };
    ASSERT_OK_AND_ASSIGN(std::string partition_path_str,
                         PartitionPathUtils::GeneratePartitionPath(partition_spec));
    ASSERT_EQ(partition_path_str, "f0=v0/f1=v1%3D/%2Ff2%3F=这是一段不是特别长%0A的%5B中文%5D/");
}

TEST(PartitionPathUtilsTest, testGenerateHierarchicalPartitionPaths) {
    std::vector<std::pair<std::string, std::string>> partition_spec = {
        {"f2", "这是一段不是特别长的中文"},
        {"f0", "v0"},
        {"f1", "v1"},
    };
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> partition_path_strs,
                         PartitionPathUtils::GenerateHierarchicalPartitionPaths(partition_spec));
    ASSERT_EQ(partition_path_strs.size(), 3u);
    ASSERT_EQ(partition_path_strs[0], "f2=这是一段不是特别长的中文/");
    ASSERT_EQ(partition_path_strs[1], "f2=这是一段不是特别长的中文/f0=v0/");
    ASSERT_EQ(partition_path_strs[2], "f2=这是一段不是特别长的中文/f0=v0/f1=v1/");
}

TEST(PartitionPathUtilsTest, EscapeChar) {
    std::stringstream ss;
    PartitionPathUtils::EscapeChar(' ', &ss);
    ASSERT_EQ(ss.str(), "%20");

    ss.str("");
    ss.clear();
    PartitionPathUtils::EscapeChar('/', &ss);
    ASSERT_EQ(ss.str(), "%2F");

    ss.str("");
    ss.clear();
    PartitionPathUtils::EscapeChar('\n', &ss);
    ASSERT_EQ(ss.str(), "%0A");

    ss.str("");
    ss.clear();
    PartitionPathUtils::EscapeChar('A', &ss);
    ASSERT_EQ(ss.str(), "%41");
}

TEST(PartitionPathUtilsTest, EscapePathName) {
    ASSERT_NOK_WITH_MSG(PartitionPathUtils::EscapePathName(""), "path should not be empty");

    ASSERT_OK_AND_ASSIGN(std::string escape_path,
                         PartitionPathUtils::EscapePathName("normal_path"));
    ASSERT_EQ(escape_path, "normal_path");

    ASSERT_OK_AND_ASSIGN(escape_path, PartitionPathUtils::EscapePathName("a b/c"));
    ASSERT_EQ(escape_path, "a b%2Fc");

    ASSERT_OK_AND_ASSIGN(escape_path, PartitionPathUtils::EscapePathName(" /="));
    ASSERT_EQ(escape_path, " %2F%3D");
}

}  // namespace paimon::test
