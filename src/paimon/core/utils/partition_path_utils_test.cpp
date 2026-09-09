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

TEST(PartitionPathUtilsTest, UnescapePathName) {
    ASSERT_EQ(PartitionPathUtils::UnescapePathName(""), "");
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("normal_path"), "normal_path");
    // Round trips whatever `EscapePathName` produced, in either digit case.
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("a b%2Fc"), "a b/c");
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("a b%2fc"), "a b/c");
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("%25"), "%");

    // A `%` that starts no sequence stays as written, which is what lets a directory another
    // engine wrote be read at all.
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("100%"), "100%");
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("%zz"), "%zz");
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("%2"), "%2");
    // Only two plain hex digits count: neither whitespace nor a sign starts a sequence, and
    // `EscapePathName` never writes one.
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("% 1x"), "% 1x");
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("%+1x"), "%+1x");
    // A `%` among the last two characters starts no sequence.
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("a%41"), "aA");
    ASSERT_EQ(PartitionPathUtils::UnescapePathName("%41"), "A");
}

TEST(PartitionPathUtilsTest, ExtractPartitionKeyValue) {
    std::optional<std::pair<std::string, std::string>> key_value =
        PartitionPathUtils::ExtractPartitionKeyValue("dt=2025-01-01");
    ASSERT_TRUE(key_value.has_value());
    ASSERT_EQ(key_value->first, "dt");
    ASSERT_EQ(key_value->second, "2025-01-01");

    // Both halves are unescaped, so a value holding an escaped `=` reads back whole.
    key_value = PartitionPathUtils::ExtractPartitionKeyValue("dt=a%3Db");
    ASSERT_TRUE(key_value.has_value());
    ASSERT_EQ(key_value->second, "a=b");

    // Anything that is not one `key=value` belongs to something else: another layout, a nested
    // table, or a directory this table never wrote.
    ASSERT_FALSE(PartitionPathUtils::ExtractPartitionKeyValue("dt").has_value());
    ASSERT_FALSE(PartitionPathUtils::ExtractPartitionKeyValue("=2025").has_value());
    ASSERT_FALSE(PartitionPathUtils::ExtractPartitionKeyValue("dt=").has_value());
    // A `=` is escaped on the way in, so no directory paimon wrote carries a second one.
    ASSERT_FALSE(PartitionPathUtils::ExtractPartitionKeyValue("dt=a=b").has_value());
}

TEST(PartitionPathUtilsTest, IsHiddenName) {
    ASSERT_TRUE(PartitionPathUtils::IsHiddenName("_temporary"));
    ASSERT_TRUE(PartitionPathUtils::IsHiddenName(".hive-staging_1"));
    ASSERT_FALSE(PartitionPathUtils::IsHiddenName("dt=2025-01-01"));
    ASSERT_FALSE(PartitionPathUtils::IsHiddenName(""));
}

TEST(PartitionPathUtilsTest, ValidatePartitionValueForPath) {
    ASSERT_OK(PartitionPathUtils::ValidatePartitionValueForPath("2025", /*only_value=*/false));
    ASSERT_OK(PartitionPathUtils::ValidatePartitionValueForPath("2025", /*only_value=*/true));

    // An empty value names no directory under either layout.
    ASSERT_NOK_WITH_MSG(PartitionPathUtils::ValidatePartitionValueForPath("", false),
                        "cannot be used as a partition path component");
    // A bare "." or ".." would name the directory itself or its parent; under `key=value` the
    // "=" already keeps them apart from a relative path.
    ASSERT_NOK(PartitionPathUtils::ValidatePartitionValueForPath(".", true));
    ASSERT_NOK(PartitionPathUtils::ValidatePartitionValueForPath("..", true));
    ASSERT_OK(PartitionPathUtils::ValidatePartitionValueForPath("..", false));
}

TEST(PartitionPathUtilsTest, GenerateValueOnlyPartitionPath) {
    std::vector<std::pair<std::string, std::string>> partition_spec = {
        {"year", "2025"},
        {"month", "01"},
    };
    ASSERT_OK_AND_ASSIGN(std::string partition_path, PartitionPathUtils::GeneratePartitionPath(
                                                         partition_spec, /*only_value=*/true));
    ASSERT_EQ(partition_path, "2025/01/");

    // The value is still escaped, so a separator inside it cannot add a level.
    ASSERT_OK_AND_ASSIGN(partition_path, PartitionPathUtils::GeneratePartitionPath(
                                             {{"dt", "a/b"}}, /*only_value=*/true));
    ASSERT_EQ(partition_path, "a%2Fb/");

    // A value the layout cannot name is refused where the path is built, which is the one place
    // every writer and every commit goes through.
    ASSERT_NOK(PartitionPathUtils::GeneratePartitionPath({{"dt", ".."}}, /*only_value=*/true));
    ASSERT_NOK(PartitionPathUtils::GeneratePartitionPath({{"dt", ""}}, /*only_value=*/false));
}

}  // namespace paimon::test
