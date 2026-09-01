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

#include "paimon/common/utils/path_util.h"

#include "gtest/gtest.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(PathUtilsTest, TestJoinPath) {
    ASSERT_EQ("/tmp/test_path/test", PathUtil::JoinPath("/tmp/test_path/test", ""));
}

TEST(PathUtilsTest, TestGetParentDirPath) {
    ASSERT_EQ("/tmp/test_path", PathUtil::GetParentDirPath("/tmp/test_path/test"));
    ASSERT_EQ("/tmp/test_path", PathUtil::GetParentDirPath("/tmp/test_path/test/"));
}

TEST(PathUtilsTest, TestNormalizePathWithEmptyString) {
    std::string test_path = "";
    ASSERT_NOK_WITH_MSG(PathUtil::NormalizePath(test_path), "path is an empty string.");
}

TEST(PathUtilsTest, TestNormalizePathWithNoScheme) {
    std::string test_path = "//tmp////index";
    ASSERT_OK_AND_ASSIGN(std::string normalize_path, PathUtil::NormalizePath(test_path));
    std::string expected_path = "//tmp/index";
    ASSERT_EQ(normalize_path, expected_path);
}

TEST(PathUtilsTest, TestNormalizePath) {
    {
        // test with no authority
        std::string test_path = "hdfs:///tmp/test_path/test_subdir";
        ASSERT_OK_AND_ASSIGN(std::string normalize_path, PathUtil::NormalizePath(test_path));
        std::string expected_path = "hdfs:/tmp/test_path/test_subdir";
        ASSERT_EQ(normalize_path, expected_path);
    }
    {
        // test with authority
        std::string test_path = "hdfs://tmp/test_path//test_subdir/";
        ASSERT_OK_AND_ASSIGN(std::string normalize_path, PathUtil::NormalizePath(test_path));
        std::string expected_path = "hdfs://tmp/test_path/test_subdir";
        ASSERT_EQ(normalize_path, expected_path);
    }
    {
        // test with no authority
        std::string test_path = "hdfs:///tmp/test_path//test_subdir/";
        ASSERT_OK_AND_ASSIGN(std::string normalize_path, PathUtil::NormalizePath(test_path));
        std::string expected_path = "hdfs:/tmp/test_path/test_subdir";
        ASSERT_EQ(normalize_path, expected_path);
    }
}

TEST(PathUtilsTest, TestTrimLastDelim) {
    {
        std::string path = "hdfs://auth/test_path/test_subdir";
        PathUtil::TrimLastDelim(&path);
        ASSERT_EQ(path, "hdfs://auth/test_path/test_subdir");
    }
    {
        std::string path = "hdfs://auth/test_path/test_subdir/";
        PathUtil::TrimLastDelim(&path);
        ASSERT_EQ(path, "hdfs://auth/test_path/test_subdir");
    }
    {
        std::string path = "/";
        PathUtil::TrimLastDelim(&path);
        ASSERT_EQ(path, "/");
    }
    {
        std::string path = "";
        PathUtil::TrimLastDelim(&path);
        ASSERT_EQ(path, "");
    }
}

TEST(PathUtilsTest, TestGetWorkingDirectory) {
    ASSERT_OK_AND_ASSIGN(std::string current_path, PathUtil::GetWorkingDirectory());
    ASSERT_FALSE(current_path.empty());
    ASSERT_EQ(current_path[0], '/');
}

TEST(PathUtilsTest, TestToPath) {
    {
        std::string test_path = "";
        ASSERT_NOK_WITH_MSG(PathUtil::ToPath(test_path), "path is an empty string.");
    }
    {
        std::string test_path = "FILE:///tmp";
        ASSERT_OK_AND_ASSIGN(Path path, PathUtil::ToPath(test_path));
        ASSERT_EQ(path.scheme, "FILE");
        ASSERT_EQ(path.authority, "");
        ASSERT_EQ(path.path, "/tmp");
        ASSERT_EQ(path.ToString(), "FILE:/tmp");
    }
    {
        std::string test_path = "dfs://tmp/index";
        ASSERT_OK_AND_ASSIGN(Path path, PathUtil::ToPath(test_path));
        ASSERT_EQ(path.scheme, "dfs");
        ASSERT_EQ(path.authority, "tmp");
        ASSERT_EQ(path.path, "/index");
        ASSERT_EQ(path.ToString(), "dfs://tmp/index");
    }
    {
        std::string test_path = "http://example.com:8080/api";
        ASSERT_OK_AND_ASSIGN(Path path, PathUtil::ToPath(test_path));
        ASSERT_EQ(path.scheme, "http");
        ASSERT_EQ(path.authority, "example.com:8080");
        ASSERT_EQ(path.path, "/api");
        ASSERT_EQ(path.ToString(), "http://example.com:8080/api");
    }
    {
        std::string test_path = "/tmp/index";
        ASSERT_OK_AND_ASSIGN(Path path, PathUtil::ToPath(test_path));
        ASSERT_EQ(path.scheme, "");
        ASSERT_EQ(path.authority, "");
        ASSERT_EQ(path.path, "/tmp/index");
        ASSERT_EQ(path.ToString(), "/tmp/index");
    }
    {
        std::string test_path = ".";
        ASSERT_OK_AND_ASSIGN(Path path, PathUtil::ToPath(test_path));
        ASSERT_EQ(path.scheme, "");
        ASSERT_EQ(path.authority, "");
        ASSERT_EQ(path.path, ".");
        ASSERT_EQ(path.ToString(), ".");
    }
    {
        std::string test_path = "relative/path";
        ASSERT_OK_AND_ASSIGN(Path path, PathUtil::ToPath(test_path));
        ASSERT_EQ(path.scheme, "");
        ASSERT_EQ(path.authority, "");
        ASSERT_EQ(path.path, "relative/path");
        ASSERT_EQ(path.ToString(), "relative/path");
    }
}

TEST(PathUtilsTest, TestGetName) {
    ASSERT_EQ("test", PathUtil::GetName("hdfs://tmp/test_path/test/"));
    ASSERT_EQ("test", PathUtil::GetName("hdfs://tmp/test_path/test"));
    ASSERT_EQ("test", PathUtil::GetName("test"));
}

TEST(PathUtilsTest, TestCreateTempPath) {
    // tmp path: hdfs://tmp/test_path/.test.<uuid>.tmp;
    ASSERT_OK_AND_ASSIGN(std::string tmp_path,
                         PathUtil::CreateTempPath("hdfs://tmp/test_path/test"));
    ASSERT_EQ("hdfs://tmp/test_path", PathUtil::GetParentDirPath(tmp_path));
    auto tmp_name = PathUtil::GetName(tmp_path);
    ASSERT_TRUE(StringUtils::StartsWith(tmp_name, ".test."));
    ASSERT_TRUE(StringUtils::EndsWith(tmp_name, ".tmp"));
}

TEST(PathUtilsTest, TestCheckSinglePathComponent) {
    // Names that stay a single path component, including names that merely contain a dot and
    // names outside ascii.
    for (const char* name : {"db1", "my.db", "a..b", "a b", "数据", "\u00e9t\u00e9"}) {
        ASSERT_OK(PathUtil::CheckSinglePathComponent("database", name));
    }

    ASSERT_NOK_WITH_MSG(PathUtil::CheckSinglePathComponent("database", ""),
                        "database name cannot be empty or whitespace");
    ASSERT_NOK_WITH_MSG(PathUtil::CheckSinglePathComponent("database", " \t\n "),
                        "database name cannot be empty or whitespace");
    ASSERT_NOK_WITH_MSG(PathUtil::CheckSinglePathComponent("table", "."),
                        "table name cannot be '.' or '..'");
    ASSERT_NOK_WITH_MSG(PathUtil::CheckSinglePathComponent("table", ".."),
                        "table name cannot be '.' or '..'");
    ASSERT_NOK_WITH_MSG(PathUtil::CheckSinglePathComponent("table", "../escaped"),
                        "table name cannot contain path separators");
    ASSERT_NOK_WITH_MSG(PathUtil::CheckSinglePathComponent("table", "back\\slash"),
                        "table name cannot contain path separators");
    ASSERT_NOK_WITH_MSG(PathUtil::CheckSinglePathComponent("branch", "line\nfeed"),
                        "branch name cannot contain control characters");
    ASSERT_NOK_WITH_MSG(PathUtil::CheckSinglePathComponent("branch", std::string("nul\0byte", 8)),
                        "branch name cannot contain control characters");
}

TEST(PathUtilsTest, TestCheckSinglePathComponentEscapesRejectedName) {
    // The rejected name is escaped, so that it can neither add a line to the log the error is
    // written to nor truncate the C string it is copied into.
    Status newline = PathUtil::CheckSinglePathComponent("database", "line\nfeed\r\t");
    ASSERT_FALSE(newline.ok());
    ASSERT_EQ(newline.ToString().find('\n'), std::string::npos);
    ASSERT_NE(newline.ToString().find("line\\nfeed\\r\\t"), std::string::npos);

    Status nul = PathUtil::CheckSinglePathComponent("database", std::string("nul\0byte", 8));
    ASSERT_FALSE(nul.ok());
    ASSERT_EQ(nul.ToString().find('\0'), std::string::npos);
    ASSERT_NE(nul.ToString().find("nul\\x{00}byte"), std::string::npos);
}

}  // namespace paimon::test
