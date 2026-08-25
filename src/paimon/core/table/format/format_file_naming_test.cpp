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

#include "paimon/core/table/format/format_file_naming.h"

#include <string>

#include "gtest/gtest.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(FormatFileNamingTest, TestNamesAreDataPrefixedAndNumbered) {
    ASSERT_OK_AND_ASSIGN(
        FormatFileNaming naming,
        FormatFileNaming::Create("parquet", FormatFileNaming::kDefaultDataFilePrefix));
    std::string first = naming.NextFileName();
    std::string second = naming.NextFileName();

    ASSERT_TRUE(StringUtils::StartsWith(first, "data-"));
    ASSERT_TRUE(StringUtils::EndsWith(first, "-0.parquet"));
    ASSERT_TRUE(StringUtils::EndsWith(second, "-1.parquet"));
    // Both files of one write share its uuid.
    ASSERT_EQ(first.substr(0, first.size() - std::string("-0.parquet").size()),
              second.substr(0, second.size() - std::string("-1.parquet").size()));
}

TEST(FormatFileNamingTest, TestTwoWritesDoNotCollide) {
    ASSERT_OK_AND_ASSIGN(
        FormatFileNaming first_write,
        FormatFileNaming::Create("parquet", FormatFileNaming::kDefaultDataFilePrefix));
    ASSERT_OK_AND_ASSIGN(
        FormatFileNaming second_write,
        FormatFileNaming::Create("parquet", FormatFileNaming::kDefaultDataFilePrefix));
    ASSERT_NE(first_write.NextFileName(), second_write.NextFileName());
}

TEST(FormatFileNamingTest, TestTempPathIsAHiddenNameInATemporaryDirectory) {
    ASSERT_OK_AND_ASSIGN(
        FormatFileNaming naming,
        FormatFileNaming::Create("parquet", FormatFileNaming::kDefaultDataFilePrefix));
    ASSERT_OK_AND_ASSIGN(std::string first, naming.NextTempFilePath());
    ASSERT_OK_AND_ASSIGN(std::string second, naming.NextTempFilePath());
    // The `_temporary` directory and the leading '.' are both hidden, which is what a scan skips,
    // and are the layout Java Paimon stages under.
    ASSERT_TRUE(StringUtils::StartsWith(first, "_temporary/.tmp.")) << first;
    ASSERT_TRUE(FormatFileNaming::IsTempFilePath(first));
    // Each staged name carries a uuid of its own, so two writers staging into the one shared
    // `_temporary` directory cannot collide.
    ASSERT_NE(first, second);

    // Anything else is not a path this write staged: a plain file beside the target, a name
    // outside `_temporary`, or a deeper tree another job staged into.
    ASSERT_FALSE(FormatFileNaming::IsTempFilePath("data-abc-0.parquet"));
    ASSERT_FALSE(FormatFileNaming::IsTempFilePath(".data-abc-0.parquet.tmp"));
    ASSERT_FALSE(FormatFileNaming::IsTempFilePath("_temporary/"));
    ASSERT_FALSE(FormatFileNaming::IsTempFilePath("_temporary/.tmp."));
    ASSERT_FALSE(FormatFileNaming::IsTempFilePath("_temporary/attempt_0/part-0.parquet"));
    ASSERT_FALSE(FormatFileNaming::IsTempFilePath("_temporary/0/.tmp.abc"));
}

TEST(FormatFileNamingTest, TestPrefixComesFromDataFilePrefix) {
    ASSERT_OK_AND_ASSIGN(FormatFileNaming naming, FormatFileNaming::Create("parquet", "part-"));
    std::string name = naming.NextFileName();
    ASSERT_TRUE(StringUtils::StartsWith(name, "part-"));
    ASSERT_TRUE(StringUtils::EndsWith(name, "-0.parquet"));
}

TEST(FormatFileNamingTest, TestRejectsAPrefixThatIsNotOneFileNameComponent) {
    // The prefix goes straight into a file name that is joined onto a directory, and the file is
    // created before any commit sees it - so a separator or a `..` here would put data outside the
    // table with nothing left to stop it.
    for (const char* prefix :
         {"nested/", "../outside-", "nested/../../outside-", "a\\b", "..", "."}) {
        ASSERT_NOK(FormatFileNaming::Create("parquet", prefix)) << prefix;
    }
    // The extension reaches the same file name, so it is held to the same rule.
    ASSERT_NOK(FormatFileNaming::Create("../parquet", FormatFileNaming::kDefaultDataFilePrefix));
}

TEST(FormatFileNamingTest, TestRejectsHiddenPrefix) {
    // A scan skips every file whose name starts with '_' or '.', so such a prefix would write
    // rows that can never be read back.
    ASSERT_NOK(FormatFileNaming::Create("parquet", "_data-"));
    ASSERT_NOK(FormatFileNaming::Create("parquet", ".data-"));
}

TEST(FormatFileNamingTest, TestRejectsEmptyExtension) {
    ASSERT_NOK(FormatFileNaming::Create("", FormatFileNaming::kDefaultDataFilePrefix));
}

}  // namespace paimon::test
